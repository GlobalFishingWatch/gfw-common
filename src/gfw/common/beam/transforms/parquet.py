"""PTransform for writing hive-partitioned Parquet files."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, BinaryIO, Callable, Iterable, Literal

import apache_beam as beam
import pyarrow as pa

from apache_beam.io import fileio
from apache_beam.io.fileio import FileSink
from apache_beam.transforms.trigger import AccumulationMode
from apache_beam.transforms.window import IntervalWindow


Row = dict[str, Any]
Partitions = dict[str, Callable[[str], str]]


@dataclass
class HivePartitionConfig:
    """Hive-style partition layout for :class:`WritePartitionedParquet`.

    Args:
        fields:
            Extra partition dimensions applied before the time component, as
            an ordered ``{field_name: fn}`` dict. ``field_name`` is read from
            the element and ``fn`` maps its value to a partition string.
            Insertion order determines the directory hierarchy.
            Defaults to no extra dimensions (time-only partitioning).

        prefix:
            Prefix applied to every partition key name, including the time
            components. Avoids collisions with element field names when
            BigQuery reads the external table. Defaults to ``"event_"``.

        time_granularity:
            Granularity of the time partition component. ``"hour"`` produces
            ``{prefix}date=YYYY-MM-DD/{prefix}hour=HH/``; ``"day"`` produces
            ``{prefix}date=YYYY-MM-DD/`` only. Should be chosen based on query
            patterns, independently of ``window_size``. Defaults to ``"hour"``.
    """

    fields: Partitions = field(default_factory=dict)
    prefix: str = "event_"
    time_granularity: Literal["hour", "day"] = "hour"


def _partition_path(element: tuple[str, Row]) -> str:
    partition, _ = element
    return partition


class WritePartitionedParquet(beam.PTransform):
    """Writes elements to hive-partitioned Parquet files.

    Output files are organized using Hive-style partitioning. Time partitioning
    is always present and derived from the window start. Extra dimensions are
    user-defined and prepended before the time component::

        {path}/{prefix}{dim1}={v1}/{prefix}{dim2}={v2}/.../{prefix}date=YYYY-MM-DD/{prefix}hour=HH/

    For example, with ``HivePartitionConfig(fields={"source": fn})``::

        {path}/event_source=kpler/event_date=2024-01-15/event_hour=08/

    ## Tuning window_size and num_shards

    The primary tuning knob is the combination of ``window_size`` and
    ``num_shards``, which together determine output file size::

        rows_per_file = (rows_per_second x window_size) / num_shards

    File size directly controls:

    - Memory consumption per worker: larger files = more memory during flush.
    - Query performance: files that are too small increase metadata overhead
      in BigQuery external table scans; files that are too large reduce
      parallelism.
    - GCS write latency: files must be written within the window duration,
      so very large files with short windows can cause the pipeline to stall.

    As a rule of thumb, target file sizes of 10-50 MB.

    ## Row groups

    Each output file contains a single Parquet row group, which is optimal
    for BigQuery external table scans. This is a natural consequence of
    writing all rows for a shard in a single flush at window close.

    Args:
        path:
            Output path where files will be written.

        schema:
            PyArrow schema used by the Parquet writer.

        window_size:
            Size of the fixed window in seconds. Controls how frequently
            files are written. Shorter windows reduce latency and file size
            but produce more files. Defaults to 60 seconds.

        allowed_lateness:
            Allowed lateness in seconds. Late records arriving within this
            duration after the window closes will still be included.
            Defaults to 0.

        accumulation_mode:
            Beam accumulation mode. Defaults to ``DISCARDING``, which is
            correct for most streaming use cases.

        num_shards:
            Number of output files per partition per window. Must be tuned
            together with ``window_size`` to hit the target file size.
            Defaults to 6.

        codec:
            Compression codec for Parquet. Defaults to ``"snappy"``.

        file_suffix:
            File suffix. Defaults to ``".parquet"``.

        partition:
            Hive partition layout. Defaults to hourly time-only partitioning
            with ``"event_"`` prefix. See :class:`HivePartitionConfig`.

        sink_factory:
            A callable ``(schema, codec) -> FileSink`` used to create the sink
            for each output file. Defaults to :class:`ParquetSink`. Inject
            :class:`FakeParquetSink` to bypass GCS writes in tests while still
            exercising windowing, partitioning, and file-naming logic.
    """

    def __init__(
        self,
        path: str,
        schema: pa.Schema,
        window_size: int = 60,
        allowed_lateness: int = 0,
        accumulation_mode: AccumulationMode = AccumulationMode.DISCARDING,  # type: ignore
        num_shards: int = 6,
        codec: str = "snappy",
        file_suffix: str = ".parquet",
        partition: HivePartitionConfig | None = None,
        sink_factory: Callable[..., ParquetSink] | None = None,
    ) -> None:
        self._path = path
        self._schema = schema
        self._window_size = window_size
        self._allowed_lateness = allowed_lateness
        self._accumulation_mode = accumulation_mode
        self._num_shards = num_shards
        self._codec = codec
        self._file_suffix = file_suffix
        self._partition = partition or HivePartitionConfig()
        self._sink_factory = sink_factory or ParquetSink
        self._validate()

    def _validate(self) -> None:
        max_window = {"hour": 3600, "day": 86400}[self._partition.time_granularity]
        if self._window_size > max_window:
            granularity = self._partition.time_granularity
            raise ValueError(
                f"window_size={self._window_size}s exceeds the {granularity} partition period "
                f"({max_window}s). Data spanning multiple {granularity}s would be "
                f"silently filed under the window-start {granularity} only."
            )

    def expand(self, pcoll: beam.PCollection) -> beam.PCollection:  # type: ignore[name-defined]
        """Applies windowing, partition keying, and Parquet file writing."""
        return (
            pcoll
            | "Window"
            >> beam.WindowInto(
                beam.window.FixedWindows(self._window_size),  # type: ignore[attr-defined]
                allowed_lateness=beam.utils.timestamp.Duration(seconds=self._allowed_lateness),
                accumulation_mode=self._accumulation_mode,
            )
            | "AddPartitionKey" >> beam.ParDo(_AddPartitionKey(self._partition))
            | "WriteParquet"
            >> fileio.WriteToFiles(
                path=self._path,
                destination=_partition_path,
                sink=lambda dest: self._sink_factory(schema=self._schema, codec=self._codec),
                file_naming=_safe_filenaming(suffix=self._file_suffix),
                shards=self._num_shards,
                # The following ensures we always use sharded destinations.
                # All the data in the window will be split in a fixed X number of shards.
                max_writers_per_bundle=0,
            )
        )


class _AddPartitionKey(beam.DoFn):
    """Builds the full hive partition path for each element.

    Derives the time component from the window start (cached per window) and
    prepends any user-defined extra partition dimensions.
    """

    def __init__(self, partition: HivePartitionConfig) -> None:
        self._partition = partition

    def start_bundle(self) -> None:
        """Initialises the per-bundle window time cache."""
        self._time_cache: dict[Any, str] = {}

    def process(
        self,
        element: Row,
        window: IntervalWindow = beam.DoFn.WindowParam,  # type: ignore[assignment]
    ) -> Iterable[tuple[str, Row]]:
        """Builds the partition path and yields a keyed element.

        Args:
            element:
                The PCollection element (a dict row).

            window:
                Window metadata containing start/end timestamps.

        Yields:
            A tuple of the full partition path string and the element.
        """
        if window.start not in self._time_cache:
            start = window.start.to_utc_datetime()
            time_parts = [f"{self._partition.prefix}date={start:%Y-%m-%d}"]

            if self._partition.time_granularity == "hour":
                time_parts.append(f"{self._partition.prefix}hour={start:%H}")

            self._time_cache[window.start] = "/".join(time_parts) + "/"

        extra = "/".join(
            f"{self._partition.prefix}{field}={fn(element[field])}"
            for field, fn in self._partition.fields.items()
        )

        path = (extra + "/" if extra else "") + self._time_cache[window.start]

        yield path, element


class ParquetSink(FileSink):
    """A :class:`~apache_beam.io.fileio.FileSink` that buffers rows and flushes as one row group.

    Args:
        schema: PyArrow schema for the Parquet file.
        codec: Parquet compression codec (e.g. ``"snappy"``, ``"zstd"``).
    """

    def __init__(self, schema: pa.Schema, codec: str = "snappy") -> None:
        self._schema = schema
        self._codec = codec

    def open(self, fh: BinaryIO) -> None:
        """Open the Parquet writer and initialise the row buffer."""
        self._writer = pa.parquet.ParquetWriter(
            fh,
            self._schema,
            compression=self._codec,
        )
        self._buffer: list[Row] = []

    def write(self, element: tuple[str, Row]) -> None:
        """Buffer one keyed row for writing."""
        _, row = element
        self._buffer.append(row)

    def flush(self) -> None:
        """Write all buffered rows as a single row group and close the writer."""
        try:
            if self._buffer:
                batch = pa.RecordBatch.from_pylist(self._buffer, schema=self._schema)
                self._writer.write_batch(batch)
        finally:
            self._buffer = None  # type: ignore[assignment]
            self._writer.close()
            self._writer = None


class FakeParquetSink(ParquetSink):
    """A no-op :class:`ParquetSink` for testing.

    Discards all data without writing. Inject via ``sink_factory`` on
    :class:`WritePartitionedParquet` to exercise windowing, partitioning,
    and file-naming logic without touching the filesystem.
    """

    def __init__(self, schema: pa.Schema, codec: str = "snappy") -> None:
        pass

    def open(self, fh: BinaryIO) -> None:  # noqa: D102
        pass

    def write(self, element: tuple[str, Row]) -> None:  # noqa: D102
        pass

    def flush(self) -> None:  # noqa: D102
        pass


def _safe_filenaming(suffix: Any = None) -> Callable[..., str]:
    def _inner(
        window: Any,
        pane: Any,
        shard_index: Any,
        total_shards: Any,
        compression: Any,
        destination: Any,
    ) -> str:
        return fileio.destination_prefix_naming(suffix)(  # type: ignore[call-arg]
            window, pane, shard_index, total_shards, compression, destination
        ).replace(":", ".")

    return _inner
