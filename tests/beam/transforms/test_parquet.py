"""Unit tests for transforms/write_to_parquet.py."""

import io
import tempfile

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import apache_beam as beam
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to
from apache_beam.transforms.trigger import AccumulationMode
from apache_beam.transforms.window import IntervalWindow
from apache_beam.utils.timestamp import Timestamp

from gfw.common.beam.transforms.parquet import (
    HivePartitionConfig,
    WritePartitionedParquet,
    _AddPartitionKey,
    _ParquetSink,
    _safe_filenaming,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

SCHEMA = pa.schema(
    [
        pa.field("mmsi", pa.int64(), nullable=True),
        pa.field("nmea", pa.string(), nullable=False),
        pa.field("x", pa.float64(), nullable=True),
    ]
)


def make_window(year=2024, month=1, day=15, hour=8, minute=0, second=0):
    dt = datetime(year, month, day, hour, minute, second, tzinfo=timezone.utc)
    start = Timestamp(dt.timestamp())
    return IntervalWindow(start, Timestamp(dt.timestamp() + 60))


def make_fh():
    return io.BytesIO()


def read_parquet(fh: io.BytesIO) -> pa.Table:
    fh.seek(0)
    return pq.read_table(fh)


def make_keyed(row: dict, path: str = "event_date=2024-01-15/event_hour=08/") -> tuple:
    return (path, row)


def call_naming(suffix, hour=8):
    fn = _safe_filenaming(suffix=suffix)
    pane = MagicMock()
    pane.index = 0
    pane.is_last = True
    dest = f"event_source=kpler/event_date=2024-01-15/event_hour=0{hour}/"
    return fn(make_window(hour=hour), pane, 0, 1, None, dest)


def run_partition(dofn, element, window):
    return list(dofn.process(element, window=window))


def make_dofn(partition: HivePartitionConfig | None = None):
    dofn = _AddPartitionKey(partition or HivePartitionConfig())
    dofn.start_bundle()
    return dofn


# ---------------------------------------------------------------------------
# _AddPartitionKey — time-only (no extra partitions)
# ---------------------------------------------------------------------------


def test_time_only_hourly():
    dofn = make_dofn()
    path, _ = run_partition(dofn, {}, make_window(2024, 1, 15, 8))[0]
    assert path == "event_date=2024-01-15/event_hour=08/"


def test_time_only_daily():
    dofn = make_dofn(HivePartitionConfig(time_granularity="day"))
    path, _ = run_partition(dofn, {}, make_window(2024, 1, 15, 8))[0]
    assert path == "event_date=2024-01-15/"


def test_time_only_midnight():
    dofn = make_dofn()
    path, _ = run_partition(dofn, {}, make_window(2024, 3, 1, 0))[0]
    assert path == "event_date=2024-03-01/event_hour=00/"


def test_time_only_end_of_day():
    dofn = make_dofn()
    path, _ = run_partition(dofn, {}, make_window(2024, 3, 1, 23))[0]
    assert path == "event_date=2024-03-01/event_hour=23/"


# ---------------------------------------------------------------------------
# _AddPartitionKey — extra partitions
# ---------------------------------------------------------------------------


def test_single_partition_prepended():
    dofn = make_dofn(HivePartitionConfig(fields={"source": lambda v: v}))
    path, _ = run_partition(dofn, {"source": "kpler"}, make_window(2024, 1, 15, 8))[0]
    assert path == "event_source=kpler/event_date=2024-01-15/event_hour=08/"


def test_multiple_partitions_ordered():
    dofn = make_dofn(
        HivePartitionConfig(
            fields={
                "source": lambda v: v,
                "receiver_type": lambda v: v,
            }
        )
    )
    path, _ = run_partition(
        dofn, {"source": "kpler", "receiver_type": "satellite"}, make_window(2024, 1, 15, 8)
    )[0]
    expected = (
        "event_source=kpler/event_receiver_type=satellite/event_date=2024-01-15/event_hour=08/"
    )
    assert path == expected


def test_partition_fn_applied():
    dofn = make_dofn(HivePartitionConfig(fields={"source": lambda v: v.split("-")[0]}))
    path, _ = run_partition(dofn, {"source": "kpler-terrestrial"}, make_window(2024, 1, 15, 8))[0]
    assert path == "event_source=kpler/event_date=2024-01-15/event_hour=08/"


def test_custom_partition_prefix():
    dofn = make_dofn(HivePartitionConfig(fields={"source": lambda v: v}, prefix="p_"))
    path, _ = run_partition(dofn, {"source": "kpler"}, make_window(2024, 1, 15, 8))[0]
    assert path == "p_source=kpler/p_date=2024-01-15/p_hour=08/"


def test_element_passed_through_unchanged():
    dofn = make_dofn(HivePartitionConfig(fields={"source": lambda v: v}))
    row = {"source": "kpler", "mmsi": 123}
    _, value = run_partition(dofn, row, make_window())[0]
    assert value is row


# ---------------------------------------------------------------------------
# _AddPartitionKey — caching
# ---------------------------------------------------------------------------


def test_time_cache_hit_same_window():
    dofn = make_dofn()
    w = make_window(hour=8)
    run_partition(dofn, {}, w)
    run_partition(dofn, {}, w)
    assert len(dofn._time_cache) == 1


def test_time_cache_miss_different_window():
    dofn = make_dofn()
    run_partition(dofn, {}, make_window(hour=8))
    run_partition(dofn, {}, make_window(hour=9))
    assert len(dofn._time_cache) == 2


def test_time_cache_called_once_per_window_regardless_of_partition_values():
    """to_utc_datetime is called once per window even with many distinct sources."""
    dofn = make_dofn(HivePartitionConfig(fields={"source": lambda v: v}))
    w = make_window(hour=8)
    for source in ["kpler", "spire", "orbcomm", "exactearth"]:
        run_partition(dofn, {"source": source}, w)
    assert len(dofn._time_cache) == 1


def test_start_bundle_resets_time_cache():
    dofn = make_dofn()
    run_partition(dofn, {}, make_window(hour=8))
    dofn.start_bundle()
    assert dofn._time_cache == {}


# ---------------------------------------------------------------------------
# _AddPartitionKey — pipeline integration
# ---------------------------------------------------------------------------


def test_pipeline_integration_with_source():
    with TestPipeline() as p:
        window = make_window(2024, 1, 15, 10)
        rows = [{"mmsi": i, "source": "kpler"} for i in range(3)]
        result = (
            p
            | beam.Create(rows)
            | beam.Map(lambda r: beam.window.TimestampedValue(r, window.start))
            | beam.WindowInto(beam.window.FixedWindows(60))
            | beam.ParDo(_AddPartitionKey(HivePartitionConfig(fields={"source": lambda v: v})))
            | beam.Map(lambda x: x[0])
        )
        assert_that(
            result, equal_to(["event_source=kpler/event_date=2024-01-15/event_hour=10/"] * 3)
        )


def test_pipeline_integration_time_only():
    with TestPipeline() as p:
        window = make_window(2024, 1, 15, 10)
        rows = [{"mmsi": i} for i in range(3)]
        result = (
            p
            | beam.Create(rows)
            | beam.Map(lambda r: beam.window.TimestampedValue(r, window.start))
            | beam.WindowInto(beam.window.FixedWindows(60))
            | beam.ParDo(_AddPartitionKey(HivePartitionConfig()))
            | beam.Map(lambda x: x[0])
        )
        assert_that(result, equal_to(["event_date=2024-01-15/event_hour=10/"] * 3))


# ---------------------------------------------------------------------------
# _safe_filenaming
# ---------------------------------------------------------------------------


def test_safe_filenaming_no_colons():
    assert ":" not in call_naming(".parquet")


def test_safe_filenaming_suffix_appended():
    assert call_naming(".parquet").endswith(".parquet")


def test_safe_filenaming_no_suffix_no_colons():
    assert ":" not in call_naming(None)


# ---------------------------------------------------------------------------
# _ParquetSink — construction
# ---------------------------------------------------------------------------


def test_sink_stores_schema_and_codec():
    s = _ParquetSink(schema=SCHEMA, codec="zstd")
    assert s._schema == SCHEMA
    assert s._codec == "zstd"


def test_sink_default_codec():
    assert _ParquetSink(schema=SCHEMA)._codec == "snappy"


# ---------------------------------------------------------------------------
# _ParquetSink — open
# ---------------------------------------------------------------------------


def test_open_creates_writer_and_empty_buffer():
    s = _ParquetSink(schema=SCHEMA)
    s.open(make_fh())
    assert isinstance(s._writer, pa.parquet.ParquetWriter)
    assert s._buffer == []
    s.flush()


def test_open_passes_codec_to_writer():
    s = _ParquetSink(schema=SCHEMA, codec="gzip")
    fh = make_fh()
    with patch("pyarrow.parquet.ParquetWriter") as mock_cls:
        mock_cls.return_value = MagicMock()
        s.open(fh)
    mock_cls.assert_called_once_with(fh, SCHEMA, compression="gzip")


# ---------------------------------------------------------------------------
# _ParquetSink — write
# ---------------------------------------------------------------------------


def test_write_appends_row():
    s = _ParquetSink(schema=SCHEMA)
    s.open(make_fh())
    row = {"mmsi": 1, "nmea": "x", "x": 1.0}
    s.write(make_keyed(row))
    assert s._buffer == [row]
    s.flush()


def test_write_ignores_partition_key():
    s = _ParquetSink(schema=SCHEMA)
    s.open(make_fh())
    row = {"mmsi": 1, "nmea": "x", "x": None}
    s.write(("some/partition/path/", row))
    assert s._buffer[0] is row
    s.flush()


def test_write_accumulates_multiple_rows():
    s = _ParquetSink(schema=SCHEMA)
    s.open(make_fh())
    rows = [{"mmsi": i, "nmea": f"m{i}", "x": float(i)} for i in range(5)]
    for row in rows:
        s.write(make_keyed(row))
    assert len(s._buffer) == 5
    s.flush()


# ---------------------------------------------------------------------------
# _ParquetSink — flush: normal path
# ---------------------------------------------------------------------------


def test_flush_writes_correct_data():
    s = _ParquetSink(schema=SCHEMA)
    fh = make_fh()
    s.open(fh)
    for row in [{"mmsi": 1, "nmea": "msg1", "x": 1.0}, {"mmsi": 2, "nmea": "msg2", "x": 2.0}]:
        s.write(make_keyed(row))
    s.flush()
    table = read_parquet(fh)
    assert table.num_rows == 2
    assert table.column("nmea").to_pylist() == ["msg1", "msg2"]


def test_flush_empty_buffer_does_not_raise():
    s = _ParquetSink(schema=SCHEMA)
    s.open(make_fh())
    s.flush()
    assert s._writer is None
    assert s._buffer is None


def test_flush_nulls_writer_and_buffer():
    s = _ParquetSink(schema=SCHEMA)
    s.open(make_fh())
    s.write(make_keyed({"mmsi": 1, "nmea": "x", "x": None}))
    s.flush()
    assert s._writer is None
    assert s._buffer is None


def test_flush_handles_null_values():
    s = _ParquetSink(schema=SCHEMA)
    fh = make_fh()
    s.open(fh)
    s.write(make_keyed({"mmsi": None, "nmea": "msg", "x": None}))
    s.flush()
    table = read_parquet(fh)
    assert table.column("mmsi").to_pylist() == [None]
    assert table.column("x").to_pylist() == [None]


def test_flush_produces_single_row_group():
    s = _ParquetSink(schema=SCHEMA)
    fh = make_fh()
    s.open(fh)
    for i in range(100):
        s.write(make_keyed({"mmsi": i, "nmea": f"m{i}", "x": float(i)}))
    s.flush()
    fh.seek(0)
    assert pq.ParquetFile(fh).metadata.num_row_groups == 1


# ---------------------------------------------------------------------------
# _ParquetSink — flush: exception safety
# ---------------------------------------------------------------------------


def test_flush_closes_writer_if_write_batch_raises():
    s = _ParquetSink(schema=SCHEMA)
    s.open(make_fh())
    s.write(make_keyed({"mmsi": 1, "nmea": "x", "x": 1.0}))
    mock_writer = MagicMock()
    mock_writer.write_batch.side_effect = RuntimeError("disk full")
    s._writer = mock_writer

    with pytest.raises(RuntimeError, match="disk full"):
        s.flush()

    mock_writer.close.assert_called_once()
    assert s._writer is None
    assert s._buffer is None


def test_flush_closes_writer_if_from_pylist_raises():
    bad_schema = pa.schema([pa.field("mmsi", pa.int64())])
    s = _ParquetSink(schema=bad_schema, codec="snappy")
    s.open(make_fh())
    s._buffer = [{"mmsi": "not-an-int"}]
    mock_writer = MagicMock()
    s._writer = mock_writer

    with pytest.raises(pa.lib.ArrowInvalid):
        s.flush()

    mock_writer.close.assert_called_once()
    assert s._writer is None
    assert s._buffer is None


# ---------------------------------------------------------------------------
# WritePartitionedParquet — constructor
# ---------------------------------------------------------------------------


def test_write_partitioned_defaults():
    t = WritePartitionedParquet(path="gs://bucket/path", schema=SCHEMA)
    assert t._path == "gs://bucket/path"
    assert t._schema is SCHEMA
    assert t._window_size == 60
    assert t._allowed_lateness == 0
    assert t._num_shards == 6
    assert t._codec == "snappy"
    assert t._file_suffix == ".parquet"
    assert t._partition == HivePartitionConfig()


def test_write_partitioned_custom_parameters():
    cfg = HivePartitionConfig(fields={"source": lambda v: v}, prefix="p_", time_granularity="day")
    t = WritePartitionedParquet(
        path="gs://other/path",
        schema=SCHEMA,
        window_size=30,
        allowed_lateness=10,
        accumulation_mode=AccumulationMode.ACCUMULATING,
        num_shards=5,
        codec="zstd",
        file_suffix=".snappy.parquet",
        partition=cfg,
    )
    assert t._window_size == 30
    assert t._allowed_lateness == 10
    assert t._num_shards == 5
    assert t._codec == "zstd"
    assert t._file_suffix == ".snappy.parquet"
    assert t._partition.prefix == "p_"
    assert t._partition.time_granularity == "day"


def test_window_size_exceeds_hour_granularity_raises():
    with pytest.raises(ValueError, match="window_size"):
        WritePartitionedParquet(path="gs://b/p", schema=SCHEMA, window_size=3601)


def test_window_size_exceeds_day_granularity_raises():
    with pytest.raises(ValueError, match="window_size"):
        WritePartitionedParquet(
            path="gs://b/p",
            schema=SCHEMA,
            window_size=86401,
            partition=HivePartitionConfig(time_granularity="day"),
        )


def test_window_size_equal_to_granularity_is_valid():
    WritePartitionedParquet(path="gs://b/p", schema=SCHEMA, window_size=3600)
    WritePartitionedParquet(
        path="gs://b/p",
        schema=SCHEMA,
        window_size=86400,
        partition=HivePartitionConfig(time_granularity="day"),
    )


# ---------------------------------------------------------------------------
# WritePartitionedParquet — expand integration
# ---------------------------------------------------------------------------


def test_expand_with_source_partition():
    with tempfile.TemporaryDirectory() as tmpdir:
        with TestPipeline() as p:
            window = make_window(2024, 1, 15, 10)
            rows = [
                {"mmsi": i, "nmea": f"msg{i}", "x": float(i), "source": "kpler"} for i in range(3)
            ]
            (
                p
                | beam.Create(rows)
                | beam.Map(lambda r: beam.window.TimestampedValue(r, window.start))
                | WritePartitionedParquet(
                    path=tmpdir,
                    schema=SCHEMA,
                    window_size=60,
                    num_shards=1,
                    partition=HivePartitionConfig(fields={"source": lambda v: v}),
                )
            )


def test_expand_time_only():
    with tempfile.TemporaryDirectory() as tmpdir:
        with TestPipeline() as p:
            window = make_window(2024, 1, 15, 10)
            rows = [{"mmsi": i, "nmea": f"msg{i}", "x": float(i)} for i in range(3)]
            (
                p
                | beam.Create(rows)
                | beam.Map(lambda r: beam.window.TimestampedValue(r, window.start))
                | WritePartitionedParquet(
                    path=tmpdir,
                    schema=SCHEMA,
                    window_size=60,
                    num_shards=1,
                )
            )
