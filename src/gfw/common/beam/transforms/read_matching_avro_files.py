"""Module containing an Apache Beam transform for reading Avro files with datetime filtering."""

import codecs
import logging

from datetime import datetime, timedelta
from typing import Any, Callable, Optional, Sequence

import apache_beam as beam

from apache_beam.io import fileio
from apache_beam.io.avroio import ReadAllFromAvro
from apache_beam.pvalue import PCollection

from gfw.common.datetime import datetime_from_isoformat, datetime_from_string


logger = logging.getLogger(__name__)


class ReadMatchingAvroFilesError(Exception):
    """Custom exception for errors of `ReadMatchingAvroFiles` PTransform."""

    pass


class ReadMatchingAvroFiles(beam.PTransform):
    """Wrapper around :class:`~beam.io.avroio.ReadAllFromAvro` with filtering.

    This transform's primary function is to intelligently filter filenames
    based on a time range. It works by:

    1. **Generating Date-based Patterns**: It first generates a list of file
       patterns for each day within the specified ``start_dt`` and ``end_dt``.
       This efficiently prunes the search space for large, time-partitioned datasets.

    2. **Precise Datetime Filtering**: After matching the daily patterns, it
       applies a second, more precise filter to ensure that only files with a
       timestamp strictly within the ``start_dt`` and ``end_dt`` are processed.

    This PTransform is a generic and reusable component for any data pipeline
    that needs to perform historical data backfills on time-partitioned Avro files.

    Args:
        path:
            The path to the location of the Avro files.
            It is assumed that the data is date-partitioned,
            so this parameter must include a ``date`` placeholder. It can be local path,
            a GCS location, or any other Beam-supported filesystem path.
            For example:
                - ``gs://my-bucket/nmea-{date}/*.avro``
                - ``gs://my-bucket/*{date}*.avro``
                - ``/path/to/data/{date}/*.avro``

        start_dt:
            The start datetime of the range, in ISO format (e.g., ``YYYY-MM-DDTHH:MM:SS``).

        end_dt:
            The end datetime of the range, in ISO format (e.g., ``YYYY-MM-DDTHH:MM:SS``).
            Datetimes equal to this value are considered outside the range.

        buffer_days:
            Number of extra whole days to include before the datetime range.
            Useful to ensure boundary records are not excluded.

        buffer_minutes:
            Number of extra minutes to include before the datetime range.
            Provides finer-grained control, to avoid fetching unnecessary files.

        record_time_fn:
            Function that extracts a event timestamp from a record.
            It should accept a record dictionary and return a :class:`~datetime.datetime`.
            This allows custom logic such as accessing nested fields,
            parsing strings, or applying fallback values.
            The extracted timestamp is used for the last filtering step.

        strict:
            If True, raises an exception if the ``record_time_fn`` failed to extract the timestamp.
            If False, will skip the failing record.

        date_format:
            The strftime/strptime format to use when matching dates in avro files.
            Defaults to ``%Y-%m-%d``.

        time_format:
            The strftime/strptime format to use when matching times in avro files.
            Defaults to ``%H_%M_%SZ``.

        allow_no_time:
            If True, allows paths to not contain time information,
            and a default of 0 will be applied.
            If False, it will raise a :class:`ValueError`.

        decode:
            Whether to decode the data from bytes to string.
            Default is True.

        decode_method:
            The method used to decode the message data.
            Supported methods include standard encodings like ``utf-8``, ``ascii``, etc.
            Default is ``utf-8``.

        read_all_from_avro_kwargs:
            Any additional keyword arguments to be passed to Beam's :class:`ReadAllFromAvro` class.
            Check `official Apache Beam documentation
            <https://beam.apache.org/releases/pydoc/2.64.0/apache_beam.io.avroio.html#apache_beam.io.avroio.ReadAllFromAvro>`_.

        **kwargs:
            Additional keyword arguments passed to base PTransform class.

    Raises:
        :class:`ValueError`:
            When a path does not contain time information and ``allow_no_time`` is False.

    Returns:
        PCollection:
            A PCollection of Avro records from the files within the specified datetime range.
    """

    MSG_FAILED_EXTRACTING_TIMESTAMP = "Failed to extract timestamp from record: {}."

    def __init__(
        self,
        path: str,
        start_dt: str,
        end_dt: str,
        buffer_days: int = 1,
        buffer_minutes: int = 1,
        record_time_fn: Optional[Callable[[dict], datetime]] = None,
        strict: bool = False,
        date_format: str = "%Y-%m-%d",
        time_format: str = "%H_%M_%SZ",
        allow_no_time: bool = False,
        decode: bool = True,
        decode_method: str = "utf-8",
        read_all_from_avro_kwargs: Optional[dict[str, Any]] = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self._path = path
        self._start_dt = datetime_from_isoformat(start_dt)
        self._end_dt = datetime_from_isoformat(end_dt)
        self._buffer_days = buffer_days
        self._buffer_minutes = buffer_minutes
        self._record_time_fn = record_time_fn
        self._strict = strict
        self._date_format = date_format
        self._time_format = time_format
        self._allow_no_time = allow_no_time
        self._decode = decode
        self._decode_method = decode_method
        self._read_all_from_avro_kwargs = read_all_from_avro_kwargs or {}

        self._validate_decode_method()

    def _generate_file_patterns(self) -> Sequence[str]:
        current_date = self._start_dt.date() - timedelta(days=self._buffer_days)
        end_date = self._end_dt.date()
        patterns = []

        while current_date <= end_date:
            patterns.append(self._path.format(date=current_date.strftime(self._date_format)))
            current_date += timedelta(days=1)

        return patterns

    def _validate_decode_method(self) -> None:
        try:
            codecs.lookup(self._decode_method)
        except LookupError as e:
            raise ValueError(f"Unsupported decode method: {self._decode_method}") from e

        logger.info(f"Using decode method: {self._decode_method}.")

    def _decode_records(self, record: dict) -> dict:
        record = {**record}
        record["data"] = record["data"].decode(self._decode_method)

        return record

    def is_path_in_range(self, path: str) -> bool:
        """Checks if a path containing a datetime is within the provided datetime range."""
        dt = datetime_from_string(
            path,
            date_format=self._date_format,
            time_format=self._time_format,
            allow_no_time=self._allow_no_time,
        )

        start_dt = self._start_dt - timedelta(minutes=self._buffer_minutes)
        res = start_dt <= dt < self._end_dt

        logger.debug(f"Matched path (inside datetime range? = {res}).")
        logger.debug(path)

        return res

    def _is_record_in_range(self, record: dict) -> bool:
        try:
            dt = self._record_time_fn(record)
        except Exception as e:
            if self._strict:
                raise ReadMatchingAvroFilesError(
                    f"{self.MSG_FAILED_EXTRACTING_TIMESTAMP.format(e)}"
                    "Check if your record_time_fn is valid."
                ) from e

            logger.warning(f"{self.MSG_FAILED_EXTRACTING_TIMESTAMP.format(e)} Skipping record.")

            return False

        return self._start_dt <= dt < self._end_dt

    def expand(self, pcoll: PCollection) -> PCollection:
        """Applies the transform to the pipeline root and returns a PCollection of messages.

        Args:
            pcoll:
                An input PCollection.
                This is expected to be a :class:`PBegin` when used with a real
                or mocked :class:`ReadFromPubSub`,
                since Pub/Sub sources begin from the pipeline root.

        Returns:
            :class:`beam.PCollection`:
                A PCollection of dictionaries where each dictionary contains the following keys:

                - ``data``: The decoded message string (if decoding is enabled).
                - ``attributes``: A dictionary of message attributes (if available).
        """
        logger.info("Generating file patterns...")
        file_patterns = self._generate_file_patterns()

        logger.info(f"Generated {len(file_patterns)} file patterns, first: {file_patterns[0]}")

        records = (
            pcoll
            | "CreatePatterns" >> beam.Create(file_patterns)
            | "MatchFiles" >> fileio.MatchAll()
            | "FilterFilesByTime" >> beam.Filter(lambda m: self.is_path_in_range(m.path))
            | "ReadAvroRecords" >> ReadAllFromAvro(**self._read_all_from_avro_kwargs)
        )
        if self._record_time_fn:
            records = records | "FilterRecordsByTime" >> beam.Filter(self._is_record_in_range)

        if self._decode:
            records = records | beam.Map(self._decode_records)

        return records
