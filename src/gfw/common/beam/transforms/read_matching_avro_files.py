"""Module containing an Apache Beam transform for reading Avro files with datetime filtering."""

import codecs
import logging

from datetime import timedelta
from typing import Any, Optional, Sequence

import apache_beam as beam

from apache_beam.io import fileio
from apache_beam.io.avroio import ReadAllFromAvro
from apache_beam.pvalue import PCollection

from gfw.common.datetime import datetime_from_isoformat, datetime_from_string


logger = logging.getLogger(__name__)


class ReadMatchingAvroFiles(beam.PTransform):
    """A generic PTransform to filter and read Avro files from any Beam-supported filesystem.

    This transform's primary function is to intelligently filter filenames
    based on a time range. It works by:

    1. **Generating Date-based Patterns**: It first generates a list of file
       patterns for each day within the specified `start_dt` and `end_dt`. This
       efficiently prunes the search space for large, time-partitioned datasets.

    2. **Precise Datetime Filtering**: After matching the daily patterns, it
       applies a second, more precise filter to ensure that only files with a
       timestamp strictly within the `start_dt` and `end_dt` are processed.

    This PTransform is a generic and reusable component for any data pipeline
    that needs to perform historical data backfills on time-partitioned Avro files.

    Args:
        path:
            The path to the location of the Avro files.
            It is assumed that the data is date-partitioned,
            so this parameter must include a 'date' placeholder. It can be local path,
            a GCS location, or any other Beam-supported filesystem path.
            For example:
                - 'gs://my-bucket/nmea-{date}/*.avro'
                - 'gs://my-bucket/*{date}*.avro'
                - '/path/to/data/{date}/*.avro'

        start_dt:
            The start datetime of the range, in ISO format (e.g., 'YYYY-MM-DDTHH:MM:SS').

        end_dt:
            The end datetime of the range, in ISO format (e.g., 'YYYY-MM-DDTHH:MM:SS').
            Datetimes equal to this value are considered outside the range.

        date_format:
            The strftime/strptime format to use when matching dates in avro files.
            Defaults to "%Y-%m-%d".

        time_format:
            The strftime/strptime format to use when matching times in avro files.
            Defaults to "%H_%M_%SZ".

        decode:
            Whether to decode the data from bytes to string.
            Default is True.

        decode_method:
            The method used to decode the message data.
            Supported methods include standard encodings like "utf-8", "ascii", etc.
            Default is "utf-8".

        read_all_from_avro_kwargs:
            Any additional keyword arguments to be passed to Beam's `ReadAllFromAvro` class.
            Check official documentation:
            https://beam.apache.org/releases/pydoc/2.64.0/apache_beam.io.avroio.html#apache_beam.io.avroio.ReadAllFromAvro

        **kwargs:
            Additional keyword arguments passed to base PTransform class.

    Returns:
        PCollection:
            A PCollection of Avro records from the files within the specified datetime range.
    """

    def __init__(
        self,
        path: str,
        start_dt: str,
        end_dt: str,
        date_format: str = "%Y-%m-%d",
        time_format: str = "%H_%M_%SZ",
        decode: bool = True,
        decode_method: str = "utf-8",
        read_all_from_avro_kwargs: Optional[dict[Any, Any]] = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self._path = path
        self._start_dt = datetime_from_isoformat(start_dt)
        self._end_dt = datetime_from_isoformat(end_dt)
        self._date_format = date_format
        self._time_format = time_format
        self._decode = decode
        self._decode_method = decode_method
        self._read_all_from_avro_kwargs = read_all_from_avro_kwargs or {}

        self._validate_decode_method()

    def _generate_file_patterns(self) -> Sequence[str]:
        current_date = self._start_dt.date()
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
        try:
            dt = datetime_from_string(
                path, date_format=self._date_format, time_format=self._time_format
            )
        except ValueError as e:
            logger.error(f"Couldn't extract datetime from path: {e}")
            return False

        res = self._start_dt <= dt < self._end_dt

        logger.debug(f"Matched path (inside datetime range? = {res}).")
        logger.debug(path)

        return res

    def expand(self, pcoll: PCollection) -> PCollection:
        """Applies the transform to the pipeline root and returns a PCollection of messages.

        Args:
            pcoll:
                An input PCollection. This is expected to be a `PBegin` when used with a real
                or mocked `ReadFromPubSub`, since Pub/Sub sources begin from the pipeline root.

        Returns:
            beam.PCollection:
                A PCollection of dictionaries where each dictionary contains:
                - "data": the decoded message string (if decoding is enabled),
                - "attributes": a dictionary of message attributes (if available).
        """
        logger.info("Generating file patterns...")
        file_patterns = self._generate_file_patterns()

        logger.info(f"Generated patterns: {file_patterns}")
        records = (
            pcoll
            | "CreatePatterns" >> beam.Create(file_patterns)
            | "MatchFiles" >> fileio.MatchAll()
            | "FilterFilesByTime" >> beam.Filter(lambda m: self.is_path_in_range(m.path))
            | "ReadAvroRecords" >> ReadAllFromAvro(**self._read_all_from_avro_kwargs)
        )

        if self._decode:
            records = records | beam.Map(self._decode_records)

        return records
