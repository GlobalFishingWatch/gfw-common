"""Module containing an Apache Beam transform for reading Avro files with datetime filtering."""

import codecs
import logging

from datetime import timedelta
from typing import Any, Optional, Sequence

import apache_beam as beam

from apache_beam.io import fileio
from apache_beam.io.avroio import ReadAllFromAvro
from apache_beam.pvalue import PCollection

from gfw.common.datetime import datetime_from_string, get_datetime_from_string


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
        base_path:
            The base path to the Avro files. This can be a local directory
            (e.g., '/path/to/data'), a GCS bucket (e.g., 'gs://my-bucket/'),
            or any other Beam-supported filesystem path. The transform will
            append a date-based wildcard pattern to this path.

        start_dt:
            The start datetime of the range, in ISO format (e.g., 'YYYY-MM-DDTHH:MM:SS').

        end_dt:
            The end datetime of the range, in ISO format (e.g., 'YYYY-MM-DDTHH:MM:SS').
            Datetimes equal to this value are considered outside the range.

        path_template:
            Path template pointing to the folder containing the avro files.
            Since it is assumed that the data is stored in date-partitioned folders,
            it must include a 'date' placeholder (e.g., "nmea-{date}").

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

    DATETIME_REGEX = r"(\d{4}-\d{2}-\d{2}).*?(\d{2}_\d{2}_\d{2}Z)"
    PATTERN_TEMPLATE = "{base_path}/{folder_path}/*.avro"

    def __init__(
        self,
        base_path: str,
        start_dt: str,
        end_dt: str,
        path_template: str = "{date}",
        decode: bool = True,
        decode_method: str = "utf-8",
        read_all_from_avro_kwargs: Optional[dict[Any, Any]] = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self._base_path = base_path
        self._start_dt = datetime_from_string(start_dt)
        self._end_dt = datetime_from_string(end_dt)
        self._path_template = path_template
        self._decode = decode
        self._decode_method = decode_method
        self._read_all_from_avro_kwargs = read_all_from_avro_kwargs or {}

        self._validate_decode_method()

    def _generate_file_patterns(self) -> Sequence[str]:
        current_date = self._start_dt.date()
        end_date = self._end_dt.date()
        patterns = []

        while current_date <= end_date:
            patterns.append(
                self.PATTERN_TEMPLATE.format(
                    base_path=self._base_path,
                    folder_path=self._path_template.format(date=current_date.isoformat()),
                )
            )
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
        dt = get_datetime_from_string(path)
        if dt is None:
            logger.error(
                f"Couldn't extract datetime from path: {path} using regex: {self.DATETIME_REGEX}"
            )

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
