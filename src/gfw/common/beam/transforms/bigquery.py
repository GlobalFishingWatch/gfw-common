"""Wrapper around WriteToBigQuery with some extended functionality."""

import logging

from functools import cached_property
from typing import Any, Callable, List, Optional, Union

import apache_beam as beam

from apache_beam import PTransform
from apache_beam.io.gcp.bigquery import WriteToBigQuery
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.pvalue import PCollection
from apache_beam.utils.timestamp import Timestamp


logger = logging.getLogger(__name__)


class FakeWriteToBigQuery(WriteToBigQuery):
    """A fake WriteToBigQuery transform for testing purposes."""

    def __init__(self, **kwargs: Any) -> None:
        """Instantiates FakeWriteToBigQuery."""
        super().__init__(**kwargs)
        self.kwargs = kwargs

    def expand(self, pcoll: PCollection[Any]) -> PCollection[Any]:
        """Overrides the expand method to do nothing."""
        return pcoll


class WriteToBigQueryWrapper(PTransform[Any, Any]):
    """Wrapper around :class:`WriteToBigQuery` with extended functionality.

    Key Features:
    - Provides a simpler interface define a schema using a list of dictionaries.
    - Automatically selects writing method based on pipeline mode (streaming vs. batch) and runner.
    - Automatically converts TIMESTAMP fields to Timestamp objects when needed
        (streaming with ``STORAGE_WRITE_API``).

    Args:
        table:
            The BigQuery table to write to (in the format ``project:dataset.table``).

        schema:
            The schema for the BigQuery table.

        convert_timestamps:
            If True, converts ``TIMESTAMP`` fields to Timestamp objects when a streaming pipeline
            is using ``STORAGE_WRITE_API`` method, which requires Apache Beam Timestamp objects.
            See https://beam.apache.org/documentation/io/built-in/google-bigquery/.

        write_to_big_query_factory:
            A factory function used to create a :class:`beam.io.WriteToBigQuery` instance.
            This is primarily useful for testing, where you may want to inject a custom or fake
            implementation instead of using the real transform.
            If not provided, the default class will be used.

        **write_to_bigquery_kwargs:
            Any additional keyword arguments to be passed to
            :class:`beam.io.WriteToBigQuery` class.
            Check `official Apache Beam documentation
            <https://beam.apache.org/releases/pydoc/2.64.0/apache_beam.io.gcp.bigquery.html#apache_beam.io.gcp.bigquery.WriteToBigQuery>`_.

    Example:
        .. code-block:: python

            from pipe_nmea.common.beam.transforms import bigquery

            pcoll | "Write" >> bigquery.WriteToPartitionedBigQuery(
                table="project:dataset.table",
                schema=[{"name": "timestamp", "type": "TIMESTAMP", "mode": "REQUIRED"}, ...],
            )
    """

    def __init__(
        self,
        table: str,
        schema: Optional[list[dict[str, str]]] = None,
        label: Optional[str] = None,
        convert_timestamps: bool = False,
        write_to_bigquery_factory: Callable[..., WriteToBigQuery] = WriteToBigQuery,
        **write_to_bigquery_kwargs: Any,
    ) -> None:
        """Initializes the WriteToPartitionedBigQuery transform with the given parameters."""
        super().__init__(label=label)
        self._table = table
        self._schema = schema
        self._convert_timestamps = convert_timestamps
        self._write_to_bigquery_factory = write_to_bigquery_factory
        self._write_to_bigquery_kwargs = write_to_bigquery_kwargs

    @classmethod
    def get_client_factory(cls, mocked: bool = False) -> Callable:
        """Returns a factory for :class:`beam.WriteToBigQuery` objects."""
        if mocked:
            return FakeWriteToBigQuery

        return WriteToBigQuery

    @cached_property
    def schema(self) -> Union[dict[str, Any], None]:
        """Returns the BigQuery schema in the format expected by :class:`beam.WriteToBigQuery`.

        The provided schema as a list of dictionaries
        (e.g., ``[{"name": ..., "type": ..., ...}]``),
        is wrapped in a dictionary under the `"fields"` key.

        Returns:
            A dictionary of the form ``{"fields": [...]}``.
        """
        if self._schema is not None:
            return {"fields": self._schema}

        return self._schema

    @cached_property
    def timestamp_fields(self) -> List[str]:
        """Extract the field names of type TIMESTAMP from the schema."""
        return [f["name"] for f in self.schema.get("fields", []) if f.get("type") == "TIMESTAMP"]

    def expand(self, pcoll: PCollection[dict[str, Any]]) -> PCollection[dict[str, Any]]:
        """Writes the input PCollection to BigQuery, creating the table if it does not exist.

        Before applying the :class:`WriteToBigQuery` transform,
        this method ensures that the target table is created with the specified schema,
        partitioning, and clustering configurations.

        Args:
            pcoll:
                The input PCollection to write to BigQuery.

        Returns:
            An empty PCollection that acts as a signal for the completion of the write step.
            It can be used to chain additional transforms (e.g., logging or monitoring),
            but typically it contains no elements and exists primarily to signal that
            the write step has occurred within the pipeline.
        """
        write_to_bigquery_kwargs = dict(self._write_to_bigquery_kwargs)

        # Only resolve method if user hasn't provided it explicitly
        method = write_to_bigquery_kwargs.pop(
            "method", self.resolve_write_method(pcoll.pipeline.options.view_as(StandardOptions))
        )

        logger.info("BigQuery write method: {}".format(method))

        if method == WriteToBigQuery.Method.STORAGE_WRITE_API and self._convert_timestamps:
            pcoll = pcoll | "FloatToTimestamp" >> beam.Map(
                lambda x: self.float_to_beam_timestamp(x, self.timestamp_fields)
            )

        return pcoll | "WriteToBigQuery" >> self._write_to_bigquery_factory(
            table=self._table, schema=self.schema, method=method, **write_to_bigquery_kwargs
        )

    @staticmethod
    def resolve_write_method(standard_options: StandardOptions) -> str:
        """Resolves the appropriate write method to use to write to BigQuery.

        The selection logic is based on the StandardOptions of the pipeline
        in which :class:`beam.WriteToBigQuery` transform is used.

        The default behavior differs from the one in :class:`beam.WriteToBigQuery`,
        where ``STREAMING_INSERTS`` is used for streaming pipelines.
        Here, we prefer ``STORAGE_WRITE_API`` for streaming pipelines,
        which is Google's recommended method for high-throughput, low-latency streaming writes.

        As of Apache Beam 2.64, ``STORAGE_API_AT_LEAST_ONCE`` is not available in python,
        but ``STORAGE_WRITE_API`` can be used for at-least-once semantics.

        See https://cloud.google.com/dataflow/docs/guides/write-to-bigquery.

        Args:
            standard_options:
                The StandardOptions of the pipeline in which WriteToBigQuery transform is used.

        Returns:
            A string representing the selected write method.
            One of ``("STREAMING_INSERTS", "FILE_LOADS", "STORAGE_WRITE_API")``.
        """
        runner = (standard_options.runner or "").lower()

        if standard_options.streaming:
            if "direct" in runner:
                return WriteToBigQuery.Method.STREAMING_INSERTS

            return WriteToBigQuery.Method.STORAGE_WRITE_API

        return WriteToBigQuery.Method.FILE_LOADS

    @staticmethod
    def float_to_beam_timestamp(row: dict[str, Any], fields: list[str]) -> dict[str, Any]:
        """Converts in-place specified fields in a dictionary from float to Beam Timestamp objects.

        Args:
            row:
                A dictionary containing data with potential float values.

            fields:
                A tuple of field names to be converted to Timestamp.

        Returns:
            The input dictionary with specified fields converted to Timestamp objects.
        """
        for field in fields:
            row[field] = Timestamp(row[field])

        return row
