"""Module containing an Apache Beam transform for writing data to a partitioned BigQuery table."""

import logging

from functools import cached_property
from typing import Any, Callable, List, Optional, Union

import apache_beam as beam

from apache_beam import PTransform
from apache_beam.io.gcp.bigquery import WriteToBigQuery
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.pvalue import PCollection

from gfw.common.beam.utils import float_to_beam_timestamp
from gfw.common.bigquery.helper import BigQueryHelper


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


class WriteToPartitionedBigQuery(PTransform[Any, Any]):
    """A custom Apache Beam transform that writes to a partitioned BigQuery table.

    This transform abstracts the complexity of BigQuery's partitioning and clustering options,
    as well as other additional options.

    Key Features:
    - Provides a simpler interface to:
        - Partition the table based on a specific field (e.g., 'timestamp') and type (e.g. 'DAY').
        - Cluster the table based on specified fields for performance optimization.
        - Add a description for the table's metadata.
        - Define a schema using a list of dictionaries.
    - Automatically selects writing method based on pipeline mode (streaming vs. batch) and runner.

    Args:
        table:
            The BigQuery table to write to (in the format `project:dataset.table`).

        project (str): The ID of the project containing this table or :data:`None`
            if the table reference is specified entirely by the table argument.

        description:
            The description to use in the metadata of the BigQuery table.

        schema:
            The schema for the BigQuery table.

        partition_field:
            The field to use for partitioning the BigQuery table (optional).

        partition_type:
            The type of partitioning to use (e.g., "DAY", "HOUR").
            Defaults to "DAY".

        clustering_fields:
            A list of fields to use for clustering the BigQuery table (optional).

        bigquery_helper_factory:
            Factory for creating `BigQueryHelper` to handle table creation. While `WriteToBigQuery`
            can create tables, it has limitations with `STORAGE_WRITE_API` and `STREAMING_INSERTS`:
            - `STORAGE_WRITE_API` ignores time partitioning and fails to set descriptions.
            - `STREAMING_INSERTS` sets time partitioning but fails to set descriptions.

        write_to_big_query_factory:
            A factory function used to create a `WriteToBigQuery` instance.
            This is primarily useful for testing, where you may want to inject a custom or fake
            implementation instead of using the real `WriteToBigQuery` transform.
            If not provided, the default `WriteToBigQuery` class will be used.

        **write_to_bigquery_kwargs:
            Any additional keyword arguments to be passed directly to the `WriteToBigQuery` class.
            Check official documentation:
            https://beam.apache.org/releases/pydoc/2.64.0/apache_beam.io.gcp.bigquery.html#apache_beam.io.gcp.bigquery.WriteToBigQuery.

    Example:
            >>> from pipe_nmea.common.beam.transforms import bigquery
            >>> pcoll | "Write" >> bigquery.WriteToPartitionedBigQuery(
            ...     table="project:dataset.table",
            ...     description="My table",
            ...     schema=[{"name": "timestamp", "type": "TIMESTAMP", "mode": "REQUIRED"}, ...],
            ...     partition_field="timestamp",
            ...     partition_type="DAY",
            ...     clustering_fields=["field1", "field2"],
            ... )
    """

    def __init__(
        self,
        table: str,
        project: Optional[str] = None,
        description: str = "",
        schema: Optional[list[dict[str, str]]] = None,
        partition_field: Optional[str] = None,
        partition_type: str = "DAY",
        clustering_fields: Optional[List[str]] = None,
        label: Optional[str] = None,
        bigquery_helper_factory: Callable[..., BigQueryHelper] = BigQueryHelper,
        write_to_bigquery_factory: Callable[..., WriteToBigQuery] = WriteToBigQuery,
        **write_to_bigquery_kwargs: Any,
    ) -> None:
        """Initializes the WriteToPartitionedBigQuery transform with the given parameters."""
        super().__init__(label=label)
        self._table = table
        self._project = project
        self._schema = schema
        self._description = description
        self._partition_field = partition_field
        self._partition_type = partition_type
        self._clustering_fields = clustering_fields or []
        self._bigquery_helper_factory = bigquery_helper_factory
        self._write_to_bigquery_factory = write_to_bigquery_factory
        self._write_to_bigquery_kwargs = write_to_bigquery_kwargs

    @classmethod
    def get_client_factory(cls, mocked: bool = False) -> Callable:
        """Returns a factory for bigquery.Client objects."""
        if mocked:
            return FakeWriteToBigQuery

        return WriteToBigQuery

    @cached_property
    def schema(self) -> Union[dict[str, Any], None]:
        """Returns the BigQuery schema in the format expected by `WriteToBigQuery`.

        The provided schema as a list of dictionaries (e.g., [{"name": ..., "type": ..., ...}]),
        is wrapped in a dictionary under the `"fields"` key.

        Returns:
            A dictionary of the form `{"fields": [...]}`.
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

        Before applying the `WriteToBigQuery` transform, this method ensures that the target table
        is created with the specified schema, partitioning, and clustering configurations.

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

        logger.debug("BigQuery write method: {}".format(method))

        logger.debug("Creating table (if doesn't exist): {}".format(self._table))
        bigquery_helper = self._bigquery_helper_factory(project=self._project)
        bigquery_helper.create_table(
            table=self._table,
            description=self._description,
            schema=self._schema,
            partition_type=self._partition_type,
            partition_field=self._partition_field,
            clustering_fields=self._clustering_fields,
            exists_ok=True,
        )

        if method == WriteToBigQuery.Method.STORAGE_WRITE_API:
            # 'STORAGE_WRITE_API' requires Apache Beam Timestamp objects.
            # See https://beam.apache.org/documentation/io/built-in/google-bigquery/
            pcoll = pcoll | "Float to Timestamp" >> beam.Map(
                lambda x: float_to_beam_timestamp(x, self.timestamp_fields)
            )

        return pcoll | "Write to BigQuery" >> self._write_to_bigquery_factory(
            table=self._table, schema=self.schema, method=method, **write_to_bigquery_kwargs
        )

    @staticmethod
    def resolve_write_method(standard_options: StandardOptions) -> str:
        """Resolves the appropriate write method to use to write to BigQuery.

        The selection logic is based on the StandardOptions of the pipeline
        in which WriteToBigQuery transform is used.

        The default behavior differs from the one in WriteToBigQuery,
        where 'STREAMING_INSERTS' is used for streaming pipelines.
        Here, we prefer 'STORAGE_WRITE_API' for streaming pipelines,
        which is Google's recommended method for high-throughput, low-latency streaming writes.

        As of Apache Beam 2.64, 'STORAGE_API_AT_LEAST_ONCE' is not available in python,
        but 'STORAGE_WRITE_API' can be used for at-least-once semantics.

        See https://cloud.google.com/dataflow/docs/guides/write-to-bigquery.

        Args:
            standard_options:
                The StandardOptions of the pipeline in which WriteToBigQuery transform is used.

        Returns:
            A string representing the selected write method.
            One of ("STREAMING_INSERTS", "FILE_LOADS", "STORAGE_WRITE_API").
        """
        runner = (standard_options.runner or "").lower()

        if standard_options.streaming:
            if "direct" in runner:
                return WriteToBigQuery.Method.STREAMING_INSERTS

            return WriteToBigQuery.Method.STORAGE_WRITE_API

        return WriteToBigQuery.Method.FILE_LOADS
