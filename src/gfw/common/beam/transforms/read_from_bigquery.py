"""Module with reusable PTransforms for reading input PCollections."""

from __future__ import annotations

from typing import Any, Callable, Optional, Sequence

import apache_beam as beam

from apache_beam import io
from apache_beam.pvalue import PCollection

from gfw.common.query import Query


class FakeReadFromBigQuery(io.ReadFromBigQuery):
    """Mocks beam.io.ReadFromBigQuery.

    Args:
        elements:
            Elements to use as output Pcollection.
    """

    def __init__(self, elements: Sequence[dict] = (), **kwargs: Any) -> None:
        self._elements = elements

    def expand(self, pcoll: PCollection) -> PCollection:
        """Returns injected elements in the constructor."""
        return pcoll | beam.Create(self._elements)


class ReadFromBigQuery(beam.PTransform):
    """Beam transform to read Pcollection from BigQuery table.

    Args:
        query:
            The query to execute.

        output_type:
            The Beam type hint for the output (e.g., a NamedTuple).
            If not provided, defaults to dict.

        method:
            The method to use to read from BigQuery. It may be EXPORT or DIRECT_READ.

        use_standard_sql:
            Specifies whether to use BigQuery's standard SQL dialect for this query.
            Defaults to True.

        read_from_bigquery_factory:
            A factory function used to create a `ReadFromBigQuery` instance.
            This is primarily useful for testing, where you may want to inject a custom or fake
            implementation instead of using the real `ReadFromBigQuery` transform.
            If not provided, the default `ReadFromBigQuery` class will be used.

        write_to_bigquery_kwargs:
            Any additional keyword arguments to be passed to Beam's `ReadFromBigQuery` class.
            Check official documentation:
            https://beam.apache.org/releases/pydoc/2.64.0/apache_beam.io.gcp.bigquery.html#apache_beam.io.gcp.bigquery.ReadFromBigQuery.

        **kwargs:
            Additional keyword arguments passed to base PTransform class.
    """

    def __init__(
        self,
        query: str,
        output_type: type = dict,
        method: str = beam.io.ReadFromBigQuery.Method.EXPORT,
        use_standard_sql: bool = True,
        read_from_bigquery_factory: Callable[..., io.ReadFromBigQuery] = io.ReadFromBigQuery,
        read_from_bigquery_kwargs: Optional[dict[Any, Any]] = None,
        **kwargs: Any,
    ) -> None:
        """Initializes a ReadFromBigQuery instance."""
        super().__init__(**kwargs)
        self._query = query
        self._output_type = output_type
        self._method = method
        self._use_standard_sql = use_standard_sql
        self._read_from_bigquery_factory = read_from_bigquery_factory
        self._read_from_bigquery_kwargs = read_from_bigquery_kwargs or {}

    @classmethod
    def get_client_factory(cls, mocked: bool = False) -> Callable:
        """Returns a factory for ReadFromPubSub objects."""
        if mocked:
            return FakeReadFromBigQuery

        return io.ReadFromBigQuery

    @classmethod
    def from_query(cls, query: Query, use_type: bool = False, **kwargs: Any) -> ReadFromBigQuery:
        """Creates a ReadFromBigQuery PTransform from a Query object.

        Args:
            query:
                An instance of a Query subclass.
                Its `render()` method is used to produce the SQL query string.

            use_type:
                If True, sets `output_type` to the query's NamedTuple type.

            **kwargs:
                Any additional arguments for ReadFromBigQuery constructor.

        Returns:
            A configured ReadFromBigQuery instance ready to use in a Beam pipeline.
        """
        rendered_query = query.render(formatted=False)

        output_type: type = dict
        if use_type:
            output_type = query.output_type

        return cls(query=rendered_query, output_type=output_type, **kwargs)

    def expand(self, pcoll: PCollection) -> PCollection[Any]:
        """Applies PCollection to read from BigQuery."""
        output = pcoll | self._read_from_bigquery_factory(
            use_standard_sql=self._use_standard_sql,
            query=self._query,
            method=self._method,
            **self._read_from_bigquery_kwargs,
        ).with_output_types(dict)

        if self._output_type not in (None, dict):
            output = output | beam.Map(lambda d: self._output_type(**d)).with_output_types(
                self._output_type
            )

        return output
