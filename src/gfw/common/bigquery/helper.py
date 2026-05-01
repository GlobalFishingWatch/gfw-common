"""Contains BigQueryHelper, a wrapper with extended functionality for bigquery.Client."""

import logging

from dataclasses import dataclass
from functools import cached_property
from pathlib import Path
from typing import Any, Callable, Dict, Iterator, List, Optional, Union
from unittest import mock

import sqlparse

from google.cloud import bigquery
from google.cloud.bigquery import WriteDisposition
from jinja2 import Environment, FileSystemLoader, StrictUndefined


logger = logging.getLogger(__name__)


@dataclass
class QueryResult:
    """Wrapper around :class:`bigquery.job.QueryJob` with access to results.

    This class encapsulates :attr:`query_job` and :attr:`row_iterator` instances,
    exposing rows via iteration and providing convenience methods like
    :meth:`iter_as_dicts` and :meth:`tolist`.

    Args:
        query_job:
            The original :class:`~bigquery.job.QueryJob`, which can be used to access
            job metadata such as session IDs, job statistics, and more.

        row_iterator:
            The :class:`~bigquery.table.RowIterator` returned by the query job.

    Example:
        .. code-block:: python

            result = bq_client.run_query("SELECT * FROM my_table")

            # Iterate raw rows
            for row in result:
                print(row)

            # Iterate as dicts
            for row in result.iter_as_dicts():
                print(row)

            # Materialize
            rows = result.tolist()
            rows_as_dicts = result.tolist(as_dicts=True)

            # Access job metadata
            print(result.query_job.job_id)
            print(result.session_id)
    """

    query_job: bigquery.job.QueryJob
    """The encapsulated :class:`~bigquery.job.QueryJob` instance."""

    row_iterator: bigquery.table.RowIterator
    """The :class:`~bigquery.table.RowIterator` returned by the query job."""

    def __len__(self) -> int:
        """Returns the total number of rows in the result."""
        return self.row_iterator.total_rows

    def __iter__(self) -> Iterator[bigquery.table.Row]:
        """Iterates over raw :class:`~bigquery.table.Row` objects."""
        return iter(self.row_iterator)

    def __next__(self) -> bigquery.table.Row:
        """Returns the next raw :class:`~bigquery.table.Row`."""
        return next(self.row_iterator)

    @property
    def session_id(self) -> Optional[str]:
        """Returns the ``session_id`` of the job, or ``None`` if not available."""
        session_info = self.query_job.session_info
        if session_info is not None:
            return session_info.session_id
        return None

    def iter_as_dicts(self) -> Iterator[Dict[str, Any]]:
        """Iterates over rows as dictionaries."""
        for row in self:
            yield dict(row.items())

    def tolist(self, as_dicts: bool = False) -> List[Union[bigquery.table.Row, Dict[str, Any]]]:
        """Materializes all rows into a list.

        Args:
            as_dicts:
                If True, rows are converted to dictionaries.
                Defaults to False.

        Returns:
            A list of :class:`~bigquery.table.Row` objects, or dictionaries if ``as_dicts=True``.
        """
        if as_dicts:
            return list(self.iter_as_dicts())

        return list(self)


class BigQueryHelper:
    """Wrapper around :class:`bigquery.Client` with extended functionality.

    Args:
        client_factory:
            A callable to create bigquery client objects.
            Defaults to the canonical :class:`bigquery.Client` factory.

        dry_run:
            If True, queries jobs will be run in dry run mode.
            For more information, check `bigquery documentation
            <https://cloud.google.com/bigquery/docs/reference/rest/v2/Job#JobConfiguration.FIELDS.dry_run>`_.

        **kwargs:
            Extra keyword arguments to be passed to the provided ``client_factory``.
    """

    def __init__(
        self,
        client_factory: Callable[..., bigquery.client.Client] = bigquery.client.Client,
        dry_run: bool = False,
        **kwargs: Any,
    ) -> None:
        """Initializes BigQueryHelper instance."""
        self._client_factory = client_factory
        self._dry_run = dry_run
        self._kwargs = kwargs

    @classmethod
    def mocked(cls, **kwargs: Any) -> "BigQueryHelper":
        """Returns a :class:`BigQueryHelper` instance with a mocked client."""
        return cls(client_factory=cls.get_client_factory(mocked=True), **kwargs)

    @classmethod
    def get_client_factory(cls, mocked: bool = False) -> Callable[..., bigquery.Client]:
        """Returns a factory for :class:`bigquery.Client` objects."""

        def bigquery_client_mock_factory(
            project: Optional[str] = None, **kwargs: Any
        ) -> bigquery.Client:
            client = mock.create_autospec(
                bigquery.Client, project=project, instance=True, **kwargs
            )
            return client

        if mocked:
            return bigquery_client_mock_factory

        return bigquery.client.Client

    @cached_property
    def client(self) -> bigquery.client.Client:
        """Returns the instance of :class:`bigquery.Client` to be used."""
        query_job = bigquery.job.QueryJobConfig()
        query_job.dry_run = self._dry_run

        if query_job.dry_run:
            logger.warning("*** Running Query Jobs as DRY RUN ***.")

        return self._client_factory(default_query_job_config=query_job, **self._kwargs)

    def end_session(self, session_id: str) -> None:
        """Terminates session with given ``session_id``."""
        query_job = self.client.query(f"CALL BQ.ABORT_SESSION('{session_id}')")
        _ = query_job.result()

    def create_table(
        self,
        table: str,
        description: str = "",
        schema: Optional[List[Dict[str, str]]] = None,
        partition_field: Optional[str] = None,
        partition_type: str = bigquery.table.TimePartitioningType.DAY,
        clustering_fields: Optional[list[str]] = None,
        labels: Optional[dict[str, str]] = None,
        **kwargs: Any,
    ) -> bigquery.table.Table:
        """Creates a BigQuery table.

        Args:
            table:
                Table name like ``dataset.table``.

            description:
                Text to include in the table's description field.

            schema:
                Schema of the table.

            partition_field:
                Name of field to use for time partitioning.

            partition_type:
                The type of partitioning to use (e.g., ``DAY``, ``HOUR``).
                Defaults to ``DAY``.

            clustering_fields:
                A list of fields to use for clustering the BigQuery table (optional).

            labels:
                Dictionary of labels to audit costs.

            **kwargs:
                Extra keyword arguments to be passed to the :meth:`client.create_table` method.

        Returns:
            The created table.
        """
        table_ref = self._create_table_reference(table)

        bq_table = bigquery.table.Table(table_ref, schema=schema)
        bq_table.labels = labels or {}
        bq_table.description = description

        if partition_field is not None:
            bq_table.time_partitioning = bigquery.table.TimePartitioning(
                type_=partition_type, field=partition_field
            )

        if clustering_fields is not None:
            bq_table.clustering_fields = clustering_fields

        return self.client.create_table(bq_table, **kwargs)

    def create_view(self, view_id: str, view_query: str) -> None:
        """Creates or replaces a BigQuery view.

            This method is declarative: the provided query becomes the
            source of truth for the view definition. If the view already
            exists, it is replaced. If it does not exist, it is created.

        Args:
            view_id:
                The destination view, e.g. ``project.dataset.view_id``.

            view_query:
                The SELECT query that defines the view.
        """
        view_query = f"""
        CREATE OR REPLACE VIEW `{view_id}` AS
        {view_query}
        """
        self.client.query(view_query).result()

    def run_query(
        self,
        query_str: str,
        destination: Optional[str] = None,
        write_disposition: str = WriteDisposition.WRITE_APPEND,
        clustering_fields: Optional[list[str]] = None,
        session_id: Optional[str] = None,
        labels: Optional[dict[str, Any]] = None,
        **kwargs: Any,
    ) -> QueryResult:
        """Runs a query.

        Args:
            query_str:
                The query to run.

            destination:
                The table in which to write the outputs of the query.

            write_disposition:
                The write disposition.

            clustering_fields:
                List of field names to use for clustering.

            session_id:
                The session_id to use for the query.

            labels:
                Labels to apply.

            **kwargs:
                Extra keyword arguments to be passed to :class:`job.QueryJobConfig` constructor.

        Returns:
            An instance wrapping the BigQuery QueryJob, providing convenient access
            to the query results and metadata.
        """
        connection_properties = []

        if session_id is not None:
            connection_properties.append(
                bigquery.query.ConnectionProperty(key="session_id", value=session_id)
            )

        job_config = bigquery.job.QueryJobConfig(
            connection_properties=connection_properties,
            labels=labels or {},
            priority=bigquery.enums.QueryPriority.INTERACTIVE,
            **kwargs,
        )

        if destination is not None:
            job_config.clustering_fields = clustering_fields
            job_config.destination = self._create_table_reference(destination)
            job_config.priority = bigquery.enums.QueryPriority.BATCH
            job_config.write_disposition = write_disposition

        logger.debug("Query to execute:")
        logger.debug(sqlparse.format(query_str, reindent=True, keyword_case="upper"))

        query_job = self.client.query(query_str, job_config=job_config)
        logger.debug(f"BigQuery QueryJob id: {query_job.job_id}.")

        # Block until the job completes so errors surface immediately rather than lazily.
        row_iterator = query_job.result()

        return QueryResult(query_job, row_iterator)

    def load_from_json(
        self,
        rows: list[dict[str, Any]],
        destination: str,
        partition_field: Optional[str] = None,
        partition_type: str = bigquery.table.TimePartitioningType.DAY,
        **kwargs: Any,
    ) -> None:
        """Loads an iterable of json rows into BigQuery table.

        Args:
            rows:
                The iterable of JSON dictionaries containing data to be loaded.

            destination:
                The table in which to write the data.

            partition_field:
                The field to use for partitioning the BigQuery table (optional).

            partition_type:
                The type of partitioning to use (e.g., ``DAY``, ``HOUR``).
                Defaults to ``DAY``.

            **kwargs:
                Extra keyword arguments to be passed to the :class:`job.LoadJobConfig` constructor.
        """
        job_config = bigquery.job.LoadJobConfig(**kwargs)

        if partition_field is not None:
            job_config.time_partitioning = bigquery.table.TimePartitioning(
                type_=partition_type, field=partition_field
            )

        self.client.load_table_from_json(
            json_rows=rows, destination=destination, job_config=job_config
        )

    def _create_table_reference(self, table_name: str) -> bigquery.table.TableReference:
        return bigquery.table.TableReference.from_string(
            table_name, default_project=self.client.project
        )

    @staticmethod
    def format_jinja2(
        template_path: Path, search_path: Union[list[Path], Path] = Path("./"), **kwargs: Any
    ) -> str:
        """Render a Jinja2 template with the given keyword arguments.

        Args:
            template_path:
                The path to the Jinja2 template.

            search_path:
                The base directory in which to search for the template path.
                Can be a list of paths.

            **kwargs:
                Parameters required to render the query.
                It may contain extra parameters which are not used by the template,
                but all required parameters must be provided.

        Returns:
            The rendered query.
        """
        jinja2_env = Environment(loader=FileSystemLoader(search_path), undefined=StrictUndefined)
        sql_template = jinja2_env.get_template(str(template_path))
        query = sql_template.render(kwargs)

        return query
