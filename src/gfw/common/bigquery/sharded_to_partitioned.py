"""Generic utility for migrating date-sharded BigQuery tables into a partitioned table."""

from __future__ import annotations

import logging

from collections.abc import Iterator
from dataclasses import dataclass
from functools import cached_property
from importlib.resources import files
from pathlib import Path
from typing import Callable

from google.api_core.exceptions import GoogleAPIError, NotFound
from google.cloud import bigquery
from jinja2 import Environment, FileSystemLoader
from rich.console import Console
from rich.logging import RichHandler
from rich.progress import (
    BarColumn,
    MofNCompleteColumn,
    Progress,
    TextColumn,
    TimeElapsedColumn,
)

import gfw.assets.queries.sharded_to_partitioned as _sql_pkg

from gfw.common.logging import LoggerConfig


logger = logging.getLogger(__name__)


def _progress_console() -> Console:
    h = next((h for h in logging.root.handlers if isinstance(h, RichHandler)), None)
    return h.console if h else Console()


_env = Environment(
    loader=FileSystemLoader(str(files(_sql_pkg))),
    trim_blocks=True,
    lstrip_blocks=True,
)

_DISCOVER_DATES = _env.get_template("discover_dates.sql")
_DISCOVER_COLUMNS = _env.get_template("discover_columns.sql")
_EXISTING_PARTITIONS = _env.get_template("existing_partitions.sql")
_DELETE_MONTH = _env.get_template("delete_month.sql")
_CONSOLIDATE = _env.get_template("consolidate.sql")

_PARTITION_TYPE_MAP = {
    "DAY": bigquery.TimePartitioningType.DAY,
    "HOUR": bigquery.TimePartitioningType.HOUR,
    "MONTH": bigquery.TimePartitioningType.MONTH,
    "YEAR": bigquery.TimePartitioningType.YEAR,
}


@dataclass(frozen=True)
class Table:
    """A BigQuery table reference with unpacking and string support."""

    project: str
    dataset_id: str
    table_id: str

    def __iter__(self) -> Iterator[str]:
        """Unpack as (project, dataset_id, table_id)."""
        return iter((self.project, self.dataset_id, self.table_id))

    def __str__(self) -> str:
        """Return the fully-qualified table name."""
        return self.fully_qualified

    @classmethod
    def from_fully_qualified(cls, fqn: str) -> Table:
        """Parse a fully-qualified table name (project.dataset.table)."""
        project, dataset_id, table_id = fqn.split(".")
        return cls(project=project, dataset_id=dataset_id, table_id=table_id)

    @property
    def fully_qualified(self) -> str:
        """Return the fully-qualified table name (project.dataset.table)."""
        return f"{self.project}.{self.dataset_id}.{self.table_id}"

    @property
    def dataset(self) -> str:
        """Return the fully-qualified dataset name (project.dataset)."""
        return f"{self.project}.{self.dataset_id}"


class DateTables(dict[str, list[Table]]):
    """A mapping of YYYYMMDD date strings to the source tables that have a shard for that date."""

    def group_by_month(self) -> dict[str, "DateTables"]:
        """Group dates by YYYYMM month prefix."""
        months = {}
        for date, tables in self.items():
            months.setdefault(date[:6], DateTables())[date] = tables

        return months


class ShardedToPartitioned:
    """Migrate one or more date-sharded BigQuery tables into a single partitioned table.

    This utility consolidates tables that follow the ``base_name_YYYYMMDD`` naming
    convention into a single time-partitioned destination table. It is designed for
    use cases where data from multiple providers or pipeline versions must be merged
    into one canonical table.

    **Multiple source tables**

    Any number of source tables can be supplied. For each calendar day the tool
    discovers which source tables have a shard for that date and issues a
    ``UNION ALL`` query that writes all of them into a single target partition.
    Days that appear in more than one source table (e.g. overlapping pipeline
    versions) are therefore merged, not deduplicated.

    **Schema mismatch handling**

    Each source table may have a different set of columns. The tool discovers
    the columns present in every source table and fills any gaps with
    ``CAST(NULL AS <type>)`` using the provided schema. This ensures every row
    in the target partition has the same structure regardless of which source
    table it came from.

    **Incremental operation**

    By default the tool queries the existing partitions in the target table and
    skips months that are already fully written. A month is considered pending if
    any of its days is missing from the target. Pass ``overwrite=True`` to
    re-process and replace already-written months.

    **Dry run**

    When ``dry_run=True`` the tool discovers available dates and columns, then
    logs an example ``UNION ALL`` query for the first available date.  All source
    tables are included regardless of whether they actually have a shard for that
    date, so the query shows the full structure. Columns missing from a given
    source table are filled with ``CAST(NULL AS <type>)`` as they would be in the
    real run. No data is written and the target table is not created or modified.

    Args:
        tables:
            Fully-qualified source table names (``project.dataset.table``).
            Each name is the base without the ``_YYYYMMDD`` suffix.

        target:
            Fully-qualified destination table name (``project.dataset.table``).

        execution_project:
            GCP project used to run BigQuery jobs and bear their costs.

        schema:
            Target schema, supplied as a path to a JSON schema file or a
            pre-loaded list of :class:`~google.cloud.bigquery.SchemaField` objects.
            Required — used to align columns across sources via ``CAST(NULL AS <type>)``
            for any column absent from a given source table.

        partition_type:
            Partitioning granularity — one of ``DAY``, ``HOUR``, ``MONTH``, or ``YEAR``.
            Defaults to ``DAY``.

        partition_field:
            Name of the TIMESTAMP or DATE column used for partitioning.
            Defaults to ``timestamp``.

        bq_client_factory:
            Optional factory callable that receives a project string and returns a
            :class:`~google.cloud.bigquery.Client`.
            Defaults to :class:`~google.cloud.bigquery.Client`.
            Useful for injecting mocked clients in tests.
    """

    def __init__(
        self,
        tables: list[str],
        target: str,
        execution_project: str,
        schema: str | Path | list[bigquery.SchemaField],
        partition_type: str = "DAY",
        partition_field: str = "timestamp",
        bq_client_factory: Callable[[str], bigquery.Client] = bigquery.Client,
    ) -> None:
        self._tables = tables
        self._execution_project = execution_project
        self._client_factory = bq_client_factory
        self._target = target
        self._schema = schema
        self._partition_type = partition_type
        self._partition_field = partition_field

    @cached_property
    def tables(self) -> list[Table]:
        """Source tables parsed from their fully-qualified names."""
        return [Table.from_fully_qualified(t) for t in self._tables]

    @cached_property
    def target(self) -> Table:
        """Target table parsed from its fully-qualified name."""
        return Table.from_fully_qualified(self._target)

    @cached_property
    def client(self) -> bigquery.Client:
        """BigQuery client for the execution project."""
        return self._client_factory(self._execution_project)

    @cached_property
    def schema(self) -> list[bigquery.SchemaField]:
        """Schema fields: loaded from file if a path is given, used as-is if already a list."""
        if isinstance(self._schema, list):
            return self._schema

        return self.client.schema_from_json(str(self._schema))

    @cached_property
    def partition_type(self) -> str:
        """Partition type resolved from string (DAY, HOUR, MONTH, YEAR)."""
        if self._partition_type not in _PARTITION_TYPE_MAP:
            raise ValueError(
                f"Invalid partition_type {self._partition_type!r}. Must be one of: {list(_PARTITION_TYPE_MAP)}"
            )
        return _PARTITION_TYPE_MAP[self._partition_type]

    def run(self, dry_run: bool = False, overwrite: bool = False, limit: int = 0) -> None:
        """Run the migration.

        Args:
            dry_run: Log an example consolidation query for the first pending month and exit.
            overwrite: Re-process months that already exist in the target table.
            limit: Process at most this many months.
        """
        if not logging.root.handlers:
            LoggerConfig().setup()

        logger.info(f"Target: {self.target}")

        logger.info("Discovering available dates…")
        date_tables = self._discover_dates(self.tables)
        months = date_tables.group_by_month()
        logger.info(f"Found {len(date_tables)} dates across {len(months)} months.")

        logger.info("Computing pending months...")
        pending = months
        if not overwrite:
            pending = self._compute_pending(months)

        skipped = len(months) - len(pending)
        if skipped:
            logger.info(f"Skipping {skipped} already-written months (use overwrite=True to redo).")

        logger.info(f"Pending months: {len(pending)}")
        if limit > 0:
            pending = dict(list(pending.items())[:limit])
            logger.info(f"Limit applied: processing {len(pending)} month(s).")

        if not pending:
            logger.info("Nothing to do. No pending months to process.")
            return

        logger.info("Discovering source columns…")
        table_columns = self._discover_columns(self.tables)

        if dry_run:
            date = next(iter(date_tables))
            logger.info(f"Example query for {self.target} (showing one shard across all tables):")
            logger.info(self._build_query(DateTables({date: self.tables}), table_columns))
            return

        self._ensure_table()

        with Progress(
            TextColumn("[bold blue]{task.description}"),
            BarColumn(),
            MofNCompleteColumn(),
            TimeElapsedColumn(),
            console=_progress_console(),
        ) as progress:
            task = progress.add_task("Migrating...", total=len(pending))
            for month, dates in pending.items():
                progress.update(task, description=f"Migrating {month}...")
                self._process_month(month, dates, table_columns, overwrite=overwrite)
                progress.advance(task)

        logger.info("Done.")

    def _discover_dates(self, tables: list[Table]) -> DateTables:
        date_tables = {}
        for table in tables:
            query = _DISCOVER_DATES.render(
                fq_dataset=table.dataset,
                table_id=table.table_id,
                substr_start=len(table.table_id) + 2,
            )
            logger.debug(f"Discovering dates for {table}:\n{query}")
            for row in self.client.query(query).result():
                date_tables.setdefault(row.date, []).append(table)

        return DateTables(sorted(date_tables.items()))

    def _compute_pending(self, months: dict[str, DateTables]) -> dict[str, DateTables]:
        project, dataset, table = self.target
        query = _EXISTING_PARTITIONS.render(project=project, dataset=dataset, table=table)
        logger.debug(f"Fetching existing partitions for {self.target}:\n{query}")
        try:
            existing = {row.partition_id for row in self.client.query(query).result()}
        except NotFound:
            logger.warning(f"Target table {self.target} not found, all months are pending.")
            existing = set()

        return {
            month: dates
            for month, dates in months.items()
            if not all(d in existing for d in dates)
        }

    def _discover_columns(self, tables: list[Table]) -> dict[str, frozenset[str]]:
        result = {}
        for table in tables:
            query = _DISCOVER_COLUMNS.render(fq_dataset=table.dataset, table_id=table.table_id)
            logger.debug(f"Discovering columns for {table}:\n{query}")
            result[table.fully_qualified] = frozenset(
                row.column_name for row in self.client.query(query).result()
            )
        return result

    def _ensure_table(self) -> None:
        bq_table = bigquery.Table(str(self.target), schema=self.schema)
        bq_table.time_partitioning = bigquery.TimePartitioning(
            type_=self.partition_type, field=self._partition_field
        )
        self.client.create_table(bq_table, exists_ok=True)
        logger.info("Target table ready.")

    def _process_month(
        self,
        month: str,
        dates: DateTables,
        table_columns: dict[str, frozenset[str]],
        overwrite: bool,
    ) -> None:
        query = self._build_query(dates, table_columns)
        logger.debug(f"Consolidation query for {self.target} ({month}):\n{query}")

        try:
            if overwrite:
                logger.info(f"Overwrite: deleting {month} from target...")
                self._delete_month(month)

            job_config = bigquery.QueryJobConfig(
                destination=str(self.target),
                write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
                time_partitioning=bigquery.TimePartitioning(
                    type_=self.partition_type,
                    field=self._partition_field,
                ),
                schema_update_options=[bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION],
            )
            job = self.client.query(query, job_config=job_config)
            job.result()
            logger.info(f"{month} — {job.total_bytes_processed / 1e9:.2f} GB processed")
        except GoogleAPIError as e:
            logger.error(f"{month} failed: {e}")

    def _build_query(
        self,
        dates: DateTables,
        table_columns: dict[str, frozenset[str]],
    ) -> str:
        target_cols = [(f.name, f.field_type) for f in self.schema]
        sources = [
            {
                "fqn": table.fully_qualified,
                "date": date,
                "cols": [
                    (
                        name
                        if name in table_columns[table.fully_qualified]
                        else f"CAST(NULL AS {ftype}) AS {name}"
                    )
                    for name, ftype in target_cols
                ],
            }
            for date, tables in sorted(dates.items())
            for table in tables
        ]
        return _CONSOLIDATE.render(sources=sources)

    def _delete_month(self, month: str) -> None:
        query = _DELETE_MONTH.render(
            target=self.target,
            year=month[:4],
            month=month[4:],
            partition_field=self._partition_field,
        )
        self.client.query(query).result()
