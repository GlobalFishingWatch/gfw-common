"""Abstract base class for BigQuery table configuration.

Defines the TableConfig dataclass with common BigQuery table parameters,
including schema, partitioning, clustering, and optional description support.

Subclasses must implement the schema property.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import date
from functools import cached_property
from typing import Any, Optional, Tuple

from .table_description import TableDescription


@dataclass
class TableConfig(ABC):
    """Abstract base class for BigQuery table configuration."""

    table_id: str
    """Fully qualified BigQuery table ID."""

    schema_file: str
    """Path to the file defining the schema."""

    description: Optional[TableDescription] = None
    """Optional :class:`~gfw.common.bigquery.TableDescription` instance for the table metadata."""

    partition_type: str = "DAY"
    """Type of partitioning to apply (e.g., ``DAY``, ``MONTH``)."""

    partition_field: Optional[str] = None
    """Field used for partitioning (optional)."""

    clustering_fields: Optional[Tuple[str, ...]] = None
    """Optional tuple of fields for clustering."""

    @abstractmethod
    @cached_property
    def schema(self) -> list[dict[str, str]]:
        """Returns the schema of the table."""

    def to_bigquery_params(self, include_description: bool = True) -> dict[str, Any]:
        """Returns parameters for BigQuery table creation or write operations.

        This dictionary is intended to be unpacked as keyword arguments into
        :meth:`BigQueryHelper.create_table <gfw.common.bigquery.BigQueryHelper.create_table>`.

        Args:
            include_description:
                Whether to include the formatted description string.

        Returns:
            A dictionary of parameters suitable for BigQuery operations.
        """
        bigquery_params = {
            "table": self.table_id,
            "schema": self.schema,
            "partition_type": self.partition_type,
            "partition_field": self.partition_field,
            "clustering_fields": self.clustering_fields,
        }

        if include_description and self.description is not None:
            bigquery_params["description"] = self.description.render()

        return bigquery_params

    def delete_query(self, start_date: date, end_date: Optional[date] = None) -> str:
        """Returns the query to perform when deleting records from this table."""
        raise NotImplementedError


@dataclass
class ViewConfig(ABC):
    """Abstract base class for a BigQuery view built on top of a :class:`TableConfig`'s table.

    A table may have zero, one, or several views derived from it -- each one is its own
    :class:`ViewConfig` instance, pointing back at the same :attr:`source` table. This is
    deliberately a separate class from :class:`TableConfig` rather than a ``view_*`` field
    bolted onto it, so a table's own metadata isn't conflated with any particular view's.
    """

    source: TableConfig
    """The :class:`TableConfig` whose table this view is built on top of."""

    suffix: str = "view"
    """Suffix appended to the source table's ID to build this view's ID."""

    description: Optional[TableDescription] = None
    """Optional :class:`~gfw.common.bigquery.TableDescription` instance for the view's metadata."""

    @cached_property
    def view_id(self) -> str:
        """Returns the ID of this view."""
        return f"{self.source.table_id}_{self.suffix}"

    @property
    def schema(self) -> list[dict[str, str]]:
        """Returns the schema to attach to the view.

        Defaults to the source table's schema, which is correct whenever the view's query
        selects the same columns as the source table (the common case for a "last version of
        each row" view) -- the view's columns then carry the exact same descriptions as the
        table's, with nothing duplicated by hand. Override this when the view's output columns
        differ from the source table's.
        """
        return self.source.schema

    @abstractmethod
    def view_query(self) -> str:
        """Returns the query to perform to create this view."""
