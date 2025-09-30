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

    view_suffix: Optional[str] = "view"
    """Suffix to use when constructing the view ID."""

    @cached_property
    def view_id(self) -> str:
        """Returns the ID of the view for the table."""
        return f"{self.table_id}_{self.view_suffix}"

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

    def view_query(self) -> str:
        """Returns the query to perform to create a view for this table."""
        raise NotImplementedError

    def delete_query(self, start_date: date) -> str:
        """Returns the query to perform when deleting records from this table."""
        raise NotImplementedError
