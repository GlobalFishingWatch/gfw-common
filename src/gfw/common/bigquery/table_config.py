"""Abstract base class for BigQuery table configuration.

Defines the TableConfig dataclass with common BigQuery table parameters,
including schema, partitioning, clustering, and optional description support.

The schema is read from :attr:`TableConfig.schema_file`, in the package of the subclass.
Subclasses override :attr:`TableConfig.schema` to adjust it.
"""

from abc import ABC
from dataclasses import dataclass
from datetime import date
from functools import cached_property
from typing import Any, Optional, Tuple

from gfw.common.resources import SCHEMAS_FOLDER, load_schema, top_level_package

from .table_description import TableDescription


@dataclass
class TableConfig(ABC):
    """Abstract base class for BigQuery table configuration."""

    SCHEMAS_FOLDER = SCHEMAS_FOLDER
    """Folder of :attr:`schema_file`, relative to the top-level package of the subclass."""

    table_id: str
    """Fully qualified BigQuery table ID."""

    schema_file: str
    """Name of the JSON file defining the schema, in :attr:`SCHEMAS_FOLDER`."""

    description: Optional[TableDescription] = None
    """Optional :class:`~gfw.common.bigquery.TableDescription` instance for the table metadata."""

    partition_type: str = "DAY"
    """Type of partitioning to apply (e.g., ``DAY``, ``MONTH``)."""

    partition_field: Optional[str] = None
    """Field used for partitioning (optional)."""

    clustering_fields: Optional[Tuple[str, ...]] = None
    """Optional tuple of fields for clustering."""

    @cached_property
    def schema(self) -> list[dict[str, str]]:
        """Returns the schema of the table.

        Reads :attr:`schema_file` from :attr:`SCHEMAS_FOLDER`, in the top-level package of the
        subclass (e.g. ``pipe_gaps/assets/schemas``). Override this to adjust the schema,
        starting from ``super().schema``.
        """
        return load_schema(top_level_package(type(self)), self.schema_file, self.SCHEMAS_FOLDER)

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
