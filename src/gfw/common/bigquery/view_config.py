"""Abstract base class for BigQuery view configuration.

Defines the ViewConfig dataclass, representing a BigQuery view built on top of
a :class:`~gfw.common.bigquery.TableConfig`'s table.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from functools import cached_property
from typing import Optional

from .table_config import TableConfig
from .table_description import TableDescription


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
