"""Abstract base classes for BigQuery view configuration.

Defines the ViewConfig dataclass, an abstract representation of a BigQuery view,
along with SingleSourceViewConfig, a common specialization for views built on
top of a single :class:`~gfw.common.bigquery.TableConfig`'s table.
"""

from abc import ABC, abstractmethod
from dataclasses import InitVar, dataclass
from typing import Any, Optional

from .table_config import TableConfig
from .table_description import TableDescription


@dataclass(kw_only=True)
class ViewConfig(ABC):
    """Abstract base class for a BigQuery view.

    Deliberately makes no assumption about how many tables the view's query reads
    from -- a view backed by a single table (the common case) should subclass
    :class:`SingleSourceViewConfig` instead of this class directly.
    """

    view_id: str
    """The ID of this view."""

    description: Optional[TableDescription] = None
    """Optional :class:`~gfw.common.bigquery.TableDescription` instance for the view's metadata."""

    @property
    @abstractmethod
    def schema(self) -> list[dict[str, str]]:
        """Returns the schema to attach to the view."""

    @abstractmethod
    def view_query(self) -> str:
        """Returns the query to perform to create this view."""

    def as_create_view_params(self) -> dict[str, Any]:
        """Returns this view reshaped as parameters for BigQuery view creation.

        This dictionary is intended to be unpacked as keyword arguments into
        :meth:`BigQueryHelper.create_view <gfw.common.bigquery.BigQueryHelper.create_view>`.

        Returns:
            A dictionary of parameters suitable for :meth:`BigQueryHelper.create_view`.
        """
        return {
            "view_id": self.view_id,
            "view_query": self.view_query(),
            "description": self.description.render() if self.description else "",
            "schema": self.schema,
        }


@dataclass(kw_only=True)
class SingleSourceViewConfig(ViewConfig):
    """A :class:`ViewConfig` whose query reads from a single source table.

    A table may have zero, one, or several such views derived from it -- each one is
    its own :class:`SingleSourceViewConfig` instance, pointing back at the same
    :attr:`source` table. This is deliberately a separate class from :class:`TableConfig`
    rather than a ``view_*`` field bolted onto it, so a table's own metadata isn't
    conflated with any particular view's.
    """

    source: TableConfig
    """The :class:`TableConfig` whose table this view is built on top of."""

    suffix: str = "view"
    """Suffix appended to the source table's ID to build this view's ID, unless ``view_id``
    is passed explicitly."""

    view_id: InitVar[Optional[str]] = None
    """Explicit ID for this view. When omitted, defaults to the source table's ID plus
    :attr:`suffix` -- pass this to give the view a name unrelated to its source table's,
    e.g. when the view is meant to take over the source table's old name."""

    def __post_init__(self, view_id: Optional[str]) -> None:
        """Resolves :attr:`view_id` from the explicit value, or the source table ID and suffix."""
        self.view_id = view_id or f"{self.source.table_id}_{self.suffix}"

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
