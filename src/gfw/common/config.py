"""Defines the configuration classes of data pipeline executions.

It includes:
- A dataclass `PipelineConfig` with the settings every pipeline has: its labels, the range of
  dates it processes and any unknown arguments.
- A custom exception `PipelineConfigError` for handling invalid configuration inputs.

Intended for use in CLI-based or programmatic pipeline setups.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import date, datetime
from functools import cached_property
from types import SimpleNamespace
from typing import Any, Callable, Sequence

from jinja2 import Environment

from gfw.common.datetime import DateRange
from gfw.common.jinja2 import EnvironmentLoader


ERROR_DATE = "{} must be a date in ISO format (YYYY-MM-DD). Got: {!r}."
ERROR_LABELS = "labels must not be empty: every pipeline labels its jobs to audit costs."
ERROR_NO_DATE_RANGE = "start_date and end_date must be set to get a date_range."


class PipelineConfigError(Exception):
    """Custom exception for pipeline configuration errors."""

    pass


@dataclass(frozen=True, kw_only=True)
class PipelineConfig:
    """Configuration object for data pipeline execution.

    The processed range of dates goes from :attr:`start_date` (inclusive) to :attr:`end_date`
    (exclusive). Both accept :class:`~datetime.date` objects or ISO format strings
    (``YYYY-MM-DD``), since they can come from the command line, a config file or code, and are
    stored as :class:`~datetime.date` objects.

    A pipeline that processes no dates can make them optional by redeclaring them with a default,
    e.g. ``start_date: date | None = None``.

    Note:
        This class is completely generic and independent of any specific pipeline framework.

    Raises:
        :class:`PipelineConfigError`:
            If ``labels`` is empty, a date is not a valid ISO date, or :attr:`end_date` is not
            after :attr:`start_date`.
    """

    labels: dict[str, str]
    """Labels to apply to the pipeline's Dataflow job and any BigQuery jobs it runs."""

    start_date: date
    """First date to process (inclusive)."""

    end_date: date
    """Date the processing ends at (exclusive)."""

    name: str = ""
    """Name of the pipeline."""

    version: str = "0.1.0"
    """Version of the pipeline."""

    jinja_folder: str = "assets/queries"
    """The folder that contains the jinja2 templates."""

    mock_bq_clients: bool = False
    """If True, all BigQuery interactions will be mocked."""

    unknown_parsed_args: dict[str, Any] = field(default_factory=dict)
    """Parsed CLI or config arguments not explicitly defined in self."""

    unknown_unparsed_args: tuple[str, ...] = ()
    """Raw unparsed CLI arguments."""

    def __post_init__(self) -> None:
        """Validates the configuration, and parses the dates."""
        if not self.labels:
            raise PipelineConfigError(ERROR_LABELS)

        for name in ("start_date", "end_date"):
            object.__setattr__(self, name, _parse_date(name, getattr(self, name)))

        if self.start_date is not None and self.end_date is not None:
            try:
                DateRange(self.start_date, self.end_date)
            except ValueError as e:
                raise PipelineConfigError(str(e)) from e

    @property
    def date_range(self) -> DateRange:
        """Returns the processed range of dates.

        Raises:
            :class:`PipelineConfigError`:
                If a subclass made the dates optional and one is not set.
        """
        if self.start_date is None or self.end_date is None:
            raise PipelineConfigError(ERROR_NO_DATE_RANGE)

        return DateRange(self.start_date, self.end_date)

    @classmethod
    def from_namespace(cls, ns: SimpleNamespace, **kwargs: Any) -> PipelineConfig:
        """Creates a :class:`PipelineConfig` instance from a :class:`types.SimpleNamespace`.

        Args:
            ns:
                Namespace containing attributes matching this :class:`PipelineConfig` fields.

            **kwargs:
                Any additional arguments to be passed to the class constructor.

        Returns:
            A new :class:`PipelineConfig` instance.
        """
        ns_dict = vars(ns)
        ns_dict.update(kwargs)

        return cls(**ns_dict)

    @cached_property
    def top_level_package(self) -> str:
        """Returns the top-level package from this module."""
        module = self.__class__.__module__
        package = module.split(".")[0]

        return package

    @cached_property
    def jinja_env(self) -> Environment:
        """Returns a default jinja2 environment."""
        return EnvironmentLoader().from_package(
            package=self.top_level_package, path=self.jinja_folder
        )

    @property
    def pre_hooks(self) -> Sequence[Callable[[Any], None]]:
        """Sequence of callables executed before pipeline run."""
        return []

    @property
    def post_hooks(self) -> Sequence[Callable[[Any], None]]:
        """Sequence of callables executed after successful pipeline run."""
        return []

    def to_dict(self) -> dict[str, Any]:
        """Converts a :class:`PipelineConfig` instance to dictionary.

        Returns:
            A dictionary representation of the configuration.
        """
        return asdict(self)


def _parse_date(name: str, value: date | str | None) -> date | None:
    if value is None or (isinstance(value, date) and not isinstance(value, datetime)):
        return value

    if isinstance(value, str):
        try:
            return date.fromisoformat(value)
        except ValueError:
            pass

    raise PipelineConfigError(ERROR_DATE.format(name, value))
