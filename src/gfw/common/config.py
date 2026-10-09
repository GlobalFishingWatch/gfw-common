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

from gfw.common.jinja2 import EnvironmentLoader


ERROR_DATE = "{} must be a date in ISO format (YYYY-MM-DD). Got: {!r}."
ERROR_DATE_TYPE = "{} must be a date. Got: {!r}."
ERROR_DATE_RANGE = "The end date ({}) must be after the start date ({})."
ERROR_LABELS = "labels must not be empty: every pipeline labels its jobs to audit costs."

DATE_FIELDS = ("start_date", "end_date")


class PipelineConfigError(Exception):
    """Custom exception for pipeline configuration errors."""

    pass


@dataclass(frozen=True, kw_only=True)
class PipelineConfig:
    """Configuration object for data pipeline execution.

    The processed range of dates goes from :attr:`start_date` (inclusive) to :attr:`end_date`
    (exclusive). Both are :class:`~datetime.date` objects. :meth:`from_namespace` parses them
    from ISO format strings (``YYYY-MM-DD``), which is how they can come from a config file.

    A pipeline that processes no dates can make them optional by redeclaring them with a default,
    e.g. ``start_date: date | None = None``. The range is then validated only if both are set.

    Note:
        This class is completely generic and independent of any specific pipeline framework.

    Raises:
        :class:`PipelineConfigError`:
            If ``labels`` is empty, a date is not a :class:`~datetime.date`, or :attr:`end_date`
            is not after :attr:`start_date`.
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
        """Validates the configuration."""
        if not self.labels:
            raise PipelineConfigError(ERROR_LABELS)

        for name in DATE_FIELDS:
            value = getattr(self, name)
            if value is not None and (not isinstance(value, date) or isinstance(value, datetime)):
                raise PipelineConfigError(ERROR_DATE_TYPE.format(name, value))

        if self.start_date is None or self.end_date is None:
            return

        if self.end_date <= self.start_date:
            raise PipelineConfigError(ERROR_DATE_RANGE.format(self.end_date, self.start_date))

    @classmethod
    def from_namespace(cls, ns: SimpleNamespace, **kwargs: Any) -> PipelineConfig:
        """Creates a :class:`PipelineConfig` instance from a :class:`types.SimpleNamespace`.

        Dates given as ISO format strings, e.g. from a config file, are parsed into
        :class:`~datetime.date` objects.

        Args:
            ns:
                Namespace containing attributes matching this :class:`PipelineConfig` fields.

            **kwargs:
                Any additional arguments to be passed to the class constructor.

        Returns:
            A new :class:`PipelineConfig` instance.

        Raises:
            :class:`PipelineConfigError`:
                If a date is a string that is not a valid ISO date.
        """
        ns_dict = vars(ns)
        ns_dict.update(kwargs)

        for name in DATE_FIELDS:
            if isinstance(ns_dict.get(name), str):
                ns_dict[name] = _parse_date(name, ns_dict[name])

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


def _parse_date(name: str, value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as e:
        raise PipelineConfigError(ERROR_DATE.format(name, value)) from e
