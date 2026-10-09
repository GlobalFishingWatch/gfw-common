"""Defines the configuration classes of data pipeline executions.

It includes:
- A dataclass `PipelineConfig` with the settings every pipeline has: its labels, the time range
  it processes and any unknown arguments.
- A custom exception `PipelineConfigError` for handling invalid configuration inputs.

Intended for use in CLI-based or programmatic pipeline setups.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field, fields
from datetime import date, datetime, timezone
from functools import cached_property
from types import SimpleNamespace
from typing import Any, Callable, Sequence, get_args, get_type_hints

from jinja2 import Environment

from gfw.common.datetime import datetime_from_date, datetime_from_isoformat
from gfw.common.jinja2 import EnvironmentLoader


ERROR_DATE = "{} must be a date in ISO format (YYYY-MM-DD). Got: {!r}."
ERROR_DATETIME = "{} must be a date or datetime in ISO format. Got: {!r}."
ERROR_DATETIME_RANGE = "end_datetime ({}) must be after start_datetime ({})."
ERROR_LABELS = "labels must not be empty: every pipeline labels its jobs to audit costs."

DATE_TO_DATETIME_KEYS = {"start_date": "start_datetime", "end_date": "end_datetime"}


class PipelineConfigError(Exception):
    """Custom exception for pipeline configuration errors."""

    pass


@dataclass(frozen=True, kw_only=True)
class PipelineConfig:
    """Configuration object for data pipeline execution.

    The processed time range goes from :attr:`start_datetime` (inclusive) to
    :attr:`end_datetime` (exclusive). Both are :class:`~datetime.datetime` objects, so a pipeline
    can process whole days or any time range. Every pipeline processes a time range: they are
    required, and always validated.

    :meth:`from_namespace` builds them from the ``start_date`` and ``end_date`` of the command
    line (``--start-date`` / ``--end-date``) or a config file, as UTC datetimes at midnight.

    Note:
        This class is completely generic and independent of any specific pipeline framework.

    Raises:
        :class:`PipelineConfigError`:
            If ``labels`` is empty, or :attr:`end_datetime` is not after
            :attr:`start_datetime`.
    """

    start_datetime: datetime
    """Start of the time range to process (inclusive)."""

    end_datetime: datetime
    """End of the time range to process (exclusive)."""

    labels: dict[str, str]
    """Labels to apply to the pipeline's Dataflow job and any BigQuery jobs it runs."""

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
        self.validate_labels()
        self.validate_datetime_range()

    def validate_labels(self) -> None:
        """Validates that :attr:`labels` is not empty.

        Raises:
            :class:`PipelineConfigError`:
                If :attr:`labels` is empty.
        """
        if not self.labels:
            raise PipelineConfigError(ERROR_LABELS)

    def validate_datetime_range(self) -> None:
        """Validates that :attr:`end_datetime` is after :attr:`start_datetime`.

        Raises:
            :class:`PipelineConfigError`:
                If :attr:`end_datetime` is not after :attr:`start_datetime`.
        """
        if self.end_datetime <= self.start_datetime:
            raise PipelineConfigError(
                ERROR_DATETIME_RANGE.format(self.end_datetime, self.start_datetime)
            )

    @classmethod
    def from_namespace(cls, ns: SimpleNamespace, **kwargs: Any) -> PipelineConfig:
        """Creates a :class:`PipelineConfig` instance from a :class:`types.SimpleNamespace`.

        The ``start_date`` and ``end_date`` of the namespace, e.g. from ``--start-date`` and
        ``--end-date``, become :attr:`start_datetime` and :attr:`end_datetime`.

        Fields declared as :class:`~datetime.datetime` (or ``datetime | None``) are turned into
        timezone-aware datetimes, UTC unless a timezone is given, from dates, naive datetimes or
        ISO format strings. Fields declared as :class:`~datetime.date` are parsed from ISO format
        strings. Strings come from config files, and YAML loads unquoted dates as dates.

        Args:
            ns:
                Namespace containing attributes matching this :class:`PipelineConfig` fields.

            **kwargs:
                Any additional arguments to be passed to the class constructor.

        Returns:
            A new :class:`PipelineConfig` instance.

        Raises:
            :class:`PipelineConfigError`:
                If a date or datetime is a string that is not in ISO format.
        """
        ns_dict = vars(ns)
        ns_dict.update(kwargs)

        for date_key, datetime_key in DATE_TO_DATETIME_KEYS.items():
            if date_key in ns_dict:
                ns_dict[datetime_key] = ns_dict.pop(date_key)

        hints = get_type_hints(cls)
        for f in fields(cls):
            value = ns_dict.get(f.name)
            if value is None:
                continue

            if _is_type(hints[f.name], datetime):
                ns_dict[f.name] = _to_utc_datetime(f.name, value)
            elif _is_type(hints[f.name], date) and isinstance(value, str):
                ns_dict[f.name] = _parse_date(f.name, value)

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


def _is_type(hint: Any, type_: type) -> bool:
    return type_ in (hint, *get_args(hint))


def _to_utc_datetime(name: str, value: str | date) -> datetime:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)

        return value

    if isinstance(value, date):
        return datetime_from_date(value)

    try:
        return datetime_from_isoformat(value)
    except ValueError as e:
        raise PipelineConfigError(ERROR_DATETIME.format(name, value)) from e


def _parse_date(name: str, value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as e:
        raise PipelineConfigError(ERROR_DATE.format(name, value)) from e
