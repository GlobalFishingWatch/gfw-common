"""Defines the configuration classes of data pipeline executions.

It includes:
- A dataclass `PipelineConfig` with the settings every pipeline has: its labels and any
  unknown arguments.
- A dataclass `DatePipelineConfig`, which adds the range of dates the pipeline processes. Most
  pipelines use it.
- A dataclass `DatetimePipelineConfig`, which adds a range of datetimes instead, for pipelines
  that process ranges shorter than a day.
- A custom exception `PipelineConfigError` for handling invalid configuration inputs.

Intended for use in CLI-based or programmatic pipeline setups.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import date, datetime
from functools import cached_property
from types import SimpleNamespace
from typing import Any, Callable, Sequence, TypeVar, get_args, get_type_hints

from jinja2 import Environment

from gfw.common.datetime import datetime_from_isoformat
from gfw.common.jinja2 import EnvironmentLoader


ERROR_ISOFORMAT = "{} must be a {} in ISO format. Got: {!r}."
ERROR_LABELS = "labels must not be empty: every pipeline labels its jobs to audit costs."
ERROR_RANGE_MISSING = "{} and {} must be set. Got: {!r}, {!r}."
ERROR_RANGE_EMPTY = "{} ({}) must be after {} ({})."

T = TypeVar("T", bound="PipelineConfig")
R = TypeVar("R", date, datetime)


class PipelineConfigError(Exception):
    """Custom exception for pipeline configuration errors."""

    pass


@dataclass(frozen=True, kw_only=True)
class PipelineConfig:
    """Configuration object for data pipeline execution, without a range to process.

    Pipelines use :class:`DatePipelineConfig` (a range of dates) or :class:`DatetimePipelineConfig`
    (a range of datetimes). Code that works with any pipeline configuration takes this class.

    Note:
        This class is completely generic and independent of any specific pipeline framework.

    Raises:
        :class:`PipelineConfigError`:
            If ``labels`` is empty.
    """

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

    def validate_labels(self) -> None:
        """Validates that :attr:`labels` is not empty.

        Raises:
            :class:`PipelineConfigError`:
                If :attr:`labels` is empty.
        """
        if not self.labels:
            raise PipelineConfigError(ERROR_LABELS)

    @classmethod
    def from_namespace(cls: type[T], ns: SimpleNamespace, **kwargs: Any) -> T:
        """Creates an instance of this class from a :class:`types.SimpleNamespace`.

        String values of fields declared as :class:`~datetime.date` or
        :class:`~datetime.datetime` (also ``| None``) are parsed from ISO format, so a field can
        come from a CLI option that parses it or one that leaves it as a string. Datetimes without
        a timezone are UTC.

        Args:
            ns:
                Namespace containing attributes matching this class's fields.

            **kwargs:
                Any additional arguments to be passed to the class constructor.

        Returns:
            A new instance of this class.

        Raises:
            :class:`PipelineConfigError`:
                If a date or datetime field is a string not in ISO format.
        """
        ns_dict = vars(ns)
        ns_dict.update(kwargs)

        hints = get_type_hints(cls)
        for name, value in ns_dict.items():
            if isinstance(value, str):
                ns_dict[name] = _parse_if_date(name, value, hints.get(name))

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
        """Converts this configuration to a dictionary.

        Returns:
            A dictionary representation of the configuration.
        """
        return asdict(self)


@dataclass(frozen=True, kw_only=True)
class DatePipelineConfig(PipelineConfig):
    """Configuration of a pipeline that processes a range of dates.

    The range goes from :attr:`start_date` (inclusive) to :attr:`end_date` (exclusive).
    The CLI options for them are :func:`gfw.common.cli.date_range_options`.

    Raises:
        :class:`PipelineConfigError`:
            If ``labels`` is empty, a date is ``None``, or :attr:`end_date` is not after
            :attr:`start_date`.
    """

    start_date: date
    """First date to process (inclusive)."""

    end_date: date
    """Date the processing ends at (exclusive)."""

    def __post_init__(self) -> None:
        """Validates the configuration."""
        super().__post_init__()
        self.validate_date_range()

    def validate_date_range(self) -> None:
        """Validates that both dates are set and :attr:`end_date` is after :attr:`start_date`.

        Raises:
            :class:`PipelineConfigError`:
                If a date is ``None``, or :attr:`end_date` is not after :attr:`start_date`.
        """
        _validate_range("start_date", self.start_date, "end_date", self.end_date)


@dataclass(frozen=True, kw_only=True)
class DatetimePipelineConfig(PipelineConfig):
    """Configuration of a pipeline that processes a range of datetimes.

    For pipelines that process ranges shorter than a day. The range goes from
    :attr:`start_datetime` (inclusive) to :attr:`end_datetime` (exclusive).
    The CLI options for them are :func:`gfw.common.cli.datetime_range_options`, which give
    timezone-aware datetimes, UTC unless a timezone is given.

    Raises:
        :class:`PipelineConfigError`:
            If ``labels`` is empty, a datetime is ``None``, or :attr:`end_datetime` is not after
            :attr:`start_datetime`.
    """

    start_datetime: datetime
    """Start of the time range to process (inclusive)."""

    end_datetime: datetime
    """End of the time range to process (exclusive)."""

    def __post_init__(self) -> None:
        """Validates the configuration."""
        super().__post_init__()
        self.validate_datetime_range()

    def validate_datetime_range(self) -> None:
        """Validates that both datetimes are set and :attr:`end_datetime` is after :attr:`start_datetime`.

        Raises:
            :class:`PipelineConfigError`:
                If a datetime is ``None``, or :attr:`end_datetime` is not after
                :attr:`start_datetime`.
        """
        _validate_range("start_datetime", self.start_datetime, "end_datetime", self.end_datetime)


def _validate_range(start_name: str, start: R | None, end_name: str, end: R | None) -> None:
    if start is None or end is None:
        raise PipelineConfigError(ERROR_RANGE_MISSING.format(start_name, end_name, start, end))

    if end <= start:
        raise PipelineConfigError(ERROR_RANGE_EMPTY.format(end_name, end, start_name, start))


def _parse_if_date(name: str, value: str, hint: Any) -> Any:
    types = (hint, *get_args(hint))

    if datetime in types:
        try:
            return datetime_from_isoformat(value)
        except ValueError as e:
            raise PipelineConfigError(ERROR_ISOFORMAT.format(name, "datetime", value)) from e

    if date in types:
        try:
            return date.fromisoformat(value)
        except ValueError as e:
            raise PipelineConfigError(ERROR_ISOFORMAT.format(name, "date", value)) from e

    return value
