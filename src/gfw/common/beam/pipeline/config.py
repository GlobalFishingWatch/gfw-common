"""Defines the `PipelineConfig` class used to configure data pipeline executions.

It includes:
- A dataclass `PipelineConfig` that stores date ranges and any unknown arguments.
- A custom exception `PipelineConfigError` for handling invalid configuration inputs.

Intended for use in CLI-based or programmatic pipeline setups where date ranges
and additional arguments need to be passed and validated.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import date
from functools import cached_property
from types import SimpleNamespace
from typing import Any, Callable, Sequence

from jinja2 import Environment

from gfw.common.jinja2 import EnvironmentLoader


ERROR_DATE = "Dates must be in ISO format. Got: {}."


class PipelineConfigError(Exception):
    """Custom exception for pipeline configuration errors."""

    pass


@dataclass
class PipelineConfig:
    """Configuration object for data pipeline execution.

    Note:
        This class is completely generic and independent of any specific pipeline framework.
    """

    date_range: tuple[str, str]
    """Tuple of start and end dates in ISO format (``YYYY-MM-DD``)."""

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
    def parsed_date_range(self) -> tuple[date, date]:
        """Returns the parsed start and end dates as :class:`~datetime.date` objects.

        Raises:
            :class:`PipelineConfigError`:
                If any of the dates are not in valid ISO format.

        Returns:
            A tuple containing parsed start and end dates.
        """
        try:
            start_str, end_str = self.date_range
            return (date.fromisoformat(start_str), date.fromisoformat(end_str))
        except ValueError as e:
            raise PipelineConfigError(ERROR_DATE.format(self.date_range)) from e

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
    def start_date(self) -> date:
        """Returns the start date of the configured range.

        Returns:
            A :class:`~datetime.date` object representing the start of the range.
        """
        return self.parsed_date_range[0]

    @property
    def end_date(self) -> date:
        """Returns the end date of the configured range.

        Returns:
            A :class:`~datetime.date` object representing the end of the range.
        """
        return self.parsed_date_range[1]

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
