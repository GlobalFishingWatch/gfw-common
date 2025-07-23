"""This module defines the `PipelineConfig` class used to configure data pipeline executions.

It includes:
- A dataclass `PipelineConfig` that stores date ranges and any unknown arguments.
- Automatic parsing of date strings into `datetime.date` objects.
- Utility methods to convert from a `SimpleNamespace` or to a dictionary.
- A custom exception `PipelineConfigError` for handling invalid configuration inputs.

Intended for use in CLI-based or programmatic pipeline setups where date ranges
and additional arguments need to be passed and validated.
"""

from dataclasses import asdict, dataclass, field
from datetime import date
from functools import cached_property
from types import SimpleNamespace
from typing import Any


ERROR_DATE = "Dates must be in ISO format. Got: {}."


class PipelineConfigError(Exception):
    """Custom exception for pipeline configuration errors."""

    pass


@dataclass
class PipelineConfig:
    """Configuration object for data pipeline execution.

    Args:
        date_range:
            Tuple of start and end dates in ISO format (YYYY-MM-DD).

        name:
            Name of the pipeline.

        version:
            Version of the pipeline.

        mock_bq_clients:
            If True, all BigQuery interactions will be mocked.

        unknown_parsed_args:
            Parsed CLI or config arguments not explicitly defined in the config.

        unknown_unparsed_args:
            Raw unparsed CLI arguments.
    """

    date_range: tuple[str, str]
    name: str = ""
    version: str = "0.1.0"
    mock_bq_clients: bool = False
    unknown_parsed_args: dict[str, Any] = field(default_factory=dict)
    unknown_unparsed_args: tuple[str, ...] = ()

    @classmethod
    def from_namespace(cls, ns: SimpleNamespace) -> "PipelineConfig":
        """Creates a PipelineConfig instance from a SimpleNamespace.

        Args:
            ns: Namespace containing attributes matching PipelineConfig fields.

        Returns:
            A new PipelineConfig instance.
        """
        return cls(**vars(ns))

    @cached_property
    def parsed_date_range(self) -> tuple[date, date]:
        """Returns the parsed start and end dates as `datetime.date` objects.

        Raises:
            PipelineConfigError: If any of the dates are not in valid ISO format.

        Returns:
            A tuple containing start and end dates as `date` objects.
        """
        try:
            start_str, end_str = self.date_range
            return (date.fromisoformat(start_str), date.fromisoformat(end_str))
        except ValueError as e:
            raise PipelineConfigError(ERROR_DATE.format(self.date_range)) from e

    @property
    def start_date(self) -> date:
        """Returns the start date of the configured range.

        Returns:
            A `date` object representing the start of the range.
        """
        return self.parsed_date_range[0]

    @property
    def end_date(self) -> date:
        """Returns the end date of the configured range.

        Returns:
            A `date` object representing the end of the range.
        """
        return self.parsed_date_range[1]

    def to_dict(self) -> dict[str, Any]:
        """Converts the PipelineConfig to a dictionary.

        Returns:
            A dictionary representation of the configuration.
        """
        return asdict(self)
