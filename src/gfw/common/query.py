"""Abstract base class for SQL queries rendered via Jinja2 templates.

This module defines the `Query` class, which serves as a foundation for
representing SQL queries in a structured, reusable way. Queries are expressed
as Jinja2 templates, with subclasses responsible for providing:

  - A concrete output schema (`output_type`) defined as a `NamedTuple`.
  - The Jinja2 template filename (`template_filename`).
  - A dictionary of variables used in the template (`template_vars`).

The `Query` class provides:
  - Automatic detection and setup of the Jinja2 environment using `EnvironmentLoader`.
  - Helpers for rendering and formatting SQL queries.
  - A utility to expand `NamedTuple` schemas into `SELECT` clauses.
  - Built-in conversion for datetime fields to BigQuery-compatible Unix timestamps.
  - Support for dependency injection of custom Jinja2 environments.

This design centralizes SQL generation, improves testability, and ensures
consistency across queries within pipelines or shared libraries.
"""

from __future__ import annotations  # Avoids forward reference problem in type hints

import logging

from abc import ABC, abstractmethod
from datetime import datetime
from functools import cached_property
from typing import NamedTuple, Optional, get_type_hints

import sqlparse

from jinja2 import Environment

from gfw.common.jinja2 import EnvironmentLoader


logger = logging.getLogger(__name__)


class Query(ABC):
    """Abstract base class for SQL queries rendered via Jinja2 templates.

    Subclasses of `Query` define:
      - The output schema as a `NamedTuple` (via `output_type`).
      - A Jinja2 template filename (`template_filename`).
      - Variables to render into the template (`template_vars`).

    The base class handles Jinja2 environment setup, SQL rendering,
    and optional query formatting.
    """

    _jinja_env: Optional[Environment] = None

    DEFAULT_JINJA_FOLDER = "assets/queries"

    @classmethod
    def datetime_to_timestamp(cls, field: str) -> str:
        """Converts a datetime field into a Unix timestamp expression for BigQuery.

        Args:
            field: The column name (string) to cast.

        Returns:
            A BigQuery SQL expression converting the datetime column to FLOAT64 seconds.
        """
        return f"CAST(UNIX_MICROS({field}) AS FLOAT64) / 1000000 AS {field}"

    @abstractmethod
    @cached_property
    def output_type(self) -> type[NamedTuple]:
        """Defines the schema of the query results.

        Each subclass must return a `NamedTuple` type that models the expected
        rows from the query. The fields of this type are automatically expanded
        into the `SELECT` clause.

        Returns:
            A `NamedTuple` subclass describing the result schema.
        """

    @abstractmethod
    @cached_property
    def template_filename(self) -> str:
        """Name of the Jinja2 template file for this query.

        Subclasses must override this property to point to the SQL template file
        (relative to the `DEFAULT_JINJA_FOLDER`).

        Returns:
            The filename of the template (e.g., `"messages.sql.j2"`).
        """

    @abstractmethod
    @cached_property
    def template_vars(self) -> dict[str, str]:
        """Variables to inject into the Jinja2 template.

        Subclasses must override this property to return the dictionary of
        template context variables (e.g., start_date, end_date, table names).

        Returns:
            A dictionary of key-value pairs for template rendering.
        """

    @cached_property
    def top_level_package(self) -> str:
        """Detects the top-level package name for this query class.

        This is used to locate the query templates when using `PackageLoader`.

        Returns:
            The name of the top-level package as a string.
        """
        module = self.__class__.__module__
        package = module.split(".")[0]

        return package

    @cached_property
    def jinja_env(self) -> Environment:
        """Returns or lazily creates a Jinja2 environment for this query.

        If no environment was explicitly set with `with_env()`, one is created
        using `EnvironmentLoader.from_package()` and the detected package name.
        """
        if self._jinja_env is None:
            self._jinja_env = EnvironmentLoader().from_package(
                package=self.top_level_package, path=self.DEFAULT_JINJA_FOLDER
            )

        return self._jinja_env

    def with_env(self, env: Environment) -> Query:
        """Injects a custom Jinja2 environment into this query.

        This method enables dependency injection for testing or when using
        shared environments. Returns `self` for fluent chaining.

        Args:
            env: A configured Jinja2 `Environment`.

        Returns:
            Self (the same query instance).
        """
        self._jinja_env = env
        return self

    def render(self, formatted: bool = False) -> str:
        """Renders the Query using Jinja2.

        Args:
            formatted:
                If True, the rendered query is formatted with `sqlparse`.
                Defaults to False.

        Returns:
            The rendered query string (formatted if requested).
        """
        template = self.jinja_env.get_template(self.template_filename)

        template_vars = self.template_vars
        query = template.render(template_vars)
        formatted_query = self.format(query)

        logger.debug(f"Rendered Query for {self}: ")
        logger.debug(formatted_query)

        if formatted:
            return formatted_query

        return query

    def get_select_fields(self) -> str:
        """Builds the `SELECT` clause fields from the output schema.

        Fields typed as `datetime` are automatically cast to Unix timestamps
        (via `datetime_to_timestamp`). All other fields are passed through.

        Returns:
            A comma-separated string of SELECT fields.
        """
        fields = get_type_hints(self.output_type)

        clause_parts = []
        for field, class_ in fields.items():
            if class_ == datetime:
                clause_parts.append(self.datetime_to_timestamp(field))
            else:
                clause_parts.append(field)

        return ",".join(clause_parts)

    @staticmethod
    def sql_strings(strings: list[str]) -> list[str]:
        """Wraps each string in single quotes for safe SQL usage.

        Args:
            strings: A list of plain strings.

        Returns:
            A list of SQL-safe quoted string literals.
        """
        return [f"'{s}'" for s in strings]

    @staticmethod
    def format(query: str) -> str:
        """Formats a SQL query string for readability using `sqlparse`.

        Args:
            query: The raw SQL string.

        Returns:
            A neatly indented and uppercased SQL string.
        """
        return sqlparse.format(
            query,
            reindent=True,
            use_space_around_operators=True,
            strip_comments=True,
            keyword_case="upper",
        )
