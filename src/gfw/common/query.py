"""Utilities for repsenting SQL queries in a structured, reusable way.

.. _Jinja2: https://jinja.palletsprojects.com/

This module defines the :class:`Query` class, which provides:
  - Automatic detection and setup of the `Jinja2 <https://jinja.palletsprojects.com/>`_
    environment using :class:`~gfw.common.jinja2.EnvironmentLoader`.
  - Helpers for rendering and formatting SQL queries.
  - A utility to expand :class:`typing.NamedTuple` schemas into ``SELECT`` clauses.
  - Built-in conversion for datetime fields to BigQuery-compatible Unix timestamps.
  - Support for dependency injection of custom :class:`jinja2.Environment` instances.

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

    Subclasses must define:
      - A Jinja2 template filename (via ``template_filename`` property).
      - Variables to render into the template (via ``template_vars`` property).

    The base class handles `Jinja2 <https://jinja.palletsprojects.com/>`_ environment setup,
    SQL rendering, and optional query formatting.
    """

    _jinja_env: Optional[Environment] = None

    DEFAULT_JINJA_FOLDER = "assets/queries"
    """Default folder where Jinja2_ templates are located."""

    @classmethod
    def datetime_to_timestamp(cls, field: str) -> str:
        """Returns SQL expression to cast a datetime field to TIMESTAMP.

        Args:
            field:
                The column name (string) to cast.

        Returns:
            A BigQuery SQL expression converting the datetime column to FLOAT64 seconds.
        """
        return f"CAST(UNIX_MICROS({field}) AS FLOAT64) / 1000000 AS {field}"

    @cached_property
    def output_type(self) -> Optional[type[NamedTuple]]:
        """Optional schema for the query results.

        Subclasses may override this property to return a :class:`~typing.NamedTuple`
        type that models the expected rows. The fields of this type are automatically
        expanded into the generated ``SELECT`` clause.

        Returns:
            A subclass of :class:`~typing.NamedTuple` describing the result schema,
            or ``None`` if no schema is defined.
        """
        return None

    @abstractmethod
    @cached_property
    def template_filename(self) -> str:
        """Name of the Jinja2 template file for this query.

        Subclasses must override this property to point to the SQL template file
        (relative to the :attr:`DEFAULT_JINJA_FOLDER`).

        Returns:
            The filename of the template (e.g., ``messages.sql.j2``).
        """

    @abstractmethod
    @cached_property
    def template_vars(self) -> dict[str, str]:
        """Variables to inject into the Jinja2 template.

        Subclasses must override this property to return the dictionary of
        template context variables (e.g., ``start_date``, ``end_date``, ...).

        Returns:
            A dictionary of key-value pairs for template rendering.
        """

    @cached_property
    def top_level_package(self) -> str:
        """Detects the top-level package name for this query class.

        This is used to locate the query templates when using
        :class:`~gfw.common.jinja2.EnvironmentLoader`.

        Returns:
            The name of the top-level package as a string.
        """
        module = self.__class__.__module__
        package = module.split(".")[0]

        return package

    @cached_property
    def jinja_env(self) -> Environment:
        """Returns or lazily creates a Jinja2 environment for this query.

        If no environment was explicitly set with :meth:`with_env`, one is created using
        :meth:`~gfw.common.jinja2.EnvironmentLoader.from_package`
        and the detected package name.
        """
        if self._jinja_env is None:
            self._jinja_env = EnvironmentLoader().from_package(
                package=self.top_level_package, path=self.DEFAULT_JINJA_FOLDER
            )

        return self._jinja_env

    def with_env(self, env: Environment) -> Query:
        """Injects a custom :class:`jinja2.Environment` into this query.

        This method enables dependency injection for testing or when using
        shared environments. Returns ``self`` for fluent chaining.

        Args:
            env:
                A configured :class:`jinja2.Environment`.

        Returns:
            self (the same query instance).
        """
        self._jinja_env = env
        return self

    def render(self, formatted: bool = False) -> str:
        """Renders the Query using Jinja2.

        Args:
            formatted:
                If True, the rendered query is formatted with
                `sqlparse <https://sqlparse.readthedocs.io/>`_.
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
        """Builds the ``SELECT`` clause fields from the output schema.

        Fields typed as :class:`~datetime.datetime` are automatically cast to Unix timestamps
        (via :meth:`datetime_to_timestamp`).
        All other fields are passed through.

        Returns:
            A comma-separated string of ``SELECT`` fields.
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
            strings:
                A list of plain strings.

        Returns:
            A list of SQL-safe quoted string literals.
        """
        return [f"'{s}'" for s in strings]

    @staticmethod
    def format(query: str) -> str:
        """Formats a SQL query string for better readability.

        Args:
            query:
                The raw SQL string.

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
