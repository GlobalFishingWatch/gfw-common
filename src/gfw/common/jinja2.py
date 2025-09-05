"""Helpers for working with Jinja2 templates and environments."""

from typing import Any

from jinja2 import Environment, PackageLoader


class EnvironmentLoader:
    """Helper to instantiate :class:`jinja2.Environment` with sensible defaults.

    This class centralizes configuration for Jinja2 environments,
    including trimming blocks, left-stripping whitespace, and disabling autoescape
    (useful for SQL templates). Defaults can be overridden when instantiating
    the class or when creating an environment.
    """

    def __init__(self, **defaults: Any) -> None:
        """Initializes the EnvironmentLoader with optional default settings.

        Args:
            **defaults:
                Any arguments to be passed to :class:`jinja2.Environment` constructor.
        """
        self.defaults: dict[str, Any] = {
            "autoescape": False,
            "trim_blocks": True,
            "lstrip_blocks": True,
        }

        self.defaults.update(defaults)

    def from_package(self, package: str, path: str) -> Environment:
        """Creates a :class:`jinja2.Environment` for a given package and template path.

        Args:
            package:
                The Python package where the templates are located.

            path:
                The path to the templates inside the package.

        Returns:
            A configured :class:`jinja2.Environment` instance ready to load templates.
        """
        return Environment(
            loader=PackageLoader(package_name=package, package_path=path), **self.defaults
        )
