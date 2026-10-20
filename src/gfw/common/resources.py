"""Helpers to locate the files a package ships with, like its query templates and schemas.

Pipelines keep those files under their own top-level package (e.g. ``pipe_gaps/assets/schemas``),
so classes defined in gfw-common find them from the package of the subclass that uses them.
"""

import json

from importlib.resources import files
from typing import Any


SCHEMAS_FOLDER = "assets/schemas"


def top_level_package(cls: type) -> str:
    """Returns the top-level package of the module where ``cls`` is defined.

    Args:
        cls:
            The class to locate, e.g. ``type(self)`` in a base class, to get the package
            of the subclass.

    Returns:
        The name of the top-level package, e.g. ``"pipe_gaps"`` for
        ``pipe_gaps.pipelines.raw_gaps.table_config.RawGapsTableConfig``.
    """
    return cls.__module__.split(".")[0]


def load_schema(package: str, filename: str, folder: str = SCHEMAS_FOLDER) -> list[dict[str, Any]]:
    """Returns the BigQuery schema in a JSON file shipped with ``package``.

    Args:
        package:
            The package that ships the file, e.g. ``"pipe_gaps"``.

        filename:
            The name of the JSON file, e.g. ``"raw_gaps.json"``.

        folder:
            The folder of the file, relative to the package.

    Returns:
        The fields of the schema, as BigQuery field dicts.
    """
    schema: list[dict[str, Any]] = json.loads(
        files(package).joinpath(folder).joinpath(filename).read_text(encoding="utf-8")
    )
    return schema
