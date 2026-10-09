"""Module that contains simple IO utilities."""

import json

from pathlib import Path
from typing import Any, Callable, List, Union

import yaml


YAML_TIMESTAMP_TAG = "tag:yaml.org,2002:timestamp"


class _NoTimestampsSafeLoader(yaml.SafeLoader):
    """A :class:`yaml.SafeLoader` that loads unquoted dates and datetimes as strings."""


_NoTimestampsSafeLoader.yaml_implicit_resolvers = {
    first_char: [r for r in resolvers if r[0] != YAML_TIMESTAMP_TAG]
    for first_char, resolvers in yaml.SafeLoader.yaml_implicit_resolvers.items()
}


def yaml_load(filename: str, parse_timestamps: bool = True) -> Any:
    """Loads a YAML file from the filesystem.

    Args:
        filename:
            Path to the YAML file to be loaded.

        parse_timestamps:
            If False, unquoted dates and datetimes (e.g. ``2024-01-01``) are loaded as strings,
            like quoted ones, instead of :class:`~datetime.date` / :class:`~datetime.datetime`.

    Returns:
        The Python object resulting from parsing the YAML file.
    """
    loader: type[yaml.SafeLoader] = yaml.SafeLoader
    if not parse_timestamps:
        loader = _NoTimestampsSafeLoader

    with Path(filename).open("r") as f:
        return yaml.load(f, Loader=loader)  # Safe: a SafeLoader subclass.


def yaml_save(path: str, data: dict[str, Any], **kwargs: Any) -> None:
    """Saves a dictionary to a YAML file.

    Args:
        path:
            Path where the YAML file will be written.

        data:
            Dictionary or other serializable Python object to save.

        **kwargs:
            Additional keyword arguments passed to :func:`yaml.dump`.
    """
    with open(path, "w") as outfile:
        yaml.dump(data, outfile, default_flow_style=False, **kwargs)


def json_load(
    path: Path, lines: bool = False, coder: Callable[..., Any] = dict
) -> Union[List[dict[str, Any]], dict[str, Any]]:
    """Opens JSON file.

    Args:
        path:
            The source path.

        lines:
            If True, expects JSON Lines format.

        coder:
            Coder to use when reading JSON records.
    """
    if not lines:
        with open(path) as file:
            return json.load(file)

    with open(path, "r") as file:
        return [json.loads(each_line, object_hook=lambda d: coder(**d)) for each_line in file]


def json_save(
    path: Path, data: list[dict[Any, Any]], indent: int = 4, lines: bool = False
) -> Path:
    """Writes JSON file.

    Args:
        path:
            The destination path.

        data:
            List of records to write.

        indent:
            Amount of indentation.

        lines:
            If True, writes in JSON Lines format.
    """
    if not lines:
        with open(path, mode="w") as file:
            json.dump(data, file, indent=indent)
            return path

    with open(path, mode="w") as f:
        for item in data:
            f.write(json.dumps(item) + "\n")

    return path
