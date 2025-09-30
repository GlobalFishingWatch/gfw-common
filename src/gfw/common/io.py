"""Module that contains simple IO utilities."""

import json

from pathlib import Path
from typing import Any, Callable, List, Union

import yaml


def yaml_load(filename: str, **kwargs: Any) -> Any:
    """Loads a YAML file from the filesystem.

    Args:
        filename:
            Path to the YAML file to be loaded.

        **kwargs:
            Additional keyword arguments passed to :func:`yaml.safe_load`.

    Returns:
        The Python object resulting from parsing the YAML file.
    """
    with Path(filename).open("r") as f:
        return yaml.safe_load(f, **kwargs)


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
