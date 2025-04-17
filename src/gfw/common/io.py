"""Module that contains simple IO utilities."""

import json

from pathlib import Path
from typing import Any

import yaml


def yaml_load(filename: str, **kwargs: Any) -> Any:
    """Loads YAML file from filesystem."""
    with Path(filename).open("r") as f:
        return yaml.safe_load(f, **kwargs)


def yaml_save(path: str, data: dict[str, Any], **kwargs: Any) -> None:
    """Saves dictionary to YAML file."""
    with open(path, "w") as outfile:
        yaml.dump(data, outfile, default_flow_style=False, **kwargs)


def json_load(path: str, **kwargs: Any) -> Any:
    """Loads JSON file from filesystem."""
    with open(path) as f:
        return json.load(f, **kwargs)
