import json

from pathlib import Path

import yaml

from gfw.common import io


def test_json_load(tmp_path):
    filepath = tmp_path.joinpath("test.json")
    data = {"test": 123}

    with open(filepath, mode="w") as f:
        json.dump(data, f)

    assert io.json_load(filepath) == data


def test_yaml_load(tmp_path):
    filepath = tmp_path.joinpath("test.yaml")
    data = {"test": 123}

    with open(filepath, mode="w") as f:
        yaml.dump(data, f)

    assert io.yaml_load(filepath) == data


def test_yaml_save(tmp_path):
    filepath = tmp_path.joinpath("test.yaml")
    data = {"test": 123}
    io.yaml_save(filepath, data)

    path = Path(filepath)
    assert path.is_file()
    assert io.yaml_load(filepath) == data
