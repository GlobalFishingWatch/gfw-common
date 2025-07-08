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


def test_json_save_and_load(tmp_path: Path):
    data = [{"a": 1, "b": 2}, {"a": 3, "b": 4}]

    # Test normal JSON save/load
    file_path = tmp_path / "test.json"
    io.json_save(file_path, data, indent=2, lines=False)

    loaded = io.json_load(file_path, lines=False)
    assert loaded == data

    # Test JSON Lines save/load
    file_path_lines = tmp_path / "test_lines.jsonl"
    io.json_save(file_path_lines, data, lines=True)

    loaded_lines = io.json_load(file_path_lines, lines=True)
    assert loaded_lines == data


def test_json_load_with_custom_coder(tmp_path: Path):
    class CustomDict(dict):
        def __init__(self, **kwargs):
            super().__init__(**kwargs)
            self.custom_attr = True

    data = [{"x": 1}, {"x": 2}]
    file_path = tmp_path / "custom.jsonl"

    # Write JSON lines manually
    with open(file_path, "w") as f:
        for item in data:
            f.write(json.dumps(item) + "\n")

    loaded = io.json_load(file_path, lines=True, coder=CustomDict)
    assert all(isinstance(d, CustomDict) for d in loaded)
    assert all(getattr(d, "custom_attr", False) for d in loaded)
