import json

import pytest

from gfw.common.bigquery.table_config import TableConfig
from gfw.common.resources import load_schema, top_level_package


SCHEMA_FILE = "normalize-schema.json"


def test_top_level_package_of_a_class_in_this_package():
    class Local:
        pass

    assert top_level_package(Local) == "tests"


def test_top_level_package_of_a_class_in_another_package():
    assert top_level_package(TableConfig) == "gfw"


def test_load_schema_reads_the_fields_from_the_package():
    schema = load_schema("tests", SCHEMA_FILE)

    assert schema[0]["name"] == "msgid"
    assert all("type" in field for field in schema)


def test_load_schema_reads_from_another_folder(tmp_path, monkeypatch):
    package = tmp_path / "fake_package"
    (package / "schemas").mkdir(parents=True)
    (package / "__init__.py").touch()
    (package / "schemas" / "t.json").write_text(json.dumps([{"name": "id", "type": "STRING"}]))
    monkeypatch.syspath_prepend(str(tmp_path))

    assert load_schema("fake_package", "t.json", folder="schemas") == [
        {"name": "id", "type": "STRING"}
    ]


def test_load_schema_raises_if_the_file_does_not_exist():
    with pytest.raises(FileNotFoundError):
        load_schema("tests", "missing.json")
