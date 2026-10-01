import pytest

from gfw.common.bigquery.table_config import TableConfig
from gfw.common.bigquery.table_description import TableDescription
from gfw.common.bigquery.view_config import SingleSourceViewConfig


class DummyTableConfig(TableConfig):
    @property
    def schema(self):
        return [{"name": "id", "type": "STRING"}]


class DummyViewConfig(SingleSourceViewConfig):
    def view_query(self):
        return f"SELECT * FROM `{self.source.table_id}`"


@pytest.fixture
def config():
    return DummyTableConfig(
        table_id="project.dataset.table",
        schema_file="schema.json",
        partition_field="timestamp",
        clustering_fields=("vessel_id",),
        description=TableDescription(
            version="1.2.3",
            repo_name="my-repo",
            relevant_params={"source": "AIS", "country": "AR"},
        ),
    )


def test_view_config_view_id_defaults_to_source_table_id_plus_suffix(config):
    view = DummyViewConfig(source=config)
    assert view.view_id == "project.dataset.table_view"


def test_view_config_view_id_uses_custom_suffix(config):
    view = DummyViewConfig(source=config, suffix="last_versions")
    assert view.view_id == "project.dataset.table_last_versions"


def test_view_config_view_id_override_takes_precedence_over_suffix(config):
    view = DummyViewConfig(source=config, suffix="last_versions", view_id="raw_gaps")
    assert view.view_id == "raw_gaps"


def test_view_config_schema_defaults_to_source_schema(config):
    view = DummyViewConfig(source=config)
    assert view.schema == config.schema


def test_view_config_schema_can_be_overridden(config):
    class CustomSchemaViewConfig(SingleSourceViewConfig):
        def view_query(self):
            return "SELECT 1"

        @property
        def schema(self):
            return [{"name": "other", "type": "INTEGER"}]

    view = CustomSchemaViewConfig(source=config)
    assert view.schema == [{"name": "other", "type": "INTEGER"}]


def test_view_config_view_query(config):
    view = DummyViewConfig(source=config)
    assert view.view_query() == "SELECT * FROM `project.dataset.table`"


def test_view_config_description(config):
    description = TableDescription(
        version="1.0.0",
        repo_name="my-repo",
        relevant_params={},
    )
    view = DummyViewConfig(source=config, description=description)
    assert view.description is description


def test_as_create_view_params_with_description(config):
    description = TableDescription(
        version="1.0.0",
        repo_name="my-repo",
        relevant_params={"x": 1},
    )
    view = DummyViewConfig(source=config, description=description)

    result = view.as_create_view_params()

    assert result["view_id"] == view.view_id
    assert result["view_query"] == view.view_query()
    assert result["schema"] == view.schema
    assert "x" in result["description"]
    assert "1.0.0" in result["description"]


def test_as_create_view_params_without_description(config):
    view = DummyViewConfig(source=config)

    result = view.as_create_view_params()

    assert result["description"] == ""
