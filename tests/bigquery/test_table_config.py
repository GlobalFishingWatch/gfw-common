import pytest

from gfw.common.bigquery.table_config import TableConfig, ViewConfig
from gfw.common.bigquery.table_description import TableDescription


class DummyTableConfig(TableConfig):
    @property
    def schema(self):
        return [{"name": "id", "type": "STRING"}]


class DummyViewConfig(ViewConfig):
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


def test_schema_property(config):
    assert config.schema == [{"name": "id", "type": "STRING"}]


def test_to_bigquery_params_with_description(config):
    result = config.to_bigquery_params(include_description=True)

    assert result["table"] == config.table_id
    assert result["schema"] == config.schema
    assert result["partition_type"] == config.partition_type
    assert result["partition_field"] == config.partition_field
    assert result["clustering_fields"] == config.clustering_fields
    assert "description" in result
    assert "AIS" in result["description"]
    assert "country" in result["description"]
    assert "1.2.3" in result["description"]


def test_to_bigquery_params_without_description(config):
    result = config.to_bigquery_params(include_description=False)
    assert "description" not in result


def test_view_config_view_id_defaults_to_source_table_id_plus_suffix(config):
    view = DummyViewConfig(source=config)
    assert view.view_id == "project.dataset.table_view"


def test_view_config_view_id_uses_custom_suffix(config):
    view = DummyViewConfig(source=config, suffix="last_versions")
    assert view.view_id == "project.dataset.table_last_versions"


def test_view_config_schema_defaults_to_source_schema(config):
    view = DummyViewConfig(source=config)
    assert view.schema == config.schema


def test_view_config_schema_can_be_overridden(config):
    class CustomSchemaViewConfig(ViewConfig):
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
