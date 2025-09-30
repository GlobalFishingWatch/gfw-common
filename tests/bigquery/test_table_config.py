import pytest

from gfw.common.bigquery.table_config import TableConfig
from gfw.common.bigquery.table_description import TableDescription


class DummyTableConfig(TableConfig):
    @property
    def schema(self):
        return [{"name": "id", "type": "STRING"}]


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


def test_view_id_property(config):
    assert config.view_id == "project.dataset.table_view"


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
