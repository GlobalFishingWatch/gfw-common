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


def test_schema_property(config):
    assert config.schema == [{"name": "id", "type": "STRING"}]


class DefaultSchemaTableConfig(TableConfig):
    pass


def test_schema_defaults_to_the_schema_file_in_the_package_of_the_subclass():
    config = DefaultSchemaTableConfig(table_id="p.d.t", schema_file="normalize-schema.json")

    assert config.schema[0]["name"] == "msgid"


def test_schema_can_be_adjusted_starting_from_the_default():
    class AdjustedTableConfig(TableConfig):
        @property
        def schema(self):
            return [*super().schema, {"name": "extra", "type": "STRING"}]

    config = AdjustedTableConfig(table_id="p.d.t", schema_file="normalize-schema.json")

    assert config.schema[0]["name"] == "msgid"
    assert config.schema[-1] == {"name": "extra", "type": "STRING"}


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
