from unittest import mock

import pyarrow as pa
import pytest

from google.cloud import bigquery
from google.cloud.bigquery import SchemaField, WriteDisposition

from gfw.common.bigquery.helper import BigQueryHelper, QueryResult
from gfw.common.bigquery.schema import Schema
from gfw.common.bigquery.table_config import TableConfig
from gfw.common.bigquery.table_description import TableDescription


def test_mocked_factory_creates_mock():
    helper = BigQueryHelper.mocked(project="test")
    assert isinstance(helper.client, mock.NonCallableMagicMock)
    helper.client.query.assert_not_called()  # better mock check


def test_get_client_factory_returns_real():
    factory = BigQueryHelper.get_client_factory(mocked=False)
    assert factory is bigquery.client.Client


def test_client_sets_dry_run_flag_and_warns(caplog):
    helper = BigQueryHelper.mocked(dry_run=True)
    _ = helper.client  # trigger creation
    assert isinstance(helper.client, mock.NonCallableMagicMock)
    assert "*** Running Query Jobs as DRY RUN ***" in caplog.text


def test_end_session_executes_abort():
    helper = BigQueryHelper.mocked(project="test")
    helper.client.query.return_value.result.return_value = None

    helper.end_session("abc123")

    helper.client.query.assert_called_once_with("CALL BQ.ABORT_SESSION('abc123')")
    helper.client.query.return_value.result.assert_called_once()


def test_create_table_sets_all_fields():
    helper = BigQueryHelper.mocked(project="test")
    schema = [bigquery.SchemaField("id", "STRING")]
    helper.client.project = "test"

    table = helper.create_table(
        table="my_dataset.my_table",
        description="desc",
        schema=schema,
        partition_field="date",
        partition_type=bigquery.TimePartitioningType.HOUR,
        clustering_fields=["id"],
        labels={"env": "test"},
    )

    helper.client.create_table.assert_called_once()
    created_table = helper.client.create_table.call_args[0][0]

    assert created_table.schema == schema
    assert created_table.description == "desc"
    assert created_table.labels == {"env": "test"}
    assert created_table.time_partitioning.field == "date"
    assert created_table.time_partitioning.type_ == bigquery.TimePartitioningType.HOUR
    assert created_table.clustering_fields == ["id"]
    assert isinstance(table, mock.MagicMock)


def test_create_table_without_partition_field():
    bq = BigQueryHelper.mocked(project="test-project")

    table_name = "dataset.table_no_partition"
    description = "Test table without partition"
    schema = [{"name": "id", "type": "STRING"}]
    labels = {"env": "test"}

    bq.client.create_table.return_value = mock.Mock()  # mock return value

    result = bq.create_table(
        table=table_name,
        description=description,
        schema=schema,
        partition_field=None,
        labels=labels,
    )

    bq.client.create_table.assert_called_once()
    args, _ = bq.client.create_table.call_args

    created_table = args[0]
    assert created_table.description == description

    # Compare schema field by field
    for field, expected_field in zip(created_table.schema, schema, strict=False):
        assert field.name == expected_field["name"]
        assert field.field_type == expected_field["type"]
        # Mode is nullable by default in BigQuery SchemaField
        expected_mode = expected_field.get("mode", "NULLABLE")
        assert field.mode == expected_mode

    assert created_table.labels == labels
    assert created_table.time_partitioning is None
    assert result == bq.client.create_table.return_value


def test_create_view_executes_create_or_replace_view_query():
    helper = BigQueryHelper.mocked(project="test")
    helper.client.project = "test"

    # mock the query job returned by client.query
    query_job_mock = mock.MagicMock()
    helper.client.query.return_value = query_job_mock

    result = helper.create_view("my_dataset.my_view", "SELECT 1")

    helper.client.query.assert_called_once()
    query = helper.client.query.call_args[0][0]

    assert "CREATE OR REPLACE VIEW `my_dataset.my_view`" in query
    assert "SELECT 1" in query

    query_job_mock.result.assert_called_once()
    assert result is None


def test_create_view_with_description_adds_options_clause():
    helper = BigQueryHelper.mocked(project="test")
    helper.client.project = "test"
    helper.client.query.return_value = mock.MagicMock()

    helper.create_view("my_dataset.my_view", "SELECT 1", description="A view description.")

    query = helper.client.query.call_args[0][0]
    assert 'OPTIONS(description="""A view description.""")' in query


def test_create_view_without_description_renders_empty_options_clause():
    helper = BigQueryHelper.mocked(project="test")
    helper.client.project = "test"
    helper.client.query.return_value = mock.MagicMock()

    helper.create_view("my_dataset.my_view", "SELECT 1")

    query = helper.client.query.call_args[0][0]
    assert 'OPTIONS(description="""""")' in query


def test_create_view_with_schema_orders_column_list_by_dry_run_not_schema_arg():
    helper = BigQueryHelper.mocked(project="test")
    helper.client.project = "test"

    # Dry-run result deliberately in a DIFFERENT order than the `schema` arg below,
    # to prove the column list's order comes from the query (dry run), not `schema`.
    dry_run_job = mock.MagicMock()
    dry_run_job.schema = [
        SchemaField("created_at", "TIMESTAMP"),
        SchemaField("id", "STRING"),
    ]
    create_job = mock.MagicMock()

    def fake_query(query, job_config=None, **kwargs):
        if job_config is not None and job_config.dry_run:
            return dry_run_job
        return create_job

    helper.client.query.side_effect = fake_query

    schema = [
        {"name": "id", "type": "STRING", "description": "The id."},
        {"name": "created_at", "type": "TIMESTAMP"},
    ]
    helper.create_view("my_dataset.my_view", "SELECT 1", schema=schema)

    assert helper.client.query.call_count == 2

    dry_run_call = helper.client.query.call_args_list[0]
    assert dry_run_call[0][0] == "SELECT 1"
    assert dry_run_call[1]["job_config"].dry_run is True

    ddl_call = helper.client.query.call_args_list[1]
    query = ddl_call[0][0]
    assert (
        '(created_at OPTIONS(description=""""""), id OPTIONS(description="""The id."""))' in query
    )
    create_job.result.assert_called_once()


def test_create_view_without_schema_omits_column_list():
    helper = BigQueryHelper.mocked(project="test")
    helper.client.project = "test"
    helper.client.query.return_value = mock.MagicMock()

    helper.create_view("my_dataset.my_view", "SELECT 1")

    query = helper.client.query.call_args[0][0]
    assert 'CREATE OR REPLACE VIEW `my_dataset.my_view` OPTIONS(description="""""") AS' in query
    helper.client.query.assert_called_once()  # no dry run when there's no schema


def test_run_query_with_session_and_destination():
    helper = BigQueryHelper.mocked(project="test")
    mock_query_job = mock.MagicMock()
    helper.client.query.return_value = mock_query_job
    helper.client.project = "test"

    result = helper.run_query(
        query_str="SELECT 1",
        destination="dataset.output",
        write_disposition=WriteDisposition.WRITE_TRUNCATE,
        clustering_fields=["id"],
        session_id="abc123",
        labels={"env": "test"},
    )

    helper.client.query.assert_called_once()
    assert isinstance(result, QueryResult)
    assert result.query_job is mock_query_job


def test_run_query_without_destination():
    helper = BigQueryHelper.mocked(project="test")
    mock_query_job = mock.MagicMock()
    helper.client.query.return_value = mock_query_job
    helper.client.project = "test"

    result = helper.run_query("SELECT 1")
    assert isinstance(result, QueryResult)
    assert result.query_job is mock_query_job


def _helper_with_query_job():
    helper = BigQueryHelper.mocked(project="test")
    helper.client.query.return_value = mock.MagicMock()
    helper.client.project = "test"
    return helper


def _job_config(helper):
    return helper.client.query.call_args.kwargs["job_config"]


def test_run_query_sets_time_partitioning_and_clustering():
    helper = _helper_with_query_job()

    helper.run_query(
        "SELECT 1",
        destination="dataset.output",
        partition_field="trip_start",
        partition_type="MONTH",
        clustering_fields=("trip_start",),
    )

    job_config = _job_config(helper)
    assert job_config.time_partitioning.type_ == "MONTH"
    assert job_config.time_partitioning.field == "trip_start"
    assert job_config.clustering_fields == ["trip_start"]


def test_run_query_without_partition_field_leaves_partitioning_unset():
    helper = _helper_with_query_job()

    helper.run_query("SELECT 1", destination="dataset.output")

    assert _job_config(helper).time_partitioning is None


def test_run_query_updates_table_metadata_after_the_query():
    helper = _helper_with_query_job()
    schema = [{"name": "id", "type": "STRING", "description": "The id."}]

    helper.run_query(
        "SELECT 1",
        destination="dataset.output",
        write_disposition=WriteDisposition.WRITE_TRUNCATE,
        schema=schema,
        description="A table.",
        labels={"env": "test"},
    )

    helper.client.query.return_value.result.assert_called_once()
    table = helper.client.update_table.call_args.args[0]
    assert helper.client.update_table.call_args.args[1] == ["schema", "description", "labels"]
    assert table.description == "A table."
    assert table.labels == {"env": "test"}


def test_run_query_sets_labels_on_the_destination_table():
    helper = _helper_with_query_job()

    helper.run_query("SELECT 1", destination="dataset.output", labels={"env": "test"})

    table = helper.client.update_table.call_args.args[0]
    assert helper.client.update_table.call_args.args[1] == ["labels"]
    assert table.labels == {"env": "test"}


def test_run_query_without_metadata_or_labels_leaves_the_table_unchanged():
    helper = _helper_with_query_job()

    helper.run_query("SELECT 1", destination="dataset.output", labels={})

    assert helper.client.update_table.call_args.args[1] == []


def test_run_query_without_destination_does_not_touch_any_table():
    helper = _helper_with_query_job()

    helper.run_query("SELECT 1", labels={"env": "test"}, description="ignored")

    helper.client.get_table.assert_not_called()
    helper.client.update_table.assert_not_called()


def test_update_table_metadata_only_updates_given_fields():
    helper = BigQueryHelper.mocked(project="test")
    helper.client.project = "test"

    helper.update_table_metadata("dataset.output", description="A table.")

    table = helper.client.update_table.call_args.args[0]
    assert helper.client.update_table.call_args.args[1] == ["description"]
    assert table.description == "A table."


def test_update_table_metadata_with_nothing_given_updates_no_fields():
    helper = BigQueryHelper.mocked(project="test")
    helper.client.project = "test"

    helper.update_table_metadata("dataset.output")

    assert helper.client.update_table.call_args.args[1] == []


def test_run_query_warns_on_write_truncate_without_schema(caplog):
    helper = _helper_with_query_job()

    helper.run_query(
        "SELECT 1", destination="dataset.output", write_disposition=WriteDisposition.WRITE_TRUNCATE
    )

    assert "WRITE_TRUNCATE into dataset.output without a schema" in caplog.text


def test_run_query_does_not_warn_on_write_truncate_with_schema(caplog):
    helper = _helper_with_query_job()

    helper.run_query(
        "SELECT 1",
        destination="dataset.output",
        write_disposition=WriteDisposition.WRITE_TRUNCATE,
        schema=[{"name": "id", "type": "STRING"}],
    )

    assert "without a schema" not in caplog.text


def test_run_query_with_table_config_fields():
    class DummyTableConfig(TableConfig):
        @property
        def schema(self):
            return [{"name": "id", "type": "STRING"}]

    table_config = DummyTableConfig(
        table_id="dataset.output",
        schema_file="schema.json",
        partition_type="MONTH",
        partition_field="trip_start",
        clustering_fields=("trip_start",),
        description=TableDescription(version="1.2.3", repo_name="my-repo"),
    )
    helper = _helper_with_query_job()

    helper.run_query(
        "SELECT 1",
        destination=table_config.table_id,
        write_disposition=WriteDisposition.WRITE_TRUNCATE,
        partition_type=table_config.partition_type,
        partition_field=table_config.partition_field,
        clustering_fields=table_config.clustering_fields,
        schema=table_config.schema,
        description=table_config.description.render(),
    )

    job_config = _job_config(helper)
    assert job_config.destination.table_id == "output"
    assert job_config.time_partitioning.type_ == "MONTH"
    assert job_config.clustering_fields == ["trip_start"]
    assert helper.client.update_table.call_args.args[1] == ["schema", "description"]


def test_format_jinja2(tmp_path):
    template_file = tmp_path / "query.sql"
    template_file.write_text("SELECT * FROM {{ table }}")

    rendered = BigQueryHelper.format_jinja2(
        template_file.name, search_path=tmp_path, table="my_table"
    )

    assert rendered.strip() == "SELECT * FROM my_table"


def test_create_table_reference_uses_project():
    helper = BigQueryHelper.mocked(project="test")
    helper.client.project = "my-project"

    ref = helper._create_table_reference("dataset.table")
    assert ref.project == "my-project"
    assert ref.dataset_id == "dataset"
    assert ref.table_id == "table"


def test_query_result_len():
    row_iterator = mock.Mock()
    row_iterator.total_rows = 42
    query_job = mock.Mock()
    result = QueryResult(query_job, row_iterator)
    assert len(result) == 42


def test_query_result_iter():
    row1 = mock.Mock()
    row2 = mock.Mock()
    row_iterator = mock.MagicMock()
    row_iterator.__iter__.return_value = iter([row1, row2])
    row_iterator.total_rows = 2
    query_job = mock.Mock()
    result = QueryResult(query_job, row_iterator)
    assert list(result) == [row1, row2]


def test_query_result_iter_as_dicts():
    row1 = mock.Mock()
    row1.items.return_value = {"a": 1}.items()
    row2 = mock.Mock()
    row2.items.return_value = {"b": 2}.items()
    row_iterator = mock.MagicMock()
    row_iterator.__iter__.return_value = iter([row1, row2])
    query_job = mock.Mock()
    result = QueryResult(query_job, row_iterator)
    assert list(result.iter_as_dicts()) == [{"a": 1}, {"b": 2}]


def test_query_result_next_returns_row():
    row = mock.Mock()
    row_iterator = mock.MagicMock()
    row_iterator.__iter__.return_value = iter([row])
    query_job = mock.Mock()
    result = QueryResult(query_job, row_iterator)
    assert next(iter(result)) == row


def test_query_result_tolist():
    row = mock.Mock()
    row_iterator = mock.MagicMock()
    row_iterator.__iter__.return_value = iter([row])
    query_job = mock.Mock()
    result = QueryResult(query_job, row_iterator)
    assert result.tolist() == [row]


def test_query_result_tolist_as_dicts():
    row = mock.Mock()
    row.items.return_value = {"a": 1}.items()
    row_iterator = mock.MagicMock()
    row_iterator.__iter__.return_value = iter([row])
    query_job = mock.Mock()
    result = QueryResult(query_job, row_iterator)
    assert result.tolist(as_dicts=True) == [{"a": 1}]


def test_load_from_json_with_partition_field():
    bq = BigQueryHelper.mocked(project="test-project")
    bq.load_from_json(
        rows=[{"ts": "2024-01-01T00:00:00Z"}],
        destination="dataset.table",
        partition_field="ts",
        partition_type=bigquery.table.TimePartitioningType.HOUR,
    )

    call = bq.client.load_table_from_json.call_args
    _, kwargs = call

    job_config = kwargs["job_config"]
    assert isinstance(job_config, bigquery.LoadJobConfig)
    assert job_config.time_partitioning is not None
    assert job_config.time_partitioning.field == "ts"
    assert job_config.time_partitioning.type_ == bigquery.table.TimePartitioningType.HOUR


def test_load_from_json_with_schema():
    bq = BigQueryHelper.mocked(project="test-project")
    schema = [{"name": "id", "type": "STRING"}]

    bq.load_from_json(
        rows=[{"id": "abc"}],
        destination="dataset.table",
        schema=schema,
    )

    call = bq.client.load_table_from_json.call_args
    _, kwargs = call

    job_config = kwargs["job_config"]
    assert job_config.schema is not None
    assert job_config.schema[0].name == "id"
    assert job_config.schema[0].field_type == "STRING"


def test_load_from_json_with_clustering():
    bq = BigQueryHelper.mocked(project="test-project")

    bq.load_from_json(
        rows=[{"ts": "2024-01-01T00:00:00Z", "id": "123"}],
        destination="dataset.table",
        partition_field="ts",
        clustering_fields=["id"],
    )

    call = bq.client.load_table_from_json.call_args
    _, kwargs = call

    job_config = kwargs["job_config"]
    assert job_config.clustering_fields == ["id"]


def test_load_from_json_with_write_disposition():
    bq = BigQueryHelper.mocked(project="test-project")

    bq.load_from_json(
        rows=[{"id": "1"}],
        destination="dataset.table",
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )

    call = bq.client.load_table_from_json.call_args
    _, kwargs = call

    job_config = kwargs["job_config"]
    assert job_config.write_disposition == bigquery.WriteDisposition.WRITE_TRUNCATE


def test_load_from_json_with_partitioning():
    bq = BigQueryHelper.mocked(project="test-project")

    bq.load_from_json(
        rows=[{"id": "abc"}],
        destination="dataset.table",
        partition_field="id",
        partition_type="HOUR",
    )

    job_config = bq.client.load_table_from_json.call_args.kwargs["job_config"]
    assert isinstance(job_config.time_partitioning, bigquery.table.TimePartitioning)
    assert job_config.time_partitioning.type_ == "HOUR"
    assert job_config.time_partitioning.field == "id"


def test_fetch_schema_returns_schema_object():
    helper = BigQueryHelper.mocked(project="test")
    fields = [
        SchemaField("ssvid", "STRING", mode="NULLABLE"),
        SchemaField("lat", "FLOAT", mode="REQUIRED"),
    ]
    helper.client.get_table.return_value.schema = fields

    result = helper.fetch_schema("proj.ds.table")

    helper.client.get_table.assert_called_once_with("proj.ds.table")
    assert isinstance(result, Schema)
    assert result.fields == fields


def test_fetch_schema_as_pyarrow():
    helper = BigQueryHelper.mocked(project="test")
    helper.client.get_table.return_value.schema = [
        SchemaField("ssvid", "STRING", mode="NULLABLE"),
        SchemaField("lat", "FLOAT", mode="REQUIRED"),
    ]

    pa_schema = helper.fetch_schema("proj.ds.table").as_pyarrow()

    assert pa_schema.field("ssvid").type == pa.string()
    assert not pa_schema.field("lat").nullable


@pytest.mark.integration
def test_run_query_creates_session_and_returns_session_id():
    helper = BigQueryHelper()

    result = helper.run_query("SELECT 1", create_session=True)
    session_id = result.session_id

    assert session_id is not None
    assert isinstance(session_id, str)
