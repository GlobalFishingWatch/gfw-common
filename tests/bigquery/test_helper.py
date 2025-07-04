from unittest import mock

import pytest

from google.cloud import bigquery
from google.cloud.bigquery import WriteDisposition

from gfw.common.bigquery.helper import BigQueryHelper, QueryResult


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


def test_create_view_creates_view_and_logs():
    helper = BigQueryHelper.mocked(project="test")
    helper.client.project = "test"

    view = helper.create_view("my_dataset.my_view", "SELECT 1")

    helper.client.create_table.assert_called_once()
    created = helper.client.create_table.call_args[0][0]
    assert created.view_query == "SELECT 1"
    assert isinstance(view, mock.MagicMock)


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
    query_job.result.return_value = row_iterator

    result = QueryResult(query_job)
    assert len(result) == 42
    query_job.result.assert_called_once()


def test_query_result_iter():
    row1 = mock.Mock()
    row1.items.return_value = {"a": 1}.items()
    row2 = mock.Mock()
    row2.items.return_value = {"b": 2}.items()

    row_iterator = mock.MagicMock()
    row_iterator.__iter__.return_value = iter([row1, row2])
    row_iterator.total_rows = 2

    query_job = mock.Mock()
    query_job.result.return_value = row_iterator

    result = QueryResult(query_job)
    assert list(result) == [{"a": 1}, {"b": 2}]
    query_job.result.assert_called_once()


def test_query_result_next_returns_dict():
    row = mock.Mock()
    row.items.return_value = {"b": 2}.items()
    row_iterator = mock.MagicMock()
    row_iterator.__iter__.return_value = iter([row])
    row_iterator.total_rows = 1

    query_job = mock.Mock()
    query_job.result.return_value = row_iterator

    result = QueryResult(query_job)
    assert next(iter(result)) == {"b": 2}
    query_job.result.assert_called_once()


def test_query_result_to_list():
    row = mock.Mock()
    row.items.return_value = {"a": 1}.items()
    row_iterator = mock.MagicMock()
    row_iterator.__iter__.return_value = iter([row])
    row_iterator.total_rows = 1

    query_job = mock.Mock()
    query_job.result.return_value = row_iterator

    result = QueryResult(query_job)
    assert result.tolist() == [{"a": 1}]
    query_job.result.assert_called_once()


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


@pytest.mark.integration
def test_run_query_creates_session_and_returns_session_id():
    helper = BigQueryHelper()

    result = helper.run_query("SELECT 1", create_session=True)
    session_id = result.session_id

    assert session_id is not None
    assert isinstance(session_id, str)
