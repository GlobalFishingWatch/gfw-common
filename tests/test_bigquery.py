from unittest import mock
from datetime import date
from importlib import resources

from google.cloud import bigquery

from gfw.common.bigquery_helper import BigQueryHelper, QueryResult
from gfw.common.io import json_load

from tests.assets import queries, schemas


def test_get_client_factory():
    factory = BigQueryHelper.get_client_factory()
    assert factory == bigquery.Client


def test_dry_run_properly_set():
    bq = BigQueryHelper.mocked(dry_run=True)
    assert bq.client.default_query_job_config.dry_run is True

    bq = BigQueryHelper.mocked()
    assert bq.client.default_query_job_config.dry_run is False


def test_create_table_with_defaults():
    project = "test-project"

    bq = BigQueryHelper.mocked(project=project)
    assert bq.client.project == project

    table_name = "dataset.table"
    bq.create_table(table=table_name)


def test_create_table():
    bq = BigQueryHelper.mocked(project="test-project")
    table_name = "dataset.table"
    bq.create_table(
        table=table_name,
        schema=json_load(resources.files(schemas) / "normalize-schema.json"),
        partition_field="test",
        clustering_fields=["field1"],
    )


def test_begin_session():
    bq = BigQueryHelper.mocked(project="test-project")
    bq.begin_session()


def test_end_session():
    bq = BigQueryHelper.mocked(project="test-project")
    bq.end_session(session_id="1234")


def test_create_view():
    bq = BigQueryHelper.mocked(project="test-project")
    bq.create_view(view_id="dataset.view", view_query="SELECT 1")


def test_run_query():
    bq = BigQueryHelper.mocked(project="test-project")

    result = bq.run_query("SELECT 2")
    assert isinstance(result, QueryResult)


def test_query_result():
    data = [{"first": 1}, {"second": 2}]

    def __iter__(self):
        for x in data:
            yield x

    row_iterator = mock.create_autospec(bigquery.table.RowIterator, total_rows=len(data))
    row_iterator.__iter__ = __iter__

    result = QueryResult(row_iterator)
    assert len(result) == 2
    assert result.tolist() == data

    for item, expected in zip(result, data):
        assert isinstance(item, dict)
        assert item == expected

    item = next(result) == {}


def test_run_query_with_destination():
    bq = BigQueryHelper.mocked(project="test-project")
    bq.run_query("SELECT 1", destination="dataset.table")


def test_run_query_with_session_id():
    bq = BigQueryHelper.mocked(project="test-project")
    bq.run_query("SELECT 1", session_id="123456")


def test_load_from_json():
    bq = BigQueryHelper.mocked(project="test-project")
    bq.load_from_json(rows=[{}], destination="")


def test_format_jinja2():
    bq = BigQueryHelper.mocked(project="test-project")
    bq.format_jinja2(
        template_path="hourly-to-daily.sql.j2",
        search_path=resources.files(queries),
        source_table="TABLE",
        date=date(2024, 1, 1)
    )
