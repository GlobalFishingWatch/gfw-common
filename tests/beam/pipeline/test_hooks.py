from datetime import date
from typing import Optional
from unittest import mock

import pytest

from gfw.common.beam.pipeline import Pipeline
from gfw.common.beam.pipeline.hooks import (
    create_table_hook,
    create_view_hook,
    delete_events_hook,
    update_table_metadata_hook,
)
from gfw.common.bigquery.helper import BigQueryHelper


@pytest.fixture
def table_config():
    class DummyTableConfig:
        table_id = "project.dataset.table"
        description = None

        def to_bigquery_params(self):
            return {"table": self.table_id, "schema": [{"name": "id", "type": "STRING"}]}

        def delete_query(self, start_date: date, end_date: Optional[date] = None):
            return f"DELETE FROM dataset.table WHERE event_date > '{start_date}'"

    return DummyTableConfig()


@pytest.fixture
def bq_clients():
    """Mock BigQuery clients created by a hook, to inspect what it sent to BigQuery."""
    return []


@pytest.fixture
def bq_client_factory(bq_clients):
    """A bq_client_factory to inject into a hook, recording the clients it makes."""
    mock_factory = BigQueryHelper.get_client_factory(mocked=True)

    def factory(**kwargs):
        client = mock_factory(**kwargs)
        bq_clients.append(client)
        return client

    return factory


@pytest.fixture
def pipeline():
    return Pipeline(project="test-project")


@pytest.fixture
def view_config():
    class DummyViewConfig:
        view_id = "project.dataset.view"

        def as_create_view_params(self):
            return {
                "view_id": self.view_id,
                "view_query": "SELECT * FROM dataset.source",
                "description": "",
                "schema": [{"name": "id", "type": "STRING"}],
            }

    return DummyViewConfig()


LABELS = {"project": "ais", "stage": "raw_gaps"}


def test_delete_events_hook_runs_the_delete_query_with_labels(
    table_config, pipeline, bq_client_factory, bq_clients
):
    hook = delete_events_hook(
        table_config,
        start_date=date(2024, 1, 1),
        labels=LABELS,
        bq_client_factory=bq_client_factory,
    )
    hook(pipeline)

    (client,) = bq_clients
    query, kwargs = client.query.call_args[0][0], client.query.call_args[1]
    assert query == table_config.delete_query(start_date=date(2024, 1, 1))
    assert kwargs["job_config"].labels == LABELS


def test_create_view_hook_labels_the_view_and_its_ddl_job(
    view_config, pipeline, bq_client_factory, bq_clients
):
    hook = create_view_hook(view_config, labels=LABELS, bq_client_factory=bq_client_factory)
    hook(pipeline)

    (client,) = bq_clients
    ddl_call = client.query.call_args_list[-1]
    assert "CREATE OR REPLACE VIEW `project.dataset.view`" in ddl_call[0][0]
    assert 'labels=[("project", "ais"), ("stage", "raw_gaps")]' in ddl_call[0][0]
    assert ddl_call[1]["job_config"].labels == LABELS


def test_create_table_hook_creates_the_table_with_labels(
    table_config, pipeline, bq_client_factory, bq_clients
):
    hook = create_table_hook(table_config, labels=LABELS, bq_client_factory=bq_client_factory)
    hook(pipeline)

    (client,) = bq_clients
    created_table = client.create_table.call_args[0][0]
    assert created_table.table_id == "table"
    assert created_table.labels == LABELS
    assert client.create_table.call_args[1]["exists_ok"] is True


def test_update_table_metadata_hook_sets_the_description_and_labels(
    table_config, pipeline, bq_client_factory, bq_clients
):
    table_config.description = mock.Mock(render=mock.Mock(return_value="A table."))

    hook = update_table_metadata_hook(
        table_config, labels=LABELS, bq_client_factory=bq_client_factory
    )
    hook(pipeline)

    (client,) = bq_clients
    updated_table, fields = client.update_table.call_args[0]
    assert updated_table.description == "A table."
    assert updated_table.labels == LABELS
    assert fields == ["description", "labels"]


def test_update_table_metadata_hook_without_description_only_sets_the_labels(
    table_config, pipeline, bq_client_factory, bq_clients
):
    hook = update_table_metadata_hook(
        table_config, labels=LABELS, bq_client_factory=bq_client_factory
    )
    hook(pipeline)

    (client,) = bq_clients
    _, fields = client.update_table.call_args[0]
    assert fields == ["labels"]


@pytest.mark.parametrize(
    "make_hook", [create_view_hook, create_table_hook, update_table_metadata_hook]
)
def test_hooks_require_labels(make_hook):
    with pytest.raises(TypeError, match="labels"):
        make_hook(object())


def test_delete_events_hook_requires_labels(table_config):
    with pytest.raises(TypeError, match="labels"):
        delete_events_hook(table_config, start_date=date(2024, 1, 1))
