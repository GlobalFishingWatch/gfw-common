from datetime import date
from typing import Optional

import pytest

from gfw.common.beam.pipeline import Pipeline
from gfw.common.beam.pipeline.hooks import (
    create_table_hook,
    create_view_hook,
    delete_events_hook,
)
from gfw.common.bigquery.helper import BigQueryHelper


@pytest.fixture
def table_config():
    class DummyTableConfig:
        table_id = "project.dataset.table"

        def to_bigquery_params(self):
            return {"table": self.table_id, "schema": [{"name": "id", "type": "STRING"}]}

        def delete_query(self, start_date: date, end_date: Optional[date] = None):
            return f"DELETE FROM dataset.table WHERE event_date > '{start_date}'"

    return DummyTableConfig()


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


def test_delete_events_hook(table_config):
    hook = delete_events_hook(table_config, start_date=date(2024, 1, 1), labels={}, mock=True)

    pipeline = Pipeline(project="test-project")
    hook(pipeline)


def test_delete_events_hook_labels_the_query_job(table_config, monkeypatch):
    calls = []
    monkeypatch.setattr(BigQueryHelper, "run_query", lambda self, **kwargs: calls.append(kwargs))
    labels = {"project": "ais", "stage": "port_state_transitions"}

    hook = delete_events_hook(table_config, start_date=date(2024, 1, 1), labels=labels, mock=True)
    hook(Pipeline(project="test-project"))

    assert calls[0]["labels"] == labels


def test_create_view_hook(view_config):
    hook = create_view_hook(view_config, labels={}, mock=True)

    pipeline = Pipeline(project="test-project")
    hook(pipeline)


def test_create_view_hook_labels_the_view(view_config, monkeypatch):
    calls = []
    monkeypatch.setattr(BigQueryHelper, "create_view", lambda self, **kwargs: calls.append(kwargs))
    labels = {"project": "ais", "stage": "raw_gaps"}

    hook = create_view_hook(view_config, labels=labels, mock=True)
    hook(Pipeline(project="test-project"))

    assert calls[0]["labels"] == labels


def test_create_table_hook_labels_the_table(table_config, monkeypatch):
    calls = []
    monkeypatch.setattr(
        BigQueryHelper, "create_table", lambda self, **kwargs: calls.append(kwargs)
    )
    labels = {"project": "ais", "stage": "raw_gaps"}

    hook = create_table_hook(table_config, labels=labels, mock=True)
    hook(Pipeline(project="test-project"))

    assert calls[0]["labels"] == labels
    assert calls[0]["exists_ok"] is True


@pytest.mark.parametrize("make_hook", [create_view_hook, create_table_hook])
def test_hooks_require_labels(make_hook):
    with pytest.raises(TypeError, match="labels"):
        make_hook(object(), mock=True)


def test_delete_events_hook_requires_labels(table_config):
    with pytest.raises(TypeError, match="labels"):
        delete_events_hook(table_config, start_date=date(2024, 1, 1), mock=True)
