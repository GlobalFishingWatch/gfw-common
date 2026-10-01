from datetime import date
from typing import Optional

import pytest

from gfw.common.beam.pipeline import Pipeline
from gfw.common.beam.pipeline.hooks import create_view_hook, delete_events_hook


@pytest.fixture
def table_config():
    class DummyTableConfig:
        table_id = "project.dataset.table"

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
    hook = delete_events_hook(table_config, start_date=date(2024, 1, 1), mock=True)

    pipeline = Pipeline(project="test-project")
    hook(pipeline)


def test_create_view_hook(view_config):
    hook = create_view_hook(view_config, mock=True)

    pipeline = Pipeline(project="test-project")
    hook(pipeline)
