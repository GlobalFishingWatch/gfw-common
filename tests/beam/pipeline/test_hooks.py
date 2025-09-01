from datetime import date

import pytest

from gfw.common.beam.pipeline import Pipeline
from gfw.common.beam.pipeline.hooks import create_view_hook, delete_events_hook


@pytest.fixture
def table_config():
    class DummyTableConfig:
        view_id = "project.dataset.view"
        table_id = "project.dataset.table"

        def view_query(self):
            return "SELECT * FROM dataset.source"

        def delete_query(self, start_date):
            return f"DELETE FROM dataset.table WHERE event_date > '{start_date}'"

    return DummyTableConfig()


def test_delete_events_hook(table_config):
    hook = delete_events_hook(table_config, start_date=date(2024, 1, 1), mock=True)

    pipeline = Pipeline(project="test-project")
    hook(pipeline)


def test_create_view_hook(table_config):
    hook = create_view_hook(table_config, mock=True)

    pipeline = Pipeline(project="test-project")
    hook(pipeline)
