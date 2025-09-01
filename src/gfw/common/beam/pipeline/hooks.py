"""Pipeline hooks for pre- or post-processing operations.

This module provides helper functions that generate hooks to be executed
during a pipeline's lifecycle. Hooks are callable functions that take a
`Pipeline` object and perform arbitrary operations, such as creating views,
deleting data, or any other custom task.

Hooks can be attached to pipeline steps for pre-processing, post-processing,
or cleanup tasks.

Functions:
    create_view_hook:
        Returns a hook that creates a BigQuery view.

    delete_events_hook:
        Returns a hook that deletes events from a BigQuery table after a specified date.
"""

import logging

from datetime import date
from typing import Callable

from gfw.common.beam.pipeline import Pipeline
from gfw.common.bigquery.helper import BigQueryHelper
from gfw.common.bigquery.table_config import TableConfig


logger = logging.getLogger(__name__)


def create_view_hook(
    table_config: TableConfig,
    mock: bool = False,
) -> Callable[[Pipeline], None]:
    """Returns a hook function to create a view of a BigQuery table.

    Args:
        table_config:
            TableConfig object containing view details.

        mock:
            If True, uses a mocked BQ client instead of performing real operations.

    Returns:
        A callable hook that accepts a `Pipeline` instance and creates the view.
    """

    def _hook(p: Pipeline) -> None:
        view_id = table_config.view_id
        view_query = table_config.view_query()
        logger.info(f"Creating view: {view_id}...")
        client_factory = BigQueryHelper.get_client_factory(mocked=mock)
        bq_client = BigQueryHelper(client_factory=client_factory, project=p.cloud_options.project)
        bq_client.create_view(view_id=view_id, view_query=view_query, exists_ok=True)
        logger.info("Done.")

    return _hook


def delete_events_hook(
    table_config: TableConfig,
    start_date: date,
    mock: bool = False,
) -> Callable[[Pipeline], None]:
    """Returns a hook function to delete events from a BigQuery table.

    Args:
        table_config:
            TableConfig object containing table details and delete query.

        start_date:
            Date after which events should be deleted.

        mock:
            If True, uses a mocked BQ client instead of performing real operations.

    Returns:
        A callable hook that accepts a `Pipeline` instance and deletes events.
    """

    def _hook(p: Pipeline) -> None:
        table_id = table_config.table_id
        logger.info(f"Deleting events from '{table_id}' after '{start_date}'...")
        delete_query = table_config.delete_query(start_date=start_date)
        client_factory = BigQueryHelper.get_client_factory(mocked=mock)
        bq_client = BigQueryHelper(client_factory=client_factory, project=p.cloud_options.project)
        bq_client.run_query(query_str=delete_query)
        logger.info("Done.")

    return _hook
