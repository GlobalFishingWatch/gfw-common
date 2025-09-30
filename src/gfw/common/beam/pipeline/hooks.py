"""Pipeline hooks for pre- or post-processing operations.

This module provides helper functions that generate hooks to be executed
during a pipeline's lifecycle. Hooks are callable functions that take a
:class:`~gfw.common.beam.pipeline.Pipeline` object and perform arbitrary operations,
such as creating views, deleting data, or any other custom task.
"""

import logging

from datetime import date
from typing import Callable

from gfw.common.bigquery.helper import BigQueryHelper
from gfw.common.bigquery.table_config import TableConfig

from .base import Pipeline


logger = logging.getLogger(__name__)


def create_view_hook(
    table_config: TableConfig,
    mock: bool = False,
) -> Callable[[Pipeline], None]:
    """Returns a hook function to create a view of a BigQuery table.

    Args:
        table_config:
            :class:`~gfw.common.bigquery.TableConfig` instance containing view details.

        mock:
            If True, uses a mocked BQ client instead of performing real operations.

    Returns:
        A callable hook that accepts a :class:`~gfw.common.beam.pipeline.Pipeline`
        instance and creates the view.
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
            :class:`~gfw.common.bigquery.TableConfig` object containing
            table detailsand delete query.

        start_date:
            Date after which events should be deleted.

        mock:
            If True, uses a mocked BQ client instead of performing real operations.

    Returns:
        A callable hook that accepts a :class:`~gfw.common.beam.pipeline.Pipeline` instance
        and deletes events.
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


def create_table_hook(
    table_config: TableConfig,
    mock: bool = False,
) -> Callable[[Pipeline], None]:
    """Returns a hook function to create a BigQuery table.

    Args:
        table_config:
            :class:`~gfw.common.bigquery.TableConfig` instance containing view details.

        mock:
            If True, uses a mocked BQ client instead of performing real operations.

    Returns:
        A callable hook that accepts a :class:`~gfw.common.beam.pipeline.Pipeline`
        instance and creates the view.
    """

    def _hook(p: Pipeline) -> None:
        view_id = table_config.table_id
        logger.info(f"Creating table: {view_id}...")
        client_factory = BigQueryHelper.get_client_factory(mocked=mock)
        bq_client = BigQueryHelper(client_factory=client_factory, project=p.cloud_options.project)
        params = table_config.to_bigquery_params()
        bq_client.create_table(**params, exists_ok=True)
        logger.info("Done.")

    return _hook
