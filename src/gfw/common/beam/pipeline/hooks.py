"""Pipeline hooks for pre- or post-processing operations.

This module provides helper functions that generate hooks to be executed
during a pipeline's lifecycle. Hooks are callable functions that take a
:class:`~gfw.common.beam.pipeline.Pipeline` object and perform arbitrary operations,
such as creating views, deleting data, or any other custom task.
"""

import logging

from datetime import date
from typing import Callable, Optional

from google.cloud import bigquery

from gfw.common.bigquery.helper import BigQueryHelper
from gfw.common.bigquery.table_config import TableConfig
from gfw.common.bigquery.view_config import ViewConfig

from .base import Pipeline


logger = logging.getLogger(__name__)


def create_view_hook(
    view_config: ViewConfig,
    *,
    labels: dict[str, str],
    bq_client_factory: Callable[..., bigquery.Client] = bigquery.client.Client,
) -> Callable[[Pipeline], None]:
    """Returns a hook function to create a BigQuery view.

    Args:
        view_config:
            :class:`~gfw.common.bigquery.ViewConfig` instance containing view details.

        labels:
            Labels to set on the view and on the query job that creates it,
            e.g., for cost tracking.

        bq_client_factory:
            Factory for the BigQuery client. Defaults to a real client. Pass a mocked one
            (e.g., ``BigQueryHelper.get_client_factory(mocked=True)``) to run without BigQuery.

    Returns:
        A callable hook that accepts a :class:`~gfw.common.beam.pipeline.Pipeline`
        instance and creates the view.
    """

    def _hook(p: Pipeline) -> None:
        logger.info(f"Creating view: {view_config.view_id}...")
        bq_client = BigQueryHelper(
            client_factory=bq_client_factory, project=p.cloud_options.project
        )
        bq_client.create_view(**view_config.as_create_view_params(), labels=labels)
        logger.info("Done.")

    return _hook


def delete_events_hook(
    table_config: TableConfig,
    start_date: date,
    end_date: Optional[date] = None,
    *,
    labels: dict[str, str],
    bq_client_factory: Callable[..., bigquery.Client] = bigquery.client.Client,
) -> Callable[[Pipeline], None]:
    """Returns a hook function to delete events from a BigQuery table.

    Args:
        table_config:
            :class:`~gfw.common.bigquery.TableConfig` object containing
            table detailsand delete query.

        start_date:
            Date after which events should be deleted.

        end_date:
            Date up to which events should be deleted (exclusive).

        labels:
            Labels to attach to the delete query job, e.g., for cost tracking.

        bq_client_factory:
            Factory for the BigQuery client. Defaults to a real client. Pass a mocked one
            (e.g., ``BigQueryHelper.get_client_factory(mocked=True)``) to run without BigQuery.

    Returns:
        A callable hook that accepts a :class:`~gfw.common.beam.pipeline.Pipeline` instance
        and deletes events.
    """

    def _hook(p: Pipeline) -> None:
        table_id = table_config.table_id
        logger.info(f"Deleting events from '{table_id}' after '{start_date}'...")
        delete_query = table_config.delete_query(start_date=start_date, end_date=end_date)
        bq_client = BigQueryHelper(
            client_factory=bq_client_factory, project=p.cloud_options.project
        )
        bq_client.run_query(query_str=delete_query, labels=labels)
        logger.info("Done.")

    return _hook


def create_table_hook(
    table_config: TableConfig,
    *,
    labels: dict[str, str],
    bq_client_factory: Callable[..., bigquery.Client] = bigquery.client.Client,
) -> Callable[[Pipeline], None]:
    """Returns a hook function to create a BigQuery table.

    Args:
        table_config:
            :class:`~gfw.common.bigquery.TableConfig` instance containing view details.

        labels:
            Labels to set on the table when it's created, e.g., for cost tracking.
            An existing table keeps its labels.

        bq_client_factory:
            Factory for the BigQuery client. Defaults to a real client. Pass a mocked one
            (e.g., ``BigQueryHelper.get_client_factory(mocked=True)``) to run without BigQuery.

    Returns:
        A callable hook that accepts a :class:`~gfw.common.beam.pipeline.Pipeline`
        instance and creates the view.
    """

    def _hook(p: Pipeline) -> None:
        view_id = table_config.table_id
        logger.info(f"Creating table: {view_id}...")
        bq_client = BigQueryHelper(
            client_factory=bq_client_factory, project=p.cloud_options.project
        )
        params = table_config.to_bigquery_params()
        bq_client.create_table(**params, labels=labels, exists_ok=True)
        logger.info("Done.")

    return _hook


def update_table_metadata_hook(
    table_config: TableConfig,
    *,
    labels: dict[str, str],
    bq_client_factory: Callable[..., bigquery.Client] = bigquery.client.Client,
) -> Callable[[Pipeline], None]:
    """Returns a hook function to set a BigQuery table's description and labels.

    Meant as a post-hook for pipelines that write with ``CREATE_NEVER``: the table is
    created with its partitioning and clustering in a pre-hook (:func:`create_table_hook`),
    and its description and labels are set here once the data is written. Passing the
    description to the load job instead (``destinationTableProperties``) only works on new
    tables: on an existing table, BigQuery fails the job if the description differs from
    the current one.

    Args:
        table_config:
            :class:`~gfw.common.bigquery.TableConfig` instance containing table details.
            Its description, if any, replaces the table's.

        labels:
            Labels to set on the table, replacing its existing ones, e.g., for cost tracking.

        bq_client_factory:
            Factory for the BigQuery client. Defaults to a real client. Pass a mocked one
            (e.g., ``BigQueryHelper.get_client_factory(mocked=True)``) to run without BigQuery.

    Returns:
        A callable hook that accepts a :class:`~gfw.common.beam.pipeline.Pipeline`
        instance and updates the table's metadata.
    """

    def _hook(p: Pipeline) -> None:
        table_id = table_config.table_id
        logger.info(f"Updating the description and labels of {table_id}...")
        description = None
        if table_config.description is not None:
            description = table_config.description.render()

        bq_client = BigQueryHelper(
            client_factory=bq_client_factory, project=p.cloud_options.project
        )
        bq_client.update_table_metadata(table_id, description=description, labels=labels)
        logger.info("Done.")

    return _hook
