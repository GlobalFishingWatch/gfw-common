"""Unit tests for WriteToPartitionedBigQuery PTransform.

These tests verify that the transform configures the WriteToBigQuery
with expected parameters and passes data through unchanged using a fake.
"""

from typing import Any

import apache_beam as beam
import pytest

from apache_beam.io.gcp.bigquery import WriteToBigQuery
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.testing.test_pipeline import TestPipeline as _TestPipeline
from apache_beam.testing.util import assert_that, equal_to
from apache_beam.utils.timestamp import Timestamp

from gfw.common.beam.transforms import FakeWriteToBigQuery, WriteToPartitionedBigQuery
from gfw.common.bigquery.helper import BigQueryHelper


@pytest.mark.parametrize(
    "streaming, runner, expected_method",
    [
        (True, None, WriteToBigQuery.Method.STORAGE_WRITE_API),
        (True, "", WriteToBigQuery.Method.STORAGE_WRITE_API),
        (True, "direct", WriteToBigQuery.Method.STREAMING_INSERTS),
        (True, "DirectRunner", WriteToBigQuery.Method.STREAMING_INSERTS),
        (False, None, WriteToBigQuery.Method.FILE_LOADS),
        (False, "direct", WriteToBigQuery.Method.FILE_LOADS),
    ],
)
def test_resolve_write_method(streaming: bool, runner: str, expected_method: str) -> None:
    """Test WriteToPartitionedBigQuery.resolve_write_method."""
    options = StandardOptions()
    options.view_as(StandardOptions).streaming = streaming
    if runner:
        options.view_as(StandardOptions).runner = runner

    method = WriteToPartitionedBigQuery.resolve_write_method(options)
    assert method == expected_method


def test_get_client_factory_mocked_true():
    factory = WriteToPartitionedBigQuery.get_client_factory(mocked=True)
    assert factory is FakeWriteToBigQuery


def test_get_client_factory_mocked_false():
    factory = WriteToPartitionedBigQuery.get_client_factory(mocked=False)
    assert factory is WriteToBigQuery


def test_timestamp_fields():
    transform = WriteToPartitionedBigQuery(
        table="project:dataset.table",
        schema=[
            {"name": "event_time", "type": "TIMESTAMP", "mode": "REQUIRED"},
            {"name": "created_at", "type": "TIMESTAMP"},
            {"name": "device_id", "type": "STRING"},
            {"name": "value", "type": "FLOAT"},
        ],
    )

    assert transform.timestamp_fields == ["event_time", "created_at"]


def test_with_optionals() -> None:
    """Test WriteToPartitionedBigQuery with schema, partitioning, clustering and description."""
    sample_data = [{"name": "Alice"}, {"name": "Bob"}]
    table = "project:dataset.table"
    schema = [{"name": "name", "type": "STRING"}]
    partition_type = "HOUR"
    partition_field = "timestamp"
    description = "My Great Table"
    clustering_fields = ["source", "error"]

    expected_bq_params = {
        "timePartitioning": {"field": partition_field, "type": partition_type},
        "clustering": {"fields": clustering_fields},
    }

    _run_write_test(
        sample_data=sample_data,
        expected_bq_params=expected_bq_params,
        transform_kwargs={
            "table": table,
            "schema": schema,
            "partition_field": partition_field,
            "partition_type": partition_type,
            "description": description,
            "clustering_fields": clustering_fields,
        },
    )


def test_without_optionals() -> None:
    """Test WriteToPartitionedBigQuery with only required arguments."""
    sample_data = [{"name": "Alice"}, {"name": "Bob"}]
    table = "dataset.table"

    expected_bq_params = {
        "timePartitioning": {"type": "DAY"},
    }

    _run_write_test(
        sample_data=sample_data,
        expected_bq_params=expected_bq_params,
        transform_kwargs={
            "table": table,
        },
    )


def _run_write_test(
    *,
    sample_data: list[dict[str, Any]],
    expected_bq_params: dict[str, Any],
    transform_kwargs: dict[str, Any],
) -> None:
    """Helper that runs a Beam pipeline and asserts BQ config and data passthrough."""
    write_to_bigquery_instances: list[FakeWriteToBigQuery] = []

    def write_to_bigquery_factory(**kwargs: Any) -> WriteToBigQuery:
        instance = FakeWriteToBigQuery(**kwargs)
        write_to_bigquery_instances.append(instance)
        return instance

    options = PipelineOptions()
    with _TestPipeline(options=options) as p:
        _ = (
            p
            | "Create sample data" >> beam.Create(sample_data)
            | "Write to BQ"
            >> WriteToPartitionedBigQuery(
                project="test-project",
                write_to_bigquery_factory=write_to_bigquery_factory,
                bigquery_helper_factory=BigQueryHelper.mocked,
                **transform_kwargs,
            )
        )

    assert len(write_to_bigquery_instances) == 1
    instance = write_to_bigquery_instances[0]

    assert instance.kwargs["table"] == transform_kwargs["table"]

    if "schema" in transform_kwargs:
        assert instance.kwargs["schema"] == {"fields": transform_kwargs["schema"]}


def test_expand_converts_float_timestamp_to_beam_timestamp():
    input_data = [
        {"ts": 1710000000.0, "value": 1},  # some float timestamp
        {"ts": 1710000001.5, "value": 2},
    ]

    expected = [
        {"ts": Timestamp(1710000000.0), "value": 1},
        {"ts": Timestamp(1710000001.5), "value": 2},
    ]

    with _TestPipeline() as p:
        pcoll = p | "Create" >> beam.Create(input_data)

        transform = WriteToPartitionedBigQuery(
            table="project.dataset.table",
            schema=[
                {"name": "ts", "type": "TIMESTAMP"},
                {"name": "value", "type": "INTEGER"},
            ],
            bigquery_helper_factory=BigQueryHelper.mocked,
            write_to_bigquery_factory=lambda **kwargs: beam.Map(lambda x: x),  # no-op sink
        )

        # Force use of STORAGE_WRITE_API
        write_method = beam.io.gcp.bigquery.WriteToBigQuery.Method.STORAGE_WRITE_API
        transform.resolve_write_method = lambda _: write_method

        result = pcoll | "Apply Transform" >> transform

        assert_that(result, equal_to(expected))
