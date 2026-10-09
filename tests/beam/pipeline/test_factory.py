from datetime import datetime, timezone
from unittest.mock import Mock

import pytest

from gfw.common.beam.pipeline import Pipeline, PipelineFactory
from gfw.common.beam.pipeline.factory import PipelineFactoryError
from gfw.common.config import PipelineConfig


def test_build_pipeline_creates_pipeline():
    config = PipelineConfig(
        start_datetime=datetime(2025, 1, 1, tzinfo=timezone.utc),
        end_datetime=datetime(2025, 1, 2, tzinfo=timezone.utc),
        labels={"team": "pipeline"},
        unknown_unparsed_args=["--foo", "bar"],
        unknown_parsed_args={"opt_a": 123, "opt_b": "xyz"},
    )
    mock_dag = Mock(name="MockDag")
    mock_dag_factory = Mock()
    mock_dag_factory.build_dag.return_value = mock_dag

    factory = PipelineFactory(config=config, dag_factory=mock_dag_factory)
    pipeline = factory.build_pipeline()

    assert isinstance(pipeline, Pipeline)
    assert pipeline._dag is mock_dag
    assert pipeline._unparsed_args == ["--foo", "bar"]
    assert pipeline._options == {"opt_a": 123, "opt_b": "xyz", "labels": {"team": "pipeline"}}

    mock_dag_factory.build_dag.assert_called_once()


def test_build_pipeline_forwards_labels_from_config():
    config = PipelineConfig(
        start_datetime=datetime(2025, 1, 1, tzinfo=timezone.utc),
        end_datetime=datetime(2025, 1, 2, tzinfo=timezone.utc),
        labels={"team": "pipeline", "env": "prod"},
    )
    mock_dag_factory = Mock()
    mock_dag_factory.build_dag.return_value = Mock(name="MockDag")

    pipeline = PipelineFactory(config=config, dag_factory=mock_dag_factory).build_pipeline()

    assert pipeline._options["labels"] == {"team": "pipeline", "env": "prod"}


def test_build_pipeline_raises_when_kwarg_collides_with_config_field():
    config = PipelineConfig(
        start_datetime=datetime(2025, 1, 1, tzinfo=timezone.utc),
        end_datetime=datetime(2025, 1, 2, tzinfo=timezone.utc),
        labels={"team": "pipeline"},
    )
    mock_dag_factory = Mock()

    factory = PipelineFactory(config=config, dag_factory=mock_dag_factory, labels={"env": "x"})

    with pytest.raises(PipelineFactoryError, match="labels"):
        factory.build_pipeline()

    mock_dag_factory.build_dag.assert_not_called()
