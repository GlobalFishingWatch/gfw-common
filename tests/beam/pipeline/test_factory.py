from unittest.mock import Mock

from gfw.common.beam.pipeline import Pipeline
from gfw.common.beam.pipeline.factory import PipelineFactory
from gfw.common.pipeline.config import PipelineConfig


def test_build_pipeline_creates_pipeline():
    config = PipelineConfig(
        date_range=("2025-01-01", "2025-01-02"),
        version="v1.2.3",
        name="test-pipeline",
        unknown_unparsed_args=["--foo", "bar"],
        unknown_parsed_args={"opt_a": 123, "opt_b": "xyz"},
    )
    mock_dag = Mock(name="MockDag")
    mock_dag_factory = Mock()
    mock_dag_factory.build_dag.return_value = mock_dag

    factory = PipelineFactory(config=config, dag_factory=mock_dag_factory)
    pipeline = factory.build_pipeline()

    assert isinstance(pipeline, Pipeline)
    assert pipeline._name == "test-pipeline"
    assert pipeline._version == "v1.2.3"
    assert pipeline._dag is mock_dag
    assert pipeline._unparsed_args == ["--foo", "bar"]
    assert pipeline._options == {"opt_a": 123, "opt_b": "xyz"}

    mock_dag_factory.build_dag.assert_called_once()
