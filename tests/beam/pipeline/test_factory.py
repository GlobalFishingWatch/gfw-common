from unittest.mock import Mock

from gfw.common.beam.pipeline import Pipeline
from gfw.common.beam.pipeline.factory import PipelineFactory


class DummyConfig:
    def __init__(self):
        self.version = "v1.2.3"
        self.unknown_unparsed_args = ["--foo", "bar"]
        self.unknown_parsed_args = {"opt_a": 123, "opt_b": "xyz"}


def test_build_pipeline_creates_pipeline():
    config = DummyConfig()
    mock_dag = Mock(name="MockDag")
    mock_dag_factory = Mock()
    mock_dag_factory.build_dag.return_value = mock_dag

    factory = PipelineFactory(config=config, dag_factory=mock_dag_factory, name="test-pipeline")
    pipeline = factory.build_pipeline()

    assert isinstance(pipeline, Pipeline)
    assert pipeline._name == "test-pipeline"
    assert pipeline._version == "v1.2.3"
    assert pipeline._dag is mock_dag
    assert pipeline._unparsed_args == ["--foo", "bar"]
    assert pipeline._options == {"opt_a": 123, "opt_b": "xyz"}

    mock_dag_factory.build_dag.assert_called_once()
