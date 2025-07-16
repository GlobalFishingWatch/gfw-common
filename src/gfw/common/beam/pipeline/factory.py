"""Factory for constructing Beam pipelines from configuration and DAG factories.

This module defines the PipelineFactory class, which builds a fully configured
Pipeline instance from a given PipelineConfig and DagFactory.
"""

from gfw.common.beam.pipeline import Pipeline
from gfw.common.beam.pipeline.dag.factory import DagFactory
from gfw.common.pipeline.config import PipelineConfig


class PipelineFactory:
    """Builds a Beam Pipeline from a configuration object and a DAG factory.

    Attributes:
        config:
            Configuration for the pipeline, including version and CLI arguments.

        dag_factory:
            Factory that produces the pipeline's DAG.

        name:
            Optional name for the pipeline.
    """

    def __init__(
        self,
        config: PipelineConfig,
        dag_factory: DagFactory,
        name: str = "",
    ) -> None:
        """Initializes the factory with config, DAG factory, and optional name.

        Args:
            config:
                The pipeline configuration.

            dag_factory:
                Factory that provides the pipeline DAG.

            name:
                Optional name for the pipeline.
        """
        self.config = config
        self.dag_factory = dag_factory
        self.name = name

    def build_pipeline(self) -> Pipeline:
        """Constructs and returns a fully configured Pipeline instance.

        Returns:
            A pipeline with DAG, version, name, and CLI arguments.
        """
        return Pipeline(
            name=self.name,
            version=self.config.version,
            dag=self.dag_factory.build_dag(),
            unparsed_args=self.config.unknown_unparsed_args,
            **self.config.unknown_parsed_args,
        )
