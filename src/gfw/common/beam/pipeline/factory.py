"""Factory for constructing Beam pipelines from configuration and DAG factories.

This module defines the PipelineFactory class, which builds a fully configured
Pipeline instance from a given PipelineConfig and DagFactory.
"""

from typing import Any

from .base import Pipeline
from .config import PipelineConfig
from .dag import DagFactory


class PipelineFactory:
    """Builds a :class:`Pipeline` instance from :class:`PipelineConfig` and :class:`DagFactory`.

    Args:
        config:
            Configuration for the pipeline.

        dag_factory:
            Factory that produces the pipeline's :class:`~gfw.common.beam.pipeline.Dag`.

        **kwargs:
            Any additional parameters to be passed to :class:`Pipeline` constructor.
    """

    def __init__(
        self,
        config: PipelineConfig,
        dag_factory: DagFactory,
        **kwargs: Any,
    ) -> None:
        """Initializes the factory with config, DAG factory, and optional name."""
        self._config = config
        self._dag_factory = dag_factory
        self._kwargs = kwargs

    def build_pipeline(self) -> Pipeline:
        """Constructs and returns a fully configured Pipeline instance.

        Returns:
            A pipeline with DAG, version, name, and CLI arguments.
        """
        return Pipeline(
            name=self._config.name,
            version=self._config.version,
            dag=self._dag_factory.build_dag(),
            pre_hooks=self._config.pre_hooks,
            post_hooks=self._config.post_hooks,
            unparsed_args=self._config.unknown_unparsed_args,
            **self._config.unknown_parsed_args,
            **self._kwargs,
        )
