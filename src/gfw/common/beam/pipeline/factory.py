"""Factory for constructing Beam pipelines from configuration and DAG factories.

This module defines the PipelineFactory class, which builds a fully configured
Pipeline instance from a given PipelineConfig and DagFactory.
"""

from typing import Any

from gfw.common.config import PipelineConfig

from .base import Pipeline
from .dag import DagFactory


class PipelineFactoryError(Exception):
    """Raised when PipelineFactory is misconfigured."""

    pass


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

        Raises:
            PipelineFactoryError:
                If a kwarg passed to this factory collides with a :class:`PipelineConfig`
                field of the same name -- that value should be set on the config object
                instead, not duplicated as a kwarg here.
        """
        conflicting = set(self._kwargs) & set(self._config.to_dict())
        if conflicting:
            raise PipelineFactoryError(
                f"kwargs {sorted(conflicting)} collide with PipelineConfig fields of the "
                "same name. Set these values on the config object, not as kwargs to "
                "PipelineFactory."
            )

        return Pipeline(
            name=self._config.name,
            version=self._config.version,
            dag=self._dag_factory.build_dag(),
            pre_hooks=self._config.pre_hooks,
            post_hooks=self._config.post_hooks,
            unparsed_args=self._config.unknown_unparsed_args,
            labels=self._config.labels or None,
            **self._config.unknown_parsed_args,
            **self._kwargs,
        )
