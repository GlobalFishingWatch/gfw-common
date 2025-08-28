"""Factories for building Apache Beam DAGs with BigQuery integration.

This module defines abstract base classes for DAG factories that produce
Apache Beam pipelines, including support for creating BigQuery read/write
clients and helpers with optional mocking capabilities.

Classes:
    DagFactory: Abstract base class providing BigQuery client factories and
        requiring a build_dag method.

    LinearDagFactory: Extends DagFactory for linear pipelines composed of
        sources, core, optional side inputs, and sinks.
"""

from abc import ABC, abstractmethod
from functools import partial
from typing import Callable, Optional, Sequence, Tuple

from apache_beam import PTransform
from apache_beam.io.gcp import bigquery

from gfw.common.beam.pipeline.base import Pipeline
from gfw.common.beam.pipeline.dag import Dag, LinearDag
from gfw.common.beam.transforms import ReadFromBigQuery, WriteToPartitionedBigQuery
from gfw.common.bigquery.helper import BigQueryHelper
from gfw.common.pipeline.config import PipelineConfig


class DagFactory(ABC):
    """Abstract base class for DAG factories producing Apache Beam pipelines.

    Provides factory properties for BigQuery read/write clients and helpers.
    """

    def __init__(self, config: PipelineConfig) -> None:
        self.config = config

    @property
    def read_from_bigquery_factory(self) -> Callable[..., bigquery.ReadFromBigQuery]:
        """Returns a factory for ReadFromBigQuery clients.

        Uses mocked clients if configured.
        """
        return ReadFromBigQuery.get_client_factory(mocked=self.config.mock_bq_clients)

    @property
    def write_to_bigquery_factory(self) -> Callable[..., bigquery.WriteToBigQuery]:
        """Returns a factory for WriteToPartitionedBigQuery clients.

        Uses mocked clients if configured.
        """
        return WriteToPartitionedBigQuery.get_client_factory(mocked=self.config.mock_bq_clients)

    @property
    def bigquery_helper_factory(self) -> Callable[..., BigQueryHelper]:
        """Returns a factory for BigQueryHelper instances.

        Returns:
            Callable that creates BigQueryHelper instances with
            the appropriate client factory.
        """
        client_factory = BigQueryHelper.get_client_factory(mocked=self.config.mock_bq_clients)
        return partial(BigQueryHelper, client_factory=client_factory)

    @property
    def pre_hooks(self) -> Sequence[Callable[[Pipeline], None]]:
        """Sequence of callables executed before pipeline run."""
        return []

    @property
    def post_hooks(self) -> Sequence[Callable[[Pipeline], None]]:
        """Sequence of callables executed after pipeline run completes successfully."""
        return []

    @abstractmethod
    def build_dag(self) -> Dag:
        """Builds the DAG.

        Must be implemented in subclasses.

        Returns:
            A tuple of PTransforms representing the DAG components.
        """
        pass


class LinearDagFactory(DagFactory, ABC):
    """Base class for linear DAG factories that assemble sources, core, side inputs, and sinks."""

    @property
    @abstractmethod
    def sources(self) -> Tuple[PTransform, ...]:
        """Returns the source PTransforms for the LinearDag.

        Returns:
            Tuple of PTransforms serving as data sources.
        """
        pass

    @property
    @abstractmethod
    def core(self) -> PTransform:
        """Returns the core PTransform that processes data in the LinearDag.

        Returns:
            The core processing PTransform.
        """
        pass

    @property
    def side_inputs(self) -> Optional[PTransform]:
        """Returns optional side inputs PTransform for the LinearDag.

        Returns:
            A PTransform for side inputs or None if not used.
        """
        return None

    @property
    @abstractmethod
    def sinks(self) -> Tuple[PTransform, ...]:
        """Returns the sink PTransforms for the LinearDag.

        Returns:
            Tuple of PTransforms serving as data sinks.
        """
        pass

    def build_dag(self) -> LinearDag:
        """Builds a LinearDag instance from the configured pipeline parts.

        Returns:
            A LinearDag composed of sources, core, side inputs, and sinks.
        """
        return LinearDag(
            sources=tuple(self.sources),
            core=self.core,
            side_inputs=self.side_inputs,
            sinks=tuple(self.sinks),
        )
