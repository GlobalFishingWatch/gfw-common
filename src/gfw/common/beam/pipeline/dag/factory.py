"""Factories for building Apache Beam DAGs with BigQuery integration.

This module defines abstract base classes for DAG factories that produce
Apache Beam pipelines, including support for creating BigQuery read/write
clients and helpers with optional mocking capabilities.
"""

from abc import ABC, abstractmethod
from functools import partial
from typing import Callable, Optional, Tuple

from apache_beam import PTransform
from apache_beam.io.gcp import bigquery

from gfw.common.beam.pipeline.config import PipelineConfig
from gfw.common.beam.transforms import ReadFromBigQuery, WriteToBigQueryWrapper
from gfw.common.bigquery.helper import BigQueryHelper

from .base import Dag
from .linear import LinearDag


class DagFactory(ABC):
    """Abstract base class for DAG factories producing :class:`Dag` objects.

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
        return WriteToBigQueryWrapper.get_client_factory(mocked=self.config.mock_bq_clients)

    @property
    def bigquery_helper_factory(self) -> Callable[..., BigQueryHelper]:
        """Returns a factory for BigQueryHelper instances.

        Returns:
            Callable that creates BigQueryHelper instances with
            the appropriate client factory.
        """
        client_factory = BigQueryHelper.get_client_factory(mocked=self.config.mock_bq_clients)
        return partial(BigQueryHelper, client_factory=client_factory)

    @abstractmethod
    def build_dag(self) -> Dag:
        """Builds the DAG.

        Must be implemented in subclasses.

        Returns:
            A tuple of PTransforms representing the DAG components.
        """
        pass


class LinearDagFactory(DagFactory, ABC):
    """Abstract base class for factories producing :class:`LinearDag` objects."""

    @property
    @abstractmethod
    def sources(self) -> Tuple[PTransform, ...]:
        """Returns the source PTransforms`.

        Returns:
            Tuple of PTransforms serving as data sources.
        """
        pass

    @property
    @abstractmethod
    def core(self) -> PTransform:
        """Returns the core PTransform for data processing."""
        pass

    @property
    def side_inputs(self) -> Optional[PTransform]:
        """Returns optional side inputs for the core PTransform."""
        return None

    @property
    @abstractmethod
    def sinks(self) -> Tuple[PTransform, ...]:
        """Returns the sink PTransforms to write data outputs."""
        pass

    def build_dag(self) -> LinearDag:
        """Builds a :class:`LinearDag` instance from the configured pipeline parts.

        Returns:
            A :class:`LinearDag` composed of sources, core, side inputs, and sinks.
        """
        return LinearDag(
            sources=tuple(self.sources),
            core=self.core,
            side_inputs=self.side_inputs,
            sinks=tuple(self.sinks),
        )
