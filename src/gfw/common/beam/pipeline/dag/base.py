"""This module contains a base DAG class for Apache Beam pipelines."""

from abc import ABC, abstractmethod

import apache_beam as beam

from apache_beam.pvalue import PCollection


class Dag(ABC):
    """Base class for DAG construction logic."""

    @abstractmethod
    def apply(self, pipeline: beam.Pipeline) -> PCollection:
        """Applies a series of PTransforms to the pipeline."""
