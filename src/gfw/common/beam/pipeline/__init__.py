"""Simplifies Apache Beam pipeline configuration and DAG management.

These components help build configurable, maintainable Beam pipelines with less boilerplate.

.. currentmodule:: gfw.common.beam.pipeline

Classes
-------

.. autosummary::
   :toctree: ../_autosummary/
   :template: custom-class-template.rst

   Pipeline
   PipelineFactory
   PipelineConfig
   Dag
   DagFactory
   LinearDag
   LinearDagFactory
"""

from .base import Pipeline
from .config import PipelineConfig
from .dag import Dag, DagFactory, LinearDag, LinearDagFactory
from .factory import PipelineFactory


__all__ = [
    "Dag",
    "DagFactory",
    "LinearDag",
    "LinearDagFactory",
    "Pipeline",
    "PipelineConfig",
    "PipelineFactory",
]
