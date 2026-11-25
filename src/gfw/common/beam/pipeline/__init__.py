"""Simplifies Apache Beam pipeline configuration and DAG management.

These components help build configurable, maintainable Beam pipelines with less boilerplate.

.. currentmodule:: gfw.common.beam.pipeline

Classes
-------

.. autosummary::
   :toctree: ../_autosummary/
   :template: custom-class-template.rst
   :signatures: none

   Pipeline
   PipelineFactory
   PipelineFailedError
   Dag
   DagFactory
   LinearDag
   LinearDagFactory

Modules
-------

.. autosummary::
   :toctree: ../_autosummary/
   :template: custom-module-template.rst
   :signatures: none

   hooks

----
"""

from .base import Pipeline, PipelineFailedError
from .dag import Dag, DagFactory, LinearDag, LinearDagFactory
from .factory import PipelineFactory


__all__ = [
    "Dag",
    "DagFactory",
    "LinearDag",
    "LinearDagFactory",
    "Pipeline",
    "PipelineFactory",
    "PipelineFailedError",
]
