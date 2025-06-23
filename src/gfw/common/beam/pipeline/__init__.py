"""Simplifies Apache Beam pipeline configuration and DAG management.

This package provides:

- Pipeline: A class that streamlines Beam pipeline setup, option merging,
  DAG application, and optional Google Cloud Profiler integration.

- Dag: Base class for defining pipeline Directed Acyclic Graphs (DAGs).

- LinearDag: A simple linear DAG implementation.

These components help build configurable, maintainable Beam pipelines with less boilerplate.
"""

from .base import Pipeline
from .dag import Dag, LinearDag


__all__ = [
    "Dag",
    "LinearDag",
    "Pipeline",
]
