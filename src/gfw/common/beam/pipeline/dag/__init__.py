"""Simplifies Apache Beam pipeline configuration by providing DAG abstractions.

This package exposes:

- Dag: Base DAG class for defining pipeline workflows.
- LinearDag: A simple linear DAG implementation.

These classes support building customizable pipeline graphs that integrate
seamlessly with the Pipeline class for streamlined Beam pipeline setup and execution.
"""

from .base import Dag
from .linear import LinearDag


__all__ = [
    "Dag",
    "LinearDag",
]
