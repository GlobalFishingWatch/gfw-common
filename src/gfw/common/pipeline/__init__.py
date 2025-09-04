"""Configuration models and utilities for building data pipelines.

The `pipeline` package defines configuration models and utilities
for building and managing data processing pipelines.

This package is typically used by pipeline factories and runners
to extract configuration parameters, validate inputs, and
initialize pipeline components with consistent settings.

.. currentmodule:: gfw.common.pipeline

Classes
-------

.. autosummary::
   :toctree: ../_autosummary/
   :template: custom-class-template.rst

   PipelineConfig


"""

from .config import PipelineConfig


__all__ = ["PipelineConfig"]
