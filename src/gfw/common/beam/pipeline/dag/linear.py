"""This module contains a linear DAG implementation for Apache Beam pipelines."""

import logging

from functools import cached_property
from pathlib import Path
from typing import Any, List, Optional, Tuple, Union

import apache_beam as beam

from apache_beam import PTransform
from apache_beam.pvalue import PCollection

from gfw.common.beam.transforms import SampleAndLogElements

from .base import Dag


class LinearDag(Dag):
    """A linear DAG implementation for Apache Beam pipelines.

    This DAG:
        1. Applies multiple sources PTransforms and merges outputs into a single PCollection.
        2. Applies a single core PTransform, with an optional side inputs PTransform.
        3. Applies multiple sinks PTransforms.

    Args:
        sources:
            A list of PTransforms that read input data.

        core:
            The core PTransform that processes the data.

        side_inputs:
            A PTransform used to read side inputs that will be injected into the core transform.

        sinks:
            A list of PTransforms that write the output data.

    Attributes:
        output_paths:
            A list of output paths for each sink, if they contain the path attribute.

    """

    def __init__(
        self,
        sources: Tuple[PTransform[Any, Any], ...] = (),
        core: Optional[PTransform[Any, Any]] = None,
        side_inputs: Optional[PTransform[Any, Any]] = None,
        sinks: Tuple[PTransform[Any, Any], ...] = (),
    ) -> None:
        """Initializes the :class:`LinearDag` object."""
        self._sources = sources
        self._core = core or beam.Map(lambda x: x)
        self._sinks = sinks
        self._side_inputs = side_inputs

    def apply(self, p: beam.Pipeline) -> PCollection:
        """Applies the linear DAG implementation to an Apache Beam pipeline."""
        if self._side_inputs is not None:
            side_inputs = p | self._side_inputs
            self._core.set_side_inputs(side_inputs)

        # Source transformations
        inputs = [p | transform for transform in self._sources]

        if len(inputs) > 1:
            inputs = inputs | "JoinSources" >> beam.Flatten()
        else:
            inputs = inputs[0]

        # Core transformation
        outputs = inputs | self._core

        # Sink transformations
        for transform in self._sinks:
            outputs | transform

        if logging.getLogger().level == logging.DEBUG:
            inputs | "Log Inputs" >> SampleAndLogElements(message="Input: {e}", sample_size=1)
            outputs | "Log Outputs" >> SampleAndLogElements(message="Output: {e}", sample_size=1)

        return outputs

    @cached_property
    def output_paths(self) -> List[Union[Path, str]]:
        """Resolves and returns a list of output paths for each sink in the pipeline."""
        paths = []
        for sink in self._sinks:
            paths.append(getattr(sink, "path", "Unknown."))

        return paths
