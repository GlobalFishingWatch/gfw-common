"""This module encapsulates a generic Apache Beam pipeline with some builtin features.

Main Features:
    - Support for multiple sources and sinks.
    - Built-in integration with Google Cloud Profiler for performance monitoring.
    - Options for custom pipeline configurations via the constructor and the command line.
    - Automatic unique label generation for repeated PTransforms to avoid naming conflicts.
    - Logging of sample elements for inputs and outputs when debug logging is enabled.
"""

import json
import logging

from collections import ChainMap
from functools import cached_property
from pathlib import Path
from typing import Any, List, Optional, Tuple

import apache_beam as beam
import googlecloudprofiler

from apache_beam import PTransform
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.pvalue import PCollection

from .transforms import SampleAndLogElements
from .utils import generate_unique_labels


logger = logging.getLogger(__name__)

DATAFLOW_SDK_CONTAINER_IMAGE = "sdk_container_image"
DATAFLOW_SERVICE_OPTIONS = "dataflow_service_options"
DATAFLOW_ENABLE_PROFILER = "enable_google_cloud_profiler"


class BeamPipeline:
    """A generic Apache Beam pipeline that integrates sources, core transforms, and sinks.

    Args:
        sources:
            A list of PTransforms that read input data.

        core:
            The core PTransform that processes the data.

        sinks:
            A list of PTransforms that write the output data.

        name:
            The name of the pipeline.
            Defaults to an empty string.

        version:
            The version of the pipeline.
            Defaults to "0.1.0".

        unparsed_args:
            A list of unparsed arguments to pass to Beam options.
            Defaults to an empty tuple.

        **options:
            Additional options to pass to the Beam pipeline.

    Attributes:
        parsed_args:
            The parsed arguments from the `unparsed_args` list.

        pipeline_options:
            The merged pipeline options, including parsed args, user options, and defaults.

        output_paths:
            A dictionary mapping sink types to their output paths.
    """

    def __init__(
        self,
        sources: Tuple[PTransform[Any, Any], ...] = (),
        core: Optional[PTransform[Any, Any]] = None,
        sinks: Tuple[PTransform[Any, Any], ...] = (),
        name: str = "",
        version: str = "0.1.0",
        unparsed_args: Tuple[str, ...] = (),
        **options: Any,
    ) -> None:
        """Initializes the BeamPipeline object with sources, core, sinks, and options."""
        self._sources = sources
        self._core = core or beam.Map(lambda x: x)
        self._sinks = sinks
        self._name = name
        self._version = version
        self._unparsed_args = unparsed_args
        self._options = options

    @cached_property
    def parsed_args(self) -> dict[str, Any]:
        """Parses the unparsed arguments using Beam's `PipelineOptions`.

        Returns:
            A dictionary of parsed arguments.
        """
        args = {}
        if len(self._unparsed_args) > 0:
            args = PipelineOptions(self._unparsed_args).get_all_options(drop_default=True)
            logger.debug("Options parsed by Apache Beam: {}".format(args))

        return args

    @cached_property
    def pipeline_options(self) -> PipelineOptions:
        """Resolves pipelines options.

        Combines parsed arguments by beam CLI, constructor parameters and defaults into a single
        `PipelineOptions` object.

        Returns:
            The merged pipeline options.
        """
        options = dict(ChainMap(self.parsed_args, self._options, self.default_options()))

        if DATAFLOW_SDK_CONTAINER_IMAGE not in options:
            options["setup_file"] = "./setup.py"

        logger.info("Beam options to use:")
        logger.info(json.dumps(dict(options), indent=4))

        return PipelineOptions.from_dictionary(options)

    @cached_property
    def output_paths(self) -> List[Path]:
        """Resolves and returns a list of output paths for each sink in the pipeline."""
        paths = []
        for sink in self._sinks:
            path = getattr(sink, "path", None)
            if path is not None:
                paths.append(path)

        return paths

    @cached_property
    def pipeline(self) -> tuple[beam.Pipeline, PCollection[Any]]:
        """Constructs and returns the Apache Beam pipeline object."""
        # Create the Beam pipeline
        p = beam.Pipeline(options=self.pipeline_options)

        source_labels = generate_unique_labels(self._sources)
        sinks_labels = generate_unique_labels(self._sinks)

        # Source transformations
        inputs = [
            p | label >> transform
            for label, transform in zip(source_labels, self._sources, strict=True)
        ]

        if len(inputs) > 1:
            inputs = inputs | "JoinSources" >> beam.Flatten()
        else:
            inputs = inputs[0]

        # Core transformation
        outputs = inputs | self._core

        # Sink transformations
        for label, sink_transform in zip(sinks_labels, self._sinks, strict=True):
            outputs | label >> sink_transform

        if logging.getLogger().level == logging.DEBUG:
            inputs | "Log Inputs" >> SampleAndLogElements(message="Input: {e}", sample_size=1)
            outputs | "Log Outputs" >> SampleAndLogElements(message="Output: {e}", sample_size=1)

        return p, outputs

    def run(self) -> PCollection[Any]:
        """Executes the Apache Beam pipeline."""
        if self._is_profiler_enabled():
            logger.info("Stating Google Cloud Profiler...")
            self._start_profiler()

        p, outputs = self.pipeline
        result = p.run()
        result.wait_until_finish()

        return outputs

    def _is_profiler_enabled(self) -> bool:
        if DATAFLOW_ENABLE_PROFILER in self.pipeline_options.display_data().get(
            DATAFLOW_SERVICE_OPTIONS, []
        ):
            return True

        return False

    def _start_profiler(self) -> None:
        try:
            googlecloudprofiler.start(
                service=self._name,
                service_version=self._version,
                # verbose is the logging level.
                # 0-error, 1-warning, 2-info, 3-debug. It defaults to 0 (error) if not set.
                verbose=2,
            )
        except (ValueError, NotImplementedError) as e:
            logger.warning(f"Profiler couldn't start: {e}")

    @staticmethod
    def default_options() -> dict[str, Any]:
        """Returns the default options for the pipeline."""
        return {
            "runner": "DirectRunner",
            "max_num_workers": 100,
            "machine_type": "e2-standard-2",  # 2 cores - 8GB RAM.
            "disk_size_gb": 25,
            "use_public_ips": False,
            "temp_location": "gs://pipe-temp-us-east-ttl7/dataflow_temp",
            "staging_location": "gs://pipe-temp-us-east-ttl7/dataflow_staging",
            "region": "us-east1",
            "network": "gfw-internal-network",
            "subnetwork": "regions/us-east1/subnetworks/gfw-internal-us-east1",
        }
