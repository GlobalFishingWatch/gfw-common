"""This module encapsulates a Pipeline class to simplify Apache Beam pipelines configuration."""

import json
import logging

from collections import ChainMap
from functools import cached_property
from typing import Any, Optional, Tuple

import apache_beam as beam
import googlecloudprofiler

from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.pvalue import PCollection
from apache_beam.runners.runner import PipelineResult

from gfw.common.beam.pipeline.dag import Dag, LinearDag


logger = logging.getLogger(__name__)

DATAFLOW_SDK_CONTAINER_IMAGE = "sdk_container_image"
DATAFLOW_SERVICE_OPTIONS = "dataflow_service_options"
DATAFLOW_ENABLE_PROFILER = "enable_google_cloud_profiler"


class Pipeline:
    """This class simplifies Apache Beam pipeline configuration.

    Features:
      - Merges unparsed, parsed, and default options.
      - Supports custom DAG definitions.
      - Enables Google Cloud Profiler integration.
      - Automatically adds './setup.py' when 'sdk_container_image' is not specified.

    You can implement your own Dag object to be injected in the constructor,
    reuse the provided LinearDag, or just override the `apply_dag` method of this class.

    Args:
        name:
            The name of the pipeline.
            Defaults to an empty string.

        version:
            The version of the pipeline.
            Defaults to "0.1.0".

        dag:
            The DAG to be applied to the pipeline.
            Defaults to an empty LinearDag.

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

        pipeline:
            The initialized beam Pipeline object.
    """

    def __init__(
        self,
        name: str = "",
        version: str = "0.1.0",
        dag: Optional[Dag] = None,
        unparsed_args: Tuple[str, ...] = (),
        **options: Any,
    ) -> None:
        """Initializes the BeamPipeline object with sources, core, sinks, and options."""
        self._name = name
        self._version = version
        self._dag = dag or LinearDag()
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
    def pipeline(self) -> beam.Pipeline:
        """Returns the initialized beam.Pipeline object."""
        return beam.Pipeline(options=self.pipeline_options)

    def apply_dag(self) -> PCollection:
        """Applies the provided DAG implementation to the self.pipeline."""
        return self._dag.apply(self.pipeline)

    def run(self) -> tuple[PipelineResult, PCollection]:
        """Executes the Apache Beam pipeline."""
        if self._is_profiler_enabled():
            logger.info("Starting Google Cloud Profiler...")
            self._start_profiler()

        # Apply the DAG and store the main output(s).
        # This can be a PCollection, a tuple, a dict, or None.
        outputs = self.apply_dag()

        result = self.pipeline.run()
        result.wait_until_finish()

        return result, outputs

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
