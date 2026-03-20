"""This module encapsulates a Pipeline class to simplify Apache Beam pipelines configuration."""

import json
import logging

from collections import ChainMap
from functools import cached_property
from typing import Any, Callable, Optional, Sequence, Tuple

import apache_beam as beam

from apache_beam.options.pipeline_options import (
    GoogleCloudOptions,
    PipelineOptions,
    StandardOptions,
)
from apache_beam.pvalue import PCollection
from apache_beam.runners.runner import PipelineResult, PipelineState

from .dag.base import Dag
from .dag.linear import LinearDag


logger = logging.getLogger(__name__)

DATAFLOW_SDK_CONTAINER_IMAGE = "sdk_container_image"
DATAFLOW_SERVICE_OPTIONS = "dataflow_service_options"


class Pipeline:
    """Wrapper around :class:`beam.Pipeline` with extended functionality.

    Features:
        - Merges unparsed, parsed, and default options.
        - Supports custom DAG definitions.
        - Enables Google Cloud Profiler integration.
        - Automatically adds ``./setup.py`` when ``sdk_container_image`` is not specified.

    You can implement your own Dag object to be injected in the constructor,
    reuse the provided :class:`LinearDag`,
    or just override the :meth:`apply_dag` method of this class.

    Args:
        name:
            The name of the pipeline.
            Defaults to an empty string.

        version:
            The version of the pipeline.
            Defaults to ``0.1.0``.

        dag:
            The DAG to be applied to the pipeline.
            Defaults to an empty LinearDag.

        pre_hooks:
            Sequence of callables executed before pipeline run.
            Each callable receives the pipeline instance as its only argument.

        post_hooks:
            Sequence of callables executed after pipeline run completes successfully.
            Each callable receives the pipeline instance as its only argument.

        unparsed_args:
            A list of unparsed arguments to pass to Beam options.
            Defaults to an empty tuple.


        **options:
            Additional options to pass to the Beam pipeline.

    Attributes:
        parsed_args:
            The parsed arguments from the ``unparsed_args`` list.

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
        pre_hooks: Sequence[Callable[..., None]] = (),
        post_hooks: Sequence[Callable[..., None]] = (),
        unparsed_args: Tuple[str, ...] = (),
        **options: Any,
    ) -> None:
        """Initializes the BeamPipeline object with sources, core, sinks, and options."""
        self._name = name
        self._version = version
        self._dag = dag or LinearDag()
        self._pre_hooks = pre_hooks
        self._post_hooks = post_hooks
        self._unparsed_args = unparsed_args
        self._options = options

    @cached_property
    def parsed_args(self) -> dict[str, Any]:
        """Parses the unparsed arguments using Beam's :class:`PipelineOptions`.

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
        """Resolves pipeline options.

        Combines parsed arguments by beam CLI, constructor parameters and defaults into a single
        :class:`PipelineOptions` object.

        Returns:
            The merged pipeline options.
        """
        options = dict(ChainMap(self.parsed_args, self._options, self.default_options()))

        if options.get("project") is None:
            raise ValueError("You must configure a project to run a Beam pipeline.")

        if DATAFLOW_SDK_CONTAINER_IMAGE not in options:
            options["setup_file"] = "./setup.py"

        logger.info("Beam options to use:")
        logger.info(json.dumps(dict(options), indent=4))

        return PipelineOptions.from_dictionary(options)

    @cached_property
    def cloud_options(self) -> GoogleCloudOptions:
        """Returns the :class:`GoogleCloudOptions` view of the :class:`PipelineOptions`."""
        return self.pipeline_options.view_as(GoogleCloudOptions)

    @cached_property
    def pipeline(self) -> beam.Pipeline:
        """Returns the initialized :class:`beam.Pipeline` object."""
        return beam.Pipeline(options=self.pipeline_options)

    @cached_property
    def is_streaming(self) -> bool:
        """Returns whether the pipeline is running in streaming mode."""
        return self.pipeline_options.view_as(StandardOptions).streaming

    def apply_dag(self) -> PCollection:
        """Applies the provided DAG implementation to the self.pipeline."""
        return self._dag.apply(self.pipeline)

    def run(self, wait_until_finish: Optional[bool] = None) -> tuple[PipelineResult, PCollection]:
        """Executes the Apache Beam pipeline.

        Runs the configured DAG and returns the pipeline result along with its main output(s).

        The execution can be either blocking or non-blocking depending on the pipeline type
        and the ``wait_until_finish`` parameter:

        - If ``wait_until_finish`` is ``None`` (default):
          - batch pipelines block until completion
          - streaming pipelines return immediately after submission

        - If ``wait_until_finish`` is explicitly set:
          - ``True`` blocks until the pipeline finishes
          - ``False`` returns immediately

        Note that waiting on a streaming pipeline will block indefinitely unless the job
        is externally cancelled.

        Post-hooks are only executed when the pipeline finishes successfully (i.e., in
        blocking mode and when the final state is ``DONE``).

        Args:
            wait_until_finish:
                Whether to wait for the pipeline execution to complete.
                If ``None``, the behavior is inferred from whether the pipeline is
                running in streaming mode.

        Returns:
            A tuple containing:
                - The :class:`PipelineResult` of the executed pipeline.
                - The main output(s) produced by the DAG, which may be a
                  :class:`PCollection`, a tuple, a dict, or ``None``.
        """
        for hook in self._pre_hooks:
            hook(self)

        outputs = self.apply_dag()
        result = self.pipeline.run()

        if wait_until_finish is None:
            wait_until_finish = not self.is_streaming

        if wait_until_finish and self.is_streaming:
            logger.warning(
                "Waiting on a streaming pipeline. "
                "This will block indefinitely unless the job is cancelled."
            )

        if wait_until_finish:
            result.wait_until_finish()

            if result.state == PipelineState.DONE:
                for hook in self._post_hooks:
                    hook(self)
            else:
                logger.warning("Pipeline did not finish successfully; skipping post-hooks.")
        else:
            logger.info("Not waiting for pipeline to finish.")

        return result, outputs

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
            "subnetwork": "regions/us-east1/subnetworks/gfw-internal-us-east1",
            "network": "gfw-internal-network",
        }
