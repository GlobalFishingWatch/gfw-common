Apache Beam (:mod:`gfw.common.beam`)
====================================

.. sectionauthor:: Tomás J. Link

.. currentmodule:: gfw.common.beam

.. contents::


This package provides utilities for managing Apache Beam pipelines.


.. _tutorial_beam:

.. currentmodule:: gfw.common.beam.pipeline

Defining the DAG
----------------

The :class:`Pipeline` class accepts an instance of type :class:`Dag`,
which is responsible for defining the Apache Beam pipeline graph.
Subclasses of :class:`Dag` must implement the abstract method :meth:`Dag.apply`.
A specific implementation provided in this package is :class:`LinearDag`.

Pipeline Configuration
----------------------

The goal of the :class:`PipelineConfig` class is to provide a standard way of configuring pipelines.
The following code shows an example of how to inherit from the base class to add custom parameters.

.. code-block:: python

    import math
    from dataclasses import dataclass, field
    from datetime import date, timedelta

    from gfw.common.beam.pipeline import PipelineConfig


    @dataclass
    class RawGapsConfig(PipelineConfig):
        filter_not_overlapping_and_short: bool = False
        filter_good_seg: bool = False
        open_gaps_start_date: str = "2019-01-01"
        skip_open_gaps: bool = False
        ssvids: tuple = field(default_factory=tuple)
        min_gap_length: float = 6
        n_hours_before: int = 12
        window_period_d: int = None
        eval_last: bool = True
        normalize_output: bool = True
        json_input_messages: str = None
        json_input_open_gaps: str = None
        bq_read_method: str = "EXPORT"
        bq_input_messages: str = None
        bq_input_segments: str = "pipe_ais_v3_published.segs_activity"
        bq_input_open_gaps: str = None
        bq_output_gaps: str = None
        bq_output_gaps_description: bool = False
        bq_write_disposition: str = "WRITE_APPEND"
        mock_bq_clients: bool = False
        save_json: bool = False
        work_dir: str = "workdir"

        name = "pipe-gaps"

        def __post_init__(self) -> None:
            if (
                self.json_input_messages is None
                and (self.bq_input_messages is None or self.bq_input_segments is None)
            ):
                raise ValueError("You need to provide either a JSON inputs or BQ input.")

        @property
        def open_gaps_start(self) -> date:
            return date.fromisoformat(self.open_gaps_start_date)

        @property
        def messages_query_start_date(self) -> date:
            buffer_days = math.ceil(self.n_hours_before / 24)
            return self.start_date - timedelta(days=buffer_days)


Creating a Pipeline
-------------------

The goal of the :class:`Pipeline` class is to provide an easy way of instantiate and manage
Apache Beam pipelines.

.. code-block:: python

    import logging
    import apache_beam as beam

    from gfw.common.beam.pipeline import LinearDag, Pipeline
    logger = logging.getLogger(__name__)


    class SourcePTransform(beam.PTransform):
        def expand(self, p):
            return p | beam.Create(["a", "b", "c"])


    dag = LinearDag(sources=[SourcePTransform()])
    pipeline = Pipeline(dag=dag, project="world-fishing-827")
    result, _ = pipeline.run()
    print(result.state)

Using DAG and Pipeline factories
--------------------------------

The use of :class:`DagFactory` and :class:`PipelineFactory` classes can simplify
and structure to the construction of a pipeline.
To use :class:`DagFactory` you need to create a subclass and implement the abstract method
:meth:`DagFactory.build_dag`.
The subclass :class:`LinearDagFactory` inherits from :class:`DagFactory` and declares abstract methods
that help simplify the instantiation of :class:`LinearDag` class.
To use :class:`PipelineFactory` you need to inject instances of :class:`DagFactory`
and :class:`PipelineConfig` classes.

The following code shows an example of how to use these classes.

.. code-block:: python

    import logging
    from types import SimpleNamespace

    from gfw.common.beam.pipeline.factory import PipelineFactory

    from pipe_gaps.pipeline.config import RawGapsConfig
    from pipe_gaps.pipeline.factory import RawGapsLinearDagFactory
    from pipe_gaps.version import __version__


    logger = logging.getLogger(__name__)


    def run(config: SimpleNamespace) -> None:
        config = RawGapsConfig.from_namespace(config, version=__version__)
        dag_factory = RawGapsLinearDagFactory(config)
        pipeline_factory = PipelineFactory(config, dag_factory=dag_factory)
        pipeline = pipeline_factory.build_pipeline()
        result, _ = pipeline.run()


Collection of PTransforms
-------------------------

There is a series of custom PTransforms that provide extra functionality and sensible defaults
around the built-in Ptransforms of Apache Beam.

.. currentmodule:: gfw.common.beam.transforms

* :class:`ApplySlidingWindows`
* :class:`FakeReadFromPubSub`
* :class:`FakeWriteToBigQuery`
* :class:`GroupBy`
* :class:`ReadAndDecodeFromPubSub`
* :class:`ReadFromBigQuery`
* :class:`ReadMatchingAvroFiles`
* :class:`SampleAndLogElements`
* :class:`WriteToPartitionedBigQuery`
