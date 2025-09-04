Pipeline (:mod:`gfw.common.pipeline`)
=====================================

.. sectionauthor:: Tomás J. Link

.. currentmodule:: gfw.common.pipeline

.. contents::


This package provides base common abstractions for managing data pipelines.
In general, will server as a bridge between a CLI interface and any framework-specific pipeline logic.


.. _tutorial_pipeline:

PipelineConfig (:mod:`gfw.common.pipeline.config`)
--------------------------------------------------

The goal of the :class:`PipelineConfig` class is to provide a standard way of configuring pipelines.
The following code shows an example of how to inherit from the base class to add custom parameters.

.. code-block:: python

    import math
    from dataclasses import dataclass, field
    from datetime import date, timedelta

    from gfw.common.pipeline.config import PipelineConfig


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
