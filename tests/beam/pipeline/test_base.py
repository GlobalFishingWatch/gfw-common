import logging

import apache_beam as beam
import pytest

from apache_beam import PTransform
from apache_beam.options.pipeline_options import (
    GoogleCloudOptions,
    PipelineOptions,
    StandardOptions,
    WorkerOptions,
)
from apache_beam.runners.runner import PipelineState
from apache_beam.testing.util import assert_that, equal_to

from gfw.common.beam.pipeline import LinearDag, Pipeline, PipelineNotDoneError


class DummySource(PTransform):
    def expand(self, pcoll):
        return pcoll | beam.Create(["a", "b", "c"])


class UppercaseTransform(PTransform):
    def expand(self, pcoll):
        return pcoll | "Uppercase" >> beam.Map(str.upper)


class DummySink(PTransform):
    def expand(self, pcoll):
        return pcoll


def dummy_hook(p: Pipeline):
    pass


def test_pipeline_run():
    dag = LinearDag(
        sources=[DummySource()],
        core=UppercaseTransform(),
        sinks=[DummySink()],
    )

    pipeline = Pipeline(
        pre_hooks=[dummy_hook], post_hooks=[dummy_hook], dag=dag, project="test-project"
    )

    assert pipeline.cloud_options.project == "test-project"

    result, outputs = pipeline.run()

    assert result.state == PipelineState.DONE
    assert_that(outputs, equal_to(["A", "B", "C"]))


def test_pipeline_run_wait_until_finish_false(caplog):
    caplog.set_level(logging.INFO)

    dag = LinearDag(
        sources=[DummySource()],
        core=UppercaseTransform(),
        sinks=[DummySink()],
    )

    pipeline = Pipeline(dag=dag, project="test-project")
    pipeline.run(wait_until_finish=False)
    assert any("Not waiting" in rec.message for rec in caplog.records)


def test_pipeline_run_wait_until_finish_true(monkeypatch, caplog):
    dag = LinearDag(
        sources=[DummySource()],
        core=UppercaseTransform(),
        sinks=[DummySink()],
    )

    pipeline = Pipeline(dag=dag, project="test-project")

    monkeypatch.setattr(pipeline, "is_streaming", lambda: True)
    pipeline.run(wait_until_finish=True)
    assert any("Waiting on a streaming pipeline" in rec.message for rec in caplog.records)


class FakeResult:
    def __init__(self, state):
        self.state = state
        self.waited = False

    def wait_until_finish(self):
        self.waited = True


def make_pipeline_with_final_state(monkeypatch, state, post_hooks=(), **kwargs):
    dag = LinearDag(
        sources=[DummySource()],
        core=UppercaseTransform(),
        sinks=[DummySink()],
    )

    pipeline = Pipeline(dag=dag, post_hooks=list(post_hooks), project="test-project", **kwargs)
    monkeypatch.setattr(pipeline.pipeline, "run", lambda: FakeResult(state))
    return pipeline


def test_pipeline_run_runs_post_hooks_if_done(monkeypatch):
    called = []
    pipeline = make_pipeline_with_final_state(
        monkeypatch, PipelineState.DONE, post_hooks=[called.append]
    )

    result, _ = pipeline.run(wait_until_finish=True)

    assert result.state == PipelineState.DONE
    assert called == [pipeline]


@pytest.mark.parametrize(
    "state",
    [
        PipelineState.FAILED,
        PipelineState.CANCELLED,
        PipelineState.DRAINED,
        PipelineState.UPDATED,
    ],
)
def test_pipeline_run_raises_if_not_done(monkeypatch, state):
    called = []
    pipeline = make_pipeline_with_final_state(monkeypatch, state, post_hooks=[called.append])

    with pytest.raises(PipelineNotDoneError) as exc_info:
        pipeline.run(wait_until_finish=True)

    assert exc_info.value.state == str(state)
    assert exc_info.value.job_id is None
    assert str(state) in str(exc_info.value)
    assert called == []


def test_pipeline_error_exposes_the_result_and_the_job_id():
    class DataflowLikeResult(FakeResult):
        def job_id(self):
            return "2026-11-25_00_00_00-123"

    result = DataflowLikeResult(PipelineState.CANCELLED)

    error = PipelineNotDoneError(result)

    assert error.result is result
    assert error.state == str(PipelineState.CANCELLED)
    assert error.job_id == "2026-11-25_00_00_00-123"
    assert str(error) == (
        f"Pipeline did not finish successfully: {PipelineState.CANCELLED} "
        "(job 2026-11-25_00_00_00-123)."
    )


def test_pipeline_run_does_not_raise_if_it_does_not_wait(monkeypatch):
    pipeline = make_pipeline_with_final_state(monkeypatch, PipelineState.RUNNING)

    result, _ = pipeline.run(wait_until_finish=False)

    assert result.state == PipelineState.RUNNING


def test_parsed_args():
    unparsed_args = [
        "--runner=DataflowRunner",
        "--project=my-project",
        "--region=us-east1",
        "--temp_location=gs://my-bucket/temp",
    ]

    pipeline = Pipeline(unparsed_args=unparsed_args)

    # Get the parsed arguments.
    parsed_args = pipeline.parsed_args

    # Check if parsed_args correctly parses the command-line arguments.
    assert parsed_args["runner"] == "DataflowRunner"
    assert parsed_args["project"] == "my-project"
    assert parsed_args["region"] == "us-east1"
    assert parsed_args["temp_location"] == "gs://my-bucket/temp"


def test_pipeline_options():
    # Simulate some command-line arguments.
    mock_unparsed_args = [
        "--runner=DataflowRunner",
        "--project=my-project",
        "--region=us-east1",
        "--temp_location=gs://my-bucket/temp",
    ]

    # Simulate additional user-provided options.
    user_options = {
        "max_num_workers": 50,
        "network": "custom-network",
        "subnetwork": "custom-subnetwork",
        "project": "test-project",
    }

    # Create the pipeline instance with mock args and user options.
    pipeline = Pipeline(
        unparsed_args=mock_unparsed_args,
        **user_options,  # passing user options as additional keyword arguments.
    )

    # Get the pipeline_options.
    pipeline_options = pipeline.pipeline_options

    assert isinstance(pipeline_options, PipelineOptions)

    # Check if the pipeline options include values from mock_unparsed_args.
    assert pipeline_options.view_as(StandardOptions).runner == "DataflowRunner"
    assert pipeline_options.view_as(GoogleCloudOptions).project == "my-project"
    assert pipeline_options.view_as(GoogleCloudOptions).region == "us-east1"
    assert pipeline_options.view_as(GoogleCloudOptions).temp_location == "gs://my-bucket/temp"

    # Check if the user options are correctly passed and merged.
    assert pipeline_options.view_as(WorkerOptions).max_num_workers == 50
    assert pipeline_options.view_as(WorkerOptions).network == "custom-network"
    assert pipeline_options.view_as(WorkerOptions).subnetwork == "custom-subnetwork"

    # Check if the default options are included as well.
    assert pipeline_options.view_as(WorkerOptions).disk_size_gb == 25
    assert pipeline_options.view_as(WorkerOptions).use_public_ips is False

    # Check if 'setup_file' is included when 'DATAFLOW_SDK_CONTAINER_IMAGE' is not set.
    assert "setup_file" in pipeline_options.view_as(PipelineOptions).get_all_options()


def test_pipeline_options_missing_project():
    pipeline = Pipeline()
    with pytest.raises(ValueError, match="You must configure a project"):
        _ = pipeline.pipeline_options


def _pipeline_options(**options):
    return Pipeline(project="my-project", **options).pipeline_options.get_all_options()


@pytest.mark.parametrize(
    "runner",
    [
        "DataflowRunner",
        "dataflow",
        "apache_beam.runners.dataflow.dataflow_runner.DataflowRunner",
    ],
)
def test_setup_file_is_set_on_dataflow_without_container_image(runner):
    assert _pipeline_options(runner=runner)["setup_file"] == "./setup.py"


def test_setup_file_is_not_set_on_dataflow_with_container_image():
    options = _pipeline_options(runner="DataflowRunner", sdk_container_image="image:tag")
    assert options["setup_file"] is None


@pytest.mark.parametrize("runner", ["DirectRunner", "PrismRunner"])
def test_setup_file_is_not_set_on_local_runners(runner):
    """Building the package on every local run only slows it down."""
    assert _pipeline_options(runner=runner)["setup_file"] is None


def test_setup_file_given_is_kept():
    options = _pipeline_options(runner="DataflowRunner", setup_file="./other/setup.py")
    assert options["setup_file"] == "./other/setup.py"


@pytest.mark.parametrize(
    "runner,expected",
    [
        pytest.param("DataflowRunner", True, id="dataflow_runner"),
        pytest.param("dataflow", True, id="dataflow"),
        pytest.param("DirectRunner", False, id="direct_runner"),
    ],
)
def test_is_dataflow_runner(runner, expected):
    pipeline = Pipeline(runner=runner, project="test-project")

    assert pipeline.is_dataflow_runner is expected


@pytest.mark.parametrize(
    "is_dataflow,wait_for_job,expected",
    [
        pytest.param(True, True, True, id="dataflow_wait"),
        pytest.param(True, False, False, id="dataflow_no_wait"),
        pytest.param(False, True, True, id="other_runner_wait"),
        pytest.param(False, False, True, id="other_runner_always_waits"),
    ],
)
def test_pipeline_run_waits_according_to_wait_for_job(
    monkeypatch, is_dataflow, wait_for_job, expected
):
    pipeline = make_pipeline_with_final_state(
        monkeypatch, PipelineState.DONE, wait_for_job=wait_for_job
    )
    monkeypatch.setattr(Pipeline, "is_dataflow_runner", is_dataflow)

    result, _ = pipeline.run()

    assert result.waited is expected


def test_pipeline_run_wait_until_finish_argument_wins_over_wait_for_job(monkeypatch):
    pipeline = make_pipeline_with_final_state(monkeypatch, PipelineState.DONE, wait_for_job=False)
    monkeypatch.setattr(Pipeline, "is_dataflow_runner", True)

    result, _ = pipeline.run(wait_until_finish=True)

    assert result.waited is True
