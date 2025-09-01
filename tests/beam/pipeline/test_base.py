import apache_beam as beam

from apache_beam import PTransform
from apache_beam.options.pipeline_options import (
    GoogleCloudOptions,
    PipelineOptions,
    StandardOptions,
    WorkerOptions,
)
from apache_beam.runners.runner import PipelineState
from apache_beam.testing.util import assert_that, equal_to

from gfw.common.beam.pipeline import LinearDag, Pipeline


class DummySource(PTransform):
    def expand(self, pcoll):
        return pcoll | beam.Create(["a", "b", "c"])


class UppercaseTransform(PTransform):
    def expand(self, pcoll):
        return pcoll | "Uppercase" >> beam.Map(str.upper)


class DummySink(PTransform):
    def expand(self, pcoll):
        return pcoll


def test_pipeline_run():
    dag = LinearDag(
        sources=[DummySource()],
        core=UppercaseTransform(),
        sinks=[DummySink()],
    )

    pipeline = Pipeline(dag=dag, project="test-project")

    assert pipeline.cloud_options.project == "test-project"

    result, outputs = pipeline.run()

    assert result.state == PipelineState.DONE
    assert_that(outputs, equal_to(["A", "B", "C"]))


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


def test_profiler_enabled_runs_profiler(monkeypatch):
    called = {}

    def mock_start(service, service_version, verbose):
        called["called"] = True
        called["service"] = service
        called["version"] = service_version
        called["verbose"] = verbose

    monkeypatch.setattr("gfw.common.beam.pipeline.base.googlecloudprofiler.start", mock_start)

    dag = LinearDag(sources=[DummySource()])

    pipeline = Pipeline(
        name="test-profiler-pipeline",
        version="1.2.3",
        dag=dag,
        runner="DirectRunner",
        dataflow_service_options=["enable_google_cloud_profiler"],
        project="test-project",
    )

    pipeline.run()

    assert called["called"] is True
    assert called["service"] == "test-profiler-pipeline"
    assert called["version"] == "1.2.3"
    assert called["verbose"] == 2
