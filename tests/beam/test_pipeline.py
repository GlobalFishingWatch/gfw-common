import apache_beam as beam
from apache_beam import PTransform
from apache_beam.testing.util import assert_that, equal_to

from apache_beam.options.pipeline_options import (
    PipelineOptions,
    StandardOptions,
    GoogleCloudOptions,
    WorkerOptions
)

from gfw.common.beam.pipeline import BeamPipeline


class DummySource(PTransform):
    def expand(self, pcoll):
        return pcoll | beam.Create(["a", "b", "c"])


class UppercaseTransform(PTransform):
    def expand(self, pcoll):
        return pcoll | "Uppercase" >> beam.Map(str.upper)


class DummySink(PTransform):
    def expand(self, pcoll):
        return pcoll


class DummySinkWithPath(PTransform):
    def __init__(self, path):
        self.path = path

    def expand(self, pcoll):
        return pcoll


def test_beam_pipeline_run():
    pipeline = BeamPipeline(
        sources=[DummySource()],
        core=UppercaseTransform(),
        sinks=[DummySink()],
    )

    outputs = pipeline.run()

    assert_that(outputs, equal_to(["A", "B", "C"]))


def test_parsed_args():
    unparsed_args = [
        "--runner=DataflowRunner",
        "--project=my-project",
        "--region=us-east1",
        "--temp_location=gs://my-bucket/temp"
    ]

    pipeline = BeamPipeline(unparsed_args=unparsed_args)

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
        "--temp_location=gs://my-bucket/temp"
    ]

    # Simulate additional user-provided options.
    user_options = {
        "max_num_workers": 50,
        "network": "custom-network",
        "subnetwork": "custom-subnetwork"
    }

    # Create the pipeline instance with mock args and user options.
    pipeline = BeamPipeline(
        unparsed_args=mock_unparsed_args,
        **user_options  # passing user options as additional keyword arguments.
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


def test_output_paths():
    # Create different sinks with path attributes
    sink1 = DummySinkWithPath(path="gs://my-bucket/output1")
    sink2 = DummySinkWithPath(path="gs://my-bucket/output2")

    # Construct the BeamPipeline with the sinks
    pipeline = BeamPipeline(sinks=[sink1, sink2])

    # Check if the output paths match the expected values
    assert pipeline.output_paths == [
        "gs://my-bucket/output1",  # Sink1's path
        "gs://my-bucket/output2"   # Sink2's path
    ]


def test_output_paths_empty_sinks():
    pipeline = BeamPipeline()
    assert pipeline.output_paths == []


def test_pipeline_construction_with_multiple_sources():
    # Create two dummy sources
    source1 = DummySource()
    source2 = DummySource()

    # Create a dummy sink
    sink1 = DummySinkWithPath(path="gs://my-bucket/output1")
    sink2 = DummySinkWithPath(path="gs://my-bucket/output2")

    # Construct the BeamPipeline with the sources and a core transform
    pipeline = BeamPipeline(sources=[source1, source2], sinks=[sink1, sink2])

    # Retrieve the pipeline object using the pipeline property
    p, _ = pipeline.pipeline


def test_pipeline_construction_with_debug():
    import logging
    old_level = logging.getLogger().level
    logging.getLogger().setLevel(logging.DEBUG)
    # ... run test

    pipeline = BeamPipeline(sources=[DummySource()])

    # Retrieve the pipeline object using the pipeline property
    p, _ = pipeline.pipeline

    logging.getLogger().setLevel(old_level)


def test_profiler_enabled_runs_profiler(monkeypatch):
    called = {}

    def mock_start(service, service_version, verbose):
        called["called"] = True
        called["service"] = service
        called["version"] = service_version
        called["verbose"] = verbose

    monkeypatch.setattr("gfw.common.beam.pipeline.googlecloudprofiler.start", mock_start)

    pipeline = BeamPipeline(
        sources=[DummySource()],
        name="test-profiler-pipeline",
        version="1.2.3",
        runner="DirectRunner",
        dataflow_service_options=["enable_google_cloud_profiler"]
    )

    pipeline.run()

    assert called["called"] is True
    assert called["service"] == "test-profiler-pipeline"
    assert called["version"] == "1.2.3"
    assert called["verbose"] == 2
