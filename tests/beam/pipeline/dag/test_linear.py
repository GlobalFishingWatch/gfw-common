import apache_beam as beam

from apache_beam import PTransform
from apache_beam.pvalue import PCollection

from gfw.common.beam.pipeline import LinearDag, Pipeline


class DummySource(PTransform):
    def expand(self, pcoll):
        return pcoll | beam.Create(["a", "b", "c"])


class DummySinkWithPath(PTransform):
    def __init__(self, path):
        self.path = path

    def expand(self, pcoll):
        return pcoll


class DummyCore(PTransform):
    def set_side_inputs(self, side_inputs: PCollection):
        self._side_inputs = side_inputs

    def expand(self, pcoll):
        return pcoll


def test_apply_with_multiple_sources():
    # Create two dummy sources
    source1 = "Source1" >> DummySource()
    source2 = "Source2" >> DummySource()

    # Create a dummy sink
    sink1 = "DummySinkWithPath1" >> DummySinkWithPath(path="gs://my-bucket/output1")
    sink2 = "DummySinkWithPath2" >> DummySinkWithPath(path="gs://my-bucket/output2")

    dag = LinearDag(sources=[source1, source2], sinks=[sink1, sink2])

    # Construct the Pipeline with the sources and a core transform
    pipeline = Pipeline(dag=dag, project="test-project")

    # Apply the DAG
    pipeline.apply_dag()


def test_apply_with_side_inputs():
    # Create two dummy sources
    source = "Source" >> DummySource()
    side_inputs = "SideInputs" >> DummySource()
    core = "Core" >> DummyCore()

    # Create a dummy sink
    sink = "DummySinkWithPath1" >> DummySinkWithPath(path="gs://my-bucket/output1")

    dag = LinearDag(sources=[source], core=core, side_inputs=side_inputs, sinks=[sink])

    # Construct the Pipeline with the sources and a core transform
    pipeline = Pipeline(dag=dag, project="test-project")

    # Apply the DAG
    pipeline.apply_dag()


def test_apply_with_debug():
    import logging

    old_level = logging.getLogger().level
    logging.getLogger().setLevel(logging.DEBUG)
    # ... run test

    dag = LinearDag(sources=[DummySource()])
    pipeline = Pipeline(dag=dag, project="test-project")

    # Retrieve the pipeline object using the pipeline property
    pipeline.apply_dag()

    logging.getLogger().setLevel(old_level)


def test_output_paths():
    # Create different sinks with path attributes
    sink1 = DummySinkWithPath(path="gs://my-bucket/output1")
    sink2 = DummySinkWithPath(path="gs://my-bucket/output2")

    dag = LinearDag(sinks=[sink1, sink2])

    # Check if the output paths match the expected values
    assert dag.output_paths == [
        "gs://my-bucket/output1",  # Sink1's path
        "gs://my-bucket/output2",  # Sink2's path
    ]


def test_output_paths_empty_sinks():
    dag = LinearDag()
    assert dag.output_paths == []
