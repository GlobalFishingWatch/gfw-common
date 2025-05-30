import pytest
import apache_beam as beam
from apache_beam.testing import util
from apache_beam.testing.test_pipeline import TestPipeline as _TestPipeline

from gfw.common.beam.transforms import SampleAndLogElements


@pytest.fixture
def input_data():
    """Fixture to provide sample input data for the tests."""
    return [
        {"id": 1, "value": "test1"},
        {"id": 2, "value": "test2"},
        {"id": 3, "value": "test3"},
        {"id": 4, "value": "test4"},
    ]


def test_sample_and_log(input_data):
    with _TestPipeline() as p:
        output = (
            p
            | "Create input" >> beam.Create(input_data)
            | "Sample and Log1" >> SampleAndLogElements(sample_size=2)
            | "Sample and Log2" >> SampleAndLogElements(pretty_print=True)
            | "Sample and Log3" >> SampleAndLogElements()
        )

        # Assert that the output matches the expected input (since it's unchanged)
        util.assert_that(output, util.equal_to(input_data))
