import json

import apache_beam as beam
import pytest

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


@pytest.mark.skip("Failing since apache-beam==2.68")
@pytest.mark.parametrize(
    "sample_size,pretty_print",
    [
        pytest.param(1, False, id="sample-size-1"),
        pytest.param(2, False, id="sample-size-2"),
        pytest.param(None, False, id="sample-size-None"),
        pytest.param(1, True, id="pretty-print"),
    ],
)
def test_sample_and_log_with_sample_size(
    input_data,
    caplog,
    sample_size,
    pretty_print,
):
    message = "My great log: {e}"

    with caplog.at_level("DEBUG", logger="gfw.common.beam.transforms.sample_and_log"):
        with _TestPipeline() as p:
            output = (
                p
                | "Create input" >> beam.Create(input_data)
                | SampleAndLogElements(
                    sample_size=sample_size,
                    pretty_print=pretty_print,
                    message=message,
                    window_size=1,
                )
            )

            # Assert that the output matches the expected input (since it's unchanged)
            util.assert_that(output, util.equal_to(input_data))

    # Assert that captures logs correct amount of messages
    expected_size = sample_size
    if expected_size is None:
        expected_size = len(input_data)

    assert len(caplog.records) == expected_size

    # Assert that captures logs contain expected messages
    formatted_elements = [json.dumps(e, indent=4) if pretty_print else e for e in input_data]

    possible_logs = [message.format(e=e) for e in formatted_elements]

    log_messages = "\n".join(record.message for record in caplog.records)
    count = sum(log in log_messages for log in possible_logs)
    assert count == expected_size
