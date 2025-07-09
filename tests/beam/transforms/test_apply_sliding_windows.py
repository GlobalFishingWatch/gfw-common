from unittest.mock import patch

import apache_beam as beam

from apache_beam.testing.test_pipeline import TestPipeline as _TestPipeline
from apache_beam.testing.util import assert_that, equal_to
from apache_beam.transforms.window import SlidingWindows

from gfw.common.beam.transforms import ApplySlidingWindows


def test_sliding_windows_without_timestamp_assignment():
    elements = [
        {"id": 1, "value": 10},
        {"id": 2, "value": 20},
    ]

    with _TestPipeline() as p:
        pcoll = p | beam.Create(elements)

        # Apply sliding windows without assigning timestamps
        windowed = pcoll | ApplySlidingWindows(period=5, offset=0, assign_timestamps=False)

        # Just verify elements pass through unchanged (windowing itself doesn't filter)
        assert_that(windowed, equal_to(elements))


def test_sliding_windows_with_timestamp_assignment():
    elements = [
        {"id": 1, "timestamp": 100, "value": 10},
        {"id": 2, "timestamp": 105, "value": 20},
    ]

    with _TestPipeline() as p:
        pcoll = p | beam.Create(elements)

        windowed = pcoll | ApplySlidingWindows(period=5, offset=0, assign_timestamps=True)

        # Check that output contains the original elements (windowing does not modify data)
        assert_that(windowed, equal_to(elements))


def test_sliding_windows_calls_slidingwindows_correctly():
    period = 5
    offset = 1
    assign_timestamps = False

    transform = ApplySlidingWindows(
        period=period, offset=offset, assign_timestamps=assign_timestamps
    )

    # Dummy PCollection that supports | operator
    class DummyPColl:
        def __or__(self, other):
            # When WindowInto is applied, return a marker string
            return "windowed_pcoll"

    dummy_pcoll = DummyPColl()

    with patch("apache_beam.WindowInto") as mock_window_into:
        result = transform.expand(dummy_pcoll)

        mock_window_into.assert_called_once()
        sliding_window_instance = mock_window_into.call_args[0][0]

        assert isinstance(sliding_window_instance, SlidingWindows)
        assert sliding_window_instance.period == period
        assert sliding_window_instance.offset == offset
        assert sliding_window_instance.size == period + offset
        assert result == "windowed_pcoll"
