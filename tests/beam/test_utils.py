from apache_beam import PTransform
from apache_beam.utils.timestamp import Timestamp

from gfw.common.beam.utils import generate_unique_labels, float_to_beam_timestamp


# Dummy transforms for testing
class DummyTransform(PTransform):
    pass


class AnotherTransform(PTransform):
    pass


def test_generate_unique_labels_single_instance():
    transforms = [DummyTransform(), AnotherTransform()]
    labels = generate_unique_labels(transforms)

    assert labels == ["DummyTransform", "AnotherTransform"]


def test_generate_unique_labels_multiple_same_class():
    transforms = [DummyTransform(), DummyTransform()]
    labels = generate_unique_labels(transforms)

    assert labels == ["DummyTransform_1", "DummyTransform_2"]


def test_generate_unique_labels_mixed_classes():
    transforms = [
        DummyTransform(), AnotherTransform(), DummyTransform(), AnotherTransform()
    ]
    labels = generate_unique_labels(transforms)

    assert labels == [
        "DummyTransform_1", "AnotherTransform_1", "DummyTransform_2", "AnotherTransform_2"
    ]


def test_generate_unique_labels_with_prefix():
    transforms = [DummyTransform(), DummyTransform()]
    labels = generate_unique_labels(transforms, prefix="Source_")

    assert labels == ["Source_DummyTransform_1", "Source_DummyTransform_2"]


def test_float_to_beam_timestamp():
    row = {
        "event_time": 1717777777.123,
        "other_time": 123456.789,
        "unchanged_field": "foo"
    }
    fields = ["event_time", "other_time"]

    result = float_to_beam_timestamp(row, fields)

    assert isinstance(result["event_time"], Timestamp)
    assert isinstance(result["other_time"], Timestamp)

    assert result["event_time"] == Timestamp(1717777777.123)
    assert result["other_time"] == Timestamp(123456.789)
    assert result["unchanged_field"] == "foo"
