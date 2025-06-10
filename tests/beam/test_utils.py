from apache_beam import PTransform
from gfw.common.beam.utils import generate_unique_labels


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
