"""General utility functions for working with Apache Beam pipelines."""

from collections import Counter, defaultdict
from typing import Any, Iterable

from apache_beam import PTransform
from apache_beam.utils.timestamp import Timestamp


def generate_unique_labels(transforms: Iterable[PTransform], prefix: str = "") -> list[str]:
    """Generates unique labels for a list of transforms based on their class names.

    If multiple transforms of the same class are present, suffixes (_1, _2, etc.) are added.

    Args:
        transforms:
            An iterable of PTransform instances.

        prefix:
            Optional prefix to include in the label.

    Returns:
        A list of unique string labels, one for each transform.
    """
    class_counts = Counter(type(t).__name__ for t in transforms)
    instance_counters: dict = defaultdict(int)
    labels = []

    for transform in transforms:
        class_name = type(transform).__name__
        if class_counts[class_name] > 1:
            instance_counters[class_name] += 1
            label = f"{prefix}{class_name}_{instance_counters[class_name]}"
        else:
            label = f"{prefix}{class_name}"
        labels.append(label)

    return labels


def float_to_beam_timestamp(row: dict[str, Any], fields: list[str]) -> dict[str, Any]:
    """Converts specified fields in a dictionary from float to Beam Timestamp objects.

    Args:
        row:
            A dictionary containing data with potential float values.

        fields:
            A tuple of field names to be converted to Timestamp.

    Returns:
        The input dictionary with specified fields converted to Timestamp objects.
    """
    new_row = row.copy()

    for field in fields:
        new_row[field] = Timestamp(row[field])

    return new_row
