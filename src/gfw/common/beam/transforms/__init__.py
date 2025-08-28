"""Package for reusable and well-tested Apache Beam PTransforms.

This package provides a collection of reusable `PTransform` components
designed to simplify and standardize data processing patterns in Apache Beam pipelines.

Each transform in this package is developed with an emphasis on clarity,
testability, and composability — making it easier to write robust and maintainable
pipelines across both batch and streaming modes.

Features:
- Well-tested `PTransform` classes for common pipeline operations.
- Consistent interfaces that make unit testing and mocking easier.
- Modular design that encourages reusability across different data domains.

These components aim to serve as building blocks to accelerate development while
maintaining high code quality and reducing duplication.
"""

from .apply_sliding_windows import ApplySlidingWindows
from .bigquery_write_to_partitioned import FakeWriteToBigQuery, WriteToPartitionedBigQuery
from .group_by import GroupBy
from .pubsub import FakeReadFromPubSub, ReadAndDecodeFromPubSub
from .read_from_bigquery import ReadFromBigQuery
from .read_matching_avro_files import ReadMatchingAvroFiles
from .sample_and_log import SampleAndLogElements


__all__ = [
    "ApplySlidingWindows",
    "FakeReadFromPubSub",
    "FakeWriteToBigQuery",
    "GroupBy",
    "ReadAndDecodeFromPubSub",
    "ReadFromBigQuery",
    "ReadMatchingAvroFiles",
    "SampleAndLogElements",
    "WriteToPartitionedBigQuery",
]
