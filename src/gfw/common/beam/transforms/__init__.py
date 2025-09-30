"""Package for reusable and well-tested Apache Beam PTransforms.

This package provides a collection of reusable `PTransform` components
designed to simplify and standardize data processing patterns in Apache Beam pipelines.

Each transform in this package is developed with an emphasis on clarity,
testability, and composability — making it easier to write robust and maintainable
pipelines across both batch and streaming modes.

These components aim to serve as building blocks to accelerate development while
maintaining high code quality and reducing duplication.

.. currentmodule:: gfw.common.beam.transforms

Classes
-------

.. autosummary::
   :toctree: ../_autosummary/
   :template: custom-class-template.rst
   :signatures: none

   ApplySlidingWindows
   GroupBy
   ReadAndDecodeFromPubSub
   ReadFromBigQuery
   ReadFromJson
   ReadMatchingAvroFiles
   SampleAndLogElements
   WriteToPartitionedBigQuery
   WriteToJson

Extra classes useful for testing
--------------------------------
.. autosummary::
   :toctree: ../_autosummary/
   :template: custom-class-template.rst
   :signatures: none

   FakeReadFromPubSub
   FakeWriteToBigQuery

"""

from .apply_sliding_windows import ApplySlidingWindows
from .bigquery import FakeWriteToBigQuery, WriteToBigQueryWrapper
from .group_by import GroupBy
from .pubsub import FakeReadFromPubSub, ReadAndDecodeFromPubSub
from .read_from_bigquery import ReadFromBigQuery
from .read_from_json import ReadFromJson
from .read_matching_avro_files import ReadMatchingAvroFiles
from .sample_and_log import SampleAndLogElements
from .write_to_json import WriteToJson


__all__ = [
    "ApplySlidingWindows",
    "FakeReadFromPubSub",
    "FakeWriteToBigQuery",
    "GroupBy",
    "ReadAndDecodeFromPubSub",
    "ReadFromBigQuery",
    "ReadFromJson",
    "ReadMatchingAvroFiles",
    "SampleAndLogElements",
    "WriteToBigQueryWrapper",
    "WriteToJson",
]
