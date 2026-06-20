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
   HivePartitionConfig
   ReadAndDecodeFromPubSub
   ReadFromBigQuery
   ReadFromJson
   ReadMatchingAvroFiles
   SampleAndLogElements
   WriteToBigQueryWrapper
   WriteToJson
   WritePartitionedParquet

Extra classes useful for testing
--------------------------------
.. autosummary::
   :toctree: ../_autosummary/
   :template: custom-class-template.rst
   :signatures: none

   FakeParquetSink
   FakeReadFromBigQuery
   FakeReadFromPubSub
   FakeWriteToBigQuery
   ParquetSink

"""

from .avro import ReadMatchingAvroFiles
from .bigquery import (
    FakeReadFromBigQuery,
    FakeWriteToBigQuery,
    ReadFromBigQuery,
    WriteToBigQueryWrapper,
)
from .core import GroupBy, SampleAndLogElements
from .json import ReadFromJson, WriteToJson
from .parquet import FakeParquetSink, HivePartitionConfig, ParquetSink, WritePartitionedParquet
from .pubsub import FakeReadFromPubSub, ReadAndDecodeFromPubSub
from .windows import ApplySlidingWindows


__all__ = [
    "ApplySlidingWindows",
    "FakeParquetSink",
    "FakeReadFromBigQuery",
    "FakeReadFromPubSub",
    "FakeWriteToBigQuery",
    "GroupBy",
    "HivePartitionConfig",
    "ParquetSink",
    "ReadAndDecodeFromPubSub",
    "ReadFromBigQuery",
    "ReadFromJson",
    "ReadMatchingAvroFiles",
    "SampleAndLogElements",
    "WritePartitionedParquet",
    "WriteToBigQueryWrapper",
    "WriteToJson",
]
