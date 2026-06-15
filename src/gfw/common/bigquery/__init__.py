"""BigQuery utilities and configuration classes.

.. currentmodule:: gfw.common.bigquery

Classes
-------

.. autosummary::
   :toctree: ../_autosummary/
   :template: custom-class-template.rst
   :signatures: none

   BigQueryHelper
   QueryResult
   TableConfig
   TableDescription
"""

from .helper import BigQueryHelper, QueryResult
from .sharded_to_partitioned import ShardedToPartitioned, Table
from .table_config import TableConfig
from .table_description import TableDescription


__all__ = [
    "BigQueryHelper",
    "QueryResult",
    "ShardedToPartitioned",
    "Table",
    "TableConfig",
    "TableDescription",
]
