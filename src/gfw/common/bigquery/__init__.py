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
   Schema
   TableConfig
   TableDescription
"""

from .helper import BigQueryHelper, QueryResult
from .schema import Schema
from .table_config import TableConfig
from .table_description import TableDescription


__all__ = [
    "BigQueryHelper",
    "QueryResult",
    "Schema",
    "TableConfig",
    "TableDescription",
]
