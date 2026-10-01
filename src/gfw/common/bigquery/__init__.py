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
   SingleSourceViewConfig
   TableConfig
   TableDescription
   ViewConfig
"""

from .helper import BigQueryHelper, QueryResult
from .schema import Schema
from .table_config import TableConfig
from .table_description import TableDescription
from .view_config import SingleSourceViewConfig, ViewConfig


__all__ = [
    "BigQueryHelper",
    "QueryResult",
    "Schema",
    "SingleSourceViewConfig",
    "TableConfig",
    "TableDescription",
    "ViewConfig",
]
