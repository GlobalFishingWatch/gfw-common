"""Utilities for converting BigQuery schemas to other formats."""

from __future__ import annotations

import json

from pathlib import Path
from typing import Any, Dict

import pyarrow as pa

from google.cloud import bigquery


_BIGQUERY_TO_PYARROW_TYPE_MAPPING: dict[str, pa.DataType] = {
    "STRING": pa.string(),
    "BYTES": pa.binary(),
    "INTEGER": pa.int64(),
    "INT64": pa.int64(),
    "FLOAT": pa.float64(),
    "FLOAT64": pa.float64(),
    "BOOLEAN": pa.bool_(),
    "BOOL": pa.bool_(),
    "TIMESTAMP": pa.timestamp("s"),
    "DATE": pa.date32(),
    "DATETIME": pa.timestamp("s"),
    "TIME": pa.time32("s"),
    "NUMERIC": pa.decimal128(38, 9),
    "BIGNUMERIC": pa.decimal256(76, 38),
    "JSON": pa.string(),
}


def _convert_schema_field(field: bigquery.SchemaField) -> pa.Field:
    if field.field_type == "RECORD":
        pa_type: pa.DataType = pa.struct([_convert_schema_field(f) for f in field.fields])
    elif field.field_type in _BIGQUERY_TO_PYARROW_TYPE_MAPPING:
        pa_type = _BIGQUERY_TO_PYARROW_TYPE_MAPPING[field.field_type]
    else:
        raise ValueError(f"Unsupported BigQuery type: {field.field_type!r}")

    if field.mode == "REPEATED":
        pa_type = pa.list_(pa_type)

    return pa.field(field.name, pa_type, nullable=field.mode != "REQUIRED")


class Schema:
    """A BigQuery schema with conversion utilities.

    Wraps a list of :class:`~google.cloud.bigquery.SchemaField` objects and provides
    methods to convert to other formats.

    Args:
        fields:
            List of BigQuery schema fields.
    """

    def __init__(self, fields: list[bigquery.SchemaField]) -> None:
        self._fields = fields

    @classmethod
    def from_dicts(cls, dicts: list[dict[str, Any]]) -> Schema:
        """Construct a :class:`Schema` from a list of BigQuery field descriptor dicts.

        Args:
            dicts:
                List of BigQuery field descriptors, each with at minimum ``name``
                and ``type`` keys. Supports optional ``mode`` and ``fields`` for
                nested ``RECORD`` types.

        Returns:
            A :class:`Schema` wrapping the parsed fields.
        """
        return cls([bigquery.SchemaField.from_api_repr(f) for f in dicts])

    @classmethod
    def from_json(cls, path: str | Path) -> Schema:
        """Load a BigQuery JSON schema file and return a :class:`Schema`.

        Args:
            path:
                Path to a JSON file containing a list of BigQuery field descriptors.

        Returns:
            A :class:`Schema` wrapping the parsed fields.
        """
        return cls.from_dicts(json.loads(Path(path).read_text()))

    @property
    def fields(self) -> list[bigquery.SchemaField]:
        """The raw BigQuery :class:`~google.cloud.bigquery.SchemaField` list."""
        return self._fields

    def as_dicts(self) -> list[Dict[str, Any]]:
        """Return the schema as a list of BigQuery field descriptor dicts.

        Useful when passing a schema to interfaces that expect the JSON dict
        format (e.g. :class:`~gfw.common.beam.transforms.bigquery.WriteToBigQueryWrapper`).
        """
        return [f.to_api_repr() for f in self._fields]

    def as_pyarrow(self) -> pa.Schema:
        """Convert to a :class:`pyarrow.Schema`."""
        return pa.schema([_convert_schema_field(f) for f in self._fields])
