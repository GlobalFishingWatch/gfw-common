"""Utilities for converting BigQuery schemas to other formats."""

from __future__ import annotations

from typing import Any

import pyarrow as pa


BQ_TO_PA: dict[str, pa.DataType] = {
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


def _convert_bq_field(bq_field: dict[str, Any]) -> pa.Field:
    name = bq_field["name"]
    bq_type = bq_field["type"].upper()
    mode = bq_field.get("mode", "NULLABLE").upper()

    if bq_type == "RECORD":
        pa_type: pa.DataType = pa.struct(
            [_convert_bq_field(f) for f in bq_field.get("fields", [])]
        )
    elif bq_type in BQ_TO_PA:
        pa_type = BQ_TO_PA[bq_type]
    else:
        raise ValueError(f"Unsupported BigQuery type: {bq_type!r}")

    if mode == "REPEATED":
        pa_type = pa.list_(pa_type)

    return pa.field(name, pa_type, nullable=mode != "REQUIRED")


def bq_schema_to_pyarrow(bq_schema: list[dict[str, Any]]) -> pa.Schema:
    """Convert a BigQuery JSON schema (list of field dicts) to a PyArrow schema.

    Handles nested ``RECORD`` fields (→ :func:`pyarrow.struct`) and ``REPEATED``
    mode (→ :func:`pyarrow.list_`). Raises :class:`ValueError` for unknown types.

    Args:
        bq_schema:
            List of BigQuery field descriptors, each with at minimum ``name``
            and ``type`` keys. Supports optional ``mode`` (``NULLABLE``,
            ``REQUIRED``, ``REPEATED``) and ``fields`` for ``RECORD`` types.

    Returns:
        The equivalent :class:`pyarrow.Schema`.
    """
    return pa.schema([_convert_bq_field(f) for f in bq_schema])
