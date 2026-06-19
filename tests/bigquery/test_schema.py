import pyarrow as pa
import pytest

from gfw.common.bigquery.schema import bq_schema_to_pyarrow


def test_flat_nullable_fields():
    schema = bq_schema_to_pyarrow(
        [
            {"name": "ssvid", "type": "STRING", "mode": "NULLABLE"},
            {"name": "timestamp", "type": "TIMESTAMP", "mode": "NULLABLE"},
            {"name": "lat", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "count", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "flag", "type": "BOOLEAN", "mode": "NULLABLE"},
        ]
    )
    assert schema.field("ssvid").type == pa.string()
    assert schema.field("timestamp").type == pa.timestamp("s")
    assert schema.field("lat").type == pa.float64()
    assert schema.field("count").type == pa.int64()
    assert schema.field("flag").type == pa.bool_()
    assert all(schema.field(n).nullable for n in ["ssvid", "timestamp", "lat", "count", "flag"])


def test_all_scalar_types():
    schema = bq_schema_to_pyarrow(
        [
            {"name": "a", "type": "STRING"},
            {"name": "b", "type": "BYTES"},
            {"name": "c", "type": "INTEGER"},
            {"name": "d", "type": "INT64"},
            {"name": "e", "type": "FLOAT"},
            {"name": "f", "type": "FLOAT64"},
            {"name": "g", "type": "BOOLEAN"},
            {"name": "h", "type": "BOOL"},
            {"name": "i", "type": "TIMESTAMP"},
            {"name": "j", "type": "DATE"},
            {"name": "k", "type": "DATETIME"},
            {"name": "l", "type": "TIME"},
            {"name": "m", "type": "NUMERIC"},
            {"name": "n", "type": "BIGNUMERIC"},
            {"name": "o", "type": "JSON"},
        ]
    )
    assert schema.field("b").type == pa.binary()
    assert schema.field("c").type == pa.int64()
    assert schema.field("d").type == pa.int64()
    assert schema.field("e").type == pa.float64()
    assert schema.field("f").type == pa.float64()
    assert schema.field("g").type == pa.bool_()
    assert schema.field("h").type == pa.bool_()
    assert schema.field("i").type == pa.timestamp("s")
    assert schema.field("j").type == pa.date32()
    assert schema.field("k").type == pa.timestamp("s")
    assert schema.field("l").type == pa.time32("s")
    assert schema.field("m").type == pa.decimal128(38, 9)
    assert schema.field("n").type == pa.decimal256(76, 38)
    assert schema.field("o").type == pa.string()


def test_required_mode_sets_not_nullable():
    schema = bq_schema_to_pyarrow(
        [
            {"name": "id", "type": "STRING", "mode": "REQUIRED"},
        ]
    )
    assert not schema.field("id").nullable


def test_repeated_mode_wraps_in_list():
    schema = bq_schema_to_pyarrow(
        [
            {"name": "tags", "type": "STRING", "mode": "REPEATED"},
        ]
    )
    assert schema.field("tags").type == pa.list_(pa.string())


def test_record_becomes_struct():
    schema = bq_schema_to_pyarrow(
        [
            {
                "name": "position",
                "type": "RECORD",
                "mode": "NULLABLE",
                "fields": [
                    {"name": "lat", "type": "FLOAT"},
                    {"name": "lon", "type": "FLOAT"},
                ],
            }
        ]
    )
    expected = pa.struct([pa.field("lat", pa.float64()), pa.field("lon", pa.float64())])
    assert schema.field("position").type == expected


def test_repeated_record_becomes_list_of_struct():
    schema = bq_schema_to_pyarrow(
        [
            {
                "name": "waypoints",
                "type": "RECORD",
                "mode": "REPEATED",
                "fields": [
                    {"name": "lat", "type": "FLOAT"},
                    {"name": "lon", "type": "FLOAT"},
                ],
            }
        ]
    )
    inner = pa.struct([pa.field("lat", pa.float64()), pa.field("lon", pa.float64())])
    assert schema.field("waypoints").type == pa.list_(inner)


def test_default_mode_is_nullable():
    schema = bq_schema_to_pyarrow([{"name": "x", "type": "STRING"}])
    assert schema.field("x").nullable


def test_unknown_type_raises():
    with pytest.raises(ValueError, match="Unsupported BigQuery type"):
        bq_schema_to_pyarrow([{"name": "x", "type": "GEOGRAPHY"}])


def test_empty_schema():
    assert bq_schema_to_pyarrow([]) == pa.schema([])
