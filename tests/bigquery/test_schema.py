import pyarrow as pa
import pytest

from google.cloud.bigquery import SchemaField

from gfw.common.bigquery.schema import Schema


def test_flat_nullable_fields():
    schema = Schema.from_dicts(
        [
            {"name": "ssvid", "type": "STRING", "mode": "NULLABLE"},
            {"name": "timestamp", "type": "TIMESTAMP", "mode": "NULLABLE"},
            {"name": "lat", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "count", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "flag", "type": "BOOLEAN", "mode": "NULLABLE"},
        ]
    ).as_pyarrow()
    assert schema.field("ssvid").type == pa.string()
    assert schema.field("timestamp").type == pa.timestamp("s")
    assert schema.field("lat").type == pa.float64()
    assert schema.field("count").type == pa.int64()
    assert schema.field("flag").type == pa.bool_()
    assert all(schema.field(n).nullable for n in ["ssvid", "timestamp", "lat", "count", "flag"])


def test_all_scalar_types():
    schema = Schema.from_dicts(
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
    ).as_pyarrow()
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
    schema = Schema.from_dicts(
        [
            {"name": "id", "type": "STRING", "mode": "REQUIRED"},
        ]
    ).as_pyarrow()
    assert not schema.field("id").nullable


def test_repeated_mode_wraps_in_list():
    schema = Schema.from_dicts(
        [
            {"name": "tags", "type": "STRING", "mode": "REPEATED"},
        ]
    ).as_pyarrow()
    assert schema.field("tags").type == pa.list_(pa.string())


def test_record_becomes_struct():
    schema = Schema.from_dicts(
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
    ).as_pyarrow()
    expected = pa.struct([pa.field("lat", pa.float64()), pa.field("lon", pa.float64())])
    assert schema.field("position").type == expected


def test_repeated_record_becomes_list_of_struct():
    schema = Schema.from_dicts(
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
    ).as_pyarrow()
    inner = pa.struct([pa.field("lat", pa.float64()), pa.field("lon", pa.float64())])
    assert schema.field("waypoints").type == pa.list_(inner)


def test_default_mode_is_nullable():
    schema = Schema.from_dicts([{"name": "x", "type": "STRING"}]).as_pyarrow()
    assert schema.field("x").nullable


def test_unknown_type_raises():
    with pytest.raises(ValueError, match="Unsupported BigQuery type"):
        Schema.from_dicts([{"name": "x", "type": "GEOGRAPHY"}]).as_pyarrow()


def test_empty_schema():
    assert Schema.from_dicts([]).as_pyarrow() == pa.schema([])


def test_from_json(tmp_path):
    import json

    schema_file = tmp_path / "schema.json"
    schema_file.write_text(
        json.dumps(
            [
                {"name": "ssvid", "type": "STRING", "mode": "NULLABLE"},
                {"name": "lat", "type": "FLOAT", "mode": "REQUIRED"},
            ]
        )
    )
    schema = Schema.from_json(schema_file).as_pyarrow()
    assert schema.field("ssvid").type == pa.string()
    assert not schema.field("lat").nullable


# --- Schema class ---


def _make_fields():
    return [
        SchemaField("ssvid", "STRING", mode="NULLABLE"),
        SchemaField("timestamp", "TIMESTAMP", mode="NULLABLE"),
        SchemaField("lat", "FLOAT", mode="REQUIRED"),
    ]


def test_schema_fields_returns_original():
    fields = _make_fields()
    schema = Schema(fields)
    assert schema.fields is fields


def test_schema_as_dicts():
    fields = _make_fields()
    dicts = Schema(fields).as_dicts()
    assert [d["name"] for d in dicts] == ["ssvid", "timestamp", "lat"]
    assert dicts[0]["type"] == "STRING"
    assert dicts[2]["mode"] == "REQUIRED"


def test_schema_as_pyarrow_flat():
    schema = Schema(_make_fields())
    pa_schema = schema.as_pyarrow()
    assert pa_schema.field("ssvid").type == pa.string()
    assert pa_schema.field("timestamp").type == pa.timestamp("s")
    assert not pa_schema.field("lat").nullable


def test_schema_as_pyarrow_nested_record():
    fields = [
        SchemaField(
            "position",
            "RECORD",
            mode="NULLABLE",
            fields=[
                SchemaField("lat", "FLOAT"),
                SchemaField("lon", "FLOAT"),
            ],
        )
    ]
    pa_schema = Schema(fields).as_pyarrow()
    expected = pa.struct([pa.field("lat", pa.float64()), pa.field("lon", pa.float64())])
    assert pa_schema.field("position").type == expected
