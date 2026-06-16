from unittest.mock import MagicMock, patch

import pytest

from google.api_core.exceptions import GoogleAPIError, NotFound
from google.cloud import bigquery

from gfw.common.bigquery.sharded_to_partitioned import DateTables, ShardedToPartitioned, Table


def _make_stp(schema=None):
    return ShardedToPartitioned(
        tables=["proj.ds.table_a", "proj.ds.table_b"],
        target="proj.ds.target",
        execution_project="proj",
        schema=schema or [],
        bq_client_factory=MagicMock(),
    )


# --- Table ---


def test_table_from_fully_qualified():
    t = Table.from_fully_qualified("my-project.my_dataset.my_table")
    assert t.project == "my-project"
    assert t.dataset_id == "my_dataset"
    assert t.table_id == "my_table"


def test_table_str_and_properties():
    t = Table(project="p", dataset_id="d", table_id="t")
    assert str(t) == "p.d.t"
    assert t.fully_qualified == "p.d.t"
    assert t.dataset == "p.d"


def test_table_unpack():
    project, dataset, table = Table(project="p", dataset_id="d", table_id="t")
    assert (project, dataset, table) == ("p", "d", "t")


# --- DateTables ---


def test_date_tables_group_by_month():
    table = Table.from_fully_qualified("p.d.t")
    dates = DateTables(
        {
            "20230101": [table],
            "20230115": [table],
            "20230201": [table],
        }
    )
    months = dates.group_by_month()
    assert set(months.keys()) == {"202301", "202302"}
    assert set(months["202301"].keys()) == {"20230101", "20230115"}
    assert set(months["202302"].keys()) == {"20230201"}


# --- ShardedToPartitioned._build_query ---


def test_build_query_includes_all_tables_for_date():
    schema = [bigquery.SchemaField("ts", "TIMESTAMP")]
    stp = _make_stp(schema=schema)
    table_a = Table.from_fully_qualified("proj.ds.table_a")
    table_b = Table.from_fully_qualified("proj.ds.table_b")
    dates = DateTables({"20230101": [table_a, table_b]})
    table_columns = {
        "proj.ds.table_a": frozenset(["ts"]),
        "proj.ds.table_b": frozenset(["ts"]),
    }

    query = stp._build_query(dates, table_columns)

    assert "proj.ds.table_a_20230101" in query
    assert "proj.ds.table_b_20230101" in query
    assert "UNION ALL" in query


def test_build_query_null_cast_for_missing_column():
    schema = [
        bigquery.SchemaField("ts", "TIMESTAMP"),
        bigquery.SchemaField("msg", "STRING"),
    ]
    stp = _make_stp(schema=schema)
    table_a = Table.from_fully_qualified("proj.ds.table_a")
    table_b = Table.from_fully_qualified("proj.ds.table_b")
    dates = DateTables({"20230101": [table_a, table_b]})
    table_columns = {
        "proj.ds.table_a": frozenset(["ts", "msg"]),
        "proj.ds.table_b": frozenset(["ts"]),  # missing "msg"
    }

    query = stp._build_query(dates, table_columns)

    # only table_b is missing "msg", so exactly one NULL cast should appear
    assert query.count("CAST(NULL AS STRING) AS msg") == 1


# --- ShardedToPartitioned properties ---


def test_schema_from_list():
    schema = [bigquery.SchemaField("ts", "TIMESTAMP")]
    stp = ShardedToPartitioned(
        tables=["proj.ds.t"],
        target="proj.ds.target",
        execution_project="proj",
        schema=schema,
        bq_client_factory=MagicMock(),
    )
    assert stp.schema == schema


def test_partition_type():
    stp = _make_stp()
    assert stp.partition_type == bigquery.TimePartitioningType.DAY


# --- ShardedToPartitioned._ensure_table ---


def test_ensure_table():
    schema = [bigquery.SchemaField("ts", "TIMESTAMP")]
    stp = _make_stp(schema=schema)
    stp._ensure_table()
    _, kwargs = stp.client.create_table.call_args
    assert kwargs["exists_ok"] is True


# --- ShardedToPartitioned._compute_pending ---


def test_compute_pending_not_found_returns_all_months():
    stp = _make_stp()
    stp.client.query.side_effect = NotFound("table not found")
    table = Table.from_fully_qualified("proj.ds.table_a")
    months = {"202301": DateTables({"20230101": [table]})}

    result = stp._compute_pending(months)

    assert result == months


# --- ShardedToPartitioned._process_month ---


def test_process_month():
    schema = [bigquery.SchemaField("ts", "TIMESTAMP")]
    stp = _make_stp(schema=schema)
    table_a = Table.from_fully_qualified("proj.ds.table_a")
    dates = DateTables({"20230101": [table_a]})
    table_columns = {"proj.ds.table_a": frozenset(["ts"])}
    stp.client.query.return_value.total_bytes_processed = 0

    stp._process_month("202301", dates, table_columns, overwrite=False)

    stp.client.query.assert_called_once()


def test_process_month_overwrite_deletes_first():
    schema = [bigquery.SchemaField("ts", "TIMESTAMP")]
    stp = _make_stp(schema=schema)
    table_a = Table.from_fully_qualified("proj.ds.table_a")
    dates = DateTables({"20230101": [table_a]})
    table_columns = {"proj.ds.table_a": frozenset(["ts"])}
    stp.client.query.return_value.total_bytes_processed = 0

    stp._process_month("202301", dates, table_columns, overwrite=True)

    assert stp.client.query.call_count == 2  # delete + insert


def test_process_month_google_api_error_returns_false():
    schema = [bigquery.SchemaField("ts", "TIMESTAMP")]
    stp = _make_stp(schema=schema)
    table_a = Table.from_fully_qualified("proj.ds.table_a")
    dates = DateTables({"20230101": [table_a]})
    table_columns = {"proj.ds.table_a": frozenset(["ts"])}
    stp.client.query.return_value.result.side_effect = GoogleAPIError("BQ error")

    assert stp._process_month("202301", dates, table_columns, overwrite=False) is False


def test_run_raises_after_all_months_attempted_when_some_fail():
    schema = [bigquery.SchemaField("ts", "TIMESTAMP")]
    stp = _make_stp(schema=schema)

    table_a = Table.from_fully_qualified("proj.ds.table_a")
    two_months = DateTables(
        {
            "20230101": [table_a],
            "20230201": [table_a],
        }
    )

    with (
        patch.object(stp, "_discover_dates", return_value=two_months),
        patch.object(stp, "_compute_pending", return_value=two_months.group_by_month()),
        patch.object(stp, "_discover_columns", return_value={}),
        patch.object(stp, "_ensure_table"),
        patch.object(stp, "_process_month", return_value=False) as mock_pm,
    ):
        with pytest.raises(RuntimeError, match="failed month"):
            stp.run()

    # Both months were attempted despite the first one failing
    assert mock_pm.call_count == 2
