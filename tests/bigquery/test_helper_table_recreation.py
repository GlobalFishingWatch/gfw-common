"""Tests for ``BigQueryHelper.run_query(..., allow_table_recreation=True)``.

BigQuery can't change an existing table's partitioning or clustering, not even with
``WRITE_TRUNCATE``, so ``allow_table_recreation`` lets ``run_query`` drop and recreate the table
through a staging table when the requested layout differs.
"""

import pytest

from google.api_core.exceptions import NotFound
from google.api_core.iam import Policy
from google.cloud import bigquery
from google.cloud.bigquery import WriteDisposition

from gfw.common.bigquery import BigQueryHelper, TableRecreationError
from gfw.common.bigquery.helper import _has_policy_tags


TABLE = "test.dataset.output"
STAGING = "test.dataset.output_staging"

LAYOUT = {"partition_field": "ts", "partition_type": "MONTH", "clustering_fields": ["id"]}


def _table(partition_type=None, partition_field=None, clustering_fields=None, schema=()):
    table = bigquery.Table(TABLE, schema=list(schema))
    if partition_field is not None:
        table.time_partitioning = bigquery.TimePartitioning(
            type_=partition_type, field=partition_field
        )
    table.clustering_fields = clustering_fields
    return table


def _helper(existing_table, dry_run=False, iam_policy=None):
    helper = BigQueryHelper.mocked(project="test", dry_run=dry_run)
    helper.client.project = "test"

    def get_table(table_ref):
        if str(table_ref) == TABLE and existing_table is not None:
            return existing_table
        # Any other table exists once a query has written it.
        if str(table_ref) in _query_destinations(helper):
            return bigquery.Table(str(table_ref))
        raise NotFound(str(table_ref))

    helper.client.get_table.side_effect = get_table
    helper.client.get_iam_policy.return_value = iam_policy or Policy()
    return helper


def _run(helper, **kwargs):
    params = {
        "destination": TABLE,
        "write_disposition": WriteDisposition.WRITE_TRUNCATE,
        "allow_table_recreation": True,
        **LAYOUT,
    }
    return helper.run_query("SELECT 1", **(params | kwargs))


def _query_destinations(helper):
    return [
        str(call.kwargs["job_config"].destination) for call in helper.client.query.call_args_list
    ]


def _assert_not_recreated(helper):
    helper.client.delete_table.assert_not_called()
    helper.client.copy_table.assert_not_called()


@pytest.fixture(autouse=True)
def row_access_policies(mocker):
    return mocker.patch.object(BigQueryHelper, "_list_row_access_policies", return_value=[])


def test_new_table_is_written_directly():
    helper = _helper(existing_table=None)

    _run(helper)

    assert _query_destinations(helper) == [TABLE]
    _assert_not_recreated(helper)


def test_table_with_the_requested_layout_is_written_directly():
    helper = _helper(_table("MONTH", "ts", ["id"]))

    _run(helper)

    assert _query_destinations(helper) == [TABLE]
    _assert_not_recreated(helper)


def test_without_a_requested_layout_the_table_keeps_its_own():
    helper = _helper(_table())

    _run(helper, partition_field=None, clustering_fields=None)

    assert _query_destinations(helper) == [TABLE]
    _assert_not_recreated(helper)


@pytest.mark.parametrize(
    "existing_table",
    [
        _table(),
        _table("DAY", "ts", ["id"]),
        _table("MONTH", "other", ["id"]),
        _table("MONTH", "ts", None),
        _table("MONTH", "ts", ["other"]),
    ],
    ids=["unpartitioned", "partition_type", "partition_field", "no_clustering", "clustering"],
)
def test_table_with_a_different_layout_is_recreated_through_staging(mocker, existing_table):
    helper = _helper(existing_table)

    _run(helper)

    assert _query_destinations(helper) == [STAGING]

    calls = helper.client.method_calls
    table_ref = bigquery.TableReference.from_string(TABLE)
    staging_ref = bigquery.TableReference.from_string(STAGING)
    assert calls.index(mocker.call.delete_table(table_ref)) < calls.index(
        mocker.call.copy_table(staging_ref, table_ref)
    )
    assert helper.client.delete_table.call_args_list == [
        mocker.call(staging_ref, not_found_ok=True),  # Leftover from a failed run.
        mocker.call(table_ref),
        mocker.call(staging_ref),
    ]


def test_clustering_only_layout_on_a_partitioned_table_is_recreated():
    """The requested layout is taken as a whole: BigQuery fails the job in this case too."""
    helper = _helper(_table("MONTH", "ts", ["id"]))

    _run(helper, partition_field=None, clustering_fields=["id"])

    assert _query_destinations(helper) == [STAGING]


def test_recreated_table_gets_schema_description_and_labels_after_the_copy():
    helper = _helper(_table())
    schema = [{"name": "id", "type": "STRING", "description": "The id."}]

    _run(helper, schema=schema, description="A table.", labels={"env": "test"})

    table, fields = helper.client.update_table.call_args.args
    assert str(table.reference) == TABLE
    assert fields == ["schema", "description", "labels"]
    assert table.description == "A table."
    assert table.labels == {"env": "test"}


def test_iam_policy_is_copied_to_the_recreated_table():
    bindings = [{"role": "roles/bigquery.dataViewer", "members": {"user:someone@example.org"}}]
    old_policy = Policy()
    old_policy.bindings = bindings
    helper = _helper(_table(), iam_policy=old_policy)

    _run(helper)

    table_ref, new_policy = helper.client.set_iam_policy.call_args.args
    assert str(table_ref) == TABLE
    assert new_policy.bindings == bindings


def test_without_iam_bindings_no_policy_is_set():
    helper = _helper(_table())

    _run(helper)

    helper.client.set_iam_policy.assert_not_called()


def test_query_failure_leaves_the_table_untouched():
    helper = _helper(_table())
    helper.client.query.return_value.result.side_effect = RuntimeError("query failed")

    with pytest.raises(RuntimeError, match="query failed"):
        _run(helper)

    staging_ref = bigquery.TableReference.from_string(STAGING)
    assert [call.args[0] for call in helper.client.delete_table.call_args_list] == [staging_ref]
    helper.client.copy_table.assert_not_called()


def test_table_with_policy_tags_is_not_recreated():
    tagged = bigquery.SchemaField(
        "id", "STRING", policy_tags=bigquery.PolicyTagList(names=["projects/p/tag"])
    )
    helper = _helper(_table(schema=[tagged]))

    with pytest.raises(TableRecreationError, match="policy tags"):
        _run(helper)

    helper.client.query.assert_not_called()
    _assert_not_recreated(helper)


def test_table_with_nested_policy_tags_is_not_recreated():
    tagged = bigquery.SchemaField(
        "id", "STRING", policy_tags=bigquery.PolicyTagList(names=["projects/p/tag"])
    )
    record = bigquery.SchemaField("record", "RECORD", fields=[tagged])
    helper = _helper(_table(schema=[record]))

    with pytest.raises(TableRecreationError, match="policy tags"):
        _run(helper)


def test_table_with_row_access_policies_is_not_recreated(row_access_policies):
    row_access_policies.return_value = [{"rowAccessPolicyReference": {"policyId": "p"}}]
    helper = _helper(_table())

    with pytest.raises(TableRecreationError, match="row-level access policies"):
        _run(helper)

    helper.client.query.assert_not_called()
    _assert_not_recreated(helper)


def test_dry_run_does_not_touch_any_table(caplog):
    helper = _helper(_table(), dry_run=True)

    _run(helper)

    assert helper.client.query.call_args.kwargs["job_config"].destination is None
    _assert_not_recreated(helper)
    assert f"{TABLE} would be recreated" in caplog.text


@pytest.mark.parametrize(
    "kwargs",
    [
        {"write_disposition": WriteDisposition.WRITE_APPEND},
        {"destination": f"{TABLE}$20240101"},
    ],
    ids=["write_append", "partition_decorator"],
)
def test_ignored_when_it_does_not_apply(caplog, kwargs):
    helper = _helper(_table())

    _run(helper, **kwargs)

    _assert_not_recreated(helper)
    assert "allow_table_recreation only applies to WRITE_TRUNCATE" in caplog.text


def test_ignored_without_destination():
    helper = _helper(_table())

    _run(helper, destination=None)

    helper.client.get_table.assert_not_called()
    _assert_not_recreated(helper)


def test_default_is_not_to_recreate():
    helper = _helper(_table())

    _run(helper, allow_table_recreation=False)

    assert _query_destinations(helper) == [TABLE]
    _assert_not_recreated(helper)


def test_list_row_access_policies_calls_the_rest_api(mocker):
    mocker.stopall()  # Undo the autouse patch: this tests the real method.
    helper = BigQueryHelper.mocked(project="test")
    helper.client._connection = mocker.MagicMock()
    helper.client._connection.api_request.return_value = {"rowAccessPolicies": [{"id": 1}]}

    policies = helper._list_row_access_policies(_table())

    assert policies == [{"id": 1}]
    helper.client._connection.api_request.assert_called_once_with(
        method="GET", path="/projects/test/datasets/dataset/tables/output/rowAccessPolicies"
    )


def test_mocked_helper_can_recreate_a_table(mocker):
    """Pipelines run with mocked clients for development: recreation must work with them too."""
    mocker.stopall()  # Undo the autouse patch: use the mocked client's row access policies.
    helper = BigQueryHelper.mocked(project="test")

    helper.run_query(
        "SELECT 1",
        destination=TABLE,
        write_disposition=WriteDisposition.WRITE_TRUNCATE,
        allow_table_recreation=True,
        **LAYOUT,
    )

    helper.client.copy_table.assert_called_once()


def test_mocked_helper_can_recreate_tables_one_after_another(mocker):
    """Recreating tables one after another works with a mocked helper.

    A mocked client returns the same table for every get_table, and run_query sets the schema on
    it as dicts, so the policy tags check must handle them.
    """
    mocker.stopall()
    helper = BigQueryHelper.mocked(project="test")
    schema = [{"name": "id", "type": "STRING", "description": "The id."}]

    for table in (TABLE, f"{TABLE}_2"):
        helper.run_query(
            "SELECT 1",
            destination=table,
            write_disposition=WriteDisposition.WRITE_TRUNCATE,
            allow_table_recreation=True,
            schema=schema,
            **LAYOUT,
        )

    assert helper.client.copy_table.call_count == 2


@pytest.mark.parametrize(
    "schema, expected",
    [
        ([{"name": "id", "type": "STRING"}], False),
        ([{"name": "id", "type": "STRING", "policyTags": {"names": ["projects/p/tag"]}}], True),
        (
            [
                {
                    "name": "record",
                    "type": "RECORD",
                    "fields": [
                        {"name": "id", "type": "STRING", "policyTags": {"names": ["p/tag"]}}
                    ],
                }
            ],
            True,
        ),
    ],
    ids=["no_tags", "tags", "nested_tags"],
)
def test_policy_tags_are_found_in_a_dict_schema(schema, expected):
    assert _has_policy_tags(schema) is expected
