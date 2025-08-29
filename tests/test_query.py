import datetime

from typing import NamedTuple

import pytest

from jinja2 import DictLoader, Environment

from gfw.common.query import Query


# A dummy template that uses the expand_schema mechanism
TEMPLATES = {
    "dummy.sql": """
    SELECT {{ fields }}
    FROM `{{ source_table }}`
    """
}


class DummySchema(NamedTuple):
    vessel_id: int
    timestamp: datetime.datetime
    score: float


class DummyQuery(Query):
    @property
    def output_type(self):
        return DummySchema

    @property
    def template_filename(self):
        return "dummy.sql"

    @property
    def template_vars(self):
        return {"source_table": "my_table", "fields": self.get_select_fields()}


@pytest.fixture
def query():
    env = Environment(loader=DictLoader(TEMPLATES))
    return DummyQuery().with_env(env)


def test_expand_schema(query):
    select_clause = query.get_select_fields()
    # NamedTuple fields should expand into SQL SELECT expressions
    assert "vessel_id" in select_clause
    assert "score" in select_clause
    # datetime field should be converted
    assert "UNIX_MICROS(timestamp)" in select_clause


def test_render_query(query):
    sql = query.render()
    expected = """
    SELECT vessel_id,
           CAST(UNIX_MICROS(timestamp) AS FLOAT64) / 1000000 AS timestamp,
           score
    FROM `my_table`
    """
    assert query.format(sql) == query.format(expected)
    assert query.format(sql) == query.render(formatted=True)


def test_format_sql(query):
    sql = "SELECT 1  FROM   table"
    formatted = query.format(sql)
    # Should normalize whitespace
    assert formatted == "SELECT 1\nFROM TABLE"


def test_requires_output_type():
    class BadQuery(Query):
        template_filename = "bad.sql"

    with pytest.raises(TypeError):
        BadQuery()


def test_top_level_package(query):
    assert query.top_level_package == "tests"


def test_default_jinja_env():
    query = DummyQuery()
    env = query.jinja_env

    assert isinstance(env, Environment)


def test_sql_strings():
    input_strings = ["hello", "world", "O'Reilly"]
    expected = ["'hello'", "'world'", "'O'Reilly'"]

    result = Query.sql_strings(input_strings)

    assert result == expected
