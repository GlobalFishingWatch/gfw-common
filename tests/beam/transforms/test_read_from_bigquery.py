from datetime import datetime
from typing import NamedTuple

import pytest

from apache_beam.testing.test_pipeline import TestPipeline as _TestPipeline
from apache_beam.testing.util import assert_that, equal_to

from gfw.common.beam.transforms.read_from_bigquery import (
    FakeReadFromBigQuery,
    ReadFromBigQuery,
)


class Message(NamedTuple):
    ssvid: str
    timestamp: float
    distance_from_shore_m: float


def query():
    return "SELECT ssvid, timestamp, distance_from_shore_m FROM dummy_table"


def messages():
    return [
        {
            "ssvid": "1234",
            "timestamp": datetime(2024, 1, 1).timestamp(),
            "distance_from_shore_m": 1,
        },
        {
            "ssvid": "5678",
            "timestamp": datetime(2024, 1, 1).timestamp(),
            "distance_from_shore_m": 2,
        },
    ]


@pytest.mark.parametrize(
    "query, output_type, elements, expected",
    [
        pytest.param(
            query(),
            None,
            messages(),
            messages(),
            id="output_type_dict_with_elements",
        ),
        pytest.param(
            query(),
            Message,
            messages(),
            [Message(**m) for m in messages()],
            id="output_type_namedtuple_with_elements",
        ),
        pytest.param(
            query(),
            None,
            [],
            [],
            id="output_type_dict_no_elements",
        ),
    ],
)
def test_read_from_bigquery_various(query, output_type, elements, expected):
    tr = ReadFromBigQuery(
        query=query,
        output_type=output_type,
        read_from_bigquery_factory=FakeReadFromBigQuery,
        read_from_bigquery_kwargs={"elements": elements},
    )

    with _TestPipeline() as p:
        output = p | tr
        assert_that(output, equal_to(expected))


def test_get_client_factory_returns_fake():
    factory = ReadFromBigQuery.get_client_factory(mocked=True)
    assert factory is FakeReadFromBigQuery


def test_get_client_factory_returns_real():
    factory = ReadFromBigQuery.get_client_factory(mocked=False)
    assert factory.__name__ == "ReadFromBigQuery"
