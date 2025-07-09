import types

import pytest

from gfw.common import iterables


CASES = [
    {"lst": [1, 2, 3, 4, 5], "n": 3, "expected": [[1, 2, 3], [4, 5]], "id": "532"},
    {"lst": [], "n": 3, "expected": [], "id": "empty_input"},
]


@pytest.mark.parametrize(
    "lst, n, expected",
    [pytest.param(case["lst"], case["n"], case["expected"], id=case["id"]) for case in CASES],
)
def test_chunk_it(lst, n, expected):
    chunks = iterables.chunked_it(lst, n)

    assert isinstance(chunks, types.GeneratorType)
    assert [list(x) for x in chunks] == expected


def test_n_less_than_one():
    with pytest.raises(ValueError):
        list(iterables.chunked_it([1, 2, 3], n=0))


@pytest.mark.parametrize(
    "items, start_value, key, expected",
    [
        pytest.param([], 5, lambda x: x, -1, id="empty list"),
        pytest.param([1, 2, 3, 4, 5], 3, lambda x: x, 2, id="exact match"),
        pytest.param([1, 2, 4, 5], 3, lambda x: x, 2, id="no exact match, find next"),
        pytest.param([1, 2, 3, 4, 5], 6, lambda x: x, -1, id="start_value too high"),
        pytest.param([1, 2, 3, 4, 5], 0, lambda x: x, 0, id="start_value too low"),
        pytest.param(
            [{"val": 10}, {"val": 20}, {"val": 30}],
            25,
            lambda x: x["val"],
            2,
            id="key function with dicts",
        ),
        pytest.param(
            [{"val": 10}, {"val": 20}, {"val": 30}],
            35,
            lambda x: x["val"],
            -1,
            id="start_value above max key with dicts",
        ),
    ],
)
def test_binary_search_first_ge(items, start_value, key, expected):
    result = iterables.binary_search_first_ge(items, start_value, key)
    assert result == expected
