import types

import pytest

from gfw.common import iterables


CASES = [
    {
        "lst": [1, 2, 3, 4, 5],
        "n": 3,
        "expected": [[1, 2, 3], [4, 5]],
        "id": "532"
    },
    {
        "lst": [],
        "n": 3,
        "expected": [],
        "id": "empty_input"
    },
]


@pytest.mark.parametrize("lst, n, expected", [
    pytest.param(
        case["lst"],
        case["n"],
        case["expected"],
        id=case["id"]
    )
    for case in CASES
])
def test_chunk_it(lst, n, expected):
    chunks = iterables.chunked_it(lst, n)

    assert isinstance(chunks, types.GeneratorType)
    assert [list(x) for x in chunks] == expected


def test_n_less_than_one():
    with pytest.raises(ValueError):
        list(iterables.chunked_it([1, 2, 3], n=0))
