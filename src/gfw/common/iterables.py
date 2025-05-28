"""Module that contains simple iterable utilities."""

import itertools

from typing import Any, Iterable, Iterator


def chunked_it(iterable: Iterable[Any], n: int) -> Iterator[itertools.chain[Any]]:
    """Splits an iterable into iterator chunks of length n. The last chunk may be shorter."""
    if n < 1:
        raise ValueError("n must be at least one")

    it = iter(iterable)
    for x in it:
        yield itertools.chain((x,), itertools.islice(it, n - 1))
