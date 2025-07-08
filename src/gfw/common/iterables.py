"""Module that contains simple iterable utilities."""

import bisect
import itertools

from typing import Any, Callable, Iterable, Iterator, List


def chunked_it(iterable: Iterable[Any], n: int) -> Iterator[itertools.chain[Any]]:
    """Splits an iterable into iterator chunks of length n. The last chunk may be shorter."""
    if n < 1:
        raise ValueError("n must be at least one")

    it = iter(iterable)
    for x in it:
        yield itertools.chain((x,), itertools.islice(it, n - 1))


def binary_search_first_ge(
    items: List[Any],
    start_value: Any,
    key: Callable[[Any], Any],
) -> int:
    """Find index of first item in sorted `items` whose key >= `start_value` using binary search.

    This function performs a binary search to efficiently locate the leftmost index
    where the key of the item is greater than or equal to `start_value`.

    Args:
        items:
            Sorted list of items.

        start_value:
            The value to compare to.

        key:
            Function to extract a comparable key from each item.

    Returns:
        Index of the first item with key >= start_value, or -1 if no such item exists.
    """
    keys = [key(item) for item in items]
    idx = bisect.bisect_left(keys, start_value)
    if idx == len(keys):
        return -1

    return idx
