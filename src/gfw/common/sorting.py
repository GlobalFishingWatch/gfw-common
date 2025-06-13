"""Utilities for sorting data structures."""

from typing import Any


def sort_dicts(obj: Any) -> Any:
    """Recursively sorts dict keys to get consistent ordering for comparison.

    Lists, tuples, and other types are returned unchanged (except their contents
    get sorted recursively if they are dicts).

    Args:
        obj: Any nested structure (dict, list, tuple, or other).

    Returns:
        A new structure with dict keys sorted recursively.
    """
    if isinstance(obj, dict):
        # Sort keys and recursively apply to values
        return {k: sort_dicts(obj[k]) for k in sorted(obj)}
    elif isinstance(obj, list):
        # Recursively apply to each element
        return [sort_dicts(e) for e in obj]
    elif isinstance(obj, tuple):
        # Recursively apply to each element and keep tuple type
        return tuple(sort_dicts(e) for e in obj)
    else:
        # Return base case unchanged
        return obj
