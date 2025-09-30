"""Utility functions for dictionary and mapping operations.

This module provides general-purpose helpers for working with dictionaries
and other mapping types, such as filtering entries or transforming data.
"""

from typing import Any, Dict, Mapping, TypeVar


K = TypeVar("K")
V = TypeVar("V")


def filter_none_values(mapping: Mapping[K, V]) -> Dict[K, V]:
    """Return a new dictionary excluding keys with None values.

    Args:
        mapping:
            Input mapping.

    Returns:
        A new dictionary with all keys having non-None values.
    """
    return {k: v for k, v in mapping.items() if v is not None}


def copy_dict_without(dictionary: Mapping[K, V], keys: list[Any]) -> Dict[Any, Any]:
    """Returns a shallow copy of the given dictionary excluding specified keys.

    Args:
        dictionary:
            The source dictionary to copy.

        keys:
            A list of keys to remove from the resulting dictionary.

    Returns:
        A new dictionary with the specified keys removed.
    """
    return {k: v for k, v in dictionary.items() if k not in keys}
