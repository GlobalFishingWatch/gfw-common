"""Utility functions for dictionary and mapping operations.

This module provides general-purpose helpers for working with dictionaries
and other mapping types, such as filtering entries or transforming data.
"""

from typing import Dict, Mapping, TypeVar


K = TypeVar("K")
V = TypeVar("V")


def filter_none_values(mapping: Mapping[K, V]) -> Dict[K, V]:
    """Return a new dictionary excluding keys with None values.

    Args:
        mapping (Mapping[K, V]): Input mapping.

    Returns:
        Dict[K, V]: A new dictionary with all keys having non-None values.
    """
    return {k: v for k, v in mapping.items() if v is not None}
