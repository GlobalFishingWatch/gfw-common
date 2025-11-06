"""Utility functions for collections."""

from collections import ChainMap
from collections.abc import Mapping
from typing import Any, TypeVar


K = TypeVar("K")
V = TypeVar("V")


def _deep_update(target: dict[Any, Any], source: Mapping[Any, Any]) -> None:
    """Recursively merge nested mappings."""
    for key, val in source.items():
        if isinstance(val, Mapping) and key in target and isinstance(target[key], Mapping):
            _deep_update(target[key], val)
        else:
            target[key] = val


class DeepChainMap(ChainMap[K, V]):
    """A recursive version of ChainMap.

    Example:
        >>> base = {"a": {"x": 1}, "b": 2}
        >>> override = {"a": {"y": 3}}
        >>> dcm = DeepChainMap(override, base)
        >>> dcm["a"]["x"]
        1
        >>> dcm["a"].to_dict()
        {'x': 1, 'y': 3}
    """

    def __getitem__(self, key: K) -> Any:
        """Returns the value for key, merging nested mappings into a new DeepChainMap if needed."""
        submaps = [m[key] for m in self.maps if key in m]
        if not submaps:
            return self.__missing__(key)

        first = submaps[0]
        if isinstance(first, Mapping):
            return DeepChainMap(*submaps)  # type: ignore[arg-type]

        return first

    def to_dict(self) -> dict[K, V]:
        """Returns a dictionary repr, taking care of any nested DeepChainMap instances."""
        d: dict[K, V] = {}
        for mapping in reversed(self.maps):
            _deep_update(d, mapping)

        return d
