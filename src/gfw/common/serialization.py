"""Serialization utilities."""

from datetime import date
from typing import Any


def to_json(s: Any) -> str:
    """Convert supported objects to a JSON-serializable string.

    Args:
        s:
            The object to serialize.

    Returns:
        A string representation of the object.

    Raises:
        NotImplementedError: If the object type is not supported.
    """
    if isinstance(s, date):
        return s.isoformat()

    raise NotImplementedError(f"No implementation to jsonify type: {type(s)}")  # pragma: no cover
