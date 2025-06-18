"""General utility functions for working with Apache Beam pipelines."""

from typing import Any

from apache_beam.utils.timestamp import Timestamp


def float_to_beam_timestamp(row: dict[str, Any], fields: list[str]) -> dict[str, Any]:
    """Converts specified fields in a dictionary from float to Beam Timestamp objects.

    Args:
        row:
            A dictionary containing data with potential float values.

        fields:
            A tuple of field names to be converted to Timestamp.

    Returns:
        The input dictionary with specified fields converted to Timestamp objects.
    """
    new_row = row.copy()

    for field in fields:
        new_row[field] = Timestamp(row[field])

    return new_row
