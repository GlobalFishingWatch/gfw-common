"""Utility functions for working with datetime objects and timezones."""

from datetime import date, datetime, time, timezone, tzinfo
from typing import Union


def datetime_from_timestamp(ts: Union[int, float], tz: tzinfo = timezone.utc) -> datetime:
    """Convert a Unix timestamp (seconds since epoch) to a timezone-aware datetime object.

    By default, the timestamp is converted to UTC (timezone.utc).
    If you need a different timezone, specify it using the 'tz' argument.

    Args:
        ts:
            The Unix timestamp to convert.

        tz:
            The timezone to apply. Defaults to UTC.

    Returns:
        A timezone-aware datetime object corresponding to the given timestamp.
    """
    return datetime.fromtimestamp(ts, tz=tz)


def datetime_from_string(s: str, tz: tzinfo = timezone.utc) -> datetime:
    """Convert a UTC string (e.g., '2025-04-30T10:20:30') to a timezone-aware datetime object.

    Args:
        s:
            The string to convert, in ISO 8601 format.

        tz:
            The timezone to apply to the resulting datetime, if not present.
            Defaults to UTC.

    Returns:
        A timezone-aware datetime object.
    """
    dt = datetime.fromisoformat(s)

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=tz)

    return dt


def datetime_from_date(d: date, t: time = time(0, 0), tz: timezone = timezone.utc) -> datetime:
    """Creates datetime from date and optional time (default 00:00:00), with timezone.

    Args:
        d:
            Date part of the datetime.

        t:
            Optional time part. Defaults to 00:00:00.

        tz:
            Timezone for the resulting datetime.
            Defaults to UTC.

    Returns:
        A timezone-aware datetime object.
    """
    return datetime.combine(d, t, tzinfo=tz)
