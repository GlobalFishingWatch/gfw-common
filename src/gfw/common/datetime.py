"""Utility functions for working with datetime objects and timezones."""

import re

from datetime import date, datetime, time, timezone, tzinfo
from typing import Union


ISOFORMAT_REGEX = r"(\d{4}-\d{2}-\d{2}).*?(\d{2}_\d{2}_\d{2}Z)"


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


def get_datetime_from_string(
    s: str, regex: str = ISOFORMAT_REGEX, tz: timezone = timezone.utc
) -> Union[datetime, None]:
    """Extracts datetime from a string using an ISO-FORMAT regular expression.

    Args:
        s:
            Date part of the datetime.

        regex:
            The regular expression to use.
            Defaults to regex that matches YYYY-MM-DD and HH_MM_SSZ.

        tz:
            The timezone to apply to the resulting datetime, if not present.
            Defaults to UTC.

    Returns:
        A timezone-aware datetime object.
    """
    match = re.search(regex, s)

    if not match:
        return None

    date_str = match.group(1)
    time_str = match.group(2).replace("_", ":").replace("Z", "+00:00")
    dt = datetime.fromisoformat(f"{date_str} {time_str}")

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=tz)

    return dt
