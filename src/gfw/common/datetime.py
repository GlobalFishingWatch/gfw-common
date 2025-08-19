"""Utility functions for working with datetime objects and timezones."""

import logging
import re

from datetime import date, datetime, time, timezone, tzinfo
from typing import Optional, Union


logger = logging.getLogger(__name__)


def datetime_from_timestamp(ts: Union[int, float], tz: tzinfo = timezone.utc) -> datetime:
    """Converts a Unix timestamp (seconds since epoch) to a timezone-aware datetime object.

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


def datetime_from_isoformat(s: str, tz: tzinfo = timezone.utc) -> datetime:
    """Converts a datetime string in ISO format to a timezone-aware datetime object.

    Args:
        s:
            The string to convert, in ISO 8601 format (e.g., '2025-04-30T10:20:30').

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


def datetime_from_date(d: date, t: Optional[time] = None, tz: timezone = timezone.utc) -> datetime:
    """Creates datetime from date and optional time (default 00:00:00), with timezone.

    Args:
        d:
            Date part of the datetime.

        t:
            Optional time part.

        tz:
            Timezone for the resulting datetime.
            Defaults to UTC.

    Returns:
        A timezone-aware datetime object.
    """
    if t is None:
        t = time(0, 0)

    return datetime.combine(d, t, tzinfo=t.tzinfo or tz)


def datetime_from_string(
    s: str,
    date_format: str = "%Y-%m-%d",
    time_format: str = "%H_%M_%SZ",
    allow_no_time: bool = True,
    tz: timezone = timezone.utc,
) -> datetime:
    """Extracts a datetime from a string using provided date and time formats.

    Args:
        s:
            The string containing the datetime to extract.

        date_format:
            The strftime/strptime format of the date part.
            Defaults to "%Y-%m-%d".

        time_format:
            The strftime/strptime format of the time part.
            Defaults to "%H_%M_%SZ".

        allow_no_time:
            If True, allows input strings with no time information.

        tz:
            The timezone to apply if the parsed datetime has no tzinfo.
            Defaults to UTC.

    Raises:
        ValueError:
            - When date is not found in the input string.
            - When time is not found in the input string and allow_no_time is False.

    Returns:
        A timezone-aware datetime object.
    """
    _FORMAT_TOKEN_REGEX = {
        "%Y": r"\d{4}",
        "%y": r"\d{2}",
        "%m": r"\d{2}",
        "%d": r"\d{2}",
        "%H": r"\d{2}",
        "%M": r"\d{2}",
        "%S": r"\d{2}",
        "%f": r"\d{6}",
        "%z": r"[+-]\d{2}:?\d{2}",  # allow +HHMM or +HH:MM
        "Z": r"Z",
    }

    def _format_to_regex(fmt: str) -> str:
        regex = re.escape(fmt)
        for token, pattern in _FORMAT_TOKEN_REGEX.items():
            regex = regex.replace(re.escape(token), pattern)
        return regex

    date_regex = _format_to_regex(date_format)
    time_regex = _format_to_regex(time_format)

    # Build full regex with two capture groups, with time as optional.
    regex = rf"({date_regex})(?:.*?({time_regex}))?"

    logger.debug(f"Regex to use: {regex}.")

    match = re.search(regex, s)
    if not match:
        raise ValueError(f"Couldn't find a date with regex '{regex}' for string '{s}'.")

    date_str = match.group(1)
    date = datetime.strptime(date_str, date_format).date()

    time_str = match.group(2)

    if time_str is None:
        if not allow_no_time:
            raise ValueError(f"Couldn't find a time with regex '{regex}' for string '{s}'.")
        time = None
    else:
        time = datetime.strptime(time_str, time_format).timetz()  # Time with preservred timezone.

    return datetime_from_date(date, time, tz=tz)
