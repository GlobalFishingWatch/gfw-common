from datetime import date, datetime, time, timedelta, timezone

import pytest

from gfw.common.datetime import (
    datetime_from_date,
    datetime_from_isoformat,
    datetime_from_string,
    datetime_from_timestamp,
)


def test_datetime_from_timestamp_utc():
    """Test converting Unix timestamp to UTC datetime."""
    ts = 1714477827  # Example Unix timestamp for April 30, 2024 11:50:27 UTC
    dt = datetime_from_timestamp(ts)
    expected_dt = datetime(2024, 4, 30, 11, 50, 27, tzinfo=timezone.utc)
    assert dt == expected_dt


def test_datetime_from_timestamp_with_timezone():
    """Test converting Unix timestamp to a datetime with a custom timezone."""
    ts = 1714477827  # Example Unix timestamp
    tz = timezone(timedelta(hours=2))  # CET (Central European Time)
    dt = datetime_from_timestamp(ts, tz)
    expected_dt = datetime(2024, 4, 30, 11, 50, 27, tzinfo=timezone.utc)
    assert dt == expected_dt


def test_datetime_from_isoformat():
    """Test converting UTC string to a timezone-aware datetime."""
    datetime_str = "2025-04-30T10:20:27+00:00"
    dt = datetime_from_isoformat(datetime_str)
    expected_dt = datetime(2025, 4, 30, 10, 20, 27, tzinfo=timezone.utc)
    assert dt == expected_dt


def test_datetime_from_isoformat_with_custom_tz():
    """Test converting datetime string to a datetime with a custom timezone."""
    datetime_str = "2025-04-30T10:20:27"
    tz = timezone(timedelta(hours=5, minutes=30))  # India Standard Time.
    dt = datetime_from_isoformat(datetime_str, tz)
    expected_dt = datetime(2025, 4, 30, 10, 20, 27, tzinfo=tz)
    assert dt == expected_dt


def test_datetime_from_timestamp_edge_case():
    """Test timestamp at the Unix epoch (1970-01-01 00:00:00 UTC)."""
    ts = 0  # Unix epoch time
    dt = datetime_from_timestamp(ts)
    expected_dt = datetime(1970, 1, 1, 0, 0, tzinfo=timezone.utc)
    assert dt == expected_dt


def test_datetime_from_date_defaults():
    d = date(2025, 7, 8)
    dt = datetime_from_date(d)
    assert dt.year == 2025
    assert dt.month == 7
    assert dt.day == 8
    assert dt.hour == 0
    assert dt.minute == 0
    assert dt.second == 0
    assert dt.tzinfo == timezone.utc


def test_datetime_from_date_custom_time():
    d = date(2025, 7, 8)
    t = time(15, 30, 45)
    dt = datetime_from_date(d, t)
    assert dt.hour == 15
    assert dt.minute == 30
    assert dt.second == 45
    assert dt.tzinfo == timezone.utc


def test_datetime_from_date_custom_timezone():
    d = date(2025, 7, 8)
    t = time(12, 0)
    tz = timezone(timedelta(hours=-5))
    dt = datetime_from_date(d, t, tz)
    assert dt.hour == 12
    assert dt.tzinfo == tz


# We declare cases here so we can create the ID for each test case pointing to the first argument.
TEST_CASES = [
    # Standard case: full datetime, default formats
    (
        "2025-08-15_12_30_45Z",
        "%Y-%m-%d",
        "%H_%M_%SZ",
        datetime(2025, 8, 15, 12, 30, 45, tzinfo=timezone.utc),
    ),
    # Date only, time defaults to 00:00
    (
        "2025-08-15",
        "%Y-%m-%d",
        "%H_%M_%SZ",
        datetime(2025, 8, 15, 0, 0, 0, tzinfo=timezone.utc),
    ),
    (
        "2025-08-15 14:45:00",
        "%Y-%m-%d",
        "%H:%M:%S",
        datetime(2025, 8, 15, 14, 45, 0, tzinfo=timezone.utc),
    ),
    (
        "file-nmea-2025-08-15_08_15_30Z.avro",
        "%Y-%m-%d",
        "%H_%M_%SZ",
        datetime(2025, 8, 15, 8, 15, 30, tzinfo=timezone.utc),
    ),
    (
        "20250814T093000Z.avro",
        "%Y%m%d",
        "%H%M%SZ",
        datetime(2025, 8, 14, 9, 30, 0, tzinfo=timezone.utc),
    ),
    (
        "2025.08.14 093000Z.data",
        "%Y.%m.%d",
        "%H%M%SZ",
        datetime(2025, 8, 14, 9, 30, 0, tzinfo=timezone.utc),
    ),
    (
        "2025_08_14_09_30_00Z.data",
        "%Y_%m_%d",
        "%H_%M_%SZ",
        datetime(2025, 8, 14, 9, 30, 0, tzinfo=timezone.utc),
    ),
    (
        "20250223-fff50d9b6da0109f805ffa5f86fd2e65.data",
        "%Y%m%d",
        "%H%M%SZ",
        datetime(2025, 2, 23, 0, 0, 0, tzinfo=timezone.utc),
    ),
    (
        "json/scheduled__2025-07-31T11:00:00+00:00",
        "%Y-%m-%d",
        "%H:%M:%S%z",
        datetime(2025, 7, 31, 11, 0, 0, tzinfo=timezone.utc),
    ),
    (
        "/api/2025-07-23.json.gz",
        "%Y-%m-%d",
        "%H:%M:%SZ",
        datetime(2025, 7, 23, tzinfo=timezone.utc),
    ),
    (
        "2025-08-15 14:45:00-03:00",  # Input string in UTC-3
        "%Y-%m-%d",
        "%H:%M:%S%z",
        datetime(2025, 8, 15, 14, 45, 0, tzinfo=timezone(timedelta(hours=-3))),
    ),
]


@pytest.mark.parametrize(
    "input_str, date_fmt, time_fmt, expected_dt", TEST_CASES, ids=[val[0] for val in TEST_CASES]
)
def test_datetime_from_string(input_str, date_fmt, time_fmt, expected_dt):
    result = datetime_from_string(input_str, date_format=date_fmt, time_format=time_fmt)
    assert result == expected_dt


@pytest.mark.parametrize(
    "args, kwargs",
    [
        pytest.param(("invalid_string",), {}, id="invalid-string"),
        pytest.param(("12_30_45Z",), {"time_format": "%H_%M_%SZ"}, id="time-without-date"),
        pytest.param(("2025-08-15",), {"allow_no_time": False}, id="time-without-time"),
    ],
)
def test_datetime_from_string_raises_value_error(args, kwargs):
    with pytest.raises(ValueError):
        datetime_from_string(*args, **kwargs)
