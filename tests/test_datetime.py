from datetime import date, datetime, time, timedelta, timezone

from gfw.common.datetime import datetime_from_date, datetime_from_string, datetime_from_timestamp


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


def test_datetime_from_string():
    """Test converting UTC string to a timezone-aware datetime."""
    datetime_str = "2025-04-30T10:20:27"
    dt = datetime_from_string(datetime_str)
    expected_dt = datetime(2025, 4, 30, 10, 20, 27, tzinfo=timezone.utc)
    assert dt == expected_dt


def test_datetime_from_string_with_custom_tz():
    """Test converting datetime string to a datetime with a custom timezone."""
    datetime_str = "2025-04-30T10:20:27"
    tz = timezone(timedelta(hours=5, minutes=30))  # India Standard Time.
    dt = datetime_from_string(datetime_str, tz)
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
