import argparse

from datetime import datetime, timedelta, timezone

import pytest

from gfw.common.cli import validations


@pytest.mark.parametrize(
    "func, invalid_arg, valid_arg",
    [
        pytest.param(validations.valid_date, "2024/04/01", "2024-04-01", id="date"),
        pytest.param(
            validations.valid_datetime, "2024/04/01T23:59:59", "2024-04-01T23:59:59", id="datetime"
        ),
        pytest.param(validations.valid_list, None, "ABC,DEF", id="list"),
    ],
)
def test_argument_validations(func, invalid_arg, valid_arg):
    parser = argparse.ArgumentParser()
    parser.add_argument("--arg", type=func)

    if invalid_arg is not None:
        with pytest.raises(SystemExit) as e:
            parser.parse_args(["--arg", invalid_arg])
            assert isinstance(e.__context__, argparse.ArgumentTypeError)

    parser.parse_args(["--arg", valid_arg])


@pytest.mark.parametrize(
    "value, expected",
    [
        ("2024-04-01T23:59:59", datetime(2024, 4, 1, 23, 59, 59, tzinfo=timezone.utc)),
        ("2024-04-01", datetime(2024, 4, 1, tzinfo=timezone.utc)),
        (
            "2024-04-01T23:59:59-03:00",
            datetime(2024, 4, 1, 23, 59, 59, tzinfo=timezone(timedelta(hours=-3))),
        ),
    ],
    ids=["naive", "date-only", "with-timezone"],
)
def test_valid_datetime_is_utc_unless_a_timezone_is_given(value, expected):
    result = validations.valid_datetime(value)

    assert result == expected
    assert result.utcoffset() == expected.utcoffset()
