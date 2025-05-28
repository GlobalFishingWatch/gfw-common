"""Validations functions to use in argparse.ArgumentParser 'type' parameter."""

import argparse

from datetime import date, datetime


def valid_list(s: str) -> list[str]:
    """Use with argparse to parse a comma-separated string.

    Example Usage:
    parser.add_argument(
        '--items',
        type=valid_list,
        default='ABC,DEF'
    )
    """
    return s.split(",")


def valid_date(s: str) -> date:
    """Use with argparse to validate a date parameter.

    Example Usage:
    parser.add_argument(
        '--date',
        type=valid_date,
        default='2019-01-01'
    )
    """
    try:
        return datetime.fromisoformat(s).date()
    except ValueError as e:
        msg = "Not a valid date: '{0}'. Expected format is YYYY-MM-DD".format(s)
        raise argparse.ArgumentTypeError(msg) from e
