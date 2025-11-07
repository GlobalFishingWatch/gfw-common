import argparse

import pytest

from gfw.common.cli.actions import NestedKeyValueAction


def make_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--labels",
        nargs="+",
        action=NestedKeyValueAction,
        default={},
        help="Nested key=value pairs (supports dotted keys)",
    )
    return parser


def test_flat_key_value():
    parser = make_parser()
    args = parser.parse_args(["--labels", "env=prod", "region=us"])
    assert args.labels == {"env": "prod", "region": "us"}


def test_json_string_and_nested_key_value():
    parser = make_parser()
    args = parser.parse_args(
        [
            "--labels",
            r'{"metrics": {"level": "high", "type": "cpu"}}',
            "metrics.level=low",
        ]
    )
    assert args.labels == {"metrics": {"level": "low", "type": "cpu"}}


def test_invalid_json_string():
    parser = make_parser()
    with pytest.raises(SystemExit):
        parser.parse_args(["--labels", r'{"metrics": {"level": "hi}'])


def test_nested_key_value():
    parser = make_parser()
    args = parser.parse_args(["--labels", "metrics.level=high", "metrics.type=cpu"])
    assert args.labels == {"metrics": {"level": "high", "type": "cpu"}}


def test_deeply_nested_key_value():
    parser = make_parser()
    args = parser.parse_args(["--labels", "metrics.service.latency=200"])
    assert args.labels == {"metrics": {"service": {"latency": "200"}}}


def test_multiple_invocations_merge():
    parser = make_parser()
    args = parser.parse_args(["--labels", "env=prod", "--labels", "metrics.level=high"])
    assert args.labels == {"env": "prod", "metrics": {"level": "high"}}


def test_invalid_format_raises():
    parser = make_parser()
    with pytest.raises(SystemExit):  # argparse exits on error
        parser.parse_args(["--labels", "invalidpair"])
