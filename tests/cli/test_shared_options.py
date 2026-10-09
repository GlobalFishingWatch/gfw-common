import argparse

from datetime import date, datetime, timedelta, timezone

import pytest

from gfw.common.cli import (
    CLI,
    ParametrizedCommand,
    date_range_options,
    datetime_range_options,
    labels_option,
)
from gfw.common.config import DatePipelineConfig, DatetimePipelineConfig, PipelineConfigError


def make_cli(options, config_class):
    command = ParametrizedCommand(
        name="command",
        options=[labels_option(), *options],
        run=lambda config, **kwargs: config_class.from_namespace(config),
    )
    return CLI(name="program", subcommands=[command])


def date_cli():
    return make_cli(date_range_options(), DatePipelineConfig)


def datetime_cli():
    return make_cli(datetime_range_options(), DatetimePipelineConfig)


LABELS = ["command", "--labels", "environment=dev"]
DATE_ARGS = [*LABELS, "--start-date", "2024-01-01"]
UTC = timezone.utc


def test_date_options_build_a_date_config():
    config, _ = date_cli().execute(args=[*DATE_ARGS, "--end-date", "2024-01-08"])

    assert config.labels == {"environment": "dev"}
    assert (config.start_date, config.end_date) == (date(2024, 1, 1), date(2024, 1, 8))


@pytest.mark.parametrize("missing", ["--labels", "--end-date"])
def test_date_options_are_required(missing):
    args = [*DATE_ARGS, "--end-date", "2024-01-08"]
    i = args.index(missing)
    del args[i : i + 2]

    with pytest.raises(argparse.ArgumentTypeError, match="Missing required arguments"):
        date_cli().execute(args=args)


def test_an_invalid_command_line_date_is_a_usage_error():
    with pytest.raises(SystemExit):
        date_cli().execute(args=[*DATE_ARGS, "--end-date", "08/01/2024"])


def test_dates_from_a_config_file_are_parsed_like_command_line_ones(tmp_path):
    # Unquoted and quoted dates both go through the option's type, like on the command line.
    config_file = tmp_path / "config.yaml"
    config_file.write_text("start_date: 2024-01-01\nend_date: '2024-01-08'\n")

    config, _ = date_cli().execute(args=[*LABELS, "--config-file", str(config_file)])

    assert (config.start_date, config.end_date) == (date(2024, 1, 1), date(2024, 1, 8))


def test_an_invalid_config_file_date_is_rejected(tmp_path):
    config_file = tmp_path / "config.yaml"
    config_file.write_text("start_date: 2024-01-01\nend_date: 08/01/2024\n")

    with pytest.raises(argparse.ArgumentTypeError, match="'end_date' in the config file"):
        date_cli().execute(args=[*LABELS, "--config-file", str(config_file)])


def test_an_empty_date_range_is_rejected():
    with pytest.raises(PipelineConfigError, match="must be after start_date"):
        date_cli().execute(args=[*DATE_ARGS, "--end-date", "2024-01-01"])


def test_datetime_options_build_a_datetime_config():
    config, _ = datetime_cli().execute(
        args=[
            *LABELS,
            "--start-datetime",
            "2024-01-01T00:00:00",
            "--end-datetime",
            "2024-01-01T06:00:00-03:00",
        ]
    )

    assert config.start_datetime == datetime(2024, 1, 1, tzinfo=UTC)
    assert config.end_datetime == datetime(2024, 1, 1, 9, tzinfo=UTC)
    assert config.end_datetime.utcoffset() == timedelta(hours=-3)


def test_datetimes_from_a_config_file_are_parsed_like_command_line_ones(tmp_path):
    config_file = tmp_path / "config.yaml"
    config_file.write_text("start_datetime: 2024-01-01\nend_datetime: 2024-01-01T06:00:00\n")

    config, _ = datetime_cli().execute(args=[*LABELS, "--config-file", str(config_file)])

    assert config.start_datetime == datetime(2024, 1, 1, tzinfo=UTC)
    assert config.end_datetime == datetime(2024, 1, 1, 6, tzinfo=UTC)
