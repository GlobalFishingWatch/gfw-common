import argparse

from datetime import datetime, timezone

import pytest

from gfw.common.cli import CLI, ParametrizedCommand, date_range_options, labels_option
from gfw.common.config import PipelineConfig, PipelineConfigError


def dated_cli():
    command = ParametrizedCommand(
        name="dated",
        options=[labels_option(), *date_range_options()],
        run=lambda config, **kwargs: PipelineConfig.from_namespace(config),
    )
    return CLI(name="program", subcommands=[command])


ARGS = ["dated", "--labels", "environment=dev", "--start-date", "2024-01-01"]
JAN_1 = datetime(2024, 1, 1, tzinfo=timezone.utc)
JAN_8 = datetime(2024, 1, 8, tzinfo=timezone.utc)


def test_shared_options_build_a_config():
    config, _ = dated_cli().execute(args=[*ARGS, "--end-date", "2024-01-08"])

    assert config.labels == {"environment": "dev"}
    assert (config.start_datetime, config.end_datetime) == (JAN_1, JAN_8)


@pytest.mark.parametrize("missing", ["--labels", "--end-date"])
def test_shared_options_are_required(missing):
    args = [*ARGS, "--end-date", "2024-01-08"]
    i = args.index(missing)
    del args[i : i + 2]

    with pytest.raises(argparse.ArgumentTypeError, match="Missing required arguments"):
        dated_cli().execute(args=args)


def test_an_invalid_command_line_date_is_a_usage_error():
    with pytest.raises(SystemExit):
        dated_cli().execute(args=[*ARGS, "--end-date", "08/01/2024"])


def test_dates_from_a_config_file_are_parsed(tmp_path):
    # YAML loads an unquoted date as a date and a quoted one as a string, which
    # PipelineConfig.from_namespace turns into UTC datetimes at midnight.
    config_file = tmp_path / "config.yaml"
    config_file.write_text("start_date: 2024-01-01\nend_date: '2024-01-08'\n")

    config, _ = dated_cli().execute(
        args=["dated", "--labels", "environment=dev", "--config-file", str(config_file)]
    )

    assert (config.start_datetime, config.end_datetime) == (JAN_1, JAN_8)


def test_an_empty_range_is_rejected():
    with pytest.raises(PipelineConfigError, match="must be after start_datetime"):
        dated_cli().execute(args=[*ARGS, "--end-date", "2024-01-01"])
