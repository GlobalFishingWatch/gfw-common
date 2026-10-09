import argparse

from datetime import date
from types import SimpleNamespace

import pytest

from gfw.common.cli import CLI, ParametrizedCommand, date_range_options, labels_option
from gfw.common.config import DateRangePipelineConfig, PipelineConfigError


def dated_cli():
    command = ParametrizedCommand(
        name="dated",
        options=[labels_option(), *date_range_options()],
        run=lambda config, **kwargs: DateRangePipelineConfig.from_namespace(config),
    )
    return CLI(name="program", subcommands=[command])


ARGS = ["dated", "--labels", "environment=dev", "--start-date", "2024-01-01"]


def test_shared_options_build_a_date_range_config():
    config, _ = dated_cli().execute(args=[*ARGS, "--end-date", "2024-01-08"])

    assert config.labels == {"environment": "dev"}
    assert (config.start_date, config.end_date) == (date(2024, 1, 1), date(2024, 1, 8))


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


def test_dates_from_a_config_file_are_parsed_by_the_config(tmp_path):
    # YAML loads an unquoted date as a date and a quoted one as a string; both work.
    config_file = tmp_path / "config.yaml"
    config_file.write_text("start_date: 2024-01-01\nend_date: '2024-01-08'\n")

    config, _ = dated_cli().execute(
        args=["dated", "--labels", "environment=dev", "--config-file", str(config_file)]
    )

    assert (config.start_date, config.end_date) == (date(2024, 1, 1), date(2024, 1, 8))


def test_an_empty_range_is_rejected():
    with pytest.raises(PipelineConfigError, match="must be after the start date"):
        dated_cli().execute(args=[*ARGS, "--end-date", "2024-01-01"])


def test_config_parses_values_the_cli_left_as_strings():
    config = DateRangePipelineConfig.from_namespace(
        SimpleNamespace(labels={"a": "b"}, start_date="2024-01-01", end_date="2024-01-02")
    )

    assert config.date_range.end == date(2024, 1, 2)
