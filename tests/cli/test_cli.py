from datetime import date
from types import SimpleNamespace

import pytest

from gfw.common.cli import CLI, Option, ParametrizedCommand
from gfw.common.cli.validations import valid_date, valid_list
from gfw.common.io import yaml_save


@pytest.fixture
def subcommand():
    return ParametrizedCommand(
        name="subcommand",
        options=[
            Option("--number-2", type=int, default=2),
            Option("--date-2", type=valid_date, default=date(2025, 1, 2)),
            Option("--list-2", type=valid_list, default=["ABC", "EFG"]),
            Option("--list-3", type=valid_list, default=[]),
            Option("--boolean-2", type=bool, default=False),
        ],
        run=lambda config, **kwargs: config.number_2 * 2,
    )


@pytest.fixture
def main_command():
    return {
        "name": "program",
        "options": [
            Option("--number-1", type=int, default=1),
            Option("--date-1", type=valid_date, default=date(2025, 1, 1)),
        ],
    }


def test_execute_with_subcommands(main_command, subcommand):
    test_cli = CLI(**main_command, subcommands=[subcommand])
    test_cli.execute(args=["subcommand", "--number-2", "3"])


def test_execute_without_subcommands(main_command):
    test_cli = CLI(**main_command)
    test_cli.execute(args=["--number-1", "4"])


def test_main_command_run_is_called_with_correct_args():
    called = {}

    def main_run(config, **kwargs):
        called["called"] = True
        called["config"] = config
        return config.number_1 * 10

    cli = CLI(
        name="program",
        options=[Option("--number-1", type=int, default=1)],
        run=main_run,
    )

    result, config = cli.execute(args=["--number-1", "7"])

    # Assert the run function was called
    assert called.get("called") is True, "run method of main command was not called!"

    # Assert the config is SimpleNamespace and has number_1 set
    assert isinstance(called["config"], SimpleNamespace)
    assert called["config"].number_1 == 7

    # Assert the return value of run is correct
    assert result == 70

    # Also verify config dictionary returned
    assert config["number_1"] == 7


def test_execute_raises_on_invalid_config_key(tmp_path, main_command):
    test_cli = CLI(**main_command)

    config_path = tmp_path / "config.yaml"
    yaml_save(config_path, data={"invalid_param": 123})

    with pytest.raises(
        ValueError, match=r"Invalid configuration file: parameters \['invalid_param'\]"
    ):
        test_cli.execute(args=["--config-file", str(config_path)])


@pytest.mark.parametrize(
    "arg, config_value, cli_value",
    [
        pytest.param("number_2", 5, 3, id="string"),
        pytest.param("boolean_2", True, False, id="bool"),
    ],
)
def test_arguments_precedence(tmp_path, main_command, subcommand, arg, config_value, cli_value):
    path = tmp_path.joinpath("config.yaml")
    default_value = subcommand.defaults()[arg]
    command_name = subcommand.name

    config_file_arg = []
    yaml_save(path, data={arg: config_value})
    config_file_arg = ["--config-file", f"{path}"]

    cli_args = []
    cli_arg = arg.replace("_", "-")
    if type(cli_value) is not bool:
        cli_args = [f"--{cli_arg}", f"{cli_value}"]
    elif cli_value is True:
        cli_args = [f"--{cli_arg}"]

    test_cli = CLI(**main_command, subcommands=[subcommand])

    _, config = test_cli.execute(args=[command_name, *config_file_arg, *cli_args])
    assert config[arg] == cli_value if cli_args else config_value

    _, config = test_cli.execute(args=[command_name, *config_file_arg])
    assert config[arg] == config_value

    _, config = test_cli.execute(args=[command_name, "--no-rich-logging"])
    assert config[arg] == default_value


def test_allow_unknown(tmp_path, main_command, subcommand):
    known = ["--number-2", "3"]
    unknown_unparsed = ["--other", "4"]
    unknown_parsed = {"other2": 5}

    config_file_arg = []
    path = tmp_path.joinpath("config.yaml")
    yaml_save(path, data=unknown_parsed)
    config_file_arg = ["--config-file", f"{path}"]

    args = known + unknown_unparsed + config_file_arg

    test_cli = CLI(**main_command, subcommands=[subcommand], allow_unknown=True)
    _, config = test_cli.execute(args=[subcommand.name, *args])

    assert CLI._KEY_UNKNOWN_UNPARSED_ARGS in config
    assert config[CLI._KEY_UNKNOWN_UNPARSED_ARGS] == unknown_unparsed
    assert config[CLI._KEY_UNKNOWN_PARSED_ARGS] == unknown_parsed


def test_dont_allow_unknown_fails(main_command, subcommand):
    unknown = ["--other", "4"]

    test_cli = CLI(**main_command, subcommands=[subcommand])
    with pytest.raises(SystemExit):
        test_cli.execute(args=["subcommand", "--number-2", "3", *unknown])


@pytest.mark.parametrize(
    "use_underscore,sep",
    [
        pytest.param(False, "-", id="use_hyphens"),
        pytest.param(True, "_", id="use_underscore"),
    ],
)
def test_only_render(main_command, subcommand, use_underscore, sep):
    known = [
        "subcommand",
        f"--only{sep}render",
        f"--number{sep}2",
        "2",
        f"--boolean{sep}2",
    ]

    unknown = ["--other", "4"]

    test_cli = CLI(
        **main_command, subcommands=[subcommand], allow_unknown=True, use_underscore=use_underscore
    )
    res, _ = test_cli.execute(args=known + unknown)

    expected = (
        "program \\"
        "\nsubcommand \\"
        "\n--other 4 \\"
        "\n--number{sep}2=2 \\"
        "\n--date{sep}2=2025-01-02 \\"
        "\n--list{sep}2=ABC,EFG \\"
        "\n--boolean{sep}2 \\"
        "\n--number{sep}1=1 \\"
        "\n--date{sep}1=2025-01-01"
    ).format(sep=sep)

    assert res == expected


def test_logs_to_stdout(main_command, capsys):
    cli = CLI(**main_command)

    # run with flag -> logs should go to stdout
    cli.execute(args=["--log-to-stdout", "--no-rich-logging"])
    out, err = capsys.readouterr()

    assert "Starting program" in out
    assert err == ""


def test_logs_to_stderr(main_command, capsys):
    cli = CLI(**main_command)

    # run without flag -> logs should go to stderr
    cli.execute(args=["--no-rich-logging"])
    out, err = capsys.readouterr()

    assert "Starting program" in err
    assert out == ""
