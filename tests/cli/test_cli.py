from datetime import date

import pytest

from gfw.common.cli import CLI, ParametrizedCommand, Option
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
        run=lambda config, **kwargs: config.number_2*2
    )


@pytest.fixture
def main_command():
    return dict(
        name="program",
        options=[
            Option("--number-1", type=int, default=1),
            Option("--date-1", type=valid_date, default=date(2025, 1, 1)),
        ],
    )


def test_execute_with_subcommands(main_command, subcommand):
    test_cli = CLI(**main_command, subcommands=[subcommand])
    test_cli.execute(args=["subcommand", "--number-2", "3"])


def test_execute_without_subcommands(main_command):
    test_cli = CLI(**main_command)
    test_cli.execute(args=["--number-1", "4"])


@pytest.mark.parametrize(
    "arg, config_value, cli_value",
    [
        pytest.param("number_2", 5, 3, id="string"),
        pytest.param("boolean_2", True, False, id="bool"),
    ],
)
def test_arguments_precedence(
    tmp_path, main_command, subcommand, arg, config_value, cli_value
):
    path = tmp_path.joinpath("config.yaml")
    default_value = subcommand.defaults()[arg]
    command_name = subcommand.name

    config_file_arg = []
    yaml_save(path, data={arg: config_value})
    config_file_arg = ["--config-file", f"{path}"]

    cli_args = []
    cli_arg = arg.replace("_", "-")
    if not type(cli_value) is bool:
        cli_args = [f"--{cli_arg}", f"{cli_value}"]
    elif cli_value is True:
        cli_args = [f"--{cli_arg}"]

    test_cli = CLI(**main_command, subcommands=[subcommand])

    _, config = test_cli.execute(args=[command_name] + config_file_arg + cli_args)
    assert config[arg] == cli_value if cli_args else config_value

    _, config = test_cli.execute(args=[command_name] + config_file_arg)
    assert config[arg] == config_value

    _, config = test_cli.execute(args=[command_name] + ["--no-rich-logging"])
    assert config[arg] == default_value


def test_allow_unknown(main_command, subcommand):
    unknown = ["--other", "4"]

    test_cli = CLI(**main_command, subcommands=[subcommand], allow_unknown=True)
    res, config = test_cli.execute(args=["subcommand", "--number-2", "3"] + unknown)

    assert CLI.KEY_UNPARSED_ARGS in config
    assert config[CLI.KEY_UNPARSED_ARGS] == unknown


def test_dont_allow_unknown_fails(main_command, subcommand):
    unknown = ["--other", "4"]

    test_cli = CLI(**main_command, subcommands=[subcommand])
    with pytest.raises(SystemExit):
        res, config = test_cli.execute(args=["subcommand", "--number-2", "3"] + unknown)


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
        f"--number{sep}2", "2",
        f"--boolean{sep}2",
    ]

    unknown = ["--other", "4"]

    test_cli = CLI(
        **main_command,
        subcommands=[subcommand],
        allow_unknown=True,
        use_underscore=use_underscore
    )
    res, config = test_cli.execute(args=known + unknown)

    expected = (
        "subcommand \\"
        "\n--other 4 \\"
        "\n--number{sep}2=2 \\"
        "\n--date{sep}2=2025-01-02 \\"
        "\n--list{sep}2=ABC,EFG \\"
        "\n--boolean{sep}2 \\"
        "\n--number{sep}1=1 \\"
        "\n--date{sep}1=2025-01-01"
    ).format(sep=sep)

    assert res == expected
