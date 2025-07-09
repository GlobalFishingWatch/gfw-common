import pytest

from gfw.common.cli import Option, ParametrizedCommand


@pytest.fixture
def command():
    return ParametrizedCommand(
        name="test-command",
        options=[Option("--option1", type=int, default=1)],
    )


def test_run(command):
    assert command.defaults() == {"option1": 1}
