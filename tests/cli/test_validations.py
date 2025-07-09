import argparse

import pytest

from gfw.common.cli import validations


@pytest.mark.parametrize(
    "func, invalid_arg, valid_arg",
    [
        pytest.param(validations.valid_date, "2024/04/01", "2024-04-01", id="date"),
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
