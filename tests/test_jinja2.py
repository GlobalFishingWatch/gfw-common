from jinja2 import Environment

from gfw.common.jinja2 import EnvironmentLoader


def test_environment_loader():
    env = EnvironmentLoader().from_package(
        package="gfw",
        path="common/assets",
    )

    assert isinstance(env, Environment)
