import logging

from rich.logging import RichHandler

from gfw.common.logging import LoggerConfig


def test_defaults():
    logger_config = LoggerConfig()
    logger = logger_config.setup()

    assert logger.level == logging.INFO
    assert len(logger.handlers) == 1
    assert isinstance(logger.handlers[0], RichHandler)


def test_warning_error_levels():
    logger_config = LoggerConfig(
        warning_level=["warn_module"],
        error_level=["err_module"],
    )

    logger_config.setup()

    assert logging.getLogger("warn_module").level == logging.WARNING
    assert logging.getLogger("err_module").level == logging.ERROR


def test_no_rich():
    logger_config = LoggerConfig()
    logger = logger_config.setup(rich=False)

    assert len(logger.handlers) == 1
    assert isinstance(logger.handlers[0], logging.StreamHandler)


def test_verbose():
    logger_config = LoggerConfig()
    logger = logger_config.setup(verbose=True)

    assert logger.level == logging.DEBUG


def test_log_file(tmp_path):
    logger_config = LoggerConfig()
    logger = logger_config.setup(log_file=tmp_path.joinpath("test.log"))

    assert len(logger.handlers) == 2
    assert logging.FileHandler in {type(x) for x in logger.handlers}
