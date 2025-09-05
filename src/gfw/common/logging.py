"""Logging utilities."""

import logging
import sys

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional, TextIO, Union

from rich.logging import RichHandler


_TIME_ENTRY = "%(asctime)s - "
_DEFAULT_LOG_FORMAT = f"{_TIME_ENTRY}%(name)s - %(message)s"


@dataclass
class LoggerConfig:
    """Helper class to setup the root logger.

    Args:
        format_:
            Logger format.

        warning_level:
            List of packages/modules for which to set the log level as ``WARNING``.

        error_level:
            List of packages/modules for which to set the log level as ``ERROR``.
    """

    format_: str = _DEFAULT_LOG_FORMAT
    warning_level: tuple[str, ...] = ()
    error_level: tuple[str, ...] = ()

    def setup(
        self,
        verbose: bool = False,
        rich: bool = True,
        log_file: Optional[Union[str, Path]] = None,
        log_stream: TextIO = sys.stderr,
    ) -> logging.Logger:
        """Initializes and configures the root logger.

        Args:
            verbose:
                If true, turns logger level to ``DEBUG``.

            rich:
                Whether to use `Rich <https://rich.readthedocs.io/en/stable/>`_ library
                to colorize console output.

            log_file:
                Path to file in which to save logs.

            log_stream:
                Destination stream for log output. Defaults to :py:attr:`sys.stderr`.
                Typically set to :py:attr:`sys.stdout` or :py:attr:`sys.stderr`,
                but can be any text-mode file-like object
                (e.g. an open file or :class:`io.StringIO`)
                that implements a ``write(str)`` method.
                Ignored if ``rich=True``, since ``RichHandler`` manages its own output stream.
        """
        logger = logging.getLogger()
        logger.handlers.clear()

        handlers: list[Any] = []
        fmt = self.format_

        if rich:
            handlers.append(RichHandler())
            fmt = fmt.replace(_TIME_ENTRY, "")
        else:
            handlers.append(logging.StreamHandler(log_stream))

        if log_file is not None:
            handlers.append(logging.FileHandler(Path(log_file)))

        logger.setLevel(logging.DEBUG if verbose else logging.INFO)

        formatter = logging.Formatter(fmt)
        for handler in handlers:
            handler.setFormatter(formatter)
            logger.addHandler(handler)

        for module in self.warning_level:
            logging.getLogger(module).setLevel(logging.WARNING)

        for module in self.error_level:
            logging.getLogger(module).setLevel(logging.ERROR)

        return logger
