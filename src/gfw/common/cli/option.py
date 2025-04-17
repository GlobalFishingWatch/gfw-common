"""CLI option wrapper for argparse.

This module provides a convenient wrapper around argparse to define command-line interface
options using the `Option` class. It supports automatic argument registration, default values,
and custom types, including boolean flags.

Classes:
    Option: Represents a command-line interface option and handles its registration in an
            ArgumentParser.
"""

from functools import cached_property
from typing import Any, Callable


class Option:
    """Represents a CLI option.

    Provides a declarative interface to define command-line options that can be
    added to an `argparse.ArgumentParser` via `add_argument` method.

    Args:
        *flags:
            One or more command-line flag strings for this option.
            These are passed directly to argparse's `add_argument()`.

            The first long flag (if any) is used to derive the internal name (`option.name`),
            which is also used as the destination (`dest`) when parsing.

            Three examples:
                Option("-c", "--config-file", ...)  # Short and long form
                Option("--verbose", ...)            # Long form only
                Option("-v", ...)                   # Short form only

        type:
            A callable that converts the command-line string to the desired Python type.
            Typically, a built-in type like `str`, `int`, `float`, or `bool`.

        default:
            The default value to use if the option is not provided. This should match
            the specified type, although no enforcement is currently done.

        **kwargs:
            Additional keyword arguments passed directly to `ArgumentParser.add_argument`.
    """

    def __init__(
        self, *flags: str, type: Callable[..., Any], default: Any = None, **kwargs: Any
    ) -> None:
        """Initializes an Option instance."""
        self.flags = flags
        self.type = type
        self.default = default
        self.kwargs = kwargs

    @cached_property
    def dest(self) -> str:
        """Returns the internal variable name used by argparse for this option.

        Uses the last long flag (e.g., '--config-file') if present, or the first
        flag as a fallback. Dashes are converted to underscores for compatibility
        with argparse's variable naming.
        """
        first_flag = next(iter(self.flags)).lstrip("-")
        last_long_flag = None

        for f in self.flags:
            if f.startswith("--"):
                last_long_flag = f.lstrip("-")

        return (last_long_flag or first_flag).replace("-", "_")
