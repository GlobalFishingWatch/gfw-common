"""This module defines the base abstractions and implementations for `CLI` commands.

It includes an abstract base class, `BaseCommand`, which specifies the interface
for CLI commands and subcommands, enforcing implementation of essential
properties and methods such as name, description, options, and run behavior.

Additionally, it provides a concrete implementation, `ParameterizedCommand`,
which allows quick command creation by passing all command parameters via
constructor arguments without subclassing.

These classes integrate with Python's argparse module to facilitate
declarative CLI construction with consistent behavior, help messages,
and argument parsing.
"""

import argparse

from abc import ABC, abstractmethod
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any, Callable, Sequence

from .option import Option


# See https://github.com/python/typeshed/issues/7539.
if TYPE_CHECKING:
    _SubparserType = argparse._SubParsersAction[argparse.ArgumentParser]  # pragma: nocover
else:
    _SubparserType = Any


class Command(ABC):
    """Abstract base class representing a CLI command or subcommand.

    This class defines the essential interface that any command implementation
    must follow to be compatible with the CLI framework.

    Subclasses must provide the command's name, description, options, and
    implement the :meth:`run` method which contains the command's logic.

    Properties:
        name:
            The name of the command (used as the CLI argument, e.g., ``mycli <name>``).
            For subcommands, this distinguishes them from the main command.

        description:
            A brief help message describing what the subcommand does.

        options:
            A list of `Option` instances representing CLI arguments specific to this subcommand.

    Methods:
        run:
            A callable to execute when the subcommand is invoked.
            It should accept keyword arguments corresponding to the CLI options.
    """

    @property
    @abstractmethod
    def name(self) -> str:
        """The name of the command (used as the CLI argument, e.g., ``mycli <name>``).

        For subcommands, this distinguishes them from the main command.
        """

    @property
    @abstractmethod
    def description(self) -> str:
        """The command's description."""

    @property
    @abstractmethod
    def options(self) -> Sequence[Option]:
        """The command's options."""

    @abstractmethod
    def run(self, config: SimpleNamespace, **kwargs: Any) -> Any:
        """Executes the command logic."""

    @property
    def header(self) -> str:
        """Returns a descriptive title for the command's options group."""
        return f"options defined by '{self.name}' command"

    def defaults(self) -> dict[str, Any]:
        """Returns a dictionary of default values for all CLI options.

        Returns:
            A dictionary mapping option dest (as used in argparse) to their default values.
        """
        return {o.dest: o.default for o in self.options}


class ParametrizedCommand(Command):
    """Command implementation that is fully parameterized via constructor arguments.

    The command's name, description, CLI options, and run behavior
    are provided when instantiating this class, allowing quick
    creation of commands without subclassing.

    Args:
        name:
            The name of the command (used as a CLI argument).

        help:
            A brief help message describing what the command does.

        options:
            A list of :class:`Option` instances representing CLI arguments
            specific to this command.

        run:
            :class:`Callable` executed when the command is invoked.
            It should accept keyword arguments matching the options.
    """

    def __init__(
        self,
        name: str,
        description: str = "",
        options: Sequence[Option] = (),
        run: Callable[..., Any] = lambda config: SimpleNamespace,
        **y: None,
    ) -> None:
        """Initializes ParametrizedCommand class."""
        self._name = name
        self._description = description
        self._options = options
        self._run = run

    @property
    def name(self) -> str:
        """The command's name."""
        return self._name

    @property
    def description(self) -> str:
        """The command's description."""
        return self._description

    @property
    def options(self) -> Sequence[Option]:
        """The command's options."""
        return self._options

    def run(self, config: SimpleNamespace, **kwargs: Any) -> Any:
        """Execute the command logic."""
        return self._run(config, **kwargs)
