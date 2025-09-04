"""Lightweight framework around argparse for building CLIs more easily."""

from .cli import CLI
from .command import Command, ParametrizedCommand
from .option import Option


__all__ = [  # functions/classes/modules importable directly from package.
    "CLI",
    "Command",
    "Option",
    "ParametrizedCommand",
]
