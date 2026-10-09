"""Lightweight framework around argparse for building CLIs more easily.

.. currentmodule:: gfw.common.cli

Classes
-------

.. autosummary::
   :toctree: ../_autosummary/
   :template: custom-class-template.rst
   :signatures: none

   CLI
   Command
   ParametrizedCommand
   Option

Functions
---------

.. autosummary::
   :toctree: ../_autosummary/

   date_range_options
   labels_option
"""

from .cli import CLI
from .command import Command, ParametrizedCommand
from .option import Option
from .shared_options import date_range_options, labels_option


__all__ = [  # functions/classes/modules importable directly from package.
    "CLI",
    "Command",
    "Option",
    "ParametrizedCommand",
    "date_range_options",
    "labels_option",
]
