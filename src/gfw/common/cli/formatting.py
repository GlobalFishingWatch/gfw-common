"""Custom argument formatting utilities for CLI applications.

This module provides tools to enhance the appearance of help messages in CLI
programs using `argparse`.

Exports:
    default_formatter: A factory function to create argparse.HelpFormatter instances.
"""

import argparse

from typing import Any, Callable


def default_formatter(max_pos: int = 30, **kwargs: Any) -> Callable[..., argparse.HelpFormatter]:
    """Create a custom argparse formatter factory.

    Args:
        max_pos:
            Maximum column for help text alignment.

        **kwargs:
            Additional arguments passed to the CustomFormatter.

    Returns:
        A callable that returns a CustomFormatter instance.
    """

    def formatter(prog: str) -> argparse.HelpFormatter:
        return argparse.RawTextHelpFormatter(prog, max_help_position=max_pos, **kwargs)

    return formatter
