"""GFW common tools CLI."""

from __future__ import annotations

import sys

from gfw.cli.commands.sharded_to_partitioned import ShardedToPartitionedCommand
from gfw.common.cli import CLI
from gfw.common.logging import LoggerConfig
from gfw.common.version import __version__


def run(args: list[str]) -> None:
    """Entry point for the gfw-common CLI."""
    CLI(
        name="gfw-common",
        description="GFW common tools.",
        subcommands=[
            ShardedToPartitionedCommand,
        ],
        version=__version__,
        examples=(
            "gfw-common sharded-to-partitioned --help",
            (
                "gfw-common sharded-to-partitioned"
                " --bq-in-sharded project.dataset.sharded"
                " --bq-out-partitioned project.dataset.consolidated"
                " --execution-project my-project"
                " --dry-run"
            ),
        ),
        logger_config=LoggerConfig(
            warning_level=[
                "urllib3",
            ],
        ),
    ).execute(args)


def main() -> None:
    """CLI entry point."""
    run(sys.argv[1:])


if __name__ == "__main__":
    main()
