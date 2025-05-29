import sys
import json
from datetime import date
from typing import Any, Sequence
from types import SimpleNamespace

from gfw.common.cli import CLI, Command, ParametrizedCommand, Option
from gfw.common.cli.validations import valid_date

from gfw.common.logging import LoggerConfig
from gfw.common.version import __version__

HELP_DRY_RUN = "If passed, all queries, if any, will be run in dry run mode."
HELP_PROJECT = "GCP project id."

HELP_LABELS = "Labels to audit costs over the queries."
DEFAULT_LABELS = {"environment": "develop", "stage": "test"}


class ParseCommand(Command):
    @property
    def name(self):
        return "parse_nmea"

    @property
    def description(self):
        return "Join & Parse NMEA sentences; BATCH (from GCS to BigQuery)."

    @property
    def options(self) -> Sequence[Option]:
        return [
            Option("-t", "--timeout", type=float, default=300, help="Timeout."),
        ]

    def run(self, config: SimpleNamespace, **kwargs: Any):
        print("Hello!")
        print("Timeout is:", config.timeout)


normalize_command = ParametrizedCommand(
    name="normalize",
    description="Normalize tables.",
    options=[
        Option("--partition-date", type=valid_date, default=date(2022, 9, 1), help="Date."),
        Option("--labels", type=json.loads, default=DEFAULT_LABELS, help=HELP_LABELS),
    ],
    run=lambda config, **kwargs: print(f"Hello!. partition-date is: {config.partition_date}")
)


def cli(args):
    nmea_cli = CLI(
        # Uncomment if running the CLI from a 'pipe-nmea' python installed package.
        # name="pipe-nmea",
        description="Tools for parsing AIS data in NMEA format from multiple sources.",
        options=[
            # Options declared here are going to be inherited by subcommands, if any.
            Option("--project", type=str, default="world-fishing-827", help=HELP_PROJECT),
            Option("--dry-run", type=bool, default=False, help=HELP_DRY_RUN),
        ],
        subcommands=[
            ParseCommand,
            normalize_command,
        ],
        version=__version__,
        examples=[
            "pipe-nmea -h",
            "pipe-nmea parse --dry-run",
        ],
        logger_config=LoggerConfig(
            warning_level=[
                "urllib3",
                "apache_beam.utils.subprocess_server",
            ]
        ),
        allow_unknown=False,
        use_underscore=False,
    )

    return nmea_cli.execute(args)


def main():
    cli(sys.argv[1:])


if __name__ == "__main__":
    main()
