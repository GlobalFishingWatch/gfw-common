"""CLI options shared by GFW pipelines.

Use them in a :class:`~gfw.common.cli.Command`'s options, or as the main command's common options,
so every pipeline exposes the same flags with the same meaning.
Their values match the fields of :class:`~gfw.common.config.PipelineConfig`, which validates them.
"""

from .actions import NestedKeyValueAction
from .option import Option
from .validations import valid_date


HELP_LABELS = (
    "Labels for the pipeline's jobs, to audit costs, e.g. --labels environment=dev step=x."
)
HELP_START_DATE = "First date to process, inclusive (YYYY-MM-DD)."
HELP_END_DATE = "Date to stop processing at, exclusive (YYYY-MM-DD)."


def labels_option() -> Option:
    """Returns the required ``--labels`` option, parsed into a dictionary."""
    return Option(
        "--labels",
        type=str,
        nargs="*",
        action=NestedKeyValueAction,
        required=True,
        help=HELP_LABELS,
    )


def date_range_options() -> list[Option]:
    """Returns the required ``--start-date`` (inclusive) and ``--end-date`` (exclusive) options.

    Command-line values are parsed into :class:`~datetime.date` objects here, so an invalid date
    is a usage error. Values from a config file skip argparse:
    :meth:`PipelineConfig.from_namespace <gfw.common.config.PipelineConfig.from_namespace>` parses
    those, and :class:`~gfw.common.config.PipelineConfig` validates the range.
    """
    return [
        Option("--start-date", type=valid_date, required=True, help=HELP_START_DATE),
        Option("--end-date", type=valid_date, required=True, help=HELP_END_DATE),
    ]
