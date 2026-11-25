"""CLI options shared by GFW pipelines.

Use them in a :class:`~gfw.common.cli.Command`'s options, or as the main command's common options,
so every pipeline exposes the same flags with the same meaning.
Their values match the fields of :class:`~gfw.common.config.PipelineConfig` and its subclasses,
which validate them.
"""

from .actions import NestedKeyValueAction
from .option import Option
from .validations import valid_date, valid_datetime


HELP_LABELS = (
    "Labels for the pipeline's jobs, to audit costs, e.g. --labels environment=dev step=x."
)
HELP_WAIT_FOR_JOB = (
    "Wait until the job finishes before returning. "
    "Pass --no-wait-for-job to return once the job is submitted."
)
HELP_START_DATE = "First date to process, inclusive (YYYY-MM-DD)."
HELP_END_DATE = "Date to stop processing at, exclusive (YYYY-MM-DD)."
HELP_START_DATETIME = (
    "Start of the time range to process, inclusive (YYYY-MM-DDTHH:MM:SS). "
    "UTC unless a timezone is given."
)
HELP_END_DATETIME = (
    "End of the time range to process, exclusive (YYYY-MM-DDTHH:MM:SS). "
    "UTC unless a timezone is given."
)


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


def wait_for_job_option() -> Option:
    """Returns the ``--wait-for-job`` option, which is ``True`` by default.

    It fills the ``wait_for_job`` field of :class:`~gfw.common.config.PipelineConfig`. Use it in
    the commands that run Apache Beam: other commands have no job to wait for.
    """
    return Option("--wait-for-job", type=bool, default=True, help=HELP_WAIT_FOR_JOB)


def date_range_options() -> list[Option]:
    """Returns the required ``--start-date`` (inclusive) and ``--end-date`` (exclusive) options.

    They fill the fields of :class:`~gfw.common.config.DatePipelineConfig`, as
    :class:`~datetime.date` objects, whether they come from the command line or a config file.
    """
    return [
        Option("--start-date", type=valid_date, required=True, help=HELP_START_DATE),
        Option("--end-date", type=valid_date, required=True, help=HELP_END_DATE),
    ]


def datetime_range_options() -> list[Option]:
    """Returns the required ``--start-datetime`` (inclusive) and ``--end-datetime`` (exclusive).

    They fill the fields of :class:`~gfw.common.config.DatetimePipelineConfig`, as
    timezone-aware :class:`~datetime.datetime` objects (UTC unless a timezone is given), whether
    they come from the command line or a config file.
    """
    return [
        Option("--start-datetime", type=valid_datetime, required=True, help=HELP_START_DATETIME),
        Option("--end-datetime", type=valid_datetime, required=True, help=HELP_END_DATETIME),
    ]
