"""Provides a class for generating structured BigQuery table descriptions.

This module defines the :class:`TableDescription` dataclass, which produces
a standardized, human-readable description string for use in
BigQuery table metadata. The description includes a title, subtitle,
summary, caveats, and a formatted list of relevant parameters.

The formatting is designed to be readable in the BigQuery UI,
GitHub, and other plaintext contexts.
"""

from dataclasses import dataclass, field
from typing import Any


DESCRIPTION_TEMPLATE = """\
❖ {title} ❖
{subtitle}.
⬖ Created by {repo_name}: v{version}.
⬖ https://github.com/GlobalFishingWatch/{repo_name}.

𝗦𝘂𝗺𝗺𝗮𝗿𝘆
{summary}

𝗖𝗮𝘃𝗲𝗮𝘁𝘀
{caveats}

For more information, see https://github.com/GlobalFishingWatch/{repo_name}/blob/HEAD/README.md.

𝗥𝗲𝗹𝗲𝘃𝗮𝗻𝘁 𝗽𝗮𝗿𝗮𝗺𝗲𝘁𝗲𝗿𝘀
{relevant_params}
"""  # noqa

TO_BE_COMPLETED = "To be completed."


@dataclass
class TableDescription:
    """Generates a structured description for BigQuery table metadata."""

    repo_name: str
    """GitHub repository name (used for URLs and headers)."""

    version: str = ""
    """Version of the project generating this table."""

    title: str = ""
    """Title of the table or dataset."""

    subtitle: str = ""
    """Subtitle or one-line summary."""

    summary: str = TO_BE_COMPLETED
    """High-level summary of the table's purpose."""

    caveats: str = TO_BE_COMPLETED
    """Known limitations or notes about the data."""

    relevant_params: dict[str, Any] = field(default_factory=dict)
    """Key parameters relevant to the table's content generation.

    The keys are parameter names (strings), and the values can be any type convertible
    to string.

    When rendered, the parameters are shown as a bullet list of key-value pairs,
    for example:

        - param1: value1
        - long_param2: value2
        - x: 42
    """

    def render(self) -> str:
        """Renders the description for use in BigQuery table metadata.

        Returns:
            A formatted string including summary, caveats, and relevant parameters.
        """
        return DESCRIPTION_TEMPLATE.format(
            repo_name=self.repo_name,
            version=self.version,
            title=self.title,
            subtitle=self.subtitle,
            summary=self.summary,
            caveats=self.caveats,
            relevant_params=self._format_params(),
        )

    def _format_params(self) -> str:
        if not self.relevant_params:
            return TO_BE_COMPLETED

        items = sorted(self.relevant_params.items())
        return "\n".join(f"- {k}: {v}" for k, v in items)
