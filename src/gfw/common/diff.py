"""General utilities for generating diffs between objects."""

from difflib import ndiff
from typing import Any, Tuple

from rich.columns import Columns
from rich.panel import Panel
from rich.pretty import pretty_repr


def diff_lines(a: str, b: str) -> Tuple[str, str, bool]:
    """Generate a line-by-line diff of two strings with rich markup.

    Args:
        a:
            First multi-line string to compare.
        b:
            Second multi-line string to compare.

    Returns:
        A tuple of (a_diff, b_diff, changed) where:
        - a_diff: The first string annotated with diff highlights.
        - b_diff: The second string annotated with diff highlights.
        - changed: True if any differences were found, False otherwise.
    """
    a_lines, b_lines = a.splitlines(), b.splitlines()
    a_out, b_out = [], []
    changed = False
    for line in ndiff(a_lines, b_lines):
        tag, content = line[0], line[2:]
        if tag == " ":
            a_out.append(f"  {content}")
            b_out.append(f"  {content}")
        elif tag == "-":
            changed = True
            a_out.append(f"[red]- {content}[/red]")
            b_out.append("")  # line not in b
        elif tag == "+":
            changed = True
            a_out.append("")  # line not in a
            b_out.append(f"[green]+ {content}[/green]")

    return "\n".join(a_out), "\n".join(b_out), changed


def compare_items(a: Any, b: Any) -> Tuple[str, str, bool]:
    """Generate a rich diff of two objects' pretty-printed representations.

    Args:
        a:
            First object to compare.
        b:
            Second object to compare.

    Returns:
        The object returned by diff_lines.
    """
    return diff_lines(
        pretty_repr(a, indent_size=4, max_width=20),
        pretty_repr(b, indent_size=4, max_width=20),
    )


def render_diff_panel(left: str, right: str, idx: int) -> Columns:
    """Render side-by-side panels of diff strings for visual comparison.

    Args:
        left:
            The left-side diff string (usually actual output).

        right:
            The right-side diff string (usually expected output).

        idx:
            Index number for labeling the diff panels.

    Returns:
        A rich Columns object containing two Panels side-by-side.
    """
    return Columns(
        [
            Panel(left, title=f"Actual #{idx}", expand=True),
            Panel(right, title=f"Expected #{idx}", expand=True),
        ],
        expand=True,
        equal=True,
    )
