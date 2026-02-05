"""Small utilities for common string manipulation tasks.

This module provides lightweight helpers to transform and clean text
for practical use across the codebase.
"""

import textwrap


def collapse_paragraphs(text: str) -> str:
    r"""Collapse paragraphs with arbitrary newlines ('\n') into single lines."""
    paragraphs = textwrap.dedent(text).strip().split("\n\n")  # preserve paragraphs.
    cleaned = [" ".join(p.split()) for p in paragraphs]
    return "\n\n".join(cleaned)
