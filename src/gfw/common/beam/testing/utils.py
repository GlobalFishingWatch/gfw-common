"""General utilities for testing Apache Beam pipelines."""

from itertools import zip_longest
from typing import Any, Callable, Iterable, List, Sequence

from apache_beam.testing.util import BeamAssertException
from rich.console import Console, Group, RenderableType

from gfw.common.diff import compare_items, render_diff_panel
from gfw.common.sorting import sort_dicts


def _default_equals_fn(e: Any, a: Any) -> bool:
    return e == a


def _raise_with_diff(diffs: Sequence[RenderableType]) -> None:
    # Set up a Rich Console that records output
    console = Console(record=True, force_terminal=True, width=130)

    # Render diffs to console (only into memory, not to screen)
    console.print(Group(*diffs))

    # Export the captured diff as text with ANSI codes
    diff_text = console.export_text(styles=True)

    # Raise exception with embedded colored diff
    raise BeamAssertException(f"PCollection contents differ: \n{diff_text}.")


def equal_to(
    expected: List[Any], equals_fn: Callable[[Any, Any], bool] = _default_equals_fn
) -> Callable[[List[Any]], None]:
    """Drop-in replacement for `apache_beam.testing.util.equal_to` with rich diff output.

    This matcher performs unordered comparison of top-level elements in actual and expected
    PCollection outputs, just like Apache Beam's `equal_to`. However, it adds a rich diff
    visualization to help debug mismatches by rendering side-by-side differences.

    Use in tests with `assert_that(pcoll, equal_to(expected))`.

    Note:
        - Only top-level permutations are considered equal:
          `[1, 2]` and `[2, 1]` are equal, but `[[1, 2]]` and `[[2, 1]]` are not.

        - If elements are not directly comparable, a fallback comparison using
          a custom equality function or deep diff is used. This helps handle:
          1) Collections with types that don't have a deterministic sort order
             (e.g., pyarrow Tables as of 0.14.1).
          2) Collections containing elements of different types.

    Args:
        expected: Iterable of expected PCollection elements.
        equals_fn: Optional function `(expected_item, actual_item) -> bool` to customize equality.

    Returns:
        A matcher function for use with `apache_beam.testing.util.assert_that`.
    """

    def _matcher(actual: Iterable[Any]) -> None:
        expected_list = [sort_dicts(e) for e in expected]
        actual_list = [sort_dicts(e) for e in actual]

        try:
            if actual_list == expected_list:
                return
        except TypeError:
            pass

        # Slower method, fallback comparison.
        unmatched_expected = expected_list[:]
        unmatched_actual = []
        for a in actual_list:
            for i, e in enumerate(unmatched_expected):
                if equals_fn(e, a):
                    unmatched_expected.pop(i)
                    break
            else:
                unmatched_actual.append(a)

        if not unmatched_actual and not unmatched_expected:
            return

        diffs = []
        for i, (a, b) in enumerate(
            zip_longest(unmatched_actual, unmatched_expected, fillvalue={}), 1
        ):
            left, right, changed = compare_items(a, b)
            if changed:
                diffs.append(render_diff_panel(left, right, i))

        if diffs:  # Diffs found. Raise exception with colorized output.
            _raise_with_diff(diffs)

    return _matcher
