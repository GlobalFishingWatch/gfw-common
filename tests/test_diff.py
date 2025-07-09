from rich.columns import Columns
from rich.panel import Panel

from gfw.common import diff


def test_diff_lines_no_difference():
    a = "line1\nline2"
    b = "line1\nline2"
    left, right, changed = diff.diff_lines(a, b)
    assert left == "  line1\n  line2"
    assert right == "  line1\n  line2"
    assert changed is False


def test_diff_lines_with_difference():
    a = "line1\nline2"
    b = "line1\nline3"
    left, right, changed = diff.diff_lines(a, b)
    assert "[red]- line2[/red]" in left
    assert "[green]+ line3[/green]" in right
    assert changed is True


def test_compare_items_identical_objects():
    a = {"foo": [1, 2, 3]}
    b = {"foo": [1, 2, 3]}
    left, right, changed = diff.compare_items(a, b)
    # Since identical, no diff markup expected
    assert "foo" in left and "foo" in right
    assert changed is False


def test_compare_items_different_objects():
    a = {"foo": [1, 2, 3]}
    b = {"foo": [1, 2, 4]}
    left, right, changed = diff.compare_items(a, b)
    # Since pretty_repr emits the whole dict in one line, expect the entire line to be marked
    assert "[red]- {'foo': [1, 2, 3]}" in left or "[red]- {'foo': [1, 2, 3]}" in right
    assert changed is True


def test_render_diff_panel_returns_columns():
    left = "some left text"
    right = "some right text"
    idx = 1
    panel = diff.render_diff_panel(left, right, idx)
    assert isinstance(panel, Columns)
    # Should contain two Panel objects
    assert len(panel.renderables) == 2
    assert all(isinstance(p, Panel) for p in panel.renderables)
    assert panel.renderables[0].title == f"Actual #{idx}"
    assert panel.renderables[1].title == f"Expected #{idx}"
