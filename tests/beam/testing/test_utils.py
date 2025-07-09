import pytest

from apache_beam.testing.util import BeamAssertException

from gfw.common.beam.testing.utils import _default_equals_fn, equal_to


def test_default_equals_fn():
    assert _default_equals_fn(1, 1)
    assert not _default_equals_fn(1, 2)


def test_equal_to_match_exact_order():
    expected = [1, 2, 3]
    actual = [1, 2, 3]

    matcher = equal_to(expected)
    matcher(actual)  # should not raise


def test_equal_to_match_different_order():
    expected = [1, 2, 3]
    actual = [3, 1, 2]

    matcher = equal_to(expected)
    matcher(actual)  # should not raise


def test_equal_to_empty_lists():
    matcher = equal_to([])
    matcher([])  # should not raise


def test_equal_to_mismatch_raises():
    expected = [1, 2]
    actual = [1, 3]

    matcher = equal_to(expected)

    with pytest.raises(BeamAssertException) as e:
        matcher(actual)

    # The exception message should contain a substring hinting at mismatch
    assert "PCollection contents differ" in str(e.value)


def test_equal_to_type_error_handling():
    class Uncomparable:
        def __eq__(self, other):
            raise TypeError()

    a = [Uncomparable()]
    b = [Uncomparable()]

    def safe_equals(x, y):
        return type(x) is type(y)

    matcher = equal_to(b, equals_fn=safe_equals)

    # Should not raise, because our fallback considers them equal by type
    matcher(a)


def test_equal_to_custom_equals_fn():
    expected = [1, 2, 3]
    actual = [3, 2, 1]

    def reversed_equals(e, a):
        return e == a

    matcher = equal_to(expected, equals_fn=reversed_equals)
    matcher(actual)  # should not raise

    # A custom equals that never matches causes exception
    def never_equals(e, a):
        return False

    matcher = equal_to(expected, equals_fn=never_equals)
    with pytest.raises(BeamAssertException):
        matcher(actual)


def test_equal_to_handles_nested_dicts_order():
    expected = [{"b": 1, "a": 2}]
    actual = [{"a": 2, "b": 1}]

    matcher = equal_to(expected)
    matcher(actual)  # Should not raise because dict keys sorted recursively


def test_equal_to_handles_unmatched_extra_and_missing():
    expected = [1, 2]
    actual = [1, 2, 3]

    matcher = equal_to(expected)

    with pytest.raises(BeamAssertException):
        matcher(actual)

    expected = [1, 2, 3]
    actual = [1, 2]

    matcher = equal_to(expected)
    with pytest.raises(BeamAssertException):
        matcher(actual)
