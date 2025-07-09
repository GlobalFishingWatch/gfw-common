import pytest

from gfw.common.dictionaries import copy_dict_without, filter_none_values


@pytest.mark.parametrize(
    "input_dict, expected",
    [
        ({"a": 1, "b": None, "c": 3}, {"a": 1, "c": 3}),
        ({"x": None, "y": None}, {}),
        ({}, {}),
        ({"key": 0, "flag": False, "none": None}, {"key": 0, "flag": False}),
    ],
)
def test_filter_none_values(input_dict, expected):
    result = filter_none_values(input_dict)
    assert result == expected
    # Make sure original dict is not modified
    assert input_dict == input_dict.copy()


@pytest.mark.parametrize(
    "original, keys_to_remove, expected",
    [
        ({"a": 1, "b": 2, "c": 3}, ["b", "x"], {"a": 1, "c": 3}),  # Ignore non existing keys
        ({"key": "value"}, [], {"key": "value"}),  # remove no keys
        ({"k1": 10, "k2": 20}, ["k1", "k2"], {}),  # remove all keys
        ({}, ["any"], {}),  # empty dictionary
    ],
)
def test_copy_dict_without(original, keys_to_remove, expected):
    result = copy_dict_without(original, keys_to_remove)
    assert result == expected
    # Also ensure original dict is unchanged
    assert original == original.copy()
