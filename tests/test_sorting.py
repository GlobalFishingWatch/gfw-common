from gfw.common.sorting import sort_dicts


def test_sort_dicts_simple_dict():
    input_data = {"b": 2, "a": 1}
    expected = {"a": 1, "b": 2}
    assert sort_dicts(input_data) == expected


def test_sort_dicts_nested_dict():
    input_data = {"b": {"y": 2, "x": 1}, "a": 3}
    expected = {"a": 3, "b": {"x": 1, "y": 2}}
    assert sort_dicts(input_data) == expected


def test_sort_dicts_list_of_dicts():
    input_data = [{"b": 2, "a": 1}, {"d": 4, "c": 3}]
    expected = [{"a": 1, "b": 2}, {"c": 3, "d": 4}]
    assert sort_dicts(input_data) == expected


def test_sort_dicts_tuple_of_dicts():
    input_data = ({"b": 2, "a": 1}, {"d": 4, "c": 3})
    expected = ({"a": 1, "b": 2}, {"c": 3, "d": 4})
    assert sort_dicts(input_data) == expected


def test_sort_dicts_mixed_nested():
    input_data = {
        "z": [{"b": 2, "a": 1}, {"d": 4, "c": 3}],
        "y": ({"f": 6, "e": 5},),
        "x": 0,
    }
    expected = {
        "x": 0,
        "y": ({"e": 5, "f": 6},),
        "z": [{"a": 1, "b": 2}, {"c": 3, "d": 4}],
    }
    assert sort_dicts(input_data) == expected


def test_sort_dicts_non_dict_types():
    input_data = 42
    assert sort_dicts(input_data) == 42

    input_data = "string"
    assert sort_dicts(input_data) == "string"

    input_data = None
    assert sort_dicts(input_data) is None


def test_sort_dicts_empty_structures():
    assert sort_dicts({}) == {}
    assert sort_dicts([]) == []
    assert sort_dicts(()) == ()


def test_sort_dicts_preserves_tuple_type():
    input_data = ({"b": 2, "a": 1},)
    output = sort_dicts(input_data)
    assert isinstance(output, tuple)
    assert output == ({"a": 1, "b": 2},)
