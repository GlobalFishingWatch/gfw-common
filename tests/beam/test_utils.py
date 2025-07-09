from apache_beam.utils.timestamp import Timestamp

from gfw.common.beam.utils import float_to_beam_timestamp


def test_float_to_beam_timestamp():
    row = {"event_time": 1717777777.123, "other_time": 123456.789, "unchanged_field": "foo"}
    fields = ["event_time", "other_time"]

    result = float_to_beam_timestamp(row, fields)

    assert isinstance(result["event_time"], Timestamp)
    assert isinstance(result["other_time"], Timestamp)

    assert result["event_time"] == Timestamp(1717777777.123)
    assert result["other_time"] == Timestamp(123456.789)
    assert result["unchanged_field"] == "foo"
