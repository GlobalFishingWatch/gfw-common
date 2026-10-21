import pytest

from gfw.common.beam.transforms import ShrinkableRecord


class Record(ShrinkableRecord):
    """A record that leaves its body to the key and shuffles the rest."""

    def __init__(self, identifier, body):
        self.identifier = identifier
        self.body = body

    def shrink(self, key):
        return self.body

    @classmethod
    def unshrink(cls, key, value):
        return cls(key, value)


def test_shrink_returns_the_value_to_shuffle():
    record = Record("a", "body")

    assert record.shrink("a") == "body"


def test_unshrink_rebuilds_the_record_from_the_key():
    rebuilt = Record.unshrink("a", "body")

    assert (rebuilt.identifier, rebuilt.body) == ("a", "body")


def test_the_key_reaches_unshrink_whole():
    """A record can read its whole key, not only the identifier in it."""
    key = ("a", "2024-01-02")

    rebuilt = Record.unshrink(key, "body")

    assert rebuilt.identifier == key


def test_the_contract_cannot_be_used_without_implementing_it():
    class Incomplete(ShrinkableRecord):
        pass

    with pytest.raises(TypeError):
        Incomplete()


def test_shrink_is_abstract():
    with pytest.raises(TypeError):
        ShrinkableRecord()
