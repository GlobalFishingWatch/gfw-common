import pytest

from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to
from apache_beam.io.gcp import pubsub

from gfw.common.beam.transforms import ReadAndDecodeFromPubSub, FakeReadFromPubSub


def test_read_and_decode_from_pubsub(monkeypatch):
    """Test ReadAndDecodeFromPubSub with mocked PubSub input and UTF-8 decoding."""

    pubsub_messages = [
        dict(
            data=b'{"test": 123}',
            attributes={"key": "value"},
        )
    ]

    monkeypatch.setattr(pubsub, "ReadFromPubSub", FakeReadFromPubSub)

    with TestPipeline() as p:
        output = (
            p
            | "ReadAndDecode" >> ReadAndDecodeFromPubSub(
                subscription_id="test-sub",
                project="test-project",
                decode=True,
                decode_method="utf-8",
                # read_from_pubsub_factory=FakeReadFromPubSub,
                messages=pubsub_messages,
            )
        )

        expected = [
            {
                "data": '{"test": 123}',
                "metadata": {"key": "value"}
            }
        ]

        assert_that(output, equal_to(expected))


def test_invalid_decode_method():
    with pytest.raises(ValueError):
        _ = ReadAndDecodeFromPubSub(
            subscription_id="test-sub",
            project="test-project",
            decode_method="INVALID",
        )
