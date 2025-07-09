import pytest

from apache_beam.io.gcp.pubsub import ReadFromPubSub
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to

from gfw.common.beam.transforms import FakeReadFromPubSub, ReadAndDecodeFromPubSub


def test_subscription_property():
    subscription_id = "sub"
    project = "project"

    transform = ReadAndDecodeFromPubSub(
        subscription_id=subscription_id,
        project=project,
        decode=False,
    )
    assert transform.subscription == ReadAndDecodeFromPubSub.SUBSCRIPTION.format(
        project=project, subscription=subscription_id
    )


def test_get_client_factory_returns_fake():
    factory = ReadAndDecodeFromPubSub.get_client_factory(mocked=True)
    assert factory is FakeReadFromPubSub


def test_get_client_factory_returns_real():
    factory = ReadAndDecodeFromPubSub.get_client_factory(mocked=False)
    assert factory is ReadFromPubSub


def test_read_and_decode_from_pubsub():
    """Test ReadAndDecodeFromPubSub with mocked PubSub input and UTF-8 decoding."""
    pubsub_messages = [
        {
            "data": b'{"test": 123}',
            "attributes": {
                "key2": "value2",
                "key1": "value1",
            },
        }
    ]

    with TestPipeline() as p:
        output = p | "ReadAndDecode" >> ReadAndDecodeFromPubSub(
            subscription_id="test-sub",
            project="test-project",
            decode=True,
            decode_method="utf-8",
            read_from_pubsub_factory=FakeReadFromPubSub,
            messages=pubsub_messages,
        )

        expected = [
            {
                "data": '{"test": 123}',
                "attributes": {
                    "key1": "value1",
                    "key2": "value2",
                },
            }
        ]

        assert_that(output, equal_to(expected))


def test_read_without_decoding():
    """Test ReadAndDecodeFromPubSub when decoding is disabled."""
    pubsub_messages = [
        {
            "data": b"some-bytes",
            "attributes": {"source": "test"},
        }
    ]

    with TestPipeline() as p:
        output = p | "ReadRawPubSub" >> ReadAndDecodeFromPubSub(
            subscription_id="sub",
            project="project",
            decode=False,
            read_from_pubsub_factory=FakeReadFromPubSub,
            messages=pubsub_messages,
        )

        # Expect raw PubsubMessage objects
        assert_that(output, equal_to([pubsub_messages[0]]))


def test_invalid_decode_method():
    with pytest.raises(ValueError):
        _ = ReadAndDecodeFromPubSub(
            subscription_id="test-sub",
            project="test-project",
            decode_method="INVALID",
        )
