"""This module contains a custom Apache Beam `PTransform` called `ReadAndDecodeFromPubSub`.

The `ReadAndDecodeFromPubSub` class is designed to read messages from a Google Cloud Pub/Sub
subscription, decode them (if necessary),
and return the messages in a structured format as a dictionary.
"""

import codecs
import logging

from functools import cached_property
from typing import Any, Callable, Optional

import apache_beam as beam

from apache_beam.io.gcp.pubsub import PubsubMessage, ReadFromPubSub
from apache_beam.pvalue import PCollection


logger = logging.getLogger(__name__)


class FakeReadFromPubSub(beam.PTransform[Any, Any]):
    """A fake ReadFromPubSub to simulate Pub/Sub messages in tests."""

    def __init__(
        self,
        *args: Any,
        messages: Optional[list[dict]] = None,
        **kwargs: Any,
    ) -> None:
        self.messages = messages or []

    def expand(self, pcoll: PCollection) -> PCollection:
        """Returns a PCollection created from self.messages list."""
        return pcoll | beam.Create([PubsubMessage(**m) for m in self.messages])


class ReadAndDecodeFromPubSub(beam.PTransform[Any, Any]):
    """A custom PTransform for reading from Pub/Sub and optionally decoding messages.

    It supports the following features:
    - Reading from a specific Pub/Sub subscription.
    - Optionally including message attributes.
    - Decoding message data using a specified method (default is UTF-8).
    - Allowing the use of a custom or mocked `ReadFromPubSub` transform for testing purposes.

    Args:
        subscription_id:
            The Pub/Sub subscription id from which to read messages.

        with_attributes:
            Whether to include attributes in the Pub/Sub message.
            Default is True.

        decode:
            Whether to decode the data from bytes to dictionary.
            Default is True.

        decode_method:
            The method used to decode the message data.
            Supported methods include standard encodings like "utf-8", "ascii", etc.
            Default is "utf-8".

        read_from_pubsub_factory:
            A factory function to create a ReadFromPubSub instance.
            This is useful for testing when a custom or mocked ReadFromPubSub
            implementation is needed. Default is the Beam ReadFromPubSub class.

        **read_from_pubsub_kwargs:
            Additional keyword arguments passed to the `ReadFromPubSub` transform.
            These can be used to specify custom parameters for the reading operation.
    """

    SUBSCRIPTION = "projects/{project}/subscriptions/{subscription}"

    def __init__(
        self,
        subscription_id: str,
        project: str,
        with_attributes: bool = True,
        decode: bool = True,
        decode_method: str = "utf-8",
        read_from_pubsub_factory: Callable[..., ReadFromPubSub] = ReadFromPubSub,
        **read_from_pubsub_kwargs: Any,
    ) -> None:
        self._subscription_id = subscription_id
        self._project = project
        self._with_attributes = with_attributes
        self._decode = decode
        self._decode_method = decode_method
        self._read_from_pubsub_factory = read_from_pubsub_factory
        self._read_from_pubsub_kwargs = read_from_pubsub_kwargs

        self._validate_decode_method()

    @classmethod
    def get_client_factory(cls, mocked: bool = False) -> Callable:
        """Returns a factory for ReadFromPubSub objects."""
        if mocked:
            return FakeReadFromPubSub

        return ReadFromPubSub

    @cached_property
    def subscription(self) -> str:
        """Generates the full subscription path from project and subscription id."""
        return self.SUBSCRIPTION.format(project=self._project, subscription=self._subscription_id)

    def expand(self, pcoll: PCollection) -> PCollection:
        """Applies the transform to the pipeline root and returns a PCollection of messages.

        Args:
            pcoll:
                An input PCollection. This is expected to be a `PBegin` when used with a real
                or mocked `ReadFromPubSub`, since Pub/Sub sources begin from the pipeline root.

        Returns:
            beam.PCollection:
                A PCollection of dictionaries where each dictionary contains:
                - "data": the decoded message string (if decoding is enabled),
                - "attributes": a dictionary of message attributes (if available).
        """
        messages = pcoll | self._read_from_pubsub_factory(
            subscription=self.subscription,
            with_attributes=self._with_attributes,
            **self._read_from_pubsub_kwargs,
        )

        return messages | "ToDict" >> beam.Map(self._to_dict)

    def _to_dict(self, message: PubsubMessage) -> dict:
        data = message.data
        if self._decode:
            data = message.data.decode(self._decode_method)

        return {"data": data, "attributes": message.attributes}

    def _validate_decode_method(self) -> None:
        try:
            codecs.lookup(self._decode_method)
        except LookupError as e:
            raise ValueError(f"Unsupported decode method: {self._decode_method}") from e

        logging.debug(f"Using decode method: {self._decode_method}.")
