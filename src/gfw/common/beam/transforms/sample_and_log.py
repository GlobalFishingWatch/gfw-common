"""This module defines a reusable Apache Beam PTransform for logging elements of a PCollection."""

import json
import logging

from typing import Any, Optional

import apache_beam as beam

from apache_beam import PTransform
from apache_beam.pvalue import PCollection
from apache_beam.transforms.combiners import Sample
from apache_beam.transforms.window import FixedWindows


logger = logging.getLogger(__name__)


class SampleAndLogElements(PTransform):
    """A Beam PTransform that logs elements of a PCollection.

    Args:
        sample_size:
            The number of elements to log. If not provided, logs all elements.

        window_size:
            The window duration in seconds used when sampling unbounded sources;
            only applicable when ``sample_size`` is set.

        pretty_print:
            If True, formats each element as pretty-printed JSON when possible.

        message:
            A custom string format for the log message. Must contain the placeholder ``{e}``.
    """

    def __init__(
        self,
        sample_size: Optional[int] = None,
        window_size: int = 60,
        pretty_print: bool = False,
        message: str = "Element: {e}",
    ) -> None:
        self._sample_size = sample_size
        self._window_size = window_size
        self._pretty_print = pretty_print
        self._message = message

    def expand(self, pcoll: PCollection) -> PCollection:
        """Log elements of a PCollection, optionally sampling a ``sample_size`` elements."""
        samples = pcoll
        if self._sample_size:
            samples = (
                pcoll
                # Windowing is needed for sampling on unbounded sources.
                | "Apply Fixed Window" >> beam.WindowInto(FixedWindows(self._window_size))
                # Defaults are not supported if you are not using a Global Window.
                | "Sample" >> Sample.FixedSizeGlobally(self._sample_size).without_defaults()
                | "Flatten Samples" >> beam.FlatMap(lambda elements: elements)
            )

        _ = samples | "Log Elements" >> beam.Map(self._log_element)

        return pcoll

    def _log_element(self, element: Any) -> Any:
        formatted = element
        if self._pretty_print:
            formatted = json.dumps(element, indent=4)

        log_message = self._message.format(e=formatted)
        logger.debug(log_message)

        return element
