"""General-purpose PTransforms that don't belong to a specific I/O system."""

import json
import logging

from operator import itemgetter
from typing import Any, Optional, Sequence

import apache_beam as beam

from apache_beam import PTransform
from apache_beam.pvalue import PCollection
from apache_beam.transforms.combiners import Sample
from apache_beam.transforms.window import FixedWindows


logger = logging.getLogger(__name__)


class GroupBy(beam.PTransform):
    """Wrapper around :class:`beam.GroupBy` with automatic labeling.

    This transform wraps Beam's native :class:`beam.GroupBy` and adds an automatically generated
    label based on the grouping keys. For example, grouping by `["user", "country"]`
    with `elements="Sessions"` results in a label like ``GroupSessionsByUserAndCountry``.

    If ``dict_fields=True`` (default), string positional fields are interpreted as dictionary keys
    and wrapped with :func:`operator.itemgetter`. If False, strings are treated as attribute names.

    Example:
        .. code-block:: python

            pcoll | GroupBy("user", "country", elements="Sessions")

    Args:
        *fields:
            Positional key fields to group by. If these are strings and ``dict_fields=True``,
            they will be interpreted as dictionary keys.

        elements:
            A human-readable label describing the grouped elements (e.g., ``Messages`` or
            ``Sessions``). It is used to generate the step label.

        dict_fields:
            If True (default), string fields are interpreted as dictionary keys and
            wrapped with :func:`operator.itemgetter`. Set to False to use Beam's default behavior
            (attribute access).

        **kwargs:
            Same as :class:`beam.GroupBy` interface.
    """

    def __init__(
        self, *fields: Any, elements: str = "", dict_fields: bool = True, **kwargs: Any
    ) -> None:
        self._fields = fields
        self._elements = elements
        self._dict_fields = dict_fields
        self._kwargs = kwargs

        if self._dict_fields:
            self._kwargs.update({k: itemgetter(k) for k in self._fields})
            self._fields = ()

        keys = list(self._fields) + list(self._kwargs.keys())
        super().__init__(label=self.create_label(keys, elements))

    @classmethod
    def create_label(cls, keys: Sequence[str], elements: str) -> str:
        """Generate a descriptive label for the GroupBy transform based on keys and elements.

        Constructs a label string combining the human-readable element description and
        the grouping keys, formatted in a CamelCase style joined by 'And'.

        For example, keys ``['user', 'country']`` and elements 'Sessions' result in
        ``GroupSessionsByUserAndCountry``.

        Args:
            keys:
                A sequence of key field names used for grouping.

            elements:
                A human-readable label describing the grouped elements.

        Returns:
            A formatted string label for use as the PTransform's step label.
        """
        key_label = "And".join(s.title() for s in keys)
        return f"Group{elements}By{key_label}"

    def expand(self, pcoll: PCollection) -> PCollection:
        """Applies the wrapped Beam GroupBy transform to the input PCollection."""
        return pcoll | beam.GroupBy(*self._fields, **self._kwargs)


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
