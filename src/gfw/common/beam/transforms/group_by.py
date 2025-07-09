"""Custom Apache Beam GroupBy transform with automatic labeling.

This module defines a wrapper around Beam's native `GroupBy` PTransform that adds
a dynamically generated label based on the grouping keys. This helps improve
pipeline readability and simplifies debugging, especially in Dataflow graphs
where stage names are important for traceability.
"""

import logging

from operator import itemgetter
from typing import Any, Sequence

import apache_beam as beam

from apache_beam.pvalue import PCollection


logger = logging.getLogger(__name__)


class GroupBy(beam.PTransform):
    """A labeled wrapper around Apache Beam's `GroupBy` transform for grouping by keys.

    This transform wraps Beam's native `GroupBy` and adds an automatically generated
    label based on the grouping keys. For example, grouping by `["user", "country"]`
    with `elements="Sessions"` results in a label like `"GroupSessionsByUserAndCountry"`.

    If `dict_fields=True` (default), string positional fields are interpreted as dictionary keys
    and wrapped with `operator.itemgetter`. If False, strings are treated as attribute names.

    Example:
    pcoll | GroupBy("user", "country", elements="Sessions")

    Args:
        *fields:
            Positional key fields to group by. If these are strings and `dict_fields=True`,
            they will be interpreted as dictionary keys.

        elements:
            A human-readable label describing the grouped elements (e.g., "Messages" or
            "Sessions"). It is used to generate the step label.

        dict_fields:
            If True (default), string fields are interpreted as dictionary keys and
            wrapped with `itemgetter`. Set to False to use Beam's default behavior
            (attribute access).

        **kwargs:
            Same as beam.GroupBy interface.
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

        For example, keys ['user', 'country'] and elements 'Sessions' result in
        'GroupSessionsByUserAndCountry'.

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
