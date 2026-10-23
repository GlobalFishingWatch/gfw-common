"""General-purpose PTransforms that don't belong to a specific I/O system."""

import datetime
import json
import logging
import struct

from operator import itemgetter
from typing import Any, Callable, Iterable, Optional, Sequence, get_type_hints

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


_EPOCH = datetime.datetime.fromtimestamp(0, datetime.UTC)
_MICROSECOND = datetime.timedelta(microseconds=1)


def _datetime_to_us(value: datetime.datetime) -> int:
    """Converts a timezone-aware datetime to integer microseconds since the epoch, exactly."""
    return (value - _EPOCH) // _MICROSECOND


def _us_to_datetime(value: int) -> datetime.datetime:
    """Converts integer microseconds since the epoch back to a UTC datetime."""
    return _EPOCH + datetime.timedelta(microseconds=value)


# (serializer, deserializer) of each field type that needs one to go through the shuffle as a
# value Beam's FastPrimitivesCoder encodes natively. NamedTuple fields get one from
# _packed_converters; fields of any other type pass unchanged.
_CONVERTERS: dict[Any, tuple[Callable[[Any], Any], Callable[[Any], Any]]] = {
    datetime.datetime: (_datetime_to_us, _us_to_datetime),
}

# struct formats of the field types a NamedTuple can be packed from: fixed-size and exact.
_STRUCT_FORMATS: dict[Any, str] = {float: "d", bool: "?"}


class _PackedNamedTuple:
    """Packs a NamedTuple of fixed-size fields into bytes, and unpacks it.

    Packed, a NamedTuple of two floats takes 18 bytes, as two floats in the shuffled tuple do:
    a bytes tag and length, then 16 bytes, where a nested tuple would add its own tag and length
    to the floats' 9 bytes each (a tag, then 8 bytes).

    It keeps the format string, not a struct.Struct, since Beam pickles the transform and a
    struct.Struct can't be pickled; struct caches the compiled format.
    """

    def __init__(self, record_type: Any, fmt: str) -> None:
        self._record_type = record_type
        self._fmt = fmt

    def serialize(self, value: Any) -> bytes:
        return struct.pack(self._fmt, *value)

    def deserialize(self, data: bytes) -> Any:
        return self._record_type._make(struct.unpack(self._fmt, data))


def _packed_converters(
    field_type: Any,
) -> Optional[tuple[Callable[[Any], Any], Callable[[Any], Any]]]:
    """Returns the (serializer, deserializer) packing a NamedTuple field type into bytes.

    Only a NamedTuple whose fields are all annotated with a type in _STRUCT_FORMATS can be
    packed; for any other type, returns None.
    """
    if not hasattr(field_type, "_fields"):
        return None

    hints = get_type_hints(field_type)
    formats = []
    for name in field_type._fields:
        fmt = _STRUCT_FORMATS.get(hints.get(name))
        if fmt is None:
            return None

        formats.append(fmt)

    if not formats:
        return None

    packer = _PackedNamedTuple(field_type, "<" + "".join(formats))
    return packer.serialize, packer.deserialize


class ShrinkGroupBy(GroupBy):
    """Groups records by key, omitting the key fields from the shuffled values.

    Works like GroupBy over a NamedTuple record type, but shuffles each record shrunken:
    a plain tuple of its non-key fields, with the field types in _CONVERTERS converted to
    primitives (datetimes become integer microseconds since epoch), and NamedTuple fields of floats
    and bools (like LatLon) packed into bytes. The records come back rebuilt from the key and
    the shrunken values.

    Field types are read from the record type's annotations. The record type is a plain
    NamedTuple — no base class or decorator required. The step label comes from GroupBy,
    like ``GroupPositionsBySsvidAndVessel_Id``.

    Example:
        .. code-block:: python

            records | ShrinkGroupBy("ssvid", "vessel_id", record_type=VisitLocationRecord)

    Args:
        *fields:
            Names of the record fields to group by. The key carries them, so they are left
            out of the shuffled values.

        record_type:
            A NamedTuple class whose records will be shrunken and restored.

        elements:
            A human-readable label describing the grouped elements (e.g., ``Positions``).
            It is used to generate the step label.

        **computed:
            Extra keys computed from each record, like a date from its timestamp. They go in
            the key after the fields, and the record keeps the fields they come from.

    Input: a PCollection of records of `record_type`.
    Output: a PCollection of (key, list[record]) pairs. The key is a tuple of the field values
    and then the computed values, or a single value if there is only one key, like GroupBy's.
    """

    def __init__(
        self,
        *fields: str,
        record_type: Any,
        elements: str = "",
        **computed: Callable[[Any], Any],
    ) -> None:
        self._validate(record_type, fields, computed)

        super().__init__(*fields, elements=elements, dict_fields=False, **computed)
        self._record_type = record_type
        self._single_key = len(fields) + len(computed) == 1

        self._key_indices = tuple(record_type._fields.index(name) for name in fields)
        self._key_getters: tuple[Callable[[Any], Any], ...] = (
            *(itemgetter(i) for i in self._key_indices),
            *computed.values(),
        )

        self._keep_indices = tuple(
            i for i, name in enumerate(record_type._fields) if name not in fields
        )

        hints = get_type_hints(record_type)
        self._serializers = []
        self._deserializers = []

        for pos, i in enumerate(self._keep_indices):
            field_type = hints.get(record_type._fields[i])
            converters = _CONVERTERS.get(field_type)
            if converters is None:
                converters = _packed_converters(field_type)
            if converters is None:
                continue

            serializer, deserializer = converters
            # Shrunken tuples are indexed by position; rebuilt records by field index.
            self._serializers.append((pos, serializer))
            self._deserializers.append((i, deserializer))

    @staticmethod
    def _validate(
        record_type: Any,
        fields: tuple[str, ...],
        computed: dict[str, Any],
    ) -> None:
        """Checks that record_type is a NamedTuple, with every key field, and there is a key."""
        if not hasattr(record_type, "_fields"):
            raise TypeError(f"{record_type.__name__} must be a NamedTuple with _fields")

        if not fields and not computed:
            raise ValueError("At least one field or computed key is required")

        for name in fields:
            if name not in record_type._fields:
                raise ValueError(f"Key {name!r} is not declared in {record_type.__name__}._fields")

    # Beam infers coders from the hints of the methods it maps: the values stay Any so it keeps
    # FastPrimitivesCoder, and unshrink takes an Iterable, which is what GroupByKey outputs.
    def shrink(self, record: Any) -> tuple[Any, tuple[Any, ...]]:
        """Keys a record by its grouping key and shrinks it to its non-key fields.

        Args:
            record:
                A record of `record_type`.

        Returns:
            A (key, shrunken_tuple) pair. The shrunken tuple holds the non-key fields as
            primitives.
        """
        key = tuple([get(record) for get in self._key_getters])
        if self._single_key:
            key = key[0]

        values = [record[i] for i in self._keep_indices]
        for pos, serialize in self._serializers:
            values[pos] = serialize(values[pos])

        return key, tuple(values)

    def unshrink(self, item: tuple[Any, Iterable[tuple[Any, ...]]]) -> tuple[Any, list[Any]]:
        """Rebuilds a group's records, from the key they were grouped by.

        The key fields are the same for the whole group, so they are placed once, in a list that
        each record then fills with its own values. The computed keys are not used.

        Args:
            item:
                A (key, shrunken_values) pair.

        Returns:
            A (key, records) pair.
        """
        key, shrunken_values = item
        key_values = key
        if self._single_key:
            key_values = (key,)

        values = [None] * len(self._record_type._fields)
        for i, value in zip(self._key_indices, key_values, strict=False):
            values[i] = value

        records = []
        for shrunken in shrunken_values:
            # Append primitive values.
            for i, value in zip(self._keep_indices, shrunken, strict=True):
                values[i] = value

            # Append serialized values.
            for i, deserialize in self._deserializers:
                values[i] = deserialize(values[i])

            records.append(self._record_type._make(values))

        return key, records

    def expand(self, records: PCollection) -> PCollection:
        """Shrinks records, groups them by key, and restores the original records.

        Args:
            records:
                A PCollection of records of `record_type`.

        Returns:
            A PCollection of (key, list[record]) pairs.
        """
        return (
            records
            | "Shrink" >> beam.Map(self.shrink)
            | "GroupByKey" >> beam.GroupByKey()
            | "Unshrink" >> beam.Map(self.unshrink)
        )


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
