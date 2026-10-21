"""Contract for records that can be grouped by key without repeating the key in every value."""

from abc import ABC, abstractmethod
from typing import Any


class ShrinkableRecord(ABC):
    """Base class for records that can be grouped by key without repeating the key in every value.

    Beam's ``GroupByKey`` puts the whole record in the value, whatever the key already holds is
    written again into every shuffled value: a coder only ever sees the value. A record that
    implements this contract says how to leave the key's content out of the value and how to put
    it back, so grouping a ``PCollection`` by key costs no more shuffled bytes than it must.

    Subclasses implement two methods:

    * :meth:`shrink` -- the record in the form it goes through the shuffle.
    * :meth:`unshrink` -- the record again, from its key and one shrunken value.

    Both are abstract, and the methods are named for the pair, so a subclass can't accidentally
    inherit a no-op that shuffles more than it looks.

    Example:
        >>> class VesselRecord(ShrinkableRecord, NamedTuple):
        ...     identifier: str
        ...     timestamp: datetime
        ...     lat: float
        ...     lon: float
        ...
        ...     def shrink(self, key):
        ...         return (self.timestamp, self.lat, self.lon)
        ...
        ...     @classmethod
        ...     def unshrink(cls, key, value):
        ...         timestamp, lat, lon = value
        ...         return cls(key, timestamp, lat, lon)

    ``key`` is whatever the pipeline groups by, and it reaches :meth:`unshrink` whole. A record
    whose key holds more than an identifier -- a date, a cell -- can still rebuild itself.
    """

    @abstractmethod
    def shrink(self, key: Any) -> Any:
        """Returns the record in the form it goes through the shuffle.

        Args:
            key:
                The ``GroupByKey`` key the record will be grouped by. A subclass leaves out of
                the returned value whatever the key already carries.

        Returns:
            The value to shuffle in place of the record.
        """
        ...

    @classmethod
    @abstractmethod
    def unshrink(cls, key: Any, value: Any) -> "ShrinkableRecord":
        """Rebuilds a record from its key and one of the values :meth:`shrink` returned.

        Args:
            key:
                The ``GroupByKey`` key the record was grouped by, as it was handed to
                :meth:`shrink`.
            value:
                One value :meth:`shrink` returned for a record of this key.

        Returns:
            The rebuilt record.
        """
        ...
