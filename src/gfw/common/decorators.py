"""Module that contains simple function decorators."""

import logging

from functools import wraps
from time import time
from typing import Any, Callable, Generator


logger = logging.getLogger(__name__)


def counter(
    items: Generator[Any, None, None], logger: logging.Logger = logger, message: str = "{count}"
) -> Generator[Any, None, None]:
    """Decorator generator function to log the number of items that are being iterated.

       This is particularly useful when you have a generator, you want to know its size
       but you don't control the function that is going to iterate it.

    Args:
        items:
            Items to be iterated.

        logger:
            The logger to use.

        message:
            The string template to use as message.
            Must include an entry for a "count" variable.
    """
    count = 0
    for item in items:
        count += 1
        yield item

    logger.info(message.format(count=count))


def timing(
    f: Callable[..., Any], quiet: bool = False, logger: logging.Logger = logger, decimals: int = 4
) -> Callable[..., tuple[Any, float]]:
    """Decorator to measure execution time of a function.

    Args:
        f:
            The function to time.

        quiet:
            If true, doesn't log the elapsed time.

        logger:
            The logger to use.

        decimals:
            How many decimals to use when rounding.
            Only used for logging.

    Returns:
        :class:`Callable`:
            A wrapper around the original callable that returns a 2D tuple with:

            - Whatever the original callable returns.
            - The elapsed time.
    """

    @wraps(f)
    def wrap(*args: Any, **kwargs: Any) -> tuple[Any, float]:
        start_time = time()
        result = f(*args, **kwargs)
        end_time = time()
        elapsed_time = end_time - start_time

        if not quiet:
            logger.info("func: {} took: {} sec".format(f.__name__, round(elapsed_time, decimals)))

        return result, elapsed_time

    return wrap
