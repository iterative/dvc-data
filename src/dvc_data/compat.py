import sys
from collections.abc import Iterable, Iterator
from itertools import islice
from typing import TYPE_CHECKING, TypeVar

if sys.version_info >= (3, 12) or TYPE_CHECKING:
    from functools import cached_property  # noqa: TID251
else:
    from funcy import cached_property  # noqa: TID251


T = TypeVar("T")


def batched(iterable: Iterable[T], n: int) -> Iterator[tuple[T, ...]]:
    if n < 1:
        raise ValueError("n must be at least one")
    it = iter(iterable)
    while batch := tuple(islice(it, n)):
        yield batch


__all__ = ["batched", "cached_property"]
