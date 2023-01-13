from typing import TYPE_CHECKING

from .diff import UNCHANGED, diff

if TYPE_CHECKING:
    from .index import BaseDataIndex, DataIndex


def update(new: "DataIndex", old: "BaseDataIndex") -> None:
    for change in diff(old, new, with_unchanged=True, meta_only=True):
        if change.typ == UNCHANGED:
            assert change.new is not None and change.old is not None
            change.new.hash_info = change.old.hash_info
