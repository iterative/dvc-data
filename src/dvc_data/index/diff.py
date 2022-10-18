from typing import TYPE_CHECKING, Iterable, Optional

from attrs import define

if TYPE_CHECKING:
    from .index import BaseDataIndex

from .index import DataIndexEntry

ADD = "add"
MODIFY = "modify"
RENAME = "rename"
DELETE = "delete"
UNCHANGED = "unchanged"


@define(frozen=True, hash=True, order=True)
class Change:
    typ: str
    old: Optional[DataIndexEntry]
    new: Optional[DataIndexEntry]

    def __bool__(self):
        return self.typ != UNCHANGED


def _diff(
    old: Optional["BaseDataIndex"],
    new: Optional["BaseDataIndex"],
    meta_only: Optional[bool] = False,
):
    old_keys = {key for key, _ in old.iteritems()} if old else set()
    new_keys = {key for key, _ in new.iteritems()} if new else set()

    for key in old_keys | new_keys:
        old_entry = old.get(key) if old is not None else None
        new_entry = new.get(key) if new is not None else None
        old_hi = old_entry.hash_info if old_entry else None
        new_hi = new_entry.hash_info if new_entry else None
        old_meta = old_entry.meta if old_entry else None
        new_meta = new_entry.meta if new_entry else None

        typ = UNCHANGED
        if old_entry and not new_entry:
            typ = DELETE
        elif not old_entry and new_entry:
            typ = ADD
        elif not meta_only and (old_hi and new_hi) and (old_hi != new_hi):
            typ = MODIFY
        elif old_meta != new_meta:
            typ = MODIFY

        yield Change(typ, old_entry, new_entry)


def _detect_renames(changes: Iterable[Change]):
    added = []
    deleted = []

    for change in changes:
        if change.typ == ADD:
            added.append(change)
        elif change.typ == DELETE:
            deleted.append(change)
        else:
            yield change

    for change in added:
        new_entry = change.new
        assert new_entry
        index, old_entry = None, None
        for idx, ch in enumerate(deleted):
            assert ch.old
            if ch.old.hash_info == new_entry.hash_info:
                index, old_entry = idx, ch.old
                break

        if index is not None:
            del deleted[index]
            yield Change(
                RENAME,
                old_entry,
                new_entry,
            )
        else:
            yield change

    yield from deleted


def diff(
    old: Optional["BaseDataIndex"],
    new: Optional["BaseDataIndex"],
    with_renames: Optional[bool] = False,
    meta_only: Optional[bool] = False,
):
    changes = _diff(old, new, meta_only=meta_only)

    if with_renames and old is not None and new is not None:
        assert not meta_only
        yield from _detect_renames(changes)
    else:
        yield from changes
