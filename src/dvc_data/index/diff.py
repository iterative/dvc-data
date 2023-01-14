from typing import TYPE_CHECKING, Any, Callable, Iterable, Optional

from attrs import define

if TYPE_CHECKING:
    from .hashfile.meta import Meta
    from .hashfile.hash_info import HashInfo
    from .index import BaseDataIndex, DataIndexKey

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

    @property
    def key(self) -> "DataIndexKey":
        if self.typ == RENAME:
            raise ValueError

        if self.typ == ADD:
            entry = self.new
        else:
            entry = self.old

        assert entry
        assert entry.key
        return entry.key

    def __bool__(self):
        return self.typ != UNCHANGED


def _diff_meta(
    old: Optional["Meta"],
    new: Optional["Meta"],
    *,
    cmp_key: Optional[Callable[["Meta"], Any]] = None,
):
    if (cmp_key is None or old is None or new is None) and old != new:
        return MODIFY

    if cmp_key is not None and cmp_key(old) != cmp_key(new):
        return MODIFY

    return UNCHANGED


def _diff_hash_info(
    old: Optional["HashInfo"],
    new: Optional["HashInfo"],
):
    if not old and new:
        return ADD

    if old and not new:
        return DELETE

    if old and new and old != new:
        return MODIFY

    return UNCHANGED


def _diff_entry(
    old: Optional["DataIndexEntry"],
    new: Optional["DataIndexEntry"],
    *,
    hash_only: Optional[bool] = False,
    meta_only: Optional[bool] = False,
    meta_cmp_key: Optional[Callable[["Meta"], Any]] = None,
):
    old_hi = old.hash_info if old else None
    new_hi = new.hash_info if new else None
    old_meta = old.meta if old else None
    new_meta = new.meta if new else None

    meta_diff = _diff_meta(old_meta, new_meta, cmp_key=meta_cmp_key)
    hi_diff = _diff_hash_info(old_hi, new_hi)

    if meta_only:
        return meta_diff

    if hash_only:
        return hi_diff

    if meta_diff != UNCHANGED:
        return MODIFY

    if hi_diff != UNCHANGED:
        return MODIFY

    return UNCHANGED


def _diff(
    old: Optional["BaseDataIndex"],
    new: Optional["BaseDataIndex"],
    *,
    with_unchanged: Optional[bool] = False,
    hash_only: Optional[bool] = False,
    meta_only: Optional[bool] = False,
    meta_cmp_key: Optional[Callable[["Meta"], Any]] = None,
    shallow: Optional[bool] = False,
):
    old_keys = (
        {key for key, _ in old.iteritems(shallow=shallow)} if old else set()
    )
    new_keys = (
        {key for key, _ in new.iteritems(shallow=shallow)} if new else set()
    )

    for key in old_keys | new_keys:
        old_entry = old.get(key) if old is not None else None
        new_entry = new.get(key) if new is not None else None

        typ = UNCHANGED
        if old_entry and not new_entry:
            typ = DELETE
        elif not old_entry and new_entry:
            typ = ADD
        else:
            typ = _diff_entry(
                old_entry,
                new_entry,
                hash_only=hash_only,
                meta_only=meta_only,
                meta_cmp_key=meta_cmp_key,
            )

        if typ == UNCHANGED and not with_unchanged:
            continue

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

    def _get_key(change):
        return change.key

    added[:] = sorted(added, key=_get_key)
    deleted[:] = sorted(deleted, key=_get_key)

    for change in added:
        new_entry = change.new
        assert new_entry

        if not new_entry.hash_info:
            yield change
            continue

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
    *,
    with_renames: Optional[bool] = False,
    with_unchanged: Optional[bool] = False,
    hash_only: Optional[bool] = False,
    meta_only: Optional[bool] = False,
    meta_cmp_key: Optional[Callable[["Meta"], Any]] = None,
    shallow: Optional[bool] = False,
):
    changes = _diff(
        old,
        new,
        with_unchanged=with_unchanged,
        hash_only=hash_only,
        meta_only=meta_only,
        meta_cmp_key=meta_cmp_key,
        shallow=shallow,
    )

    if with_renames and old is not None and new is not None:
        assert not meta_only
        yield from _detect_renames(changes)
    else:
        yield from changes
