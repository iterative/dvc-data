from collections import deque
from typing import TYPE_CHECKING, Any, Callable, Iterable, Optional

from attrs import define
from dvc_objects.fs.callbacks import DEFAULT_CALLBACK, Callback

if TYPE_CHECKING:
    from .hashfile.meta import Meta
    from .hashfile.hash_info import HashInfo
    from .index import BaseDataIndex, DataIndexKey

from .index import DataIndexDirError, DataIndexEntry

ADD = "add"
MODIFY = "modify"
RENAME = "rename"
DELETE = "delete"
UNCHANGED = "unchanged"
UNKNOWN = "unknown"


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
        elif self.typ == DELETE:
            entry = self.old
        else:
            entry = self.old or self.new

        assert entry
        assert entry.key is not None
        return entry.key

    def __bool__(self):
        return self.typ != UNCHANGED


def _diff_meta(
    old: Optional["Meta"],
    new: Optional["Meta"],
    *,
    cmp_key: Optional[Callable[["Meta"], Any]] = None,
):
    if old is None and new is not None:
        return ADD

    if old is not None and new is None:
        return DELETE

    if cmp_key is None and old != new:
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
    unknown: Optional[bool] = False,
):
    if unknown:
        return UNKNOWN

    old_hi = old.hash_info if old else None
    new_hi = new.hash_info if new else None
    old_meta = old.meta if old else None
    new_meta = new.meta if new else None

    meta_diff = _diff_meta(old_meta, new_meta, cmp_key=meta_cmp_key)
    hi_diff = _diff_hash_info(old_hi, new_hi)

    if old is None and new is not None:
        entry_diff = ADD
    elif old is not None and new is None:
        entry_diff = DELETE
    else:
        entry_diff = UNCHANGED

    if meta_only:
        return meta_diff

    if hash_only:
        return hi_diff

    if entry_diff != UNCHANGED:
        return entry_diff

    # If both meta's are None, return hi_diff
    if meta_diff == UNCHANGED and old_meta is None:
        return hi_diff

    # If both hi's are falsey, return meta_diff
    if hi_diff == UNCHANGED and not old_hi:
        return meta_diff

    # Only return UNCHANGED/ADD/DELETE when hi_diff and meta_diff match,
    # otherwise return MODIFY
    if meta_diff == hi_diff == entry_diff:
        return meta_diff

    return MODIFY


def _get_items(
    index: Optional["BaseDataIndex"],
    key,
    entry,
    *,
    shallow=False,
    with_unknown=False,
):
    items = {}
    unknown = False

    try:
        if index is not None and not (shallow and entry and entry.hash_info):
            items = dict(index.ls(key, detail=True))
    except KeyError:
        pass
    except DataIndexDirError:
        unknown = with_unknown

    return items, unknown


def _diff(
    old: Optional["BaseDataIndex"],
    new: Optional["BaseDataIndex"],
    *,
    with_unchanged: Optional[bool] = False,
    with_unknown: Optional[bool] = False,
    hash_only: Optional[bool] = False,
    meta_only: Optional[bool] = False,
    meta_cmp_key: Optional[Callable[["Meta"], Any]] = None,
    shallow: Optional[bool] = False,
    callback: Callback = DEFAULT_CALLBACK,
):
    old_root_items = {}
    new_root_items = {}

    if old is not None:
        try:
            old_root_items[()] = old.info(())
        except FileNotFoundError:
            pass

    if new is not None:
        try:
            new_root_items[()] = new.info(())
        except FileNotFoundError:
            pass

    todo = deque([(old_root_items, new_root_items, False)])
    while todo:
        old_items, new_items, unknown = todo.popleft()
        for key in callback.wrap(old_items.keys() | new_items.keys()):
            old_info = old_items.get(key) or {}
            new_info = new_items.get(key) or {}

            old_entry = old_info.get("entry")
            new_entry = new_info.get("entry")

            typ = _diff_entry(
                old_entry,
                new_entry,
                hash_only=hash_only,
                meta_only=meta_only,
                meta_cmp_key=meta_cmp_key,
                unknown=unknown,
            )

            if (
                hash_only
                and not with_unchanged
                and not unknown
                and typ == UNCHANGED
                and old_entry
                and old_entry.hash_info
                and old_entry.hash_info.isdir
            ):
                # NOTE: skipping the whole branch since we know it is unchanged
                pass
            elif (
                old_info.get("type") == "directory"
                or new_info.get("type") == "directory"
            ):
                kwargs = {"shallow": shallow, "with_unknown": with_unknown}
                old_dir_items, old_unknown = _get_items(
                    old, key, old_entry, **kwargs
                )
                new_dir_items, new_unknown = _get_items(
                    new, key, new_entry, **kwargs
                )
                dir_unknown = old_unknown or new_unknown
                todo.append((old_dir_items, new_dir_items, dir_unknown))

            if old_entry is None and new_entry is None:
                continue

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
    with_unknown: Optional[bool] = False,
    hash_only: Optional[bool] = False,
    meta_only: Optional[bool] = False,
    meta_cmp_key: Optional[Callable[["Meta"], Any]] = None,
    shallow: Optional[bool] = False,
    callback: Callback = DEFAULT_CALLBACK,
):
    changes = _diff(
        old,
        new,
        with_unchanged=with_unchanged,
        with_unknown=with_unknown,
        hash_only=hash_only,
        meta_only=meta_only,
        meta_cmp_key=meta_cmp_key,
        shallow=shallow,
        callback=callback,
    )

    if with_renames and old is not None and new is not None:
        assert not meta_only
        yield from _detect_renames(changes)
    else:
        yield from changes
