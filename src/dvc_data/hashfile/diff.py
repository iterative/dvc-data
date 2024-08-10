import functools
import reprlib
from typing import TYPE_CHECKING, Optional

from attrs import asdict, define, field

if TYPE_CHECKING:
    from .db import HashFileDB
    from .hash_info import HashInfo
    from .meta import Meta
    from .obj import HashFile


ADD = "add"
MODIFY = "modify"
DELETE = "delete"
UNCHANGED = "unchanged"


@define(unsafe_hash=True, order=True)
class TreeEntry:
    cache_meta: Optional["Meta"] = field(default=None, eq=False)
    key: tuple[str, ...] = ()
    meta: Optional["Meta"] = field(default=None, eq=False)
    oid: Optional["HashInfo"] = None

    def __bool__(self):
        return bool(self.oid)

    @property
    def in_cache(self) -> bool:
        return self.cache_meta is not None


@define(unsafe_hash=True, order=True)
class Change:
    old: TreeEntry = field(factory=TreeEntry)
    new: TreeEntry = field(factory=TreeEntry)
    typ: str = field(init=False)

    @typ.default
    def _(self):
        if not self.old and not self.new:
            return UNCHANGED

        if self.old and not self.new:
            return DELETE

        if not self.old and self.new:
            return ADD

        if self.old != self.new:
            return MODIFY

        return UNCHANGED

    def __bool__(self):
        return self.typ != UNCHANGED


@define
class DiffResult:
    added: list[Change] = field(factory=list, repr=reprlib.repr)
    modified: list[Change] = field(factory=list, repr=reprlib.repr)
    deleted: list[Change] = field(factory=list, repr=reprlib.repr)
    unchanged: list[Change] = field(factory=list, repr=reprlib.repr)

    def __bool__(self):
        return bool(self.added or self.modified or self.deleted)

    @property
    def stats(self) -> dict[str, int]:
        return {
            k: len(v)
            for k, v in asdict(self, recurse=False).items()
            if k != "unchanged"
        }


ROOT = ("",)


def diff(  # noqa: C901
    old: Optional["HashFile"],
    new: Optional["HashFile"],
    cache: "HashFileDB",
) -> DiffResult:
    from .tree import Tree

    if old is None and new is None:
        return DiffResult()

    def _get_keys(obj):
        if not obj:
            return []
        return [ROOT] + ([key for key, _, _ in obj] if isinstance(obj, Tree) else [])

    old_keys = set(_get_keys(old))
    new_keys = set(_get_keys(new))

    def _get(obj, key):
        if not obj or key == ROOT:
            return None, (obj.hash_info if obj else None)
        if not isinstance(obj, Tree):
            # obj is not a Tree and key is not a ROOT
            # hence object does not exist for a given key
            return None, None
        return obj.get(key, (None, None))

    @functools.cache
    def _cache_check(oid: Optional["str"], cache: "HashFileDB") -> Optional["Meta"]:
        from dvc_objects.errors import ObjectFormatError

        if not oid:
            return None

        try:
            return cache.check(oid)
        except (FileNotFoundError, ObjectFormatError):
            return None

    ret = DiffResult()
    for key in old_keys | new_keys:
        old_meta, old_oid = _get(old, key)
        new_meta, new_oid = _get(new, key)

        old_cache_meta = _cache_check(old_oid.value, cache) if old_oid else None
        new_cache_meta = _cache_check(new_oid.value, cache) if new_oid else None
        change = Change(
            old=TreeEntry(old_cache_meta, key, old_meta, old_oid),
            new=TreeEntry(new_cache_meta, key, new_meta, new_oid),
        )

        if change.typ == ADD:
            ret.added.append(change)
        elif change.typ == MODIFY:
            ret.modified.append(change)
        elif change.typ == DELETE:
            ret.deleted.append(change)
        else:
            assert change.typ == UNCHANGED
            ret.unchanged.append(change)
    return ret
