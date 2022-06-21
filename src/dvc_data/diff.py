from dataclasses import dataclass, field
from typing import TYPE_CHECKING, List, Optional, Tuple

if TYPE_CHECKING:
    from dvc_objects.file import HashFile

    from .hashfile.hash_info import HashInfo
    from .hashfile.meta import Meta


ADD = "add"
MODIFY = "modify"
DELETE = "delete"
UNCHANGED = "unchanged"


class TreeEntry:
    __slots__ = ("in_cache", "key", "meta", "oid")

    def __init__(
        self,
        in_cache: bool,
        key: Tuple[str],
        meta: Optional["Meta"],
        oid: Optional["HashInfo"],
    ):
        self.in_cache = in_cache
        self.key = key
        self.meta = meta
        self.oid = oid

    def __bool__(self):
        return bool(self.oid)

    def __eq__(self, other):
        if not isinstance(other, TreeEntry):
            return False

        if self.key != other.key:
            return False

        return self.oid == other.oid


class Change:
    __slots__ = ("old", "new")

    def __init__(self, old: TreeEntry, new: TreeEntry):
        self.old = old
        self.new = new

    @property
    def typ(self):
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


@dataclass
class DiffResult:
    added: List[Change] = field(default_factory=list, compare=True)
    modified: List[Change] = field(default_factory=list, compare=True)
    deleted: List[Change] = field(default_factory=list, compare=True)
    unchanged: List[Change] = field(default_factory=list, compare=True)

    def __bool__(self):
        return bool(self.added or self.modified or self.deleted)


ROOT = ("",)


def diff(
    old: Optional["HashFile"], new: Optional["HashFile"], cache
) -> DiffResult:
    from .objects.tree import Tree

    if old is None and new is None:
        return DiffResult()

    def _get_keys(obj):
        if not obj:
            return []
        return [ROOT] + (
            [key for key, _, _ in obj] if isinstance(obj, Tree) else []
        )

    old_keys = set(_get_keys(old))
    new_keys = set(_get_keys(new))

    def _get(obj, key):
        if not obj or key == ROOT:
            return None, (obj.hash_info if obj else None)

        return obj.get(key, (None, None))

    def _in_cache(oid, cache):
        from dvc_objects.errors import ObjectFormatError

        if not oid:
            return False

        try:
            cache.check(oid.value)
            return True
        except (FileNotFoundError, ObjectFormatError):
            return False

    ret = DiffResult()
    for key in old_keys | new_keys:
        old_meta, old_oid = _get(old, key)
        new_meta, new_oid = _get(new, key)

        change = Change(
            old=TreeEntry(_in_cache(old_oid, cache), key, old_meta, old_oid),
            new=TreeEntry(_in_cache(new_oid, cache), key, new_meta, new_oid),
        )

        if change.typ == ADD:
            ret.added.append(change)
        elif change.typ == MODIFY:
            ret.modified.append(change)
        elif change.typ == DELETE:
            ret.deleted.append(change)
        else:
            assert change.typ == UNCHANGED
            if not change.new.in_cache and not (
                change.new.oid and change.new.oid.isdir
            ):
                ret.modified.append(change)
            else:
                ret.unchanged.append(change)
    return ret
