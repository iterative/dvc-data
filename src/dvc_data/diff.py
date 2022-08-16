import reprlib
from typing import TYPE_CHECKING, Dict, List, Optional, Tuple

from attrs import asdict, define, field

if TYPE_CHECKING:
    from .hashfile.hash_info import HashInfo
    from .hashfile.meta import Meta
    from .hashfile.obj import HashFile


ADD = "add"
MODIFY = "modify"
DELETE = "delete"
UNCHANGED = "unchanged"


@define
class TreeEntry:
    in_cache: bool = field(default=False, eq=False)
    key: Tuple[str, ...] = ()
    meta: Optional["Meta"] = field(default=None, eq=False)
    oid: Optional["HashInfo"] = None

    def __bool__(self):
        return bool(self.oid)


@define
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
    added: List[Change] = field(factory=list, repr=reprlib.repr)
    modified: List[Change] = field(factory=list, repr=reprlib.repr)
    deleted: List[Change] = field(factory=list, repr=reprlib.repr)
    unchanged: List[Change] = field(factory=list, repr=reprlib.repr)

    def __bool__(self):
        return bool(self.added or self.modified or self.deleted)

    @property
    def stats(self) -> Dict[str, int]:
        return {k: len(v) for k, v in asdict(self).items()}


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
