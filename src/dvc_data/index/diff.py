from typing import TYPE_CHECKING, Optional, Tuple

from attrs import define, field

if TYPE_CHECKING:
    from .index import DataIndex

from .index import DataIndexEntry

ADD = "add"
MODIFY = "modify"
DELETE = "delete"
UNCHANGED = "unchanged"


@define(hash=True, order=True)
class Change:
    key: Tuple[str, ...]
    old: Optional[DataIndexEntry]
    new: Optional[DataIndexEntry]
    typ: str = field(init=False)

    @typ.default
    def _(self):
        if not self.old and not self.new:
            return UNCHANGED

        if self.old and not self.new:
            return DELETE

        if not self.old and self.new:
            return ADD

        if (self.old.hash_info and self.new.hash_info) and (
            self.old.hash_info != self.new.hash_info
        ):
            return MODIFY

        if self.old.meta != self.new.meta:
            return MODIFY

        return UNCHANGED

    def __bool__(self):
        return self.typ != UNCHANGED


def diff(old: "DataIndex", new: "DataIndex"):
    old_keys = {key for key, _ in old.iteritems()}
    new_keys = {key for key, _ in new.iteritems()}

    for key in old_keys | new_keys:
        old_entry = old.get(key)
        new_entry = new.get(key)
        yield Change(key, old_entry, new_entry)
