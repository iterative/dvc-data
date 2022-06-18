from collections import OrderedDict
from typing import Optional

HASH_DIR_SUFFIX = ".dir"


class HashInfo:
    __slots__ = ("name", "value", "obj_name")

    def __init__(
        self,
        name: Optional[str] = None,
        value: Optional[str] = None,
        obj_name: Optional[str] = None,
    ):
        self.name = name
        self.value = value
        self.obj_name = obj_name

    def __eq__(self, other):
        if not isinstance(other, HashInfo):
            return False

        return (self.name == other.name) and (self.value == other.value)

    def __bool__(self) -> bool:
        return bool(self.value)

    def __str__(self) -> str:
        return f"{self.name}: {self.value}"

    def __hash__(self) -> int:
        return hash((self.name, self.value))

    @classmethod
    def from_dict(cls, d) -> "HashInfo":
        if not d:
            return cls(None, None)

        ((name, value),) = d.items()
        return cls(name, value)

    def to_dict(self) -> dict:
        ret: dict = OrderedDict()
        if not self:
            return ret

        ret[self.name] = self.value
        return ret

    @property
    def isdir(self) -> bool:
        if not self.value:
            return False
        return self.value.endswith(HASH_DIR_SUFFIX)

    def as_raw(self) -> "HashInfo":
        assert self.value
        return HashInfo(
            self.name, self.value.rsplit(HASH_DIR_SUFFIX)[0], self.obj_name
        )
