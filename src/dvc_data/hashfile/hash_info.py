from typing import Optional

from attrs import define, field

HASH_DIR_SUFFIX = ".dir"


@define(unsafe_hash=True)
class HashInfo:
    name: Optional[str] = None
    value: Optional[str] = None
    obj_name: Optional[str] = field(default=None, eq=False, hash=False)

    def __bool__(self) -> bool:
        return bool(self.value)

    def __str__(self) -> str:
        return f"{self.name}: {self.value}"

    @classmethod
    def from_dict(cls, d: dict[str, str]) -> "HashInfo":
        if not d:
            return cls()

        ((name, value),) = d.items()
        return cls(name, value)

    def to_dict(self) -> dict[str, str]:
        if not self.value or not self.name:
            return {}
        return {self.name: self.value}

    @property
    def isdir(self) -> bool:
        if not self.value:
            return False
        return self.value.endswith(HASH_DIR_SUFFIX)

    def as_raw(self) -> "HashInfo":
        assert self.value
        value, *_ = self.value.rsplit(HASH_DIR_SUFFIX, 1)
        return HashInfo(self.name, value, self.obj_name)
