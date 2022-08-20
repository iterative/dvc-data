from typing import Any, ClassVar, Dict, Optional

from attrs import Attribute, asdict, define


def _filter_default_or_none(field: Attribute, value: Any) -> bool:
    return value is not None and value != field.default


@define
class Meta:
    PARAM_SIZE: ClassVar[str] = "size"
    PARAM_NFILES: ClassVar[str] = "nfiles"
    PARAM_ISEXEC: ClassVar[str] = "isexec"

    size: Optional[int] = None
    nfiles: Optional[int] = None
    isexec: bool = False

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "Meta":
        d = d or {}
        return cls(
            size=d.pop(cls.PARAM_SIZE, None),
            nfiles=d.pop(cls.PARAM_NFILES, None),
            isexec=d.pop(cls.PARAM_ISEXEC, False),
        )

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self, recurse=False, filter=_filter_default_or_none)
