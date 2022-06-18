from collections import OrderedDict
from typing import TYPE_CHECKING, Final, Optional

if TYPE_CHECKING:
    from .db import ObjectDB
    from .file import HashFile


class Meta:
    __slots__ = [
        "size",
        "nfiles",
        "isexec",
        "obj",
        "odb",
        "remote",
    ]

    PARAM_SIZE: Final = "size"
    PARAM_NFILES: Final = "nfiles"
    PARAM_ISEXEC: Final = "isexec"

    def __init__(
        self,
        size: Optional[int] = None,
        nfiles: Optional[int] = None,
        isexec: Optional[bool] = None,
        obj: Optional["HashFile"] = None,
        odb: Optional["ObjectDB"] = None,
        remote: Optional[str] = None,
    ):
        self.size = size
        self.nfiles = nfiles
        self.isexec = isexec
        self.obj = obj
        self.odb = odb
        self.remote = remote

    @classmethod
    def from_dict(cls, d: dict) -> "Meta":
        if not d:
            return cls()

        size = d.pop(cls.PARAM_SIZE, None)
        nfiles = d.pop(cls.PARAM_NFILES, None)
        isexec = d.pop(cls.PARAM_ISEXEC, False)

        return cls(size=size, nfiles=nfiles, isexec=isexec)

    def to_dict(self) -> dict:
        ret: dict = OrderedDict()

        if self.size is not None:
            ret[self.PARAM_SIZE] = self.size

        if self.nfiles is not None:
            ret[self.PARAM_NFILES] = self.nfiles

        if self.isexec:
            ret[self.PARAM_ISEXEC] = self.isexec

        return ret
