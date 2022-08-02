from collections import OrderedDict
from typing import Final, Optional


class Meta:
    __slots__ = [
        "size",
        "nfiles",
        "isexec",
    ]

    PARAM_SIZE: Final = "size"
    PARAM_NFILES: Final = "nfiles"
    PARAM_ISEXEC: Final = "isexec"

    def __init__(
        self,
        size: Optional[int] = None,
        nfiles: Optional[int] = None,
        isexec: Optional[bool] = None,
    ):
        self.size = size
        self.nfiles = nfiles
        self.isexec = isexec

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
