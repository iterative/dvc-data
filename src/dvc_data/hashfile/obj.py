from typing import TYPE_CHECKING

from dvc_objects.obj import Object

if TYPE_CHECKING:
    from dvc_objects.fs.base import AnyFSPath, FileSystem

    from .hash_info import HashInfo


class HashFile(Object):
    __slots__ = ("hash_info",)

    def __init__(
        self, path: "AnyFSPath", fs: "FileSystem", hash_info: "HashInfo"
    ):
        assert hash_info.value
        oid = hash_info.value
        super().__init__(path, fs, oid)
        self.hash_info = hash_info
