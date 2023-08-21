from typing import TYPE_CHECKING, Any, Iterator

from typing_extensions import Protocol

if TYPE_CHECKING:
    from dvc_objects.fs.base import AnyFSPath, FileSystem

# pylint: disable=unused-argument


class Ignore(Protocol):
    def find(self, fs: "FileSystem", path: "AnyFSPath") -> Iterator["AnyFSPath"]:
        ...

    def walk(self, fs: "FileSystem", path: "AnyFSPath", **kwargs: Any):
        ...
