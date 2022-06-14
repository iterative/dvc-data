from typing import TYPE_CHECKING, Iterator

from typing_extensions import Protocol

if TYPE_CHECKING:
    from .fs.base import AnyFSPath, FileSystem

# pylint: disable=unused-argument


class Ignore(Protocol):
    def find(
        self, fs: "FileSystem", path: "AnyFSPath"
    ) -> Iterator["AnyFSPath"]:
        ...
