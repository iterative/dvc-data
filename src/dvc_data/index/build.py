from itertools import chain
from typing import TYPE_CHECKING

from ..hashfile.meta import Meta
from .index import DataIndex, DataIndexEntry

if TYPE_CHECKING:
    from dvc_objects.fs.base import FileSystem


def build(path: str, fs: "FileSystem") -> DataIndex:
    index = DataIndex()

    for root, dirs, files in fs.walk(path, detail=True):
        if root == path:
            root_key = ()
        else:
            root_key = fs.path.relparts(root, path)

        for name, info in chain(dirs.items(), files.items()):
            index[(*root_key, name)] = DataIndexEntry(
                meta=Meta.from_info(info),
                path=fs.path.join(root, name),
                fs=fs,
            )

    return index
