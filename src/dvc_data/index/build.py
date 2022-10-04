from itertools import chain
from typing import TYPE_CHECKING, Optional

from ..hashfile.meta import Meta
from .index import DataIndex, DataIndexEntry

if TYPE_CHECKING:
    from dvc_objects.fs.base import FileSystem

    from ..hashfile._ignore import Ignore


def build(
    path: str, fs: "FileSystem", ignore: Optional["Ignore"] = None
) -> DataIndex:
    index = DataIndex()

    walk_kwargs = {"detail": True}
    if ignore:
        walk_iter = ignore.walk(fs, path, **walk_kwargs)
    else:
        walk_iter = fs.walk(path, **walk_kwargs)

    for root, dirs, files in walk_iter:
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
