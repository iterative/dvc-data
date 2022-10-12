from itertools import chain
from typing import TYPE_CHECKING, Any, Dict, Iterable, Optional, Tuple

from ..hashfile.meta import Meta
from .index import DataIndex, DataIndexEntry, DataIndexKey

if TYPE_CHECKING:
    from dvc_objects.fs.base import FileSystem

    from ..hashfile._ignore import Ignore


def build_entry(
    path: str, fs: "FileSystem", info: Optional[Dict[str, Any]] = None
):
    if info is None:
        info = fs.info(path)

    return DataIndexEntry(
        meta=Meta.from_info(info),
        path=path,
        fs=fs,
    )


def build_entries(
    path: str,
    fs: "FileSystem",
    ignore: Optional["Ignore"] = None,
) -> Iterable[Tuple[DataIndexKey, DataIndexEntry]]:
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
            yield (*root_key, name), build_entry(
                fs.path.join(root, name),
                fs,
                info=info,
            )


def build(
    path: str, fs: "FileSystem", ignore: Optional["Ignore"] = None
) -> DataIndex:
    index = DataIndex()

    for key, entry in build_entries(path, fs, ignore=ignore):
        index[key] = entry

    return index
