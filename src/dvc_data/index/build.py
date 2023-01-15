from itertools import chain
from typing import TYPE_CHECKING, Any, Dict, Iterable, Optional

from ..hashfile.hash import hash_file
from ..hashfile.meta import Meta
from .index import DataIndex, DataIndexEntry

if TYPE_CHECKING:
    from dvc_objects.fs.base import FileSystem

    from ..hashfile._ignore import Ignore
    from ..state import StateBase


def build_entry(
    path: str,
    fs: "FileSystem",
    info: Optional[Dict[str, Any]] = None,
    compute_hash: Optional[bool] = False,
    state: Optional["StateBase"] = None,
):
    if info is None:
        info = fs.info(path)

    if compute_hash and info["type"] != "directory":
        meta, hash_info = hash_file(path, fs, "md5", state=state, info=info)
    else:
        meta, hash_info = Meta.from_info(info, fs.protocol), None

    return DataIndexEntry(
        meta=meta,
        hash_info=hash_info,
        path=path,
        fs=fs,
    )


def build_entries(
    path: str,
    fs: "FileSystem",
    ignore: Optional["Ignore"] = None,
    compute_hash: Optional[bool] = False,
    state: Optional["StateBase"] = None,
) -> Iterable[DataIndexEntry]:
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
            entry = build_entry(
                fs.path.join(root, name),
                fs,
                info=info,
                compute_hash=compute_hash,
                state=state,
            )
            entry.key = (*root_key, name)
            yield entry


def build(
    path: str, fs: "FileSystem", ignore: Optional["Ignore"] = None
) -> DataIndex:
    index = DataIndex()

    for entry in build_entries(path, fs, ignore=ignore):
        index.add(entry)

    return index
