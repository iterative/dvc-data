from itertools import chain
from typing import TYPE_CHECKING, Any, Dict, Iterable, Optional, Tuple

from dvc_objects.fs.local import LocalFileSystem

from dvc_data.hashfile.hash import DEFAULT_ALGORITHM, hash_file
from dvc_data.hashfile.meta import Meta

from .index import DataIndex, DataIndexEntry, FileStorage

if TYPE_CHECKING:
    from dvc_objects.fs.base import FileSystem

    from dvc_data.hashfile._ignore import Ignore
    from dvc_data.hashfile.state import StateBase


def build_entry(
    path: str,
    fs: "FileSystem",
    info: Optional[Dict[str, Any]] = None,
    compute_hash: Optional[bool] = False,
    state: Optional["StateBase"] = None,
    hash_name: str = DEFAULT_ALGORITHM,
):
    if info is None:
        info = fs.info(path)

    if compute_hash and info["type"] != "directory":
        meta, hash_info = hash_file(path, fs, hash_name, state=state, info=info)
    else:
        meta, hash_info = Meta.from_info(info, fs.protocol), None

    return DataIndexEntry(
        meta=meta,
        hash_info=hash_info,
        loaded=meta.isdir or None,
    )


def build_entries(
    path: str,
    fs: "FileSystem",
    ignore: Optional["Ignore"] = None,
    compute_hash: Optional[bool] = False,
    state: Optional["StateBase"] = None,
    hash_name: str = DEFAULT_ALGORITHM,
) -> Iterable[DataIndexEntry]:
    # NOTE: can't use detail=True with walk, because that will make it error
    # out on broken symlinks.
    detail = not isinstance(fs, LocalFileSystem)
    if ignore:
        walk_iter = ignore.walk(fs, path, detail=detail)
    else:
        walk_iter = fs.walk(path, detail=detail)

    for root, dirs, files in walk_iter:
        if root == path:
            root_key: Tuple[str, ...] = ()
        else:
            root_key = fs.relparts(root, path)

        entries: Iterable[Tuple[str, Optional[Dict]]]
        if detail:
            entries = chain(dirs.items(), files.items())
        else:
            entries = ((name, None) for name in chain(dirs, files))

        for name, info in entries:
            try:
                entry = build_entry(
                    fs.join(root, name),
                    fs,
                    compute_hash=compute_hash,
                    state=state,
                    hash_name=hash_name,
                    info=info,
                )
            except FileNotFoundError:
                entry = DataIndexEntry()
            entry.key = (*root_key, name)
            yield entry


def build(path: str, fs: "FileSystem", ignore: Optional["Ignore"] = None) -> DataIndex:
    index = DataIndex()

    index.storage_map.add_data(FileStorage(key=(), fs=fs, path=path))

    for entry in build_entries(path, fs, ignore=ignore):
        index.add(entry)

    return index
