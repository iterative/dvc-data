from collections.abc import Iterable, Iterator
from itertools import chain, repeat
from typing import TYPE_CHECKING, Any, Optional

from dvc_objects.fs.local import LocalFileSystem

from dvc_data.hashfile.hash import DEFAULT_ALGORITHM, hash_file
from dvc_data.hashfile.meta import Meta

from .index import DataIndex, DataIndexEntry, FileStorage

if TYPE_CHECKING:
    from dvc_objects.fs.base import FileSystem

    from dvc_data.hashfile._ignore import Ignore
    from dvc_data.hashfile.hash_info import HashInfo
    from dvc_data.hashfile.state import StateBase


def build_entry(
    path: str,
    fs: "FileSystem",
    info: Optional[dict[str, Any]] = None,
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


def safe_walk(
    path: str,
    fs: "FileSystem",
    ignore: Optional["Ignore"] = None,
) -> Iterator[tuple[str, dict[str, dict], dict[str, dict], set[str]]]:
    if not isinstance(fs, LocalFileSystem):
        for root, dirs, files in fs.walk(path, detail=True):
            yield root, dirs, files, set()

        return

    # NOTE: can't use detail=True with walk, because that will make it error
    # out on broken symlinks.
    sep = fs.sep
    walk_iter = ignore.walk(fs, path, detail=False) if ignore else fs.walk(path)
    for root, dirs, files in walk_iter:
        _dirs: dict[str, dict] = {}
        _files: dict[str, dict] = {}
        broken = set()

        for name, d in chain(zip(dirs, repeat(_dirs)), zip(files, repeat(_files))):
            p = f"{root}{sep}{name}"
            try:
                d[name] = fs.info(p)
            except FileNotFoundError:
                d[name] = {}
                broken.add(name)
        yield root, _dirs, _files, broken
        dirs[:] = list(_dirs)


def build_entries(
    path: str,
    fs: "FileSystem",
    ignore: Optional["Ignore"] = None,
    compute_hash: Optional[bool] = False,
    state: Optional["StateBase"] = None,
    hash_name: str = DEFAULT_ALGORITHM,
    checksum_jobs: Optional[int] = None,
) -> Iterable[DataIndexEntry]:
    from dvc_data.hashfile.build import _get_hashes

    sep = fs.sep
    jobs = checksum_jobs or fs.hash_jobs
    for root, dirs, files, broken in safe_walk(path, fs, ignore=ignore):
        if root == path:
            root_key: tuple[str, ...] = ()
        else:
            root_key = fs.relparts(root, path)

        hashes: dict[str, tuple[Meta, HashInfo, dict]] = {}
        if compute_hash:
            file_infos = {
                f"{root}{sep}{name}": info for name, info in files.items() if info
            }
            file_paths = list(file_infos)
            hashes = _get_hashes(
                file_paths, fs, hash_name, file_infos, state=state, jobs=jobs
            )

        for name, info in chain(dirs.items(), files.items()):
            key = (*root_key, name)
            if name in broken:
                yield DataIndexEntry(key=key)
                continue

            p = f"{root}{sep}{name}"
            if p in hashes:
                meta, hash_info, _ = hashes[p]
            else:
                meta, hash_info = Meta.from_info(info, fs.protocol), None
            loaded = meta.isdir or None
            yield DataIndexEntry(key=key, meta=meta, hash_info=hash_info, loaded=loaded)


def build(path: str, fs: "FileSystem", ignore: Optional["Ignore"] = None) -> DataIndex:
    index = DataIndex()

    index.storage_map.add_data(FileStorage(key=(), fs=fs, path=path))

    for entry in build_entries(path, fs, ignore=ignore):
        index.add(entry)

    return index
