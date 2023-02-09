from collections import defaultdict
from typing import (
    TYPE_CHECKING,
    Collection,
    Dict,
    Iterator,
    List,
    Optional,
    Tuple,
)

from dvc_objects.fs.callbacks import DEFAULT_CALLBACK, Callback
from dvc_objects.fs.generic import transfer
from dvc_objects.fs.utils import exists as batch_exists

from ..hashfile.meta import Meta
from .diff import ADD, DELETE, MODIFY, diff

if TYPE_CHECKING:
    from dvc_objects.fs.base import AnyFSPath, FileSystem

    from .index import BaseDataIndex, DataIndexEntry


class VersioningNotSupported(Exception):
    pass


def test_versioning(
    src_fs: "FileSystem",
    src_path: "AnyFSPath",
    dest_fs: "FileSystem",
    dest_path: "AnyFSPath",
    callback: "Callback" = DEFAULT_CALLBACK,
) -> Meta:
    transfer(src_fs, src_path, dest_fs, dest_path, callback=callback)
    info = dest_fs.info(dest_path)
    meta = Meta.from_info(info, dest_fs.protocol)
    if meta.version_id in (None, "null"):
        raise VersioningNotSupported(
            f"while uploading {dest_path!r}, "
            "support for versioning could not be detected"
        )
    return meta


def checkout(  # noqa: C901
    index: "BaseDataIndex",
    path: str,
    fs: "FileSystem",
    old: Optional["BaseDataIndex"] = None,
    delete: bool = False,
    callback: "Callback" = DEFAULT_CALLBACK,
    latest_only: bool = True,
    update_meta: bool = True,
    jobs: Optional[int] = None,
    **kwargs,
) -> int:
    create, to_delete = _get_changes(index, old, **kwargs)
    if delete:
        to_delete = [
            entry
            for entry in to_delete
            if not entry.meta or not entry.meta.isdir
        ]
        if to_delete:
            fs.remove([fs.path.join(path, *entry.key) for entry in to_delete])

    if fs.version_aware and not latest_only:
        if callback == DEFAULT_CALLBACK:
            cb = callback
        else:
            desc = f"Checking status of existing versions in '{path}'"
            cb = Callback.as_tqdm_callback(desc=desc, unit="file")
        with cb:
            create = list(
                _prune_existing_versions(create, fs, path, callback=cb)
            )

    fs_map: Dict[
        "FileSystem", List[Tuple["DataIndexEntry", str, str]]
    ] = defaultdict(list)
    parents = set()
    for entry in create:
        if entry.meta and entry.meta.isdir:
            continue
        dest_path = fs.path.join(path, *entry.key)
        parents.add(fs.path.parent(dest_path))
        storage = index.storage_map[entry.key]
        assert storage
        if storage.fs and storage.path:
            src_fs: "FileSystem" = storage.fs
            src_path = storage.path
        else:
            assert entry.hash_info
            odb = storage.cache or storage.remote
            assert odb
            src_fs = odb.fs
            src_path = odb.oid_to_path(entry.hash_info.value)
        fs_map[src_fs].append((entry, src_path, dest_path))

    for parent in parents:
        fs.makedirs(parent, exist_ok=True)

    transferred = 0
    if fs.version_aware and fs_map:
        src_fs, items = next(iter(fs_map.items()))
        if items:
            entry, src_path, dest_path = items.pop()
            entry.meta = test_versioning(
                src_fs, src_path, fs, dest_path, callback=callback
            )
            index.add(entry)
            transferred += 1

    for src_fs, args in fs_map.items():
        if not args:
            continue

        entries, src_paths, dest_paths = zip(*args)
        transfer(
            src_fs,
            list(src_paths),
            fs,
            list(dest_paths),
            callback=callback,
            batch_size=jobs,
        )
        transferred += len(entries)
        if update_meta:
            if callback == DEFAULT_CALLBACK:
                cb = callback
            else:
                desc = f"Updating meta for new files in '{path}'"
                cb = Callback.as_tqdm_callback(desc=desc, unit="file")
            with cb:
                infos = fs.info(list(dest_paths), callback=cb, batch_size=jobs)
                for entry, info in zip(entries, infos):
                    entry.meta = Meta.from_info(info, fs.protocol)
                    index.add(entry)
    # FIXME should return new index
    for key in list(index.storage_map.keys()):
        storage = index.storage_map[key]
        storage.fs = fs
        storage.path = fs.path.join(path, *key)
        index.storage_map[key] = storage
    return transferred


def _get_changes(
    index: "BaseDataIndex", old: Optional["BaseDataIndex"], **kwargs
) -> Tuple[List["DataIndexEntry"], List["DataIndexEntry"]]:
    create = []
    delete = []
    for change in diff(old, index, **kwargs):
        if change.typ == ADD:
            create.append(change.new)
        elif change.typ == MODIFY:
            if change.new:
                create.append(change.new)
            if change.old:
                delete.append(change.old)
        elif change.typ == DELETE and delete:
            delete.append(change.old)
    return create, delete


def _prune_existing_versions(
    entries: Collection["DataIndexEntry"],
    fs: "FileSystem",
    path: str,
    callback: "Callback" = DEFAULT_CALLBACK,
    jobs: Optional[int] = None,
) -> Iterator["DataIndexEntry"]:
    assert fs.version_aware
    query_vers: Dict[str, "DataIndexEntry"] = {}
    jobs = jobs or fs.jobs
    for entry in entries:
        assert entry.meta
        if entry.meta.version_id is None:
            yield entry
        else:
            entry_path = fs.path.join(path, *entry.key)
            versioned_path = fs.path.version_path(
                entry_path, entry.meta.version_id
            )
            query_vers[versioned_path] = entry
    for path, exists in batch_exists(
        fs, query_vers.keys(), batch_size=jobs, callback=callback
    ).items():
        if not exists:
            yield query_vers[path]
