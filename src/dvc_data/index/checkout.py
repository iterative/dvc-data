from collections import defaultdict
from itertools import chain
from typing import (
    TYPE_CHECKING,
    Callable,
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
from .diff import ADD, DELETE, MODIFY, UNCHANGED, diff
from .index import FileStorage, ObjectStorage

if TYPE_CHECKING:
    from dvc_objects.fs.base import AnyFSPath, FileSystem

    from .diff import Change
    from .index import BaseDataIndex, DataIndexEntry, Storage


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
    storage: str = "cache",
    prompt: Optional[Callable] = None,
    relink: bool = False,
    force: bool = False,
    **kwargs,
) -> Dict[str, List["Change"]]:

    changes = defaultdict(list)

    for change in diff(old, index, with_unchanged=relink, **kwargs):
        changes[change.typ].append(change)

    create = [change.new for change in chain(changes[ADD], changes[MODIFY])]
    if relink:
        create.extend(change.new for change in changes[UNCHANGED])

    if delete:
        to_delete = [
            change.old
            for change in chain(changes[DELETE], changes[MODIFY])
            if not change.old.meta or not change.old.meta.isdir
        ]
        if relink:
            to_delete.extend(
                change.old
                for change in changes[UNCHANGED]
                if not change.old.meta or not change.old.meta.isdir
            )

        if prompt:
            for entry in to_delete:
                cache_fs, cache_path = index.storage_map.get_cache(entry)
                if not force and not cache_fs.exists(cache_path):
                    entry_path = fs.path.join(path, *entry.key)
                    msg = (
                        f"file/directory '{entry_path}' is going to be "
                        "removed. Are you sure you want to proceed?"
                    )

                    if not prompt(msg):
                        from dvc_data.hashfile.checkout import PromptError

                        raise PromptError(entry_path)

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

    by_storage: Dict[
        "Storage", List[Tuple["DataIndexEntry", str, str]]
    ] = defaultdict(list)
    parents = set()
    for entry in create:
        dest_path = fs.path.join(path, *entry.key)
        if entry.meta and entry.meta.isdir:
            parents.add(dest_path)
            continue
        parents.add(fs.path.parent(dest_path))

        storage_info = index.storage_map[entry.key]
        storage_obj = getattr(storage_info, storage)

        try:
            src_fs, src_path = storage_obj.get(entry)
        except ValueError:
            pass

        by_storage[storage_obj].append((entry, src_path, dest_path))

    for parent in parents:
        fs.makedirs(parent, exist_ok=True)

    if fs.version_aware and by_storage:
        storage_obj, items = next(iter(by_storage.items()))
        src_fs = storage_obj.fs
        if items:
            entry, src_path, dest_path = items.pop()
            entry.meta = test_versioning(
                src_fs, src_path, fs, dest_path, callback=callback
            )
            index.add(entry)

    for storage_obj, args in by_storage.items():
        if not args:
            continue

        src_fs = storage_obj.fs
        entries, src_paths, dest_paths = zip(*args)

        links = None
        if isinstance(storage_obj, ObjectStorage):
            links = storage_obj.odb.cache_types

        transfer(
            src_fs,
            list(src_paths),
            fs,
            list(dest_paths),
            callback=callback,
            batch_size=jobs,
            links=links,
        )
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
    if update_meta:
        for key in list(index.storage_map.keys()):
            index.storage_map.add_data(
                FileStorage(
                    key,
                    fs,
                    fs.path.join(path, *key),
                )
            )
    return changes


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
