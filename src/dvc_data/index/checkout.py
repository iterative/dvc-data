import logging
import os
import stat
from collections import defaultdict
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
from dvc_objects.fs.local import LocalFileSystem
from dvc_objects.fs.utils import exists as batch_exists

from ..hashfile.checkout import CheckoutError
from ..hashfile.meta import Meta
from .diff import ADD, DELETE, MODIFY, UNCHANGED, diff
from .index import FileStorage, ObjectStorage

if TYPE_CHECKING:
    from dvc_objects.fs.base import AnyFSPath, FileSystem

    from ..hashfile.state import StateBase
    from .diff import Change
    from .index import BaseDataIndex, DataIndexEntry, Storage

logger = logging.getLogger(__name__)


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


def _delete_files(
    entries: List["DataIndexEntry"],
    index: "BaseDataIndex",
    path: str,
    fs: "FileSystem",
    prompt: Optional[Callable] = None,
    force: bool = False,
):
    if not entries:
        return

    if prompt and not force:
        for entry in entries:
            try:
                cache_fs, cache_path = index.storage_map.get_cache(entry)
            except ValueError:
                continue

            if not cache_fs.exists(cache_path):
                entry_path = fs.path.join(path, *entry.key)
                msg = (
                    f"file/directory '{entry_path}' is going to be "
                    "removed. Are you sure you want to proceed?"
                )

                if not prompt(msg):
                    from dvc_data.hashfile.checkout import PromptError

                    raise PromptError(entry_path)

    fs.remove([fs.path.join(path, *entry.key) for entry in entries])


def _create_files(  # noqa: C901
    entries,
    index: "BaseDataIndex",
    path: str,
    fs: "FileSystem",
    callback: "Callback" = DEFAULT_CALLBACK,
    update_meta: bool = True,
    jobs: Optional[int] = None,
    storage: str = "cache",
    onerror=None,
    state: Optional["StateBase"] = None,
):
    by_storage: Dict[
        "Storage", List[Tuple["DataIndexEntry", str, str]]
    ] = defaultdict(list)
    parents = set()
    for entry in entries:
        dest_path = fs.path.join(path, *entry.key)
        if entry.meta and entry.meta.isdir:
            parents.add(dest_path)
            continue
        parents.add(fs.path.parent(dest_path))

        storage_info = index.storage_map[entry.key]
        storage_obj = getattr(storage_info, storage)

        try:
            src_fs, src_path = storage_obj.get(entry)
        except ValueError as exc:
            logger.warning(
                "No file hash info found for '%s'. It won't be created.",
                dest_path,
            )
            onerror(None, dest_path, exc)
            continue

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
            on_error=onerror,
        )

        if state:
            for (entry, _, dest_path) in args:
                try:
                    state.save(dest_path, fs, entry.hash_info)
                except FileNotFoundError:
                    continue

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


def _delete_dirs(entries, path, fs):
    for entry in entries:
        fs.rmdir(fs.path.join(path, *entry.key))


def _create_dirs(entries, path, fs):
    for entry in entries:
        fs.makedirs(fs.path.join(path, *entry.key), exist_ok=True)


def _chmod_files(entries, path, fs):
    if not isinstance(fs, LocalFileSystem):
        return

    for entry in entries:
        entry_path = fs.path.join(path, *entry.key)
        mode = os.stat(entry_path).st_mode | stat.S_IEXEC
        try:
            os.chmod(entry_path, mode)
        except OSError:
            logger.debug(
                "failed to chmod '%s' '%s'",
                oct(mode),
                entry_path,
                exc_info=True,
            )


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
    allow_missing: bool = False,
    state: Optional["StateBase"] = None,
    **kwargs,
) -> Dict[str, List["Change"]]:

    failed = []

    changes = defaultdict(list)
    files_delete = []
    dirs_delete = []
    files_create = []
    dirs_create = []
    files_chmod = []

    def _add_file_create(entry):
        if entry.meta and entry.meta.isexec:
            files_chmod.append(entry)

        files_create.append(entry)

    def _add_create(entry):
        if entry.meta and entry.meta.isdir:
            dirs_create.append(entry)
            return

        _add_file_create(entry)

    def _add_delete(entry):
        if entry.meta and entry.meta.isdir:
            dirs_delete.append(entry)
            return

        files_delete.append(entry)

    def meta_cmp_key(meta):
        if meta is None:
            return meta
        return (meta.isdir, meta.isexec)

    for change in diff(
        old, index, with_unchanged=relink, meta_cmp_key=meta_cmp_key
    ):
        if change.typ == ADD:
            _add_create(change.new)
        elif change.typ == DELETE:
            if not delete:
                continue

            _add_delete(change.old)
        elif change.typ == UNCHANGED:
            assert relink

            if not change.old.meta or not change.old.meta.isdir:
                files_delete.append(change.old)

            if not change.new.meta or not change.new.meta.isdir:
                _add_file_create(change.new)

            continue
        elif change.typ == MODIFY:
            old_hi = change.old.hash_info
            new_hi = change.new.hash_info
            old_meta = change.old.meta
            new_meta = change.new.meta
            old_isdir = old_meta.isdir if old_meta is not None else False
            new_isdir = new_meta.isdir if new_meta is not None else False
            old_isexec = old_meta.isexec if old_meta is not None else False
            new_isexec = new_meta.isexec if new_meta is not None else False

            if old_hi != new_hi or old_isdir != new_isdir:
                if old_isdir and new_isdir:
                    # no need to recreate the dir
                    continue

                _add_delete(change.old)
                _add_create(change.new)

            elif old_isexec != new_isexec and not new_isdir:
                files_chmod.append(change.new)
            else:
                continue
        else:
            raise AssertionError()

        changes[change.typ].append(change)

    if fs.version_aware and not latest_only:
        if callback == DEFAULT_CALLBACK:
            cb = callback
        else:
            desc = f"Checking status of existing versions in '{path}'"
            cb = Callback.as_tqdm_callback(desc=desc, unit="file")
        with cb:
            files_create = list(
                _prune_existing_versions(files_create, fs, path, callback=cb)
            )

    _delete_files(files_delete, index, path, fs, prompt=prompt, force=force)
    _delete_dirs(dirs_delete, path, fs)
    _create_dirs(dirs_create, path, fs)

    def onerror(_src_path, dest_path, _exc):
        failed.append(dest_path)

    _create_files(
        files_create,
        index,
        path,
        fs,
        onerror=onerror,
        jobs=jobs,
        storage=storage,
        callback=callback,
        update_meta=update_meta,
        state=state,
    )

    _chmod_files(files_chmod, path, fs)

    if failed and not allow_missing:
        raise CheckoutError(failed)

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
