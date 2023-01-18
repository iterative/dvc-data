from functools import partial
from typing import (
    TYPE_CHECKING,
    Collection,
    Dict,
    Iterator,
    List,
    MutableSequence,
    Optional,
    Tuple,
)

from dvc_objects.executors import ThreadPoolExecutor
from dvc_objects.fs.callbacks import DEFAULT_CALLBACK, Callback
from dvc_objects.fs.generic import transfer

from ..hashfile.meta import Meta
from .diff import ADD, DELETE, MODIFY, diff

if TYPE_CHECKING:
    from dvc_objects.fs.base import FileSystem

    from .index import BaseDataIndex, DataIndexEntry


def checkout(
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
    transferred = 0
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

    if callback != DEFAULT_CALLBACK:
        callback.set_size(
            sum(
                entry.meta is not None and not entry.meta.isdir
                for _, entry in index.iteritems()
            )
        )
    processor = partial(
        _do_create,
        path,
        fs,
        callback=callback,
        update_meta=update_meta,
    )
    with ThreadPoolExecutor(max_workers=jobs or fs.jobs) as executor:
        transferred += sum(executor.imap_unordered(processor, create))
    return transferred


def _do_create(
    path: str,
    fs: "FileSystem",
    entry: "DataIndexEntry",
    callback: "Callback" = DEFAULT_CALLBACK,
    update_meta: bool = True,
) -> int:
    assert entry.meta
    if entry.meta.isdir:
        return 0

    entry_path = fs.path.join(path, *entry.key)
    sources = []
    if entry.hash_info:
        odb = entry.odb or entry.cache or entry.remote
        assert odb
        sources.append((odb.fs, odb.oid_to_path(entry.hash_info.value)))
    if entry.fs and entry.path:
        sources.append((entry.fs, entry.path))
    fs.makedirs(fs.path.parent(entry_path), exist_ok=True)
    _try_sources(fs, entry_path, sources, callback=callback)
    if update_meta:
        entry.fs = fs
        entry.path = entry_path
        entry.meta = Meta.from_info(fs.info(entry_path), fs.protocol)
    return 1


def _get_changes(
    index: "BaseDataIndex", old: Optional["BaseDataIndex"], **kwargs
) -> Tuple[List["DataIndexEntry"], List["DataIndexEntry"]]:
    create = []
    delete = []
    for change in diff(old, index, **kwargs):
        if change.typ == ADD:
            create.append(change.new)
        elif change.typ == MODIFY:
            create.append(change.new)
            delete.append(change.old)
        elif change.typ == DELETE and delete:
            delete.append(change.old)
    return create, delete


def _try_sources(
    fs: "FileSystem",
    entry_path: str,
    sources: MutableSequence[Tuple["FileSystem", str]],
    **kwargs,
):
    while sources:
        src_fs, src_path = sources.pop(0)
        try:
            transfer(src_fs, src_path, fs, entry_path, **kwargs)
            return
        except Exception:  # pylint: disable=broad-except
            if not sources:
                raise


def _prune_existing_versions(
    entries: Collection["DataIndexEntry"],
    fs: "FileSystem",
    path: str,
    callback: "Callback" = DEFAULT_CALLBACK,
    jobs: Optional[int] = None,
) -> Iterator["DataIndexEntry"]:
    from dvc_objects.fs.utils import exists as batch_exists

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
