from functools import partial
from typing import TYPE_CHECKING, List, MutableSequence, Optional, Tuple

from dvc_objects.executors import ThreadPoolExecutor
from dvc_objects.fs.callbacks import DEFAULT_CALLBACK
from dvc_objects.fs.generic import transfer

from ..hashfile.meta import Meta
from .diff import ADD, DELETE, MODIFY, diff

if TYPE_CHECKING:
    from dvc_objects.fs.base import FileSystem
    from dvc_objects.fs.callbacks import Callback

    from .index import BaseDataIndex, DataIndexEntry


def checkout(
    index: "BaseDataIndex",
    path: str,
    fs: "FileSystem",
    old: Optional["BaseDataIndex"] = None,
    delete=False,
    callback: "Callback" = DEFAULT_CALLBACK,
    latest_only: bool = True,
    update_meta: bool = True,
    jobs: Optional[int] = None,
    **kwargs,
) -> int:
    transferred = 0
    create, delete = _get_changes(index, old, **kwargs)
    for entry in delete:
        if entry.meta and entry.meta.isdir:
            continue
        fs.remove(fs.path.join(path, *entry.key))

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
        latest_only=latest_only,
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
    latest_only: bool = True,
    update_meta: bool = True,
) -> int:
    assert entry.meta
    if entry.meta.isdir:
        return 0

    entry_path = fs.path.join(path, *entry.key)
    if (
        not latest_only
        and fs.version_aware
        and entry.meta.version_id is not None
        and fs.exists(fs.path.version_path(entry_path, entry.meta.version_id))
    ):
        callback.relative_update()
        return 0

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
