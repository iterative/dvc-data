from typing import TYPE_CHECKING, Optional

from dvc_objects.fs.callbacks import DEFAULT_CALLBACK
from dvc_objects.fs.generic import transfer

from ..hashfile.meta import Meta
from .diff import ADD, DELETE, MODIFY, diff

if TYPE_CHECKING:
    from dvc_objects.fs.base import FileSystem
    from dvc_objects.fs.callbacks import Callback

    from .index import BaseDataIndex


def checkout(
    index: "BaseDataIndex",
    path: str,
    fs: "FileSystem",
    old: Optional["BaseDataIndex"] = None,
    delete=False,
    callback: "Callback" = DEFAULT_CALLBACK,
) -> None:
    delete = []
    create = []
    for change in diff(old, index):
        if change.typ == ADD:
            create.append(change.new)
        elif change.typ == MODIFY:
            create.append(change.new)
            delete.append(change.old)
        elif change.typ == DELETE and delete:
            delete.append(change.new)

    for entry in delete:
        fs.remove(fs.path.join(path, *entry.key))

    if callback != DEFAULT_CALLBACK:
        callback.set_size(
            sum(
                entry.meta is not None and not entry.meta.isdir
                for _, entry in index.iteritems()
            )
        )
    for entry in create:
        if entry.meta and entry.meta.isdir:
            continue

        try_sources = []
        if entry.hash_info:
            odb = entry.odb or entry.cache or entry.remote
            try_sources.append(
                (odb.fs, odb.oid_to_path(entry.hash_info.value))
            )
        if entry.fs and entry.path:
            try_sources.append((entry.fs, entry.path))

        entry_path = fs.path.join(path, *entry.key)
        fs.makedirs(fs.path.parent(entry_path), exist_ok=True)
        while try_sources:
            src_fs, src_path = try_sources.pop(0)
            try:
                transfer(
                    src_fs,
                    src_path,
                    fs,
                    entry_path,
                    callback=callback,
                )
                entry.fs = fs
                entry.path = entry_path
                entry.meta = Meta.from_info(fs.info(entry_path))
                break
            except Exception:  # pylint: disable=broad-except
                if not try_sources:
                    raise
