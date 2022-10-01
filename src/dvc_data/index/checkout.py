from typing import TYPE_CHECKING, Optional

from dvc_objects.fs.generic import transfer

from ..hashfile.meta import Meta
from .diff import ADD, DELETE, MODIFY, diff

if TYPE_CHECKING:
    from dvc_objects.fs.base import FileSystem

    from .index import DataIndex


def checkout(
    index: "DataIndex",
    path: str,
    fs: "FileSystem",
    old: Optional["DataIndex"] = None,
    delete=False,
) -> None:
    delete = []
    create = []
    for change in diff(old, index):
        if change.typ == ADD:
            create.append((change.key, change.new))
        elif change.typ == MODIFY:
            create.append((change.key, change.new))
            delete.append((change.key, change.old))
        elif change.typ == DELETE and delete:
            delete.append((change.key, change.new))

    for key, _ in delete:
        fs.remove(fs.path.join(path, *key))

    for key, entry in create:
        if entry.meta and entry.meta.isdir:
            continue

        odb = entry.odb or entry.cache or entry.remote
        cache_fs = odb.fs
        cache_file = odb.oid_to_path(entry.hash_info.value)
        entry_path = fs.path.join(path, *key)
        fs.makedirs(fs.path.parent(entry_path), exist_ok=True)
        transfer(
            cache_fs,
            cache_file,
            fs,
            entry_path,
        )
        entry.meta = Meta.from_info(fs.info(entry_path))
