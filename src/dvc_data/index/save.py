from typing import TYPE_CHECKING, List

from ..hashfile.hash_info import HashInfo
from ..hashfile.meta import Meta

if TYPE_CHECKING:
    from .index import DataIndex, DataIndexKey


def md5(index: "DataIndex", force=False) -> None:
    from ..hashfile.hash import file_md5

    for _, entry in index.iteritems():
        if entry.meta.isdir:
            continue

        if entry.meta.version_id and entry.fs.version_aware:
            # NOTE: if we have versioning available - there is no need to check
            # metadata as we can directly get correct file content using
            # version_id.
            path = entry.fs.path.version_path(
                entry.path, entry.meta.version_id
            )
        else:
            path = entry.path

        try:
            meta = Meta.from_info(entry.fs.info(path))
        except FileNotFoundError:
            entry.meta = None
            entry.hash_info = None

        if not force and entry.hash_info and entry.meta == meta:
            continue

        entry.meta = meta
        entry.hash_info = HashInfo("md5", file_md5(path, entry.fs))


def _save_dir_entry(index: "DataIndex", key: "DataIndexKey") -> None:
    from ..hashfile.db import add_update_tree
    from ..hashfile.tree import tree_from_index

    entry = index[key]
    assert entry.cache
    meta, tree = tree_from_index(index, key)
    tree = add_update_tree(entry.cache, tree)
    entry.meta = meta
    entry.hash_info = tree.hash_info
    assert tree.hash_info.name and tree.hash_info.value
    setattr(entry.meta, tree.hash_info.name, tree.hash_info.value)


def save(index: "DataIndex") -> None:
    dir_entries: List["DataIndexKey"] = []

    for key, entry in index.iteritems():
        if entry.meta.isdir:
            dir_entries.append(key)
            continue

        if entry.meta.version_id and entry.fs.version_aware:
            # NOTE: if we have versioning available - there is no need to check
            # metadata as we can directly get correct file content using
            # version_id.
            path = entry.fs.path.version_path(
                entry.path, entry.meta.version_id
            )
        else:
            path = entry.path

        if entry.hash_info:
            entry.cache.add(
                path,
                entry.fs,
                entry.hash_info.value,
            )

    for key in dir_entries:
        _save_dir_entry(index, key)
