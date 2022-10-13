from typing import TYPE_CHECKING, List

from ..hashfile.hash_info import HashInfo
from ..hashfile.meta import Meta

if TYPE_CHECKING:
    from .index import BaseDataIndex, DataIndexKey


def md5(index: "BaseDataIndex") -> None:
    from ..hashfile.hash import fobj_md5

    for _, entry in index.iteritems():
        assert entry.fs
        if entry.meta and entry.meta.isdir:
            continue

        if entry.hash_info:
            continue

        if entry.meta and entry.meta.version_id and entry.fs.version_aware:
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
            continue

        if entry.meta != meta:
            continue

        with entry.fs.open(path, "rb") as fobj:
            entry.hash_info = HashInfo(
                "md5",
                fobj_md5(fobj),
            )


def _save_dir_entry(
    index: "BaseDataIndex", key: "DataIndexKey", odb=None
) -> None:
    from ..hashfile.db import add_update_tree
    from ..hashfile.tree import tree_from_index

    entry = index[key]
    cache = odb or entry.cache
    assert cache
    meta, tree = tree_from_index(index, key)
    tree = add_update_tree(cache, tree)
    entry.meta = meta
    entry.hash_info = tree.hash_info
    assert tree.hash_info.name and tree.hash_info.value
    setattr(entry.meta, tree.hash_info.name, tree.hash_info.value)


def save(index: "BaseDataIndex", odb=None, **kwargs) -> None:
    dir_entries: List["DataIndexKey"] = []

    for key, entry in index.iteritems():
        assert entry.meta and entry.fs
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
            cache = odb or entry.cache
            assert entry.hash_info.value
            assert cache
            cache.add(
                path,
                entry.fs,
                entry.hash_info.value,
                **kwargs,
            )

    for key in dir_entries:
        _save_dir_entry(index, key, odb=odb)
