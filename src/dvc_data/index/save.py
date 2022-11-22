from functools import wraps
from typing import TYPE_CHECKING, Any, Callable, Dict, List, Optional, Tuple

from dvc_objects.fs.callbacks import DEFAULT_CALLBACK

from ..hashfile.hash_info import HashInfo
from ..hashfile.meta import Meta
from ..hashfile.tree import Tree

if TYPE_CHECKING:
    from dvc_objects.fs.callbacks import Callback

    from ..hashfile.db import HashFileDB
    from .index import BaseDataIndex, DataIndexKey


def md5(index: "BaseDataIndex") -> None:
    from ..hashfile.hash import fobj_md5

    for _, entry in index.iteritems():
        assert entry.fs
        if entry.meta and entry.meta.isdir:
            continue

        if entry.hash_info and entry.hash_info.name == "md5":
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
            meta = Meta.from_info(entry.fs.info(path), entry.fs.protocol)
        except FileNotFoundError:
            continue

        if entry.meta != meta:
            continue

        with entry.fs.open(path, "rb") as fobj:
            entry.hash_info = HashInfo(
                "md5",
                fobj_md5(fobj),
            )


def build_tree(
    index: "BaseDataIndex",
    prefix: "DataIndexKey",
) -> Tuple["Meta", Tree]:
    tree_meta = Meta(size=0, nfiles=0, isdir=True)
    assert tree_meta.size is not None and tree_meta.nfiles is not None
    tree = Tree()
    for key, entry in index.iteritems(prefix=prefix):
        if key == prefix or entry.meta and entry.meta.isdir:
            continue
        assert entry.meta and entry.hash_info
        tree_key = key[len(prefix) :]
        tree.add(tree_key, entry.meta, entry.hash_info)
        tree_meta.size += entry.meta.size or 0
        tree_meta.nfiles += 1
    tree.digest()
    return tree_meta, tree


def _save_dir_entry(
    index: "BaseDataIndex",
    key: "DataIndexKey",
    odb: Optional["HashFileDB"] = None,
) -> None:
    from ..hashfile.db import add_update_tree

    entry = index[key]
    cache = odb or entry.cache
    assert cache
    meta, tree = build_tree(index, key)
    tree = add_update_tree(cache, tree)
    entry.meta = meta
    entry.obj = tree
    entry.hash_info = tree.hash_info
    assert tree.hash_info.name and tree.hash_info.value
    setattr(entry.meta, tree.hash_info.name, tree.hash_info.value)


def _wrap_add(callback: "Callback", fn: Callable):
    wrapped = callback.wrap_fn(fn)

    @wraps(fn)
    def func(path: str, *args, **kwargs):
        kw: Dict[str, Any] = dict(kwargs)
        with callback.branch(path, path, kw):
            return wrapped(path, *args, **kw)

    return func


def save(
    index: "BaseDataIndex",
    odb: Optional["HashFileDB"] = None,
    callback: "Callback" = DEFAULT_CALLBACK,
    **kwargs
) -> int:
    dir_entries: List["DataIndexKey"] = []
    transferred = 0

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
            add = _wrap_add(callback, cache.add)
            transferred += add(
                path,
                entry.fs,
                entry.hash_info.value,
                callback=callback,
                **kwargs,
            )

    for key in dir_entries:
        _save_dir_entry(index, key, odb=odb)

    return transferred
