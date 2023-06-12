from collections import defaultdict
from functools import wraps
from typing import TYPE_CHECKING, Any, Callable, Dict, List, Optional, Tuple

from dvc_objects.fs.callbacks import DEFAULT_CALLBACK

from ..hashfile.hash import DEFAULT_ALGORITHM
from ..hashfile.hash_info import HashInfo
from ..hashfile.meta import Meta
from ..hashfile.tree import Tree

if TYPE_CHECKING:
    from dvc_objects.fs.base import FileSystem
    from dvc_objects.fs.callbacks import Callback

    from ..hashfile.db import HashFileDB
    from ..hashfile.state import StateBase
    from .index import BaseDataIndex, DataIndexKey


def md5(
    index: "BaseDataIndex",
    state: Optional["StateBase"] = None,
    storage: str = "data",
    name: str = DEFAULT_ALGORITHM,
    check_meta: bool = True,
) -> None:
    from ..hashfile.hash import fobj_md5
    from .index import DataIndexEntry

    entries = {}

    for key, entry in index.iteritems():
        if entry.meta and entry.meta.isdir:
            continue

        if entry.hash_info and entry.hash_info.name in ("md5", "md5-dos2unix"):
            continue

        fs, path = index.storage_map.get_storage(entry, storage)

        if check_meta:
            try:
                meta = Meta.from_info(fs.info(path), fs.protocol)
            except FileNotFoundError:
                continue

            if entry.meta != meta:
                continue

        if state:
            _, hash_info = state.get(path, fs)
            if hash_info:
                entries[key] = DataIndexEntry(
                    key=entry.key,
                    meta=entry.meta,
                    hash_info=hash_info,
                )
                continue

        with fs.open(path, "rb") as fobj:
            entries[key] = DataIndexEntry(
                key=entry.key,
                meta=entry.meta,
                hash_info=HashInfo(
                    name,
                    fobj_md5(fobj, name=name),
                ),
            )

        if state:
            state.save(path, fs, entries[key].hash_info)

    for key, entry in entries.items():
        index[key] = entry


def build_tree(
    index: "BaseDataIndex",
    prefix: "DataIndexKey",
    name: str = DEFAULT_ALGORITHM,
) -> Tuple["Meta", Tree]:
    tree_meta = Meta(size=0, nfiles=0, isdir=True)
    assert tree_meta.size is not None and tree_meta.nfiles is not None
    tree = Tree()
    for key, entry in index.iteritems(prefix=prefix):
        if key == prefix or entry.meta and entry.meta.isdir:
            continue
        tree_key = key[len(prefix) :]
        tree.add(tree_key, entry.meta, entry.hash_info)
        tree_meta.size += (entry.meta.size if entry.meta else 0) or 0
        tree_meta.nfiles += 1
    tree.digest(name=name)
    return tree_meta, tree


def _save_dir_entry(
    index: "BaseDataIndex",
    key: "DataIndexKey",
    odb: Optional["HashFileDB"] = None,
) -> None:
    from ..hashfile.db import add_update_tree
    from .index import StorageKeyError

    entry = index[key]

    try:
        cache = odb or index.storage_map.get_cache_odb(entry)
    except StorageKeyError:
        return

    assert cache
    meta, tree = build_tree(index, key)
    tree = add_update_tree(cache, tree)
    entry.meta = meta
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


if TYPE_CHECKING:
    _ODBMap = Dict["HashFileDB", "_FSMap"]
    _FSMap = Dict["FileSystem", List[Tuple[str, str]]]


def save(
    index: "BaseDataIndex",
    odb: Optional["HashFileDB"] = None,
    callback: "Callback" = DEFAULT_CALLBACK,
    jobs: Optional[int] = None,
    storage: str = "data",
    **kwargs,
) -> int:
    dir_entries: List["DataIndexKey"] = []
    transferred = 0

    odb_map: "_ODBMap" = {}
    for key, entry in index.iteritems():
        if entry.meta and entry.meta.isdir:
            dir_entries.append(key)
            continue
        fs, path = index.storage_map.get_storage(entry, storage)
        if entry.hash_info:
            cache = odb or index.storage_map.get_cache_odb(entry)
            assert cache
            assert entry.hash_info.value
            oid = entry.hash_info.value
            if cache not in odb_map:
                odb_map[cache] = defaultdict(list)
            odb_map[cache][fs].append((path, oid))
    for cache, fs_map in odb_map.items():
        for fs, args in fs_map.items():
            paths, oids = zip(*args)
            transferred += cache.add(
                list(paths),
                fs,
                list(oids),
                callback=callback,
                batch_size=jobs,
                **kwargs,
            )

    for key in dir_entries:
        _save_dir_entry(index, key, odb=odb)

    return transferred
