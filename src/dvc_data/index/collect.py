import logging
from typing import TYPE_CHECKING, Dict, List, Optional, Tuple

from dvc_objects.fs.callbacks import DEFAULT_CALLBACK

from .index import DataIndex, DataIndexEntry, FileStorage, ObjectStorage, StorageInfo

if TYPE_CHECKING:
    from dvc_objects.fs.callbacks import Callback

    from .index import Storage

logger = logging.getLogger(__name__)


def _collect_from_index(
    cache,
    cache_prefix,
    index,
    prefix,
    storage,
    callback: "Callback" = DEFAULT_CALLBACK,
    push: bool = False,
):
    entries = {}

    dir_keys = set()
    try:
        for _, entry in index.iteritems(prefix):
            callback.relative_update()
            try:
                storage_key = storage.get_key(entry)
            except ValueError:
                continue

            if entry.meta and entry.meta.isdir and entry.loaded is None:
                # NOTE: at this point it might not be loaded yet, so we can't
                # rely on entry.loaded
                dir_keys.add((entry.key, storage_key))

            meta = entry.meta
            hash_info = entry.hash_info
            if (
                not push
                and isinstance(storage, FileStorage)
                and storage.fs.version_aware
                and entry.meta
                and not entry.meta.isdir
                and entry.meta.version_id is None
            ):
                meta.md5 = None
                hash_info = None

            # NOTE: avoiding modifying cache right away, because you might
            # run into a locked database if idx and cache are using the same
            # table.
            entries[storage_key] = DataIndexEntry(
                key=storage_key,
                meta=meta,
                hash_info=hash_info,
                loaded=entry.loaded,
            )

    except KeyError:
        return

    for key, storage_key in dir_keys:
        entries[storage_key].loaded = index[key].loaded

    for key, entry in entries.items():
        cache[(*cache_prefix, *key)] = entry


def collect(  # noqa: C901, PLR0912
    idxs,
    storage,
    callback: "Callback" = DEFAULT_CALLBACK,
    cache_index=None,
    cache_key=None,
    push: bool = False,
) -> List["DataIndex"]:
    from fsspec.utils import tokenize

    storage_by_fs: Dict[Tuple[str, str], StorageInfo] = {}
    skip = set()

    if cache_index is None:
        cache_index = DataIndex()
        cache_key = ()

    for idx in idxs:
        for prefix, storage_info in idx.storage_map.items():
            data = getattr(storage_info, storage)
            cache = storage_info.cache if storage != "cache" else None
            remote = storage_info.remote if storage != "remote" else None

            if not data or (push and data.read_only):
                continue

            try:
                fsid = data.fs.fsid
            except (NotImplementedError, AttributeError):
                fsid = data.fs.protocol

            key = (fsid, tokenize(data.path))

            if key not in storage_by_fs and cache_index.has_node((*cache_key, *key)):
                skip.add(key)

            if key not in skip:
                _collect_from_index(
                    cache_index,
                    (*cache_key, *key),
                    idx,
                    prefix,
                    data,
                    callback=callback,
                    push=push,
                )
                cache_index.commit()

            if key not in storage_by_fs:
                fs_data: "Storage"
                fs_cache: Optional["Storage"]
                fs_remote: Optional["Storage"]

                if isinstance(data, ObjectStorage):
                    fs_data = ObjectStorage(key=(), odb=data.odb)
                else:
                    fs_data = FileStorage(key=(), fs=data.fs, path=data.path)

                if not cache:
                    fs_cache = None
                elif isinstance(cache, ObjectStorage):
                    fs_cache = ObjectStorage(key=(), odb=cache.odb)
                else:
                    fs_cache = FileStorage(key=(), fs=cache.fs, path=cache.path)

                if not remote:
                    fs_remote = None
                elif isinstance(remote, ObjectStorage):
                    fs_remote = ObjectStorage(key=(), odb=remote.odb)
                else:
                    fs_remote = FileStorage(
                        key=(),
                        fs=remote.fs,
                        path=remote.path,
                    )

                storage_by_fs[key] = StorageInfo(
                    data=fs_data, cache=fs_cache, remote=fs_remote
                )

    storage_indexes = []
    for key, storage_info in storage_by_fs.items():
        idx = cache_index.view((*cache_key, *key))
        idx.storage_map[()] = storage_info

        def _onerror(*args):
            pass

        idx.onerror = _onerror
        storage_indexes.append(idx)

    return storage_indexes
