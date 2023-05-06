from collections import defaultdict

from .checkout import checkout
from .index import DataIndex, DataIndexEntry, FileStorage, StorageInfo


def fetch(idxs, **kwargs):
    by_fs = defaultdict(DataIndex)

    for idx in idxs:
        for prefix, storage_info in idx.storage_map.items():
            remote = storage_info.remote
            cache = storage_info.cache
            if not remote or not cache:
                continue

            assert isinstance(cache, type(remote))

            fs_index = by_fs[remote.fs]

            if () not in fs_index.storage_map:
                fs_index.storage_map[()] = StorageInfo(
                    cache=FileStorage(
                        key=(),
                        fs=cache.fs,
                        path=cache.path,
                    ),
                    remote=FileStorage(
                        key=(),
                        fs=remote.fs,
                        path=remote.path,
                    ),
                )

            for _, entry in idx.iteritems(prefix):
                storage_key = remote.get_key(entry)
                fs_index[storage_key] = DataIndexEntry(
                    key=storage_key,
                    meta=entry.meta,
                    hash_info=entry.hash_info,
                )

    for _, fs_index in by_fs.items():
        cache = fs_index.storage_map[()].cache
        checkout(fs_index, cache.path, cache.fs, storage="remote", **kwargs)
