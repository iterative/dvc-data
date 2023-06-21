import logging
from typing import TYPE_CHECKING, Dict, Optional, Tuple

from dvc_objects.fs.callbacks import DEFAULT_CALLBACK

from dvc_data.hashfile.db import get_index
from dvc_data.hashfile.transfer import transfer

from .build import build
from .checkout import apply, compare
from .index import (
    DataIndex,
    DataIndexEntry,
    FileStorage,
    ObjectStorage,
    StorageInfo,
)
from .save import md5, save

if TYPE_CHECKING:
    from dvc_objects.fs.callbacks import Callback

    from dvc_data.hashfile.status import CompareStatusResult

    from .index import Storage

logger = logging.getLogger(__name__)


def _log_missing(status: "CompareStatusResult"):
    if status.missing:
        missing_desc = "\n".join(
            f"{hash_info}" for hash_info in status.missing
        )
        logger.warning(
            (
                "Some of the cache files do not exist neither locally "
                "nor on remote. Missing cache files:\n%s"
            ),
            missing_desc,
        )


def _collect_from_index(
    cache,
    cache_prefix,
    index,
    prefix,
    remote,
    callback: "Callback" = DEFAULT_CALLBACK,
):
    entries = {}

    try:
        for _, entry in index.iteritems(prefix):
            callback.relative_update()
            try:
                storage_key = remote.get_key(entry)
            except ValueError:
                continue

            loaded = False
            if entry.meta and entry.meta.isdir:
                # NOTE: at this point it might not be loaded yet, so we can't
                # rely on entry.loaded
                loaded = True

            # NOTE: avoiding modifying cache right away, because you might
            # run into a locked database if idx and cache are using the same
            # table.
            entries[storage_key] = DataIndexEntry(
                key=storage_key,
                meta=entry.meta,
                hash_info=entry.hash_info,
                loaded=loaded,
            )

    except KeyError:
        return

    for key, entry in entries.items():
        cache[(*cache_prefix, *key)] = entry


def collect(  # noqa: C901
    idxs,
    callback: "Callback" = DEFAULT_CALLBACK,
    cache_index=None,
    cache_key=None,
):
    from fsspec.utils import tokenize

    storage_by_fs: Dict[Tuple[str, str], StorageInfo] = {}
    skip = set()

    if cache_index is None:
        cache_index = DataIndex()
        cache_key = ()

    for idx in idxs:
        for prefix, storage_info in idx.storage_map.items():
            remote = storage_info.remote
            cache = storage_info.cache
            if not remote or not cache:
                continue

            # FIXME should use fsid instead of protocol
            key = (remote.fs.protocol, tokenize(remote.path))
            if key not in storage_by_fs:
                if cache_index.has_node((*cache_key, *key)):
                    skip.add(key)

            if key not in skip:
                _collect_from_index(
                    cache_index,
                    (*cache_key, *key),
                    idx,
                    prefix,
                    remote,
                    callback=callback,
                )
                cache_index.commit()

            if key not in storage_by_fs:
                fs_cache: "Storage"
                fs_remote: "Storage"

                if isinstance(cache, ObjectStorage):
                    fs_cache = ObjectStorage(key=(), odb=cache.odb)
                else:
                    fs_cache = FileStorage(
                        key=(), fs=cache.fs, path=cache.path
                    )

                if isinstance(remote, ObjectStorage):
                    fs_remote = ObjectStorage(key=(), odb=remote.odb)
                else:
                    fs_remote = FileStorage(
                        key=(),
                        fs=remote.fs,
                        path=remote.path,
                    )

                storage_by_fs[key] = StorageInfo(
                    cache=fs_cache, remote=fs_remote
                )

    by_fs: Dict[Tuple[str, str], DataIndex] = {}
    for key, storage in storage_by_fs.items():
        by_fs[key] = cache_index.view((*cache_key, *key))
        by_fs[key].storage_map[()] = storage

    return by_fs


def fetch(
    data,
    callback: "Callback" = DEFAULT_CALLBACK,
    jobs: Optional[int] = None,
):
    fetched, failed = 0, 0
    for (fs_protocol, _), fs_index in data.items():
        cache = fs_index.storage_map[()].cache
        remote = fs_index.storage_map[()].remote

        if callback != DEFAULT_CALLBACK:
            cb = callback.as_tqdm_callback(
                unit="file",
                total=len(fs_index),
                desc=f"Fetching from {fs_protocol}",
            )
        else:
            cb = callback

        with cb:
            if isinstance(cache, ObjectStorage) and isinstance(
                remote, ObjectStorage
            ):
                result = transfer(
                    remote.odb,
                    cache.odb,
                    [
                        entry.hash_info
                        for _, entry in fs_index.iteritems()
                        if entry.hash_info
                    ],
                    jobs=jobs,
                    src_index=get_index(remote.odb),
                    cache_odb=cache.odb,
                    verify=remote.odb.verify,
                    validate_status=_log_missing,
                    callback=cb,
                )
                fetched += len(result.transferred)
                failed += len(result.failed)
            elif isinstance(cache, ObjectStorage):
                md5(fs_index, storage="remote", check_meta=False)
                fetched += save(
                    fs_index, storage="remote", jobs=jobs, callback=cb
                )
            else:
                old = build(cache.path, cache.fs)
                diff = compare(old, fs_index)
                cache.fs.makedirs(
                    cache.fs.path.parent(cache.path), exist_ok=True
                )
                apply(
                    diff,
                    cache.path,
                    cache.fs,
                    update_meta=False,
                    storage="remote",
                    jobs=jobs,
                    callback=cb,
                )
                fetched += len(diff.changes.get("added", []))

    return fetched, failed
