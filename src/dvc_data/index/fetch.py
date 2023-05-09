import logging
from collections import defaultdict
from typing import TYPE_CHECKING, Dict, Optional, Tuple

from dvc_objects.fs.callbacks import DEFAULT_CALLBACK

from dvc_data.hashfile.db import get_index
from dvc_data.hashfile.transfer import transfer

from .build import build
from .checkout import checkout
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


def fetch(  # noqa: C901
    idxs,
    callback: "Callback" = DEFAULT_CALLBACK,
    jobs: Optional[int] = None,
    **kwargs,
):
    by_fs: Dict[Tuple[str, str], DataIndex] = defaultdict(DataIndex)

    for idx in idxs:
        for prefix, storage_info in idx.storage_map.items():
            remote = storage_info.remote
            cache = storage_info.cache
            if not remote or not cache:
                continue

            # FIXME should use fsid instead of protocol
            fs_index = by_fs[(remote.fs.protocol, remote.path)]

            if () not in fs_index.storage_map:
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

                fs_index.storage_map[()] = StorageInfo(
                    cache=fs_cache, remote=fs_remote
                )

            try:
                for _, entry in idx.iteritems(prefix):
                    try:
                        storage_key = remote.get_key(entry)
                    except ValueError:
                        continue

                    fs_index[storage_key] = DataIndexEntry(
                        key=storage_key,
                        meta=entry.meta,
                        hash_info=entry.hash_info,
                    )
            except KeyError:
                pass

    fetched, failed = 0, 0
    for (fs_protocol, _), fs_index in by_fs.items():
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
                        for entry in fs_index.values()
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
                md5(fs_index, storage="remote")
                fetched += save(
                    fs_index, storage="remote", jobs=jobs, callback=cb
                )
            else:
                old = build(cache.path, cache.fs)
                checkout_stats = checkout(
                    fs_index,
                    cache.path,
                    cache.fs,
                    storage="remote",
                    old=old,
                    jobs=jobs,
                    callback=cb,
                )
                fetched += len(checkout_stats.get("added", []))

    return fetched, failed
