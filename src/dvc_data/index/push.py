import logging
from functools import partial
from typing import TYPE_CHECKING, Any, Optional, Set

from dvc_objects.fs.callbacks import DEFAULT_CALLBACK, TqdmCallback

from dvc_data.hashfile.db import get_index
from dvc_data.hashfile.transfer import transfer

from .build import build
from .checkout import _prune_existing_versions, apply, compare
from .fetch import _log_missing
from .index import DataIndex, ObjectStorage

if TYPE_CHECKING:
    from dvc_objects.fs import FileSystem
    from dvc_objects.fs.callbacks import Callback

    from dvc_data.hashfile.meta import Meta

    from .index import DataIndexKey

logger = logging.getLogger(__name__)


# for files, if our version's checksum (etag) matches the latest remote
# checksum, we do not need to push, even if the version IDs don't match
def _meta_checksum(fs: "FileSystem", meta: "Meta") -> Any:
    if not meta or meta.isdir:
        return meta
    assert fs.PARAM_CHECKSUM
    return getattr(meta, fs.PARAM_CHECKSUM)


def _onerror(cache, data, failed_keys, src_path, dest_path, exc):
    if not isinstance(exc, FileNotFoundError) or cache.fs.exists(src_path):
        failed_keys.add(data.fs.relparts(dest_path, data.path))

    logger.debug(
        "failed to create '%s' from '%s'",
        src_path,
        dest_path,
        exc_info=True,
    )


def _filter_missing(index):
    ret = DataIndex()
    ret.storage_map = index.storage_map

    for _, entry in index.items():
        try:
            cache_fs, cache_path = index.storage_map.get_cache(entry)
        except ValueError:
            continue

        if cache_fs.exists(cache_path):
            ret.add(entry)

    return ret


def push(
    idxs,
    callback: "Callback" = DEFAULT_CALLBACK,
    jobs: Optional[int] = None,
):
    pushed, failed = 0, 0
    for fs_index in idxs:
        data = fs_index.storage_map[()].data
        cache = fs_index.storage_map[()].cache

        if isinstance(cache, ObjectStorage) and isinstance(data, ObjectStorage):
            with TqdmCallback(unit="file", desc=f"Pushing to {data.fs.protocol}") as cb:
                result = transfer(
                    cache.odb,
                    data.odb,
                    [
                        entry.hash_info
                        for _, entry in fs_index.iteritems()
                        if entry.hash_info
                    ],
                    jobs=jobs,
                    dest_index=get_index(data.odb),
                    cache_odb=data.odb,
                    validate_status=_log_missing,
                    callback=cb,
                )
                pushed += len(result.transferred)
                failed += len(result.failed)
        else:
            old = build(data.path, data.fs)

            existing_fs_index = _filter_missing(fs_index)
            diff = compare(
                old,
                existing_fs_index,
                meta_only=True,
                meta_cmp_key=partial(_meta_checksum, data.fs),
            )
            data.fs.makedirs(data.fs.parent(data.path), exist_ok=True)

            failed_keys: Set["DataIndexKey"] = set()

            if data.fs.version_aware:
                desc = f"Checking status of existing versions in {data.path!r}"
                with TqdmCallback(desc=desc, unit="file") as cb:
                    diff.files_create = list(
                        _prune_existing_versions(
                            diff.files_create, data.fs, data.path, callback=cb
                        )
                    )

            with TqdmCallback(unit="file", desc=f"Pushing to {data.fs.protocol}") as cb:
                cb.set_size(len(diff.files_create))
                apply(
                    diff,
                    data.path,
                    data.fs,
                    update_meta=False,
                    storage="cache",
                    jobs=jobs,
                    callback=cb,
                    links=["reflink", "copy"],
                    onerror=partial(_onerror, cache, data, failed_keys),
                )

            added_keys = {entry.key for entry in diff.files_create}
            pushed += len(added_keys - failed_keys)
            failed += len(failed_keys)

    return pushed, failed
