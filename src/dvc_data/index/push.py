import logging
from functools import partial
from typing import TYPE_CHECKING, Any, Optional

from dvc_objects.fs.callbacks import DEFAULT_CALLBACK

from dvc_data.hashfile.db import get_index
from dvc_data.hashfile.transfer import transfer

from .build import build
from .checkout import apply, compare
from .fetch import _log_missing
from .index import ObjectStorage

if TYPE_CHECKING:
    from dvc_objects.fs import FileSystem
    from dvc_objects.fs.callbacks import Callback

    from dvc_data.hashfile.meta import Meta


logger = logging.getLogger(__name__)


# for files, if our version's checksum (etag) matches the latest remote
# checksum, we do not need to push, even if the version IDs don't match
def _meta_checksum(fs: "FileSystem", meta: "Meta") -> Any:
    if not meta or meta.isdir:
        return meta
    assert fs.PARAM_CHECKSUM
    return getattr(meta, fs.PARAM_CHECKSUM)


def _onerror(src_path, dest_path, _exc):
    logger.debug(
        "failed to create '%s' from '%s'",
        src_path,
        dest_path,
        exc_info=True,
    )


def push(
    idxs,
    callback: "Callback" = DEFAULT_CALLBACK,
    jobs: Optional[int] = None,
):
    fetched, failed = 0, 0
    for fs_index in idxs:
        data = fs_index.storage_map[()].data
        cache = fs_index.storage_map[()].cache

        if callback != DEFAULT_CALLBACK:
            cb = callback.as_tqdm_callback(
                unit="file",
                total=len(fs_index),
                desc=f"Pushing to {data.fs.protocol}",
            )
        else:
            cb = callback

        with cb:
            if isinstance(cache, ObjectStorage) and isinstance(data, ObjectStorage):
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
                fetched += len(result.transferred)
                failed += len(result.failed)
            else:
                old = build(data.path, data.fs)
                diff = compare(
                    old,
                    fs_index,
                    meta_only=True,
                    meta_cmp_key=partial(_meta_checksum, data.fs),
                )
                data.fs.makedirs(data.fs.path.parent(data.path), exist_ok=True)
                apply(
                    diff,
                    data.path,
                    data.fs,
                    latest_only=False,
                    update_meta=False,
                    storage="cache",
                    jobs=jobs,
                    callback=cb,
                    links=["reflink", "copy"],
                    onerror=_onerror,
                )
                fetched += len(diff.changes.get("added", []))

    return fetched, failed
