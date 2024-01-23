import logging
from functools import partial
from typing import TYPE_CHECKING, Optional, Set

from dvc_objects.fs.callbacks import DEFAULT_CALLBACK, TqdmCallback

from dvc_data.hashfile.db import get_index
from dvc_data.hashfile.transfer import transfer

from .build import build
from .checkout import apply, compare
from .collect import collect  # noqa: F401
from .index import ObjectStorage
from .save import md5, save

if TYPE_CHECKING:
    from dvc_objects.fs.callbacks import Callback

    from dvc_data.hashfile.status import CompareStatusResult

    from .index import DataIndexKey

logger = logging.getLogger(__name__)


def _log_missing(status: "CompareStatusResult"):
    if status.missing:
        missing_desc = "\n".join(f"{hash_info}" for hash_info in status.missing)
        logger.warning(
            "Some of the cache files do not exist neither locally "
            "nor on remote. Missing cache files:\n%s",
            missing_desc,
        )


def _onerror(data, cache, failed_keys, src_path, dest_path, exc):
    if not isinstance(exc, FileNotFoundError) or data.fs.exists(src_path):
        failed_keys.add(cache.fs.relparts(dest_path, cache.path))

    logger.debug(
        "failed to create '%s' from '%s'",
        src_path,
        dest_path,
        exc_info=True,
    )


def fetch(
    idxs,
    callback: "Callback" = DEFAULT_CALLBACK,
    jobs: Optional[int] = None,
):
    fetched, failed = 0, 0
    for fs_index in idxs:
        data = fs_index.storage_map[()].data
        cache = fs_index.storage_map[()].cache

        if callback != DEFAULT_CALLBACK:
            cb = TqdmCallback(
                unit="file",
                total=len(fs_index),
                desc=f"Fetching from {data.fs.protocol}",
            )
        else:
            cb = callback

        try:
            # NOTE: make sure there are no auth errors
            data.fs.exists(data.path)
        except Exception:
            failed += len(fs_index)
            logger.exception(
                "failed to connect to %s (%s)", data.fs.protocol, data.path
            )
            continue

        with cb:
            if isinstance(cache, ObjectStorage) and isinstance(data, ObjectStorage):
                result = transfer(
                    data.odb,
                    cache.odb,
                    [
                        entry.hash_info
                        for _, entry in fs_index.iteritems()
                        if entry.hash_info
                    ],
                    jobs=jobs,
                    src_index=get_index(data.odb),
                    cache_odb=cache.odb,
                    verify=data.odb.verify,
                    validate_status=_log_missing,
                    callback=cb,
                )
                fetched += len(result.transferred)
                failed += len(result.failed)
            elif isinstance(cache, ObjectStorage):
                md5(fs_index, check_meta=False)

                def _on_error(failed, oid, exc):
                    if isinstance(exc, FileNotFoundError):
                        return
                    failed += 1
                    logger.debug(
                        "failed to transfer '%s'",
                        oid,
                        exc_info=True,
                    )

                fetched += save(
                    fs_index,
                    jobs=jobs,
                    callback=cb,
                    on_error=partial(_on_error, failed),
                )
            else:
                old = build(cache.path, cache.fs)
                diff = compare(old, fs_index)
                cache.fs.makedirs(cache.fs.parent(cache.path), exist_ok=True)

                failed_keys: Set["DataIndexKey"] = set()
                apply(
                    diff,
                    cache.path,
                    cache.fs,
                    update_meta=False,
                    storage="data",
                    jobs=jobs,
                    callback=cb,
                    onerror=partial(_onerror, data, cache, failed_keys),
                )

                added_keys = {entry.key for entry in diff.files_create}
                fetched += len(added_keys - failed_keys)
                failed += len(failed_keys)

    return fetched, failed
