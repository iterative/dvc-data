import logging
from typing import TYPE_CHECKING, Optional

from dvc_objects.fs.callbacks import DEFAULT_CALLBACK

from dvc_data.hashfile.db import get_index
from dvc_data.hashfile.transfer import transfer

from .build import build
from .checkout import apply, compare
from .collect import collect  # noqa: F401, pylint: disable=unused-import
from .index import ObjectStorage
from .save import md5, save

if TYPE_CHECKING:
    from dvc_objects.fs.callbacks import Callback

    from dvc_data.hashfile.status import CompareStatusResult

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
            cb = callback.as_tqdm_callback(
                unit="file",
                total=len(fs_index),
                desc=f"Fetching from {data.fs.protocol}",
            )
        else:
            cb = callback

        try:
            # NOTE: make sure there are no auth errors
            data.fs.exists(data.path)
        except Exception:  # pylint: disable=W0703
            failed += len(fs_index)
            logger.exception(
                f"failed to connect to {data.fs.protocol} ({data.path})"
            )
            continue

        with cb:
            if isinstance(cache, ObjectStorage) and isinstance(
                data, ObjectStorage
            ):
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
                fetched += save(fs_index, jobs=jobs, callback=cb)
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
                    storage="data",
                    jobs=jobs,
                    callback=cb,
                )
                fetched += len(diff.changes.get("added", []))

    return fetched, failed
