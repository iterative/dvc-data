import logging
from typing import TYPE_CHECKING, Optional

from dvc_objects.fs.callbacks import DEFAULT_CALLBACK

from dvc_data.hashfile.db import get_index
from dvc_data.hashfile.transfer import transfer

from .build import build
from .checkout import apply, compare
from .fetch import _log_missing
from .index import ObjectStorage

if TYPE_CHECKING:
    from dvc_objects.fs.callbacks import Callback


logger = logging.getLogger(__name__)


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
            if isinstance(cache, ObjectStorage) and isinstance(
                data, ObjectStorage
            ):
                result = transfer(
                    cache.odb,
                    data.odb,
                    [
                        entry.hash_info
                        for _, entry in fs_index.iteritems()
                        if entry.hash_info
                    ],
                    jobs=jobs,
                    src_index=get_index(cache.odb),
                    cache_odb=data.odb,
                    verify=cache.odb.verify,
                    validate_status=_log_missing,
                    callback=cb,
                )
                fetched += len(result.transferred)
                failed += len(result.failed)
            else:
                old = build(data.path, data.fs)
                diff = compare(old, fs_index)
                data.fs.makedirs(data.fs.path.parent(data.path), exist_ok=True)
                apply(
                    diff,
                    data.path,
                    data.fs,
                    update_meta=False,
                    storage="cache",
                    jobs=jobs,
                    callback=cb,
                )
                fetched += len(diff.changes.get("added", []))

    return fetched, failed
