from functools import partial, wraps
from typing import TYPE_CHECKING, Any, Callable, Dict, List, NamedTuple, Tuple

from dvc_objects.executors import ThreadPoolExecutor
from dvc_objects.fs.callbacks import DEFAULT_CALLBACK

if TYPE_CHECKING:
    from dvc_objects.fs.base import FileSystem
    from dvc_objects.fs.callbacks import Callback

    from . import HashFileDB


class PreparedMigration(NamedTuple):
    src: "HashFileDB"
    dest: "HashFileDB"
    paths: List[str]
    oids: List[str]


def migrate(
    migration: "PreparedMigration", callback: "Callback" = DEFAULT_CALLBACK
) -> int:
    """Migrate objects from one HashFileDB to another.

    Files from src will be re-hashed and transferred to dest with hardlinking
    enabled.
    """
    src, dest, paths, oids = migration
    return dest.add(paths, src.fs, oids, hardlink=True, callback=callback)


def prepare(
    src: "HashFileDB",
    dest: "HashFileDB",
    callback: "Callback" = DEFAULT_CALLBACK,
) -> PreparedMigration:
    """Prepare to migrate objects from one HashFileDB to another.

    Objects from src will be rehashed for addition to dest.
    """
    src_paths = [src.oid_to_path(oid) for oid in src._list_oids()]
    callback.set_size(len(src_paths))
    with ThreadPoolExecutor(
        max_workers=src.fs.hash_jobs, cancel_on_error=True
    ) as executor:
        func = partial(
            _hash_task,
            dest.hash_name,
            src.fs,
            state=dest.state,
            callback=callback,
        )
        results = list(executor.imap_unordered(func, src_paths))
        if results:
            paths, oids = zip(*results)
        else:
            paths, oids = (), ()
    return PreparedMigration(src, dest, list(paths), list(oids))


def _hash_task(
    hash_name: str,
    fs: "FileSystem",
    path: str,
    callback: "Callback" = DEFAULT_CALLBACK,
    **kwargs,
) -> Tuple[str, str]:
    from dvc_data.hashfile.hash import hash_file

    func = _wrap_hash_file(callback, hash_file)
    _meta, hash_info = func(path, fs, hash_name, **kwargs)
    assert hash_info.value
    if path.endswith(".dir"):
        hash_info.value += ".dir"
    return path, hash_info.value


def _wrap_hash_file(callback: "Callback", fn: Callable):
    @wraps(fn)
    def func(path: str, *args, **kwargs):
        kw: Dict[str, Any] = dict(kwargs)
        with callback.branch(path, path, kw):
            res = fn(path, *args, **kw)
            callback.relative_update()
            return res

    return func
