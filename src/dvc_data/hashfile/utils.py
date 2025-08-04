import errno
import hashlib
import json
from typing import TYPE_CHECKING, Optional

from dvc_data.fsutils import _localfs_info

if TYPE_CHECKING:
    from dvc_objects.fs.base import AnyFSPath, FileSystem

    from ._ignore import Ignore
    from .diff import DiffResult


def to_nanoseconds(ts: float) -> int:
    return round(ts * 1_000_000_000)


def _tokenize_mtimes(files_mtimes: dict[str, float]) -> str:
    data = json.dumps(files_mtimes, sort_keys=True).encode("utf-8")
    digest = hashlib.md5(data, usedforsecurity=False)
    return digest.hexdigest()


def get_mtime_and_size(
    path: "AnyFSPath", fs: "FileSystem", ignore: Optional["Ignore"] = None
) -> tuple[str, int]:
    if not fs.isdir(path):
        base_stat = fs.info(path)
        size = base_stat["size"]
        mtime = str(to_nanoseconds(base_stat["mtime"]))
        return mtime, size

    size = 0
    files_mtimes = {}
    if ignore:
        walk_iterator = ignore.find(fs, path)
    else:
        walk_iterator = fs.find(path)
    for file_path in walk_iterator:
        try:
            stats = _localfs_info(file_path)
        except OSError as exc:
            # NOTE: broken symlink case.
            if exc.errno != errno.ENOENT:
                raise
            continue
        size += stats["size"]
        files_mtimes[file_path] = stats["mtime"]

    # We track file changes and moves, which cannot be detected with simply
    # max(mtime(f) for f in non_ignored_files)
    mtime = _tokenize_mtimes(files_mtimes)
    return mtime, size


def _get_mtime_from_changes(
    path: str,
    fs: "FileSystem",
    diff: "DiffResult",
    updated_mtimes: dict[str, float],
) -> str:
    from .diff import ROOT

    fs_info = _localfs_info(path)
    if fs_info["type"] == "file":
        return str(to_nanoseconds(fs_info["mtime"]))

    mtimes: dict[str, float] = {}
    mtimes.update(updated_mtimes)

    sep = fs.sep

    for change in diff.unchanged:
        key = change.old.key
        if key == ROOT:
            continue

        entry_path = sep.join((path, *key))
        if entry_path in mtimes:
            continue
        meta = change.old.meta
        mtime = meta.mtime if meta is not None else None
        if mtime is None:
            try:
                stats = _localfs_info(entry_path)
            except OSError as exc:
                # NOTE: broken symlink case.
                if exc.errno != errno.ENOENT:
                    raise
                continue
            mtime = stats["mtime"]
            assert mtime is not None
        mtimes[entry_path] = mtime

    return _tokenize_mtimes(mtimes)
