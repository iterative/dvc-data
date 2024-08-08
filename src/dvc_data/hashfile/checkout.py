import logging
import os
from functools import partial
from itertools import chain
from os.path import samefile
from typing import TYPE_CHECKING, Optional

from dvc_objects.executors import ThreadPoolExecutor
from dvc_objects.fs.generic import test_links, transfer
from dvc_objects.fs.local import LocalFileSystem
from fsspec.callbacks import DEFAULT_CALLBACK

from .build import build
from .diff import ROOT, DiffResult
from .diff import diff as odiff

if TYPE_CHECKING:
    from dvc_objects.fs.base import FileSystem
    from fsspec import Callback

    from ._ignore import Ignore
    from .db import HashFileDB
    from .diff import Change
    from .hash_info import HashInfo
    from .meta import Meta

logger = logging.getLogger(__name__)


class PromptError(Exception):
    def __init__(self, path: str) -> None:
        self.path = path
        super().__init__(f"unable to remove '{path}' without a confirmation.")


class CheckoutError(Exception):
    def __init__(self, paths: list[str]) -> None:
        self.paths = paths
        super().__init__("Checkout failed")


class LinkError(Exception):
    def __init__(self, path: str) -> None:
        self.path = path
        super().__init__("No possible cache link types for '{path}'.")


def _remove(path, fs, in_cache, force=False, prompt=None):
    if not force and not in_cache:
        if not fs.exists(path):
            return

        msg = (
            f"file/directory '{path}' is going to be removed. "
            "Are you sure you want to proceed?"
        )

        if prompt is None or not prompt(msg):
            raise PromptError(path)

    try:
        fs.remove(path)
    except FileNotFoundError:
        pass


def _relink(link, cache, cache_info, fs, path, in_cache, force, prompt=None):
    _remove(path, fs, in_cache, force=force, prompt=prompt)
    link(cache, cache_info, fs, path)
    # NOTE: Depending on a file system (e.g. on NTFS), `_remove` might reset
    # read-only permissions in order to delete a hardlink to protected object,
    # which will also reset it for the object itself, making it unprotected,
    # so we need to protect it back.
    cache.protect(cache_info)


def _checkout_file(
    link,
    path,
    fs,
    change,
    cache,
    force,
    relink=False,
    state=None,
    prompt=None,
):
    """The file is changed we need to checkout a new copy"""
    modified = False

    cache_path = cache.oid_to_path(change.new.oid.value)
    if change.old.oid:
        if relink:
            if fs.iscopy(path) and cache.cache_types[0] == "copy":
                cache.unprotect(path)
            else:
                _relink(
                    link,
                    cache,
                    cache_path,
                    fs,
                    path,
                    change.old.in_cache,
                    force=force,
                    prompt=prompt,
                )
        else:
            modified = True
            _relink(
                link,
                cache,
                cache_path,
                fs,
                path,
                change.old.in_cache,
                force=force,
                prompt=prompt,
            )
    else:
        link(cache, cache_path, fs, path)
        modified = True
    return modified


def _needs_relink(
    path: str, cache: "HashFileDB", meta: "Meta", oid: Optional[str]
) -> bool:
    destination = meta.destination
    is_symlink = meta.is_link
    is_hardlink = meta.nlink > 1
    is_copy = not is_symlink and not is_hardlink

    obj_path: Optional[str] = None

    for link_type in cache.cache_types:
        if link_type in ("copy", "reflink") and is_copy:
            return False
        if not oid:
            continue

        if obj_path is None:
            obj_path = cache.oid_to_path(oid)

        if link_type == "symlink" and is_symlink and destination:
            return destination != obj_path
        if link_type == "hardlink" and is_hardlink and samefile(path, obj_path):
            return False
    return True


def _change_needs_relink(
    change: "Change", path: str, fs: "FileSystem", cache: "HashFileDB"
) -> tuple["Change", bool]:
    meta = change.old.meta
    if meta is None:
        return change, True

    assert meta is not None
    p = fs.sep.join((path, *change.old.key))
    oid = change.new.oid.value if change.new.oid else None
    relink = _needs_relink(p, cache, meta, oid)
    return change, relink


def _check_relink(
    diff: DiffResult, path: str, fs: "FileSystem", cache: "HashFileDB"
) -> None:
    if not isinstance(fs, LocalFileSystem) or not isinstance(cache.fs, LocalFileSystem):
        diff.modified.extend(diff.unchanged)
        return

    relink_func = partial(_change_needs_relink, fs=fs, path=path, cache=cache)
    max_workers = min(16, (os.cpu_count() or 1) + 4)
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        for change, relink in executor.imap_unordered(relink_func, diff.unchanged):
            if relink:
                diff.modified.append(change)


def _diff(
    path,
    fs,
    obj,
    cache,
    relink=False,
    ignore: Optional["Ignore"] = None,
):
    old = None
    try:
        _, _, old = build(
            cache,
            path,
            fs,
            obj.hash_info.name if obj else cache.hash_name,
            dry_run=True,
            ignore=ignore,
        )
    except FileNotFoundError:
        pass

    diff = odiff(old, obj, cache)
    if relink:
        _check_relink(diff, path, fs, cache)
    else:
        for change in diff.unchanged:
            if not change.new.in_cache and not (
                change.new.oid and change.new.oid.isdir
            ):
                diff.modified.append(change)

    return diff


class Link:
    def __init__(self, links, callback: "Callback" = DEFAULT_CALLBACK):
        self._links = links
        self._callback = callback

    def __call__(self, cache, from_path, to_fs, to_path):
        parent = to_fs.parent(to_path)
        to_fs.makedirs(parent)
        try:
            transfer(
                cache.fs,
                from_path,
                to_fs,
                to_path,
                links=self._links,
                callback=self._callback,
            )
        except FileNotFoundError as exc:
            raise CheckoutError([to_path]) from exc
        except OSError as exc:
            raise LinkError(to_path) from exc


def _checkout(  # noqa: C901
    diff,
    path,
    fs,
    cache,
    force=False,
    progress_callback: "Callback" = DEFAULT_CALLBACK,
    relink=False,
    state=None,
    prompt=None,
):
    if not diff:
        return

    links = test_links(cache.cache_types, cache.fs, cache.path, fs, path)
    if not links:
        raise LinkError(path)

    progress_callback.set_size(sum(diff.stats.values()))
    link = Link(links, callback=progress_callback)
    for change in diff.deleted:
        entry_path = fs.join(path, *change.old.key) if change.old.key != ROOT else path
        _remove(entry_path, fs, change.old.in_cache, force=force, prompt=prompt)

    failed = []
    hashes_to_update: list[tuple[str, HashInfo, None]] = []
    is_local_fs = isinstance(fs, LocalFileSystem)
    for change in chain(diff.added, diff.modified):
        entry_path = fs.join(path, *change.new.key) if change.new.key != ROOT else path
        if change.new.oid.isdir:
            fs.makedirs(entry_path)
            continue

        try:
            _checkout_file(
                link,
                entry_path,
                fs,
                change,
                cache,
                force,
                relink,
                state=state,
                prompt=prompt,
            )
        except CheckoutError as exc:
            failed.extend(exc.paths)
        else:
            if is_local_fs:
                info = fs.info(entry_path)
                hashes_to_update.append((entry_path, change.new.oid, info))

    if state is not None:
        state.save_many(hashes_to_update, fs)

    if failed:
        raise CheckoutError(failed)


def checkout(  # noqa: PLR0913
    path,
    fs,
    obj,
    cache,
    force=False,
    progress_callback: "Callback" = DEFAULT_CALLBACK,
    relink=False,
    quiet=False,
    ignore: Optional["Ignore"] = None,
    state=None,
    prompt=None,
):
    # if protocol(path) not in ["local", cache.fs.protocol]:
    #    raise NotImplementedError

    diff = _diff(
        path,
        fs,
        obj,
        cache,
        relink=relink,
        ignore=ignore,
    )

    failed = []
    if not obj:
        if not quiet:
            logger.warning(
                "No file hash info found for '%s'. It won't be created.",
                path,
            )
        failed.append(path)

    try:
        _checkout(
            diff,
            path,
            fs,
            cache,
            force=force,
            progress_callback=progress_callback,
            relink=relink,
            state=state,
            prompt=prompt,
        )
    except CheckoutError as exc:
        failed.extend(exc.paths)

    if state and (diff or relink):
        state.save_link(path, fs)

    if failed or not diff:
        if progress_callback and obj:
            progress_callback.relative_update(len(obj))
        if failed:
            raise CheckoutError(failed)
        return

    return bool(diff) and not relink
