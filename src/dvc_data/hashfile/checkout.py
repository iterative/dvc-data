import logging
from functools import partial
from multiprocessing import Pool
from typing import TYPE_CHECKING, Optional

from dvc_objects.fs.generic import test_links, transfer
from dvc_objects.fs.local import LocalFileSystem
from fsspec.callbacks import DEFAULT_CALLBACK

from dvc_data.compat import batched

from .build import build
from .diff import ROOT
from .diff import diff as odiff

if TYPE_CHECKING:
    from fsspec import Callback

    from ._ignore import Ignore
    from .hash_info import HashInfo

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
        diff.modified.extend(diff.unchanged)
    else:
        for change in diff.unchanged:
            if not change.new.in_cache and not (
                change.new.oid and change.new.oid.isdir
            ):
                diff.modified.append(change)

    return diff


class Link:
    def __init__(self, links, callback: Optional["Callback"] = None):
        self._links = links
        self._callback = callback

    def __call__(self, cache, from_path, to_fs, to_path):
        parent = to_fs.parent(to_path)
        to_fs.makedirs(parent)
        kw = {"callback": self._callback} if self._callback else {}
        try:
            transfer(
                cache.fs,
                from_path,
                to_fs,
                to_path,
                links=self._links,
                **kw,
            )
        except FileNotFoundError as exc:
            raise CheckoutError([to_path]) from exc
        except OSError as exc:
            raise LinkError(to_path) from exc


def _checkout_files(changes, fs, path, cache, force, link, relink, state, prompt):
    failed = []
    hashes_to_update = []
    is_local_fs = isinstance(fs, LocalFileSystem)
    for change in changes:
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
    return failed, hashes_to_update


def _checkout(
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
    link = Link(links, None if isinstance(fs, LocalFileSystem) else progress_callback)
    for change in diff.deleted:
        entry_path = fs.join(path, *change.old.key) if change.old.key != ROOT else path
        _remove(entry_path, fs, change.old.in_cache, force=force, prompt=prompt)

    progress_callback.relative_update(len(diff.deleted))

    failed = []
    hashes_to_update: list[tuple[str, HashInfo, None]] = []

    checkout_func = partial(
        _checkout_files,
        fs=fs,
        path=path,
        cache=cache,
        force=force,
        link=link,
        relink=relink,
        state=state,
        prompt=prompt,
    )
    changes = diff.added + diff.modified
    if isinstance(fs, LocalFileSystem):
        with Pool() as pool:
            chunks = batched(changes, 1000)
            for _failed, _hashes in pool.imap_unordered(checkout_func, chunks):
                progress_callback.absolute_update(len(hashes_to_update))
                failed.extend(_failed)
                hashes_to_update.extend(_hashes)
    else:
        failed, hashes_to_update = checkout_func(changes)

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

    if diff and state:
        state.save_link(path, fs)

    if failed or not diff:
        if progress_callback and obj:
            progress_callback.relative_update(len(obj))
        if failed:
            raise CheckoutError(failed)
        return

    return bool(diff) and not relink
