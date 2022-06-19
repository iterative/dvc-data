import hashlib
import logging
import os
from functools import partial
from typing import TYPE_CHECKING, Dict, Optional, Tuple

from dvc_objects._tqdm import Tqdm

from .db.reference import ReferenceHashFileDB
from .hashfile.hash import hash_file
from .hashfile.meta import Meta
from .hashfile.obj import HashFile

if TYPE_CHECKING:
    from dvc_objects._ignore import Ignore
    from dvc_objects.fs.base import AnyFSPath, FileSystem

    from .hashfile.db import HashFileDB


DefaultIgnoreFile = ".dvcignore"


class IgnoreInCollectedDirError(Exception):
    def __init__(self, ignore_file: str, ignore_dirname: str) -> None:
        super().__init__(
            f"{ignore_file} file should not be in collected dir path: "
            f"'{ignore_dirname}'"
        )


logger = logging.getLogger(__name__)


_STAGING_MEMFS_PATH = "dvc-staging"


def _upload_file(from_path, fs, odb, upload_odb, callback=None):
    from dvc_objects.fs.callbacks import Callback
    from dvc_objects.fs.utils import tmp_fname

    from .hashfile.hash import HashStreamFile

    path = upload_odb.fs.path
    tmp_info = path.join(upload_odb.path, tmp_fname())
    with fs.open(from_path, mode="rb") as stream:
        stream = HashStreamFile(stream)
        size = fs.size(from_path)
        with Callback.as_tqdm_callback(
            callback,
            desc=path.name(from_path),
            bytes=True,
            size=size,
        ) as cb:
            upload_odb.fs.put_file(stream, tmp_info, size=size, callback=cb)

    oid = stream.hash_value
    odb.add(tmp_info, upload_odb.fs, oid)
    meta = Meta(size=size)
    return from_path, meta, odb.get(oid)


def _stage_file(path, fs, name, odb=None, upload_odb=None, dry_run=False):
    state = odb.state if odb else None
    meta, hash_info = hash_file(path, fs, name, state=state)
    if upload_odb and not dry_run:
        assert odb and name == "md5"
        return _upload_file(path, fs, odb, upload_odb)

    oid = hash_info.value
    if dry_run:
        obj = HashFile(path, fs, hash_info)
    else:
        odb.add(path, fs, oid, hardlink=False)
        obj = odb.get(oid)

    return path, meta, obj


def _build_objects(
    path,
    fs,
    name,
    ignore: "Ignore" = None,
    no_progress_bar=False,
    **kwargs,
):
    if ignore:
        walk_iterator = ignore.find(fs, path)
    else:
        walk_iterator = fs.find(path)
    with Tqdm(
        unit="md5",
        desc="Computing file/dir hashes (only done once)",
        disable=no_progress_bar,
    ) as pbar:
        worker = pbar.wrap_fn(
            partial(
                _stage_file,
                fs=fs,
                name=name,
                **kwargs,
            )
        )
        yield from map(worker, walk_iterator)


def _iter_objects(path, fs, name, **kwargs):
    yield from _build_objects(path, fs, name, **kwargs)


def _build_tree(path, fs, name, **kwargs):
    from .objects.tree import Tree

    tree_meta = Meta(size=0, nfiles=0)
    # assuring mypy that they are not None but integer
    assert tree_meta.size is not None
    assert tree_meta.nfiles is not None

    tree = Tree()
    for file_path, meta, obj in _iter_objects(path, fs, name, **kwargs):
        if fs.path.name(file_path) == DefaultIgnoreFile:
            raise IgnoreInCollectedDirError(
                DefaultIgnoreFile, fs.path.parent(file_path)
            )

        # NOTE: this is lossy transformation:
        #   "hey\there" -> "hey/there"
        #   "hey/there" -> "hey/there"
        # The latter is fine filename on Windows, which
        # will transform to dir/file on back transform.
        #
        # Yes, this is a BUG, as long as we permit "/" in
        # filenames on Windows and "\" on Unix

        # NOTE: we know for sure that file_path starts with path, so we can
        # use faster string manipulation instead of a more robust relparts()
        key = tuple(file_path[len(path) + 1 :].split(fs.sep))
        assert key
        tree.add(key, meta, obj.hash_info)

        tree_meta.size += meta.size or 0
        tree_meta.nfiles += 1

    return tree_meta, tree


def _stage_tree(path, fs, fs_info, name, odb=None, **kwargs):
    from .hashfile.hash_info import HashInfo
    from .objects.tree import Tree

    value = fs_info.get(name)
    if odb and value:
        try:
            tree = Tree.load(odb, HashInfo(name, value))
            return Meta(nfiles=len(tree)), tree
        except FileNotFoundError:
            pass

    meta, tree = _build_tree(path, fs, name, odb=odb, **kwargs)
    tree.digest()
    odb.add(tree.path, tree.fs, tree.oid, hardlink=False)
    raw = odb.get(tree.oid)
    tree.fs = raw.fs
    tree.path = raw.path
    return meta, tree


_url_cache: Dict[str, str] = {}


def _make_staging_url(
    fs: "FileSystem", odb: "HashFileDB", path: Optional[str]
):
    from dvc_objects.fs import Schemes

    url = f"{Schemes.MEMORY}://{_STAGING_MEMFS_PATH}"

    if path is not None:
        if odb.fs.protocol == Schemes.LOCAL:
            path = os.path.abspath(path)

        if path not in _url_cache:
            _url_cache[path] = hashlib.sha256(path.encode("utf-8")).hexdigest()

        url = fs.path.join(url, _url_cache[path])

    return url


def _get_staging(odb: "HashFileDB") -> "ReferenceHashFileDB":
    """Return an ODB that can be used for staging objects.

    Staging will be a reference ODB stored in the the global memfs.
    """

    from dvc_objects.fs import MemoryFileSystem

    fs = MemoryFileSystem()
    path = _make_staging_url(fs, odb, odb.path)
    state = odb.state
    return ReferenceHashFileDB(fs, path, state=state, hash_name=odb.hash_name)


def _stage_external_tree_info(odb, tree, name):
    # NOTE: used only for external outputs. Initial reasoning was to be
    # able to validate .dir files right in the workspace (e.g. check s3
    # etag), but could be dropped for manual validation with regular md5,
    # that would be universal for all clouds.
    assert odb and name != "md5"

    oid = tree.hash_info.value
    odb.add(tree.path, tree.fs, oid)
    raw = odb.get(oid)
    _, hash_info = hash_file(raw.path, raw.fs, name, state=odb.state)
    tree.path = raw.path
    tree.fs = raw.fs
    tree.hash_info.name = hash_info.name
    tree.hash_info.value = hash_info.value
    if not tree.hash_info.value.endswith(".dir"):
        tree.hash_info.value += ".dir"
    return tree


def stage(
    odb: "HashFileDB",
    path: "AnyFSPath",
    fs: "FileSystem",
    name: str,
    upload: bool = False,
    dry_run: bool = False,
    **kwargs,
) -> Tuple["HashFileDB", "Meta", "HashFile"]:
    """Stage (prepare) objects from the given path for addition to an ODB.

    Returns at tuple of (staging_odb, object) where addition to the ODB can
    be completed by transferring the object from staging to the dest ODB.

    If dry_run is True, object hashes will be computed and returned, but file
    objects themselves will not be added to the staging ODB (i.e. the resulting
    file objects cannot transferred from staging to another ODB).

    If upload is True, files will be uploaded to a temporary path on the dest
    ODB filesystem, and staged objects will reference the uploaded path rather
    than the original source path.
    """
    assert path
    # assert protocol(path) == fs.protocol

    details = fs.info(path)
    staging = _get_staging(odb)

    if details["type"] == "directory":
        meta, obj = _stage_tree(
            path,
            fs,
            details,
            name,
            odb=staging,
            upload_odb=odb if upload else None,
            dry_run=dry_run,
            **kwargs,
        )
        logger.debug("staged tree '%s'", obj)
        if name != "md5":
            obj = _stage_external_tree_info(odb, obj, name)
    else:
        _, meta, obj = _stage_file(
            path,
            fs,
            name,
            odb=staging,
            upload_odb=odb if upload else None,
            dry_run=dry_run,
        )

    return staging, meta, obj
