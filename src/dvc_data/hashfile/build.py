import hashlib
import logging
import os
from typing import TYPE_CHECKING, Any, Optional, cast

from fsspec.callbacks import DEFAULT_CALLBACK, Callback

from dvc_data.callbacks import TqdmCallback

from .db.reference import ReferenceHashFileDB
from .hash import hash_file
from .meta import Meta
from .obj import HashFile

if TYPE_CHECKING:
    from typing import BinaryIO

    from dvc_objects.fs.base import AnyFSPath, FileSystem

    from ._ignore import Ignore
    from .db import HashFileDB
    from .tree import Tree


DefaultIgnoreFile = ".dvcignore"


class IgnoreInCollectedDirError(Exception):
    def __init__(self, ignore_file: str, ignore_dirname: str) -> None:
        super().__init__(
            f"{ignore_file} file should not be in collected dir path: "
            f"'{ignore_dirname}'"
        )


logger = logging.getLogger(__name__)


_STAGING_MEMFS_PATH = "dvc-staging"


def _upload_file(
    from_path: "AnyFSPath",
    fs: "FileSystem",
    odb: "HashFileDB",
    upload_odb: "HashFileDB",
    callback: Optional[Callback] = None,
) -> tuple[Meta, HashFile]:
    from dvc_objects.fs.utils import tmp_fname

    from .hash import HashStreamFile

    tmp_info = upload_odb.fs.join(upload_odb.path, tmp_fname())
    with fs.open(from_path, mode="rb") as stream:
        hashed_stream = HashStreamFile(stream)
        size = fs.size(from_path)
        cb = callback or TqdmCallback(
            desc=upload_odb.fs.name(from_path),
            bytes=True,
            size=size,
        )
        with cb:
            fileobj = cast("BinaryIO", hashed_stream)
            upload_odb.fs.put_file(fileobj, tmp_info, size=size, callback=cb)

    oid = hashed_stream.hash_value
    odb.add(tmp_info, upload_odb.fs, oid)
    meta = Meta(size=size)
    return meta, odb.get(oid)


def _build_file(path, fs, name, odb=None, upload_odb=None, dry_run=False):
    state = odb.state if odb else None
    meta, hash_info = hash_file(path, fs, name, state=state)
    if upload_odb and not dry_run:
        assert odb
        assert name == "md5"
        return _upload_file(path, fs, odb, upload_odb)

    oid = hash_info.value
    if dry_run:
        obj = HashFile(path, fs, hash_info)
    else:
        odb.add(path, fs, oid, hardlink=False)
        obj = odb.get(oid)

    return meta, obj


def _build_tree(
    path,
    fs,
    fs_info,
    name,
    odb=None,
    ignore: Optional["Ignore"] = None,
    callback: "Callback" = DEFAULT_CALLBACK,
    **kwargs,
):
    from .db import add_update_tree
    from .hash_info import HashInfo
    from .tree import Tree

    value = fs_info.get(name)
    if odb and value:
        try:
            tree = Tree.load(odb, HashInfo(name, value))
            return Meta(nfiles=len(tree)), tree
        except FileNotFoundError:
            pass

    path = path.rstrip(fs.sep)

    if ignore:
        walk_iter = ignore.walk(fs, path)
    else:
        walk_iter = fs.walk(path)

    tree_meta = Meta(size=0, nfiles=0, isdir=True)
    # assuring mypy that they are not None but integer
    assert tree_meta.size is not None
    assert tree_meta.nfiles is not None

    tree = Tree()

    for root, _, fnames in walk_iter:
        if DefaultIgnoreFile in fnames:
            raise IgnoreInCollectedDirError(
                DefaultIgnoreFile, fs.join(root, DefaultIgnoreFile)
            )

        # NOTE: we know for sure that root starts with path, so we can use
        # faster string manipulation instead of a more robust relparts()
        rel_key: tuple[Optional[Any], ...] = ()
        if root != path:
            rel_key = tuple(root[len(path) + 1 :].split(fs.sep))

        for fname in fnames:
            if fname == "":
                # NOTE: might happen with s3/gs/azure/etc, where empty
                # objects like `dir/` might be used to create an empty dir
                continue

            callback.relative_update(1)
            meta, obj = _build_file(
                f"{root}{fs.sep}{fname}", fs, name, odb=odb, **kwargs
            )
            key = (*rel_key, fname)
            tree.add(key, meta, obj.hash_info)
            tree_meta.size += meta.size or 0
            tree_meta.nfiles += 1

    if not tree_meta.nfiles:
        # This will raise FileNotFoundError if it is a
        # broken symlink or TreeError
        next(iter(fs.ls(path)), None)

    tree.digest()
    tree = add_update_tree(odb, tree)
    return tree_meta, tree


_url_cache: dict[str, str] = {}


def _make_staging_url(fs: "FileSystem", odb: "HashFileDB", path: Optional[str]):
    from dvc_objects.fs import Schemes

    url = f"{Schemes.MEMORY}://{_STAGING_MEMFS_PATH}-{odb.hash_name}"

    if path is not None:
        if odb.fs.protocol == Schemes.LOCAL:
            path = os.path.abspath(path)

        if path not in _url_cache:
            _url_cache[path] = hashlib.sha256(path.encode("utf-8")).hexdigest()

        url = fs.join(url, _url_cache[path])

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


def _build_external_tree_info(odb: "HashFileDB", tree: "Tree", name: str) -> "Tree":
    # NOTE: used only for external outputs. Initial reasoning was to be
    # able to validate .dir files right in the workspace (e.g. check s3
    # etag), but could be dropped for manual validation with regular md5,
    # that would be universal for all clouds.
    assert odb
    assert name != "md5"

    assert tree.fs
    assert tree.path
    assert tree.hash_info
    assert tree.hash_info.value

    oid = tree.hash_info.value
    odb.add(tree.path, tree.fs, oid)
    raw = odb.get(oid)
    _, hash_info = hash_file(raw.path, raw.fs, odb.hash_name, state=odb.state)

    assert hash_info.value

    tree.path = raw.path
    tree.fs = raw.fs
    tree.hash_info.name = hash_info.name
    tree.hash_info.value = hash_info.value

    if not tree.hash_info.value.endswith(".dir"):
        tree.hash_info.value += ".dir"
    return tree


def build(
    odb: "HashFileDB",
    path: "AnyFSPath",
    fs: "FileSystem",
    name: str,
    upload: bool = False,
    dry_run: bool = False,
    **kwargs,
) -> tuple["HashFileDB", "Meta", "HashFile"]:
    """Stage (prepare) objects from the given path for addition to an ODB.

    Returns at tuple of (object_store, object) where addition to the ODB can
    be completed by transferring the object from object_store to the dest ODB.

    If dry_run is True, object hashes will be computed and returned, but file
    objects themselves will not be added to the object_store ODB (i.e. the
    resulting file objects cannot transferred from object_store to another
    ODB).

    If upload is True, files will be uploaded to a temporary path on the dest
    ODB filesystem, and built objects will reference the uploaded path rather
    than the original source path.
    """
    assert path
    # assert protocol(path) == fs.protocol

    details = fs.info(path)
    staging = _get_staging(odb)

    if details["type"] == "directory":
        meta, obj = _build_tree(
            path,
            fs,
            details,
            name,
            odb=staging,
            upload_odb=odb if upload else None,
            dry_run=dry_run,
            **kwargs,
        )
        logger.debug("built tree '%s'", obj)
        if name != "md5":
            obj = _build_external_tree_info(odb, obj, name)
    else:
        meta, obj = _build_file(
            path,
            fs,
            name,
            odb=staging,
            upload_odb=odb if upload else None,
            dry_run=dry_run,
        )

    return staging, meta, obj
