import hashlib
import logging
import os
from collections.abc import Iterator
from typing import TYPE_CHECKING, Any, Optional, cast

from dvc_objects.executors import ThreadPoolExecutor
from dvc_objects.fs.local import LocalFileSystem
from fsspec.callbacks import DEFAULT_CALLBACK, Callback

from dvc_data.callbacks import TqdmCallback
from dvc_data.fsutils import _localfs_info
from dvc_data.hashfile.hash_info import HashInfo
from dvc_data.hashfile.state import StateBase, StateNoop

from .db.reference import ReferenceHashFileDB
from .hash import hash_file
from .meta import Meta
from .obj import HashFile

if TYPE_CHECKING:
    from typing import BinaryIO

    from dvc_objects.db import ObjectDB
    from dvc_objects.fs.base import AnyFSPath, FileSystem

    from ._ignore import Ignore
    from .db import HashFileDB
    from .state import StateBase
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
    upload_odb: "ObjectDB",
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


def _hash_files(
    small_files: list[tuple[str, dict]],
    large_files: list[tuple[str, dict]],
    fs: "FileSystem",
    name: str,
    jobs: Optional[int] = None,
) -> Iterator[tuple[str, tuple[Meta, HashInfo, dict]]]:
    def _hash(arg: tuple[str, dict]) -> tuple[str, tuple[Meta, HashInfo, dict]]:
        p, info = arg
        # `hash_file` only hashes files that are not in `state`, so we need to pass
        # `state=None` here. We'll save the hashes outside this function.
        meta, hi = hash_file(p, fs, name, state=None, info=info)
        return p, (meta, hi, info)

    if len(large_files) < 2:
        small_files.extend(large_files)
        large_files.clear()

    yield from map(_hash, small_files)
    if large_files:
        max_workers = jobs if jobs else min(16, (os.cpu_count() or 1) + 4)
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            yield from executor.imap_unordered(_hash, large_files)


def _get_hashes(
    paths: list["AnyFSPath"],
    fs: "FileSystem",
    name: str,
    infos: dict[str, dict],
    state: Optional["StateBase"] = None,
    callback: Optional["Callback"] = None,
    jobs: Optional[int] = None,
    large_file_threshold: int = 2**21,
) -> dict[str, tuple[Meta, HashInfo, dict]]:
    hashes: dict[str, tuple[Meta, HashInfo, dict]] = {}
    state = state if state is not None else StateNoop()

    large_files: list[tuple[str, dict]] = []
    small_files: list[tuple[str, dict]] = []
    lappend = large_files.append
    sappend = small_files.append

    for path, meta, hi in state.get_many(paths, fs, infos):
        info = infos[path]
        if meta is not None and hi is not None and hi.name == name:
            hashes[path] = meta, hi, info
        elif (size := info.get("size")) and size > large_file_threshold:
            lappend((path, info))
        else:
            sappend((path, info))

    if callback:
        callback.relative_update(len(hashes))

    hashes_it = _hash_files(small_files, large_files, fs, name, jobs=jobs)
    if callback:
        hashes_it = callback.wrap(hashes_it)
    new_hashes: dict[str, tuple[Meta, HashInfo, dict]] = dict(hashes_it)
    items = ((path, hi, info) for path, (_, hi, info) in new_hashes.items())
    state.save_many(items, fs)

    hashes.update(new_hashes)
    return hashes


def _build_files(
    root: Optional["AnyFSPath"],
    file_infos: dict[str, dict],
    fs: "FileSystem",
    name: str,
    odb: Optional["HashFileDB"] = None,
    callback: "Callback" = DEFAULT_CALLBACK,
    upload_odb: Optional["ObjectDB"] = None,
    dry_run: bool = False,
    jobs: Optional[int] = None,
    large_file_threshold: int = 2**20,
) -> dict[str, tuple[Meta, HashInfo]]:
    sep = fs.sep
    fnames = list(file_infos)
    infos = file_infos
    if root:
        infos = {f"{root}{sep}{fname}": info for fname, info in infos.items()}

    paths = list(infos)
    state = odb.state if odb is not None else None
    hashes = _get_hashes(
        paths,
        fs,
        name,
        infos,
        state=state,
        callback=callback,
        jobs=jobs,
        large_file_threshold=large_file_threshold,
    )

    objects: dict[str, tuple[Meta, HashInfo]] = {}
    if upload_odb is not None and not dry_run:
        assert odb is not None
        assert name == "md5"
        for fname, path in zip(fnames, paths):
            meta, obj = _upload_file(path, fs, odb, upload_odb)
            objects[fname] = meta, obj.hash_info
        return objects

    if not dry_run:
        assert odb is not None
        to_add = {path: hashes[path][1].value for path in paths}
        oids = list(to_add.values())
        paths = list(to_add)
        odb.add(paths, fs, oids, hardlink=False)  # type: ignore[arg-type]
    return {fname: hashes[path][:2] for fname, path in zip(fnames, paths)}


def _build_file(
    path: "AnyFSPath",
    fs: "FileSystem",
    name: str,
    odb: Optional["HashFileDB"] = None,
    upload_odb: Optional["ObjectDB"] = None,
    dry_run: bool = False,
) -> tuple[Meta, HashFile]:
    objects = _build_files(
        None,
        {path: fs.info(path)},
        fs,
        name,
        odb=odb,
        upload_odb=upload_odb,
        dry_run=dry_run,
    )
    ((fname, (meta, hi)),) = objects.items()
    assert path == fname
    assert hi.value is not None
    if dry_run:
        obj = HashFile(path, fs, hi)
    else:
        assert odb is not None
        obj = odb.get(hi.value)
    return meta, obj


def _walk_files(
    fs: "FileSystem", path: str, ignore: Optional["Ignore"] = None
) -> Iterator[tuple[str, dict[str, dict]]]:
    if not isinstance(fs, LocalFileSystem):
        # reduce no. of info calls for non-local fs
        for root, _, files in fs.walk(path, detail=True):
            assert isinstance(root, str)
            assert isinstance(files, dict)
            yield root, files
        return

    sep = fs.sep
    walk_iter = ignore.walk(fs, path) if ignore else fs.walk(path)
    for root, _, files in walk_iter:
        assert isinstance(root, str)
        yield root, {file: _localfs_info(f"{root}{sep}{file}") for file in files}


def _build_tree(
    path: "AnyFSPath",
    fs: "FileSystem",
    fs_info: dict,
    name: str,
    odb: Optional["HashFileDB"] = None,
    upload_odb: Optional["ObjectDB"] = None,
    ignore: Optional["Ignore"] = None,
    callback: "Callback" = DEFAULT_CALLBACK,
    dry_run: bool = False,
    checksum_jobs: Optional[int] = None,
    **kwargs: Any,
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

    tree = Tree()
    size = 0
    for root, files in _walk_files(fs, path, ignore=ignore):
        # NOTE: might happen with s3/gs/azure/etc, where empty
        # objects like `dir/` might be used to create an empty dir
        files.pop("", None)
        if not files:
            continue
        if DefaultIgnoreFile in files:
            raise IgnoreInCollectedDirError(DefaultIgnoreFile, root)

        # NOTE: we know for sure that root starts with path, so we can use
        # faster string manipulation instead of a more robust relparts()
        rel_key: tuple[str, ...] = ()
        if root != path:
            rel_key = tuple(root[len(path) + 1 :].split(fs.sep))

        callback.set_size((callback.size or 0) + len(files))
        objects = _build_files(
            root,
            files,
            fs,
            name,
            odb=odb,
            callback=callback,
            upload_odb=upload_odb,
            dry_run=dry_run,
            jobs=checksum_jobs,
        )
        for fname, (meta, hi) in objects.items():
            key = (*rel_key, fname)
            tree.add(key, meta, hi)
            size += meta.size or 0

    tree_meta = Meta(size=size, nfiles=len(tree), isdir=True)
    if not tree_meta.nfiles:
        # This will raise FileNotFoundError if it is a
        # broken symlink or TreeError
        next(iter(fs.ls(path, detail=False)), None)

    tree.digest()
    if odb:
        tree = add_update_tree(odb, tree)
    return tree_meta, tree


_url_cache: dict[str, str] = {}


def _make_staging_url(fs: "FileSystem", odb: "HashFileDB", path: Optional["AnyFSPath"]):
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
    ignore: Optional["Ignore"] = None,
    callback: "Callback" = DEFAULT_CALLBACK,
    checksum_jobs: Optional[int] = None,
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
            ignore=ignore,
            callback=callback,
            dry_run=dry_run,
            checksum_jobs=checksum_jobs,
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
