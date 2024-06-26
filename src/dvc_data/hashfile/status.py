import logging
from collections.abc import Iterable
from typing import TYPE_CHECKING, NamedTuple, Optional

from dvc_objects.fs import Schemes

from .hash_info import HashInfo
from .tree import Tree

if TYPE_CHECKING:
    from dvc_objects.db import ObjectDB

    from .db import HashFileDB
    from .db.index import ObjectDBIndexBase
    from .obj import HashFile

logger = logging.getLogger(__name__)


class StatusResult(NamedTuple):
    exists: set["HashInfo"]
    missing: set["HashInfo"]


class CompareStatusResult(NamedTuple):
    ok: set["HashInfo"]
    missing: set["HashInfo"]
    new: set["HashInfo"]
    deleted: set["HashInfo"]


def _indexed_dir_hashes(
    odb: "ObjectDB", index: "ObjectDBIndexBase", dir_objs, name, cache_odb, jobs=None
):
    # Validate our index by verifying all indexed .dir hashes
    # still exist on the remote
    from ._progress import QueryingProgress

    dir_hashes = set(dir_objs.keys())
    indexed_dirs = set(index.dir_hashes())
    indexed_dir_exists: set[str] = set()
    if indexed_dirs:
        hashes = QueryingProgress(
            odb.list_oids_exists(indexed_dirs, jobs=jobs),
            total=len(indexed_dirs),
        )
        indexed_dir_exists.update(hashes)
        missing_dirs = indexed_dirs.difference(indexed_dir_exists)
        if missing_dirs:
            logger.debug(
                "Remote cache missing indexed .dir hashes '%s', clearing remote index",
                ", ".join(missing_dirs),
            )
            index.clear()

    # Check if non-indexed (new) dir hashes exist on remote
    dir_exists = dir_hashes.intersection(indexed_dir_exists)
    dir_missing = dir_hashes - dir_exists
    dir_exists.update(
        QueryingProgress(
            odb.list_oids_exists(dir_missing, jobs=jobs),
            total=len(dir_missing),
        )
    )

    # If .dir hash exists in the ODB, assume directory contents
    # also exists
    for dir_hash in dir_exists:
        tree = dir_objs.get(dir_hash)
        if not tree:
            try:
                tree = Tree.load(cache_odb, HashInfo(name, dir_hash))
            except FileNotFoundError:
                continue
        file_hashes = [hi.value for _, _, hi in tree]
        if dir_hash not in index:
            logger.debug(
                "Indexing new .dir '%s' with '%s' nested files",
                dir_hash,
                len(file_hashes),
            )
            index.update([dir_hash], file_hashes)
        yield from file_hashes
        yield tree.hash_info.value


def status(  # noqa: C901, PLR0912
    odb: "HashFileDB",
    obj_ids: Iterable["HashInfo"],
    name: Optional[str] = None,
    index: Optional["ObjectDBIndexBase"] = None,
    cache_odb: Optional["HashFileDB"] = None,
    shallow: bool = True,
    jobs: Optional[int] = None,
) -> "StatusResult":
    """Return status of whether or not the specified objects exist odb.

    If cache_odb is set, trees will be loaded from cache_odb instead of odb
    when needed.

    Status is returned as a tuple of:
        exists: objs that exist in odb
        missing: objs that do not exist in ODB
    """
    logger.debug("Preparing to collect status from '%s'", odb.path)
    if not name:
        name = odb.hash_name

    if cache_odb is None:
        cache_odb = odb

    hash_infos: dict[str, HashInfo] = {}
    dir_objs: dict[str, Optional[HashFile]] = {}
    for hash_info in obj_ids:
        assert hash_info.value
        if hash_info.isdir:
            if shallow:
                tree = None
            else:
                tree = Tree.load(cache_odb, hash_info)
                for _, _, oid in tree:
                    assert oid
                    assert oid.value
                    hash_infos[oid.value] = oid
            if index:
                dir_objs[hash_info.value] = tree
        hash_infos[hash_info.value] = hash_info

    if odb.fs.protocol == Schemes.MEMORY:
        # assume memfs staged objects already exist
        return StatusResult(set(hash_infos.values()), set())

    hashes: set[str] = set(hash_infos.keys())
    exists: set[str] = set()

    logger.debug("Collecting status from '%s'", odb.path)
    if index and hashes:
        if dir_objs:
            exists = hashes.intersection(
                _indexed_dir_hashes(odb, index, dir_objs, name, cache_odb, jobs=jobs)
            )
            hashes.difference_update(exists)
        if hashes:
            exists.update(index.intersection(hashes))
            hashes.difference_update(exists)

    if hashes:
        from ._progress import QueryingProgress

        with QueryingProgress(phase="Checking", name=odb.path) as pbar:
            exists.update(odb.oids_exist(hashes, jobs=jobs, progress=pbar.callback))
    return StatusResult(
        {hash_infos[hash_] for hash_ in exists},
        {hash_infos[hash_] for hash_ in (hashes - exists)},
    )


def compare_status(
    src: "HashFileDB",
    dest: "HashFileDB",
    obj_ids: Iterable["HashInfo"],
    check_deleted: bool = True,
    src_index: Optional["ObjectDBIndexBase"] = None,
    dest_index: Optional["ObjectDBIndexBase"] = None,
    cache_odb: Optional["HashFileDB"] = None,
    jobs: Optional[int] = None,
    **kwargs,
) -> "CompareStatusResult":
    """Compare status for the specified objects between two ODBs.

    Status is returned as a tuple of:
        ok: hashes that exist in both src and dest
        missing: hashes that do not exist in neither src nor dest
        new: hashes that only exist in src
        deleted: hashes that only exist in dest
    """
    if cache_odb is None:
        cache_odb = src
    dest_exists, dest_missing = status(
        dest,
        obj_ids,
        index=dest_index,
        jobs=jobs,
        cache_odb=cache_odb,
        **kwargs,
    )
    # for transfer operations we can skip src status check when all objects
    # already exist in dest
    if dest_missing or check_deleted:
        src_exists, src_missing = status(
            src, obj_ids, index=src_index, jobs=jobs, **kwargs
        )
    else:
        src_exists = dest_exists
        src_missing = set()
    return CompareStatusResult(
        src_exists & dest_exists,
        src_missing & dest_missing,
        src_exists - dest_exists,
        dest_exists - src_exists,
    )
