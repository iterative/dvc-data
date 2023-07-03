import errno
import logging
from collections import defaultdict
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    Iterable,
    List,
    NamedTuple,
    Optional,
    Set,
    Tuple,
)

from dvc_objects.fs.callbacks import DEFAULT_CALLBACK
from funcy import split

from .hash_info import HashInfo

if TYPE_CHECKING:
    from dvc_objects.fs.base import FileSystem
    from dvc_objects.fs.callbacks import Callback

    from .db import HashFileDB
    from .db.index import ObjectDBIndexBase
    from .status import CompareStatusResult
    from .tree import Tree

logger = logging.getLogger(__name__)


class TransferResult(NamedTuple):
    transferred: Set["HashInfo"]
    failed: Set["HashInfo"]


def _log_exception(oid: str, exc: BaseException):
    # NOTE: this means we ran out of file descriptors and there is no
    # reason to try to proceed, as we will hit this error anyways.
    # pylint: disable=no-member
    if isinstance(exc, OSError) and exc.errno == errno.EMFILE:
        raise exc
    logger.error("failed to transfer '%s'", oid, exc_info=exc)


def find_tree_by_obj_id(
    odbs: Iterable[Optional["HashFileDB"]], obj_id: "HashInfo"
) -> Optional["Tree"]:
    from dvc_objects.errors import ObjectFormatError

    from .tree import Tree

    for odb in odbs:
        if odb is not None:
            try:
                return Tree.load(odb, obj_id)
            except (FileNotFoundError, ObjectFormatError):
                pass
    return None


def _do_transfer(
    src: "HashFileDB",
    dest: "HashFileDB",
    obj_ids: Iterable["HashInfo"],
    missing_ids: Iterable["HashInfo"],
    src_index: Optional["ObjectDBIndexBase"] = None,
    dest_index: Optional["ObjectDBIndexBase"] = None,
    cache_odb: Optional["HashFileDB"] = None,
    **kwargs: Any,
) -> Set["HashInfo"]:
    """Do object transfer.

    Returns:
        Set containing any hash_infos which failed to transfer.
    """
    dir_ids, file_ids = split(lambda hash_info: hash_info.isdir, obj_ids)
    failed_ids: Set["HashInfo"] = set()
    succeeded_dir_objs = []
    all_file_ids = set(file_ids)

    for dir_hash in dir_ids:
        dir_obj = find_tree_by_obj_id([cache_odb, src], dir_hash)
        assert dir_obj

        entry_ids = {oid for _, _, oid in dir_obj}
        bound_file_ids = all_file_ids & entry_ids
        all_file_ids -= entry_ids

        logger.debug(
            "transfer dir: %s with %d files", dir_hash, len(bound_file_ids)
        )

        dir_fails = _add(src, dest, bound_file_ids, **kwargs)
        if dir_fails:
            logger.debug(
                "failed to upload full contents of '%s', "
                "aborting .dir file upload",
                dir_hash,
            )
            logger.debug(
                "failed to upload '%s' to '%s'",
                src.get(dir_obj.oid).path,
                dest.get(dir_obj.oid).path,
            )
            failed_ids.update(dir_fails)
            failed_ids.add(dir_obj.hash_info)
        elif entry_ids.intersection(missing_ids):
            # if for some reason a file contained in this dir is
            # missing both locally and in the remote, we want to
            # push whatever file content we have, but should not
            # push .dir file
            logger.debug(
                "directory '%s' contains missing files,"
                "skipping .dir file upload",
                dir_hash,
            )
        else:
            if _add(src, dest, [dir_obj.hash_info], **kwargs):
                failed_ids.add(dir_obj.hash_info)
            else:
                succeeded_dir_objs.append(dir_obj)

    # insert the rest
    failed_ids.update(_add(src, dest, all_file_ids, **kwargs))
    if failed_ids:
        if src_index:
            src_index.clear()
        return failed_ids

    # index successfully pushed dirs
    if dest_index:
        for dir_obj in succeeded_dir_objs:
            file_hashes = {oid.value for _, _, oid in dir_obj}
            logger.debug(
                "Indexing pushed dir '%s' with '%s' nested files",
                dir_obj.hash_info,
                len(file_hashes),
            )
            assert dir_obj.hash_info and dir_obj.hash_info.value
            dest_index.update([dir_obj.hash_info.value], file_hashes)

    return set()


def _add(
    src: "HashFileDB",
    dest: "HashFileDB",
    hash_infos: Iterable["HashInfo"],
    **kwargs,
) -> Set["HashInfo"]:

    failed: Set["HashInfo"] = set()
    if not hash_infos:
        return failed

    def _error(oid: str, exc: BaseException):
        _log_exception(oid, exc)
        failed.add(HashInfo(src.hash_name, oid))

    fs_map: Dict["FileSystem", List[Tuple[str, str]]] = defaultdict(list)
    for hash_info in hash_infos:
        assert hash_info.value
        obj = src.get(hash_info.value)
        fs_map[obj.fs].append((obj.path, obj.oid))

    for fs, args in fs_map.items():
        paths, oids = zip(*args)
        dest.add(
            list(paths),
            fs,
            list(oids),
            on_error=_error,
            **kwargs,
        )
    return failed


def transfer(
    src: "HashFileDB",
    dest: "HashFileDB",
    obj_ids: Iterable["HashInfo"],
    jobs: Optional[int] = None,
    verify: bool = False,
    hardlink: bool = False,
    validate_status: Callable[["CompareStatusResult"], None] = None,
    src_index: Optional["ObjectDBIndexBase"] = None,
    dest_index: Optional["ObjectDBIndexBase"] = None,
    cache_odb: Optional["HashFileDB"] = None,
    shallow: bool = True,
    callback: "Callback" = DEFAULT_CALLBACK,
) -> "TransferResult":
    """Transfer (copy) the specified objects from one ODB to another.

    Returns the number of successfully transferred objects
    """
    from .status import compare_status

    logger.debug(
        "Preparing to transfer data from '%s' to '%s'",
        src.path,
        dest.path,
    )
    if src == dest:
        return TransferResult(set(), set())

    status = compare_status(
        src,
        dest,
        obj_ids,
        check_deleted=False,
        jobs=jobs,
        src_index=src_index,
        dest_index=dest_index,
        cache_odb=cache_odb,
        shallow=shallow,
    )

    if validate_status:
        validate_status(status)

    if not status.new:
        return TransferResult(set(), set())

    if callback != DEFAULT_CALLBACK:
        callback = callback.as_tqdm_callback(unit="file", desc="Transferring")

    callback.set_size(len(status.new))
    jobs = jobs or dest.fs.jobs

    failed = _do_transfer(
        src,
        dest,
        status.new,
        status.missing,
        verify=verify,
        hardlink=hardlink,
        callback=callback,
        batch_size=jobs,
        check_exists=False,
        src_index=src_index,
        dest_index=dest_index,
        cache_odb=cache_odb,
    )
    return TransferResult(status.new - failed, failed)
