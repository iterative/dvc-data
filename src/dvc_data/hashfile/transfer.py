import errno
import logging
from functools import partial, wraps
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Iterable,
    NamedTuple,
    Optional,
    Set,
)

from dvc_objects._tqdm import Tqdm
from dvc_objects.executors import ThreadPoolExecutor
from funcy import split

if TYPE_CHECKING:
    from dvc_objects.db import ObjectDB

    from .db.index import ObjectDBIndexBase
    from .hash_info import HashInfo
    from .status import CompareStatusResult
    from .tree import Tree

logger = logging.getLogger(__name__)


class TransferResult(NamedTuple):
    transferred: Set["HashInfo"]
    failed: Set["HashInfo"]


def _log_exceptions(
    func: Callable[["HashInfo"], None]
) -> Callable[["HashInfo"], Optional["HashInfo"]]:
    @wraps(func)
    def wrapper(oid: "HashInfo") -> Optional["HashInfo"]:
        try:
            func(oid)
            return None
        except Exception as exc:  # pylint: disable=broad-except
            # NOTE: this means we ran out of file descriptors and there is no
            # reason to try to proceed, as we will hit this error anyways.
            # pylint: disable=no-member
            if isinstance(exc, OSError) and exc.errno == errno.EMFILE:
                raise
            logger.exception("failed to transfer '%s'", oid)
            return oid

    return wrapper


def find_tree_by_obj_id(
    odbs: Iterable[Optional["ObjectDB"]], obj_id: "HashInfo"
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
    src: "ObjectDB",
    dest: "ObjectDB",
    obj_ids: Iterable["HashInfo"],
    missing_ids: Iterable["HashInfo"],
    processor: Callable[
        [Iterable["HashInfo"]], Iterable[Optional["HashInfo"]]
    ],
    src_index: Optional["ObjectDBIndexBase"] = None,
    dest_index: Optional["ObjectDBIndexBase"] = None,
    cache_odb: Optional["ObjectDB"] = None,
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

        dir_fails = [
            hash_info
            for hash_info in processor(bound_file_ids)
            if hash_info is not None
        ]
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
            is_dir_failed = any(
                hash_info
                for hash_info in processor([dir_obj.hash_info])
                if hash_info is not None
            )
            if is_dir_failed:
                failed_ids.add(dir_obj.hash_info)
            else:
                succeeded_dir_objs.append(dir_obj)

    # insert the rest
    failed_ids.update(
        hash_info
        for hash_info in processor(all_file_ids)
        if hash_info is not None
    )
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


def transfer(
    src: "ObjectDB",
    dest: "ObjectDB",
    obj_ids: Iterable["HashInfo"],
    jobs: Optional[int] = None,
    verify: bool = False,
    hardlink: bool = False,
    validate_status: Callable[["CompareStatusResult"], None] = None,
    **kwargs,
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
        src, dest, obj_ids, check_deleted=False, jobs=jobs, **kwargs
    )

    if validate_status:
        validate_status(status)

    if not status.new:
        return TransferResult(set(), set())

    def func(hash_info: "HashInfo") -> None:
        obj = src.get(hash_info.value)
        return dest.add(
            obj.path,
            obj.fs,
            obj.oid,
            verify=verify,
            hardlink=hardlink,
        )

    total = len(status.new)
    jobs = jobs or dest.fs.jobs
    with Tqdm(total=total, unit="file", desc="Transferring") as pbar:
        with ThreadPoolExecutor(max_workers=jobs) as executor:
            wrapped_func = pbar.wrap_fn(_log_exceptions(func))
            processor = partial(executor.imap_unordered, wrapped_func)
            failed = _do_transfer(
                src, dest, status.new, status.missing, processor, **kwargs
            )
    return TransferResult(status.new - failed, failed)
