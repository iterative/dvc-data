from collections.abc import Iterable
from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    from .db import HashFileDB
    from .hash_info import HashInfo


def gc(  # noqa: C901
    odb: "HashFileDB",
    used: Iterable["HashInfo"],
    jobs: Optional[int] = None,
    cache_odb: Optional["HashFileDB"] = None,
    shallow: bool = True,
    dry: bool = False,
):
    from dvc_objects.errors import ObjectDBPermissionError

    from ._progress import QueryingProgress
    from .tree import Tree

    if odb.read_only:
        raise ObjectDBPermissionError("Cannot gc read-only ODB")
    if not cache_odb:
        cache_odb = odb
    used_hashes = set()
    for hash_info in used:
        if hash_info.name != odb.hash_name:
            continue
        used_hashes.add(hash_info.value)
        if hash_info.isdir and not shallow:
            tree = Tree.load(cache_odb, hash_info)
            used_hashes.update(entry_obj.hash_info.value for _, entry_obj in tree)

    def _is_dir_hash(_hash):
        from .hash_info import HASH_DIR_SUFFIX

        return _hash.endswith(HASH_DIR_SUFFIX)

    num_removed = 0

    dir_paths = []
    file_paths = []
    for hash_ in QueryingProgress(odb.all(jobs), name=odb.path):
        if hash_ in used_hashes:
            continue
        path = odb.oid_to_path(hash_)
        if _is_dir_hash(hash_):
            # backward compatibility
            odb._remove_unpacked_dir(hash_)
            dir_paths.append(path)
        else:
            file_paths.append(path)

    for paths in (dir_paths, file_paths):
        if paths:
            num_removed += len(paths)
            if not dry:
                odb.fs.remove(paths)

    return num_removed
