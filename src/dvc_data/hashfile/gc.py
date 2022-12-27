def gc(odb, used, jobs=None, cache_odb=None, shallow=True):
    from dvc_objects.errors import ObjectDBPermissionError

    from ._progress import QueryingProgress
    from .tree import Tree

    if odb.read_only:
        raise ObjectDBPermissionError("Cannot gc read-only ODB")
    if not cache_odb:
        cache_odb = odb
    used_hashes = set()
    for hash_info in used:
        used_hashes.add(hash_info.value)
        if hash_info.isdir and not shallow:
            tree = Tree.load(cache_odb, hash_info)
            used_hashes.update(
                entry_obj.hash_info.value for _, entry_obj in tree
            )

    def _is_dir_hash(_hash):
        from .hash_info import HASH_DIR_SUFFIX

        return _hash.endswith(HASH_DIR_SUFFIX)

    removed = False

    dir_paths = []
    file_paths = []
    for hash_ in QueryingProgress(odb.all(jobs), name=odb.path):
        if hash_ in used_hashes:
            continue
        path = odb.oid_to_path(hash_)
        if _is_dir_hash(hash_):
            # backward compatibility
            # pylint: disable=protected-access
            odb._remove_unpacked_dir(hash_)
            dir_paths.append(path)
        else:
            file_paths.append(path)

    for paths in (dir_paths, file_paths):
        if paths:
            removed = True
            odb.fs.remove(paths)

    return removed
