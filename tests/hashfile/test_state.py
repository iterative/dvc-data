from os import fspath
from unittest.mock import ANY

from dvc_objects.fs.implementations.local import localfs

from dvc_data.hashfile.hash import hash_file
from dvc_data.hashfile.state import State


def test_bulk_save(tmp_path):
    expected = {}
    for idx in range(10):
        fs_path = fspath(tmp_path / f"path{idx}")
        localfs.pipe(fs_path, b"contents" + bytes(idx))
        meta, hash_info = hash_file(fs_path, localfs, "md5")
        meta.isexec = ANY  # we don't care about isexec
        expected[fs_path] = meta, hash_info

    state = State(tmp_path, tmp_path / "state")

    with state.bulk_save(localfs) as save:
        for path, (_, hash_info) in expected.items():
            save(path, hash_info)

    actual = {key: state.get(key, localfs) for key in state.hashes}
    assert expected == actual
