from copy import deepcopy

import pytest
from attrs import evolve
from dvc_objects.fs.generic import transfer
from dvc_objects.fs.local import LocalFileSystem

from dvc_data.hashfile.build import build
from dvc_data.hashfile.checkout import _determine_files_to_relink
from dvc_data.hashfile.db import HashFileDB
from dvc_data.hashfile.diff import Change, DiffResult, TreeEntry


@pytest.mark.parametrize("cache_type", ["copy", "hardlink", "symlink", "reflink"])
@pytest.mark.parametrize("link", ["copy", "hardlink", "symlink", "reflink"])
def test_determine_relinking(tmp_path, cache_type, link):
    fs = LocalFileSystem()
    cache = HashFileDB(fs, str(tmp_path), type=[cache_type])

    foo_oid = "acbd18db4cc2f85cedef654fccc4a4d8"
    cache.add_bytes(foo_oid, b"foo")
    bar_oid = "37b51d194a7513e45b56f6524f2d51f2"
    cache.add_bytes(bar_oid, b"bar")

    try:
        transfer(
            fs,
            [cache.oid_to_path(foo_oid), cache.oid_to_path(bar_oid)],
            fs,
            [str(tmp_path / "foo"), str(tmp_path / "bar")],
            links=[link],
        )
    except OSError:
        pytest.skip(f"Link {link} not supported")

    _, foo_meta, foo_obj = build(cache, str(tmp_path / "foo"), fs, "md5")
    _, bar_meta, bar_obj = build(cache, str(tmp_path / "bar"), fs, "md5")
    foo_entry = TreeEntry(cache.check(foo_oid), ("foo",), foo_meta, foo_obj.hash_info)
    bar_entry = TreeEntry(cache.check(bar_oid), ("bar",), bar_meta, bar_obj.hash_info)
    diff = DiffResult(
        unchanged=[Change(foo_entry, foo_entry), Change(bar_entry, bar_entry)]
    )

    # _determine_files_to_relink modifies existing diff
    old_diff = deepcopy(diff)
    _determine_files_to_relink(diff, "dataset", fs, cache)

    if cache_type == link or {cache_type, link} <= {"copy", "reflink"}:
        assert diff == old_diff
    else:
        assert diff == evolve(old_diff, modified=old_diff.unchanged)
