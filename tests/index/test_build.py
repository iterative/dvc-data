import pytest
from dvc_objects.fs.local import LocalFileSystem

from dvc_data.index.build import DataIndexEntry, build, build_entry


def test_build_entry(tmp_path):
    (tmp_path / "foo").write_bytes(b"foo\n")

    fs = LocalFileSystem()
    entry = build_entry(str(tmp_path / "foo"), fs)
    assert isinstance(entry, DataIndexEntry)

    assert entry.meta
    assert entry.meta.size == 4
    assert entry.key is None
    assert entry.hash_info is None

    with pytest.raises(FileNotFoundError):
        build_entry(str(tmp_path / "missing"), fs)


def test_build(tmp_path):
    (tmp_path / "foo").write_bytes(b"foo\n")
    (tmp_path / "data").mkdir()
    (tmp_path / "data" / "bar").write_bytes(b"bar\n")
    (tmp_path / "data" / "baz").write_bytes(b"baz\n")

    fs = LocalFileSystem()
    index = build(str(tmp_path), fs)
    assert index[("foo",)].meta.size == 4
    assert index.storage_map.get_data(index[("foo",)]) == (
        fs,
        str(tmp_path / "foo"),
    )
    assert index[("data",)].meta.isdir
    assert index[("data", "bar")].meta.size == 4
    assert index.storage_map.get_data(index[("data", "bar")]) == (
        fs,
        str(tmp_path / "data" / "bar"),
    )
    assert index[("data", "baz")].meta.size == 4
    assert index.storage_map.get_data(index[("data", "baz")]) == (
        fs,
        str(tmp_path / "data" / "baz"),
    )
