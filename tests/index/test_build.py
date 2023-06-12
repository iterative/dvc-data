import pytest

from dvc_data.index.build import DataIndexEntry, build, build_entry


def test_build_entry(tmp_upath, as_filesystem):
    (tmp_upath / "foo").write_bytes(b"foo\n")

    fs = as_filesystem(tmp_upath.fs)

    entry = build_entry(str(tmp_upath / "foo"), fs)
    assert isinstance(entry, DataIndexEntry)
    assert entry.meta.size == 4
    assert entry.key is None
    assert entry.hash_info is None

    with pytest.raises(FileNotFoundError):
        build_entry(str(tmp_upath / "missing"), fs)


def test_build(tmp_upath, as_filesystem):
    (tmp_upath / "foo").write_bytes(b"foo\n")
    (tmp_upath / "data").mkdir()
    (tmp_upath / "data" / "bar").write_bytes(b"bar\n")
    (tmp_upath / "data" / "baz").write_bytes(b"baz\n")

    fs = as_filesystem(tmp_upath.fs)
    index = build(str(tmp_upath), fs)
    assert index[("foo",)].meta.size == 4
    assert index.storage_map.get_data(index[("foo",)]) == (
        fs,
        str(tmp_upath / "foo"),
    )
    assert index[("data",)].meta.isdir
    assert index[("data", "bar")].meta.size == 4
    assert index.storage_map.get_data(index[("data", "bar")]) == (
        fs,
        str(tmp_upath / "data" / "bar"),
    )
    assert index[("data", "baz")].meta.size == 4
    assert index.storage_map.get_data(index[("data", "baz")]) == (
        fs,
        str(tmp_upath / "data" / "baz"),
    )
