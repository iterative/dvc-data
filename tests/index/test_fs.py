import pytest

from dvc_data.fs import DataFileSystem
from dvc_data.hashfile.hash_info import HashInfo
from dvc_data.hashfile.meta import Meta
from dvc_data.index import (
    DataIndex,
    DataIndexDirError,
    DataIndexEntry,
    FileStorage,
    ObjectStorage,
)


def test_fs(tmp_upath, odb, as_filesystem):
    index = DataIndex(
        {
            ("foo",): DataIndexEntry(
                key=("foo",),
                hash_info=HashInfo(
                    name="md5", value="d3b07384d113edec49eaa6238ad5ff00"
                ),
            ),
            ("data",): DataIndexEntry(
                key=("data",),
                meta=Meta(isdir=True),
                hash_info=HashInfo(
                    name="md5",
                    value="1f69c66028c35037e8bf67e5bc4ceb6a.dir",
                ),
            ),
        }
    )
    index.storage_map.add_cache(ObjectStorage((), odb))
    fs = DataFileSystem(index)
    assert fs.exists("foo")
    assert fs.cat("foo") == b"foo\n"
    with pytest.raises(NotADirectoryError):
        fs.ls("foo")
    assert fs.ls("/", detail=False) == ["/foo", "/data"]
    assert fs.ls("/", detail=True) == [fs.info("/foo"), fs.info("/data")]
    assert fs.cat("/data/bar") == b"bar\n"
    assert fs.cat("/data/baz") == b"baz\n"
    with pytest.raises(NotADirectoryError):
        fs.ls("/data/bar")
    assert fs.ls("/data", detail=False) == ["/data/bar", "/data/baz"]
    assert fs.ls("/data", detail=True) == [
        fs.info("/data/bar"),
        fs.info("/data/baz"),
    ]


def test_fs_file_storage(tmp_upath, as_filesystem):
    (tmp_upath / "foo").write_bytes(b"foo\n")
    (tmp_upath / "data").mkdir()
    (tmp_upath / "data" / "bar").write_bytes(b"bar\n")
    (tmp_upath / "data" / "baz").write_bytes(b"baz\n")

    index = DataIndex(
        {
            ("foo",): DataIndexEntry(
                key=("foo",),
            ),
            ("data",): DataIndexEntry(
                key=("data",),
            ),
        }
    )
    index.storage_map.add_cache(
        FileStorage((), as_filesystem(tmp_upath.fs), str(tmp_upath))
    )
    fs = DataFileSystem(index)
    assert fs.exists("foo")
    assert fs.cat("foo") == b"foo\n"
    assert sorted(fs.ls("/", detail=False)) == sorted(["/foo", "/data"])
    assert sorted(fs.ls("/", detail=True), key=lambda entry: entry["name"]) == sorted(
        [fs.info("/foo"), fs.info("/data")],
        key=lambda entry: entry["name"],
    )
    assert fs.cat("/data/bar") == b"bar\n"
    assert fs.cat("/data/baz") == b"baz\n"
    assert sorted(fs.ls("/data", detail=False)) == sorted(["/data/bar", "/data/baz"])
    assert sorted(
        fs.ls("/data", detail=True), key=lambda entry: entry["name"]
    ) == sorted(
        [
            fs.info("/data/bar"),
            fs.info("/data/baz"),
        ],
        key=lambda entry: entry["name"],
    )


def test_fs_broken(tmp_upath, odb, as_filesystem):
    index = DataIndex(
        {
            ("foo",): DataIndexEntry(
                key=("foo",),
                hash_info=HashInfo(
                    name="md5", value="d3b07384d113edec49eaa6238ad5ff00"
                ),
            ),
            ("data",): DataIndexEntry(
                key=("data",),
                meta=Meta(isdir=True),
                hash_info=HashInfo(
                    name="md5",
                    value="1f69c66028c35037e8bf67e5bc4ceb6a.dir",
                ),
            ),
            ("broken",): DataIndexEntry(
                key=("broken",),
                meta=Meta(isdir=True),
                hash_info=HashInfo(
                    name="md5",
                    value="bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb.dir",
                ),
            ),
        }
    )
    index.storage_map.add_cache(ObjectStorage((), odb))
    fs = DataFileSystem(index)
    assert fs.exists("foo")
    assert fs.cat("foo") == b"foo\n"
    with pytest.raises(NotADirectoryError):
        fs.ls("foo")

    assert fs.ls("/", detail=False) == ["/foo", "/data", "/broken"]
    assert fs.ls("/", detail=True) == [
        fs.info("/foo"),
        fs.info("/data"),
        fs.info("/broken"),
    ]

    assert fs.cat("/data/bar") == b"bar\n"
    assert fs.cat("/data/baz") == b"baz\n"
    with pytest.raises(NotADirectoryError):
        fs.ls("/data/bar")
    assert fs.ls("/data", detail=False) == ["/data/bar", "/data/baz"]
    assert fs.ls("/data", detail=True) == [
        fs.info("/data/bar"),
        fs.info("/data/baz"),
    ]

    assert fs.exists("/broken")
    assert fs.isdir("/broken")
    with pytest.raises(DataIndexDirError):
        fs.ls("/broken", detail=False)

    with pytest.raises(DataIndexDirError):
        fs.ls("/broken", detail=True)

    def onerror(_entry, _exc):
        pass

    fs.index.onerror = onerror
    assert fs.ls("/broken", detail=False) == []
    assert fs.ls("/broken", detail=True) == []
