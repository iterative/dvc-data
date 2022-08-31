import pytest

from dvc_data.fs import DataFileSystem
from dvc_data.hashfile.db import HashFileDB
from dvc_data.hashfile.hash_info import HashInfo
from dvc_data.hashfile.meta import Meta
from dvc_data.index import (
    DataIndex,
    DataIndexEntry,
    build,
    checkout,
    collect,
    commit,
    save,
)


@pytest.fixture
def odb(tmp_upath_factory, as_filesystem):
    path = tmp_upath_factory.mktemp()
    fs = as_filesystem(path.fs)
    odb = HashFileDB(fs, path)

    foo = tmp_upath_factory.mktemp() / "foo"
    foo.write_bytes(b"foo\n")

    data = tmp_upath_factory.mktemp() / "data.dir"
    data.write_bytes(
        b'[{"md5": "c157a79031e1c40f85931829bc5fc552", "relpath": "bar"}, '
        b'{"md5": "258622b1688250cb619f3c9ccaefb7eb", "relpath": "baz"}]'
    )

    bar = tmp_upath_factory.mktemp() / "bar"
    bar.write_bytes(b"bar\n")

    baz = tmp_upath_factory.mktemp() / "baz"
    baz.write_bytes(b"baz\n")

    odb.add(str(foo), fs, "d3b07384d113edec49eaa6238ad5ff00")
    odb.add(str(data), fs, "1f69c66028c35037e8bf67e5bc4ceb6a.dir")
    odb.add(str(bar), fs, "c157a79031e1c40f85931829bc5fc552")
    odb.add(str(baz), fs, "258622b1688250cb619f3c9ccaefb7eb")

    return odb


def test_index():
    index = DataIndex()
    index[("foo",)] = DataIndexEntry()


def test_fs(tmp_upath, odb, as_filesystem):
    index = DataIndex(
        {
            ("foo",): DataIndexEntry(
                odb=odb,
                hash_info=HashInfo(
                    name="md5", value="d3b07384d113edec49eaa6238ad5ff00"
                ),
            ),
            ("data",): DataIndexEntry(
                odb=odb,
                hash_info=HashInfo(
                    name="md5",
                    value="1f69c66028c35037e8bf67e5bc4ceb6a.dir",
                ),
            ),
        }
    )
    fs = DataFileSystem(index)
    assert fs.exists("foo")
    assert fs.cat("foo") == b"foo\n"
    assert fs.ls("/", detail=False) == ["/foo", "/data"]
    assert fs.ls("/", detail=True) == [fs.info("/foo"), fs.info("/data")]
    assert fs.cat("/data/bar") == b"bar\n"
    assert fs.cat("/data/baz") == b"baz\n"
    assert fs.ls("/data", detail=False) == ["/data/bar", "/data/baz"]
    assert fs.ls("/data", detail=True) == [
        fs.info("/data/bar"),
        fs.info("/data/baz"),
    ]


def test_collect(tmp_upath, odb, as_filesystem):
    (tmp_upath / "foo").write_bytes(b"foo\n")
    (tmp_upath / "data").mkdir()
    (tmp_upath / "data" / "bar").write_bytes(b"bar\n")
    (tmp_upath / "data" / "baz").write_bytes(b"baz\n")

    index = DataIndex(
        {
            ("foo",): DataIndexEntry(odb=odb, cache=odb),
            ("data",): DataIndexEntry(odb=odb, cache=odb),
        },
    )
    fs = as_filesystem(tmp_upath.fs)
    collect(index, tmp_upath, fs)
    assert index[("foo",)].meta == Meta(size=4)
    assert index[("foo",)].fs == fs
    assert index[("foo",)].path == str(tmp_upath / "foo")
    assert index[("data",)].meta.isdir
    assert index[("data",)].fs == fs
    assert index[("data",)].path == str(tmp_upath / "data")
    assert index[("data", "bar")].meta == Meta(size=4)
    assert index[("data", "bar")].fs == fs
    assert index[("data", "bar")].path == str(tmp_upath / "data" / "bar")
    assert index[("data", "baz")].meta == Meta(size=4)
    assert index[("data", "baz")].fs == fs
    assert index[("data", "baz")].path == str(tmp_upath / "data" / "baz")


def test_save(tmp_upath, odb, as_filesystem):
    (tmp_upath / "foo").write_bytes(b"foo\n")
    (tmp_upath / "data").mkdir()
    (tmp_upath / "data" / "bar").write_bytes(b"bar\n")
    (tmp_upath / "data" / "baz").write_bytes(b"baz\n")

    index = DataIndex(
        {
            ("foo",): DataIndexEntry(odb=odb, cache=odb),
            ("data",): DataIndexEntry(odb=odb, cache=odb),
        },
    )
    fs = as_filesystem(tmp_upath.fs)
    collect(index, tmp_upath, fs)
    save(index)
    assert odb.exists("d3b07384d113edec49eaa6238ad5ff00")
    assert odb.exists("1f69c66028c35037e8bf67e5bc4ceb6a.dir")
    assert odb.exists("c157a79031e1c40f85931829bc5fc552")
    assert odb.exists("258622b1688250cb619f3c9ccaefb7eb")


def test_build(tmp_upath, odb, as_filesystem):
    (tmp_upath / "foo").write_text("foo\n")
    (tmp_upath / "data").mkdir()
    (tmp_upath / "data" / "bar").write_text("bar\n")
    (tmp_upath / "data" / "baz").write_text("baz\n")

    index = DataIndex(
        {
            ("foo",): DataIndexEntry(odb=odb, cache=odb),
            ("data",): DataIndexEntry(odb=odb, cache=odb),
        },
    )
    build(index, tmp_upath, as_filesystem(tmp_upath.fs))
    assert index[("foo",)].hash_info.name == "md5"
    assert (
        index[("foo",)].hash_info.value == "d3b07384d113edec49eaa6238ad5ff00"
    )
    assert index[("foo",)].odb != odb
    assert index[("foo",)].cache == odb
    assert index[("data",)].hash_info.name == "md5"
    assert (
        index[("data",)].hash_info.value
        == "1f69c66028c35037e8bf67e5bc4ceb6a.dir"
    )
    assert index[("data", "bar")].hash_info.name == "md5"
    assert (
        index[("data", "bar")].hash_info.value
        == "c157a79031e1c40f85931829bc5fc552"
    )
    assert index[("data", "baz")].hash_info.name == "md5"
    assert (
        index[("data", "baz")].hash_info.value
        == "258622b1688250cb619f3c9ccaefb7eb"
    )


def test_checkout(tmp_upath, odb, as_filesystem):
    index = DataIndex(
        {
            ("foo",): DataIndexEntry(
                odb=odb,
                hash_info=HashInfo(
                    name="md5", value="d3b07384d113edec49eaa6238ad5ff00"
                ),
            ),
            ("data",): DataIndexEntry(
                odb=odb,
                hash_info=HashInfo(
                    name="md5",
                    value="1f69c66028c35037e8bf67e5bc4ceb6a.dir",
                ),
            ),
        }
    )
    checkout(index, str(tmp_upath), as_filesystem(tmp_upath.fs))
    assert (tmp_upath / "foo").read_text() == "foo\n"
    assert (tmp_upath / "data").is_dir()
    assert (tmp_upath / "data" / "bar").read_text() == "bar\n"
    assert (tmp_upath / "data" / "baz").read_text() == "baz\n"
    assert set(tmp_upath.iterdir()) == {
        (tmp_upath / "foo"),
        (tmp_upath / "data"),
    }
    assert set((tmp_upath / "data").iterdir()) == {
        (tmp_upath / "data" / "bar"),
        (tmp_upath / "data" / "baz"),
    }


def test_commit(tmp_upath, odb, as_filesystem):
    (tmp_upath / "foo").write_text("foo\n")
    (tmp_upath / "data").mkdir()
    (tmp_upath / "data" / "bar").write_text("bar\n")
    (tmp_upath / "data" / "baz").write_text("baz\n")

    index = DataIndex(
        {
            ("foo",): DataIndexEntry(odb=odb, cache=odb),
            ("data",): DataIndexEntry(odb=odb, cache=odb),
        },
    )
    build(index, tmp_upath, as_filesystem(tmp_upath.fs))
    commit(index)
    assert odb.exists("d3b07384d113edec49eaa6238ad5ff00")
    assert odb.exists("1f69c66028c35037e8bf67e5bc4ceb6a.dir")
    assert odb.exists("c157a79031e1c40f85931829bc5fc552")
    assert odb.exists("258622b1688250cb619f3c9ccaefb7eb")
