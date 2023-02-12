import pytest

from dvc_data.fs import DataFileSystem
from dvc_data.hashfile.db import HashFileDB
from dvc_data.hashfile.hash_info import HashInfo
from dvc_data.hashfile.meta import Meta
from dvc_data.index import (
    DataIndex,
    DataIndexEntry,
    ObjectStorage,
    add,
    build,
    checkout,
    md5,
    read_db,
    read_json,
    save,
    update,
    view,
    write_db,
    write_json,
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
    assert fs.ls("/", detail=False) == ["/foo", "/data"]
    assert fs.ls("/", detail=True) == [fs.info("/foo"), fs.info("/data")]
    assert fs.cat("/data/bar") == b"bar\n"
    assert fs.cat("/data/baz") == b"baz\n"
    assert fs.ls("/data", detail=False) == ["/data/bar", "/data/baz"]
    assert fs.ls("/data", detail=True) == [
        fs.info("/data/bar"),
        fs.info("/data/baz"),
    ]


def test_md5(tmp_upath, odb, as_filesystem):
    (tmp_upath / "foo").write_bytes(b"foo\n")
    (tmp_upath / "data").mkdir()
    (tmp_upath / "data" / "bar").write_bytes(b"bar\n")
    (tmp_upath / "data" / "baz").write_bytes(b"baz\n")

    fs = as_filesystem(tmp_upath.fs)
    index = build(str(tmp_upath), fs)
    md5(index)
    assert index[("foo",)].hash_info == HashInfo(
        "md5",
        "d3b07384d113edec49eaa6238ad5ff00",
    )
    assert index[("data", "bar")].hash_info == HashInfo(
        "md5",
        "c157a79031e1c40f85931829bc5fc552",
    )
    assert index[("data", "baz")].hash_info == HashInfo(
        "md5",
        "258622b1688250cb619f3c9ccaefb7eb",
    )


def test_save(tmp_upath, odb, as_filesystem):
    (tmp_upath / "foo").write_bytes(b"foo\n")
    (tmp_upath / "data").mkdir()
    (tmp_upath / "data" / "bar").write_bytes(b"bar\n")
    (tmp_upath / "data" / "baz").write_bytes(b"baz\n")

    fs = as_filesystem(tmp_upath.fs)
    index = build(str(tmp_upath), fs)
    md5(index)
    save(index, odb=odb)
    assert odb.exists("d3b07384d113edec49eaa6238ad5ff00")
    assert odb.exists("1f69c66028c35037e8bf67e5bc4ceb6a.dir")
    assert odb.exists("c157a79031e1c40f85931829bc5fc552")
    assert odb.exists("258622b1688250cb619f3c9ccaefb7eb")


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


def test_add(tmp_upath, as_filesystem):
    (tmp_upath / "foo").write_bytes(b"foo\n")
    (tmp_upath / "data").mkdir()
    (tmp_upath / "data" / "bar").write_bytes(b"bar\n")
    (tmp_upath / "data" / "baz").write_bytes(b"baz\n")

    fs = as_filesystem(tmp_upath.fs)
    index = build(str(tmp_upath), fs)
    index = DataIndex()

    add(index, str(tmp_upath / "foo"), fs, ("foo",))
    assert len(index) == 1
    assert index[("foo",)].meta.size == 4
    assert index.storage_map.get_data(index[("foo",)]) == (
        fs,
        str(tmp_upath / "foo"),
    )

    add(index, str(tmp_upath / "data"), fs, ("data",))
    assert len(index) == 4
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


def test_checkout(tmp_upath, odb, as_filesystem):
    index = DataIndex(
        {
            ("foo",): DataIndexEntry(
                key=("foo",),
                meta=Meta(),
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


@pytest.mark.parametrize(
    "write, read",
    [
        (write_db, read_db),
        (write_json, read_json),
    ],
)
def test_write_read(odb, tmp_path, write, read):
    index = DataIndex(
        {
            ("foo",): DataIndexEntry(),
            ("data",): DataIndexEntry(),
        },
    )
    index.load()

    path = str(tmp_path / "index")

    write(index, path)
    new_index = read(path)
    assert len(index) == len(new_index)
    for key, entry in new_index.iteritems():
        assert index[key].meta == entry.meta
        assert index[key].hash_info == entry.hash_info


@pytest.mark.parametrize(
    "keys, filter_fn, ensure_loaded",
    [
        (
            {
                ("foo",),
                ("dir", "subdir", "bar"),
                ("dir", "subdir", "bar", "bar"),
                ("dir", "subdir", "bar", "baz"),
            },
            lambda k: True,
            True,
        ),
        (
            {
                ("foo",),
                ("dir", "subdir", "bar"),
            },
            lambda k: True,
            False,
        ),
        (
            set(),
            lambda k: False,
            True,
        ),
        (
            set(),
            lambda k: False,
            False,
        ),
    ],
)
def test_view_iteritems(odb, keys, filter_fn, ensure_loaded):
    index = DataIndex(
        {
            ("foo",): DataIndexEntry(
                hash_info=HashInfo(
                    name="md5", value="d3b07384d113edec49eaa6238ad5ff00"
                ),
            ),
            ("dir", "subdir", "bar"): DataIndexEntry(
                hash_info=HashInfo(
                    name="md5",
                    value="1f69c66028c35037e8bf67e5bc4ceb6a.dir",
                ),
            ),
        }
    )
    index.storage_map.add_cache(ObjectStorage((), odb))
    index_view = view(index, filter_fn)
    assert keys == {
        key for key, _ in index_view._iteritems(ensure_loaded=ensure_loaded)
    }


def test_view(odb):
    expected_key = ("dir", "subdir", "bar")
    expected_entry = DataIndexEntry(
        key=expected_key,
        hash_info=HashInfo(
            name="md5",
            value="1f69c66028c35037e8bf67e5bc4ceb6a.dir",
        ),
    )
    index = DataIndex(
        {
            ("foo",): DataIndexEntry(
                key=("foo",),
                hash_info=HashInfo(
                    name="md5", value="d3b07384d113edec49eaa6238ad5ff00"
                ),
            ),
            expected_key: expected_entry,
        }
    )
    index.storage_map.add_cache(ObjectStorage((), odb))
    index_view = view(index, lambda k: "dir" in k)
    assert {expected_key} == set(index_view.keys())
    assert expected_key in index_view
    assert ("foo",) not in index_view
    assert len(index_view) == 1

    # iteritems() should ensure dirs are loaded
    assert len(list(index_view.iteritems())) == 3
    assert index_view[expected_key] == expected_entry
    assert index_view[expected_key] is index[expected_key]


def test_view_ls(odb):
    index = DataIndex(
        {
            ("foo",): DataIndexEntry(
                hash_info=HashInfo(
                    name="md5", value="d3b07384d113edec49eaa6238ad5ff00"
                ),
            ),
            ("dir", "subdir", "bar"): DataIndexEntry(
                hash_info=HashInfo(
                    name="md5",
                    value="1f69c66028c35037e8bf67e5bc4ceb6a.dir",
                ),
            ),
        }
    )
    index.storage_map.add_cache(ObjectStorage((), odb))
    index_view = view(index, lambda k: "dir" in k)
    assert list(index_view.ls((), detail=False)) == [("dir",)]
    assert list(index_view.ls(("dir",), detail=False)) == [
        (
            "dir",
            "subdir",
        )
    ]
    assert list(index_view.ls(("dir", "subdir"), detail=False)) == [
        ("dir", "subdir", "bar")
    ]


def test_view_traverse(odb):
    index = DataIndex(
        {
            ("foo",): DataIndexEntry(
                hash_info=HashInfo(
                    name="md5", value="d3b07384d113edec49eaa6238ad5ff00"
                ),
            ),
            ("dir", "subdir", "bar"): DataIndexEntry(
                hash_info=HashInfo(
                    name="md5",
                    value="1f69c66028c35037e8bf67e5bc4ceb6a.dir",
                ),
            ),
        }
    )
    index.storage_map.add_cache(ObjectStorage((), odb))
    index_view = view(index, lambda k: "dir" in k)

    keys = []

    def node_factory(_, key, children, *args):
        if key:
            keys.append(key)
        list(children)

    index_view.traverse(node_factory)
    assert keys == [
        ("dir",),
        ("dir", "subdir"),
        ("dir", "subdir", "bar"),
    ]


def test_update(tmp_upath, odb, as_filesystem):
    (tmp_upath / "foo").write_bytes(b"foo\n")
    (tmp_upath / "data").mkdir()
    (tmp_upath / "data" / "bar").write_bytes(b"bar\n")
    (tmp_upath / "data" / "baz").write_bytes(b"baz\n")

    fs = as_filesystem(tmp_upath.fs)
    old = build(str(tmp_upath), fs)
    md5(old)

    index = build(str(tmp_upath), fs)
    update(index, old)
    assert index[("foo",)].hash_info == HashInfo(
        "md5",
        "d3b07384d113edec49eaa6238ad5ff00",
    )
    assert index[("data", "bar")].hash_info == HashInfo(
        "md5",
        "c157a79031e1c40f85931829bc5fc552",
    )
    assert index[("data", "baz")].hash_info == HashInfo(
        "md5",
        "258622b1688250cb619f3c9ccaefb7eb",
    )
