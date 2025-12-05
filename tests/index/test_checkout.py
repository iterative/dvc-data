from dvc_objects.fs.local import LocalFileSystem

from dvc_data.hashfile.hash_info import HashInfo
from dvc_data.hashfile.meta import Meta
from dvc_data.index import DataIndex, DataIndexEntry, ObjectStorage
from dvc_data.index.checkout import apply, compare

localfs = LocalFileSystem()


def test_checkout(tmp_path, odb):
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
    diff = compare(None, index)
    apply(diff, str(tmp_path), localfs)
    assert (tmp_path / "foo").read_text() == "foo\n"
    assert (tmp_path / "data").is_dir()
    assert (tmp_path / "data" / "bar").read_text() == "bar\n"
    assert (tmp_path / "data" / "baz").read_text() == "baz\n"
    assert set(tmp_path.iterdir()) == {
        (tmp_path / "foo"),
        (tmp_path / "data"),
    }
    assert set((tmp_path / "data").iterdir()) == {
        (tmp_path / "data" / "bar"),
        (tmp_path / "data" / "baz"),
    }


def test_checkout_file(tmp_path, odb):
    index = DataIndex(
        {
            (): DataIndexEntry(
                key=(),
                meta=Meta(),
                hash_info=HashInfo(
                    name="md5", value="d3b07384d113edec49eaa6238ad5ff00"
                ),
            ),
        }
    )
    index.storage_map.add_cache(ObjectStorage((), odb))
    diff = compare(None, index)
    apply(diff, str(tmp_path / "foo"), localfs)
    assert (tmp_path / "foo").read_text() == "foo\n"


def test_checkout_broken_dir(tmp_path, odb):
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
    diff = compare(None, index)
    apply(diff, str(tmp_path), localfs)
    assert (tmp_path / "foo").read_text() == "foo\n"
    assert (tmp_path / "data").is_dir()
    assert (tmp_path / "data" / "bar").read_text() == "bar\n"
    assert (tmp_path / "data" / "baz").read_text() == "baz\n"
    assert set(tmp_path.iterdir()) == {
        (tmp_path / "foo"),
        (tmp_path / "data"),
    }
    assert set((tmp_path / "data").iterdir()) == {
        (tmp_path / "data" / "bar"),
        (tmp_path / "data" / "baz"),
    }
    assert not (tmp_path / "broken").exists()


def test_checkout_delete_nested_dir(tmp_path, odb):
    old = DataIndex(
        {
            ("dir1",): DataIndexEntry(
                key=("dir1",),
                meta=Meta(isdir=True),
            ),
            ("dir1", "subdir1"): DataIndexEntry(
                key=("dir1", "subdir1"),
                meta=Meta(isdir=True),
            ),
        }
    )
    diff = compare(None, old)
    apply(diff, str(tmp_path), localfs)

    assert (tmp_path / "dir1").exists()
    assert (tmp_path / "dir1").is_dir()
    assert (tmp_path / "dir1" / "subdir1").exists()
    assert (tmp_path / "dir1" / "subdir1").is_dir()

    new = DataIndex({})
    diff = compare(old, new, delete=True)
    apply(diff, str(tmp_path), localfs)

    assert not (tmp_path / "dir1" / "subdir1").exists()
    assert not (tmp_path / "dir1").exists()
