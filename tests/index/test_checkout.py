from dvc_data.hashfile.hash_info import HashInfo
from dvc_data.hashfile.meta import Meta
from dvc_data.index import DataIndex, DataIndexEntry, ObjectStorage
from dvc_data.index.checkout import apply, compare


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
    diff = compare(None, index)
    apply(diff, str(tmp_upath), as_filesystem(tmp_upath.fs))
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


def test_checkout_file(tmp_upath, odb, as_filesystem):
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
    apply(diff, str(tmp_upath / "foo"), as_filesystem(tmp_upath.fs))
    assert (tmp_upath / "foo").read_text() == "foo\n"
