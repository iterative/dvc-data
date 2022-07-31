import pytest

from dvc_data.hashfile.db import HashFileDB
from dvc_data.hashfile.hash_info import HashInfo
from dvc_data.index import DataIndex, DataIndexEntry, build, checkout


@pytest.fixture
def odb(tmp_upath_factory, as_filesystem):
    path = tmp_upath_factory.mktemp()
    fs = as_filesystem(path.fs)
    odb = HashFileDB(fs, path)

    foo = tmp_upath_factory.mktemp() / "foo"
    foo.write_text("foo\n")

    odb.add(str(foo), fs, "d3b07384d113edec49eaa6238ad5ff00")

    return odb


def test_index():
    index = DataIndex()
    index[("foo",)] = DataIndexEntry()


def test_build(tmp_upath, odb, as_filesystem):
    (tmp_upath / "foo").write_text("foo\n")
    index = DataIndex({("foo",): DataIndexEntry(odb=odb)})
    build(index, tmp_upath, as_filesystem(tmp_upath.fs))
    assert index[("foo",)].hash_info.name == "md5"
    assert (
        index[("foo",)].hash_info.value == "d3b07384d113edec49eaa6238ad5ff00"
    )
    assert index[("foo",)].odb == odb


def test_checkout(tmp_upath, odb, as_filesystem):
    index = DataIndex(
        {
            ("foo",): DataIndexEntry(
                odb=odb,
                hash_info=HashInfo(
                    name="md5", value="d3b07384d113edec49eaa6238ad5ff00"
                ),
            )
        }
    )
    checkout(index, str(tmp_upath), as_filesystem(tmp_upath.fs))
    assert (tmp_upath / "foo").read_text() == "foo\n"
