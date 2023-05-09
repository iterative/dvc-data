import os

import pytest

from dvc_data.hashfile.db import HashFileDB


@pytest.fixture
def make_odb(tmp_upath_factory, as_filesystem):
    def _make_odb():
        path = tmp_upath_factory.mktemp()
        fs = as_filesystem(path.fs)
        return HashFileDB(fs, os.fspath(path))

    return _make_odb


@pytest.fixture
def odb(tmp_upath_factory, make_odb):
    odb = make_odb()

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

    odb.add(str(foo), odb.fs, "d3b07384d113edec49eaa6238ad5ff00")
    odb.add(str(data), odb.fs, "1f69c66028c35037e8bf67e5bc4ceb6a.dir")
    odb.add(str(bar), odb.fs, "c157a79031e1c40f85931829bc5fc552")
    odb.add(str(baz), odb.fs, "258622b1688250cb619f3c9ccaefb7eb")

    return odb
