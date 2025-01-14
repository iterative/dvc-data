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
def odb(make_odb):
    odb = make_odb()

    odb.add_bytes("d3b07384d113edec49eaa6238ad5ff00", b"foo\n")
    odb.add_bytes("c157a79031e1c40f85931829bc5fc552", b"bar\n")
    odb.add_bytes("258622b1688250cb619f3c9ccaefb7eb", b"baz\n")
    odb.add_bytes(
        "1f69c66028c35037e8bf67e5bc4ceb6a.dir",
        (
            b'[{"md5": "c157a79031e1c40f85931829bc5fc552", "relpath": "bar"}, '
            b'{"md5": "258622b1688250cb619f3c9ccaefb7eb", "relpath": "baz"}]'
        ),
    )
    return odb
