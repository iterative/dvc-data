import pytest
from dvc_objects.errors import ObjectFormatError

from dvc_data.hashfile.db import HashFile, HashFileDB
from dvc_data.hashfile.db.local import LocalHashFileDB
from dvc_data.hashfile.meta import Meta


def test_db(tmp_upath, as_filesystem):
    odb = HashFileDB(as_filesystem(tmp_upath.fs), str(tmp_upath))

    assert not odb.exists("123456")
    assert list(odb.all()) == []

    obj = odb.get("123456")
    assert isinstance(obj, HashFile)


@pytest.mark.parametrize("tmp_upath", ["local", "memory"], indirect=True)
def test_db_check(tmp_upath, as_filesystem):
    fs = as_filesystem(tmp_upath.fs)
    db_cls = LocalHashFileDB if fs.protocol == "local" else HashFileDB
    odb = db_cls(as_filesystem(tmp_upath.fs), str(tmp_upath))

    oid = "acbd18db4cc2f85cedef654fccc4a4d8"
    path = odb.oid_to_path(oid)

    with pytest.raises(FileNotFoundError):
        odb.check(oid)

    odb.add_bytes(oid, b"foo")
    assert odb.check(oid) == Meta.from_info(odb.fs.info(path))

    odb.protect(oid)
    assert odb.check(oid) == Meta.from_info(odb.fs.info(path))

    odb.delete(oid)

    odb.add_bytes(oid, b"bar")
    with pytest.raises(ObjectFormatError):
        odb.check(oid)
