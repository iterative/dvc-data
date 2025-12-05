import pytest
from dvc_objects.errors import ObjectFormatError
from dvc_objects.fs.local import LocalFileSystem
from dvc_objects.fs.memory import MemoryFileSystem

from dvc_data.hashfile.db import HashFile, HashFileDB
from dvc_data.hashfile.db.local import LocalHashFileDB
from dvc_data.hashfile.meta import Meta
from dvc_data.index import TYPE_CHECKING

if TYPE_CHECKING:
    from dvc_objects.fs.base import FileSystem

fs = LocalFileSystem()


def test_db(tmp_path):
    odb = HashFileDB(LocalFileSystem(), str(tmp_path))

    assert not odb.exists("123456")
    assert list(odb.all()) == []

    obj = odb.get("123456")
    assert isinstance(obj, HashFile)


@pytest.mark.parametrize("fs_protocol", ["local", "memory"])
def test_db_check(fs_protocol, tmp_path_factory):
    if fs_protocol == "local":
        fs: FileSystem = LocalFileSystem()
        odb: HashFileDB = LocalHashFileDB(fs, str(tmp_path_factory.mktemp("odb")))
    elif fs_protocol == "memory":
        fs = MemoryFileSystem(global_store=False)
        odb = HashFileDB(fs, fs.root_marker)
    else:
        raise ValueError(f"Unsupported fs protocol: {fs_protocol}")

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
