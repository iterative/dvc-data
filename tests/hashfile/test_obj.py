from dvc_objects.fs.local import LocalFileSystem

from dvc_data.hashfile.hash_info import HashInfo
from dvc_data.hashfile.obj import HashFile


def test_obj(tmp_path):
    fs = LocalFileSystem()
    hash_info = HashInfo("md5", "123456")
    obj = HashFile(tmp_path, fs, hash_info)
    assert obj.path == tmp_path
    assert obj.fs == fs
    assert obj.oid == "123456"
    assert obj.hash_info == hash_info
