from dvc_data.hashfile.hash_info import HashInfo
from dvc_data.hashfile.obj import HashFile


def test_obj(tmp_upath):
    hash_info = HashInfo("md5", "123456")
    obj = HashFile(tmp_upath, tmp_upath.fs, hash_info)
    assert obj.path == tmp_upath
    assert obj.fs == tmp_upath.fs
    assert obj.oid == "123456"
    assert obj.hash_info == hash_info
