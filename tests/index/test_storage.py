from dvc_objects.fs.local import LocalFileSystem

from dvc_data.index import FileStorage, ObjectStorage, StorageInfo, StorageMapping


def test_map_get(tmp_path, odb):
    smap = StorageMapping()

    fs = LocalFileSystem()

    data = FileStorage(key=(), fs=fs, path=str(tmp_path))
    cache = FileStorage(key=("dir",), fs=fs, path=str(tmp_path))
    remote = FileStorage(key=("dir", "subdir"), fs=fs, path=str(tmp_path))
    foo_cache = ObjectStorage(key=("dir", "foo"), odb=odb)

    smap[()] = StorageInfo(data=data)
    smap[("dir",)] = StorageInfo(cache=cache)
    smap[("dir", "subdir")] = StorageInfo(remote=remote)
    smap[("dir", "foo")] = StorageInfo(cache=foo_cache)

    sinfo = smap[()]
    assert sinfo.data == data
    assert sinfo.cache is None
    assert sinfo.remote is None

    sinfo = smap[("dir",)]
    assert sinfo.data == data
    assert sinfo.cache == cache
    assert sinfo.remote is None

    sinfo = smap[("dir", "foo")]
    assert sinfo.data == data
    assert sinfo.cache == foo_cache
    assert sinfo.remote is None

    sinfo = smap[("dir", "subdir")]
    assert sinfo.data == data
    assert sinfo.cache == cache
    assert sinfo.remote == remote

    sinfo = smap[("dir", "subdir", "file")]
    assert sinfo.data == data
    assert sinfo.cache == cache
    assert sinfo.remote == remote

    sinfo = smap[("dir", "subdir", "subsubdir", "otherfile")]
    assert sinfo.data == data
    assert sinfo.cache == cache
    assert sinfo.remote == remote
