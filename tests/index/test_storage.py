from dvc_data.index import FileStorage, ObjectStorage, StorageInfo, StorageMapping


def test_map_get(tmp_upath, as_filesystem, odb):
    smap = StorageMapping()

    data = FileStorage(key=(), fs=as_filesystem(tmp_upath.fs), path=str(tmp_upath))
    cache = FileStorage(
        key=("dir",), fs=as_filesystem(tmp_upath.fs), path=str(tmp_upath)
    )
    remote = FileStorage(
        key=("dir", "subdir"), fs=as_filesystem(tmp_upath.fs), path=str(tmp_upath)
    )
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
