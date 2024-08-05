import os

from dvc_objects.fs.local import LocalFileSystem

from dvc_data.hashfile.build import build
from dvc_data.hashfile.db import HashFileDB
from dvc_data.hashfile.hash_info import HashInfo
from dvc_data.hashfile.meta import Meta
from dvc_data.hashfile.tree import Tree


def test_build_file(tmp_path):
    fs = LocalFileSystem()
    file = tmp_path / "foo"

    odb = HashFileDB(fs, os.fspath(tmp_path / ".dvc" / ".cache" / "files" / "md5"))

    fs.pipe({file: b"foo"})

    _, meta, obj = build(odb, str(file), fs, "md5")
    assert meta.isdir is False
    assert meta.size == 3
    assert obj.hash_info == HashInfo("md5", "acbd18db4cc2f85cedef654fccc4a4d8")


def test_build_directory(tmp_path):
    fs = LocalFileSystem()
    directory = tmp_path / "dir"
    directory.mkdir()

    odb = HashFileDB(fs, os.fspath(tmp_path / ".dvc" / ".cache" / "files" / "md5"))

    fs.pipe({directory / "foo": b"foo", directory / "bar": b"bar"})

    _, meta, tree = build(odb, str(directory), fs, "md5")
    assert meta == Meta(isdir=True, size=6, nfiles=2)
    assert isinstance(tree, Tree)
    assert tree.hash_info == HashInfo("md5", "5ea40360f5b4ec688df672a4db9c17d1.dir")
    assert tree.as_list() == [
        {"md5": "37b51d194a7513e45b56f6524f2d51f2", "relpath": "bar"},
        {"md5": "acbd18db4cc2f85cedef654fccc4a4d8", "relpath": "foo"},
    ]
