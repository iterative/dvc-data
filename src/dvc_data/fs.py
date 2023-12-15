import copy
import errno
import logging
import os
import posixpath
import typing
from collections import deque
from typing import Any, BinaryIO, NamedTuple, Optional, Tuple

from dvc_objects.fs.callbacks import DEFAULT_CALLBACK
from fsspec import AbstractFileSystem

if typing.TYPE_CHECKING:
    from dvc_objects.fs.base import AnyFSPath, FileSystem
    from dvc_objects.fs.callbacks import Callback

    from dvc_data.hashfile.db import HashFileDB

    from .hashfile.hash_info import HashInfo
    from .index import DataIndex, DataIndexEntry, ObjectStorage

logger = logging.getLogger(__name__)


class FileInfo(NamedTuple):
    typ: str
    storage: "ObjectStorage"
    cache_storage: "ObjectStorage"
    hash_info: Optional["HashInfo"]
    fs: "FileSystem"
    fs_path: "AnyFSPath"


class DataFileSystem(AbstractFileSystem):
    root_marker = "/"

    def __init__(self, index: "DataIndex", **kwargs: Any):
        super().__init__(**kwargs)
        self.index = index

    @classmethod
    def join(cls, *parts: str) -> str:
        return posixpath.join(*parts)

    @classmethod
    def parts(cls, path: str) -> Tuple[str, ...]:
        ret = []
        while True:
            path, part = posixpath.split(path)

            if part:
                ret.append(part)
                continue

            if path:
                ret.append(path)

            break

        ret.reverse()

        return tuple(ret)

    def getcwd(self) -> str:
        return self.root_marker

    def normpath(self, path: str) -> str:
        return posixpath.normpath(path)

    def abspath(self, path: str) -> str:
        if not posixpath.isabs(path):
            path = self.join(self.getcwd(), path)
        return self.normpath(path)

    def relpath(self, path: str, start: Optional[str] = None) -> str:
        if start is None:
            start = "."
        return posixpath.relpath(self.abspath(path), start=self.abspath(start))

    def relparts(self, path: str, start: Optional[str] = None) -> Tuple[str, ...]:
        return self.parts(self.relpath(path, start=start))

    def _get_key(self, path: str) -> Tuple[str, ...]:
        path = self.abspath(path)
        if path == self.root_marker:
            return ()

        key = self.relparts(path, self.root_marker)
        if key in ((".",), ("",)):
            key = ()

        return key

    def _get_fs_path(self, path: "AnyFSPath") -> FileInfo:
        from .index import StorageKeyError

        info = self.info(path)
        if info["type"] == "directory":
            raise IsADirectoryError(errno.EISDIR, os.strerror(errno.EISDIR), path)

        entry: Optional["DataIndexEntry"] = info["entry"]

        assert entry
        hash_info: Optional["HashInfo"] = entry.hash_info

        for typ in ["cache", "remote", "data"]:
            try:
                info = self.index.storage_map[entry.key]
                storage = getattr(info, typ)
                if not storage:
                    continue
                data = storage.get(entry)
            except (ValueError, StorageKeyError):
                continue
            if data:
                fs, fs_path = data
                if fs.exists(fs_path):
                    return FileInfo(typ, storage, info.cache, hash_info, fs, fs_path)

        raise FileNotFoundError(errno.ENOENT, "No storage files available", path)

    def _cache_remote_file(
        self,
        cache_storage: "ObjectStorage",
        fs: "FileSystem",
        path: "AnyFSPath",
        hash_info: Optional["HashInfo"],
    ) -> Tuple["FileSystem", "AnyFSPath"]:
        from dvc_objects.fs.local import LocalFileSystem

        odb: "HashFileDB" = cache_storage.odb
        oid = hash_info.value if hash_info else None
        hash_name = hash_info.name if hash_info else None
        assert odb.hash_name

        if isinstance(fs, LocalFileSystem) or not oid or odb.hash_name != hash_name:
            return fs, path

        odb.add(path, fs, oid)
        return odb.fs, odb.oid_to_path(oid)

    def _open(self, path: "AnyFSPath", **kwargs: Any) -> "BinaryIO":
        typ, _, cache_storage, hi, fs, fspath = self._get_fs_path(path)

        if kwargs.get("cache", False) and typ == "remote" and cache_storage:
            fs, fspath = self._cache_remote_file(cache_storage, fs, fspath, hi)

        return fs.open(fspath, mode="rb")

    def ls(self, path: "AnyFSPath", detail: bool = True, **kwargs: Any):
        root_key = self._get_key(path)
        try:
            info = self.index.info(root_key)
            if info["type"] != "directory":
                raise NotADirectoryError(path)

            if not detail:
                return [
                    self.join(path, key[-1])
                    for key in self.index.ls(root_key, detail=False)
                ]

            entries = []
            for key, info in self.index.ls(root_key, detail=True):
                info["name"] = self.join(path, key[-1])
                entries.append(info)
            return entries
        except KeyError as exc:
            raise FileNotFoundError(
                errno.ENOENT, os.strerror(errno.ENOENT), path
            ) from exc

    def info(self, path: "AnyFSPath", **kwargs: Any):
        key = self._get_key(path)

        try:
            info = self.index.info(key)
        except KeyError as exc:
            raise FileNotFoundError(
                errno.ENOENT,
                os.strerror(errno.ENOENT),
                path,
            ) from exc

        info["name"] = path
        return info

    def get_file(
        self,
        rpath: "AnyFSPath",
        lpath: "AnyFSPath",
        callback: "Callback" = DEFAULT_CALLBACK,
        **kwargs: Any,
    ) -> None:
        from dvc_objects.fs.generic import transfer
        from dvc_objects.fs.local import LocalFileSystem

        from dvc_data.index import ObjectStorage

        try:
            typ, storage, cache_storage, hi, fs, path = self._get_fs_path(rpath)
        except IsADirectoryError:
            os.makedirs(lpath, exist_ok=True)
            return None

        cache = kwargs.pop("cache", False)
        if cache and typ == "remote" and cache_storage:
            fs, path = self._cache_remote_file(cache_storage, fs, path, hi)

        if (
            isinstance(storage, ObjectStorage)
            and isinstance(fs, LocalFileSystem)
            and storage.odb.cache_types
        ):
            try:
                transfer(
                    fs,
                    path,
                    fs,
                    os.fspath(lpath),
                    callback=callback,
                    links=copy.copy(storage.odb.cache_types),
                )
                return
            except OSError:
                pass

        fs.get_file(path, lpath, callback=callback, **kwargs)

    def checksum(self, path: str) -> str:
        info = self.info(path)
        md5 = info.get("md5")
        if md5:
            assert isinstance(md5, str)
            return md5
        raise NotImplementedError

    def du(self, path, total=True, maxdepth=None, withdirs=False, **kwargs):
        if maxdepth is not None:
            raise NotImplementedError

        sizes = {}
        todo = deque([self.info(path)])
        while todo:
            info = todo.popleft()

            sizes[info["name"]] = info["size"] or 0

            if info["type"] != "directory":
                continue

            entry = info.get("entry")
            if entry is not None and entry.size is not None:
                continue

            todo.extend(self.ls(info["name"], detail=True))

        if total:
            return sum(sizes.values())

        return sizes
