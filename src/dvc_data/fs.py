import copy
import errno
import logging
import os
import typing

from dvc_objects.fs.callbacks import DEFAULT_CALLBACK
from fsspec import AbstractFileSystem
from funcy import cached_property

if typing.TYPE_CHECKING:
    from dvc_objects.fs.base import AnyFSPath

logger = logging.getLogger(__name__)


class DataFileSystem(AbstractFileSystem):  # pylint:disable=abstract-method
    root_marker = "/"

    def __init__(self, index, **kwargs):
        super().__init__(**kwargs)
        self.index = index

    @cached_property
    def path(self):
        from dvc_objects.fs.path import Path

        def _getcwd():
            return self.root_marker

        return Path(self.sep, getcwd=_getcwd)

    @property
    def config(self):
        raise NotImplementedError

    def _get_key(self, path):
        path = self.path.abspath(path)
        if path == self.root_marker:
            return ()

        key = self.path.relparts(path, self.root_marker)
        if key == (".") or key == (""):
            key = ()

        return key

    def _get_fs_path(self, path: "AnyFSPath"):
        from .index import StorageKeyError

        info = self.info(path)
        if info["type"] == "directory":
            raise IsADirectoryError(
                errno.EISDIR, os.strerror(errno.EISDIR), path
            )

        entry = info["entry"]

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
                    return typ, storage, fs, fs_path

        raise FileNotFoundError(
            errno.ENOENT, "No storage files available", path
        )

    def open(  # type: ignore
        self, path: str, mode="r", encoding=None, **kwargs
    ):  # pylint: disable=arguments-renamed, arguments-differ
        cache_odb = kwargs.pop("cache_odb", None)
        typ, _, fs, fspath = self._get_fs_path(path, **kwargs)

        if cache_odb and typ == "remote":
            from dvc_data.hashfile.build import _upload_file

            _, obj = _upload_file(fspath, fs, cache_odb, cache_odb)
            fs, fspath = cache_odb.fs, obj.path

        return fs.open(fspath, mode=mode, encoding=encoding)

    def ls(self, path, detail=True, **kwargs):
        from .index import TreeError

        root_key = self._get_key(path)
        try:
            info = self.index.info(root_key)
            if info["type"] != "directory":
                raise NotADirectoryError(path)

            if not detail:
                return [
                    self.path.join(path, key[-1])
                    for key in self.index.ls(root_key, detail=False)
                ]

            entries = []
            for key, info in self.index.ls(root_key, detail=True):
                info["name"] = self.path.join(path, key[-1])
                entries.append(info)
            return entries
        except (KeyError, TreeError) as exc:
            raise FileNotFoundError(
                errno.ENOENT, os.strerror(errno.ENOENT), path
            ) from exc

    def info(self, path, **kwargs):
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

    def get_file(  # pylint: disable=arguments-differ
        self, rpath, lpath, callback=DEFAULT_CALLBACK, **kwargs
    ):
        from dvc_objects.fs.generic import transfer
        from dvc_objects.fs.local import LocalFileSystem

        from dvc_data.index import ObjectStorage

        try:
            _, storage, fs, path = self._get_fs_path(rpath)
        except IsADirectoryError:
            os.makedirs(lpath, exist_ok=True)
            return None

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

    def checksum(self, path):
        info = self.info(path)
        md5 = info.get("md5")
        if md5:
            return md5
        raise NotImplementedError
