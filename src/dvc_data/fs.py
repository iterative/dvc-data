import logging
import typing

from dvc_objects.fs.callbacks import DEFAULT_CALLBACK
from fsspec import AbstractFileSystem
from funcy import cached_property

if typing.TYPE_CHECKING:
    from dvc.types import AnyPath

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

    def _get_fs_path(self, path: "AnyPath"):
        info = self.info(path)
        if info["type"] == "directory":
            raise IsADirectoryError

        value = info.get("md5")
        if not value:
            raise FileNotFoundError

        entry = info["entry"]

        cache_path = entry.odb.oid_to_path(value)

        if entry.odb.fs.exists(cache_path):
            return entry.odb.fs, cache_path

        if not entry.remote:
            raise FileNotFoundError

        remote_fs_path = entry.remote.oid_to_path(value)
        return entry.remote.fs, remote_fs_path

    def open(  # type: ignore
        self, path: str, mode="r", encoding=None, **kwargs
    ):  # pylint: disable=arguments-renamed, arguments-differ
        fs, fspath = self._get_fs_path(path, **kwargs)
        return fs.open(fspath, mode=mode, encoding=encoding)

    def ls(self, path, detail=True, **kwargs):
        info = self.info(path)
        if info["type"] != "directory":
            return [info] if detail else [path]

        root_key = self._get_key(path)
        try:
            entries = [
                self.path.join(path, name) if path else name
                for name in self.index.ls(prefix=root_key)
            ]
        except KeyError as exc:
            raise FileNotFoundError from exc

        if not detail:
            return entries

        return [self.info(epath) for epath in entries]

    def isdvc(self, path, recursive=False, strict=True):
        try:
            info = self.info(path)
        except FileNotFoundError:
            return False

        recurse = recursive or not strict
        if not recurse:
            return info.get("isout")

        key = self._get_key(path)
        return self.index.has_node(key)

    def info(self, path, **kwargs):
        from dvc_data.index import ShortKeyError

        key = self._get_key(path)

        try:
            entry = self.index[key]
            isdir = entry.hash_info and entry.hash_info.isdir
            return {
                "type": "directory" if isdir else "file",
                "name": path,
                "size": entry.meta.size if entry.meta else 0,
                "isexec": entry.meta.isexec if entry.meta else False,
                "isdvc": True,
                "isout": True,
                "obj": entry.obj,
                "entry": entry,
                entry.hash_info.name: entry.hash_info.value,
            }
        except ShortKeyError:
            return {
                "type": "directory",
                "name": path,
                "size": 0,
                "isexec": False,
                "isdvc": bool(self.index.longest_prefix(key)),
                "isout": False,
                "obj": None,
                "entry": None,
            }
        except KeyError as exc:
            raise FileNotFoundError from exc

    def get_file(  # pylint: disable=arguments-differ
        self, rpath, lpath, callback=DEFAULT_CALLBACK, **kwargs
    ):
        fs, path = self._get_fs_path(rpath)
        fs.get_file(path, lpath, callback=callback, **kwargs)

    def checksum(self, path):
        info = self.info(path)
        md5 = info.get("md5")
        if md5:
            return md5
        raise NotImplementedError
