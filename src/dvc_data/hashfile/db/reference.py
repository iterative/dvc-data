import logging
from typing import TYPE_CHECKING, Dict

from ..obj import HashFile
from . import HashFileDB, HashInfo

if TYPE_CHECKING:
    from dvc_objects.fs.base import AnyFSPath, FileSystem
    from dvc_objects.fs.callbacks import Callback

logger = logging.getLogger(__name__)


class ReferenceHashFileDB(HashFileDB):
    def __init__(self, fs: "FileSystem", path: str, **config):
        super().__init__(fs, path, **config)
        self._obj_cache: Dict["str", "HashFile"] = {}

    def __hash__(self):
        return hash((self.fs.protocol, self.path, *self._obj_cache.keys()))

    def exists(self, oid: str) -> bool:
        return oid in self._obj_cache

    def get(self, oid: str):
        try:
            return self._obj_cache[oid]
        except KeyError:
            return super().get(oid)

    def add(
        self,
        path: "AnyFSPath",
        fs: "FileSystem",
        oid: str,
        hardlink: bool = False,
        callback: "Callback" = None,
        **kwargs,
    ):  # pylint: disable=arguments-differ
        hash_info = HashInfo(self.hash_name, oid)
        self._obj_cache[oid] = HashFile(path, fs, hash_info)

    def check(
        self,
        oid: str,
        check_hash: bool = True,
    ):
        return
