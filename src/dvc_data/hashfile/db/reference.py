import logging
from typing import TYPE_CHECKING, Callable, Dict, List, Optional, Union

from dvc_data.hashfile.obj import HashFile

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
        path: Union["AnyFSPath", List["AnyFSPath"]],
        fs: "FileSystem",
        oid: Union[str, List[str]],
        hardlink: bool = False,
        callback: Optional["Callback"] = None,
        check_exists: bool = True,
        on_error: Optional[Callable[[str, BaseException], None]] = None,
        **kwargs,
    ):
        paths = [path] if isinstance(path, str) else path
        oids = [oid] if isinstance(oid, str) else oid
        assert len(paths) == len(oids)

        for i in range(len(paths)):
            hash_info = HashInfo(self.hash_name, oids[i])
            self._obj_cache[oids[i]] = HashFile(paths[i], fs, hash_info)

    def check(
        self,
        oid: str,
        check_hash: bool = True,
    ):
        return
