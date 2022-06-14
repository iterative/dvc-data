import logging
from contextlib import suppress
from typing import TYPE_CHECKING, Dict

from dvc_objects.db import ObjectDB
from dvc_objects.errors import ObjectFormatError

from ..hashfile.db import HashFileDB, HashInfo
from ..hashfile.hash import hash_file
from ..hashfile.obj import HashFile
from ..objects.reference import ReferenceObject

if TYPE_CHECKING:
    from dvc_objects.fs.base import AnyFSPath, FileSystem
    from dvc_objects.fs.callbacks import Callback

logger = logging.getLogger(__name__)


class ReferenceHashFileDB(HashFileDB):
    """Reference ODB.

    File objects are stored as ReferenceObjects which reference paths outside
    of the staging ODB fs. Tree objects are stored natively.
    """

    def __init__(self, fs: "FileSystem", path: str, **config):
        super().__init__(fs, path, **config)
        self.raw = ObjectDB(self.fs, self.path, **self.config)
        self._fs_cache: Dict[tuple, "FileSystem"] = {}
        self._obj_cache: Dict["HashInfo", "ReferenceObject"] = {}

    def _deref(self, obj):
        return HashFile(obj.ref.path, obj.ref.fs, obj.ref.hash_info)

    def get(self, oid: str):
        raw = self.raw.get(oid)

        hash_info = HashInfo(self.hash_name, oid)

        if hash_info.isdir:
            return HashFile(raw.path, raw.fs, hash_info)

        try:
            return self._obj_cache[hash_info]
        except KeyError:
            pass

        try:
            obj = ReferenceObject.from_raw(raw, fs_cache=self._fs_cache)
        except ObjectFormatError:
            raw.fs.remove(raw.path)
            raise

        deref = self._deref(obj)
        self._obj_cache[hash_info] = deref

        return deref

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
        if hash_info.isdir:
            return self.raw.add(
                path, fs, oid, hardlink=hardlink, callback=callback, **kwargs
            )

        obj = ReferenceObject.from_path(
            path, fs, hash_info, fs_cache=self._fs_cache
        )
        self._obj_cache[hash_info] = self._deref(obj)

        return self.raw.add(
            obj.path,
            obj.fs,
            oid,
            hardlink=hardlink,
            callback=callback,
            **kwargs,
        )

    def check(
        self,
        oid: str,
        check_hash: bool = True,
    ):
        if not check_hash:
            if not self.exists(oid):
                raise FileNotFoundError
            return

        obj = self.get(oid)

        _, actual = hash_file(obj.path, obj.fs, obj.hash_info.name, self.state)
        assert actual.name == self.hash_name
        assert actual.value
        if actual.value.split(".")[0] != oid.split(".")[0]:
            raw = self.raw.get(oid)
            logger.debug("corrupted cache file '%s'.", raw.path)
            with suppress(FileNotFoundError):
                raw.fs.remove(raw.path)
            raise ObjectFormatError(f"{obj} is corrupted")
