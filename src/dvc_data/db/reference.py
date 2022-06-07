import logging
from contextlib import suppress
from typing import TYPE_CHECKING, Dict

from dvc_objects.errors import ObjectFormatError
from dvc_objects.hashfile.db import ObjectDB

from ..objects.reference import ReferenceHashFile

if TYPE_CHECKING:
    from dvc_objects.fs.base import AnyFSPath, FileSystem
    from dvc_objects.hashfile.hash_info import HashInfo

logger = logging.getLogger(__name__)


class ReferenceObjectDB(ObjectDB):
    """Reference ODB.

    File objects are stored as ReferenceHashFiles which reference paths outside
    of the staging ODB fs. Tree objects are stored natively.
    """

    def __init__(self, fs: "FileSystem", path: str, **config):
        super().__init__(fs, path, **config)
        self.raw = ObjectDB(self.fs, self.fs_path, **self.config)
        self._fs_cache: Dict[tuple, "FileSystem"] = {}
        self._obj_cache: Dict["HashInfo", "ReferenceHashFile"] = {}

    def get(self, hash_info: "HashInfo"):
        raw = self.raw.get(hash_info)

        if hash_info.isdir:
            return raw

        try:
            return self._obj_cache[hash_info]
        except KeyError:
            pass

        try:
            obj = ReferenceHashFile.from_raw(raw, fs_cache=self._fs_cache)
        except ObjectFormatError:
            raw.fs.remove(raw.fs_path)
            raise

        deref = obj.deref()
        self._obj_cache[hash_info] = deref

        return deref

    def add(
        self,
        fs_path: "AnyFSPath",
        fs: "FileSystem",
        hash_info: "HashInfo",
        **kwargs,
    ):  # pylint: disable=arguments-differ
        if hash_info.isdir:
            return self.raw.add(fs_path, fs, hash_info, **kwargs)

        obj = ReferenceHashFile.from_path(
            fs_path, fs, hash_info, fs_cache=self._fs_cache
        )
        self._obj_cache[hash_info] = obj.deref()

        return self.raw.add(obj.fs_path, obj.fs, hash_info, **kwargs)

    def check(
        self,
        hash_info: "HashInfo",
        check_hash: bool = True,
    ):
        obj = self.get(hash_info)

        try:
            obj.check(self, check_hash=check_hash)
        except ObjectFormatError:
            raw = self.raw.get(hash_info)
            logger.debug("corrupted cache file '%s'.", raw.fs_path)
            with suppress(FileNotFoundError):
                raw.fs.remove(raw.fs_path)
            raise
