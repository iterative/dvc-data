import json
import logging
import os
from dataclasses import dataclass
from typing import TYPE_CHECKING

from dvc_objects.errors import ObjectFormatError
from dvc_objects.obj import Object

from ..hashfile.hash_info import HashInfo

if TYPE_CHECKING:
    from dvc_objects.fs.base import AnyFSPath

logger = logging.getLogger(__name__)


@dataclass
class Reference:
    path: "AnyFSPath"
    fs_config: tuple
    checksum: int
    hash_info: "HashInfo"


class ReferenceObject(Object):
    PARAM_PATH = "path"
    PARAM_HASH = "hash"
    PARAM_CHECKSUM = "checksum"
    PARAM_FS_CONFIG = "fs_config"

    def __init__(
        self,
        path: "AnyFSPath",
        fs_config: tuple,
        checksum: int,
        hash_info: "HashInfo",
    ):  # pylint: disable=super-init-not-called
        self.path = None
        self.fs = None
        self.oid = hash_info.value
        self.ref = Reference(path, fs_config, checksum, hash_info)

    def as_dict(self):
        # NOTE: dumping reference FS's this way is insecure, as the
        # fully parsed remote FS config will include credentials
        #
        # ReferenceObject should currently only be serialized in
        # memory and not to disk
        return {
            self.PARAM_PATH: self.ref.path,
            self.PARAM_HASH: self.ref.hash_info.to_dict(),
            self.PARAM_CHECKSUM: self.ref.checksum,
            self.PARAM_FS_CONFIG: self.ref.fs_config,
        }

    @classmethod
    def from_dict(cls, dict_):
        try:
            path = dict_[cls.PARAM_PATH]
        except KeyError as exc:
            raise ObjectFormatError("ReferenceObject is corrupted") from exc

        fs_config = dict_.get(cls.PARAM_FS_CONFIG)
        checksum = dict_.get(cls.PARAM_CHECKSUM)
        hash_info = HashInfo.from_dict(dict_.get(cls.PARAM_HASH))

        return cls(path, fs_config, checksum, hash_info)

    def as_bytes(self):
        return json.dumps(self.as_dict(), sort_keys=True).encode("utf-8")

    @classmethod
    def from_bytes(cls, byts):
        try:
            data = json.loads(byts.decode("utf-8"))
        except ValueError as exc:
            raise ObjectFormatError("ReferenceObject is corrupted") from exc

        return cls.from_dict(data)

    @classmethod
    def load(cls, path, fs):
        byts = fs.cat_file(path)

        obj = cls.from_bytes(byts)
        obj.path = path
        obj.fs = fs

        return obj

    def serialize(self):
        from dvc_objects.fs import MemoryFileSystem
        from dvc_objects.fs.utils import tmp_fname

        self.fs = MemoryFileSystem()
        self.path = "memory://{}".format(tmp_fname(""))

        try:
            self.fs.pipe_file(self.path, self.as_bytes())
        except OSError as exc:
            if isinstance(exc, FileExistsError) or (
                os.name == "nt"
                and exc.__context__
                and isinstance(exc.__context__, FileExistsError)
            ):
                logger.debug("'%s' file already exists, skipping", self.path)
            else:
                raise
