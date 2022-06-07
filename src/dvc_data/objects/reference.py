import json
import logging
import os
from dataclasses import dataclass
from typing import TYPE_CHECKING

from dvc_objects.errors import ObjectFormatError
from dvc_objects.fs import FS_MAP, LocalFileSystem
from dvc_objects.hashfile.obj import HashFile

if TYPE_CHECKING:
    from dvc_objects.fs.base import AnyFSPath, FileSystem
    from dvc_objects.hashfile.hash_info import HashInfo

logger = logging.getLogger(__name__)


@dataclass
class Reference:
    fs_path: "AnyFSPath"
    fs: "FileSystem"
    checksum: int


class ReferenceHashFile(HashFile):
    PARAM_PATH = "path"
    PARAM_HASH = "hash"
    PARAM_CHECKSUM = "checksum"
    PARAM_FS_CONFIG = "fs_config"
    PARAM_FS_CLS = "fs_name"

    def __init__(
        self,
        fs_path: "AnyFSPath",
        fs: "FileSystem",
        hash_info: "HashInfo",
        ref: "Reference",
    ):
        super().__init__(fs_path, fs, hash_info)
        self.ref = ref

    def deref(self):
        return HashFile(self.ref.fs_path, self.ref.fs, self.hash_info)

    @staticmethod
    def config_tuple(fs: "FileSystem"):
        return (
            fs.protocol,
            tuple(
                (key, value)
                for key, value in sorted(
                    fs.config.items(), key=lambda item: item[0]
                )
            ),
        )

    def as_dict(self):
        # NOTE: dumping reference FS's this way is insecure, as the
        # fully parsed remote FS config will include credentials
        #
        # ReferenceHashFiles should currently only be serialized in
        # memory and not to disk
        fs_cls = type(self.ref.fs)
        mod = None
        if fs_cls not in FS_MAP.values() and fs_cls != LocalFileSystem:
            mod = ".".join((fs_cls.__module__, fs_cls.__name__))

        fs_config = self.config_tuple(self.ref.fs)

        return {
            self.PARAM_PATH: self.ref.fs_path,
            self.PARAM_HASH: self.hash_info.to_dict(),
            self.PARAM_CHECKSUM: self.ref.checksum,
            self.PARAM_FS_CONFIG: fs_config,
            self.PARAM_FS_CLS: mod,
        }

    @classmethod
    def from_dict(cls, dict_, fs_cache=None):
        from dvc_objects.fs import get_fs_cls
        from dvc_objects.hashfile.hash_info import HashInfo

        try:
            fs_path = dict_[cls.PARAM_PATH]
        except KeyError as exc:
            raise ObjectFormatError("ReferenceHashFile is corrupted") from exc

        protocol, config_pairs = dict_.get(cls.PARAM_FS_CONFIG)
        fs = fs_cache.get((protocol, config_pairs)) if fs_cache else None
        if not fs:
            config = dict(config_pairs)
            mod = dict_.get(cls.PARAM_FS_CLS, None)
            fs_cls = get_fs_cls(config, cls=mod, scheme=protocol)
            fs = fs_cls(**config)

        checksum = dict_.get(cls.PARAM_CHECKSUM)
        hash_info = HashInfo.from_dict(dict_.get(cls.PARAM_HASH))

        ref = Reference(fs_path, fs, checksum)

        return cls(None, None, hash_info, ref)

    def as_bytes(self):
        return json.dumps(self.as_dict(), sort_keys=True).encode("utf-8")

    @classmethod
    def from_bytes(cls, byts, **kwargs):
        try:
            data = json.loads(byts.decode("utf-8"))
        except ValueError as exc:
            raise ObjectFormatError("ReferenceHashFile is corrupted") from exc

        return cls.from_dict(data, **kwargs)

    @classmethod
    def from_raw(cls, raw, **kwargs):
        byts = raw.fs.cat_file(raw.fs_path)

        obj = cls.from_bytes(byts, **kwargs)
        obj.fs_path = raw.fs_path
        obj.fs = raw.fs

        if raw.hash_info != obj.hash_info:
            raise ObjectFormatError(
                "ReferenceHashFile is corrupted: hash mismatch "
                f"(raw: {raw.hash_info}, obj: {obj.hash_info})"
            )

        return obj

    @classmethod
    def from_path(cls, fs_path, fs, hash_info, fs_cache=None):
        from dvc_objects.fs import MemoryFileSystem, Schemes
        from dvc_objects.fs.utils import tmp_fname

        checksum = fs.checksum(fs_path)
        ref = Reference(fs_path, fs, checksum)

        if fs_cache and fs.protocol != Schemes.LOCAL:
            fs_cache[cls.config_tuple(fs)] = fs

        memfs = MemoryFileSystem()
        memfs_path = "memory://{}".format(tmp_fname(""))

        obj = cls(memfs_path, memfs, hash_info, ref)

        try:
            obj.fs.pipe_file(obj.fs_path, obj.as_bytes())
        except OSError as exc:
            if isinstance(exc, FileExistsError) or (
                os.name == "nt"
                and exc.__context__
                and isinstance(exc.__context__, FileExistsError)
            ):
                logger.debug("'%s' file already exists, skipping", obj.fs_path)
            else:
                raise

        return obj

    def check(self, *args, **kwargs):
        if not self.fs.exists(self.fs_path):
            raise FileNotFoundError

        actual = self.ref.fs.checksum(self.ref.fs_path)
        if self.ref.checksum != actual:
            raise ObjectFormatError(f"{self} is changed")
