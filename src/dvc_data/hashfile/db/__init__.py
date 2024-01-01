import errno
import logging
import os
from contextlib import suppress
from copy import copy
from typing import TYPE_CHECKING, Callable, ClassVar, List, Optional, Union

from dvc_objects.db import ObjectDB
from dvc_objects.errors import ObjectFormatError
from dvc_objects.fs.callbacks import DEFAULT_CALLBACK

from dvc_data.hashfile.hash_info import HashInfo
from dvc_data.hashfile.obj import HashFile

if TYPE_CHECKING:
    from dvc_objects.fs.base import AnyFSPath, FileSystem
    from dvc_objects.fs.callbacks import Callback

    from dvc_data.hashfile.tree import Tree

    from .index import ObjectDBIndexBase


logger = logging.getLogger(__name__)


def get_odb(fs, path, **config):
    from dvc_objects.fs import Schemes

    from .local import LocalHashFileDB

    if fs.protocol == Schemes.LOCAL:
        return LocalHashFileDB(fs, path, **config)

    return HashFileDB(fs, path, **config)


def get_index(odb) -> "ObjectDBIndexBase":
    import hashlib

    from .index import ObjectDBIndex, ObjectDBIndexNoop

    cls = ObjectDBIndex if odb.tmp_dir else ObjectDBIndexNoop
    return cls(
        odb.tmp_dir,
        hashlib.sha256(odb.fs.unstrip_protocol(odb.path).encode("utf-8")).hexdigest(),
    )


class HashFileDB(ObjectDB):
    DEFAULT_VERIFY = False
    DEFAULT_CACHE_TYPES: ClassVar[List[str]] = ["copy"]
    CACHE_MODE: Optional[int] = None

    def __init__(self, fs: "FileSystem", path: str, read_only: bool = False, **config):
        from dvc_data.hashfile.state import StateNoop

        super().__init__(fs, path, read_only=read_only)
        self.state = config.get("state", StateNoop())
        self.verify = config.get("verify", self.DEFAULT_VERIFY)
        self.cache_types = config.get("type") or copy(self.DEFAULT_CACHE_TYPES)
        self.slow_link_warning = config.get("slow_link_warning", True)
        self.tmp_dir = config.get("tmp_dir")
        self.hash_name = config.get("hash_name", self.fs.PARAM_CHECKSUM)

    def get(self, oid: str) -> HashFile:
        return HashFile(
            self.oid_to_path(oid),
            self.fs,
            HashInfo(self.hash_name, oid),
        )

    def add(
        self,
        path: Union["AnyFSPath", List["AnyFSPath"]],
        fs: "FileSystem",
        oid: Union[str, List[str]],
        hardlink: bool = False,
        callback: "Callback" = DEFAULT_CALLBACK,
        check_exists: bool = True,
        on_error: Optional[Callable[[str, BaseException], None]] = None,
        **kwargs,
    ) -> int:
        verify = kwargs.get("verify")
        if verify is None:
            verify = self.verify

        paths = [path] if isinstance(path, str) else path
        oids = [oid] if isinstance(oid, str) else oid
        assert len(paths) == len(oids)

        if verify:
            for oid in oids:
                try:
                    self.check(oid, check_hash=True)
                except (ObjectFormatError, FileNotFoundError):
                    pass

        transferred = super().add(
            paths,
            fs,
            oids,
            hardlink=hardlink,
            callback=callback,
            check_exists=check_exists,
            on_error=on_error,
            **kwargs,
        )

        for oid in oids:
            cache_path = self.oid_to_path(oid)
            try:
                if verify:
                    self.check(oid, check_hash=True)
                self.protect(cache_path)
                self.state.save(
                    cache_path,
                    self.fs,
                    HashInfo(name=self.hash_name, value=oid),
                )
            except (ObjectFormatError, FileNotFoundError):
                pass
        return transferred

    def protect(self, path):
        pass

    def is_protected(self, path):
        return False

    def unprotect(self, path):
        pass

    def set_exec(self, path):
        pass

    def check(
        self,
        oid: str,
        check_hash: bool = True,
    ):
        """Compare the given hash with the (corresponding) actual one if
        check_hash is specified, or just verify the existence of the cache
        files on the filesystem.

        - Use `State` as a cache for computed hashes
            + The entries are invalidated by taking into account the following:
                * mtime
                * inode
                * size
                * hash

        - Remove the file from cache if it doesn't match the actual hash
        """
        from dvc_data.hashfile.hash import hash_file

        obj = self.get(oid)
        if self.is_protected(obj.path):
            return

        if not check_hash:
            assert obj.fs
            if not obj.fs.exists(obj.path):
                raise FileNotFoundError(
                    errno.ENOENT, os.strerror(errno.ENOENT), obj.path
                )
            return

        _, actual = hash_file(obj.path, obj.fs, self.hash_name, self.state)

        assert actual.name == self.hash_name
        assert actual.value
        if actual.value.split(".")[0] != oid.split(".")[0]:
            logger.debug("corrupted cache file '%s'.", obj.path)
            with suppress(FileNotFoundError):
                self.fs.remove(obj.path)

            raise ObjectFormatError(f"{obj} is corrupted")

        if check_hash:
            # making cache file read-only so we don't need to check it
            # next time
            self.protect(obj.path)

    def _remove_unpacked_dir(self, hash_):
        pass


def add_update_tree(odb: HashFileDB, tree: "Tree") -> "Tree":
    """Add tree to ODB and update fs/path to use ODB fs/path."""
    assert tree.oid
    odb.add(tree.path, tree.fs, tree.oid, hardlink=False)
    raw = odb.get(tree.oid)
    tree.fs = raw.fs
    tree.path = raw.path
    return tree
