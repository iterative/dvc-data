import logging
import os
import stat
from functools import partial

from dvc_objects.db import noop, wrap_iter
from dvc_objects.errors import ObjectDBError, ObjectFormatError
from dvc_objects.fs.system import umask
from dvc_objects.fs.utils import copyfile, relpath, remove
from shortuuid import uuid

from . import HashFileDB

logger = logging.getLogger(__name__)


class LocalHashFileDB(HashFileDB):
    DEFAULT_CACHE_TYPES = ["reflink", "copy"]
    CACHE_MODE = 0o444
    UNPACKED_DIR_SUFFIX = ".unpacked"

    def __init__(self, fs, path, **config):
        super().__init__(fs, path, **config)

        shared = config.get("shared")
        if shared:
            self._file_mode = 0o664
            self._dir_mode = 0o2775
        else:
            self._file_mode = 0o666 & ~umask
            self._dir_mode = 0o777 & ~umask

    def move(self, from_info, to_info):
        super().move(from_info, to_info)
        os.chmod(to_info, self._file_mode)

    def makedirs(self, path):
        from dvc_objects.fs.utils import makedirs

        makedirs(path, exist_ok=True, mode=self._dir_mode)

    def oid_to_path(self, oid):
        # NOTE: `self.path` is already normalized so we can simply use
        # `os.sep` instead of `os.path.join`. This results in this helper
        # being ~5.5 times faster.
        return f"{self.path}{os.sep}{oid[0:2]}{os.sep}{oid[2:]}"

    def oids_exist(
        self, oids, jobs=None, progress=noop
    ):  # pylint: disable=unused-argument
        ret = []
        progress = partial(progress, "querying", len(oids))

        for oid in wrap_iter(oids, progress):
            try:
                self.check(oid)
                ret.append(oid)
            except (FileNotFoundError, ObjectFormatError):
                pass

        return ret

    def _list_paths(self, prefix=None):
        assert self.path is not None
        if prefix:
            path = self.fs.path.join(self.path, prefix[:2])
            if not self.fs.exists(path):
                return
        else:
            path = self.path
        yield from self.fs.find(path)

    def _remove_unpacked_dir(self, hash_):
        hash_path = self.oid_to_path(hash_)
        path = self.fs.path.with_name(
            hash_path,
            self.fs.path.name(hash_path) + self.UNPACKED_DIR_SUFFIX,
        )
        self.fs.remove(path)

    def _unprotect_file(self, path):
        if self.fs.is_symlink(path) or self.fs.is_hardlink(path):
            logger.debug("Unprotecting '%s'", path)
            tmp = os.path.join(os.path.dirname(path), "." + uuid())

            # The operations order is important here - if some application
            # would access the file during the process of copyfile then it
            # would get only the part of file. So, at first, the file should be
            # copied with the temporary name, and then original file should be
            # replaced by new.
            copyfile(path, tmp, name=f"Unprotecting '{relpath(path)}'")
            remove(path)
            os.rename(tmp, path)

        else:
            logger.debug(
                "Skipping copying for '%s', since it is not a symlink or a hardlink.",
                path,
            )

        os.chmod(path, self._file_mode)

    def _unprotect_dir(self, path):
        for fname in self.fs.find(path):
            self._unprotect_file(fname)

    def unprotect(self, path):
        if not os.path.exists(path):
            raise ObjectDBError(f"can't unprotect non-existing data '{path}'")

        if os.path.isdir(path):
            self._unprotect_dir(path)
        else:
            self._unprotect_file(path)

    def protect(self, path):
        try:
            os.chmod(path, self.CACHE_MODE)
        except OSError:
            # NOTE: not being able to protect cache file is not fatal, it
            # might happen on funky filesystems (e.g. Samba, see #5255),
            # read-only filesystems or in a shared cache scenario.
            logger.debug("failed to protect '%s'", path, exc_info=True)

    def is_protected(self, path):
        try:
            mode = os.stat(path).st_mode
        except FileNotFoundError:
            return False

        return stat.S_IMODE(mode) == self.CACHE_MODE

    def set_exec(self, path):
        mode = os.stat(path).st_mode | stat.S_IEXEC
        try:
            os.chmod(path, mode)
        except OSError:
            logger.debug(
                "failed to chmod '%s' '%s'",
                oct(mode),
                path,
                exc_info=True,
            )
