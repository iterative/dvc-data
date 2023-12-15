import os
from typing import Optional

from dvc_objects.fs import localfs
from dvc_objects.fs.base import FileSystem

from .index import DataIndex


class NotARepoError(Exception):
    pass


class Repo:
    def __init__(self, root: str = "", fs: Optional[FileSystem] = None) -> None:
        fs = fs or localfs
        root = root or fs.getcwd()
        control_dir: str = os.getenv("DVC_DIR") or fs.join(root, ".dvc")

        if not fs.isdir(control_dir):
            raise NotARepoError(f"{root} is not a data repo.")

        self.fs = fs or localfs
        self.root = root
        self._control_dir = control_dir
        self._tmp_dir: str = fs.join(self._control_dir, "tmp")
        self._object_dir: str = fs.join(self._control_dir, "cache")

        self.index = DataIndex()

    @classmethod
    def discover(
        cls,
        start: str = ".",
        fs: Optional[FileSystem] = None,
    ) -> "Repo":
        remaining = start
        fs = fs or localfs
        path = start = fs.abspath(start)
        while remaining:
            try:
                return cls(path, fs)
            except NotARepoError:
                path, remaining = fs.split(path)
        raise NotARepoError(f"No data repository was found at {start}")

    @property
    def control_dir(self):
        return self._control_dir

    @property
    def tmp_dir(self):
        return self._tmp_dir

    @property
    def object_dir(self):
        return self._object_dir
