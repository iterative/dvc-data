import os

from dvc_objects.fs import localfs
from dvc_objects.fs.base import FileSystem

from .index import DataIndex


class NotARepo(Exception):
    pass


class Repo:
    def __init__(self, root: str = "", fs: FileSystem = None) -> None:
        fs = fs or localfs
        root = root or fs.path.getcwd()
        control_dir: str = os.getenv("DVC_DIR") or fs.path.join(root, ".dvc")

        if not fs.isdir(control_dir):
            raise NotARepo(f"{root} is not a data repo.")

        self.fs = fs or localfs
        self.root = root
        self._control_dir = control_dir
        self._tmp_dir: str = fs.path.join(self._control_dir, "tmp")
        self._object_dir: str = fs.path.join(self._control_dir, "cache")

        self.index = DataIndex()

    @classmethod
    def discover(
        cls,
        start: str = ".",
        fs: FileSystem = None,
    ) -> "Repo":
        remaining = True
        fs = fs or localfs
        path = start = fs.path.abspath(start)
        while remaining:
            try:
                return cls(path, fs)
            except NotARepo:
                path, remaining = fs.path.split(path)
        raise NotARepo(f"No data repository was found at {start}")

    @property
    def control_dir(self):
        return self._control_dir

    @property
    def tmp_dir(self):
        return self._tmp_dir

    @property
    def object_dir(self):
        return self._object_dir
