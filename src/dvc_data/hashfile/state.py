"""Manages state database used for checksum caching."""
import json
import logging
import os
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

from dvc_objects.fs import LocalFileSystem
from dvc_objects.fs.system import inode as get_inode
from dvc_objects.fs.utils import relpath

from .hash_info import HashInfo
from .utils import get_mtime_and_size

if TYPE_CHECKING:
    from ._ignore import Ignore


logger = logging.getLogger(__name__)


class StateBase(ABC):
    @abstractmethod
    def close(self):
        pass

    @abstractmethod
    def save(self, path, fs, hash_info):
        pass

    @abstractmethod
    def get(self, path, fs):
        pass

    @abstractmethod
    def save_link(self, path, fs):
        pass


class StateNoop(StateBase):
    def close(self):
        pass

    def save(self, path, fs, hash_info):
        pass

    def get(self, path, fs):  # pylint: disable=unused-argument
        return None, None

    def save_link(self, path, fs):
        pass


class State(StateBase):  # pylint: disable=too-many-instance-attributes
    def __init__(self, root_dir=None, tmp_dir=None, ignore: "Ignore" = None):
        from .cache import Cache

        super().__init__()

        self.tmp_dir = tmp_dir
        self.root_dir = root_dir
        self.ignore = ignore

        if not tmp_dir:
            return

        links_dir = os.path.join(tmp_dir, "links")
        hashes_dir = os.path.join(tmp_dir, "hashes", "local")
        self.links = Cache(links_dir)
        self.hashes = Cache(hashes_dir)

    def close(self):
        self.hashes.close()
        self.links.close()

    def save(self, path, fs, hash_info):
        """Save hash for the specified path info.

        Args:
            path (str): path to save hash for.
            hash_info (HashInfo): hash to save.
        """

        if not isinstance(fs, LocalFileSystem):
            return

        entry = {
            "checksum": fs.checksum(path),
            "hash_info": hash_info.to_dict(),
        }

        self.hashes[path] = json.dumps(entry)

    def get(self, path, fs):
        """Gets the hash for the specified path info. Hash will be
        retrieved from the state database if available.

        Args:
            path (str): path info to get the hash for.

        Returns:
            HashInfo or None: hash for the specified path info or None if it
            doesn't exist in the state database.
        """
        from .meta import Meta

        if not isinstance(fs, LocalFileSystem):
            return None, None

        raw = self.hashes.get(path)
        if not raw:
            return None, None

        try:
            entry = json.loads(raw)
        except ValueError:
            return None, None

        try:
            actual = fs.checksum(path)
        except FileNotFoundError:
            return None, None

        if entry["checksum"] != actual:
            return None, None

        return Meta(), HashInfo.from_dict(entry["hash_info"])

    def save_link(self, path, fs):
        """Adds the specified path to the list of links created by dvc. This
        list is later used on `dvc checkout` to cleanup old links.

        Args:
            path (str): path info to add to the list of links.
        """
        if not isinstance(fs, LocalFileSystem):
            return

        try:
            mtime, _ = get_mtime_and_size(path, fs, self.ignore)
        except FileNotFoundError:
            return

        inode = get_inode(path)
        relative_path = relpath(path, self.root_dir)

        with self.links as ref:
            ref[relative_path] = (inode, mtime)

    def get_unused_links(self, used, fs):
        """Removes all saved links except the ones that are used.

        Args:
            used (list): list of used links that should not be removed.
        """
        if not isinstance(fs, LocalFileSystem):
            return

        unused = []

        with self.links as ref:
            for relative_path in ref:
                path = os.path.join(self.root_dir, relative_path)

                if path in used or not fs.exists(path):
                    continue

                inode = get_inode(path)
                mtime, _ = get_mtime_and_size(path, fs, self.ignore)

                if ref[relative_path] == (inode, mtime):
                    logger.debug("Removing '%s' as unused link.", path)
                    unused.append(relative_path)

        return unused

    def remove_links(self, unused, fs):
        if not isinstance(fs, LocalFileSystem):
            return

        for path in unused:
            fs.remove(os.path.join(self.root_dir, path))

        with self.links as ref:
            for path in unused:
                del ref[path]
