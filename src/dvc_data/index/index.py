from collections import defaultdict
from collections.abc import MutableMapping
from dataclasses import dataclass
from itertools import chain
from typing import TYPE_CHECKING, Dict, Iterable, Optional, Tuple

from dvc_objects.errors import ObjectFormatError
from pygtrie import ShortKeyError  # noqa: F401, pylint: disable=unused-import
from pygtrie import Trie

from ..hashfile.hash_info import HashInfo
from ..hashfile.meta import Meta
from ..hashfile.tree import Tree, TreeError

if TYPE_CHECKING:
    from dvc_objects.fs.base import FileSystem

    from ..hashfile.db import HashFileDB
    from ..hashfile.obj import HashFile


DataIndexKey = Tuple[str]


@dataclass
class DataIndexEntry:
    meta: Optional["Meta"] = None
    obj: Optional["HashFile"] = None
    hash_info: Optional["HashInfo"] = None
    odb: Optional["HashFileDB"] = None
    cache: Optional["HashFileDB"] = None
    remote: Optional["HashFileDB"] = None

    fs: Optional["FileSystem"] = None
    path: Optional["str"] = None

    loaded: Optional[bool] = None

    @classmethod
    def from_dict(cls, d: Dict[str, Dict]) -> "DataIndexEntry":
        ret = cls()

        meta = d.get("meta")
        if meta:
            ret.meta = Meta.from_dict(meta)

        hash_info = d.get("hash_info")
        if hash_info:
            ret.hash_info = HashInfo.from_dict(hash_info)

        return ret

    def to_dict(self) -> Dict[str, Dict]:
        ret = {}

        if self.meta:
            ret["meta"] = self.meta.to_dict()

        if self.hash_info:
            ret["hash_info"] = self.hash_info.to_dict()

        return ret


def _try_load(
    odbs: Iterable["HashFileDB"],
    hash_info: "HashInfo",
) -> Optional["HashFile"]:
    for odb in odbs:
        if not odb:
            continue

        try:
            return Tree.load(odb, hash_info)
        except (FileNotFoundError, ObjectFormatError):
            pass

    return None


class DataIndex(MutableMapping):
    def __init__(self, *args, **kwargs):
        self._trie = Trie(*args, **kwargs)

    def __setitem__(self, key, value):
        self._trie[key] = value

    def __getitem__(self, key):
        item = self._trie.get(key)
        if item:
            return item

        dir_key, dir_entry = self._trie.longest_prefix(key)
        self._load(dir_key, dir_entry)
        return self._trie[key]

    def __delitem__(self, key):
        del self._trie[key]

    def __iter__(self):
        return iter(self._trie)

    def __len__(self):
        return len(self._trie)

    def _load(self, key, entry):
        if not entry:
            return

        if entry.loaded:
            return

        if not entry.hash_info:
            return

        if not entry.hash_info.isdir:
            return

        if not entry.obj:
            entry.obj = _try_load([entry.odb, entry.remote], entry.hash_info)

        if not entry.obj:
            return

        for ikey, (meta, hash_info) in entry.obj.iteritems():
            if not meta and entry.hash_info and entry.hash_info == hash_info:
                meta = entry.meta
            self._trie[key + ikey] = DataIndexEntry(
                odb=entry.odb,
                cache=entry.odb,
                remote=entry.remote,
                hash_info=hash_info,
                meta=meta,
            )

        entry.loaded = True

    def load(self, **kwargs):
        for key, entry in self.iteritems(shallow=True, **kwargs):
            self._load(key, entry)

    def has_node(self, key):
        return self._trie.has_node(key)

    def shortest_prefix(self, *args, **kwargs):
        return self._trie.shortest_prefix(*args, **kwargs)

    def longest_prefix(self, *args, **kwargs):
        return self._trie.longest_prefix(*args, **kwargs)

    def traverse(self, *args, **kwargs):
        return self._trie.traverse(*args, **kwargs)

    def iteritems(self, prefix=None, shallow=False):
        kwargs = {"shallow": shallow}
        if prefix:
            kwargs = {"prefix": prefix}
            item = self._trie.longest_prefix(prefix)
            if item:
                key, entry = item
                self._load(key, entry)

        for key, entry in self._trie.iteritems(**kwargs):
            self._load(key, entry)
            yield key, entry

    def info(self, key):
        try:
            entry = self[key]
            isdir = entry.hash_info and entry.hash_info.isdir
            return {
                "type": "directory" if isdir else "file",
                "size": entry.meta.size if entry.meta else 0,
                "isexec": entry.meta.isexec if entry.meta else False,
                "isdvc": True,
                "isout": True,
                "obj": entry.obj,
                "entry": entry,
                entry.hash_info.name: entry.hash_info.value,
            }
        except ShortKeyError:
            return {
                "type": "directory",
                "size": 0,
                "isexec": False,
                "isdvc": bool(self.longest_prefix(key)),
                "isout": False,
                "obj": None,
                "entry": None,
            }
        except KeyError as exc:
            raise FileNotFoundError from exc

    def _ensure_loaded(self, prefix):
        entry = self._trie.get(prefix)
        if (
            entry
            and entry.hash_info
            and entry.hash_info.isdir
            and not entry.loaded
        ):
            self._load(prefix, entry)
            if not entry.obj:
                raise TreeError

    def ls(self, root_key, detail=True):
        self._ensure_loaded(root_key)
        if not detail:

            def node_factory(_, key, children, *args):
                if key == root_key:
                    return children
                else:
                    return key

        else:

            def node_factory(_, key, children, *args):
                if key == root_key:
                    return children
                else:
                    return key, self.info(key)

        return self.traverse(node_factory, prefix=root_key)


def _collect_dir(index, prefix, prefix_entry, path, fs, update=False):
    dir_meta = Meta(nfiles=0, size=0, isdir=True)

    for root, dnames, fnames in fs.walk(path):
        sub_prefix = fs.path.relparts(root, path) if root != path else ()
        for name in chain(dnames, fnames):
            key = (*prefix, *sub_prefix, name)
            entry_path = fs.path.join(root, name)
            entry = index.get(key)
            if entry is None:
                entry = DataIndexEntry()
                index[key] = entry

            entry.fs = fs
            entry.path = entry_path
            entry.cache = prefix_entry.cache
            entry.remote = prefix_entry.remote

            # TODO: localfs.walk doesn't currently support detail=True,
            # so we have to call fs.info() manually
            meta = Meta.from_info(
                fs.info(entry_path, refresh=True), fs.protocol
            )
            if entry.meta != meta and not update:
                entry.hash_info = None

            entry.meta = meta
            dir_meta.nfiles += 1
            dir_meta.size += meta.size

    return dir_meta


def collect(index, path, fs, update=False):
    # NOTE: converting to list to avoid iterating and modifying the dict the
    # same time.
    items = list(index.iteritems(shallow=True))
    for key, entry in items:
        entry_path = fs.path.join(path, *key)

        info = fs.info(entry_path, refresh=True)

        fs_meta = Meta.from_info(info, fs.protocol)
        if entry.meta != fs_meta and not update:
            entry.hash_info = None
        entry.meta = fs_meta
        entry.fs = fs
        entry.path = entry_path

        if info["type"] == "file":
            continue

        entry.meta = _collect_dir(index, key, entry, entry_path, fs)


def transfer(index, src, dst):
    from ..hashfile.transfer import transfer as otransfer

    by_direction = defaultdict(set)
    for _, entry in index.iteritems():
        src_odb = getattr(entry, src)
        assert src_odb
        dst_odb = getattr(entry, dst)
        assert dst_odb
        by_direction[(src_odb, dst_odb)].add(entry.hash_info)

    for (src_odb, dst_odb), hash_infos in by_direction.items():
        otransfer(src_odb, dst_odb, hash_infos)


def commit(index):
    transfer(index, "odb", "cache")


def push(index):
    transfer(index, "cache", "remote")


def fetch(index):
    transfer(index, "remote", "cache")
