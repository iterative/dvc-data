import json
from collections import defaultdict
from collections.abc import MutableMapping
from dataclasses import dataclass
from itertools import chain
from typing import TYPE_CHECKING, Dict, Iterable, List, Optional, Tuple

from dvc_objects.errors import ObjectFormatError
from pygtrie import ShortKeyError  # noqa: F401, pylint: disable=unused-import
from pygtrie import Trie

from .hashfile.cache import Cache
from .hashfile.hash_info import HashInfo
from .hashfile.meta import Meta
from .hashfile.tree import Tree, TreeError

if TYPE_CHECKING:
    from dvc_objects.fs.base import FileSystem

    from .hashfile.db import HashFileDB
    from .hashfile.obj import HashFile


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


def md5(index):
    from .hashfile.hash import file_md5

    for _, entry in index.iteritems():
        if entry.meta.isdir:
            continue

        if entry.meta.version_id and entry.fs.version_aware:
            # NOTE: if we have versioning available - there is no need to check
            # metadata as we can directly get correct file content using
            # version_id.
            path = entry.fs.path.version_path(
                entry.path, entry.meta.version_id
            )
        else:
            path = entry.path

        entry.hash_info = HashInfo("md5", file_md5(path, entry.fs))


def save(index):
    dir_entries: List[DataIndexKey] = []

    for key, entry in index.iteritems():
        if entry.meta.isdir:
            dir_entries.append(key)
            continue

        if entry.meta.version_id and entry.fs.version_aware:
            # NOTE: if we have versioning available - there is no need to check
            # metadata as we can directly get correct file content using
            # version_id.
            path = entry.fs.path.version_path(
                entry.path, entry.meta.version_id
            )
        else:
            path = entry.path

        if entry.hash_info:
            entry.cache.add(
                path,
                entry.fs,
                entry.hash_info.value,
            )

    for key in dir_entries:
        _save_dir_entry(index, key)


def _save_dir_entry(index: DataIndex, key: DataIndexKey):
    from .hashfile.db import add_update_tree
    from .hashfile.tree import tree_from_index

    entry = index[key]
    assert entry.cache
    meta, tree = tree_from_index(index, key)
    tree = add_update_tree(entry.cache, tree)
    entry.meta = meta
    entry.hash_info = tree.hash_info
    assert tree.hash_info.name and tree.hash_info.value
    setattr(entry.meta, tree.hash_info.name, tree.hash_info.value)


def build(index, path, fs, **kwargs):
    from .hashfile.build import build as obuild

    # NOTE: converting to list to avoid iterating and modifying the dict the
    # same time.
    items = list(index.iteritems(shallow=True))
    for key, entry in items:
        if entry and entry.hash_info and entry.hash_info.isdir:
            del index[key:]

        try:
            odb, meta, obj = obuild(
                entry.odb,
                fs.path.join(path, *key),
                fs,
                entry.odb.hash_name,
                **kwargs,
            )
            hash_info = obj.hash_info
        except FileNotFoundError:
            meta = None
            obj = None
            hash_info = None

        entry.odb = odb
        entry.meta = meta
        entry.obj = obj
        entry.hash_info = hash_info

        index[key] = entry
    index.load()


def checkout(index, path, fs, **kwargs):
    from .hashfile import load
    from .hashfile.checkout import checkout as ocheckout

    for key, entry in index.iteritems():
        if not entry.obj:
            entry.obj = load(entry.odb, entry.hash_info)
        ocheckout(fs.path.join(path, *key), fs, entry.obj, entry.odb, **kwargs)


def transfer(index, src, dst):
    from .hashfile.transfer import transfer as otransfer

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


def write_db(index, path):
    cache = Cache(path)
    with cache.transact():
        for key, entry in index.iteritems():
            cache["/".join(key)] = entry.to_dict()


def read_db(path):
    index = DataIndex()
    cache = Cache(path)

    with cache.transact():
        for key in cache:
            value = cache.get(key)
            index[key.split("/")] = DataIndexEntry.from_dict(value)

    return index


def write_json(index, path):
    with open(path, "w", encoding="utf-8") as fobj:
        json.dump(
            {
                "/".join(key): entry.to_dict()
                for key, entry in index.iteritems()
            },
            fobj,
        )


def read_json(path):
    index = DataIndex()

    with open(path, "r", encoding="utf-8") as fobj:
        for key, value in json.load(fobj).items():
            index[key.split("/")] = DataIndexEntry.from_dict(value)

    return index
