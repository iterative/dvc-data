from abc import ABC, abstractmethod
from collections import defaultdict
from dataclasses import dataclass
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    Iterable,
    Iterator,
    Mapping,
    MutableMapping,
    Optional,
    Tuple,
)

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


@dataclass(unsafe_hash=True)
class DataIndexEntry:
    key: Optional[DataIndexKey] = None
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


class BaseDataIndex(ABC, Mapping[DataIndexKey, DataIndexEntry]):
    @abstractmethod
    def iteritems(
        self, prefix: Optional[DataIndexKey] = None, shallow: bool = False
    ) -> Iterator[Tuple[DataIndexKey, DataIndexEntry]]:
        pass

    @abstractmethod
    def traverse(self, node_factory: Callable, **kwargs) -> Any:
        pass

    @abstractmethod
    def has_node(self, key: DataIndexKey) -> bool:
        pass

    @abstractmethod
    def longest_prefix(
        self, key: DataIndexKey
    ) -> Tuple[Optional[DataIndexKey], Optional[DataIndexEntry]]:
        pass

    def ls(self, root_key: DataIndexKey, detail=True):
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

    def info(self, key: DataIndexKey):
        try:
            entry = self[key]
            isdir = entry.meta and entry.meta.isdir
            ret = {
                "type": "directory" if isdir else "file",
                "size": entry.meta.size if entry.meta else 0,
                "isexec": entry.meta.isexec if entry.meta else False,
                "isdvc": True,
                "isout": True,
                "obj": entry.obj,
                "entry": entry,
            }

            if entry.hash_info:
                assert entry.hash_info.name
                ret[entry.hash_info.name] = entry.hash_info.value

            return ret
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


class DataIndex(BaseDataIndex, MutableMapping[DataIndexKey, DataIndexEntry]):
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

    def add(self, entry: DataIndexEntry):
        self[entry.key] = entry

    def _load(self, key, entry):
        if not entry:
            return

        if entry.loaded:
            return

        if not entry.hash_info and not entry.obj:
            return

        if not (
            entry.hash_info.isdir
            or (entry.meta is not None and entry.meta.isdir)
        ):
            return

        if not entry.obj:
            entry.obj = _try_load([entry.odb, entry.remote], entry.hash_info)

        if not entry.obj:
            return

        dirs = set()
        for ikey, (meta, hash_info) in entry.obj.iteritems():
            if not meta and entry.hash_info and entry.hash_info == hash_info:
                meta = entry.meta

            if len(ikey) >= 2:
                # NOTE: current .dir obj format doesn't include subdirs, so
                # we need to create entries for them manually.
                for idx in range(1, len(ikey)):
                    dirs.add(ikey[:-idx])

            entry_key = key + ikey
            child_entry = DataIndexEntry(
                key=entry_key,
                odb=entry.odb,
                cache=entry.odb,
                remote=entry.remote,
                hash_info=hash_info,
                meta=meta,
            )
            if entry.fs and entry.path:
                child_entry.fs = entry.fs
                child_entry.path = entry.fs.path.join(entry.path, *ikey)
            self._trie[entry_key] = child_entry

        for dkey in dirs:
            entry_key = key + dkey
            self._trie[entry_key] = DataIndexEntry(
                key=entry_key,
                odb=entry.odb,
                cache=entry.odb,
                remote=entry.remote,
                meta=Meta(isdir=True),
            )

        entry.loaded = True

    def load(self, **kwargs):
        for key, entry in self.iteritems(shallow=True, **kwargs):
            self._load(key, entry)

    def has_node(self, key: DataIndexKey) -> bool:
        return self._trie.has_node(key)

    def shortest_prefix(self, *args, **kwargs):
        return self._trie.shortest_prefix(*args, **kwargs)

    def longest_prefix(
        self, key: DataIndexKey
    ) -> Tuple[Optional[DataIndexKey], Optional[DataIndexEntry]]:
        return self._trie.longest_prefix(key)

    def traverse(self, *args, **kwargs) -> Any:
        return self._trie.traverse(*args, **kwargs)

    def iteritems(
        self, prefix: Optional[DataIndexKey] = None, shallow: bool = False
    ) -> Iterator[Tuple[DataIndexKey, DataIndexEntry]]:
        kwargs: Dict[str, Any] = {"shallow": shallow}
        if prefix:
            kwargs = {"prefix": prefix}
            item = self._trie.longest_prefix(prefix)
            if item:
                key, entry = item
                self._load(key, entry)

        for key, entry in self._trie.iteritems(**kwargs):
            self._load(key, entry)
            yield key, entry

    def iterkeys(self, *args, **kwargs):
        return self._trie.iterkeys(*args, **kwargs)

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

    def ls(self, root_key: DataIndexKey, detail=True):
        self._ensure_loaded(root_key)
        return super().ls(root_key, detail=detail)


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
