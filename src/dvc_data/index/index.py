import errno
import logging
import os
from abc import ABC, abstractmethod
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    Iterator,
    MutableMapping,
    Optional,
    Tuple,
    cast,
)

import attrs
from sqltrie import ShortKeyError  # noqa: F401, pylint: disable=unused-import
from sqltrie import JSONTrie, PyGTrie, SQLiteTrie

from dvc_data.utils import cached_property

from ..hashfile.hash_info import HashInfo
from ..hashfile.meta import Meta
from ..hashfile.tree import Tree

if TYPE_CHECKING:
    from dvc_objects.fs.base import FileSystem

    from ..hashfile.db import HashFileDB


logger = logging.getLogger(__name__)

DataIndexKey = Tuple[str, ...]


@attrs.define(hash=True)
class DataIndexEntry:
    key: Optional[DataIndexKey] = None
    meta: Optional["Meta"] = None
    hash_info: Optional["HashInfo"] = None

    loaded: Optional[bool] = None

    @property
    def isdir(self) -> bool:
        if self.meta:
            return self.meta.isdir

        return False

    @classmethod
    def from_dict(cls, d: Dict[str, Dict]) -> "DataIndexEntry":
        ret = cls()

        meta = d.get("meta")
        if meta:
            ret.meta = Meta.from_dict(meta)

        hash_info = d.get("hash_info")
        if hash_info:
            ret.hash_info = HashInfo.from_dict(hash_info)

        ret.loaded = cast(bool, d["loaded"])

        return ret

    def to_dict(self) -> Dict[str, Any]:
        ret: Dict[str, Any] = {}

        if self.meta:
            ret["meta"] = self.meta.to_dict()

        if self.hash_info:
            ret["hash_info"] = self.hash_info.to_dict()

        ret["loaded"] = self.loaded

        return ret


class DataIndexTrie(JSONTrie):
    def __init__(self, *args, **kwargs):
        self._cache = {}
        super().__init__(*args, **kwargs)

    @cached_property
    def _trie(self):
        return SQLiteTrie()

    @classmethod
    def open(cls, path):
        ret = cls()
        ret._trie = SQLiteTrie.open(path)
        return ret

    def _load(self, key, value):
        try:
            return self._cache[key]
        except KeyError:
            pass
        if value is None:
            return None

        d = super()._load(key, value)
        assert isinstance(d, dict)
        entry = DataIndexEntry.from_dict(d)
        entry.key = key
        return entry

    def _dump(self, key, value):
        if key not in self._cache:
            self._cache[key] = value
        if value is None:
            return None
        return super()._dump(key, value.to_dict())

    def __setitem__(self, key, value):
        self._cache.pop(key, None)
        super().__setitem__(key, value)

    def __delitem__(self, key):
        self._cache.pop(key, None)
        super().__delitem__(key)

    def delete_node(self, key):
        self._cache.pop(key, None)
        super().delete_node(key)


class Storage(ABC):
    def __init__(self, key: "DataIndexKey"):
        self.key = key

    @property
    @abstractmethod
    def fs(self):
        pass

    @property
    @abstractmethod
    def path(self):
        pass

    @abstractmethod
    def get_key(self, entry: "DataIndexEntry") -> "DataIndexKey":
        pass

    @abstractmethod
    def get(self, entry: "DataIndexEntry") -> Tuple["FileSystem", str]:
        pass

    def exists(self, entry: "DataIndexEntry") -> bool:
        fs, path = self.get(entry)
        return fs.exists(path)


class ObjectStorage(Storage):
    def __init__(
        self,
        key: "DataIndexKey",
        odb: "HashFileDB",
        index: Optional["DataIndex"] = None,
    ):
        self.odb = odb
        self.index = index
        super().__init__(key)

    @property
    def fs(self):
        return self.odb.fs

    @property
    def path(self):
        return self.odb.path

    def get_key(self, entry: "DataIndexEntry") -> "DataIndexKey":
        if not entry.hash_info or not entry.hash_info.value:
            raise ValueError

        return self.odb._oid_parts(  # pylint: disable=protected-access
            entry.hash_info.value
        )

    def get(self, entry: "DataIndexEntry") -> Tuple["FileSystem", str]:
        if not entry.hash_info:
            raise ValueError

        return self.odb.fs, self.odb.oid_to_path(entry.hash_info.value)

    def exists(self, entry: "DataIndexEntry", refresh: bool = False) -> bool:
        if not entry.hash_info:
            return False

        value = cast(str, entry.hash_info.value)

        if self.index is None:
            return self.odb.exists(value)

        key = self.odb._oid_parts(value)  # pylint: disable=protected-access
        if not refresh:
            return key in self.index

        try:
            from .build import build_entry

            fs, path = self.get(entry)
            self.index[key] = build_entry(path, fs)
            return True
        except FileNotFoundError:
            self.index.pop(key, None)
            return False
        finally:
            self.index.commit()


class FileStorage(Storage):
    def __init__(
        self,
        key: "DataIndexKey",
        fs: "FileSystem",
        path: "str",
        index: Optional["DataIndex"] = None,
        prefix: Optional["DataIndexKey"] = None,
    ):
        self._fs = fs
        self._path = path
        self.index = index
        self.prefix = prefix if prefix is not None else key
        super().__init__(key)

    @property
    def fs(self):
        return self._fs

    @property
    def path(self):
        return self._path

    def get_key(self, entry: "DataIndexEntry") -> "DataIndexKey":
        assert entry.key
        assert entry.key[: len(self.prefix)] == self.prefix
        return entry.key[len(self.prefix) :]

    def get(self, entry: "DataIndexEntry") -> Tuple["FileSystem", str]:
        assert entry.key is not None
        assert entry.key[: len(self.prefix)] == self.prefix
        path = self.fs.path.join(self.path, *entry.key[len(self.prefix) :])
        if self.fs.version_aware and entry.meta and entry.meta.version_id:
            path = self.fs.path.version_path(path, entry.meta.version_id)
        return self.fs, path

    def exists(self, entry: "DataIndexEntry", refresh: bool = False) -> bool:
        if self.index is None:
            return super().exists(entry)

        assert entry.key
        assert entry.key[: len(self.prefix)] == self.prefix
        key = entry.key[len(self.prefix) :]
        if not refresh:
            return key in self.index

        try:
            from .build import build_entry

            fs, path = self.get(entry)
            self.index[key] = build_entry(path, fs)
            return True
        except FileNotFoundError:
            self.index.pop(key, None)
            return False
        finally:
            self.index.commit()


@attrs.define
class StorageInfo:
    """Describes where the data contents could be found"""

    # could be in memory
    data: Optional[Storage] = None
    # typically localfs
    cache: Optional[Storage] = None
    # typically cloud
    remote: Optional[Storage] = None


class StorageError(Exception):
    pass


class StorageKeyError(StorageError, KeyError):
    pass


class StorageMapping(MutableMapping):
    def __init__(self, *args, **kwargs):
        self._map = dict(*args, **kwargs)

    def __setitem__(self, key, value):
        self._map[key] = value

    def __delitem__(self, key):
        del self._map[key]

    def __getitem__(self, key):
        storages = []
        for prefix, storage in self._map.items():
            if len(prefix) > len(key):
                continue

            if key[: len(prefix)] == prefix:
                storages.append((prefix, storage))

        if not storages:
            raise StorageKeyError(key)

        storages = sorted(storages, key=lambda entry: len(entry[0]), reverse=True)
        data = None
        cache = None
        remote = None
        for _, storage in storages:
            if data is None:
                data = storage.data
            if cache is None:
                cache = storage.cache
            if remote is None:
                remote = storage.remote
            if data and cache and remote:
                break

        return StorageInfo(data=data, cache=cache, remote=remote)

    def __iter__(self):
        yield from self._map.keys()

    def __len__(self):
        return len(self._map)

    def add_data(self, storage: "Storage"):
        info = self.get(storage.key) or StorageInfo()
        info.data = storage
        self[storage.key] = info

    def add_cache(self, storage: "Storage"):
        info = self.get(storage.key) or StorageInfo()
        info.cache = storage
        self[storage.key] = info

    def add_remote(self, storage: "Storage"):
        info = self.get(storage.key) or StorageInfo()
        info.remote = storage
        self[storage.key] = info

    def get_storage_odb(self, entry: "DataIndexEntry", typ: str) -> "HashFileDB":
        info = self[entry.key]
        storage = getattr(info, typ)
        if not storage:
            raise StorageKeyError(entry.key)

        if not isinstance(storage, ObjectStorage):
            raise StorageKeyError(entry.key)

        return storage.odb

    def get_data_odb(self, entry: "DataIndexEntry") -> "HashFileDB":
        return self.get_storage_odb(entry, "data")

    def get_cache_odb(self, entry: "DataIndexEntry") -> "HashFileDB":
        return self.get_storage_odb(entry, "cache")

    def get_remote_odb(self, entry: "DataIndexEntry") -> "HashFileDB":
        return self.get_storage_odb(entry, "remote")

    def get_storage(
        self, entry: "DataIndexEntry", typ: str
    ) -> Tuple["FileSystem", str]:
        info = self[entry.key]
        storage = getattr(info, typ)
        if not storage:
            raise StorageKeyError(entry.key)

        return storage.get(entry)

    def get_data(self, entry: "DataIndexEntry") -> Tuple["FileSystem", str]:
        return self.get_storage(entry, "data")

    def get_cache(self, entry: "DataIndexEntry") -> Tuple["FileSystem", str]:
        return self.get_storage(entry, "cache")

    def get_remote(self, entry: "DataIndexEntry") -> Tuple["FileSystem", str]:
        return self.get_storage(entry, "remote")

    def cache_exists(self, entry: "DataIndexEntry", **kwargs) -> bool:
        storage = self[entry.key]
        if not storage.cache:
            raise StorageKeyError(entry.key)

        return storage.cache.exists(entry, **kwargs)

    def remote_exists(self, entry: "DataIndexEntry", **kwargs) -> bool:
        storage = self[entry.key]
        if not storage.remote:
            raise StorageKeyError(entry.key)

        return storage.remote.exists(entry, **kwargs)


class BaseDataIndex(ABC, MutableMapping[DataIndexKey, DataIndexEntry]):
    storage_map: StorageMapping

    @abstractmethod
    def iteritems(
        self,
        prefix: Optional[DataIndexKey] = None,
        shallow: Optional[bool] = False,
    ) -> Iterator[Tuple[DataIndexKey, DataIndexEntry]]:
        pass

    @abstractmethod
    def traverse(self, node_factory: Callable, **kwargs) -> Any:
        pass

    @abstractmethod
    def has_node(self, key: DataIndexKey) -> bool:
        pass

    @abstractmethod
    def delete_node(self, key: DataIndexKey) -> None:
        pass

    @abstractmethod
    def longest_prefix(
        self, key: DataIndexKey
    ) -> Tuple[Optional[DataIndexKey], Optional[DataIndexEntry]]:
        pass

    def _get_meta(self, key, entry):
        if entry.hash_info:
            return Meta()

        info = self.storage_map.get(key)
        if not info:
            return None

        for storage in [info.data, info.cache, info.remote]:
            if not storage:
                continue

            if not isinstance(storage, FileStorage):
                continue

            fs, path = storage.get(entry)
            try:
                info = fs.info(path)
                return Meta(isdir=(info["type"] == "directory"))
            except FileNotFoundError:
                continue

        return None

    def _info_from_entry(self, key, entry):
        if entry is None:
            return {
                "type": "directory",
                "size": 0,
                "isexec": False,
                "entry": None,
            }

        meta = entry.meta
        if meta is None:
            meta = self._get_meta(key, entry)
            entry.meta = meta

        isdir = meta and meta.isdir
        ret = {
            "type": "directory" if isdir else "file",
            "size": meta.size if meta else 0,
            "isexec": meta.isexec if meta else False,
            "entry": entry,
        }

        if entry.hash_info:
            assert entry.hash_info.name
            ret[entry.hash_info.name] = entry.hash_info.value

        return ret

    def add(self, entry: DataIndexEntry):
        self[cast(DataIndexKey, entry.key)] = entry

    @abstractmethod
    def ls(self, root_key: DataIndexKey, detail=True):
        pass

    def info(self, key: DataIndexKey):
        try:
            entry = self[key]
        except ShortKeyError:
            entry = None

        return self._info_from_entry(key, entry)


def _load_from_object_storage(trie, root_entry, storage):
    if not root_entry.hash_info or not root_entry.hash_info.isdir:
        raise FileNotFoundError

    obj = Tree.load(storage.odb, root_entry.hash_info, hash_name=storage.odb.hash_name)

    dirs = set()
    for ikey, (meta, hash_info) in obj.iteritems():
        if not meta and root_entry.hash_info and root_entry.hash_info == hash_info:
            meta = root_entry.meta

        if len(ikey) >= 2:
            # NOTE: current .dir obj format doesn't include subdirs, so
            # we need to create entries for them manually.
            for idx in range(1, len(ikey)):
                dirs.add(ikey[:-idx])

        entry_key = root_entry.key + ikey
        child_entry = DataIndexEntry(
            key=entry_key,
            hash_info=hash_info,
            meta=meta,
        )
        trie[entry_key] = child_entry

    for dkey in dirs:
        entry_key = root_entry.key + dkey
        trie[entry_key] = DataIndexEntry(
            key=entry_key,
            meta=Meta(isdir=True),
            loaded=True,
        )


def _load_from_file_storage(trie, root_entry, storage):
    from .build import build_entries

    fs, path = storage.get(root_entry)

    if not fs.exists(path):
        raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), path)

    for entry in build_entries(path, fs):
        entry.key = root_entry.key + entry.key
        trie[entry.key] = entry


class DataIndexError(Exception):
    pass


class DataIndexDirError(DataIndexError):
    pass


def _load_from_storage(trie, entry, storage_info):
    last_exc = None

    for storage in [
        storage_info.data,
        storage_info.cache,
        storage_info.remote,
    ]:
        if not storage:
            continue

        try:
            if isinstance(storage, ObjectStorage):
                _load_from_object_storage(trie, entry, storage)
            else:
                _load_from_file_storage(trie, entry, storage)
            return True
        except Exception as exc:  # pylint: disable=W0703
            # NOTE: this might be some random fs exception, e.g. auth error
            last_exc = exc
            logger.debug(
                "failed to load %s from storage %s (%s)",
                entry.key,
                storage.fs.protocol,
                storage.path,
                exc_info=True,
            )

    raise DataIndexDirError(f"failed to load directory {entry.key}") from last_exc


class DataIndex(BaseDataIndex, MutableMapping[DataIndexKey, DataIndexEntry]):
    def __init__(self, *args, **kwargs):
        # NOTE: by default, using an in-memory pygtrie trie that doesn't
        # serialize values, so we can save some time.
        self._trie = PyGTrie()

        self.storage_map = StorageMapping()

        def _onerror(_, exc):
            raise exc

        self.onerror = _onerror

        self.update(*args, **kwargs)

    @classmethod
    def open(cls, path):
        ret = cls()
        ret._trie = DataIndexTrie.open(path)
        return ret

    def view(self, key):
        import copy

        ret = DataIndex()
        ret._trie = self._trie.view(key)  # pylint: disable=protected-access
        ret.storage_map = copy.deepcopy(self.storage_map)
        return ret

    def commit(self):
        self._trie.commit()

    def rollback(self):
        self._trie.rollback()

    def close(self):
        self._trie.close()

    def __setitem__(self, key, value):
        self._trie[key] = value

    def __getitem__(self, key):
        item = self._trie.get(key)
        if item:
            if item.meta is None:
                item.meta = self._get_meta(key, item)
            return item

        lprefix = self._trie.longest_prefix(key)
        if lprefix is not None:
            dir_key, dir_entry = lprefix
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

        if not entry.meta or not entry.meta.isdir:
            return

        try:
            _load_from_storage(self._trie, entry, self.storage_map[key])
        except DataIndexDirError as exc:
            self.onerror(entry, exc)
            return

        entry.loaded = True
        del self._trie[key]
        self._trie[key] = entry
        self._trie.commit()

    def load(self, **kwargs):
        for key, entry in self.iteritems(shallow=True, **kwargs):
            self._load(key, entry)

    def has_node(self, key: DataIndexKey) -> bool:
        return self._trie.has_node(key)

    def delete_node(self, key: DataIndexKey) -> None:
        return self._trie.delete_node(key)

    def shortest_prefix(self, *args, **kwargs):
        return self._trie.shortest_prefix(*args, **kwargs)

    def longest_prefix(
        self, key: DataIndexKey
    ) -> Tuple[Optional[DataIndexKey], Optional[DataIndexEntry]]:
        return self._trie.longest_prefix(key)

    def traverse(self, *args, **kwargs) -> Any:
        return self._trie.traverse(*args, **kwargs)

    def iteritems(
        self,
        prefix: Optional[DataIndexKey] = None,
        shallow: Optional[bool] = False,
    ) -> Iterator[Tuple[DataIndexKey, DataIndexEntry]]:
        kwargs: Dict[str, Any] = {"shallow": shallow}
        if prefix:
            kwargs = {"prefix": prefix}
            item = self._trie.longest_prefix(prefix)
            if item:
                key, entry = item
                self._load(key, entry)

        # FIXME could filter by loaded and/or isdir in sql on sqltrie side
        for key, entry in self._trie.items(**kwargs):
            self._load(key, entry)

        yield from self._trie.items(**kwargs)

    def iterkeys(self, *args, **kwargs):
        return self._trie.keys(*args, **kwargs)

    def _ensure_loaded(self, prefix):
        entry = self.get(prefix)
        if entry and entry.meta and entry.meta.isdir and not entry.loaded:
            self._load(prefix, entry)

    def ls(self, root_key: DataIndexKey, detail=True):
        self._ensure_loaded(root_key)
        if detail:
            yield from (
                (key, self._info_from_entry(key, entry))
                for key, entry in self._trie.ls(root_key, with_values=True)
            )
        else:
            yield from self._trie.ls(root_key)
