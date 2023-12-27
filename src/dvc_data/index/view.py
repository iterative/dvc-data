from collections import deque
from typing import TYPE_CHECKING, Any, Callable, Iterator, Optional, Tuple

from .index import BaseDataIndex, DataIndex, DataIndexEntry, DataIndexKey

if TYPE_CHECKING:
    from .index import StorageMapping


class DataIndexView(BaseDataIndex):
    def __init__(
        self,
        index: DataIndex,
        filter_fn: Callable[[DataIndexKey], bool],
    ):
        self._index = index
        self.filter_fn = filter_fn

    @property
    def onerror(self):
        return self._index.onerror

    @onerror.setter
    def onerror(self, onerror):
        self._index.onerror = onerror

    @property
    def storage_map(self) -> "StorageMapping":  # type: ignore[override]
        return self._index.storage_map

    def __setitem__(self, key, value):
        if self.filter_fn(key):
            self._index[key] = value
        else:
            raise KeyError

    def __getitem__(self, key: DataIndexKey) -> DataIndexEntry:
        if key == () or self.filter_fn(key):
            return self._index[key]
        raise KeyError

    def __delitem__(self, key: DataIndexKey):
        if self.filter_fn(key):
            del self._index[key]
        else:
            raise KeyError

    def __iter__(self) -> Iterator[DataIndexKey]:
        return (key for key, _ in self._iteritems())

    def __len__(self):
        return len(list(iter(self)))

    def _iteritems(
        self,
        prefix: Optional[DataIndexKey] = None,
        shallow: bool = False,
        ensure_loaded: bool = False,
    ) -> Iterator[Tuple[DataIndexKey, DataIndexEntry]]:
        # NOTE: iteration is implemented using traverse and not iter/iteritems
        # since it supports skipping subtrie traversal for prefixes that are
        # not in the view.

        class _FilterNode:
            def __init__(self, key, children, *args):
                self.key = key
                self.children = children
                self.value = args[0] if args else None

            def build(self, stack):
                if not self.key or not shallow:
                    for child in self.children:
                        stack.append(child)
                return self.key, self.value

        def _node_factory(_, key, children, *args) -> Optional[_FilterNode]:
            return _FilterNode(key, children, *args)

        kwargs = {"prefix": prefix} if prefix is not None else {}
        stack = deque([self.traverse(_node_factory, **kwargs)])
        while stack:
            node = stack.popleft()
            if node is not None:
                key, value = node.build(stack)
                if key and value:
                    yield key, value
                    if ensure_loaded:
                        yield from self._load_dir_keys(key, value, shallow=shallow)

    def _load_dir_keys(
        self,
        prefix: DataIndexKey,
        entry: Optional[DataIndexEntry],
        shallow: Optional[bool] = False,
    ) -> Iterator[Tuple[DataIndexKey, DataIndexEntry]]:
        # NOTE: traverse() will not enter subtries that have been added
        # in-place during traversal. So for dirs which we load in-place, we
        # need to iterate over the new keys ourselves.
        if (
            entry is not None
            and entry.hash_info
            and entry.hash_info.isdir
            and not entry.loaded
        ):
            self._index._load(prefix, entry)
            if not shallow:
                for key, val in self._index.iteritems(entry.key):
                    if key != prefix and self.filter_fn(key):
                        yield key, val

    def iteritems(
        self,
        prefix: Optional[DataIndexKey] = None,
        shallow: bool = False,
    ) -> Iterator[Tuple[DataIndexKey, DataIndexEntry]]:
        return self._iteritems(prefix=prefix, shallow=shallow, ensure_loaded=True)

    def traverse(self, node_factory: Callable, **kwargs) -> Any:
        def _node_factory(path_conv, key, children, *args):
            if not key or self.filter_fn(key):
                return node_factory(path_conv, key, children, *args)
            return None

        return self._index.traverse(_node_factory, **kwargs)

    def ls(self, root_key: DataIndexKey, detail=True):
        self._index._ensure_loaded(root_key)

        def _filter_fn(entry):
            key = entry[0] if detail else entry
            return self.filter_fn(key)

        yield from filter(_filter_fn, self._index.ls(root_key, detail=detail))

    def has_node(self, key: DataIndexKey) -> bool:
        return self.filter_fn(key) and self._index.has_node(key)

    def delete_node(self, key: DataIndexKey) -> None:
        if not self.filter_fn(key):
            raise KeyError
        self._index.delete_node(key)

    def longest_prefix(
        self, key: DataIndexKey
    ) -> Tuple[Optional[DataIndexKey], Optional[DataIndexEntry]]:
        if self.filter_fn(key):
            return self._index.longest_prefix(key)
        return (None, None)


def view(index: DataIndex, filter_fn: Callable[[DataIndexKey], bool]) -> DataIndexView:
    """Return read-only filtered view of an index."""
    return DataIndexView(index, filter_fn=filter_fn)
