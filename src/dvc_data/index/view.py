from collections import deque
from typing import Any, Callable, Iterable, Iterator, Optional, Set, Tuple

from .index import BaseDataIndex, DataIndex, DataIndexEntry, DataIndexKey


class DataIndexView(BaseDataIndex):
    def __init__(
        self,
        index: DataIndex,
        prefixes: Iterable[DataIndexKey],
        keys: Iterable[DataIndexKey],
    ):
        self._index = index
        self._prefixes = set(prefixes)
        self._keys = set(keys)

    def __getitem__(self, key: DataIndexKey) -> DataIndexEntry:
        if key in self._keys:
            return self._index[key]
        raise KeyError

    def __iter__(self) -> Iterator[DataIndexKey]:
        return iter(self._keys)

    def __len__(self):
        return len(self._keys)

    def iteritems(
        self, prefix: Optional[DataIndexKey] = None, shallow: bool = False
    ) -> Iterator[Tuple[DataIndexKey, DataIndexEntry]]:
        if prefix is None:
            yield from self.items()
            return
        if prefix in self._prefixes:
            for key in self._index.iterkeys(prefix=prefix, shallow=shallow):
                if key in self._keys:
                    yield key, self._index[key]

    def traverse(self, node_factory: Callable, **kwargs) -> Any:
        def _node_factory(path_conv, key, children, *args):
            if not key or key in self._keys or key in self._prefixes:
                return node_factory(path_conv, key, children, *args)

        return self._index.traverse(_node_factory, **kwargs)

    def has_node(self, key: DataIndexKey) -> bool:
        return key in self._keys or key in self._prefixes

    def longest_prefix(
        self, key: DataIndexKey
    ) -> Tuple[Optional[DataIndexKey], Optional[DataIndexEntry]]:
        if key in self._keys:
            return self._index.longest_prefix(key)
        return (None, None)


def _view_keys(
    index: DataIndex, filter_fn: Callable[[DataIndexKey], bool]
) -> Tuple[Set[DataIndexKey], Set[DataIndexKey]]:
    """Return (prefixes, keys) matching the specified filter."""

    class _FilterNode:
        def __init__(self, key, children, has_value):
            self.key = key
            self.children = children
            self.has_value = has_value

        def build(self, stack):
            for child in self.children:
                stack.append(child)
            return self.key, bool(self.children), self.has_value

    def node_factory(_, key, children, *args) -> Optional[_FilterNode]:
        if not key or filter_fn(key):
            return _FilterNode(key, children, bool(args))
        return None

    prefixes: Set[DataIndexKey] = set()
    keys: Set[DataIndexKey] = set()
    stack = deque([index.traverse(node_factory)])
    while stack:
        node = stack.popleft()
        if node is not None:
            key, is_prefix, has_value = node.build(stack)
            if key:
                if is_prefix:
                    prefixes.add(key)
                if has_value:
                    keys.add(key)
    return prefixes, keys


def view(
    index: DataIndex, filter_fn: Callable[[DataIndexKey], bool]
) -> DataIndexView:
    """Return read-only filtered view of an index."""
    prefixes, keys = _view_keys(index, filter_fn)
    return DataIndexView(index, prefixes, keys)
