from collections import deque
from typing import Any, Callable, Iterator, Optional, Tuple, cast

from ..hashfile.tree import Tree
from .index import BaseDataIndex, DataIndex, DataIndexEntry, DataIndexKey


class DataIndexView(BaseDataIndex):
    def __init__(
        self,
        index: DataIndex,
        filter_fn: Callable[[DataIndexKey], bool],
    ):
        self._index = index
        self.filter_fn = filter_fn

    def __getitem__(self, key: DataIndexKey) -> DataIndexEntry:
        if self.filter_fn(key):
            return self._index[key]
        raise KeyError

    def __iter__(self) -> Iterator[DataIndexKey]:
        return (key for key, _ in self._iteritems())

    def __len__(self):
        return len(list(iter(self)))

    def _iteritems(
        self,
        prefix: Optional[DataIndexKey] = None,
        shallow: Optional[bool] = False,
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
                        for loaded_key in self._load_dir_keys(
                            key, value, shallow=shallow
                        ):
                            # pylint: disable-next=protected-access
                            trie = self._index._trie
                            yield loaded_key, trie.get(loaded_key)

    def _load_dir_keys(
        self,
        prefix: DataIndexKey,
        entry: Optional[DataIndexEntry],
        shallow: Optional[bool] = False,
    ) -> Iterator[DataIndexKey]:
        # NOTE: traverse() will not enter subtries that have been added
        # in-place during traversal. So for dirs which we load in-place, we
        # need to iterate over the new keys ourselves.
        if (
            entry is not None
            and entry.hash_info
            and entry.hash_info.isdir
            and not entry.loaded
        ):
            self._index._load(  # pylint: disable=protected-access
                prefix, entry
            )
            if not shallow:
                for key, _ in cast(Tree, entry.obj).iteritems():
                    yield prefix + key

    def iteritems(
        self,
        prefix: Optional[DataIndexKey] = None,
        shallow: Optional[bool] = False,
    ) -> Iterator[Tuple[DataIndexKey, DataIndexEntry]]:
        return self._iteritems(
            prefix=prefix, shallow=shallow, ensure_loaded=True
        )

    def traverse(self, node_factory: Callable, **kwargs) -> Any:
        def _node_factory(path_conv, key, children, *args):
            if not key or self.filter_fn(key):
                return node_factory(path_conv, key, children, *args)

        return self._index.traverse(_node_factory, **kwargs)

    def ls(self, root_key: DataIndexKey, detail=True):
        def node_factory(_, key, children, entry=None):
            if key == root_key:
                return children

            if detail:
                return key, self._info_from_entry(key, entry)

            return key

        self._index._ensure_loaded(  # pylint: disable=protected-access
            root_key
        )
        return self.traverse(node_factory, prefix=root_key)

    def has_node(self, key: DataIndexKey) -> bool:
        return self.filter_fn(key) and self._index.has_node(key)

    def longest_prefix(
        self, key: DataIndexKey
    ) -> Tuple[Optional[DataIndexKey], Optional[DataIndexEntry]]:
        if self.filter_fn(key):
            return self._index.longest_prefix(key)
        return (None, None)


def view(
    index: DataIndex, filter_fn: Callable[[DataIndexKey], bool]
) -> DataIndexView:
    """Return read-only filtered view of an index."""
    return DataIndexView(index, filter_fn=filter_fn)
