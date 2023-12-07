from typing import TYPE_CHECKING, Optional

from .build import build_entries, build_entry
from .index import FileStorage

if TYPE_CHECKING:
    from dvc_objects.fs import FileSystem

    from dvc_data.hashfile._ignore import Ignore

    from .index import DataIndex, DataIndexKey


def add(
    index: "DataIndex",
    path: str,
    fs: "FileSystem",
    key: "DataIndexKey",
    ignore: Optional["Ignore"] = None,
):
    entry = build_entry(path, fs)
    entry.key = key
    index.add(entry)

    index.storage_map.add_data(FileStorage(key=key, fs=fs, path=path))

    if not fs.isdir(path):
        return

    for entry in build_entries(path, fs, ignore=ignore):
        assert entry.key is not None
        entry.key = (*key, *entry.key)
        index.add(entry)
