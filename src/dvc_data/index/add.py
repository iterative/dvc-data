from typing import TYPE_CHECKING, Optional

from .build import build_entries, build_entry

if TYPE_CHECKING:
    from dvc_objects.fs import FileSystem

    from ..hashfile._ignore import Ignore
    from .index import DataIndex, DataIndexKey


def add(
    index: "DataIndex",
    path: str,
    fs: "FileSystem",
    key: "DataIndexKey",
    ignore: Optional["Ignore"] = None,
):
    if not fs.isdir(path):
        index[key] = build_entry(path, fs)
        return

    for entry_key, entry in build_entries(path, fs, ignore=ignore):
        index[(*key, *entry_key)] = entry
