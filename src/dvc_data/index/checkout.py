from typing import TYPE_CHECKING

from ..hashfile import load
from ..hashfile.checkout import checkout as ocheckout

if TYPE_CHECKING:
    from dvc_objects.fs.base import FileSystem

    from .index import DataIndex


def checkout(
    index: "DataIndex", path: str, fs: "FileSystem", **kwargs
) -> None:
    for key, entry in index.iteritems():
        if not entry.obj:
            entry.obj = load(entry.odb, entry.hash_info)
        ocheckout(fs.path.join(path, *key), fs, entry.obj, entry.odb, **kwargs)
