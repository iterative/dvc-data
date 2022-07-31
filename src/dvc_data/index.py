from dataclasses import dataclass
from typing import TYPE_CHECKING, Optional

from pygtrie import Trie

if TYPE_CHECKING:
    from .hashfile.db import HashFileDB
    from .hashfile.hash_info import HashInfo
    from .hashfile.meta import Meta
    from .hashfile.obj import HashFile


@dataclass
class DataIndexEntry:
    meta: Optional["Meta"] = None
    obj: Optional["HashFile"] = None
    hash_info: Optional["HashInfo"] = None
    odb: Optional["HashFileDB"] = None
    remote: Optional["HashFileDB"] = None


class DataIndex(Trie):
    pass


def build(index, path, fs, **kwargs):
    from .build import build as obuild

    for key, entry in index.iteritems():
        try:
            _, meta, obj = obuild(
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

        entry.meta = meta
        entry.obj = obj
        entry.hash_info = hash_info


def checkout(index, path, fs, **kwargs):
    from . import load
    from .checkout import checkout as ocheckout

    for key, entry in index.iteritems():
        if not entry.obj:
            entry.obj = load(entry.odb, entry.hash_info)
        ocheckout(fs.path.join(path, *key), fs, entry.obj, entry.odb, **kwargs)
