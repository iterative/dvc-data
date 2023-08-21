"""DVC data."""
import logging
from typing import TYPE_CHECKING, Iterator, Union, cast

from .tree import Tree

if TYPE_CHECKING:
    from .db import HashFileDB
    from .hash_info import HashInfo
    from .obj import HashFile

logger = logging.getLogger(__name__)


def check(odb: "HashFileDB", obj: "HashFile", **kwargs):
    if isinstance(obj, Tree):
        for _, _, hash_info in obj:
            odb.check(hash_info.value, **kwargs)

    odb.check(obj.oid, **kwargs)


def load(odb: "HashFileDB", hash_info: "HashInfo") -> "HashFile":
    if hash_info.isdir:
        return Tree.load(odb, hash_info)
    return odb.get(cast(str, hash_info.value))


def iterobjs(obj: Union["Tree", "HashFile"]) -> Iterator[Union["Tree", "HashFile"]]:
    if isinstance(obj, Tree):
        yield from (entry_obj for _, entry_obj in obj)
    yield obj
