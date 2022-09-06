"""DVC data."""
import logging
from typing import TYPE_CHECKING, Iterator, Union

from .tree import Tree

if TYPE_CHECKING:
    from dvc_objects.db import ObjectDB

    from .hash_info import HashInfo
    from .obj import HashFile

logger = logging.getLogger(__name__)


def check(odb: "ObjectDB", obj: "HashFile", **kwargs):
    if isinstance(obj, Tree):
        for _, _, hash_info in obj:
            odb.check(hash_info.value, **kwargs)

    odb.check(obj.oid, **kwargs)


def load(odb: "ObjectDB", hash_info: "HashInfo") -> "HashFile":
    if hash_info.isdir:
        return Tree.load(odb, hash_info)
    return odb.get(hash_info.value)


def iterobjs(
    obj: Union["Tree", "HashFile"]
) -> Iterator[Union["Tree", "HashFile"]]:
    if isinstance(obj, Tree):
        yield from (entry_obj for _, entry_obj in obj)
    yield obj
