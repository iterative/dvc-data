import json

from ..hashfile.cache import Cache
from .index import DataIndex, DataIndexEntry


def write_db(index: DataIndex, path: str) -> None:
    cache = Cache(path)
    with cache.transact():
        for key, entry in index.iteritems():
            cache["/".join(key)] = entry.to_dict()


def read_db(path: str) -> DataIndex:
    index = DataIndex()
    cache = Cache(path)

    with cache.transact():
        for key in cache:
            value = cache.get(key)
            index[key.split("/")] = DataIndexEntry.from_dict(value)

    return index


def write_json(index: DataIndex, path: str) -> None:
    with open(path, "w", encoding="utf-8") as fobj:
        json.dump(
            {
                "/".join(key): entry.to_dict()
                for key, entry in index.iteritems()
            },
            fobj,
        )


def read_json(path: str) -> DataIndex:
    index = DataIndex()

    with open(path, "r", encoding="utf-8") as fobj:
        for key, value in json.load(fobj).items():
            index[key.split("/")] = DataIndexEntry.from_dict(value)

    return index
