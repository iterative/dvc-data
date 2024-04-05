import os
import pickle
import time
from collections.abc import Iterable, Iterator
from functools import wraps
from itertools import repeat
from typing import Any, Optional

import diskcache
from diskcache import Disk as _Disk
from diskcache import (
    Index,  # noqa: F401
    Timeout,  # noqa: F401
)

from dvc_data.compat import batched


class DiskError(Exception):
    def __init__(self, directory: str, type: str) -> None:  # noqa: A002
        self.directory = directory
        self.type = type
        super().__init__(f"Could not open disk '{type}' in {directory}")


def translate_pickle_error(fn):
    @wraps(fn)
    def wrapped(self, *args, **kwargs):
        try:
            return fn(self, *args, **kwargs)
        except (pickle.PickleError, ValueError) as e:
            if isinstance(e, ValueError) and "pickle protocol" not in str(e):
                raise

            raise DiskError(self._directory, type=self._type) from e

    return wrapped


class Disk(_Disk):
    """Reraise pickle-related errors as DiskError."""

    # we need type to differentiate cache for better error messages
    _type: str

    put = translate_pickle_error(_Disk.put)
    get = translate_pickle_error(_Disk.get)
    store = translate_pickle_error(_Disk.store)
    fetch = translate_pickle_error(_Disk.fetch)


class Cache(diskcache.Cache):
    """Extended to handle pickle errors and use a constant pickle protocol."""

    def __init__(
        self,
        directory: Optional[str] = None,
        timeout: int = 60,
        disk: _Disk = Disk,
        type: Optional[str] = None,  # noqa: A002
        **settings: Any,
    ) -> None:
        settings.setdefault("disk_pickle_protocol", 4)
        settings.setdefault("cull_limit", 0)
        super().__init__(directory=directory, timeout=timeout, disk=disk, **settings)
        self.disk._type = self._type = type or os.path.basename(self.directory)

    def __getstate__(self):
        return (*super().__getstate__(), self._type)

    def get_many(self, keys: Iterable[str]) -> Iterator[tuple[str, Optional[str]]]:
        if self.is_empty():
            yield from zip(keys, repeat(None))
            return

        for chunk in batched(keys, 999):
            select = (
                "SELECT key, value FROM Cache WHERE key IN (%s) and raw = 1"  # noqa: S608
                % ",".join("?" * len(chunk))
            )
            d: dict[str, str] = dict(self._sql(select, chunk).fetchall())
            for key in chunk:
                yield key, d.get(key)

    def set_many(self, items: list[tuple[str, str]], retry: bool = True) -> None:
        if not items:
            return

        raw = True
        access_time = store_time = time.time()
        expire_time = None
        access_count, tag, size, mode, filename = 0, None, 0, 1, None
        with self.transact(retry):
            self._con.executemany(
                "INSERT OR REPLACE INTO Cache("
                " key, raw, store_time, expire_time, access_time,"
                " access_count, tag, size, mode, filename, value"
                ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    (
                        key,
                        raw,
                        store_time,
                        expire_time,
                        access_time,
                        access_count,
                        tag,
                        size,
                        mode,
                        filename,
                        value,
                    )
                    for (key, value) in items
                ),
            )

    def is_empty(self) -> bool:
        res = self._sql("SELECT EXISTS (SELECT 1 FROM Cache)", ())
        ((exists,),) = res
        return exists == 0
