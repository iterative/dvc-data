import os
import pickle
import time
from functools import wraps
from typing import Any

import diskcache
from diskcache import Disk as disk
from diskcache import Index  # noqa: F401, pylint: disable=unused-import
from diskcache import Timeout  # noqa: F401, pylint: disable=unused-import

# pylint: disable=redefined-builtin
from funcy import chunks


class DiskError(Exception):
    def __init__(self, directory: str, type: str) -> None:
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
            # pylint: disable=protected-access
            raise DiskError(self._directory, type=self._type) from e

    return wrapped


class Disk(disk):
    """Reraise pickle-related errors as DiskError."""

    # we need type to differentiate cache for better error messages
    _type: str

    put = translate_pickle_error(disk.put)
    get = translate_pickle_error(disk.get)
    store = translate_pickle_error(disk.store)
    fetch = translate_pickle_error(disk.fetch)


class Cache(diskcache.Cache):
    """Extended to handle pickle errors and use a constant pickle protocol."""

    def __init__(
        self,
        directory: str = None,
        timeout: int = 60,
        disk: disk = Disk,  # pylint: disable=redefined-outer-name
        type: str = None,
        **settings: Any,
    ) -> None:
        settings.setdefault("disk_pickle_protocol", 4)
        super().__init__(
            directory=directory, timeout=timeout, disk=disk, **settings
        )
        self.disk._type = self._type = type or os.path.basename(self.directory)

    def __getstate__(self):
        return (*super().__getstate__(), self._type)

    def get_many(self, keys):
        for chunk in chunks(999, keys):
            select = (
                "SELECT key, value FROM Cache WHERE key IN (%s) and raw = 1"
                % ",".join("?" * len(chunk))
            )
            rows = self._sql(select, chunk).fetchall()

            chunk = set(chunk)
            for (key, value) in rows:
                chunk.remove(key)
                yield (key, value)

            yield from ((key, None) for key in chunk)

    def set_many(self, d, expire=None, retry=False):
        if not d:
            return

        raw = True
        access_time = store_time = now = time.time()
        expire_time = None if expire is None else now + expire
        access_count, tag, size, mode, filename = 0, None, 0, 1, None
        with self._transact(retry):
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
                    for (key, value) in d
                ),
            )
