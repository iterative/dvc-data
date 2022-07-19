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
from funcy import print_durations


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

    def set(self, key, value, expire=None, read=False, tag=None, retry=False):
        """Set `key` and `value` item in cache.

        When `read` is `True`, `value` should be a file-like object opened
        for reading in binary mode.

        Raises :exc:`Timeout` error when database timeout occurs and `retry` is
        `False` (default).

        :param key: key for item
        :param value: value for item
        :param float expire: seconds until item expires
            (default None, no expiry)
        :param bool read: read value as bytes from file (default False)
        :param str tag: text to associate with key (default None)
        :param bool retry: retry if database timeout occurs (default False)
        :return: True if item was set
        :raises Timeout: if database timeout occurs

        """
        now = time.time()
        db_key, raw = self._disk.put(key)
        expire_time = None if expire is None else now + expire
        size, mode, filename, db_value = self._disk.store(value, read, key=key)
        with self._transact(retry, filename) as (sql, _cleanup):
            sql(
                "INSERT OR REPLACE INTO Cache("
                " key, raw, store_time, expire_time, access_time,"
                " access_count, tag, size, mode, filename, value"
                ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    db_key,
                    raw,
                    now,  # store_time
                    expire_time,
                    now,  # access_time
                    0,  # access_count
                    tag,
                    size,
                    mode,
                    filename,
                    db_value,
                ),
            )

    @print_durations
    def set_many(self, d, expire=None, read=False, tag=None, retry=False):
        """Set `key` and `value` item in cache.

        When `read` is `True`, `value` should be a file-like object opened
        for reading in binary mode.

        Raises :exc:`Timeout` error when database timeout occurs and `retry` is
        `False` (default).

        :param key: key for item
        :param value: value for item
        :param float expire: seconds until item expires
            (default None, no expiry)
        :param bool read: read value as bytes from file (default False)
        :param str tag: text to associate with key (default None)
        :param bool retry: retry if database timeout occurs (default False)
        :return: True if item was set
        :raises Timeout: if database timeout occurs

        """
        now = time.time()
        expire_time = None if expire is None else now + expire

        with self._transact(retry) as (sql, _cleanup):
            for (key, value) in d:
                db_key, raw = self._disk.put(key)
                size, mode, filename, db_value = self._disk.store(
                    value, read, key=key
                )
                sql(
                    "INSERT OR REPLACE INTO Cache("
                    " key, raw, store_time, expire_time, access_time,"
                    " access_count, tag, size, mode, filename, value"
                    ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        db_key,
                        raw,
                        now,  # store_time
                        expire_time,
                        now,  # access_time
                        0,  # access_count
                        tag,
                        size,
                        mode,
                        filename,
                        db_value,
                    ),
                )
