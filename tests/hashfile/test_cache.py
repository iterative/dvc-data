import pickle
from os import fspath
from typing import Any

import pytest

from dvc_data.hashfile.cache import Cache, DiskError, HashesCache


def set_value(cache: Cache, key: str, value: Any) -> Any:
    cache[key] = value
    return cache[key]


@pytest.mark.parametrize("disk_type", [None, "test"])
def test_pickle_protocol_error(tmp_path, disk_type):
    directory = tmp_path / "test"
    cache = Cache(
        fspath(directory),
        disk_pickle_protocol=pickle.HIGHEST_PROTOCOL + 1,
        type=disk_type,
    )
    with pytest.raises(DiskError) as exc, cache as cache:
        set_value(cache, "key", ("value1", "value2"))
    assert exc.value.directory == fspath(directory)
    assert exc.value.type == "test"
    assert f"Could not open disk 'test' in {directory}" == str(exc.value)


@pytest.mark.parametrize(
    "proto_a, proto_b",
    [
        (pickle.HIGHEST_PROTOCOL - 1, pickle.HIGHEST_PROTOCOL),
        (pickle.HIGHEST_PROTOCOL, pickle.HIGHEST_PROTOCOL - 1),
    ],
)
def test_pickle_backwards_compat(tmp_path, proto_a, proto_b):
    with Cache(
        directory=fspath(tmp_path / "test"),
        disk_pickle_protocol=proto_a,
    ) as cache:
        set_value(cache, "key", ("value1", "value2"))
    with Cache(
        directory=fspath(tmp_path / "test"),
        disk_pickle_protocol=proto_b,
    ) as cache:
        assert cache["key"] == ("value1", "value2")
        set_value(cache, "key", ("value3", "value4"))
        assert cache["key"] == ("value3", "value4")


def test_hashes_cache(tmp_path):
    with HashesCache(tmp_path / "test") as cache:
        assert cache.is_empty()
        assert cache.set("key", "value")
        assert not cache.is_empty()
        assert cache.get("key") == "value"
        assert cache.get("not-existing-key") is None


def test_hashes_cache_many(tmp_path):
    with HashesCache(tmp_path / "test") as cache:
        assert cache.is_empty()
        assert list(cache.get_many(("key1",))) == [("key1", None)]

        cache.set_many((("key1", "value1"), ("key2", "value2")))
        assert not cache.is_empty()
        assert list(cache.get_many(("key1", "key2"))) == [
            ("key1", "value1"),
            ("key2", "value2"),
        ]
        assert list(cache.get_many(("key1", "key2", "not-existing-key"))) == [
            ("key1", "value1"),
            ("key2", "value2"),
            ("not-existing-key", None),
        ]


@pytest.mark.parametrize("upsert", [True, False])
def test_hashes_cache_update(tmp_path, upsert):
    with HashesCache(tmp_path / "test") as cache:
        cache.SUPPORTS_UPSERT = upsert

        assert cache.is_empty()
        cache.set("key1", "value")
        cache.set_many((("key1", "value1"), ("key2", "value2")))
        assert list(cache.get_many(("key1", "key2"))) == [
            ("key1", "value1"),
            ("key2", "value2"),
        ]
