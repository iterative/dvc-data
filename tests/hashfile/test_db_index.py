from contextlib import closing

import pytest

from dvc_data.hashfile.db.index import ObjectDBIndex


@pytest.fixture
def index(tmp_upath):
    with closing(ObjectDBIndex(tmp_upath, "foo")) as _index:
        yield _index


def test_roundtrip(request, tmp_upath, index):
    expected_dir = {"1234.dir"}
    expected_file = {"5678"}
    index.update(expected_dir, expected_file)

    new_index = ObjectDBIndex(tmp_upath, "foo")
    request.addfinalizer(new_index.close)

    assert set(new_index.dir_hashes()) == expected_dir
    assert set(new_index.hashes()) == expected_dir | expected_file


def test_clear(index):
    index.update(["1234.dir"], ["5678"])
    index.clear()
    assert not list(index.hashes())


def test_update(index):
    expected_dir = {"1234.dir"}
    expected_file = {"5678"}
    index.update(expected_dir, expected_file)
    assert set(index.dir_hashes()) == expected_dir
    assert set(index.hashes()) == expected_dir | expected_file


def test_intersection(index):
    hashes = (str(i) for i in range(2000))
    expected = {str(i) for i in range(1000)}
    index.update([], hashes)
    assert set(index.intersection(expected)) == expected
