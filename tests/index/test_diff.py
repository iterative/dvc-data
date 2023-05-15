import pytest

from dvc_data.hashfile.hash_info import HashInfo
from dvc_data.hashfile.meta import Meta
from dvc_data.index import DataIndex, DataIndexEntry
from dvc_data.index.diff import (
    ADD,
    DELETE,
    MODIFY,
    RENAME,
    UNCHANGED,
    Change,
    diff,
)


def test_diff():
    old_foo_key = ("foo",)
    old_foo_entry = DataIndexEntry(
        key=old_foo_key,
        meta=Meta(),
        hash_info=HashInfo(
            name="md5", value="d3b07384d113edec49eaa6238ad5ff00"
        ),
    )
    old_bar_key = ("dir", "subdir", "bar")
    old_bar_entry = DataIndexEntry(
        key=old_bar_key,
        meta=Meta(isdir=True),
        hash_info=HashInfo(
            name="md5",
            value="1f69c66028c35037e8bf67e5bc4ceb6a.dir",
        ),
    )
    old = DataIndex({old_foo_key: old_foo_entry, old_bar_key: old_bar_entry})

    assert set(diff(old, old, with_unchanged=True)) == {
        Change(UNCHANGED, old_foo_entry, old_foo_entry),
        Change(UNCHANGED, old_bar_entry, old_bar_entry),
    }
    assert set(diff(old, old, with_renames=True, with_unchanged=True)) == {
        Change(UNCHANGED, old_foo_entry, old_foo_entry),
        Change(UNCHANGED, old_bar_entry, old_bar_entry),
    }

    new_foo_key = ("data", "FOO")
    new_foo_entry = DataIndexEntry(
        key=new_foo_key,
        meta=Meta(),
        hash_info=HashInfo(
            name="md5", value="d3b07384d113edec49eaa6238ad5ff00"
        ),
    )
    new = DataIndex(
        {
            (
                "data",
                "FOO",
            ): new_foo_entry,
            old_bar_key: old_bar_entry,
        }
    )

    assert set(diff(old, new, with_unchanged=True)) == {
        Change(ADD, None, new_foo_entry),
        Change(DELETE, old_foo_entry, None),
        Change(UNCHANGED, old_bar_entry, old_bar_entry),
    }
    assert set(diff(old, new, with_renames=True, with_unchanged=True)) == {
        Change(RENAME, old_foo_entry, new_foo_entry),
        Change(UNCHANGED, old_bar_entry, old_bar_entry),
    }


def test_diff_no_hashes():
    index = DataIndex(
        {
            ("foo",): DataIndexEntry(key=("foo",)),
        }
    )
    assert not set(diff(index, None, hash_only=True))


def test_diff_meta_only():
    key = ("foo",)
    old_entry = DataIndexEntry(
        key=key,
        meta=Meta(etag="abc"),
        hash_info=HashInfo(name="md5", value="123"),
    )
    new_entry = DataIndexEntry(
        key=key,
        meta=Meta(etag="abc"),
        hash_info=HashInfo(name="md5", value="456"),
    )
    old = DataIndex({key: old_entry})
    new = DataIndex({key: new_entry})

    assert list(diff(old, new, meta_only=True, with_unchanged=True)) == [
        Change(UNCHANGED, old_entry, new_entry),
    ]

    new_entry.meta = Meta("def")
    assert list(diff(old, new, meta_only=True, with_unchanged=True)) == [
        Change(MODIFY, old_entry, new_entry),
    ]


@pytest.mark.parametrize(
    "typ, left_meta, left_hi, right_meta, right_hi",
    [
        (
            UNCHANGED,
            Meta(etag="123"),
            HashInfo(name="md5", value="123"),
            Meta(etag="123"),
            HashInfo(name="md5", value="123"),
        ),
        (
            ADD,
            None,
            None,
            Meta(etag="123"),
            HashInfo(name="md5", value="123"),
        ),
        (
            DELETE,
            Meta(etag="123"),
            HashInfo(name="md5", value="123"),
            None,
            None,
        ),
    ],
)
def test_diff_combined(typ, left_meta, left_hi, right_meta, right_hi):
    key = ("foo",)
    old_entry = DataIndexEntry(
        key=key,
        meta=left_meta,
        hash_info=left_hi,
    )
    new_entry = DataIndexEntry(
        key=key,
        meta=right_meta,
        hash_info=right_hi,
    )
    old = DataIndex({key: old_entry})
    new = DataIndex({key: new_entry})

    # diff should return UNCHANGED if both meta and hash info match,
    # but MODIFY if they don't since entries still exist
    assert list(diff(old, new, with_unchanged=True)) == [
        Change(
            UNCHANGED if typ == UNCHANGED else MODIFY, old_entry, new_entry
        ),
    ]

    # diff should return UNCHANGED if both meta and hash info match,
    # but MODIFY if they don't since entries still exist
    old_entry.meta = None
    new_entry.meta = None
    assert list(diff(old, new, with_unchanged=True)) == [
        Change(
            UNCHANGED if typ == UNCHANGED else MODIFY, old_entry, new_entry
        ),
    ]

    # diff should return meta diff when both hash infos are None
    old_entry.meta = left_meta
    new_entry.meta = right_meta
    old_entry.hash_info = None
    new_entry.hash_info = None
    assert list(diff(old, new, with_unchanged=True)) == [
        Change(typ, old_entry, new_entry),
    ]

    # diff should return modify when meta and hash info diff do not match
    old_entry.meta = Meta(etag="abc")
    new_entry.meta = Meta(etag="def")
    old_entry.hash_info = left_hi
    new_entry.hash_info = right_hi
    assert list(diff(old, new, with_unchanged=True)) == [
        Change(MODIFY, old_entry, new_entry),
    ]
    old_entry.meta = left_meta
    new_entry.meta = right_meta
    old_entry.hash_info = HashInfo(name="md5", value="abc")
    new_entry.hash_info = HashInfo(name="md5", value="def")
    assert list(diff(old, new, with_unchanged=True)) == [
        Change(MODIFY, old_entry, new_entry),
    ]
