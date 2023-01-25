from dvc_data.hashfile.hash_info import HashInfo
from dvc_data.index import DataIndex, DataIndexEntry
from dvc_data.index.diff import ADD, DELETE, RENAME, UNCHANGED, Change, diff


def test_diff():
    old_foo_key = ("foo",)
    old_foo_entry = DataIndexEntry(
        key=old_foo_key,
        hash_info=HashInfo(
            name="md5", value="d3b07384d113edec49eaa6238ad5ff00"
        ),
    )
    old_bar_key = ("dir", "subdir", "bar")
    old_bar_entry = DataIndexEntry(
        key=old_bar_key,
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
