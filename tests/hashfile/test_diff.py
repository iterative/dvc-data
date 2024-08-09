import pytest

from dvc_data.hashfile.diff import ROOT, Change, TreeEntry, diff
from dvc_data.hashfile.meta import Meta
from dvc_data.hashfile.obj import HashFile
from dvc_data.hashfile.tree import Tree


@pytest.fixture
def tree():
    tree = Tree.from_list(
        [
            {"md5": "37b51d194a7513e45b56f6524f2d51f2", "relpath": "bar"},
            {"md5": "acbd18db4cc2f85cedef654fccc4a4d8", "relpath": "foo"},
        ]
    )
    tree.digest()
    return tree


def test_diff_unchanged(mocker, tree):
    meta = Meta()
    mocked_cache = mocker.MagicMock(check=mocker.MagicMock(return_value=meta))
    _, bar_oid = tree.get(("bar",))
    obj = HashFile("data", mocker.MagicMock(), bar_oid)

    assert not diff(obj, obj, mocked_cache)
    assert not diff(tree, tree, mocked_cache)


def test_different_object_type_tree_to_hashfile(mocker, tree):
    meta = Meta()
    mocked_cache = mocker.MagicMock(check=mocker.MagicMock(return_value=meta))

    (_, bar_oid), (_, foo_oid) = tree.get(("bar",)), tree.get(("foo",))
    obj = HashFile("data", mocker.MagicMock(), bar_oid)
    d = diff(tree, obj, mocked_cache)

    assert d.stats == {"modified": 1, "deleted": 2, "added": 0}
    assert not d.unchanged
    assert d.modified == [
        Change(
            old=TreeEntry(cache_meta=meta, key=ROOT, oid=tree.hash_info),
            new=TreeEntry(cache_meta=meta, key=ROOT, oid=bar_oid),
        )
    ]
    assert sorted(d.deleted) == [
        Change(
            old=TreeEntry(cache_meta=meta, key=("bar",), oid=bar_oid),
            new=TreeEntry(key=("bar",)),
        ),
        Change(
            old=TreeEntry(cache_meta=meta, key=("foo",), oid=foo_oid),
            new=TreeEntry(key=("foo",)),
        ),
    ]


def test_different_object_type_hashfile_to_tree(mocker, tree):
    meta = Meta()
    mocked_cache = mocker.MagicMock(check=mocker.MagicMock(return_value=meta))
    (_, bar_oid), (_, foo_oid) = tree.get(("bar",)), tree.get(("foo",))
    obj = HashFile("data", mocker.MagicMock(), bar_oid)
    d = diff(obj, tree, mocked_cache)

    assert d.stats == {"modified": 1, "deleted": 0, "added": 2}
    assert not d.unchanged
    assert d.modified == [
        Change(
            old=TreeEntry(cache_meta=meta, key=ROOT, oid=bar_oid),
            new=TreeEntry(cache_meta=meta, key=ROOT, oid=tree.hash_info),
        )
    ]
    assert sorted(d.added) == [
        Change(
            old=TreeEntry(cache_meta=meta, key=("bar",)),
            new=TreeEntry(key=("bar",), oid=bar_oid),
        ),
        Change(
            old=TreeEntry(cache_meta=meta, key=("foo",)),
            new=TreeEntry(key=("foo",), oid=foo_oid),
        ),
    ]
