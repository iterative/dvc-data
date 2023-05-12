import pytest

from dvc_data.hashfile.diff import ROOT, Change, TreeEntry, diff
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
    yield tree


@pytest.fixture
def mocked_cache(mocker):
    return mocker.MagicMock(cache=mocker.MagicMock(return_value=True))


def test_diff_unchanged(mocker, tree, mocked_cache):
    _, bar_oid = tree.get(("bar",))
    obj = HashFile("data", mocker.MagicMock(), bar_oid)

    assert not diff(obj, obj, mocked_cache)
    assert not diff(tree, tree, mocked_cache)


def test_different_object_type_tree_to_hashfile(mocker, tree, mocked_cache):
    (_, bar_oid), (_, foo_oid) = tree.get(("bar",)), tree.get(("foo",))
    obj = HashFile("data", mocker.MagicMock(), bar_oid)
    d = diff(tree, obj, mocked_cache)

    assert d.stats == {"modified": 1, "deleted": 2, "added": 0}
    assert not d.unchanged
    assert d.modified == [
        Change(
            old=TreeEntry(in_cache=True, key=ROOT, oid=tree.hash_info),
            new=TreeEntry(in_cache=True, key=ROOT, oid=bar_oid),
        )
    ]
    assert sorted(d.deleted) == [
        Change(
            old=TreeEntry(in_cache=True, key=("bar",), oid=bar_oid),
            new=TreeEntry(key=("bar",)),
        ),
        Change(
            old=TreeEntry(in_cache=True, key=("foo",), oid=foo_oid),
            new=TreeEntry(key=("foo",)),
        ),
    ]


def test_different_object_type_hashfile_to_tree(mocker, tree, mocked_cache):
    (_, bar_oid), (_, foo_oid) = tree.get(("bar",)), tree.get(("foo",))
    obj = HashFile("data", mocker.MagicMock(), bar_oid)
    d = diff(obj, tree, mocked_cache)

    assert d.stats == {"modified": 1, "deleted": 0, "added": 2}
    assert not d.unchanged
    assert d.modified == [
        Change(
            old=TreeEntry(in_cache=True, key=ROOT, oid=bar_oid),
            new=TreeEntry(in_cache=True, key=ROOT, oid=tree.hash_info),
        )
    ]
    assert sorted(d.added) == [
        Change(
            old=TreeEntry(in_cache=True, key=("bar",)),
            new=TreeEntry(key=("bar",), oid=bar_oid),
        ),
        Change(
            old=TreeEntry(in_cache=True, key=("foo",)),
            new=TreeEntry(key=("foo",), oid=foo_oid),
        ),
    ]
