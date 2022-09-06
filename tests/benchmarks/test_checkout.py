import shutil
from os import fspath

import pytest
from dvc_objects.fs import localfs
from dvc_objects.fs.generic import test_links as _test_links

from dvc_data.cli import build, gentree, get_odb
from dvc_data.hashfile.checkout import checkout
from dvc_data.hashfile.state import State


@pytest.mark.parametrize("link", ["reflink", "copy", "symlink", "hardlink"])
def test_checkout(tmp_local_path, benchmark, link):
    (tmp_local_path / ".dvc").mkdir()

    fs_path = fspath(tmp_local_path / "dataset")
    odb = get_odb(type=[link])

    if not _test_links([link], localfs, odb.path, localfs, fs_path):
        pytest.skip(f"unsupported link type: {link}")

    gentree(tmp_local_path / "dataset", 1000, "50Mb")
    obj = build(tmp_local_path / "dataset", write=True)
    state = odb.state

    def setup():
        for path in (state.tmp_dir, fs_path):
            try:
                shutil.rmtree(path)
            except FileNotFoundError:
                pass
        State(state.root_dir, state.tmp_dir, state.ignore)  # recreate db

    assert benchmark.pedantic(
        checkout,
        setup=setup,
        args=(fs_path, localfs, obj, odb),
        kwargs={"state": state},
        rounds=5,
    )
