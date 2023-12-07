import shutil
from os import fspath
from pathlib import Path
from tempfile import TemporaryDirectory

import pytest
from dvc_objects.fs import localfs
from dvc_objects.fs.generic import test_links as _test_links

from dvc_data.cli import build, gentree, get_odb
from dvc_data.hashfile.checkout import checkout
from dvc_data.hashfile.state import State


@pytest.fixture
def repo(request, monkeypatch):
    """Create a dvc data repo within pytest'scache directory.
    The cache directory by default, is in the root of the repo, where reflink
    may be supported.
    """
    cache = request.config.cache
    path = cache.mkdir("dvc_data_repo")
    with TemporaryDirectory(dir=path) as tmp_dir:
        monkeypatch.chdir(tmp_dir)
        path = Path(tmp_dir)
        (path / ".dvc").mkdir()
        yield path


@pytest.mark.parametrize("link", ["reflink", "copy", "symlink", "hardlink"])
def test_checkout(repo, benchmark, link):
    fs_path = fspath(repo / "dataset")
    odb = get_odb(type=[link])

    if not _test_links([link], localfs, odb.path, localfs, fs_path):
        pytest.skip(f"unsupported link type: {link}")

    gentree(repo / "dataset", 1000, "50Mb")
    obj = build(repo / "dataset", write=True)
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
        rounds=10,
        warmup_rounds=2,
    )
