from os import fspath, stat
from pathlib import Path

import pytest
from dvc_objects.fs import localfs

from dvc_data.cli import build


def stat_files(files):
    for file in files:
        stat(file)


def make_dataset(tmp_path, num_files):
    dataset = tmp_path / "dataset"
    dataset.mkdir()
    for i in range(num_files):
        dataset.joinpath(str(i)).write_text(f"content {i}")
    return fspath(dataset)


@pytest.mark.parametrize("num_files", [1, 10, 100, 1000, 10000, 100000, 1_000_000])
def test_stat(tmp_path, benchmark, num_files):
    dataset = make_dataset(tmp_path, num_files)
    files = list(localfs.find(dataset))
    assert len(files) == num_files
    benchmark(stat_files, files)


@pytest.mark.parametrize("num_files", [1, 10, 100, 1000, 10000, 100000, 1_000_000])
def test_stat_cache(monkeypatch, tmp_path, benchmark, num_files):
    monkeypatch.chdir(tmp_path)
    (tmp_path / ".dvc").mkdir()
    dataset = make_dataset(tmp_path, num_files)

    build(Path(dataset), write=True)

    files = list(localfs.find(fspath(tmp_path / ".dvc" / "cache" / "files" / "md5")))
    assert len(files) == num_files + 1
    benchmark(stat_files, files)
