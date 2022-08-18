import os

from dvc_objects.fs import LocalFileSystem

from dvc_data.hashfile.utils import get_mtime_and_size


def test_mtime_and_size(tmp_path):
    directory = tmp_path / "dir"
    directory.mkdir(parents=True)
    dir_file = directory / "file"
    dir_file.write_text("dir_file", encoding="utf8")

    sub = directory / "sub"
    sub.mkdir(parents=True)
    subfile = sub / "file"
    subfile.write_text("sub_file", encoding="utf8")

    fs = LocalFileSystem(url=tmp_path)
    file_time, file_size = get_mtime_and_size(dir_file, fs)
    dir_time, dir_size = get_mtime_and_size(directory, fs)

    actual_file_size = os.path.getsize(dir_file)
    actual_dir_size = os.path.getsize(dir_file) + os.path.getsize(subfile)

    assert isinstance(file_time, str)
    assert isinstance(file_size, int)
    assert file_size == actual_file_size
    assert isinstance(dir_time, str)
    assert isinstance(dir_size, int)
    assert dir_size == actual_dir_size


def test_path_object_and_str_are_valid_types_get_mtime_and_size(tmp_path):
    directory = tmp_path / "dir"
    directory.mkdir()
    (directory / "file").write_text("dir_file_content")
    file = directory / "file"
    file.write_text("file_content", encoding="utf8")

    fs = LocalFileSystem(url=tmp_path)

    time, size = get_mtime_and_size(directory, fs)
    object_time, object_size = get_mtime_and_size(directory, fs)
    assert time == object_time
    assert size == object_size

    time, size = get_mtime_and_size(file, fs)
    object_time, object_size = get_mtime_and_size(file, fs)
    assert time == object_time
    assert size == object_size
