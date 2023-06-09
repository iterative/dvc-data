from os import fspath

from dvc_objects.fs import LocalFileSystem

from dvc_data.hashfile.hash import file_md5


def test_file_md5(tmp_path):
    foo = tmp_path / "foo"
    foo.write_text("foo content", encoding="utf8")

    fs = LocalFileSystem()
    assert file_md5(fspath(foo), fs) == file_md5(fspath(foo), fs)


def test_file_md5_dos2unix(tmp_path):
    fs = LocalFileSystem()
    cr = tmp_path / "cr"
    crlf = tmp_path / "crlf"
    cr.write_bytes(b"a\nb\nc")
    crlf.write_bytes(b"a\r\nb\r\nc")
    assert file_md5(fspath(cr), fs, name="md5-dos2unix") == file_md5(
        fspath(crlf), fs, name="md5-dos2unix"
    )
