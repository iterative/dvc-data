import pytest
from dvc_objects.fs.memory import MemoryFileSystem

from dvc_data.hashfile.istextfile import istextblock, istextfile

pytestmark = pytest.mark.parametrize(
    "block, expected",
    [
        (b"", True),
        (b"text", True),
        (b"\x00\x001", False),
        (
            (
                b"True\x80\x04\x95\x1a\x00\x00\x00\x00\x00\x00\x00\x8c\x08\r\n"
                b"__main__\x94\x8c\x06Animal\x94\x93\x94)\x81\x94."
            ),
            False,
        ),
    ],
    ids=["empty", "text", "binary", "long_binary"],
)


def test_istextblock(block, expected):
    assert istextblock(block) is expected


def test_istextfile(block, expected):
    fs = MemoryFileSystem(global_store=False)
    fs.pipe_file("/file", block)

    assert istextfile("/file", fs) is expected
