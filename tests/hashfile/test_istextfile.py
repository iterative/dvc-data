import os
import sys

import pytest

from dvc_data.hashfile.istextfile import istextblock, istextfile

pytestmark = pytest.mark.parametrize(
    "block, expected",
    [
        (b"", True),
        (b"text", True),
        (b"\x00\x001", False),
        (
            b"True\x80\x04\x95\x1a\x00\x00\x00\x00\x00\x00\x00\x8c\x08\r\n"
            b"__main__\x94\x8c\x06Animal\x94\x93\x94)\x81\x94.",
            False,
        ),
    ],
    ids=["empty", "text", "binary", "long_binary"],
)


def test_istextblock(block, expected):
    assert istextblock(block) is expected


@pytest.mark.parametrize(
    "tmp_upath",
    [
        "local",
        pytest.param(
            "s3",
            marks=pytest.mark.skipif(
                sys.version_info >= (3, 11), reason="no aiohttp"
            ),
        ),
    ],
    indirect=True,
)
def test_istextfile(tmp_upath, block, expected):
    path = tmp_upath / "file"
    path.write_bytes(block)
    assert istextfile(os.fspath(path), path.fs) is expected
