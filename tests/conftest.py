import pathlib
import shlex
import subprocess
import time

import pytest
import requests
from dvc_objects.fs.implementations.local import LocalFileSystem
from funcy import silent
from upath import UPath


def wait_until(pred, timeout: float, pause: float = 0.1):
    start = time.perf_counter()
    while (time.perf_counter() - start) < timeout:
        value = pred()
        if value:
            return value
        time.sleep(pause)
    raise TimeoutError("timed out waiting")


class MockedS3Server:
    def __init__(self, port: int = 5555):
        self.endpoint_url = f"http://127.0.0.1:{port}"
        self.port = port
        self.proc = None

    def __enter__(self):
        try:
            # should fail since we didn't start server yet
            r = requests.get(self.endpoint_url)
        except:  # noqa: E722, B001
            pass
        else:
            if r.ok:
                raise RuntimeError("moto server already up")
        self.proc = subprocess.Popen(
            shlex.split("moto_server s3 -p %s" % self.port)
        )
        wait_until(silent(lambda: requests.get(self.endpoint_url).ok), 5)
        return self

    def close(self):
        if self.proc is not None:
            self.proc.terminate()
            self.proc.wait()
        self.proc = None

    def __exit__(self, *exc_args):
        self.close()


@pytest.fixture
def s3_server(monkeypatch):
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "foo")
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "foo")
    with MockedS3Server() as server:
        yield server


@pytest.fixture
def s3path(s3_server):
    path = UPath(
        "s3://test",
        client_kwargs={"endpoint_url": s3_server.endpoint_url},
    )
    path.mkdir()
    yield path


class LocalPath(type(pathlib.Path())):  # type: ignore[misc]
    def __init__(self, *args):
        super().__init__()
        if not getattr(self._accessor, "_fs", None):
            self._accessor._fs = LocalFileSystem()

    @property
    def fs(self):
        return self._accessor._fs


@pytest.fixture
def local_path(tmp_path_factory, monkeypatch):
    ret = LocalPath(tmp_path_factory.mktemp("dvc-obj"))
    monkeypatch.chdir(ret)
    yield ret


@pytest.fixture
def tmp_upath(request):
    param = getattr(request, "param", "local")
    if param == "local":
        return request.getfixturevalue("local_path")
    elif param == "s3":
        return request.getfixturevalue("s3path")
    raise ValueError(f"unknown {param=}")
