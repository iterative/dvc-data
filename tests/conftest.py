import dvc_objects
import pytest


@pytest.fixture
def as_filesystem():
    return dvc_objects.fs.as_filesystem
