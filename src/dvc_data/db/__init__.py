from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .index import ObjectDBIndexBase


def get_odb(fs, fs_path, **config):
    from dvc_objects.fs import Schemes
    from dvc_objects.hashfile.db import HashFileDB

    from .local import LocalHashFileDB

    if fs.protocol == Schemes.LOCAL:
        return LocalHashFileDB(fs, fs_path, **config)

    return HashFileDB(fs, fs_path, **config)


def get_index(odb) -> "ObjectDBIndexBase":
    import hashlib

    from .index import ObjectDBIndex, ObjectDBIndexNoop

    cls = ObjectDBIndex if odb.tmp_dir else ObjectDBIndexNoop
    return cls(
        odb.tmp_dir,
        hashlib.sha256(
            odb.fs.unstrip_protocol(odb.path).encode("utf-8")
        ).hexdigest(),
    )
