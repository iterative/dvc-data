import hashlib
import io
import logging
from typing import TYPE_CHECKING, BinaryIO, Optional, Tuple, cast

from dvc_objects.fs import localfs
from dvc_objects.fs.callbacks import DEFAULT_CALLBACK, Callback, TqdmCallback
from tqdm.utils import CallbackIOWrapper

from .hash_info import HashInfo
from .istextfile import DEFAULT_CHUNK_SIZE, istextblock
from .meta import Meta

logger = logging.getLogger(__name__)


if TYPE_CHECKING:
    from dvc_objects.fs.base import AnyFSPath, FileSystem

    from .state import StateBase


def dos2unix(data: bytes) -> bytes:
    return data.replace(b"\r\n", b"\n")


algorithms_available = hashlib.algorithms_available | {
    "blake3",
    "md5-dos2unix",
}
DEFAULT_ALGORITHM = "md5"


def get_hasher(name: str) -> "hashlib._Hash":
    if name == "blake3":
        from blake3 import blake3

        return blake3(max_threads=blake3.AUTO)  # type: ignore[return-value]
    if name == "md5-dos2unix":
        name = "md5"

    try:
        return getattr(hashlib, name)()
    except AttributeError:
        return hashlib.new(name)


class HashStreamFile(io.IOBase):
    def __init__(
        self,
        fobj: BinaryIO,
        hash_name: str = DEFAULT_ALGORITHM,
    ) -> None:
        self.fobj = fobj
        self.total_read = 0
        hash_name = hash_name.lower()
        self.hasher = get_hasher(hash_name)
        super().__init__()

    def readable(self) -> bool:
        return True

    def tell(self) -> int:
        return self.fobj.tell()

    def read(self, n=-1) -> bytes:
        chunk = self.fobj.read(n)
        self.hasher.update(chunk)
        self.total_read += len(chunk)
        return chunk

    @property
    def hash_value(self) -> str:
        return self.hasher.hexdigest()

    @property
    def hash_name(self) -> str:
        return self.hasher.name


class Dos2UnixHashStreamFile(HashStreamFile):
    def read(self, n=-1) -> bytes:
        # ideally, we want the heuristics to be applied in a similar way,
        # regardless of the size of the first chunk,
        # for which we may need to buffer till DEFAULT_CHUNK_SIZE.
        assert n >= DEFAULT_CHUNK_SIZE
        chunk = self.fobj.read(n)
        is_text = istextblock(chunk[:DEFAULT_CHUNK_SIZE]) if chunk else False

        data = dos2unix(chunk) if is_text else chunk
        self.hasher.update(data)
        self.total_read += len(data)
        return chunk


def get_hash_stream(fobj: BinaryIO, name: str = DEFAULT_ALGORITHM) -> HashStreamFile:
    cls = Dos2UnixHashStreamFile if name == "md5-dos2unix" else HashStreamFile
    return cls(fobj, hash_name=name)


def fobj_md5(
    fobj: BinaryIO,
    chunk_size: int = 2**20,
    name: str = DEFAULT_ALGORITHM,
) -> str:
    stream = get_hash_stream(fobj, name=name)
    while True:
        data = stream.read(chunk_size)
        if not data:
            break
    return stream.hash_value


def file_md5(
    fname: "AnyFSPath",
    fs: "FileSystem" = localfs,
    callback: "Callback" = DEFAULT_CALLBACK,
    name: str = DEFAULT_ALGORITHM,
    size: Optional[int] = None,
) -> str:
    if size is None:
        size = fs.size(fname) or 0

    callback.set_size(size)
    with fs.open(fname, "rb") as fobj:
        wrapped = cast("BinaryIO", CallbackIOWrapper(callback.relative_update, fobj))
        return fobj_md5(wrapped, name=name)


def _hash_file(
    path: "AnyFSPath",
    fs: "FileSystem",
    name: str,
    callback: "Callback" = DEFAULT_CALLBACK,
    info: Optional[dict] = None,
) -> Tuple["str", Meta]:
    info = info or fs.info(path)
    meta = Meta.from_info(info, fs.protocol)

    value = getattr(meta, name, None)
    if value:
        assert not value.endswith(".dir")
        return value, meta

    if hasattr(fs, name):
        func = getattr(fs, name)
        return str(func(path)), meta

    if name in algorithms_available:
        return (
            file_md5(path, fs, callback=callback, size=meta.size, name=name),
            meta,
        )
    raise NotImplementedError


class LargeFileHashingCallback(TqdmCallback):
    """Callback that only shows progress bar if self.size > LARGE_FILE_SIZE."""

    LARGE_FILE_SIZE = 2**30

    def __init__(self, *args, **kwargs):
        kwargs.setdefault("bytes", True)
        super().__init__(*args, **kwargs)
        self._logged = False
        self.fname = kwargs.get("desc", "")

    # TqdmCallback force renders progress bar on `set_size`.
    set_size = Callback.set_size

    def call(self, hook_name=None, **kwargs):
        if self.size and self.size > self.LARGE_FILE_SIZE:
            if not self._logged:
                logger.info(
                    "Computing md5 for a large file %r. This is only done once.",
                    self.fname,
                )
                self._logged = True
            super().call()


def hash_file(
    path: "AnyFSPath",
    fs: "FileSystem",
    name: str,
    state: Optional["StateBase"] = None,
    callback: Optional["Callback"] = None,
    info: Optional[dict] = None,
) -> Tuple["Meta", "HashInfo"]:
    if state:
        meta, hash_info = state.get(path, fs, info=info)
        if hash_info and hash_info.name == name:
            return meta, hash_info

    cb = callback or LargeFileHashingCallback(desc=path)
    with cb:
        hash_value, meta = _hash_file(path, fs, name, callback=cb, info=info)
    hash_info = HashInfo(name, hash_value)
    if state:
        assert ".dir" not in hash_info.value
        state.save(path, fs, hash_info, info=info)

    return meta, hash_info
