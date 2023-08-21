from typing import Any, ClassVar, Dict, List, Optional

from attrs import define, field, fields_dict
from dvc_objects.fs.utils import is_exec


@define(hash=True)
class Meta:
    PARAM_ISDIR: ClassVar[str] = "isdir"
    PARAM_SIZE: ClassVar[str] = "size"
    PARAM_NFILES: ClassVar[str] = "nfiles"
    PARAM_ISEXEC: ClassVar[str] = "isexec"
    PARAM_VERSION_ID: ClassVar[str] = "version_id"
    PARAM_ETAG: ClassVar[str] = "etag"
    PARAM_CHECKSUM: ClassVar[str] = "checksum"
    PARAM_MD5: ClassVar[str] = "md5"
    PARAM_INODE: ClassVar[str] = "inode"
    PARAM_MTIME: ClassVar[str] = "mtime"
    PARAM_REMOTE: ClassVar[str] = "remote"

    fields: ClassVar[List[str]]

    isdir: bool = False
    size: Optional[int] = None
    nfiles: Optional[int] = None
    isexec: bool = False
    version_id: Optional[str] = None
    etag: Optional[str] = None
    checksum: Optional[str] = None
    md5: Optional[str] = None
    inode: Optional[int] = None
    mtime: Optional[float] = None

    remote: Optional[str] = field(default=None, eq=False)

    @classmethod
    def from_info(cls, info: Dict[str, Any], protocol: Optional[str] = None) -> "Meta":
        etag = info.get("etag")
        checksum = info.get("checksum")

        if protocol == "s3" and "ETag" in info:
            etag = info["ETag"].strip('"')
        elif protocol == "gs" and "etag" in info:
            import base64

            etag = base64.b64decode(info["etag"]).hex()
        elif (
            protocol
            and protocol.startswith("http")
            and ("ETag" in info or "Content-MD5" in info)
        ):
            checksum = info.get("ETag") or info.get("Content-MD5")

        version_id = info.get("version_id")
        if protocol == "s3" and "VersionId" in info:
            version_id = info.get("VersionId")
        elif protocol == "gs" and "generation" in info:
            version_id = info.get("generation")

        return Meta(
            isdir=info["type"] == "directory",
            size=info.get("size"),
            isexec=is_exec(info.get("mode", 0)),
            version_id=version_id,
            etag=etag,
            checksum=checksum,
            md5=info.get("md5"),
            inode=info.get("ino"),
            mtime=info.get("mtime"),
            remote=info.get("remote"),
        )

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "Meta":
        kwargs = {}
        for field_ in cls.fields:
            if field_ in d:
                kwargs[field_] = d[field_]
        return cls(**kwargs)

    def to_dict(self) -> Dict[str, Any]:
        ret: Dict[str, Any] = {}

        if self.isdir:
            ret[self.PARAM_ISDIR] = self.isdir

        if self.size is not None:
            ret[self.PARAM_SIZE] = self.size

        if self.nfiles is not None:
            ret[self.PARAM_NFILES] = self.nfiles

        if self.isexec:
            ret[self.PARAM_ISEXEC] = self.isexec

        if self.version_id:
            ret[self.PARAM_VERSION_ID] = self.version_id

        if self.etag:
            ret[self.PARAM_ETAG] = self.etag

        if self.checksum:
            ret[self.PARAM_CHECKSUM] = self.checksum

        if self.md5:
            ret[self.PARAM_MD5] = self.md5

        if self.remote:
            ret[self.PARAM_REMOTE] = self.remote

        return ret


Meta.fields = list(fields_dict(Meta))
