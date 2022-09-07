from typing import Any, ClassVar, Dict, List, Optional

from attrs import Attribute, asdict, define, fields_dict
from dvc_objects.fs.utils import is_exec


def _filter_default_or_none(field: Attribute, value: Any) -> bool:
    return value is not None and value != field.default


@define
class Meta:
    PARAM_ISDIR: ClassVar[str] = "isdir"
    PARAM_SIZE: ClassVar[str] = "size"
    PARAM_NFILES: ClassVar[str] = "nfiles"
    PARAM_ISEXEC: ClassVar[str] = "isexec"
    PARAM_VERSION_ID: ClassVar[str] = "version_id"
    PARAM_ETAG: ClassVar[str] = "etag"
    PARAM_CHECKSUM: ClassVar[str] = "checksum"
    PARAM_MD5: ClassVar[str] = "md5"

    fields: ClassVar[List[str]]

    isdir: bool = False
    size: Optional[int] = None
    nfiles: Optional[int] = None
    isexec: bool = False
    version_id: Optional[str] = None
    etag: Optional[str] = None
    checksum: Optional[str] = None
    md5: Optional[str] = None

    @classmethod
    def from_info(
        cls, info: Dict[str, Any], protocol: Optional[str] = None
    ) -> "Meta":
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

        return Meta(
            isdir=info["type"] == "directory",
            size=info.get("size"),
            isexec=is_exec(info.get("mode", 0)),
            version_id=version_id,
            etag=etag,
            checksum=checksum,
            md5=info.get("md5"),
        )

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "Meta":
        kwargs = {}
        for field in cls.fields:
            if field in d:
                kwargs[field] = d[field]
        return cls(**kwargs)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self, recurse=False, filter=_filter_default_or_none)


Meta.fields = list(fields_dict(Meta))
