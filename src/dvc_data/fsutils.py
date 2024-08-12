from os import readlink, stat
from stat import S_ISDIR, S_ISLNK, S_ISREG
from typing import Any


def _localfs_info(path: str) -> dict[str, Any]:
    out = stat(path, follow_symlinks=False)
    if link := S_ISLNK(out.st_mode):
        out = stat(path, follow_symlinks=True)
    if S_ISDIR(out.st_mode):
        t = "directory"
    elif S_ISREG(out.st_mode):
        t = "file"
    else:
        t = "other"

    result = {
        "name": path,
        "size": out.st_size,
        "type": t,
        "created": out.st_ctime,
        "islink": link,
        "mode": out.st_mode,
        "uid": out.st_uid,
        "gid": out.st_gid,
        "mtime": out.st_mtime,
        "ino": out.st_ino,
        "nlink": out.st_nlink,
    }
    if link:
        result["destination"] = readlink(path)
    return result
