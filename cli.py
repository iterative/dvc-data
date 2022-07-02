#! /usr/bin/env python3

import enum
import hashlib
import os
import posixpath
import sys
from pathlib import Path
from posixpath import relpath
from typing import Optional

import typer  # pylint: disable=import-error
from dvc_objects.errors import ObjectFormatError
from dvc_objects.fs import LocalFileSystem, MemoryFileSystem
from dvc_objects.fs.callbacks import Callback
from dvc_objects.fs.utils import human_readable_to_bytes

from dvc_data.hashfile.db import HashFileDB
from dvc_data.hashfile.hash import file_md5 as _file_md5
from dvc_data.hashfile.hash import fobj_md5 as _fobj_md5
from dvc_data.hashfile.hash_info import HashInfo
from dvc_data.hashfile.state import State
from dvc_data.objects.tree import Tree
from dvc_data.stage import stage as _stage
from dvc_data.transfer import transfer as _transfer

file_type = typer.Argument(
    ...,
    exists=True,
    file_okay=True,
    dir_okay=False,
    readable=True,
    resolve_path=True,
    allow_dash=True,
    path_type=str,
)
dir_file_type = typer.Argument(
    ...,
    exists=True,
    file_okay=True,
    dir_okay=True,
    readable=True,
    resolve_path=True,
    allow_dash=True,
    path_type=str,
)

HashEnum = enum.Enum(  # type: ignore[misc]
    "HashEnum", {h: h for h in sorted(hashlib.algorithms_available)}
)
SIZE_HELP = "Human readable size, eg: '1kb', '100Mb', '10GB' etc"
ODB_PATH = typer.Option(
    ".dvc/cache", help="Path to the root of the odb", envvar="ODB_PATH"
)

app = typer.Typer(name="dvc-data", help="dvc-data testingtool")


@app.command(name="hash", help="Compute checksum of the file")
def hash_file(
    file: Path = file_type,
    name: HashEnum = typer.Option("md5", "-n", "--name"),
    progress: bool = typer.Option(False, "--progress", "-p"),
    text: Optional[bool] = typer.Option(None, "--text/--binary", "-t/-b"),
):
    path = relpath(file)
    hash_name = name.value
    callback = Callback.as_callback()
    if progress:
        callback = Callback.as_tqdm_callback(
            desc=f"hashing {path} with {hash_name}", bytes=True
        )

    with callback:
        if path == "-":
            fobj = callback.wrap_attr(sys.stdin.buffer)
            hash_value = _fobj_md5(fobj, text=text, name=hash_name)
        else:
            hash_value = _file_md5(
                path, name=hash_name, callback=callback, text=text
            )
    typer.echo(f"{hash_name}: {hash_value}")


@app.command(help="Generate sparse file")
def gensparse(
    file: Path = typer.Argument(..., allow_dash=True),
    size: str = typer.Argument(..., help=SIZE_HELP),
):
    with file.open("wb") as f:
        f.seek(human_readable_to_bytes(size) - 1)
        f.write(b"\0")


@app.command(help="Generate file with random contents")
def genrand(
    file: Path = typer.Argument(..., allow_dash=True),
    size: str = typer.Argument(..., help=SIZE_HELP),
):
    with file.open("wb") as f:
        f.write(os.urandom(human_readable_to_bytes(size)))


def from_shortoid(odb: HashFileDB, oid: str) -> str:
    oid = oid if oid != "-" else sys.stdin.read().strip()
    try:
        return odb.exists_prefix(oid)
    except KeyError as exc:
        typer.echo(f"Not a valid {oid=}", err=True)
        raise typer.Exit(1) from exc
    except ValueError as exc:
        typer.echo(f"Ambiguous {oid=}", err=True)
        raise typer.Exit(1) from exc


def get_odb(path):
    state = State(tmp_dir=os.path.join(path, "tmp"))
    return HashFileDB(LocalFileSystem(), path, state=state)


@app.command(help="Oid to path")
def o2p(oid: str = typer.Argument(..., allow_dash=True), db: str = ODB_PATH):
    odb = get_odb(db)
    path = odb.oid_to_path(from_shortoid(odb, oid))
    typer.echo(path)


@app.command(help="Path to Oid")
def p2o(path: Path = typer.Argument(..., allow_dash=True), db: str = ODB_PATH):
    odb = get_odb(db)
    fs_path = relpath(path)
    if fs_path == "-":
        fs_path = sys.stdin.read().strip()

    oid = odb.path_to_oid(fs_path)
    typer.echo(oid)


@app.command(help="Provide content of the objects")
def cat(
    oid: str = typer.Argument(..., allow_dash=True),
    db: str = ODB_PATH,
    check: bool = typer.Option(False, "--check", "-c"),
):
    odb = get_odb(db)
    oid = from_shortoid(odb, oid)
    if check:
        try:
            return odb.check(oid, check_hash=True)
        except ObjectFormatError as exc:
            typer.echo(exc, err=True)
            raise typer.Exit(1) from exc

    path = odb.oid_to_path(oid)
    contents = odb.fs.cat_file(path)
    return typer.echo(contents)


@app.command(help="Stage and optionally write object to the database")
def stage(
    path: Path = dir_file_type,
    db: str = ODB_PATH,
    write: bool = typer.Option(False, "--write", "-w"),
    shallow: bool = False,
):
    odb = get_odb(db)
    fs_path = relpath(path)

    fs = odb.fs
    if fs_path == "-":
        fs = MemoryFileSystem()
        fs.put_file(sys.stdin.buffer, fs_path)

    staging, _, obj = _stage(odb, fs_path, fs, name="md5")
    if write:
        _transfer(
            staging,
            odb,
            {obj.hash_info},
            hardlink=True,
            shallow=shallow,
        )
    typer.echo(obj)


@app.command(help="List object")
def ls(oid: str = typer.Argument(..., allow_dash=True), db: str = ODB_PATH):
    odb = get_odb(db)
    oid = from_shortoid(odb, oid)
    try:
        tree = Tree.load(odb, HashInfo("md5", oid))
    except ObjectFormatError as exc:
        typer.echo(str(exc), err=True)
        raise typer.Exit(1) from exc

    for key, (_, hash_info) in tree.iteritems():
        typer.echo(f"{hash_info.value}\t{posixpath.join(*key)}")


@app.command(help="Verify objects in the database")
def fsck(db: str = ODB_PATH):
    odb = get_odb(db)
    ret = 0
    for oid in odb.all():
        try:
            odb.check(oid, check_hash=True)
        except ObjectFormatError as exc:
            ret = 1
            typer.echo(exc)
    raise typer.Exit(ret)


if __name__ == "__main__":
    app()
