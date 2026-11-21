"""Python equivalents of the session ``mv``/``cp``/``cat`` commands and ``Maintenance Scripts/*``.

UC Volumes are FUSE-mounted under ``/Volumes`` on Databricks compute, so plain file APIs work on them.
"""
from __future__ import annotations

import fnmatch
import shutil
from pathlib import Path


def remove_file(directory: str, filename: str) -> bool:
    """``Maintenance Scripts/remove_file``: delete ``directory/filename`` if it is a regular file."""
    target = Path(directory, filename)
    if target.is_file():
        target.unlink()
        return True
    return False


def archive_files(input_dir: str, file_dest: str, pay_period: str) -> list[str]:
    """``Maintenance Scripts/archive_files``: move every file to ``<dest>/<basename .txt>_P<pay period>.txt``."""
    dest = Path(file_dest)
    dest.mkdir(parents=True, exist_ok=True)
    moved = []
    for src in sorted(Path(input_dir).iterdir()):
        if not src.is_file() or src.name.startswith("."):
            continue
        prefix = src.name[: -len(".txt")] if src.name.endswith(".txt") and src.name != ".txt" else src.name
        target = dest / f"{prefix}_P{pay_period}.txt"
        shutil.move(str(src), str(target))
        moved.append(str(target))
    return moved


def move(src: str, dst: str) -> str:
    Path(dst).parent.mkdir(parents=True, exist_ok=True)
    return str(shutil.move(src, dst))


def copy(src: str, dst: str) -> str:
    Path(dst).parent.mkdir(parents=True, exist_ok=True)
    return str(shutil.copyfile(src, dst))


def move_glob(src_dir: str, pattern: str, dst_dir: str) -> list[str]:
    """``cd src_dir; mv <pattern> dst_dir/``. Like the shell, fails when nothing matches."""
    matches = sorted(p for p in Path(src_dir).iterdir() if fnmatch.fnmatchcase(p.name, pattern))
    if not matches:
        raise FileNotFoundError(f"mv: cannot stat '{Path(src_dir, pattern)}': No such file or directory")
    Path(dst_dir).mkdir(parents=True, exist_ok=True)
    return [str(shutil.move(str(p), str(Path(dst_dir, p.name)))) for p in matches]


def concat(srcs: list[str], dst: str) -> str:
    """``cat a b > dst``."""
    Path(dst).parent.mkdir(parents=True, exist_ok=True)
    with open(dst, "wb") as out:
        for src in srcs:
            with open(src, "rb") as f:
                shutil.copyfileobj(f, out)
    return dst
