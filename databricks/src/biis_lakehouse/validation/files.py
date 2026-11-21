"""Byte/line diff of fixed-width outbound files (AFPS, CDC, NIH, OIG, FDA, message files, ...).

Pure Python on POSIX paths so it works locally and on UC Volumes (``/Volumes/...`` FUSE mount).
A path may be a plain file or a Spark text-output directory (``write_fixed_width``), in which case
its ``part-*`` files are concatenated in name order.
"""
from __future__ import annotations

import difflib
import hashlib
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional, Sequence

from biis_lakehouse.common.fixed_width import Field

# Above this many lines (and equal line counts) lines are compared positionally instead of aligned with difflib.
MAX_ALIGN_LINES = 200_000


def _local(path: str) -> Path:
    return Path(path[len("dbfs:"):] if path.startswith("dbfs:") else path)


def read_output_bytes(path: str) -> bytes:
    p = _local(path)
    if p.is_dir():
        parts = sorted(x for x in p.iterdir() if x.name.startswith("part-") and not x.name.endswith(".crc"))
        return b"".join(x.read_bytes() for x in parts)
    return p.read_bytes()


@dataclass
class LineDiff:
    kind: str  # "changed" | "legacy_only" | "target_only"
    legacy_line: Optional[int]  # 1-based
    target_line: Optional[int]
    legacy: Optional[str]
    target: Optional[str]
    first_col: Optional[int] = None  # 1-based column of first differing byte
    field: Optional[str] = None  # fixed-width field containing first_col


@dataclass
class FileDiff:
    legacy_path: str
    target_path: str
    legacy_bytes: int
    target_bytes: int
    legacy_sha256: str
    target_sha256: str
    legacy_lines: int
    target_lines: int
    byte_identical: bool
    lines_identical: bool
    bad_length_lines: list[int] = field(default_factory=list)  # target lines != record length
    diff_count: int = 0
    diffs: list[LineDiff] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return self.lines_identical and not self.bad_length_lines

    def summary(self) -> str:
        status = "OK" if self.ok else "MISMATCH"
        s = (
            f"[{status}] {self.target_path}: lines legacy={self.legacy_lines} target={self.target_lines}, "
            f"bytes legacy={self.legacy_bytes} target={self.target_bytes}, byte_identical={self.byte_identical}"
        )
        if self.bad_length_lines:
            s += f"\n  - {len(self.bad_length_lines)} target lines with wrong record length, e.g. {self.bad_length_lines[:5]}"
        for d in self.diffs:
            where = f" col {d.first_col}" + (f" ({d.field})" if d.field else "") if d.first_col else ""
            s += f"\n  - {d.kind} legacy#{d.legacy_line} target#{d.target_line}{where}: {d.legacy!r} != {d.target!r}"
        if self.diff_count > len(self.diffs):
            s += f"\n  - ... {self.diff_count - len(self.diffs)} more"
        return s


def _field_at(col: int, fields: Optional[Sequence[Field]]) -> Optional[str]:
    pos = 1
    for f in fields or ():
        if pos <= col < pos + f.length:
            return f.name
        pos += f.length
    return None


def _first_col(a: str, b: str) -> int:
    for i, (x, y) in enumerate(zip(a, b)):
        if x != y:
            return i + 1
    return min(len(a), len(b)) + 1


def diff_fixed_width_files(
    legacy_path: str,
    target_path: str,
    *,
    fields: Optional[Sequence[Field]] = None,
    encoding: str = "latin-1",
    ignore_line_endings: bool = True,
    ignore_trailing_spaces: bool = False,
    sort_lines: bool = False,
    max_diffs: int = 20,
) -> FileDiff:
    """Compare a PowerCenter-produced file with the Databricks-produced one.

    ``byte_identical`` is the strict cutover criterion. ``lines_identical`` applies the relaxations:
    ``ignore_line_endings`` (CRLF vs LF, missing final newline), ``ignore_trailing_spaces`` and
    ``sort_lines`` (for outputs whose row order PowerCenter never guaranteed). With ``fields`` the
    first differing column is mapped to a field name and target lines whose length differs from the
    record length are listed. Lines are aligned with :mod:`difflib`, so one inserted record is reported
    once rather than shifting every following line.
    """
    a_raw, b_raw = read_output_bytes(legacy_path), read_output_bytes(target_path)
    a, b = a_raw.decode(encoding), b_raw.decode(encoding)
    if ignore_line_endings:
        a_lines, b_lines = a.splitlines(), b.splitlines()
    else:
        a_lines, b_lines = a.split("\n"), b.split("\n")
    bad_len: list[int] = []
    if fields:
        reclen = sum(f.length for f in fields)
        last = len(b_lines) - 1
        bad_len = [i + 1 for i, line in enumerate(b_lines) if len(line) != reclen and not (i == last and line == "")]
    if ignore_trailing_spaces:
        a_lines, b_lines = [x.rstrip(" ") for x in a_lines], [x.rstrip(" ") for x in b_lines]
    if sort_lines:
        a_lines, b_lines = sorted(a_lines), sorted(b_lines)

    diffs: list[LineDiff] = []
    count = 0

    def add(d: LineDiff) -> None:
        nonlocal count
        count += 1
        if len(diffs) < max_diffs:
            diffs.append(d)

    def changed(i: int, j: int) -> None:
        col = _first_col(a_lines[i], b_lines[j])
        add(LineDiff("changed", i + 1, j + 1, a_lines[i], b_lines[j], col, _field_at(col, fields)))

    if a_lines == b_lines:
        pass
    elif len(a_lines) == len(b_lines) and len(a_lines) > MAX_ALIGN_LINES:
        for i, (x, y) in enumerate(zip(a_lines, b_lines)):
            if x != y:
                changed(i, i)
    else:
        sm = difflib.SequenceMatcher(None, a_lines, b_lines, autojunk=False)
        for op, i1, i2, j1, j2 in sm.get_opcodes():
            if op == "equal":
                continue
            paired = min(i2 - i1, j2 - j1) if op == "replace" else 0
            for k in range(paired):
                changed(i1 + k, j1 + k)
            for i in range(i1 + paired, i2):
                add(LineDiff("legacy_only", i + 1, None, a_lines[i], None))
            for j in range(j1 + paired, j2):
                add(LineDiff("target_only", None, j + 1, None, b_lines[j]))

    return FileDiff(
        legacy_path=legacy_path,
        target_path=target_path,
        legacy_bytes=len(a_raw),
        target_bytes=len(b_raw),
        legacy_sha256=hashlib.sha256(a_raw).hexdigest(),
        target_sha256=hashlib.sha256(b_raw).hexdigest(),
        legacy_lines=len(a_lines),
        target_lines=len(b_lines),
        byte_identical=a_raw == b_raw,
        lines_identical=count == 0,
        bad_length_lines=bad_len,
        diff_count=count,
        diffs=diffs,
    )
