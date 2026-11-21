"""Minimal SQL*Plus replacement: parse ``databricks/sql`` files, render variables, run on Spark, spool.

Statement rules (Databricks SQL, one statement per ``;``):

- ``--`` comments are ignored, except directive lines ``-- @name value`` that apply to the next statement:
  ``@label`` (spool title), ``@metric NAME`` (first column of the first row is returned as ``NAME``),
  ``@assert_empty MESSAGE`` (the query must return no rows, otherwise the task fails).
- ``${var}`` placeholders are substituted before execution (schemas, ``load_date`` ...).
- Any Spark error propagates and fails the job task (``WHENEVER SQLERROR EXIT FAILURE`` semantics).
"""
from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from string import Template
from typing import Mapping

from biis_lakehouse.context import RunContext

log = logging.getLogger(__name__)

_DIRECTIVE = re.compile(r"^--\s*@(\w+)\s*(.*)$")
_QUERY_START = re.compile(r"^\s*\(?\s*(SELECT|WITH|SHOW|DESCRIBE|DESC|EXPLAIN)\b", re.IGNORECASE)


class SqlStepError(RuntimeError):
    """A ported SQL step failed (stands in for SQL*Plus ``ERROR`` lines / non-zero exit)."""


@dataclass(frozen=True)
class Statement:
    sql: str
    directives: Mapping[str, str] = field(default_factory=dict)


def _strip_inline_comment(line: str) -> str:
    in_quote = False
    i = 0
    while i < len(line):
        ch = line[i]
        if ch == "'":
            in_quote = not in_quote
        elif not in_quote and line.startswith("--", i):
            return line[:i]
        i += 1
    return line


def _ends_statement(line: str) -> bool:
    in_quote = False
    last = ""
    for ch in line:
        if ch == "'":
            in_quote = not in_quote
        if not ch.isspace():
            last = ch
    return not in_quote and last == ";"


def parse_sql(text: str) -> list[Statement]:
    statements: list[Statement] = []
    buf: list[str] = []
    directives: dict[str, str] = {}

    def flush() -> None:
        nonlocal buf, directives
        sql = "\n".join(buf).strip().rstrip(";").strip()
        if sql:
            statements.append(Statement(sql, dict(directives)))
        buf, directives = [], {}

    for line in text.splitlines():
        stripped = line.strip()
        if stripped.startswith("--"):
            m = _DIRECTIVE.match(stripped)
            if m and not buf:
                directives[m.group(1)] = m.group(2).strip()
            continue
        code = _strip_inline_comment(line)
        if not code.strip():
            continue
        buf.append(code.rstrip())
        if _ends_statement(code):
            flush()
    flush()
    return statements


def load_sql(path: Path) -> list[Statement]:
    return parse_sql(path.read_text())


def render(sql: str, variables: Mapping[str, str]) -> str:
    try:
        return Template(sql).substitute(variables)
    except KeyError as exc:
        raise SqlStepError(f"undefined SQL variable ${{{exc.args[0]}}} in: {sql[:200]}") from exc


def is_query(sql: str) -> bool:
    return bool(_QUERY_START.match(sql))


class Spool:
    """Collects what SQL*Plus wrote to the spool/log file; flushed to the landing zone at the end."""

    def __init__(self, path: str | None):
        self.path = path
        self.lines: list[str] = []

    def write(self, text: str = "") -> None:
        for line in str(text).splitlines() or [""]:
            log.info(line)
            self.lines.append(line)

    def table(self, columns: list[str], rows: list) -> None:
        self.write(" | ".join(columns))
        for row in rows:
            self.write(" | ".join("" if v is None else str(v) for v in row))
        self.write(f"{len(rows)} row(s) selected.")

    def close(self) -> None:
        if not self.path:
            return
        p = Path(self.path)
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text("\n".join(self.lines) + "\n")


def run_statements(
    ctx: RunContext, statements: list[Statement], variables: Mapping[str, str], spool: Spool, source: str = ""
) -> dict[str, object]:
    """Execute ``statements`` in order; returns ``@metric`` values."""
    metrics: dict[str, object] = {}
    for idx, st in enumerate(statements, 1):
        sql = render(st.sql, variables)
        label = st.directives.get("label") or f"{source} #{idx}"
        spool.write(f"-- {label}")
        spool.write(f"SQL> {sql}")
        started = datetime.now()
        try:
            df = ctx.spark.sql(sql)
            rows = df.collect() if (is_query(sql) or df.columns) else []
        except Exception as exc:
            spool.write(f"ERROR at {label}: {exc}")
            raise SqlStepError(f"{label} failed: {exc}") from exc
        if df.columns:
            spool.table(df.columns, rows)
        spool.write(f"Elapsed: {datetime.now() - started}")
        if "metric" in st.directives:
            metrics[st.directives["metric"]] = rows[0][0] if rows else None
        if "assert_empty" in st.directives and rows:
            msg = st.directives["assert_empty"] or "check query returned rows"
            spool.write(f"ERROR: {msg}")
            raise SqlStepError(f"{label}: {msg}: {[tuple(r) for r in rows[:10]]}")
    return metrics
