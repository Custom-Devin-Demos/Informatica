"""Runtime for the generated record specs: project target fields and format fixed-width records.

A spec field is ``(name, datatype, precision, scale, spark_sql | None)`` as produced by ``_codegen``.
PowerExchange copybook datatypes are rendered as:

* ``CHAR(p)`` - left-aligned, space padded, truncated to ``p``.
* ``UZONED(p, s)`` - unsigned zoned decimal: ``|value| * 10**s`` as ``p`` zero-padded digits.
* ``ZONED(p, s)`` - signed zoned decimal with a trailing overpunched sign
  (``{ABCDEFGHI`` positive, ``}JKLMNOPQR`` negative, IBM convention).

Unconnected target ports are written as spaces (CHAR) or zeros (numeric), like PowerExchange.
Records are padded with spaces to the session ``LRECL`` (``RECFM = FB``).
"""
from __future__ import annotations

from typing import Iterable, Sequence

from pyspark.sql import Column, DataFrame, functions as F

from biis_lakehouse.pipelines.cpm_extracts._pcexpr import bind_params

SpecField = tuple  # (name, datatype, precision, scale, sql | None)

_POS, _NEG = "{ABCDEFGHI", "}JKLMNOPQR"


def spark_type(dt: str, precision: int, scale: int) -> str:
    dt = dt.lower()
    if dt.startswith("number") or dt in ("decimal", "numeric"):
        return f"decimal({max(precision, 1)},{scale})"
    if dt in ("integer", "int", "smallint"):
        return "int"
    if dt == "bigint":
        return "bigint"
    if dt.startswith("date") or dt.startswith("timestamp"):
        return "timestamp"
    return "string"


def project(df: DataFrame, fields: Sequence[SpecField], params: dict[str, str] | None = None, typed: bool = True) -> DataFrame:
    """``SELECT <field expr> AS <field>`` for every target field (Oracle targets: cast to the column type)."""
    cols = []
    for name, dt, p, s, sql in fields:
        c = F.expr(bind_params(sql, params or {})) if sql else F.lit(None)
        cols.append((c.cast(spark_type(dt, p, s)) if typed else c).alias(name))
    return df.select(*cols)


def _digits(c: Column, p: int, s: int) -> Column:
    scaled = F.abs(c.cast(f"decimal(38,{s})")) * F.lit(10 ** s)
    return F.lpad(F.coalesce(F.floor(scaled).cast("decimal(38,0)").cast("string"), F.lit("0")), p, "0")


def format_field(c: Column, dt: str, p: int, s: int) -> Column:
    dt = dt.upper()
    if dt == "UZONED":
        return _digits(c, p, s)
    if dt == "ZONED":
        d = _digits(c, p, s)
        last = F.substring(d, p, 1)
        neg = F.coalesce(c.cast(f"decimal(38,{s})") < 0, F.lit(False))
        punched = F.when(neg, F.translate(last, "0123456789", _NEG)).otherwise(F.translate(last, "0123456789", _POS))
        return F.concat(F.substring(d, 1, p - 1), punched) if p > 1 else punched
    if dt in ("NUMBER", "BIGINT", "INTEGER", "DECIMAL"):
        return F.lpad(F.coalesce(c.cast("string"), F.lit("")), p, " ")
    return F.rpad(F.coalesce(c.cast("string"), F.lit("")), p, " ")


def to_records(df: DataFrame, fields: Sequence[SpecField], params: dict[str, str] | None = None, lrecl: int | None = None) -> DataFrame:
    """Project and format ``fields`` into one fixed-width ``value`` column."""
    projected = project(df, fields, params, typed=False)
    formatted = projected.select(*[format_field(F.col(f"`{n}`"), dt, p, s).alias(f"f{i}") for i, (n, dt, p, s, _) in enumerate(fields)])
    rec = F.concat(*[F.col(f"f{i}") for i in range(len(fields))])
    if lrecl:
        rec = F.rpad(rec, lrecl, " ")
    return formatted.select(rec.alias("value"))


def write_records(df: DataFrame, path: str, fields: Sequence[SpecField], params: dict[str, str] | None = None, lrecl: int | None = None) -> None:
    to_records(df, fields, params, lrecl).coalesce(1).write.mode("overwrite").text(path)


def record_length(fields: Iterable[SpecField]) -> int:
    return sum(f[2] for f in fields)
