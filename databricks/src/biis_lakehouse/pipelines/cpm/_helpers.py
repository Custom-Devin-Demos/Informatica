"""VSAM record parsing and target writers shared by the wf_CPM sessions."""
from __future__ import annotations

from typing import Iterable

from pyspark.sql import Column, DataFrame, functions as F

from biis_lakehouse.common import delta
from biis_lakehouse.common.fixed_width import Field, read_fixed_width, write_fixed_width
from biis_lakehouse.context import RunContext

from ._layouts import FLAT_TARGETS, TABLES, VSAM_LAYOUTS

AREA = "CPM"


# --------------------------------------------------------------------------- VSAM / copybook records
def vsam_fields(source: str, group: str | None = None) -> list[tuple]:
    """Elementary fields of ``source`` (optionally only those under the 01/03-level ``group`` redefine)."""
    layout = VSAM_LAYOUTS[source]
    if group is None:
        return [f for f in layout if f[7] == "ELEMITEM"]
    out, inside, level = [], False, None
    for f in layout:
        name, _, _, _, _, _, lvl, ftype, _, _ = f
        if name == group:
            inside, level = True, int(lvl)
            continue
        if inside:
            if int(lvl) <= level:
                break
            if ftype == "ELEMITEM":
                out.append(f)
    return out


def _decode(raw: Column, scale: int, numeric: bool, pic: str) -> Column:
    if not numeric:
        return raw
    txt = F.trim(raw)
    # Display numerics with a separate leading sign (+9(7)V9(2)) and an implied decimal point.
    signed = F.regexp_replace(txt, r"^\+", "")
    val = F.expr("try_cast(NULL AS decimal(38,0))")
    val = F.when(signed.rlike(r"^-?\d+$"), signed.cast("decimal(38,0)"))
    if scale:
        return (val / F.lit(10**scale)).cast(f"decimal(38,{scale})")
    return val


def vsam_record_length(source: str) -> int:
    return max(int(f[1]) + int(f[2]) for f in VSAM_LAYOUTS[source])


def read_vsam(ctx: RunContext, source: str, filename: str, group: str | None = None) -> DataFrame:
    """Read a VSAM extract (one fixed-length record per line) into the copybook's elementary fields.

    Every REDEFINES view of the record is decoded (as the VSAM Normalizer does); the router downstream
    picks the view by record type. Alphanumeric fields keep their trailing blanks (``IS_SPACES`` tests rely
    on them); signed display numerics (``+9(7)V9(2)``) are decoded with their implied decimals.
    ``_SEQ`` keeps file order.
    """
    reclen = vsam_record_length(source)
    raw = ctx.spark.read.text(ctx.path("in", AREA, filename))
    raw = raw.select(F.rpad("value", reclen, " ").alias("value")).withColumn("_SEQ", F.monotonically_increasing_id())
    cols = [F.col("_SEQ")]
    seen = set()
    for name, off, length, scale, numeric, _occ, _lvl, _t, _r, pic in vsam_fields(source, group):
        if name.startswith("FILLER") or name in seen:
            continue
        seen.add(name)
        c = F.substring("value", int(off) + 1, int(length))
        cols.append(_decode(c, int(scale), numeric, pic).alias(name))
    return raw.select(*cols)


# --------------------------------------------------------------------------- targets
def conform(df: DataFrame, table: str) -> DataFrame:
    """Project ``df`` onto the Oracle target definition (missing ports -> NULL), casting to target types."""
    cols = []
    for name, typ, _ in TABLES[table]:
        cols.append((F.col(name) if name in df.columns else F.lit(None)).cast(typ).alias(name))
    return df.select(*cols)


def load_truncate(ctx: RunContext, df: DataFrame, table: str) -> DataFrame:
    """Target with *Truncate target table option* = YES, rows inserted."""
    out = conform(df, table)
    delta.overwrite(ctx, out, ctx.table_name(table))
    return out


def load_insert(ctx: RunContext, df: DataFrame, table: str) -> DataFrame:
    out = conform(df, table)
    delta.append(ctx, out, ctx.table_name(table))
    return out


def write_flat(ctx: RunContext, df: DataFrame, target: str, filename: str, numeric: Iterable[str] = ()) -> str:
    fields = [Field(n, int(l), n in set(numeric)) for n, l in FLAT_TARGETS[target]]
    path = ctx.path("out", AREA, filename)
    write_fixed_width(df, path, fields)
    return path


def read_flat(ctx: RunContext, target: str, filename: str) -> DataFrame:
    fields = [Field(n, int(l)) for n, l in FLAT_TARGETS[target]]
    return read_fixed_width(ctx.spark, ctx.path("out", AREA, filename), fields)
