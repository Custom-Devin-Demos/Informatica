"""Shared building blocks for the wf_CPM sessions (VSAM record parsing, PowerCenter function semantics, mapplets)."""
from __future__ import annotations

from typing import Iterable, Sequence

from pyspark.sql import Column, DataFrame, Window, functions as F

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


# --------------------------------------------------------------------------- PowerCenter semantics
def pc_concat(*cols: Column | str) -> Column:
    """PowerCenter ``||`` treats NULL operands as empty strings."""
    return F.concat_ws("", *[F.col(c) if isinstance(c, str) else c for c in cols])


def last_where(value: Column | str, cond: Column, order: str = "_SEQ") -> Column:
    """Aggregator ``LAST(value, cond)``: last non-null value (in input order) among rows where ``cond``."""
    v = F.col(value) if isinstance(value, str) else value
    return F.max_by(v, F.when(cond & v.isNotNull(), F.col(order)))


def first_where(value: Column | str, cond: Column, order: str = "_SEQ") -> Column:
    v = F.col(value) if isinstance(value, str) else value
    return F.min_by(v, F.when(cond & v.isNotNull(), F.col(order)))


def sum_where(value: Column | str, cond: Column) -> Column:
    """``SUM(value, cond)``; PowerCenter returns NULL when no row qualifies."""
    v = F.col(value) if isinstance(value, str) else value
    return F.sum(F.when(cond, v))


def truncate_digits(c: Column | str, int_digits: int, scale: int, sign_from: Column | str | None = None) -> Column:
    """The ``*_EXCEEDS_LIMIT`` pattern: NULL -> 0; when |x| exceeds the target precision keep only the
    low-order ``int_digits`` integer digits (string SUBSTR in PowerCenter == modulo), keeping the sign."""
    x = F.col(c) if isinstance(c, str) else c
    s = x if sign_from is None else (F.col(sign_from) if isinstance(sign_from, str) else sign_from)
    limit = 10**int_digits
    a = F.abs(x)
    trunc = F.signum(s) * (F.floor(a) % F.lit(limit) + (a - F.floor(a)))
    if scale == 0:
        trunc = F.signum(s) * (F.floor(a) % F.lit(limit))
    max_val = F.lit(limit) - F.lit(10 ** (-scale)) if scale else F.lit(limit - 1)
    out = F.when(x.isNull(), F.lit(0)).when(a > max_val, trunc).otherwise(x)
    return out.cast(f"decimal({int_digits + scale},{scale})")


def mplt_convert_num_to_prec7(df: DataFrame, mapping: dict[str, str]) -> DataFrame:
    """Mapplet ``mplt_Convert_Num_To_Prec7``: decimal(12,2) -> decimal(7,2) (NULL -> 0, overflow keeps
    the low-order 5 integer digits with the original sign). ``mapping`` is input column -> output column."""
    for src, dst in mapping.items():
        df = df.withColumn(dst, truncate_digits(F.col(src).cast("decimal(12,2)"), 5, 2))
    return df


def is_number(c: Column | str) -> Column:
    x = F.col(c) if isinstance(c, str) else c
    return x.rlike(r"^\s*[+-]?(\d+\.?\d*|\.\d+)([eE][+-]?\d+)?\s*$")


def to_decimal(c: Column | str, scale: int = 0, precision: int = 28) -> Column:
    """``TO_DECIMAL``: non-numeric strings become 0 in PowerCenter (Spark would give NULL)."""
    x = F.col(c) if isinstance(c, str) else c
    xs = F.trim(x.cast("string"))
    return F.when(x.isNull(), F.lit(None)).when(is_number(xs), xs.cast(f"decimal({precision},{scale})")).otherwise(
        F.lit(0).cast(f"decimal({precision},{scale})")
    )


def first_per_key(df: DataFrame, keys: Sequence[str], order: Sequence[Column] | None = None, last: bool = False) -> DataFrame:
    """Lookup multiple-match policy ``Use First Value`` / ``Use Last Value`` / ``Use Any Value``."""
    order = list(order) if order else [F.col(k) for k in keys]
    if last:
        order = [o.desc() for o in order]
    w = Window.partitionBy(*keys).orderBy(*order)
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")


def lookup(df: DataFrame, lkp: DataFrame, on: dict[str, str], returns: dict[str, str], policy: str = "first") -> DataFrame:
    """Connected lookup as a left join. ``on`` = input column -> lookup column; ``returns`` = lookup column ->
    output column. ``policy`` follows *Lookup policy on multiple match* (first/last/any)."""
    lk_keys = list(on.values())
    lk = first_per_key(lkp.select(*lk_keys, *[c for c in returns if c not in lk_keys]), lk_keys, last=policy == "last")
    alias = {k: f"__lk_{i}" for i, k in enumerate(lk_keys)}
    lk = lk.select(*[F.col(k).alias(alias[k]) for k in lk_keys], *[F.col(c).alias(o) for c, o in returns.items()])
    cond = None
    for src, dst in on.items():
        c = df[src] == lk[alias[dst]]
        cond = c if cond is None else cond & c
    return df.join(F.broadcast(lk), cond, "left").drop(*alias.values())


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


def pp(ctx: RunContext) -> tuple[int, int]:
    """``$$MAP_PP_END_YEAR`` / ``$$MAP_PP_NUM`` (assigned from ``$$WF_PP_END_YEAR`` / ``$$WF_PP_NUM`` pre-session)."""
    return int(ctx.param("WF_PP_END_YEAR")), int(ctx.param("WF_PP_NUM"))
