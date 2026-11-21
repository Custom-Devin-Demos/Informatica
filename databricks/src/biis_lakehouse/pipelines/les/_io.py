"""LES-specific IO helpers: VSAM record parsing, flat-file targets and PowerCenter idioms."""
from __future__ import annotations

import os
from functools import reduce

from pyspark.sql import Column, DataFrame, Window, functions as F

from biis_lakehouse.common.fixed_width import write_fixed_width
from biis_lakehouse.context import RunContext
from biis_lakehouse.pipelines.les.layouts import FLAT_FILE_SOURCES, FLAT_FILE_TARGETS, VSAM_LAYOUTS

ORDER_COLS = ["_file_seq", "_rec_seq"]
_POS = "{ABCDEFGHI"
_NEG = "}JKLMNOPQR"


def input_dir(ctx: RunContext) -> str:
    """``$$WF_LES_INPUT_DIR`` / ``$Param_Root_Directory/data/int/in/LES``."""
    return ctx.param("WF_LES_INPUT_DIR", "") or ctx.path("in", "LES")


def output_path(ctx: RunContext, filename: str) -> str:
    """``$PMTargetFileDir`` for wf_LES is ``data/int/out/LES``."""
    return ctx.path("out", "LES", filename)


def input_files(ctx: RunContext) -> list[str]:
    """PowerCenter file-list command ``cd <in/LES>; ls -1|sort``."""
    d = input_dir(ctx)
    if not os.path.isdir(d):
        return []
    return [os.path.join(d, f) for f in sorted(os.listdir(d)) if os.path.isfile(os.path.join(d, f))]


def _read_lines(ctx: RunContext, files: list[str]) -> DataFrame:
    """All records of the listed files in file/record order (``_file_seq``, ``_rec_seq`` are 1-based)."""
    spark = ctx.spark
    parts = []
    for i, f in enumerate(files, start=1):
        rdd = spark.sparkContext.textFile(f, minPartitions=1).zipWithIndex().map(lambda t, i=i: (i, t[1] + 1, t[0]))
        parts.append(spark.createDataFrame(rdd, "_file_seq int, _rec_seq long, value string"))
    if not parts:
        return spark.createDataFrame([], "_file_seq int, _rec_seq long, value string")
    return reduce(DataFrame.unionByName, parts)


def _numeric(c: Column, scale: int, signed: bool) -> Column:
    s = F.trim(c)
    last = F.substring(s, -1, 1)
    head = s.substr(F.lit(1), F.greatest(F.length(s) - 1, F.lit(0)))  # type: ignore[arg-type]
    if signed:
        # zoned-decimal overpunch: '{'/A-I = +0..+9, '}'/J-R = -0..-9 on the last byte
        digit = F.translate(last, _POS + _NEG, "01234567890123456789")
        digits = F.concat(head, digit)
        sign = F.when(last.isin(*_NEG), F.lit(-1)).otherwise(F.lit(1))
    else:
        digits, sign = s, F.lit(1)
    num = digits.cast("decimal(38,0)") * sign
    if scale:
        num = num / F.lit(10 ** scale)
    return F.when(F.length(s) > 0, num.cast(f"decimal(18,{scale})"))


def read_vsam(ctx: RunContext, source: str, files: list[str] | None = None) -> DataFrame:
    """Read every record of the LES input files with the copybook layout of VSAM source ``source``.

    Columns follow the XML SOURCEFIELD definitions (PHYSICALOFFSET/PHYSICALLENGTH, REDEFINES share
    the same bytes); ``_file_seq``/``_rec_seq`` keep PowerCenter's file and record order.
    """
    raw = _read_lines(ctx, input_files(ctx) if files is None else files)
    cols = []
    for name, off, length, scale, signed in VSAM_LAYOUTS[source]:
        c = F.substring("value", off + 1, length)
        cols.append((F.rtrim(c) if scale is None else _numeric(c, scale, signed)).alias(name))
    return raw.select(*ORDER_COLS, F.substring("value", 12, 7).alias("_E_LES_NUM"), *cols)


def read_flat(ctx: RunContext, source: str, path: str | list[str] | None = None) -> DataFrame:
    """Fixed-width flat-file source ``source`` (default: every LES input file, in sorted order)."""
    paths = input_files(ctx) if path is None else [path] if isinstance(path, str) else path
    raw = _read_lines(ctx, [p for p in paths if os.path.exists(p)])
    cols, pos = [], 1
    for f in FLAT_FILE_SOURCES[source]:
        cols.append(F.rtrim(F.substring("value", pos, f.length)).alias(f.name))
        pos += f.length
    return raw.select(*ORDER_COLS, *cols)


def write_flat(ctx: RunContext, df: DataFrame, target: str, filename: str) -> str:
    path = output_path(ctx, filename)
    write_fixed_width(df, path, FLAT_FILE_TARGETS[target])
    return path


def first_match(df: DataFrame, keys: list[str], order: list[Column] | None = None) -> DataFrame:
    """Lookup multiple-match policy "Use Any Value"/"Use First Value": keep one row per key."""
    w = Window.partitionBy(*keys).orderBy(*(order or [F.lit(1)]))
    return df.withColumn("__rn", F.row_number().over(w)).where("__rn = 1").drop("__rn")


def raise_abort(rows: list, message) -> None:
    """PowerCenter ``ABORT(msg)``: fail the session with the first offending row's message."""
    if rows:
        raise RuntimeError(message(rows[0]) if callable(message) else message)


def pp_num_str(c: Column) -> Column:
    """``IIF(PP_NUM < 10, LPAD(TO_CHAR(PP_NUM), 2, '0'), TO_CHAR(PP_NUM))``."""
    s = c.cast("int").cast("string")
    return F.when(c < 10, F.lpad(s, 2, "0")).otherwise(s)


def is_date(c: Column, fmt: str) -> Column:
    """``IS_DATE(str, fmt)`` with Java pattern ``fmt`` (strict, so 02/30 is not a date)."""
    return F.to_date(c, fmt).isNotNull() & (F.date_format(F.to_date(c, fmt), fmt) == c)


def ordered(df: DataFrame) -> Window:
    return Window.orderBy(*ORDER_COLS)


def is_number(c: Column) -> Column:
    """``IS_NUMBER(str)``."""
    return F.trim(c).rlike(r"^[+-]?(\d+\.?\d*|\.\d+)([eE][+-]?\d+)?$")


def to_integer(c: Column) -> Column:
    """``IIF(IS_NUMBER(v), TO_INTEGER(v))`` (truncates toward zero)."""
    return F.when(is_number(c), F.trim(c).cast("double").cast("long"))


def rr_date(mmddyy: Column) -> Column:
    """``IIF(IS_DATE(v,'MM/DD/RR'), TO_DATE(v,'MM/DD/RR'))`` where ``v = MM || '/' || DD || '/' || YY``.

    RR century rule (current year's last two digits < 50): YY 00-49 -> 20YY, 50-99 -> 19YY.
    """
    import datetime as _dt

    cur = _dt.date.today().year
    cur_cent, cur_yy = cur // 100 * 100, cur % 100
    yy = F.substring(mmddyy, 5, 2)
    yyi = yy.cast("int")
    if cur_yy < 50:
        cent = F.when(yyi < 50, F.lit(cur_cent)).otherwise(F.lit(cur_cent - 100))
    else:
        cent = F.when(yyi < 50, F.lit(cur_cent + 100)).otherwise(F.lit(cur_cent))
    full = F.concat_ws("/", F.substring(mmddyy, 1, 2), F.substring(mmddyy, 3, 2), (cent + yyi).cast("string"))
    ok = yy.rlike(r"^\d\d$") & is_date(full, "MM/dd/yyyy")
    return F.when(ok, F.to_timestamp(full, "MM/dd/yyyy"))


def carry_les_number(df: DataFrame, type_col: str, les_expr: Column | None = None) -> DataFrame:
    """``v_LES_NUMBER = IIF(rec_type = '*E', <les_expr>, v_LES_NUMBER)`` then ``o_LES_NUMBER = TO_INTEGER(v)``.

    The variable port persists across rows, so every detail record inherits the LES number of the
    most recent ``*E`` employee record (in file/record order). Each mapping's ``<les_expr>`` re-reads the
    ``*E`` record through its own copybook (e.g. ``SUBSTR(WS_LES_NETPAY_FINORG,10,7)``); all of them
    resolve to bytes 12-18 (``EMP_REC_TYPE_E.WS_EMP_LES_NUM``), which is the default ``les_expr``.
    """
    les_expr = F.col("_E_LES_NUM") if les_expr is None else les_expr
    w = Window.orderBy(*ORDER_COLS).rowsBetween(Window.unboundedPreceding, Window.currentRow)
    v = F.last(F.when(F.col(type_col) == "*E", les_expr), ignorenulls=True).over(w)
    return df.withColumn("v_LES_NUMBER", v).withColumn("LES_NUMBER", to_integer(F.col("v_LES_NUMBER")))
