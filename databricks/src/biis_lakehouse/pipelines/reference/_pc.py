"""PowerCenter expression/runtime semantics shared by the reference-area pipelines."""
from __future__ import annotations

from typing import Sequence

from pyspark.sql import Column, DataFrame, Window, functions as F

from biis_lakehouse.common import delta
from biis_lakehouse.common.fixed_width import Field, to_fixed_width
from biis_lakehouse.context import RunContext

IS_NUMBER_RE = r"^\s*[+-]?(\d+\.?\d*|\.\d+)([eE][+-]?\d+)?\s*$"


def is_number(c: Column) -> Column:
    """``IS_NUMBER(s)``; NULL input is FALSE (PowerCenter returns NULL, which filters/IIF treat as FALSE)."""
    return F.coalesce(c.rlike(IS_NUMBER_RE), F.lit(False))


def is_date(c: Column, fmt: str) -> Column:
    """``IS_DATE(s, fmt)`` with a Spark pattern (e.g. ``yyyyMMdd``); strict length check like PowerCenter."""
    return F.coalesce(F.to_date(c, fmt).isNotNull() & (F.length(F.trim(c)) == len(fmt)), F.lit(False))


def to_date(c: Column, fmt: str) -> Column:
    return F.when(is_date(c, fmt), F.to_date(c, fmt))


def pc_concat(*cols: Column) -> Column:
    """PowerCenter ``||``: NULL operands are treated as empty strings; NULL only if all operands are NULL."""
    return F.when(F.coalesce(*cols).isNull(), F.lit(None)).otherwise(
        F.concat(*[F.coalesce(c.cast("string"), F.lit("")) for c in cols])
    )


def pp2(c: Column) -> Column:
    """``IIF(PP_NUM < 10, LPAD(TO_CHAR(PP_NUM), 2, '0'), TO_CHAR(PP_NUM))``."""
    s = c.cast("int").cast("string")
    return F.when(c < 10, F.lpad(s, 2, "0")).otherwise(s)


def environment(ctx: RunContext) -> Column:
    """``DECODE(SUBSTR($PMRepositoryServiceName,1,4), 'Dev_','Dev: ', 'Test','Test: ', 'Prod','Prod: ')``."""
    prefix = ctx.params.get("PMRepositoryServiceName", "")[:4]
    return F.lit({"Dev_": "Dev: ", "Test": "Test: ", "Prod": "Prod: "}.get(prefix)).cast("string")


def session_start(ctx: RunContext) -> Column:
    """``SESSSTARTTIME``; overridable with param ``SESSSTARTTIME`` (``yyyy-MM-dd HH:mm:ss``) for reruns/tests."""
    ts = ctx.params.get("SESSSTARTTIME")
    return F.to_timestamp(F.lit(ts)) if ts else F.current_timestamp()


def any_value(df: DataFrame, keys: Sequence[str]) -> DataFrame:
    """Lookup ``Use Any Value`` multiple-match policy: keep one deterministic row per condition key."""
    if not keys:
        return df.limit(1)
    w = Window.partitionBy(*keys).orderBy(*[F.col(c).asc_nulls_last() for c in df.columns])
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")


def abort_if(df: DataFrame, cond: str | Column, message: str) -> None:
    """``ABORT(message)`` when any row satisfies ``cond`` (session fails)."""
    c = F.expr(cond) if isinstance(cond, str) else cond
    if df.filter(c).limit(1).count():
        raise RuntimeError(message)


def update_rows(ctx: RunContext, df: DataFrame, table: str, keys: Sequence[str], expr: str = "DD_UPDATE") -> None:
    """Update Strategy with ``Update as Update`` only: rows with no matching target key update nothing
    (PowerCenter issues ``UPDATE ... WHERE key = ?``), so they are dropped before the MERGE."""
    target = ctx.spark.table(table).select(*keys).distinct()
    cond = [df[k].eqNullSafe(target[k]) for k in keys]
    matched = df.join(target, cond, "left_semi")
    for k in keys:
        matched = matched.filter(F.col(k).isNotNull())
    delta.merge(ctx, delta.update_strategy(matched, expr), table, keys=list(keys))


def write_flat(df: DataFrame, path: str, fields: Sequence[Field], header: bool = False) -> None:
    """Fixed-width flat-file target; ``header=True`` mirrors session ``Header Options = Output Field Names``."""
    body = to_fixed_width(df, fields).withColumn("_o", F.lit(1))
    if header:
        names = "".join(f.name.ljust(f.length)[: f.length] for f in fields)
        body = df.sparkSession.createDataFrame([(names, 0)], "value string, _o int").unionByName(body)
    body.coalesce(1).orderBy("_o").drop("_o").write.mode("overwrite").text(path)


def read_flat(ctx: RunContext, path: str, fields: Sequence[Field]) -> DataFrame:
    """Fixed-width (line sequential) flat-file source; values are not trimmed (PowerCenter keeps trailing blanks
    only for string ports, numeric conversions trim)."""
    from biis_lakehouse.common.fixed_width import read_fixed_width

    return read_fixed_width(ctx.spark, path, fields, trim=False)


def read_delimited(ctx: RunContext, path: str, names: Sequence[str], sep: str = ",") -> DataFrame:
    """Delimited flat-file source (``DELIMITED=YES``, ``QUOTE_CHARACTER=DOUBLE``); all columns read as strings."""
    schema = ", ".join(f"`{n}` string" for n in names)
    return ctx.spark.read.csv(path, schema=schema, sep=sep, quote='"', header=False)


def ensure_table(ctx: RunContext, table: str, ddl: str) -> None:
    """Create an empty Delta target with the Oracle column layout if it doesn't exist yet."""
    ctx.spark.sql(f"CREATE TABLE IF NOT EXISTS {table} ({ddl}) USING DELTA")


COUNTER_DDL = (
    "RUN_DATE timestamp, PROCESS_NAME string, COUNTER_DESCRIPTION string, COUNTER_VALUE decimal(15,0), "
    "PP_END_YEAR decimal(4,0), PP_NUM decimal(2,0), CYCLE_ID decimal(1,0)"
)


def nullable_count(df: DataFrame) -> int | None:
    """Lookup ``SELECT COUNT(*) ...`` with condition ``COUNT_CURRENT >= 1``: a zero count is a lookup miss (NULL)."""
    n = df.count()
    return n if n >= 1 else None
