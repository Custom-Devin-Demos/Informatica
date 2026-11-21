"""Delta write helpers that stand in for PowerCenter target load types and Update Strategy."""
from __future__ import annotations

from typing import Sequence

from pyspark.sql import DataFrame, functions as F

from biis_lakehouse.context import RunContext

# PowerCenter Update Strategy constants.
DD_INSERT, DD_UPDATE, DD_DELETE, DD_REJECT = 0, 1, 2, 3
_OP_NAMES = {"DD_INSERT": DD_INSERT, "DD_UPDATE": DD_UPDATE, "DD_DELETE": DD_DELETE, "DD_REJECT": DD_REJECT}


def update_strategy(df: DataFrame, expr: str, op_col: str = "_row_op") -> DataFrame:
    """Add ``op_col`` from an Update Strategy expression already translated to Spark SQL.

    ``DD_*`` tokens are replaced with their numeric codes, so ``"IIF(x, DD_UPDATE, DD_INSERT)"``
    style logic can be expressed as ``"CASE WHEN x THEN DD_UPDATE ELSE DD_INSERT END"``.
    """
    for name, code in _OP_NAMES.items():
        expr = expr.replace(name, str(code))
    return df.withColumn(op_col, F.expr(expr).cast("int"))


def ensure_table(ctx: RunContext, df: DataFrame, table: str) -> None:
    if not ctx.spark.catalog.tableExists(table):
        df.limit(0).write.format("delta").saveAsTable(table)


def overwrite(ctx: RunContext, df: DataFrame, table: str) -> None:
    """Truncate-and-load target (``Truncate target table option = YES``)."""
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(table)


def append(ctx: RunContext, df: DataFrame, table: str) -> None:
    """Normal insert target."""
    df.write.format("delta").mode("append").saveAsTable(table)


def merge(ctx: RunContext, df: DataFrame, table: str, keys: Sequence[str], op_col: str | None = "_row_op") -> None:
    """MERGE ``df`` into ``table`` on ``keys``.

    With ``op_col`` present, rows flagged DD_INSERT/DD_UPDATE upsert, DD_DELETE deletes and
    DD_REJECT is dropped (PowerCenter writes rejects to the bad file). Without it, plain upsert.
    """
    from delta.tables import DeltaTable

    has_op = op_col is not None and op_col in df.columns
    if has_op:
        df = df.filter(F.col(op_col) != DD_REJECT)
    data_cols = [c for c in df.columns if c != op_col]
    ensure_table(ctx, df.select(*data_cols), table)

    cond = " AND ".join(f"t.`{k}` <=> s.`{k}`" for k in keys)
    assign = {f"`{c}`": f"s.`{c}`" for c in data_cols}
    m = DeltaTable.forName(ctx.spark, table).alias("t").merge(df.alias("s"), cond)
    if has_op:
        m = (
            m.whenMatchedDelete(condition=f"s.`{op_col}` = {DD_DELETE}")
            .whenMatchedUpdate(condition=f"s.`{op_col}` IN ({DD_INSERT}, {DD_UPDATE})", set=assign)
            .whenNotMatchedInsert(condition=f"s.`{op_col}` IN ({DD_INSERT}, {DD_UPDATE})", values=assign)
        )
    else:
        m = m.whenMatchedUpdate(set=assign).whenNotMatchedInsert(values=assign)
    m.execute()
