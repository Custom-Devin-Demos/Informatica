"""DataFrame-level reconciliation between a legacy extract and the Delta output."""
from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from decimal import Decimal
from functools import reduce
from typing import Iterable, Optional, Sequence

from pyspark.sql import DataFrame, functions as F, types as T

from biis_lakehouse.common import delta
from biis_lakehouse.context import RunContext

_NUMERIC = (T.ByteType, T.ShortType, T.IntegerType, T.LongType, T.FloatType, T.DoubleType, T.DecimalType)
_NULL_TOKEN = "\u0000<NULL>"


def normalize(
    df: DataFrame,
    *,
    lower_names: bool = True,
    trim_strings: bool = True,
    empty_as_null: bool = True,
    decimal_scale: Optional[int] = None,
) -> DataFrame:
    """Make a legacy extract and a Delta table comparable.

    Oracle extracts arrive with upper-case column names, CHAR padding, and Oracle stores ``''`` as
    NULL, so by default names are lower-cased, strings trimmed and empty strings nulled.
    ``decimal_scale`` rounds float/double/decimal columns (e.g. ``2`` for money).
    """
    cols = []
    for f in df.schema.fields:
        c = F.col(f"`{f.name}`")
        if isinstance(f.dataType, T.StringType):
            if trim_strings:
                c = F.trim(c)
            if empty_as_null:
                c = F.when(c == "", F.lit(None).cast("string")).otherwise(c)
        elif decimal_scale is not None and isinstance(f.dataType, (T.FloatType, T.DoubleType, T.DecimalType)):
            c = F.round(c, decimal_scale)
        cols.append(c.alias(f.name.lower() if lower_names else f.name))
    return df.select(*cols)


def align_types(legacy: DataFrame, target: DataFrame) -> DataFrame:
    """Cast ``legacy`` columns to the type of the same-named ``target`` column (case-insensitive).

    CSV extracts are all strings; JDBC returns Oracle ``NUMBER`` as ``decimal(38,10)``. Casting to the
    Delta type makes keys joinable and checksums stable. Values that do not cast become NULL and so
    surface as mismatches.
    """
    target_types = {f.name.lower(): f.dataType for f in target.schema.fields}
    cols = []
    for f in legacy.schema.fields:
        dtype = target_types.get(f.name.lower())
        c = F.col(f"`{f.name}`")
        cols.append((c.cast(dtype) if dtype is not None and dtype != f.dataType else c).alias(f.name))
    return legacy.select(*cols)


@dataclass
class CountCompare:
    legacy: int
    target: int
    tolerance: int = 0

    @property
    def diff(self) -> int:
        return self.target - self.legacy

    @property
    def ok(self) -> bool:
        return abs(self.diff) <= self.tolerance


def compare_row_counts(legacy: DataFrame, target: DataFrame, tolerance: int = 0) -> CountCompare:
    return CountCompare(legacy.count(), target.count(), tolerance)


def _key_cond(left: DataFrame, right: DataFrame, keys: Sequence[str]):
    return reduce(lambda a, b: a & b, [left[k].eqNullSafe(right[k]) for k in keys])


def _rows(df: DataFrame, limit: int) -> list[dict]:
    return [r.asDict() for r in df.limit(limit).collect()] if limit else []


@dataclass
class KeyDiff:
    keys: list[str]
    missing_in_target: int
    extra_in_target: int
    duplicate_legacy: int
    duplicate_target: int
    missing_sample: list[dict] = field(default_factory=list)
    extra_sample: list[dict] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return not (self.missing_in_target or self.extra_in_target or self.duplicate_legacy or self.duplicate_target)


def _dup_count(df: DataFrame, keys: Sequence[str]) -> int:
    return df.groupBy(*keys).count().filter(F.col("count") > 1).count()


def key_diff(legacy: DataFrame, target: DataFrame, keys: Sequence[str], sample: int = 20) -> KeyDiff:
    """Set difference of key columns (null-safe) plus duplicate-key counts on each side."""
    keys = list(keys)
    lk = legacy.select(*keys).distinct()
    tk = target.select(*keys).distinct()
    missing = lk.join(tk, _key_cond(lk, tk, keys), "left_anti")
    extra = tk.join(lk, _key_cond(tk, lk, keys), "left_anti")
    return KeyDiff(
        keys=keys,
        missing_in_target=missing.count(),
        extra_in_target=extra.count(),
        duplicate_legacy=_dup_count(legacy, keys),
        duplicate_target=_dup_count(target, keys),
        missing_sample=_rows(missing.orderBy(*keys), sample),
        extra_sample=_rows(extra.orderBy(*keys), sample),
    )


def _checksum(col: str):
    # Order-independent: sum of per-value 64-bit hashes, summed as decimal so it cannot overflow.
    h = F.xxhash64(F.coalesce(F.col(f"`{col}`").cast("string"), F.lit(_NULL_TOKEN)))
    return F.sum(h.cast("decimal(38,0)"))


def column_profile(df: DataFrame, columns: Sequence[str]) -> dict[str, dict]:
    """One pass over ``df``: per column non-null count, distinct count, min, max, sum (numeric) and checksum."""
    types = {f.name: f.dataType for f in df.schema.fields}
    exprs = []
    for i, c in enumerate(columns):
        col = F.col(f"`{c}`")
        exprs += [
            F.count(col).alias(f"{i}__non_null"),
            F.countDistinct(col).alias(f"{i}__distinct"),
            F.min(col).cast("string").alias(f"{i}__min"),
            F.max(col).cast("string").alias(f"{i}__max"),
            _checksum(c).alias(f"{i}__checksum"),
        ]
        if isinstance(types[c], _NUMERIC):
            exprs.append(F.sum(col.cast("decimal(38,10)")).alias(f"{i}__sum"))
    row = df.agg(*exprs).collect()[0].asDict()
    out: dict[str, dict] = {c: {} for c in columns}
    for k, v in row.items():
        idx, metric = k.split("__", 1)
        out[columns[int(idx)]][metric] = v
    return out


@dataclass
class ColumnCompare:
    column: str
    metric: str
    legacy: object
    target: object
    ok: bool


def _close(a, b, tol: float) -> bool:
    if a is None or b is None:
        return a is None and b is None
    return abs(Decimal(str(a)) - Decimal(str(b))) <= Decimal(str(tol))


def compare_columns(
    legacy: DataFrame, target: DataFrame, columns: Sequence[str], sum_tolerance: float = 0.0
) -> list[ColumnCompare]:
    """Compare :func:`column_profile` of both sides. Numeric sums allow ``sum_tolerance`` absolute drift."""
    lp, tp = column_profile(legacy, columns), column_profile(target, columns)
    out = []
    for c in columns:
        for metric in ("non_null", "distinct", "min", "max", "sum", "checksum"):
            if metric not in lp[c] and metric not in tp[c]:
                continue
            a, b = lp[c].get(metric), tp[c].get(metric)
            ok = _close(a, b, sum_tolerance) if metric == "sum" else a == b
            out.append(ColumnCompare(c, metric, a, b, ok))
    return out


def row_diff(legacy: DataFrame, target: DataFrame, keys: Sequence[str], columns: Sequence[str]) -> DataFrame:
    """Rows present on both sides whose non-key values differ: ``keys..., column, legacy_value, target_value``."""
    l, t = legacy.alias("l"), target.alias("t")
    cond = reduce(lambda a, b: a & b, [F.col(f"l.`{k}`").eqNullSafe(F.col(f"t.`{k}`")) for k in keys])
    diffs = F.array(
        *[
            F.when(
                ~F.col(f"l.`{c}`").eqNullSafe(F.col(f"t.`{c}`")),
                F.struct(
                    F.lit(c).alias("column"),
                    F.col(f"l.`{c}`").cast("string").alias("legacy_value"),
                    F.col(f"t.`{c}`").cast("string").alias("target_value"),
                ),
            )
            for c in columns
        ]
    )
    joined = l.join(t, cond, "inner").select(*[F.col(f"l.`{k}`").alias(k) for k in keys], diffs.alias("_d"))
    return (
        joined.select(*keys, F.explode(F.filter("_d", lambda x: x.isNotNull())).alias("_d"))
        .select(*keys, "_d.column", "_d.legacy_value", "_d.target_value")
    )


@dataclass
class ReconReport:
    name: str
    counts: CountCompare
    keys: Optional[KeyDiff] = None
    columns: list[ColumnCompare] = field(default_factory=list)
    mismatched_rows: int = 0
    mismatch_sample: list[dict] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return (
            self.counts.ok
            and (self.keys is None or self.keys.ok)
            and all(c.ok for c in self.columns)
            and self.mismatched_rows == 0
        )

    def failures(self) -> list[str]:
        out = []
        if not self.counts.ok:
            out.append(f"row count legacy={self.counts.legacy} target={self.counts.target}")
        if self.keys is not None and not self.keys.ok:
            k = self.keys
            out.append(
                f"keys missing_in_target={k.missing_in_target} extra_in_target={k.extra_in_target} "
                f"dup_legacy={k.duplicate_legacy} dup_target={k.duplicate_target}"
            )
        out += [f"{c.column}.{c.metric} legacy={c.legacy} target={c.target}" for c in self.columns if not c.ok]
        if self.mismatched_rows:
            out.append(f"{self.mismatched_rows} matched rows with differing values")
        return out

    def summary(self) -> str:
        status = "OK" if self.ok else "MISMATCH"
        lines = [f"[{status}] {self.name}: legacy={self.counts.legacy} target={self.counts.target}"]
        lines += [f"  - {f}" for f in self.failures()]
        return "\n".join(lines)

    def result_rows(self) -> list[dict]:
        """Flat rows for the ``recon_results`` audit table."""
        rows = [dict(check="row_count", column=None, legacy=self.counts.legacy, target=self.counts.target, ok=self.counts.ok)]
        if self.keys is not None:
            k = self.keys
            rows += [
                dict(check="keys_missing_in_target", column=",".join(k.keys), legacy=None, target=k.missing_in_target, ok=k.missing_in_target == 0),
                dict(check="keys_extra_in_target", column=",".join(k.keys), legacy=None, target=k.extra_in_target, ok=k.extra_in_target == 0),
                dict(check="duplicate_keys", column=",".join(k.keys), legacy=k.duplicate_legacy, target=k.duplicate_target, ok=not (k.duplicate_legacy or k.duplicate_target)),
            ]
        rows += [dict(check=c.metric, column=c.column, legacy=c.legacy, target=c.target, ok=c.ok) for c in self.columns]
        if self.keys is not None:
            rows.append(dict(check="mismatched_rows", column=None, legacy=None, target=self.mismatched_rows, ok=self.mismatched_rows == 0))
        return rows

    def to_dict(self) -> dict:
        return {"name": self.name, "ok": self.ok, **asdict(self)}

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), default=str, indent=2)


def reconcile(
    legacy: DataFrame,
    target: DataFrame,
    *,
    name: str,
    keys: Sequence[str] = (),
    columns: Optional[Sequence[str]] = None,
    exclude: Iterable[str] = (),
    count_tolerance: int = 0,
    sum_tolerance: float = 0.0,
    decimal_scale: Optional[int] = None,
    cast_legacy: bool = True,
    sample: int = 20,
) -> ReconReport:
    """Full reconciliation of one table/output.

    Both sides are :func:`normalize`-d (lower-case names, trimmed strings, ``''`` → NULL), the legacy
    side is cast to the target's types, then row counts, key sets, column profiles and (when ``keys``
    are given) row-level values are compared over the common columns minus ``exclude``
    (e.g. load timestamps).
    """
    legacy = normalize(legacy, decimal_scale=decimal_scale)
    target = normalize(target, decimal_scale=decimal_scale)
    if cast_legacy:
        legacy = align_types(legacy, target)
    keys = [k.lower() for k in keys]
    excluded = {c.lower() for c in exclude}
    if columns is None:
        tcols = set(target.columns)
        columns = [c for c in legacy.columns if c in tcols]
    columns = [c.lower() for c in columns if c.lower() not in excluded]
    missing = [c for c in [*keys, *columns] if c not in legacy.columns or c not in target.columns]
    if missing:
        raise ValueError(f"{name}: columns not on both sides: {missing}")

    legacy, target = legacy.cache(), target.cache()
    try:
        report = ReconReport(name=name, counts=compare_row_counts(legacy, target, count_tolerance))
        if columns:
            report.columns = compare_columns(legacy, target, columns, sum_tolerance)
        if keys:
            report.keys = key_diff(legacy, target, keys, sample)
            value_cols = [c for c in columns if c not in keys]
            if value_cols:
                diffs = row_diff(legacy, target, keys, value_cols).cache()
                report.mismatched_rows = diffs.select(*keys).distinct().count()
                report.mismatch_sample = _rows(diffs.orderBy(*keys, "column"), sample)
                diffs.unpersist()
        return report
    finally:
        legacy.unpersist()
        target.unpersist()


RESULTS_SCHEMA = (
    "run_ts timestamp, run_id string, pay_period string, name string, check string, "
    "column string, legacy string, target string, ok boolean"
)


def write_results(ctx: RunContext, reports: Sequence[ReconReport], table: Optional[str] = None, run_id: Optional[str] = None) -> str:
    """Append report rows to a Delta audit table (default ``<catalog>.biis.recon_results``) and return its name.

    ``pay_period`` comes from ``WF_PP_YEAR_NUM`` (falls back to ``WF_PP_END_YEAR``/``WF_PP_NUM``) so
    parallel-run results can be tracked per pay period.
    """
    import datetime as dt
    import uuid

    table = table or ctx.table_name("RECON_RESULTS")
    pp = ctx.params.get("WF_PP_YEAR_NUM") or "".join(
        ctx.params.get(k, "") for k in ("WF_PP_END_YEAR", "WF_PP_NUM")
    ) or None
    now, run_id = dt.datetime.now(), run_id or uuid.uuid4().hex
    rows = [
        (now, run_id, pp, r.name, x["check"], x["column"],
         None if x["legacy"] is None else str(x["legacy"]),
         None if x["target"] is None else str(x["target"]), bool(x["ok"]))
        for r in reports
        for x in r.result_rows()
    ]
    delta.append(ctx, ctx.spark.createDataFrame(rows, RESULTS_SCHEMA), table)
    return table
