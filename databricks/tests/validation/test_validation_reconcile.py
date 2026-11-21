"""Reconciliation helpers on small PAY_PERIOD / CPM_NEWPAY_TBL-style fixtures."""
import json

import pytest

from biis_lakehouse.common import delta
from biis_lakehouse.validation import (
    compare_columns,
    compare_row_counts,
    key_diff,
    normalize,
    reconcile,
    row_diff,
    write_results,
)
from biis_lakehouse.validation.sources import read_csv_extract

PP_SCHEMA = "pp_num int, pp_end_year int, pp_begin_dt date, current_flag string, gross_amt decimal(11,2)"
# Oracle unload: upper-case headers, CHAR padding, '' for NULL, everything a string.
LEGACY_CSV = """PP_NUM,PP_END_YEAR,PP_BEGIN_DT,CURRENT_FLAG,GROSS_AMT
1,2019,2019-01-06,N ,100.00
2,2019,2019-01-20,N ,200.50
3,2019,2019-02-03,Y ,300.25
4,2019,2019-02-17,,0.00
"""


@pytest.fixture
def legacy(ctx, tmp_path):
    p = tmp_path / "pay_period.csv"
    p.write_text(LEGACY_CSV)
    return read_csv_extract(ctx.spark, str(p))


def _target(ctx, rows):
    import datetime as dt
    from decimal import Decimal

    data = [(n, y, dt.date.fromisoformat(d), f, Decimal(a)) for n, y, d, f, a in rows]
    tbl = ctx.table_name("PAY_PERIOD", "HISTDBA")
    delta.overwrite(ctx, ctx.spark.createDataFrame(data, PP_SCHEMA), tbl)
    return ctx.spark.table(tbl)


GOOD = [
    (1, 2019, "2019-01-06", "N", "100.00"),
    (2, 2019, "2019-01-20", "N", "200.50"),
    (3, 2019, "2019-02-03", "Y", "300.25"),
    (4, 2019, "2019-02-17", None, "0.00"),
]


def test_identical_outputs_reconcile(ctx, legacy):
    target = _target(ctx, GOOD)
    r = reconcile(legacy, target, name="pay_period", keys=["PP_NUM", "PP_END_YEAR"])
    assert r.ok, r.summary()
    assert (r.counts.legacy, r.counts.target) == (4, 4)
    assert r.keys.missing_in_target == r.keys.extra_in_target == 0
    checks = {(c.column, c.metric): c for c in r.columns}
    assert checks[("gross_amt", "sum")].target == checks[("gross_amt", "sum")].legacy
    assert checks[("current_flag", "non_null")].legacy == 3  # '' -> NULL like Oracle
    assert json.loads(r.to_json())["ok"] is True


def test_detects_missing_extra_changed_rows(ctx, legacy):
    rows = [list(x) for x in GOOD[:3]] + [[5, 2019, "2019-03-03", "N", "0.00"]]
    rows[1][4] = "200.51"
    rows[2][3] = "N"
    target = _target(ctx, [tuple(x) for x in rows])
    r = reconcile(legacy, target, name="pay_period", keys=["pp_num", "pp_end_year"])
    assert not r.ok
    assert r.counts.ok  # 4 vs 4: counts alone would miss this
    assert r.keys.missing_in_target == 1 and r.keys.missing_sample == [{"pp_num": 4, "pp_end_year": 2019}]
    assert r.keys.extra_in_target == 1 and r.keys.extra_sample == [{"pp_num": 5, "pp_end_year": 2019}]
    assert r.mismatched_rows == 2
    assert {(m["pp_num"], m["column"], m["legacy_value"], m["target_value"]) for m in r.mismatch_sample} == {
        (2, "gross_amt", "200.50", "200.51"),
        (3, "current_flag", "Y", "N"),
    }
    failed = {(c.column, c.metric) for c in r.columns if not c.ok}
    assert ("gross_amt", "checksum") in failed and ("pp_num", "max") in failed
    assert "MISMATCH" in r.summary()


def test_sum_tolerance_and_exclude(ctx, legacy):
    rows = [list(x) for x in GOOD]
    rows[0][4] = "100.01"
    target = _target(ctx, [tuple(x) for x in rows])
    r = reconcile(legacy, target, name="pp", columns=["gross_amt"], sum_tolerance=0.05)
    assert {c.metric: c.ok for c in r.columns}["sum"] is True
    assert {c.metric: c.ok for c in r.columns}["checksum"] is False
    r = reconcile(legacy, target, name="pp", keys=["pp_num"], exclude=["gross_amt"])
    assert r.ok and "gross_amt" not in {c.column for c in r.columns}


def test_duplicate_keys_and_null_keys(spark):
    l = spark.createDataFrame([("A", 1), ("A", 1), (None, 2)], "k string, v int")
    t = spark.createDataFrame([("A", 1), (None, 2)], "k string, v int")
    kd = key_diff(l, t, ["k"])
    assert kd.missing_in_target == 0 and kd.extra_in_target == 0  # NULL keys match null-safely
    assert (kd.duplicate_legacy, kd.duplicate_target) == (1, 0) and not kd.ok
    assert compare_row_counts(l, t).diff == -1
    assert compare_row_counts(l, t, tolerance=1).ok


def test_row_diff_and_column_compare(spark):
    l = spark.createDataFrame([(1, "x", None), (2, "y", 5)], "id int, a string, b int")
    t = spark.createDataFrame([(1, "x", 0), (2, "z", 5)], "id int, a string, b int")
    d = {(r.id, r.column): (r.legacy_value, r.target_value) for r in row_diff(l, t, ["id"], ["a", "b"]).collect()}
    assert d == {(1, "b"): (None, "0"), (2, "a"): ("y", "z")}
    cmp = {(c.column, c.metric): c.ok for c in compare_columns(l, t, ["a", "b"])}
    assert cmp[("b", "non_null")] is False and cmp[("a", "non_null")] is True


def test_normalize(spark):
    df = spark.createDataFrame([("  AB ", 1.234), ("", 2.0)], "NAME string, AMT double")
    rows = normalize(df, decimal_scale=2).collect()
    assert [tuple(r) for r in rows] == [("AB", 1.23), (None, 2.0)]
    assert normalize(df).columns == ["name", "amt"]


def test_unknown_column_raises(ctx, legacy):
    target = _target(ctx, GOOD)
    with pytest.raises(ValueError, match="not on both sides"):
        reconcile(legacy, target, name="pp", keys=["no_such_key"])


def test_write_results(ctx, legacy):
    target = _target(ctx, GOOD[:3])
    ctx.params.update({"WF_PP_YEAR_NUM": "201903"})
    r = reconcile(legacy, target, name="pay_period", keys=["pp_num", "pp_end_year"])
    tbl = write_results(ctx, [r], run_id="run1")
    assert tbl == "biis.recon_results"
    out = ctx.spark.table(tbl)
    assert out.count() == len(r.result_rows())
    row = out.filter("check = 'row_count'").collect()[0]
    assert (row.pay_period, row.run_id, row.legacy, row.target, row.ok) == ("201903", "run1", "4", "3", False)
    assert out.filter("check = 'keys_missing_in_target'").collect()[0].target == "1"
