"""JSON spec runner, CLI and JDBC option building (no Oracle needed)."""
import json

import pytest

from biis_lakehouse.common import delta
from biis_lakehouse.validation import __main__ as cli
from biis_lakehouse.validation.sources import oracle_jdbc_options
from biis_lakehouse.validation.spec import run_spec, substitute


def _setup(ctx, tmp_path):
    spark = ctx.spark
    tgt = spark.createDataFrame(
        [("111", 12, 2019, "100.00"), ("222", 12, 2019, "50.00"), ("333", 11, 2019, "75.00")],
        "ssn string, pp_num int, pp_end_year int, gross string",
    ).selectExpr("ssn", "pp_num", "pp_end_year", "cast(gross as decimal(11,2)) as gross_amt")
    delta.overwrite(ctx, tgt, ctx.table_name("CPM_NEWPAY_TBL"))
    (tmp_path / "legacy").mkdir()
    (tmp_path / "legacy" / "cpm_newpay.csv").write_text(
        "SSN,PP_NUM,PP_END_YEAR,GROSS_AMT\n111,12,2019,100.00\n222,12,2019,50.00\n999,11,2019,1.00\n"
    )
    out = tmp_path / "landing" / "out" / "CPM"
    out.mkdir(parents=True)
    (out / "oig.txt").write_text("AAA\nBBB\n")
    (tmp_path / "legacy" / "oig_201912.txt").write_text("AAA\nBBB\n")
    return {
        "checks": [
            {
                "name": "cpm_newpay_tbl",
                "legacy": {"format": "csv", "path": str(tmp_path / "legacy" / "cpm_newpay.csv"), "where": "PP_NUM = ${WF_PP_NUM}"},
                "target": {"table": "CPM_NEWPAY_TBL", "where": "pp_num = ${WF_PP_NUM}"},
                "keys": ["ssn", "pp_num"],
                "decimal_scale": 2,
            },
            {
                "name": "oig_file",
                "type": "file",
                "legacy": str(tmp_path / "legacy" / "oig_${WF_PP_YEAR_NUM}.txt"),
                "target": "out/CPM/oig.txt",
                "fields": [{"name": "REC", "length": 3}],
            },
        ]
    }


def test_run_spec_pay_period_slice(ctx, tmp_path):
    ctx.params.update(WF_PP_NUM="12", WF_PP_YEAR_NUM="201912")
    res = run_spec(ctx, _setup(ctx, tmp_path))
    assert res.ok, res.summary()
    (t,) = res.tables
    assert (t.counts.legacy, t.counts.target) == (2, 2)
    assert res.files[0].byte_identical
    assert res.results_table == "biis.recon_results"
    assert ctx.spark.table("biis.recon_results").filter("name = 'cpm_newpay_tbl' and not ok").count() == 0


def test_run_spec_mismatch_other_period(ctx, tmp_path):
    ctx.params.update(WF_PP_NUM="11", WF_PP_YEAR_NUM="201912")
    res = run_spec(ctx, _setup(ctx, tmp_path), write=False)
    assert not res.ok and res.results_table is None
    t = res.tables[0]
    assert t.keys.missing_sample == [{"ssn": "999", "pp_num": 11}]
    assert t.keys.extra_sample == [{"ssn": "333", "pp_num": 11}]


def test_cli_exit_codes(ctx, tmp_path, capsys):
    spec = tmp_path / "recon.json"
    spec.write_text(json.dumps(_setup(ctx, tmp_path)))
    base = ["--spec", str(spec), "--landing-root", ctx.landing_root, "--param", "WF_PP_YEAR_NUM=201912"]
    assert cli.main([*base, "--param", "WF_PP_NUM=12"]) == 0
    assert "[OK] cpm_newpay_tbl" in capsys.readouterr().out
    assert cli.main([*base, "--param", "WF_PP_NUM=11", "--no-write"]) == 1
    assert cli.main([*base, "--param", "WF_PP_NUM=11", "--no-write", "--warn-only"]) == 0


def test_substitute_missing_param():
    assert substitute({"a": ["x_${P}"]}, {"P": "1"}) == {"a": ["x_1"]}
    with pytest.raises(KeyError, match="Q"):
        substitute("${Q}", {})


def test_oracle_jdbc_options_use_secret_scope():
    calls = []

    def secret(scope, key):
        calls.append((scope, key))
        return f"<{key}>"

    o = oracle_jdbc_options("jdbc:oracle:thin:@//db:1521/BIIS", dbtable="HISTDBA.PAY_PERIOD", get_secret=secret)
    assert o["dbtable"] == "HISTDBA.PAY_PERIOD" and o["user"] == "<oracle_user>" and o["password"] == "<oracle_password>"
    assert o["driver"] == "oracle.jdbc.OracleDriver" and "query" not in o
    assert calls == [("biis", "oracle_user"), ("biis", "oracle_password")]
    with pytest.raises(ValueError):
        oracle_jdbc_options("u", get_secret=secret)
