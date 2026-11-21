from biis_lakehouse.common import delta
from biis_lakehouse.common.fixed_width import Field, read_fixed_width, write_fixed_width
from biis_lakehouse.runner import _parse_params


def test_table_name(ctx):
    assert ctx.table_name("PAY_PERIOD", "HISTDBA") == "histdba.pay_period"
    assert ctx.table_name("CPM_NEWPAY_TBL") == "biis.cpm_newpay_tbl"
    ctx.catalog = "biis_prod"
    assert ctx.table_name("NWK_ACTION_PRIMARY_TBL", "NKNIGHT") == "biis_prod.nknight.nwk_action_primary_tbl"


def test_params():
    p = _parse_params(["$$WF_PP_NUM=12", "$Param_CPM_PAD_filename=pad.txt", "X=a=b"])
    assert p == {"WF_PP_NUM": "12", "CPM_PAD_filename": "pad.txt", "X": "a=b"}


def test_merge_with_update_strategy(ctx):
    spark, tbl = ctx.spark, ctx.table_name("PAY_PERIOD", "HISTDBA")
    delta.overwrite(ctx, spark.createDataFrame([(1, 2019, "N"), (2, 2019, "N"), (3, 2019, "N")], "pp_num int, pp_end_year int, flag string"), tbl)
    src = spark.createDataFrame([(1, 2019, "Y"), (2, 2019, "X"), (3, 2019, "Z"), (4, 2019, "Y")], "pp_num int, pp_end_year int, flag string")
    src = delta.update_strategy(src, "CASE WHEN flag = 'X' THEN DD_DELETE WHEN flag = 'Z' THEN DD_REJECT ELSE DD_UPDATE END")
    delta.merge(ctx, src, tbl, keys=["pp_num", "pp_end_year"])
    rows = {r.pp_num: r.flag for r in spark.table(tbl).collect()}
    assert rows == {1: "Y", 3: "N", 4: "Y"}


def test_fixed_width_roundtrip(ctx):
    fields = [Field("SSN", 9), Field("AMT", 7, numeric=True), Field("NAME", 5)]
    df = ctx.spark.createDataFrame([("123456789", "42", "AB")], "SSN string, AMT string, NAME string")
    out = ctx.path("out", "TEST", "f.txt")
    write_fixed_width(df, out, fields)
    lines = [r.value for r in ctx.spark.read.text(out).collect()]
    assert lines == ["1234567890000042AB   "]
    back = read_fixed_width(ctx.spark, out, fields).collect()[0]
    assert (back.SSN, back.AMT, back.NAME) == ("123456789", "0000042", "AB")
