import pytest

from biis_lakehouse.pipelines.cpm_extracts import wf_cpm_oig as wf
from biis_lakehouse.pipelines.cpm_extracts._common import out_file

from conftest import newpay_row, read_lines


def test_task_order():
    assert list(wf.TASKS) == [
        "s_CPM_OIG_Set_CPM_Calendar",
        "s_CPM_OIG_Set_Pay_Calendar",
        "s_CPM_OIG_Load_CPM_OIG_File",
        "s_CPM_OIG_Build_Message",
        "email_CPM_OIG",
    ]


def test_set_cpm_calendar_defaults_to_current_pay_period(ctx, pay_period):
    out = wf.TASKS["s_CPM_OIG_Set_CPM_Calendar"](ctx)
    assert out == {"WF_PP_END_YEAR": "2019", "WF_PP_NUM": "5", "WF_PP_YEAR_NUM": "201905"}
    assert read_lines(out_file(ctx, "cpm_oig_cpm_pay_period_file.txt")) == ["2019-05   "]


def test_set_cpm_calendar_uses_parameters(ctx, pay_period):
    ctx.params.update({"WF_PP_END_YEAR": "2019", "WF_PP_NUM": "4"})
    out = wf.TASKS["s_CPM_OIG_Set_CPM_Calendar"](ctx)
    assert out["WF_PP_YEAR_NUM"] == "201904"


def test_set_cpm_calendar_aborts_on_bad_parameter(ctx, pay_period):
    ctx.params.update({"WF_PP_END_YEAR": "20X9", "WF_PP_NUM": "4"})
    with pytest.raises(RuntimeError, match="not a valid pay period year"):
        wf.TASKS["s_CPM_OIG_Set_CPM_Calendar"](ctx)


def test_set_pay_calendar_aborts_without_oig_rows(ctx, pay_period, newpay, params):
    ctx.params.update(params)
    newpay([newpay_row(1, "CDC00")])
    with pytest.raises(RuntimeError, match="no OIG records"):
        wf.TASKS["s_CPM_OIG_Set_Pay_Calendar"](ctx)
    newpay([newpay_row(1, "OIG00")])
    assert wf.TASKS["s_CPM_OIG_Set_Pay_Calendar"](ctx) is None
    assert read_lines(out_file(ctx, "cpm_oig_pay_period_file.txt")) == ["2019-05   "]


def test_load_oig_file_and_message(ctx, pay_period, newpay, params):
    ctx.params.update(params)
    newpay([newpay_row(1, "OIG00"), newpay_row(2, "OIG00"), newpay_row(3, "NIH00"), newpay_row(4, "OIG00", MP_POOL_DES="CASH")])
    wf.TASKS["s_CPM_OIG_Load_CPM_OIG_File"](ctx)
    lines = read_lines(out_file(ctx, "CPM_OIG_out.txt"))
    assert len(lines) == 2
    assert all(len(line) == wf.LRECL for line in lines)
    fields = {f[0]: f for f in wf.SKPAYROLL_MASTER}
    offsets, pos = {}, 0
    for f in wf.SKPAYROLL_MASTER:
        offsets[f[0]] = (pos, pos + f[2])
        pos += f[2]
    a, b = offsets["SKSOC_SEC_NO"]
    assert sorted(line[a:b] for line in lines) == ["00123450001", "00123450002"]
    a, b = offsets["SKSURNAME_3"]
    assert lines[0][a:b] == "DOE"
    a, b = offsets["SKHRLY_RATE_INT"]
    assert lines[0][a:b] == "025"
    a, b = offsets["SKHRLY_RATE_DEC"]
    assert fields["SKHRLY_RATE_DEC"][1] == "UZONED" and lines[0][a:b] == "75"
    a, b = offsets["SKCPP_OT_PAY_SIGN"]
    assert lines[0][a:b] == "-"

    out = wf.TASKS["s_CPM_OIG_Build_Message"](ctx)
    assert out["WF_SUBJECT"] == "Prod: CPM OIG Process Completed Successfully for: 2019-05"
    assert out["WF_MESSAGE"] == "Total number of CPM OIG records loaded is: 2"
    msg = read_lines(out_file(ctx, "cpm_oig_message_file.txt"))
    assert len(msg) == 1 and msg[0].startswith("Prod: CPM OIG") and len(msg[0]) == 400

    ctx.params.update(out)
    assert wf.TASKS["email_CPM_OIG"](ctx)["EMAIL_TO"] == "cpm@example.gov"
