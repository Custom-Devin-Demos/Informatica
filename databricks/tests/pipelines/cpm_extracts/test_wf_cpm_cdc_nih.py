import pytest

from biis_lakehouse.pipelines.cpm_extracts import wf_cpm_cdc, wf_cpm_nih
from biis_lakehouse.pipelines.cpm_extracts._common import out_file

from conftest import newpay_row, read_lines


def _offsets(fields):
    out, pos = {}, 0
    for f in fields:
        out[f[0]] = (pos, pos + f[2])
        pos += f[2]
    return out


def test_task_order():
    assert list(wf_cpm_cdc.TASKS) == [
        "s_CPM_CDC_Set_CPM_Calendar",
        "s_CPM_CDC_Set_Pay_Calendar",
        "s_CPM_CDC_Load_CPM_CDC_Header_File",
        "s_CPM_CDC_Load_CPM_CDC_Data_File",
        "s_CPM_CDC_Concatenate_Files",
        "s_CPM_CDC_Build_Message",
        "email_CPM_CDC",
    ]
    assert [t.replace("NIH", "CDC") for t in wf_cpm_nih.TASKS] == list(wf_cpm_cdc.TASKS)


def test_cdc_workflow(ctx, pay_period, newpay, params):
    ctx.params.update(params)
    newpay([
        newpay_row(1, "CDC00"),
        newpay_row(2, "ATSDR"),
        newpay_row(3, "HRSA0", ORG_CDE="ANC341  "),
        newpay_row(4, "NIH00"),
        newpay_row(5, "CDC00", PP_NUM=4),
    ])
    assert wf_cpm_cdc.TASKS["s_CPM_CDC_Set_CPM_Calendar"](ctx)["WF_PP_YEAR_NUM"] == "201905"
    wf_cpm_cdc.TASKS["s_CPM_CDC_Set_Pay_Calendar"](ctx)

    wf_cpm_cdc.TASKS["s_CPM_CDC_Load_CPM_CDC_Header_File"](ctx)
    hdr = read_lines(out_file(ctx, "CPM_CDC_HDR.txt"))
    off = _offsets(wf_cpm_cdc.WS_CDC_HDR)
    assert len(hdr) == 1 and len(hdr[0]) == wf_cpm_cdc.LRECL
    a, b = off["HDR_LNAME"]
    assert hdr[0][a:b] == "REGULAR".ljust(16)
    a, b = off["HDR_PP_NUMBER"]
    assert hdr[0][a:b] == "05"

    wf_cpm_cdc.TASKS["s_CPM_CDC_Load_CPM_CDC_Data_File"](ctx)
    dat = read_lines(out_file(ctx, "CPM_CDC_DAT.txt"))
    assert len(dat) == 3 and all(len(r) == wf_cpm_cdc.LRECL for r in dat)
    a, b = _offsets(wf_cpm_cdc.WS_PAY_OUT_REC)["SOC_SEC_NO"]
    assert sorted(r[a:b] for r in dat) == ["00123450001", "00123450002", "00123450003"]

    wf_cpm_cdc.TASKS["s_CPM_CDC_Concatenate_Files"](ctx)
    assert read_lines(out_file(ctx, "generic_target_file.txt")) == ["1"]

    out = wf_cpm_cdc.TASKS["s_CPM_CDC_Build_Message"](ctx)
    # 3 data records + 1 header (lkp_Pay_Period_Total)
    assert out["WF_MESSAGE"] == "Total number of CPM CDC records loaded is: 4"
    assert out["WF_SUBJECT"].endswith("CPM CDC Process Completed Successfully for: 2019-05")


def test_cdc_set_pay_calendar_requires_cdc00(ctx, pay_period, newpay, params):
    ctx.params.update(params)
    newpay([newpay_row(1, "ATSDR")])
    with pytest.raises(RuntimeError, match="no CDC records"):
        wf_cpm_cdc.TASKS["s_CPM_CDC_Set_Pay_Calendar"](ctx)


def test_nih_workflow(ctx, pay_period, newpay, params):
    ctx.params.update(params)
    newpay([newpay_row(1, "NIH00"), newpay_row(2, "NIH00", CPP_OT_PAY="-1.25"), newpay_row(3, "CDC00")])
    wf_cpm_nih.TASKS["s_CPM_NIH_Set_CPM_Calendar"](ctx)
    wf_cpm_nih.TASKS["s_CPM_NIH_Set_Pay_Calendar"](ctx)
    wf_cpm_nih.TASKS["s_CPM_NIH_Load_CPM_NIH_Header_File"](ctx)
    hdr = read_lines(out_file(ctx, "CPM_NIH_HDR.txt"))
    assert len(hdr) == 1 and len(hdr[0]) == wf_cpm_nih.HDR_LRECL
    a, b = _offsets(wf_cpm_nih.WS_NIH_HDR)["HDR_LVII_NUMBER"]
    assert hdr[0][a:b] == "15"

    wf_cpm_nih.TASKS["s_CPM_NIH_Load_CPM_NIH_Data_File"](ctx)
    dat = read_lines(out_file(ctx, "CPM_NIH_DAT.txt"))
    assert len(dat) == 2 and all(len(r) == wf_cpm_nih.DAT_LRECL for r in dat)
    fields = {f[0]: f for f in wf_cpm_nih.NIH_PAYROLL_MASTER}
    zoned = [n for n, f in fields.items() if f[1] == "ZONED" and f[4] and "`CPP_OT_PAY`" in f[4]]
    assert zoned, "expected a signed zoned field fed by CPP_OT_PAY"
    a, b = _offsets(wf_cpm_nih.NIH_PAYROLL_MASTER)[zoned[0]]
    assert {r[b - 1] for r in dat} <= set("{ABCDEFGHI}JKLMNOPQR")

    wf_cpm_nih.TASKS["s_CPM_NIH_Concatenate_Files"](ctx)
    out = wf_cpm_nih.TASKS["s_CPM_NIH_Build_Message"](ctx)
    assert out["WF_MESSAGE"] == "Total number of CPM NIH records loaded is: 3"
    ctx.params.update(out)
    assert wf_cpm_nih.TASKS["email_CPM_NIH"](ctx)["EMAIL_SUBJECT"] == out["WF_SUBJECT"]
