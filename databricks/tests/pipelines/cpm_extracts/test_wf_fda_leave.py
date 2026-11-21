import os
from datetime import datetime
from decimal import Decimal

import pytest

from biis_lakehouse.pipelines.cpm_extracts import wf_fda_leave as wf
from biis_lakehouse.pipelines.cpm_extracts._common import out_file

from cpm_extracts_support import make_table, read_lines

T = wf.TASKS
E1, E2, E3 = "900000001", "900000002", "900000003"


def tatran_line(emp: str, rec: str, data: str = "") -> str:
    return f"12345{emp}201905{rec}{data}"


def write_input(ctx, name="tatranP905", lines=None):
    d = ctx.path("in", "CPM")
    os.makedirs(d, exist_ok=True)
    lines = lines if lines is not None else [
        tatran_line(E1, "01", "HEADER"),
        tatran_line(E1, "02", "100|008.25|"),
        tatran_line(E1, "03", "215|001.25|"),
        tatran_line(E2, "02", "100|004.75|"),
        tatran_line(E3, "02", "100|001.50|"),
        tatran_line(E3, "99", "TRAILER"),
    ]
    with open(os.path.join(d, name), "w") as fh:
        fh.write("\n".join(lines) + "\n")


DEC = "decimal(7,2)"


def lookup_tables(ctx):
    pp = {"PP_END_YEAR": "decimal(10,0)", "PP_NUM": "decimal(10,0)"}
    cols = wf._ports_columns()
    mer = {**pp, "MER_SSN": "string", "MER_ORG": "string", **{c: DEC for c in cols["lkp_CPM_MER_DETAIL_STG_TBL"] if c != "MER_SSN"}}
    make_table(ctx, "CPM_MER_DETAIL_STG_TBL", "INFO_TARGET_DEV", mer, [
        {"PP_END_YEAR": 2019, "PP_NUM": 5, "MER_SSN": E1, "MER_ORG": "D123456", "MER_ANN_LV_ACCRUAL_RT": "6.00", "MER_ANN_LV_ENDBAL": "-12.50"},
        {"PP_END_YEAR": 2019, "PP_NUM": 5, "MER_SSN": E2, "MER_ORG": "E123456", "MER_ANN_LV_ACCRUAL_RT": "4.00"},
    ])
    pad = {**pp, "PAD_SOC_SEC_NO": "string", "PAD_ORGANIZATION": "string", **{c: DEC for c in cols["lkp_CPM_PAD_DETAIL_STG_TBL"] if c != "PAD_SOC_SEC_NO"}}
    make_table(ctx, "CPM_PAD_DETAIL_STG_TBL", "INFO_TARGET_DEV", pad, [
        {"PP_END_YEAR": 2019, "PP_NUM": 5, "PAD_SOC_SEC_NO": E1, "PAD_ORGANIZATION": "D1", "PAD_OVERTIME_HOURS": "3.25"},
    ])
    ytd = {**pp, "DYD_SSN_1": "string", "DYD_ACTIVITY": "string", "DYD_ADMINISTRATIVE_LEAVE": DEC, "DYD_LWOP_AWOL": DEC}
    make_table(ctx, "CPM_YTD_DETAIL_STG_TBL", "INFO_TARGET_DEV", ytd, [
        {"PP_END_YEAR": 2019, "PP_NUM": 5, "DYD_SSN_1": E1, "DYD_ACTIVITY": "D1", "DYD_LWOP_AWOL": "1.50"},
    ])
    newpay = {"PP_END_YEAR": "decimal(4,0)", "PP_NUM": "decimal(2,0)", "MP_POOL_DES": "string", "DFAS_PSEUDO_SSN": "string",
              "SOC_SEC_NO": "decimal(11,0)", "HRLY_RATE": "decimal(5,2)", "OT_RATE": "decimal(5,2)"}
    make_table(ctx, "CPM_NEWPAY_TBL", "INFO_TARGET_DEV", newpay, [
        {"PP_END_YEAR": 2019, "PP_NUM": 5, "MP_POOL_DES": " ", "DFAS_PSEUDO_SSN": E1, "SOC_SEC_NO": 123450001, "HRLY_RATE": "25.75", "OT_RATE": "38.63"},
        {"PP_END_YEAR": 2019, "PP_NUM": 5, "MP_POOL_DES": " ", "DFAS_PSEUDO_SSN": E2, "SOC_SEC_NO": 123450002, "HRLY_RATE": "20.00", "OT_RATE": "30.00"},
    ])
    make_table(ctx, "PSEUDOSSN_TBL", "INFO_TARGET_DEV", {"PSEUDOSSN": "string", "SSN": "string"}, [{"PSEUDOSSN": E1, "SSN": "123450001"}])


@pytest.fixture
def fda(ctx, pay_period, params):
    ctx.params.update({**params, "WF_PP_NUM": "05", "WF_CYCLE_ID": "1", "FDA_12100_file": "tatran*", "FDA_12200_out_file": "fda_12200.txt",
                       "WF_SUBJECT": "start", "WF_MESSAGE": "msg"})
    lookup_tables(ctx)
    write_input(ctx)
    return ctx


def test_task_order():
    assert list(T) == [
        "s_0010_PM_FDA_Verify_File", "s_0020_PM_FDA_Set_CPM_Calendar", "s_0025_PM_FDA_Set_Pay_Calendar",
        "s_0050_PM_FDA_Update_CPM_CYCLE_TBL_FDA", "s_0100_PM_FDA_Load_TATRAN_To_DB", "s_0150_PM_FDA_Error_Counter",
        "s_0200_PM_FDA_Create_200_Rows", "s_0300_PM_FDA_Create_Output_File", "s_0500_PM_FDA_IO_Counter", "s_1100_PM_FDA_Send_Email",
    ]


def test_verify_file_and_calendars(fda):
    ctx = fda
    assert T["s_0010_PM_FDA_Verify_File"](ctx) is None
    assert read_lines(out_file(ctx, "cpm_fda_pay_period_file.txt")) == ["2019-05   "]
    ctx.params.update({"WF_PP_END_YEAR": "", "WF_PP_NUM": ""})
    out = T["s_0020_PM_FDA_Set_CPM_Calendar"](ctx)
    assert out == {"WF_PP_END_YEAR": "2019", "WF_PP_NUM": "05"}
    ctx.params.update(out)
    assert T["s_0025_PM_FDA_Set_Pay_Calendar"](ctx) is None
    assert read_lines(out_file(ctx, "cpm_fda_cpm_pay_period_file.txt")) == ["2019-05   "]


def test_verify_file_aborts(fda):
    ctx = fda
    write_input(ctx, "tatranP904")
    with pytest.raises(RuntimeError, match="There are : 2 FDA TATRAN files"):
        T["s_0010_PM_FDA_Verify_File"](ctx)
    os.remove(ctx.path("in", "CPM", "tatranP905"))
    with pytest.raises(RuntimeError, match="does not match the current pay period. 2019-05"):
        T["s_0010_PM_FDA_Verify_File"](ctx)
    os.remove(ctx.path("in", "CPM", "tatranP904"))
    with pytest.raises(RuntimeError, match="no FDA TATRAN files"):
        T["s_0010_PM_FDA_Verify_File"](ctx)


def test_update_cycle(fda):
    ctx = fda
    cols = {"PROCESS_NAME": "string", "PP_END_YEAR": "decimal(4,0)", "PP_NUM": "decimal(2,0)", "PP_END_DTE": "timestamp", "CYCLE_ID": "decimal(3,0)"}
    make_table(ctx, "CPM_CYCLE_TBL", "INFO_TARGET_DEV", cols, [
        {"PROCESS_NAME": "FDA", "PP_END_YEAR": 2019, "PP_NUM": 4, "PP_END_DTE": datetime(2019, 2, 23), "CYCLE_ID": 3},
    ])
    assert T["s_0050_PM_FDA_Update_CPM_CYCLE_TBL_FDA"](ctx) == {"WF_CYCLE_ID": "0"}
    assert ctx.table("CPM_CYCLE_TBL", "INFO_TARGET_DEV").collect()[0].PP_NUM == 5


def test_end_to_end_extract(fda):
    ctx = fda
    T["s_0100_PM_FDA_Load_TATRAN_To_DB"](ctx)
    rows = ctx.table("HI_PM_FDA_TATRAN_TBL", "INFO_TARGET_DEV").orderBy("FDA_SEQ").collect()
    assert [(r.FDA_BATCH_ID, r.FDA_EMP_ID, r.FDA_REC_TYPE, r.FDA_SEQ) for r in rows] == [
        (1, E1, "02", 1), (1, E1, "03", 2), (2, E2, "02", 3), (3, E3, "02", 4)]

    T["s_0150_PM_FDA_Error_Counter"](ctx)
    errs = ctx.table("ERROR_TBL", "INFO_TARGET_DEV").collect()
    assert sorted((e.SOURCE_KEY, e.ERROR_MESSAGE[:28]) for e in errs) == sorted([
        (E2, "DOES NOT HAVE A PAD RECORD ("), (E2, "DOES NOT HAVE A YTD RECORD ("),
        (E3, "DOES NOT HAVE A PAD RECORD ("), (E3, "DOES NOT HAVE A YTD RECORD ("),
        (E3, "DOES NOT HAVE A PAYMASTER RE"), (E3, "DOES NOT HAVE A MER RECORD (")])
    assert {e.CYCLE_ID for e in errs} == {1} and {e.PROCESS_NAME for e in errs} == {"m_0150_PM_FDA_Error_Counter"}

    T["s_0200_PM_FDA_Create_200_Rows"](ctx)
    new = ctx.table("HI_PM_FDA_TATRAN_TBL", "INFO_TARGET_DEV").filter("FDA_REC_TYPE = '12'").collect()
    assert len(new) == 28
    e1 = {int(r.FDA_SEQ): r for r in new if r.FDA_BATCH_ID == 1}
    assert len(e1) == 14 and {r.FDA_EMP_ID for r in e1.values()} == {"123450001"}
    assert {r.FDA_EMP_ID for r in new if r.FDA_BATCH_ID == 2} == {None}
    assert e1[1001000].FDA_DATA.startswith("200|+006.00")
    assert e1[1001009].FDA_DATA.startswith("209|+00001.50")
    assert e1[1001015].FDA_DATA.startswith("215|+025.75")

    T["s_0300_PM_FDA_Create_Output_File"](ctx)
    lines = read_lines(out_file(ctx, "fda_12200.txt"))
    assert len(lines) == 32 and {len(x) for x in lines} == {136}
    assert tatran_line(E1, "02", "100|008.15|").ljust(136) in lines
    assert tatran_line(E1, "03", "215|001.25|").ljust(136) in lines

    out = T["s_0500_PM_FDA_IO_Counter"](ctx)
    assert out == {"WF_SUBJECT": "Prod: FDA Extract completed successfully for Pay Period:  2019-05", "WF_MESSAGE": "msg"}
    msg = read_lines(out_file(ctx, "fda_extract_message_file.txt"))
    assert [m[:50].rstrip() for m in msg] == ["FDA Leave Counts for Pay Period: 2019-05", "", "TATRAN Records Read", "Leave Records Read",
                                             "Error Records", "New TATRAN Records Written"]
    assert [m[50:].strip() for m in msg[2:]] == ["4", "3", "6", "32"]
    ctr = {r.COUNTER_DESCRIPTION: r for r in ctx.table("COUNTER_TBL", "INFO_TARGET_DEV").collect()}
    assert len(ctr) == 4 and ctr["New TATRAN Records Written"].COUNTER_VALUE == Decimal(32)
    assert {r.PROCESS_NAME for r in ctr.values()} == {"FDA EXTRACT"} and {r.PP_NUM for r in ctr.values()} == {5}

    T["s_1100_PM_FDA_Send_Email"](ctx)
    assert read_lines(out_file(ctx, "generic_target_file1.out")) == ["1"]


def test_io_counter_without_errors_writes_nothing(fda):
    ctx = fda
    T["s_0100_PM_FDA_Load_TATRAN_To_DB"](ctx)
    make_table(ctx, "ERROR_TBL", "INFO_TARGET_DEV", {"PROCESS_NAME": "string", "ERROR_MESSAGE": "string", "SOURCE_KEY": "string", "ERROR_DATE": "timestamp",
                                                     "PP_END_YEAR": "decimal(4,0)", "PP_NUM": "decimal(2,0)", "CYCLE_ID": "decimal(3,0)", "ERROR_CODE": "string"}, [])
    assert T["s_0500_PM_FDA_IO_Counter"](ctx) == {"WF_SUBJECT": "start", "WF_MESSAGE": "msg"}
    assert read_lines(out_file(ctx, "fda_extract_message_file.txt")) == []
