from datetime import datetime
from decimal import Decimal

import pytest

from biis_lakehouse.pipelines.cpm_extracts import wf_cpm_afps as wf
from biis_lakehouse.pipelines.cpm_extracts._common import out_file

from conftest import NEWPAY_COLUMNS, PP_NUM, PP_YEAR, make_table, newpay_row, read_lines

T = wf.TASKS


def afps_row(i: int, **kw) -> dict:
    row = {c: 0 for c, t in NEWPAY_COLUMNS.items() if t.startswith("decimal")}
    row.update(newpay_row(i, "HHS00", MP_POOL_DES="REG", LINE_TYPE="R1", TK_NO=100 + i, CPP_BASE_PAY="1000.00",
                          CPP_GROSS_PAY="1500.00", CPP_NET_PAY="900.00", REG_HRS_CPP="80.00"))
    row.update(kw)
    return {k: v for k, v in row.items() if k in NEWPAY_COLUMNS}


@pytest.fixture
def afps(ctx, pay_period, newpay, params):
    ctx.params.update({**params, "WF_PP_NUM": "05", "WF_CYCLE_ID": "1", "CPM_AFPS_out_filename": "afps_feeder.txt"})
    newpay([afps_row(1), afps_row(2, CPP_BASE_PAY="500.00"), afps_row(3, MP_POOL_DES=" "), afps_row(4, PP_NUM=4)])
    return ctx


def test_task_order():
    assert len(T) == 17
    assert list(T)[0] == "s_CPM_AFPS_0010_Set_CPM_Calendar" and list(T)[-1] == "s_CPM_AFPS_1000_Send_Report"
    assert [k[10:14] for k in T] == sorted(k[10:14] for k in T)


def test_set_calendars(afps):
    ctx = afps
    ctx.params.update({"WF_PP_END_YEAR": "", "WF_PP_NUM": ""})
    out = T["s_CPM_AFPS_0010_Set_CPM_Calendar"](ctx)
    assert out == {"WF_PP_END_YEAR": "2019", "WF_PP_NUM": "05", "WF_PP_YEAR_NUM": "201905"}
    ctx.params.update(out)
    assert T["s_CPM_AFPS_0025_Set_Pay_Calendar"](ctx) == {"WF_PP_END_YEAR_LAST_DIGIT": "9"}
    assert read_lines(out_file(ctx, "cpm_afps_pay_period_file1.out")) == ["2019-05   "]


def test_set_pay_calendar_aborts_without_reg_rows(afps, newpay):
    newpay([afps_row(1, MP_POOL_DES="CASH")])
    with pytest.raises(RuntimeError, match="no FEEDER records"):
        T["s_CPM_AFPS_0025_Set_Pay_Calendar"](afps)


def test_update_cycle_tbl(afps):
    ctx = afps
    cols = {"PROCESS_NAME": "string", "PP_END_YEAR": "decimal(4,0)", "PP_NUM": "decimal(2,0)", "PP_END_DTE": "timestamp", "CYCLE_ID": "decimal(3,0)"}
    make_table(ctx, "CPM_CYCLE_TBL", "INFO_TARGET_DEV", cols, [
        {"PROCESS_NAME": "AFPS", "PP_END_YEAR": 2019, "PP_NUM": 5, "PP_END_DTE": datetime(2019, 3, 9), "CYCLE_ID": 2},
        {"PROCESS_NAME": "FDA", "PP_END_YEAR": 2019, "PP_NUM": 4, "PP_END_DTE": datetime(2019, 2, 23), "CYCLE_ID": 7},
    ])
    assert T["s_CPM_AFPS_0050_Update_CPM_CYCLE_TBL"](ctx) == {"WF_CYCLE_ID": "3"}
    ctx.params.update({"WF_PP_NUM": "4"})
    assert T["s_CPM_AFPS_0050_Update_CPM_CYCLE_TBL"](ctx) == {"WF_CYCLE_ID": "0"}
    rows = {r.PROCESS_NAME: r for r in ctx.table("CPM_CYCLE_TBL", "INFO_TARGET_DEV").collect()}
    assert rows["AFPS"].PP_NUM == 4 and rows["AFPS"].CYCLE_ID == 0 and rows["FDA"].CYCLE_ID == 7


def test_feeder_table_flat_file_and_gross_exp(afps):
    ctx = afps
    T["s_CPM_AFPS_0100_Data_Seperate"](ctx)
    feeder = ctx.table("HI_AFPS_FEEDER_TBL", "INFO_TARGET_DEV").orderBy("FSOC_SEC_NO").collect()
    assert len(feeder) == 2
    assert [r.FPAYROLL_CODE for r in feeder] == ["R", "R"]
    assert feeder[0].FFEEDER_SORT == "9000000011R1" and feeder[1].FBASE_PAY_AMOUNT == Decimal("500.00")

    T["s_CPM_AFPS_0200_Debridge_To_FEEDER_FLAT"](ctx)
    lines = read_lines(out_file(ctx, "afps_feeder.txt"))
    assert len(lines) == 2 and {len(x) for x in lines} == {sum(f[2] for f in wf.FEEDER_RECORD)}

    T["s_CPM_AFPS_0300_Gross_Exp_Report"](ctx)
    g = ctx.table("HI_GROSS_EXP_TBL", "INFO_TARGET_DEV").orderBy("EMPL_ID").collect()
    assert len(g) == 2
    assert g[0].EMPL_ID == "001234500" and g[0].TK_NO == "101" and g[0].PP_NUM == 5
    assert g[0].CHECK_TOT_ADD == Decimal("987.66") and g[0].DIFF == g[0].FNET_PAY - g[0].TEST_NET_TOT


def test_crossfoot_errors_and_report_files(afps):
    ctx = afps
    gcols = {"PP_END_YEAR": "decimal(4,0)", "PP_NUM": "decimal(2,0)", "EMPL_ID": "string", "CAN": "string", "TK_NO": "string", "DIFF": "decimal(11,2)"}
    make_table(ctx, "HI_GROSS_EXP_TBL", "INFO_TARGET_DEV", gcols, [
        {"PP_END_YEAR": PP_YEAR, "PP_NUM": PP_NUM, "EMPL_ID": "111223333", "CAN": "9ABC", "TK_NO": "12", "DIFF": "10.00"},
        {"PP_END_YEAR": PP_YEAR, "PP_NUM": PP_NUM, "EMPL_ID": "444556666", "CAN": "9XYZ", "TK_NO": "7", "DIFF": "-3.50"},
        {"PP_END_YEAR": PP_YEAR, "PP_NUM": PP_NUM, "EMPL_ID": "777889999", "CAN": "9QQQ", "TK_NO": "1", "DIFF": "0.00"},
    ])
    pcols = {"PP_END_YEAR": "decimal(4,0)", "PP_NUM": "decimal(2,0)", "PYF_REC_NO_PDT3": "string", "PYF_EYE_ID_PDT3": "string",
             "PYF_ACTUAL_AMT": "decimal(11,2)", "PYF_GEN_SEQ_ID": "decimal(10,0)", "PYF_PAY_DET_CD": "string", "PYF_ADJ_DATE_EFF": "string"}
    make_table(ctx, "CPM_PM3_STG_TBL", "INFO_TARGET_DEV", pcols, [
        {"PP_END_YEAR": PP_YEAR, "PP_NUM": PP_NUM, "PYF_REC_NO_PDT3": "3", "PYF_EYE_ID_PDT3": "111223333", "PYF_ACTUAL_AMT": "-5.00", "PYF_GEN_SEQ_ID": 9, "PYF_PAY_DET_CD": "AA"},
        {"PP_END_YEAR": PP_YEAR, "PP_NUM": PP_NUM, "PYF_REC_NO_PDT3": "3", "PYF_EYE_ID_PDT3": "111223333", "PYF_ACTUAL_AMT": "10.00", "PYF_GEN_SEQ_ID": 4, "PYF_PAY_DET_CD": "ZZ"},
    ])
    T["s_CPM_AFPS_0400_Crossfoot_Errors"](ctx)
    errs = ctx.table("ERROR_TBL", "INFO_TARGET_DEV").collect()
    assert len(errs) == 2
    hit = [e for e in errs if e.SOURCE_KEY == "111223333"]
    assert len(hit) == 1 and hit[0].ERROR_MESSAGE == "CROSSFOOT ERROR FOR SSN 111223333 CAN 9ABC TK 00012 AMOUNT IS 10.00"
    assert hit[0].ERROR_CODE.startswith("ZZ  ") and hit[0].CYCLE_ID == 1

    T["s_CPM_AFPS_0600_Crossfoot_Message_Details"](ctx)
    details = read_lines(out_file(ctx, "afps_details.txt"))
    assert len(details) == 2 and details[0].startswith("CROSSFOOT ERROR FOR SSN XXX-XX-3333")

    T["s_CPM_AFPS_0100_Data_Seperate"](ctx)
    T["s_CPM_AFPS_0700_Crossfoot_Message_Summary_Counts"](ctx)
    counts = read_lines(out_file(ctx, "afps_summary_counts.txt"))
    assert len(counts) == 7 and counts[1].startswith("FEEDER RECORDS READ") and counts[1].endswith(" 2")
    assert counts[3].rstrip().endswith(" 2") and counts[3].startswith("CROSS FOOT ERROR RECORDS")
    T["s_CPM_AFPS_0720_Crossfoot_Message_Gross_Expend"](ctx)
    assert read_lines(out_file(ctx, "afps_summary_gross_expend.txt")) == ["GROSS-EXPEND TOTAL".ljust(50) + "1475.32".rjust(19)]


def test_counters_and_messages(afps):
    ctx = afps
    T["s_CPM_AFPS_0100_Data_Seperate"](ctx)
    T["s_CPM_AFPS_0800_Build_Message_Counters"](ctx)
    T["s_CPM_AFPS_0820_Build_Message_Totals"](ctx)
    hdr = read_lines(out_file(ctx, "cpm_afps_counts_header.txt"))
    assert len(hdr) == 6 and hdr[0].startswith("AFPS Counts for Pay Period: 2019-05") and hdr[4].endswith(" 2")
    tot = read_lines(out_file(ctx, "cpm_afps_counts_total.txt"))
    assert len(tot) == 8 and tot[4].startswith("AMOUNT OF BASE PAY ON MASTER") and tot[4].endswith("1500.00")
    c = ctx.table("AFPS_COUNTER_TBL", "INFO_TARGET_DEV").collect()
    assert len(c) == 10 and {r.PROCESS_NAME for r in c} == {"m_CPM_AFPS_0800_Build_Message_Counters", "m_CPM_AFPS_0820_Build_Message_Totals"}

    v = T["s_CPM_AFPS_0500_Crossfoot_Message_Header"](ctx)
    assert v["WF_SUBJECT"].endswith("AFPS Process Completed Successfully for: 2019-05")
    assert read_lines(out_file(ctx, "afps_header.txt"))[0][500:].startswith("CROSSFOOT ERROR MESSAGE REPORT FOR PAY PERIOD: 2019-05")
    m = T["s_CPM_AFPS_0900_Build_Message"](ctx)
    assert "AFPS_FLAT_PP05.txt" in m["WF_MESSAGE"] and "P3306D04.GPRECON.P905" in m["WF_MESSAGE"]
    for k in ("s_CPM_AFPS_0760_Concatenate_Crossfoot_Files", "s_CPM_AFPS_0860_Concatenate_Counts_Files", "s_CPM_AFPS_1000_Send_Report"):
        T[k](ctx)
    assert read_lines(out_file(ctx, "generic_target_file1.out")) == ["1"]
