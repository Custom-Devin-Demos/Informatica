from __future__ import annotations

import datetime as dt

import pytest

from biis_lakehouse.pipelines.cpm import wf_cpm
from biis_lakehouse.pipelines.cpm._helpers import read_flat

from conftest import make_table, vsam_line, write_vsam


def _rows(ctx, table, owner=None):
    return ctx.table(table, owner).collect()


def test_tasks_cover_every_session_in_workflow_order():
    assert list(wf_cpm.TASKS) == [
        "s_CPM_Current_Pay_Period",
        "s_CPM_Load_CPM_MER_Staging_Tables",
        "s_CPM_Load_CPM_PAD_Staging_Tables",
        "s_CPM_Load_CPM_YTD_Staging_Tables",
        "s_CPM_Load_CPM_PMR_Staging_Tables",
        "s_CPM_Load_CPM_NEWPAY_STG_YTD_STATE_TBL",
        "s_CPM_Load_CPM_NEWPAY_STG_ALT_TBL",
        "s_CPM_Load_CPM_NEWPAY_STG_DETAIL_TBL",
        "s_CPM_Load_CPM_NEWPAY_STG_TYPE_1_2_TBL",
        "s_CPM_Load_CPM_NEWPAY_STG_TYPE_3_FDR_TBL",
        "s_CPM_Load_From_FDR_CPM_NEWPAY_STG_TYPE_3_TBL",
        "s_CPM_Load_FDR_CPM_NEWPAY_TBL",
        "s_CPM_Load_CPM_NEWPAY_TBL",
        "s_CPM_Build_Message_Counters",
        "s_CPM_Send_Counts",
    ]


def _pay_periods(ctx):
    make_table(ctx, "PAY_PERIOD", [
        {"PP_NUM": 4, "PP_END_YEAR": 2024, "PP_START_DTE": dt.datetime(2024, 2, 11), "PP_END_DTE": dt.datetime(2024, 2, 24), "CURR_PP_FLAG": "N"},
        {"PP_NUM": 5, "PP_END_YEAR": 2024, "PP_START_DTE": dt.datetime(2024, 2, 25), "PP_END_DTE": dt.datetime(2024, 3, 9), "CURR_PP_FLAG": "Y"},
    ], owner="HISTDBA")


def test_current_pay_period(cpm):
    _pay_periods(cpm)
    out = wf_cpm.TASKS["s_CPM_Current_Pay_Period"](cpm)
    assert out == {"WF_PP_END_YEAR": "2024", "WF_PP_NUM": "5", "WF_PP_YEAR_NUM": "202405"}
    rows = read_flat(cpm, "CPM_PAY_PERIOD_DATE_FILE", "cpm_pay_period.txt").collect()
    assert [r.PAY_PERIOD_TXT for r in rows] == ["202405"]


def _count(ctx, table):
    return ctx.table(table).count()


def test_mer_staging(cpm):
    _pay_periods(cpm)
    write_vsam(cpm, "CPM_Biweekly_MER_filename", [
        vsam_line("MER_FILE", {"MER_RECTYP1": "1", "MER_REPORT_DT": "240301", "MER_FILE_ID": "MER01"}),
        vsam_line("MER_FILE", {"MER_RECTYP": "3", "MER_SSN": "111111111", "MER_ORG": "ORG0001"}),
        vsam_line("MER_FILE", {"MER_RECTYP": "3", "MER_SSN": "222222222", "MER_ORG": "ORG0002"}),
    ])
    wf_cpm.TASKS["s_CPM_Load_CPM_MER_Staging_Tables"](cpm)
    hdr = _rows(cpm, "CPM_MER_HEADER_STG_TBL")
    assert len(hdr) == 1
    det = _rows(cpm, "CPM_MER_DETAIL_STG_TBL")
    assert sorted(r.MER_SSN for r in det) == ["111111111", "222222222"]
    assert {(int(r.PP_END_YEAR), int(r.PP_NUM)) for r in det} == {(2024, 5)}


def test_mer_header_outside_pay_period_aborts(cpm):
    _pay_periods(cpm)
    write_vsam(cpm, "CPM_Biweekly_MER_filename", [
        vsam_line("MER_FILE", {"MER_RECTYP1": "1", "MER_REPORT_DT": "240215"}),
    ])
    with pytest.raises(Exception, match="not in the current pay period"):
        wf_cpm.TASKS["s_CPM_Load_CPM_MER_Staging_Tables"](cpm)


def test_pad_staging(cpm):
    _pay_periods(cpm)
    write_vsam(cpm, "CPM_PAD_filename", [
        vsam_line("PAD_FILE", {"PAD_RECORD_TYPE": "H", "PAD_FILE_ID": "P6331X01", "PAD_PP_END_DTE": "240309",
                               "PAD_DISBURSING_DTE": "240315", "PAD_QTR_END_DTE": "240331"}),
        vsam_line("PAD_FILE", {"PAD_SERVING_AGENCY": "DE", "PAD_SOC_SEC_NO": "111111111", "PAD_REGULAR_EARNINGS": "1234.56"}),
        vsam_line("PAD_FILE", {"PAD_SERVING_AGENCY": "DE", "PAD_SOC_SEC_NO": "222222222", "PAD_REGULAR_EARNINGS": "-10.00"}),
    ])
    wf_cpm.TASKS["s_CPM_Load_CPM_PAD_Staging_Tables"](cpm)
    assert _count(cpm, "CPM_PAD_HEADER_STG_TBL") == 1
    det = {r.PAD_SOC_SEC_NO: r for r in _rows(cpm, "CPM_PAD_DETAIL_STG_TBL")}
    assert set(det) == {"111111111", "222222222"}
    assert float(det["111111111"].PAD_REGULAR_EARNINGS) == 1234.56


def test_ytd_staging(cpm):
    _pay_periods(cpm)
    write_vsam(cpm, "CPM_Year_To_Date_filename", [
        vsam_line("YTD_FILE", {"DFAS_YTD_RECORD_TYPE1": "0", "DFAS_YTD_FILE_NAME": "YTDFILE", "DFAS_YTD_PPEND_DATE": "20240309"}),
        vsam_line("YTD_FILE", {"DFAS_YTD_RECORD_TYPE2": "1", "DYD_SSN_1": "111111111", "DYD_BASE_PAY": "50000.00"}),
        vsam_line("YTD_FILE", {"DFAS_YTD_RECORD_TYPE": "2", "DYD_SSN_2": "111111111", "DYD_STATE_TAX_CODE": "NY",
                               "DYD_STATE_TAX_DEDUC": "100.25", "DYD_PAY_SUB_STATE_TAX": "2000.00"}),
        vsam_line("YTD_FILE", {"DFAS_YTD_RECORD_TYPE": "2", "DYD_SSN_2": "111111111", "DYD_STATE_TAX_CODE": "CA",
                               "DYD_STATE_TAX_DEDUC": "50.00", "DYD_PAY_SUB_STATE_TAX": "1000.00"}),
    ])
    wf_cpm.TASKS["s_CPM_Load_CPM_YTD_Staging_Tables"](cpm)
    assert _count(cpm, "CPM_YTD_HEADER_STG_TBL") == 1
    assert [r.DYD_SSN_1 for r in _rows(cpm, "CPM_YTD_DETAIL_STG_TBL")] == ["111111111"]
    st = _rows(cpm, "CPM_YTD_STATE_STG_TBL")
    assert sorted(r.DYD_STATE_TAX_CODE for r in st) == ["CA", "NY"]


def _paymaster_lines():
    return [
        vsam_line("PAYMASTER_FILE", {"PYF_REC_NO_BLANK": " ", "PYF_REC_PPE_DATE": "20240309"}),
        vsam_line("PAYMASTER_FILE", {"PYF_REC_NO_1": "1", "PYF_EYE_ID_1": "111111111", "PYF_EYE_NME": "DOE JANE"}),
        vsam_line("PAYMASTER_FILE", {"PYF_REC_NO_2": "2", "PYF_EYE_ID_2": "111111111", "PYF_AGY_CD": "DE"}),
        vsam_line("PAYMASTER_FILE", {"PYF_REC_NO_PDT3": "3", "PYF_EYE_ID_PDT3": "111111111", "PYF_PAY_TAC_TYP": "00100",
                                     "PYF_PAY_DET_CD": "WL", "PYF_ACTUAL_AMT": "100.00"}),
        vsam_line("PAYMASTER_FILE", {"PYF_REC_NO_PDT3": "3", "PYF_EYE_ID_PDT3": "111111111", "PYF_PAY_TAC_TYP": "00200",
                                     "PYF_PAY_DET_CD": "WD", "PYF_ACTUAL_AMT": "25.50"}),
    ]


def test_pmr_staging(cpm):
    _pay_periods(cpm)
    write_vsam(cpm, "CPM_Department_of_Energy_filename", _paymaster_lines())
    wf_cpm.TASKS["s_CPM_Load_CPM_PMR_Staging_Tables"](cpm)
    assert _count(cpm, "CPM_PMH_STG_TBL") == 1
    assert [r.PYF_EYE_ID_1 for r in _rows(cpm, "CPM_PM1_STG_TBL")] == ["111111111"]
    assert [r.PYF_EYE_ID_2 for r in _rows(cpm, "CPM_PM2_STG_TBL")] == ["111111111"]
    pm3 = _rows(cpm, "CPM_PM3_STG_TBL")
    assert sorted(r.PYF_PAY_DET_CD for r in pm3) == ["WD", "WL"]
