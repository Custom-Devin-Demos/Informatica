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


# --------------------------------------------------------------------------- whole workflow, session by session
SSN, SSN2 = "111111111", "222222222"

TARGETS = {
    "s_CPM_Current_Pay_Period": [],
    "s_CPM_Load_CPM_MER_Staging_Tables": ["CPM_MER_HEADER_STG_TBL", "CPM_MER_DETAIL_STG_TBL"],
    "s_CPM_Load_CPM_PAD_Staging_Tables": ["CPM_PAD_HEADER_STG_TBL", "CPM_PAD_DETAIL_STG_TBL"],
    "s_CPM_Load_CPM_YTD_Staging_Tables": ["CPM_YTD_HEADER_STG_TBL", "CPM_YTD_DETAIL_STG_TBL", "CPM_YTD_STATE_STG_TBL"],
    "s_CPM_Load_CPM_PMR_Staging_Tables": ["CPM_PMH_STG_TBL", "CPM_PM1_STG_TBL", "CPM_PM2_STG_TBL", "CPM_PM3_STG_TBL"],
    "s_CPM_Load_CPM_NEWPAY_STG_YTD_STATE_TBL": ["CPM_NEWPAY_STG_YTD_STATE_TBL"],
    "s_CPM_Load_CPM_NEWPAY_STG_ALT_TBL": ["CPM_NEWPAY_STG_ALT_TBL", "ERROR_TBL"],
    "s_CPM_Load_CPM_NEWPAY_STG_DETAIL_TBL": ["CPM_NEWPAY_STG_DETAIL_TBL"],
    "s_CPM_Load_CPM_NEWPAY_STG_TYPE_1_2_TBL": ["CPM_NEWPAY_STG_TYPE_1_2_TBL", "ERROR_TBL"],
    "s_CPM_Load_CPM_NEWPAY_STG_TYPE_3_FDR_TBL": ["CPM_NEWPAY_STG_TYPE_3_FDR_TBL"],
    "s_CPM_Load_From_FDR_CPM_NEWPAY_STG_TYPE_3_TBL": ["CPM_NEWPAY_STG_TYPE_3_TBL"],
    "s_CPM_Load_FDR_CPM_NEWPAY_TBL": ["CPM_NEWPAY_TBL"],
    "s_CPM_Load_CPM_NEWPAY_TBL": ["CPM_NEWPAY_TBL"],
    "s_CPM_Build_Message_Counters": ["COUNTER_TBL"],
    "s_CPM_Send_Counts": [],
}


def _seed(ctx):
    _pay_periods(ctx)
    make_table(ctx, "PSEUDOSSN_TBL", [{"PP_END_YEAR": 2024, "PP_NUM": 5, "PSEUDOSSN": SSN, "SSN": "987654321"}])
    make_table(ctx, "HI_GENERIC_SRC_TBL", [{"ID": "1"}])
    for t in ("ERROR_TBL", "CPM_NEWPAY_TBL", "COUNTER_TBL"):
        make_table(ctx, t, [])
    write_vsam(ctx, "CPM_Biweekly_MER_filename", [
        vsam_line("MER_FILE", {"MER_RECTYP1": "1", "MER_REPORT_DT": "240301", "MER_FILE_ID": "MER01"}),
        vsam_line("MER_FILE", {"MER_RECTYP": "3", "MER_SSN": SSN, "MER_ORG": "ORG0001", "MER_ADDR_STATE": "NY",
                               "MER_ANN_LV_ACCRUAL_RT": "6", "MER_SICK_LV_ENDBAL": "120.50"}),
    ])
    write_vsam(ctx, "CPM_PAD_filename", [
        vsam_line("PAD_FILE", {"PAD_RECORD_TYPE": "H", "PAD_FILE_ID": "P6331X01", "PAD_PP_END_DTE": "240309",
                               "PAD_DISBURSING_DTE": "240315", "PAD_QTR_END_DTE": "240331"}),
        vsam_line("PAD_FILE", {"PAD_SERVING_AGENCY": "DE", "PAD_SOC_SEC_NO": SSN, "PAD_REGULAR_EARNINGS": "2500.00"}),
    ])
    write_vsam(ctx, "CPM_Year_To_Date_filename", [
        vsam_line("YTD_FILE", {"DFAS_YTD_RECORD_TYPE1": "0", "DFAS_YTD_FILE_NAME": "YTDFILE", "DFAS_YTD_PPEND_DATE": "20240309"}),
        vsam_line("YTD_FILE", {"DFAS_YTD_RECORD_TYPE2": "1", "DYD_SSN_1": SSN, "DYD_BASE_PAY": "12500.00"}),
        vsam_line("YTD_FILE", {"DFAS_YTD_RECORD_TYPE": "2", "DYD_SSN_2": SSN, "DYD_STATE_TAX_CODE": "NY",
                               "DYD_STATE_TAX_DEDUC": "100.25", "DYD_PAY_SUB_STATE_TAX": "2000.00"}),
        vsam_line("YTD_FILE", {"DFAS_YTD_RECORD_TYPE": "2", "DYD_SSN_2": SSN, "DYD_STATE_TAX_CODE": "CA",
                               "DYD_STATE_TAX_DEDUC": "50.00", "DYD_PAY_SUB_STATE_TAX": "1000.00"}),
    ])
    pm3 = lambda **kv: vsam_line("PAYMASTER_FILE", {"PYF_REC_NO_PDT3": "3", "PYF_EYE_ID_PDT3": SSN, **kv})
    write_vsam(ctx, "CPM_Department_of_Energy_filename", [
        vsam_line("PAYMASTER_FILE", {"PYF_REC_NO_BLANK": " ", "PYF_REC_PPE_DATE": "20240309"}),
        vsam_line("PAYMASTER_FILE", {"PYF_REC_NO_1": "1", "PYF_EYE_ID_1": SSN, "PYF_EYE_NME": "DOE JANE", "PYF_ORG": "ORG0001"}),
        vsam_line("PAYMASTER_FILE", {"PYF_REC_NO_2": "2", "PYF_EYE_ID_2": SSN, "PYF_AGY_CD": "DE"}),
        vsam_line("PAYMASTER_FILE", {"PYF_REC_NO_1": "1", "PYF_EYE_ID_1": SSN2, "PYF_EYE_NME": "ROE RICHARD"}),
        pm3(PYF_PAY_TAC_TYP="00100", PYF_PAY_DET_CD="RA", PYF_HRS_SCD_AMT="80.00", PYF_ACTUAL_AMT="2500.00", PYF_PAY_RAT="31.25"),
        pm3(PYF_PAY_TAC_TYP="00100", PYF_PAY_DET_CD="WL", PYF_ACTUAL_AMT="100.00", PFY_ID_BREAK_SSN="021000021", PYF_DDU_PYE="123456789"),
        pm3(PYF_PAY_TAC_TYP="00100", PYF_PAY_DET_CD="WD", PYF_ACTUAL_AMT="25.50"),
        pm3(PYF_PAY_TAC_TYP="00100", PYF_PAY_DET_CD="JO", PYF_ACTUAL_AMT="10.00"),
    ])


@pytest.fixture(scope="module")
def chain(spark, tmp_path_factory):
    """Run every wf_CPM task in workflow order on one small fixture; snapshot each session's targets."""
    from biis_lakehouse.context import RunContext
    from conftest import FILES

    ctx = RunContext(spark=spark, catalog=None, landing_root=str(tmp_path_factory.mktemp("cpm") / "landing"))
    ctx.params.update(FILES)
    _seed(ctx)
    snap, returned = {}, {}
    for name, task in wf_cpm.TASKS.items():
        out = task(ctx) or {}
        returned[name] = out
        ctx.params.update(out)
        snap[name] = {t: ctx.table(t).collect() for t in TARGETS[name]}
    return ctx, snap, returned


def _snap(chain, session, table):
    return chain[1][session][table]


def test_chain_current_pay_period(chain):
    ctx, _, returned = chain
    assert returned["s_CPM_Current_Pay_Period"] == {"WF_PP_END_YEAR": "2024", "WF_PP_NUM": "5", "WF_PP_YEAR_NUM": "202405"}
    assert [r.PAY_PERIOD_TXT for r in read_flat(ctx, "CPM_PAY_PERIOD_DATE_FILE", "cpm_pay_period.txt").collect()] == ["202405"]


def test_chain_mer_pad_ytd_pmr_staging(chain):
    s = "s_CPM_Load_CPM_MER_Staging_Tables"
    assert [r.FILE_ID for r in _snap(chain, s, "CPM_MER_HEADER_STG_TBL")] == ["MER01"]
    assert [r.MER_SSN for r in _snap(chain, s, "CPM_MER_DETAIL_STG_TBL")] == [SSN]
    s = "s_CPM_Load_CPM_PAD_Staging_Tables"
    assert len(_snap(chain, s, "CPM_PAD_HEADER_STG_TBL")) == 1
    assert [r.PAD_SOC_SEC_NO for r in _snap(chain, s, "CPM_PAD_DETAIL_STG_TBL")] == [SSN]
    s = "s_CPM_Load_CPM_YTD_Staging_Tables"
    assert len(_snap(chain, s, "CPM_YTD_HEADER_STG_TBL")) == 1
    assert [int(r.GEN_SEQ_ID) for r in sorted(_snap(chain, s, "CPM_YTD_STATE_STG_TBL"), key=lambda r: r.GEN_SEQ_ID)] == [1, 2]
    s = "s_CPM_Load_CPM_PMR_Staging_Tables"
    assert sorted(r.PYF_EYE_ID_1 for r in _snap(chain, s, "CPM_PM1_STG_TBL")) == [SSN, SSN2]
    assert len(_snap(chain, s, "CPM_PM2_STG_TBL")) == 1
    assert sorted(r.PYF_PAY_DET_CD for r in _snap(chain, s, "CPM_PM3_STG_TBL")) == ["JO", "RA", "WD", "WL"]


def test_chain_newpay_stg_ytd_state(chain):
    rows = _snap(chain, "s_CPM_Load_CPM_NEWPAY_STG_YTD_STATE_TBL", "CPM_NEWPAY_STG_YTD_STATE_TBL")
    assert len(rows) == 1
    r = rows[0]
    assert (r.DFAS_PSEUDO_SSN, r.YTD_STATE_1, int(r.YTD_ST_TX_1_CDE), r.YTD_STATE_2, int(r.YTD_ST_TX_2_CDE)) == (SSN, "NY", 36, "CA", 6)
    assert float(r.YTD_ST_TX_1_DED) == 100.25 and float(r.YTD_ST_TX_2_GRS) == 1000.0


def test_chain_newpay_stg_alt(chain):
    s = "s_CPM_Load_CPM_NEWPAY_STG_ALT_TBL"
    rows = _snap(chain, s, "CPM_NEWPAY_STG_ALT_TBL")
    assert [(r.DFAS_PSEUDO_SSN, float(r.CPP_ALT_1_DED), int(r.ALT_1_INST_AC_NO)) for r in rows] == [(SSN, 100.0, 123456789)]
    assert _snap(chain, s, "ERROR_TBL") == []


def test_chain_newpay_stg_detail(chain):
    rows = _snap(chain, "s_CPM_Load_CPM_NEWPAY_STG_DETAIL_TBL", "CPM_NEWPAY_STG_DETAIL_TBL")
    assert [(r.DFAS_PSEUDO_SSN, int(r.PP_NUM)) for r in rows] == [(SSN, 5)]


def test_chain_newpay_stg_type_1_2(chain):
    s = "s_CPM_Load_CPM_NEWPAY_STG_TYPE_1_2_TBL"
    rows = _snap(chain, s, "CPM_NEWPAY_STG_TYPE_1_2_TBL")
    assert len(rows) == 1  # SSN2 has no PM2 record -> dropped by the SQ inner join
    r = rows[0]
    assert (r.DFAS_PSEUDO_SSN, r.SURNAME_3, int(r.SOC_SEC_NO), r.YTD_STATE_1, float(r.SICK_LV_CUR_BAL)) == (
        SSN, "DOE", 98765432100, "NY", 120.5)
    assert _snap(chain, s, "ERROR_TBL") == []


def test_chain_newpay_stg_type_3_fdr(chain):
    rows = _snap(chain, "s_CPM_Load_CPM_NEWPAY_STG_TYPE_3_FDR_TBL", "CPM_NEWPAY_STG_TYPE_3_FDR_TBL")
    assert len(rows) == 1
    r = rows[0]
    assert (r.DFAS_PSEUDO_SSN, r.LINE_TYPE, float(r.CPP_BASE_PAY), float(r.REG_HRS_CPP), float(r.CPP_FED_WITH_DED)) == (
        SSN, "O0001", 2500.0, 80.0, 25.5)


def test_chain_newpay_stg_type_3(chain):
    rows = _snap(chain, "s_CPM_Load_From_FDR_CPM_NEWPAY_STG_TYPE_3_TBL", "CPM_NEWPAY_STG_TYPE_3_TBL")
    assert [(r.DFAS_PSEUDO_SSN, float(r.CPP_BASE_PAY), float(r.CPP_ALT_1_DED)) for r in rows] == [(SSN, 2500.0, 100.0)]


def test_chain_cpm_newpay_loads_append(chain):
    fdr = _snap(chain, "s_CPM_Load_FDR_CPM_NEWPAY_TBL", "CPM_NEWPAY_TBL")
    assert [(r.DFAS_PSEUDO_SSN, r.LINE_TYPE) for r in fdr] == [(SSN, "O0001")]
    both = _snap(chain, "s_CPM_Load_CPM_NEWPAY_TBL", "CPM_NEWPAY_TBL")
    assert sorted(r.LINE_TYPE for r in both) == ["O0001", "T0001"]


def test_chain_build_message_counters(chain):
    ctx, snap, returned = chain
    counters = {r.COUNTER_DESCRIPTION: int(r.COUNTER_VALUE) for r in snap["s_CPM_Build_Message_Counters"]["COUNTER_TBL"]}
    assert len(counters) == 18
    assert counters["DOE Records Read"] == 7
    assert counters["DOE Type 1 Read"] == 2
    assert counters["Masters Written"] == 1  # COUNT(*, MP_POOL_DES = ' '): only the T0001 line
    out = returned["s_CPM_Build_Message_Counters"]
    assert out["WF_SUBJECT"] == "CPM Process Completed Successfully for: 2024-05"
    assert out["WF_MESSAGE"].startswith("Paymaster Counts for Pay Period: 2024-05")
    msg = read_flat(ctx, "CPM_MESSAGE_FILE", "cpm_message_file.txt").collect()
    assert len(msg) == 20 and msg[0].COUNT_DESC == "Paymaster Counts for Pay Period: 2024-05"


def test_chain_send_counts(chain):
    ctx = chain[0]
    assert [r.ID for r in read_flat(ctx, "GENERIC_TARGET_FILE", "generic_target_file1.out").collect()] == ["1"]
