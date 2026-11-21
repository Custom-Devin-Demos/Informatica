"""wf_LES on a tiny two-employee LES transmission (19 records) and small PAY_PERIOD / PSEUDOSSN_TBL fixtures."""
import datetime as dt
import os

import pytest

from biis_lakehouse.context import OWNER_SCHEMAS, RunContext
from biis_lakehouse.pipelines.les import wf_les
from biis_lakehouse.pipelines.les.layouts import VSAM_LAYOUTS

OWNER = "INFO_TARGET_DEV"
REC_LEN = 115
SSN1, SSN2 = "111223333", "999887777"  # SSN2 is not on PSEUDOSSN_TBL
LES1, LES2 = "0000101", "0000102"


def rec(source: str, **values) -> str:
    """One fixed-length record laid out with the copybook of ``source`` (values are placed at their XML offsets)."""
    buf = [" "] * REC_LEN
    layout = {n: (o, ln, s) for n, o, ln, s, _ in VSAM_LAYOUTS[source]}
    for name, v in values.items():
        off, length, scale = layout[name]
        if scale is not None:
            v = str(int(round(float(v) * 10 ** scale))).zfill(length)
        v = str(v).ljust(length)[:length]
        buf[off:off + length] = list(v)
    return "".join(buf)


def emp(ssn: str, les: str) -> str:
    return rec("EMP_REC_TYPE_E", WS_EMP_REC_TYPE="*E", WS_SSN3=ssn[:3], WS_SSN2=ssn[3:5], WS_SSN4=ssn[5:],
               WS_EMP_LES_NUM=les, WS_EMP_LES_DELIVERY_IDC="Y")


def les_records(trailer_count: int = 2, header_date: str = "20261107") -> list[str]:
    return [
        rec("EMP_REC_TYPE_0", RECORD_TYPE="0", PPD_END_DATE=header_date, HEADER_LOC_ID="NIH",
            HEADER_LOC_NAME="NATIONAL INSTITUTES OF HLTH", SYSTEM_ID="NFC01", TRANSMISSION_CENTURY="20",
            TRANSMISSION_YEAR="26", TRANSMISSION_MONTH="11", TRANSMISSION_DAY="12"),
        emp(SSN1, LES1),
        rec("EMP_REC_TYPE_1", WS_LES_REC_TYPE1="*1", WS_LES_PPD_END_DT="110726", WS_LES_PAY_DT="111726",
            WS_LES_EYE_NME="DOE JANE", WS_LES_PAY_PLAN="GS", WS_LES_PAY_GRADE="12", WS_LES_PAY_STEP="03",
            WS_LES_HRLY_RATE=45.5, WS_LES_BASIC_PAY=3640, WS_LES_FLSA="E", WS_LES_SCD_LEAVE_DT="010105"),
        rec("EMP_REC_TYPE_2", WS_LES_REC_TYPE2="*2", WS_LES_NETPAY_FINORG="FIRST BANK", WS_LES_FED_TIT="FED",
            WS_LES_FED_MS="S", WS_LES_FED_ITW_XMP=1),
        rec("EMP_REC_TYPE_3", WS_LES_REC_TYPE3="*3", WS_LES_LC_TX1_NME="MONTGOMERY", WS_LES_CSRS_TIT="FERS"),
        rec("EMP_REC_TYPE_4", WS_LES_REC_TYPE4="*4", WS_LES_GRS_PAY_AMT=3640, WS_LES_GRS_PAY_YTD=80080),
        rec("EMP_REC_TYPE_5", WS_LES_REC_TYPE5="*5", WS_LES_NET_PAY_AMT=2500.25, WS_LES_ROTH_SV="ROTH", WS_LES_ROTH_SV_PCT=5),
        rec("EMP_REC_TYPE_6", WS_LES_REC_TYPE6="*6", WS_LES_VHVM_TITLE="EARNINGS FOR TSP", WS_LES_VHVM_AMT=3640),
        rec("EMP_REC_TYPE_C", WS_CURR_REC_TYPE="*C", WS_CURR_ERN_NAME1="REGULAR", WS_CURR_HRS_DAYS1=80,
            WS_CURR_AMT1=3640, WS_CURR_ERN_NAME2="OVERTIME", WS_CURR_HRS_DAYS2=2, WS_CURR_AMT2=136.5),
        rec("EMP_REC_TYPE_C", WS_CURR_REC_TYPE="*C", WS_CURR_ERN_NAME1="HOLIDAY", WS_CURR_HRS_DAYS1=8, WS_CURR_AMT1=364),
        rec("EMP_REC_TYPE_R", WS_RETR_REC_TYPE="*R", WS_RETR_ERN_NAME1="RETRO PAY", WS_RETR_HRS_DAYS1=4, WS_RETR_AMT1=50),
        rec("EMP_REC_TYPE_D", WS_DED_REC_TYPE="*D", WS_DED_NAME1="FEHB", WS_DED_CODE1="F01", WS_DED_AMT1=120,
            WS_DED_NAME2="FEGLI", WS_DED_CODE2="G01", WS_DED_AMT2=15),
        rec("EMP_REC_TYPE_L", WS_LV_REC_TYPE="*L", WS_LV_NAME="ANNUAL", WS_LV_ACCR_PP=6, WS_LV_CURR_BAL=120,
            WS_LV_TERM_DATE="01/09/27"),
        rec("EMP_REC_TYPE_U", WS_EMPLR_REC_TYPE="*U", WS_EMPLR_CONT_NAME1="FEHB GOVT", WS_EMPLR_CONT_AMT1=360),
        rec("EMP_REC_TYPE_M", WS_RMK_REC_TYPE="*M", WS_RMK_MSG="WELCOME TO THE NEW LES"),
        rec("EMP_REC_TYPE_T", WS_TA_RECORD_TYPE="*T", WS_TA_LV_NAME="SICK", WS_TA_LV_DATE_TAKEN="110326", WS_TA_LV_HOURS=8),
        emp(SSN2, LES2),
        rec("EMP_REC_TYPE_C", WS_CURR_REC_TYPE="*C", WS_CURR_ERN_NAME1="REGULAR", WS_CURR_HRS_DAYS1=80, WS_CURR_AMT1=2000),
        "2" + str(trailer_count).zfill(7) + " " * (REC_LEN - 8),
    ]


def make_ctx(spark, root) -> RunContext:
    for schema in set(OWNER_SCHEMAS.values()):
        for t in spark.catalog.listTables(schema):
            spark.sql(f"DROP TABLE IF EXISTS {schema}.{t.name}")
    ctx = RunContext(spark=spark, catalog=None, landing_root=str(root / "landing"))
    ts = dt.datetime
    spark.createDataFrame(
        [(2026, 22, ts(2026, 10, 11), ts(2026, 10, 24), "N"), (2026, 23, ts(2026, 10, 25), ts(2026, 11, 7), "Y")],
        "PP_END_YEAR int, PP_NUM int, PP_START_DTE timestamp, PP_END_DTE timestamp, CURR_PP_FLAG string",
    ).selectExpr("CAST(PP_END_YEAR AS DECIMAL(4,0)) PP_END_YEAR", "CAST(PP_NUM AS DECIMAL(2,0)) PP_NUM", "PP_START_DTE",
                 "PP_END_DTE", "CURR_PP_FLAG").write.format("delta").saveAsTable(ctx.table_name("PAY_PERIOD", OWNER))
    spark.createDataFrame(
        [(SSN1, "123456789", "E0000001", 4321, "NIH00"), ("555443333", "222334444", "E0000002", 1, "FDA00")],
        "PSEUDOSSN string, SSN string, EMPLID string, TK_NUM int, BUSINESS_UNIT string",
    ).write.format("delta").saveAsTable(ctx.table_name("PSEUDOSSN_TBL", OWNER))
    return ctx


def write_input(ctx: RunContext, records: list[str]) -> None:
    d = ctx.path("in", "LES")
    os.makedirs(d, exist_ok=True)
    with open(os.path.join(d, "LES.P202623.001"), "w") as f:
        f.write("\n".join(records) + "\n")


def run(ctx: RunContext, until: str | None = None) -> dict:
    results = {}
    for name, fn in wf_les.TASKS.items():
        out = fn(ctx) or {}
        results[name] = out
        ctx.params.update({k: str(v) for k, v in out.items() if v is not None})
        if name == until:
            break
    return results


@pytest.fixture(scope="module")
def les(spark, tmp_path_factory):
    ctx = make_ctx(spark, tmp_path_factory.mktemp("les"))
    write_input(ctx, les_records())
    results = run(ctx)
    return ctx, results


def tbl(ctx, name):
    return ctx.table(name, OWNER)


def read_out(ctx, filename):
    d = ctx.path("out", "LES", filename)
    lines = []
    for f in sorted(os.listdir(d)):
        if f.startswith("part-"):
            with open(os.path.join(d, f)) as fh:
                lines += fh.read().splitlines()
    return lines


def test_tasks_cover_every_workflow_task_in_link_order():
    names = list(wf_les.TASKS)
    assert len(names) == 33
    assert names[:5] == ["s_LES_Current_Pay_Period", "s_LES_Verify_Header", "s_LES_Verify_Record_Count",
                         "s_LES_Load_LES_HEADER_TBL", "s_LES_Load_LES_EMP_DETAIL_TBL"]
    assert names[-3:] == ["s_LES_NIH_FILE", "s_LES_Build_Message_Counters", "email_LES_Complete"]


def test_current_pay_period(les):
    ctx, results = les
    assert results["s_LES_Current_Pay_Period"] == {"WF_PP_END_YEAR": 2026, "WF_PP_NUM": 23, "WF_PP_YEAR_NUM": "202623"}
    assert read_out(ctx, "les_pay_period.txt") == ["202623"]


def test_verify_header_and_record_count(les):
    ctx, _ = les
    hdr = read_out(ctx, "les_header_file.txt")
    assert len(hdr) == 1 and hdr[0].startswith("2026231") and "20261107" in hdr[0]
    totals = read_out(ctx, "les_totals_file.txt")
    assert len(totals) == 1 and totals[0].split() == ["1", "2"]


def test_header_tbl(les):
    ctx, results = les
    rows = tbl(ctx, "LES_HEADER_TBL").collect()
    assert len(rows) == 1
    r = rows[0]
    assert (int(r.PP_END_YEAR), int(r.PP_NUM), r.HDR_LOC_ID, r.SYSTEM_ID) == (2026, 23, "NIH", "NFC01")
    assert r.PP_END_DTE == dt.datetime(2026, 11, 7) and r.TRANSMISSION_DTE == dt.datetime(2026, 11, 12)
    assert results["s_LES_Load_LES_HEADER_TBL"] == {"WF_PP_END_YEAR": 2026, "WF_PP_NUM": 23}


def test_emp_detail_and_errors(les):
    ctx, _ = les
    rows = sorted(tbl(ctx, "LES_EMP_DETAIL_TBL").collect(), key=lambda r: r.LES_NUMBER)
    assert [(r.PSEUDOSSN, int(r.LES_NUMBER), int(r.PP_NUM)) for r in rows] == [(SSN1, 101, 23), (SSN2, 102, 23)]
    errs = tbl(ctx, "ERROR_TBL").collect()
    assert len(errs) == 1
    assert errs[0].PROCESS_NAME == "m_LES_Load_LES_EMP_DETAIL_TBL" and SSN2 in errs[0].ERROR_MESSAGE


@pytest.mark.parametrize("code,expected", [("1", 1), ("2", 1), ("3", 1), ("4", 1), ("5", 1), ("6", 1),
                                           ("D", 1), ("U", 1), ("M", 1), ("T", 1)])
def test_rectype_tables(les, code, expected):
    ctx, _ = les
    rows = tbl(ctx, f"LES_EMP_DETAIL_RECTYPE_{code}_TBL").collect()
    assert len(rows) == expected
    assert (rows[0].PSEUDOSSN, int(rows[0].LES_NUMBER), int(rows[0].PP_END_YEAR)) == (SSN1, 101, 2026)


def test_rectype_values(les):
    ctx, _ = les
    r1 = tbl(ctx, "LES_EMP_DETAIL_RECTYPE_1_TBL").first()
    assert (r1.EMPLOYEE_NAME, float(r1.HRLY_RATE), r1.PAY_DTE) == ("DOE JANE", 45.5, dt.datetime(2026, 11, 17))
    assert r1.LV_SCD_DTE == dt.datetime(2005, 1, 1)
    assert float(tbl(ctx, "LES_EMP_DETAIL_RECTYPE_5_TBL").first().NET_PAY_AMT) == 2500.25
    d = tbl(ctx, "LES_EMP_DETAIL_RECTYPE_D_TBL").first()
    assert (d.DEDUCTION_1_NAME, int(d.DEDUCTION_ID), float(d.DEDUCTION_2_AMT)) == ("FEHB", 1, 15.0)
    t = tbl(ctx, "LES_EMP_DETAIL_RECTYPE_T_TBL").first()
    assert (t.LEAVE_NAME, t.LEAVE_TAKEN_DTE, int(t.TIME_ATTEND_ID)) == ("SICK", dt.datetime(2026, 11, 3), 1)


def test_rectype_c_numbers_lines_per_employee(les):
    ctx, _ = les
    rows = sorted(tbl(ctx, "LES_EMP_DETAIL_RECTYPE_C_TBL").collect(), key=lambda r: (r.LES_NUMBER, r.CURR_EARN_ID))
    assert [(int(r.LES_NUMBER), int(r.CURR_EARN_ID), r.CURR_EARN_NAME1) for r in rows] == [
        (101, 1, "REGULAR"), (101, 2, "HOLIDAY"), (102, 1, "REGULAR")]


def test_rectype_r_and_l(les):
    ctx, _ = les
    r = tbl(ctx, "LES_EMP_DETAIL_RECTYPE_R_TBL").collect()
    assert len(r) == 1 and (r[0].RETRO_ACTIVE_EARN_NAME1, int(r[0].RETRO_ACTIVE_ID)) == ("RETRO PAY", 1)
    lv = tbl(ctx, "LES_EMP_DETAIL_RECTYPE_L_TBL").collect()
    assert len(lv) == 1
    assert (lv[0].LEAVE_NAME, lv[0].LEAVE_USE_OR_LOSE_TERM_DTE, lv[0].LEAVE_USE_OR_LOSE_TERM_BAL_HRS) == (
        "ANNUAL", dt.datetime(2027, 1, 9), None)


def test_primary_data(les):
    ctx, _ = les
    rows = tbl(ctx, "LES_PRIMARY_DATA_TBL").collect()
    assert len(rows) == 1  # employee 2 has no type 1-6 records
    r = rows[0]
    assert (r.PSEUDOSSN, r.SSN, r.EMPLID, int(r.LES_NUMBER), r.HDR_LOC_ID, r.EMPLOYEE_NAME, r.ROTH_SAVINGS) == (
        SSN1, "123456789", "E0000001", 101, "NIH", "DOE JANE", "ROTH")


def test_curr_and_retro_earn(les):
    ctx, _ = les
    rows = sorted(tbl(ctx, "LES_EMP_DETAIL_CURR_EARN_TBL").collect(), key=lambda r: r.CURR_EARN_ID)
    assert [(int(r.LES_NUMBER), int(r.CURR_EARN_ID), r.CURR_EARN_NAME) for r in rows] == [
        (101, 1, "REGULAR"), (101, 2, "OVERTIME"), (101, 3, "HOLIDAY"), (102, 4, "REGULAR")]
    retro = tbl(ctx, "LES_EMP_DETAIL_RETRO_EARN_TBL").collect()
    assert [(r.RETRO_ACTIVE_EARN_NAME, float(r.RETRO_ACTIVE_EARN_AMT)) for r in retro] == [("RETRO PAY", 50.0)]


def test_leave(les):
    ctx, _ = les
    rows = tbl(ctx, "LES_EMP_DETAIL_LEAVE_TBL").collect()
    assert [(int(r.LEAVE_ID), r.LEAVE_NAME, float(r.LEAVE_CURR_BAL)) for r in rows] == [(1, "ANNUAL", 120.0)]


@pytest.mark.parametrize("table,count", [("LESC", 3), ("LESD", 1), ("LESL", 1), ("LESM", 1), ("LESR", 1),
                                         ("LESS", 1), ("LEST", 1), ("LESU", 1)])
def test_lesrpt_tables(les, table, count):
    ctx, _ = les
    rows = sorted(tbl(ctx, table).collect(), key=lambda r: r[f"{table}_KEY"])
    assert len(rows) == count
    assert [int(r[f"{table}_KEY"]) for r in rows] == list(range(1, count + 1))
    assert (int(rows[0].MASTER_ID), int(rows[0].PYR), int(rows[0].PPN)) == (101, 2026, 23)


def test_lesrpt_columns(les):
    ctx, _ = les
    lesc = sorted(tbl(ctx, "LESC").collect(), key=lambda r: r.LESC_KEY)
    assert [int(r.NEW_SEQ_NBR) for r in lesc] == [1825331, 1825332, 1825333]
    assert int(tbl(ctx, "LEST").first().NEW_SEQ_NBR) == 3686821
    lesl = tbl(ctx, "LESL").first()
    assert (lesl.SEQ, lesl.LV_TERM_DATE, lesl.LV_NAME) == ("1", "01/09/2027", "ANNUAL")
    less = tbl(ctx, "LESS").first()
    assert (less.SSN, less.TKN, less.EYE_NME, less.AEI_AMT) == (SSN1, 4321, "DOE JANE", None)


def test_lesrpt_keys_continue_from_existing_max(les):
    ctx, _ = les
    wf_les.s_LESRPT_Load_LESM(ctx)
    keys = sorted(int(r.LESM_KEY) for r in tbl(ctx, "LESM").collect())
    assert keys == [1, 2]


def test_nih_file(les):
    ctx, _ = les
    lines = read_out(ctx, "les_nih_emp_detail.txt")
    assert len(lines) == 15  # employee 1 (BUSINESS_UNIT NIH00) and its 14 detail records
    assert lines[0].startswith("*E123456789" + LES1)
    rows = tbl(ctx, "LES_NIH_EMPLOYEE_SUMMARY_TBL").collect()
    assert [(r.SSN, int(r.LES_NUMBER), int(r.PP_NUM)) for r in rows] == [("123456789", 101, 23)]


def test_build_message_counters_and_email(les):
    ctx, results = les
    rows = sorted(tbl(ctx, "COUNTER_TBL").collect(), key=lambda r: r.COUNTER_DESCRIPTION)
    assert {r.COUNTER_DESCRIPTION: int(r.COUNTER_VALUE) for r in rows} == {
        "Input Records": 2, "Records Loaded": 1, "NIH Records": 1, "Records not found on Pseudo": 1}
    out = results["s_LES_Build_Message_Counters"]
    assert out["WF_SUBJECT"] == "LES Process Completed Successfully for: 2026-23"
    assert "Number of Records Not on Pseudo                 = 1" in out["WF_MESSAGE"]
    assert results["email_LES_Complete"]["EMAIL_SUBJECT"] == out["WF_SUBJECT"]


# The ABORT tests rebuild the fixture tables, so they run after the module-scoped workflow run.
def test_verify_header_aborts_outside_current_pay_period(spark, tmp_path):
    ctx = make_ctx(spark, tmp_path)
    write_input(ctx, les_records(header_date="20261024"))
    with pytest.raises(RuntimeError, match="is not in the current pay period: 2026-23"):
        run(ctx, until="s_LES_Verify_Header")


def test_verify_record_count_aborts_on_mismatch(spark, tmp_path):
    ctx = make_ctx(spark, tmp_path)
    write_input(ctx, les_records(trailer_count=5))
    with pytest.raises(RuntimeError, match="Trailer Record: 5 does not match the calculated count: 2"):
        run(ctx, until="s_LES_Verify_Record_Count")
