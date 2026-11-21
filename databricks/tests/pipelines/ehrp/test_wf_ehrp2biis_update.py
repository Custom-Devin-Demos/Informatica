import datetime as dt
import re
from decimal import Decimal

import pytest
from pyspark.sql import types as T

from biis_lakehouse.pipelines.ehrp import _schema as S
from biis_lakehouse.pipelines.ehrp import wf_ehrp2biis_update as wf
from biis_lakehouse.runner import WORKFLOW_MODULES, load_tasks


def d(s):
    return dt.datetime.fromisoformat(s)


def _type(t):
    if t.startswith("string"):
        return T.StringType()
    if t == "timestamp":
        return T.TimestampType()
    p, s = map(int, re.findall(r"\d+", t))
    return T.DecimalType(p, s)


def _table(ctx, name, owner, cols, rows):
    schema = T.StructType([T.StructField(c, _type(t)) for c, t in cols])
    full = ctx.table_name(name, owner)
    ctx.spark.sql(f"DROP TABLE IF EXISTS {full}")
    ctx.spark.createDataFrame([tuple(r.get(c) for c, _ in cols) for r in rows], schema) \
        .write.format("delta").saveAsTable(full)


KEYS = [("EMPLID", "string"), ("EMPL_RCD", "decimal(38,0)"), ("EFFDT", "timestamp"), ("EFFSEQ", "decimal(38,0)")]


def k(emplid, effdt, effseq=0, rcd=0):
    return {"EMPLID": emplid, "EMPL_RCD": Decimal(rcd), "EFFDT": d(effdt), "EFFSEQ": Decimal(effseq)}


E1, E2, E3, E4 = k("10000001", "2024-01-05"), k("10000002", "2024-03-01"), k("10000003", "2025-02-01"), k("10000004", "2024-02-01")


@pytest.fixture
def sources(ctx):
    job_defaults = {"COMPANY": "HHS", "GVT_SUB_AGENCY": "NI", "STD_HOURS": Decimal("40.00"),
                    "GVT_WORK_SCHED": "F", "ACTION_DT": d("2024-01-02"), "ACTION": "AWD", "GVT_NOA_CODE": "100"}
    job = [
        {**job_defaults, **E1, "GVT_NOA_CODE": "840", "UNION_CD": "   ", "HE_AL_BALANCE": Decimal(0)},
        {**job_defaults, **E2, "GVT_WORK_SCHED": "I", "STD_HOURS": Decimal("20.00"), "GVT_NOA_CODE": "825",
         "ACTION": "BON"},
        {**job_defaults, **E3, "STD_HOURS": None, "GVT_XFER_FROM_AGCY": "HE"},
        {**job_defaults, **E4},  # no matching NWK_NEW_EHRP_ACTIONS_TBL row -> filtered by SQ join
    ]
    _table(ctx, "PS_GVT_JOB", "EHRP", S.PS_GVT_JOB_SELECT, job)
    _table(ctx, "NWK_NEW_EHRP_ACTIONS_TBL", "NKNIGHT", KEYS, [E1, E2, E3])
    _table(ctx, "SEQUENCE_NUM_TBL", "NKNIGHT", [("EHRP_YEAR", "decimal(4,0)"), ("EHRP_SEQ_NUMBER", "decimal(4,0)")],
           [{"EHRP_YEAR": Decimal(2024), "EHRP_SEQ_NUMBER": Decimal(100)},
            {"EHRP_YEAR": Decimal(2025), "EHRP_SEQ_NUMBER": Decimal(7)}])
    _table(ctx, "PS_GVT_EMPLOYMENT", "EHRP",
           KEYS + [("BUSINESS_TITLE", "string"), ("PROBATION_DT", "timestamp"), ("GVT_APPT_LIMIT_HRS", "decimal(38,0)")]
           + [(c, "timestamp") for c in ["CMPNY_SENIORITY_DT", "GVT_APPT_EXPIR_DT", "GVT_CNV_BEGIN_DATE",
                                         "GVT_DETAIL_EXPIRES", "GVT_RTND_GRADE_BEG", "GVT_RTND_GRADE_EXP",
                                         "GVT_SABBATIC_EXPIR", "GVT_SCD_LEO", "GVT_SCD_RETIRE", "GVT_SCD_TSP",
                                         "GVT_SUPV_PROB_DT", "GVT_TEMP_PRO_EXPIR", "GVT_TEMP_PSN_EXPIR",
                                         "GVT_WGI_DUE_DATE", "HIRE_DT", "LAST_INCREASE_DT", "SERVICE_DT"]]
           + [(c, "string") for c in ["GVT_COMP_LVL_PERM", "GVT_CURR_APT_AUTH1", "GVT_CURR_APT_AUTH2", "GVT_SPEP",
                                      "GVT_TENURE", "GVT_WGI_STATUS"]] + [("GVT_APPT_LIMIT_DYS", "decimal(38,0)")],
           [{**E1, "BUSINESS_TITLE": "analyst", "PROBATION_DT": d("2025-06-15")},
            {**E1, "BUSINESS_TITLE": "analyst", "PROBATION_DT": d("2025-06-15")},  # multiple match: Use Any Value
            {**E2, "BUSINESS_TITLE": "nurse", "GVT_APPT_LIMIT_HRS": Decimal(0)}])
    _table(ctx, "PS_GVT_PERS_NID", "EHRP", KEYS + [("NATIONAL_ID", "string")],
           [{**E1, "NATIONAL_ID": "111223333"}, {**E3, "NATIONAL_ID": "444556666"}])
    _table(ctx, "PS_GVT_AWD_DATA", "EHRP",
           KEYS + [("ERNCD", "string"), ("GOAL_AMT", "decimal(10,2)"), ("OTH_HRS", "decimal(6,2)"),
                   ("OTH_PAY", "decimal(10,2)"), ("GVT_TANG_BEN_AMT", "decimal(38,0)"),
                   ("EARNINGS_END_DT", "timestamp")],
           [{**E1, "ERNCD": "XYZ", "GOAL_AMT": Decimal("500.00")}, {**E2, "ERNCD": "BYO", "GOAL_AMT": Decimal("25000.00")}])
    _table(ctx, "PS_GVT_EE_DATA_TRK", "EHRP", KEYS + [("GVT_DATE_WRK", "timestamp"), ("GVT_WIP_STATUS", "string")],
           [{**E1, "GVT_DATE_WRK": d("2024-01-10"), "GVT_WIP_STATUS": "PRO"},
            {**E1, "GVT_DATE_WRK": d("2024-01-10"), "GVT_WIP_STATUS": "PRO"}])
    _table(ctx, "PS_HE_FILL_POS", "EHRP", KEYS + [("HE_FILL_POSITION", "string")], [{**E2, "HE_FILL_POSITION": "Y"}])
    _table(ctx, "PS_GVT_CITIZENSHIP", "EHRP", KEYS + [("CITIZENSHIP_STATUS", "string")],
           [{**E1, "CITIZENSHIP_STATUS": "2"}, {**E3, "CITIZENSHIP_STATUS": "4"}])
    pers_cols = ["LAST_NAME", "FIRST_NAME", "MIDDLE_NAME", "ADDRESS1", "CITY", "STATE", "POSTAL", "GEO_CODE",
                 "SEX", "MILITARY_STATUS", "GVT_CRED_MIL_SVCE", "GVT_MILITARY_COMP", "GVT_MIL_RESRVE_CAT",
                 "GVT_VET_PREF_APPT", "ETHNIC_GROUP", "GVT_DISABILITY_CD"]
    _table(ctx, "PS_GVT_PERS_DATA", "EHRP", KEYS + [(c, "string") for c in pers_cols] + [("BIRTHDATE", "timestamp")],
           [{**E1, "LAST_NAME": "DOE", "CITY": "ROCKVILLE", "STATE": "MD", "ADDRESS1": "1 MAIN ST"},
            {**E2, "LAST_NAME": "ROE", "STATE": "VA"},
            {**E4, "LAST_NAME": "NOT_IN_ACTIONS", "CITY": "X"}])
    _table(ctx, "PS_JPM_JP_ITEMS", "EHRP",
           [("JPM_PROFILE_ID", "string"), ("JPM_CAT_TYPE", "string"), ("EFF_STATUS", "string"),
            ("JPM_CAT_ITEM_ID", "string"), ("MAJOR_CODE", "string"), ("JPM_INTEGER_2", "decimal(38,0)")],
           [{"JPM_PROFILE_ID": "10000001", "JPM_CAT_TYPE": "DEG", "EFF_STATUS": "A", "JPM_CAT_ITEM_ID": "BS",
             "MAJOR_CODE": "M1", "JPM_INTEGER_2": Decimal(1999)},
            {"JPM_PROFILE_ID": "10000002", "JPM_CAT_TYPE": "DEG", "EFF_STATUS": "I", "JPM_CAT_ITEM_ID": "MS",
             "MAJOR_CODE": "M2", "JPM_INTEGER_2": Decimal(2001)}])
    for t in wf.TARGETS:
        ctx.spark.sql(f"DROP TABLE IF EXISTS {ctx.table_name(t, 'NKNIGHT')}")
    ctx.params["SYSDATE"] = "2024-04-02"
    return ctx


def test_registered_in_runner():
    assert WORKFLOW_MODULES["wf_EHRP2BIIS_UPDATE"] == wf.__name__
    assert list(load_tasks("wf_EHRP2BIIS_UPDATE")) == ["s_m_EHRP2BIIS_UPDATE"]


def test_s_m_ehrp2biis_update_writes_all_targets(sources):
    ctx = sources
    assert wf.TASKS["s_m_EHRP2BIIS_UPDATE"](ctx) == {}

    trk = {r.EMPLID: r for r in ctx.spark.table("nknight.ehrp_recs_tracking_tbl").collect()}
    sec = ctx.spark.table("nknight.nwk_action_secondary_tbl")
    assert set(trk) == {"10000001", "10000002", "10000003"}
    assert sec.count() == 3
    assert [c for c, _ in S.NWK_ACTION_PRIMARY_TBL] == ctx.spark.table("nknight.nwk_action_primary_tbl").columns

    # v_EVENT_ID: per EFFDT-ordered year run, EHRP_SEQ_NUMBER + 1, then +1 per row
    assert {e: int(r.BIIS_EVENT_ID) for e, r in trk.items()} == {"10000001": 101, "10000002": 102,
                                                                 "10000003": 8}
    assert sorted(int(r.EVENT_ID) for r in sec.collect()) == [8, 101, 102]
    assert trk["10000001"].EVENT_SUBMITTED_DT == d("2024-01-10")  # lkp_PS_GVT_EE_DATA_TRK
    assert trk["10000002"].EVENT_SUBMITTED_DT == d("2024-01-02")  # falls back to ACTION_DT
    assert trk["10000001"].NOA_CD == "840" and trk["10000001"].LOAD_DATE == d("2024-04-02")

    rows = {int(r.EVENT_ID): r for r in ctx.spark.table("nknight.nwk_action_primary_tbl").collect()}
    assert len(rows) == 3
    p1, p2, p3 = rows[101], rows[102], rows[8]
    assert (p1.EMP_ID, p2.EMP_ID, p3.EMP_ID) == ("10000001", "10000002", "10000003")
    assert p1.AGCY_ASSIGN_CD == "HHSN"  # 'HHS' || 'NI' truncated to varchar2(4)
    assert p1.SSN == "111223333" and p2.SSN is None
    assert p1.US_CITIZENSHIP_CD == "1" and p2.US_CITIZENSHIP_CD == "8" and p3.US_CITIZENSHIP_CD == "8"
    assert p1.CASH_AWARD_AMT == Decimal("500.00") and p2.CASH_AWARD_AMT is None
    assert p1.BASE_HRS == Decimal("80.00") and p2.BASE_HRS == Decimal("20.00") and p3.BASE_HRS is None
    assert (p1.PERMANENT_TEMP_POSITION_CD, p2.PERMANENT_TEMP_POSITION_CD, p3.PERMANENT_TEMP_POSITION_CD) == ("1", "3", "0")
    assert p1.POSITION_TITLE_NAME == "ANALYST" and p2.POSITION_TITLE_NAME == "NURSE"
    assert p1.PROB_TRIAL_PERIOD_START_DTE == d("2024-06-15")
    assert p1.EMP_RESID_CITY_ST_NAME == "ROCKVILLE, MD" and p2.EMP_RESID_CITY_ST_NAME == ", VA"
    assert p3.EMP_RESID_CITY_ST_NAME == ", "
    assert p1.LOAD_ID == "NK20240402" and p1.AS_OF_DAY == "04/02/2024"
    assert p1.FILLING_POSITION_CD is None and p2.FILLING_POSITION_CD == "Y"
    assert p1.INSTRUCTIONAL_PROGRAM_CD == "M1" and p1.EDUCATION_LEVEL_CD == "BS"
    assert p2.INSTRUCTIONAL_PROGRAM_CD is None  # inactive JPM item filtered by lookup override


def test_any_value_keeps_one_row_per_key(spark):
    df = spark.createDataFrame([("a", 1), ("a", 2), ("b", 3)], "k string, v int")
    assert sorted(r.k for r in wf.any_value(df, ["k"]).collect()) == ["a", "b"]
