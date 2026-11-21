"""wf_Pseudossn (folder Pseudossn): load the biweekly Pseudo-SSN file (and its SDA/backup copy) into PSEUDOSSN_TBL.

Workflow links (all on ``Status = Succeeded``):
Start -> s_Pseudossn_Verify_Header_Date_Current_Pay_Period -> s_Pseudossn_Verify_Record_Count
      -> s_Pseudossn_Verify_Header_Date_Current_Pay_Period_Pseudossn_From_SDA -> s_Pseudossn_Current_Pay_Period
      -> s_Pseudossn_Load_Archive_Pseudossn_Tbl -> s_Pseudossn_Load_Pseudossn_Tbl -> s_Pseudossn_Load_Pseudossn_From_SDA_Tbl
      -> s_Pseudossn_Update_Timekeeper_Number -> s_Pseudossn_Load_SDA_Records_Pseudossn_Tbl -> s_Pseudossn_Counters
      -> Email_Pseudossn

Sources: ``$Param_Root_Directory/data/int/in/PSEUDO/$Param_PSEUDOSSN_file`` (PSEUDOSSN_FILE, 500-byte fixed width) and
``.../$Param_PSEUDOSSN_Backup_file`` (PSEUDOSSN_FILE_TK_NUM, 505 bytes = PSEUDOSSN_FILE + TK_NUM) ->
``ctx.path("in", "PSEUDO", <file>)``. Flat-file targets go to ``ctx.path("out", "PSEUDO", <Output filename>)``.

Post-session success commands of s_Pseudossn_Counters (owned by the orchestration unit):
``mv .../in/PSEUDO/$Param_PSEUDOSSN_file .../archive/PSEUDO/PSEUDO_P$$WF_PP_YEAR_NUM.txt`` and
``mv .../in/PSEUDO/$Param_PSEUDOSSN_Backup_file .../archive/PSEUDO/bkup.pseudssn.P$$WF_PP_YEAR_NUM.txt``.
"""
from __future__ import annotations

from pyspark.sql import Column, DataFrame, Window, functions as F

from biis_lakehouse.common import delta
from biis_lakehouse.common.fixed_width import Field
from biis_lakehouse.context import RunContext
from biis_lakehouse.pipelines.reference import _pc

AREA = "PSEUDO"

# PSEUDOSSN_FILE source definition (FLATFILE DELIMITED=NO, LINESEQUENTIAL=YES); offsets are cumulative.
FILE_FIELDS = [Field(n, l) for n, l in [
    ("SSN", 9), ("CAN_CD", 8), ("PSEUDO_SSN", 9), ("EIN", 8), ("EMP_REC_NO", 2), ("LEGACY_APPT_NO", 2),
    ("FIRST_NAME", 11), ("MIDDLE_INIT", 1), ("LAST_NAME", 16), ("SEX", 1), ("VET_PREF", 1), ("TENURE_CD", 1),
    ("POSITION_CD", 1), ("CITIZENSHIP_STATUS", 1), ("APPT_TYPE_CD", 2), ("HIRE_DATE", 8), ("HANDICAP_CODE", 2),
    ("UNIF_ALLOW_CODE", 3), ("UNIF_ALLOW_DATE", 8), ("UNIF_ALLOW_AMT", 6), ("RSSSDP", 4), ("CEILING_CODE", 1),
    ("FUNCTION_CODE", 1), ("EMPLOYEE_STATUS", 1), ("MANAGER_STATUS", 1), ("POSITION_SENS_CODE", 1),
    ("CAREER_START_DATE", 8), ("CAREER_CONV_DATE", 8), ("PROBATION_DATE", 8), ("ABNORMAL_RATE_CODE", 1),
    ("APPT_LIMIT_HRS", 7), ("APPT_LIMIT_PAY", 8), ("LAST_PAY_CHANGE", 8), ("CHARITY_AREA_CODE", 3),
    ("CHARITY_EFF_DATE", 8), ("CHARITY_DED_AMT", 6), ("LAST_NOA", 4), ("FILLER_1", 4), ("QUARTERS_DEDUCTION", 8),
    ("SUBSIST_DEDUCTION", 8), ("PAY_BASIS", 2), ("WORK_SCHEDULE", 1), ("OCCUPATION_CODE", 4), ("FILLER_2", 3),
    ("GEO_LOCATION", 9), ("JOB_INDICATOR", 1), ("REG_TEMP", 1), ("SEPARATION_DATE", 8), ("FILLER_3", 1),
    ("PCA_CONTR_EFF_START_DATE", 8), ("PCA_CONTR_EFF_END_DATE", 8), ("MAX_ANNUAL_PAY", 8),
    ("PCA_BIWEEKLY_AMOUNT", 8), ("FILLER_4", 33), ("PAY_PLAN_CD", 2), ("APP_LIMIT_DATE", 8), ("SPO_CD", 4),
    ("TERM_ID", 2), ("EFFECTIVE_DATE", 8), ("EFFECTIVE_SEQ", 3), ("FILLER_5", 9), ("PAY_TABL_NO", 4),
    ("BUSINESS_UNIT", 5), ("DEPTID", 10), ("PCA_CONTR_YEAR", 2), ("FILLER_6", 150),
]]
FILE_TK_NUM_FIELDS = FILE_FIELDS + [Field("TK_NUM", 5)]

# PSEUDOSSN_TBL / PSEUDOSSN_FROM_SDA_TBL / HI_ARCH_PSEUDOSSN_TBL share this Oracle layout (PK: PSEUDOSSN on PSEUDOSSN_TBL).
_S, _D, _A = "string", "timestamp", "decimal(10,2)"
PSEUDOSSN_COLUMNS = [
    ("SSN", _S), ("CAN_CD", _S), ("PSEUDOSSN", _S), ("EMPLID", _S), ("EMPL_RCD", _S), ("APPT_NUM", _S),
    ("EMP_FIRST_NAME", _S), ("EMP_MID_INIT", _S), ("EMP_LAST_NAME", _S), ("SEX", _S), ("VETERANS_PREFERENCE_CD", _S),
    ("TENURE_CD", _S), ("POSITION_NUM", _S), ("US_CITIZENSHIP_CD", _S), ("APPT_TYPE_CD", _S), ("HIRE_DATE", _D),
    ("HANDICAP_CD", _S), ("UNF_ALLOW_CD", _S), ("UNIF_ALLOW_DATE", _D), ("UNIF_ALLOW_AMT", _A), ("RSSSDP", _S),
    ("CEIL_REPORTING_CD", _S), ("FUNCTNL_CLASSFCTN_CD", _S), ("EMP_STATUS", _S), ("MANAGER_STATUS", _S),
    ("POSITION_SENSITIVITY_CD", _S), ("CAREER_START_DATE", _D), ("CAREER_CONV_DATE", _D), ("PROBATION_DATE", _D),
    ("ABNORMAL_RATE_CD", _S), ("APPT_LIMIT_HRS", _A), ("APPT_LIMIT_PAY", _A), ("LAST_PAY_CHANGE", _D),
    ("CHARITY_AREA_CD", _S), ("CHARITY_EFF_DATE", _D), ("CHARITY_DED_AMT", _A), ("LAST_NOA_CD", _S),
    ("QUARTERS_DED_AMT", _A), ("SUBSIST_DED_AMT", _A), ("PAY_BASIS_CD", _S), ("WORK_SCHEDULE_CD", _S),
    ("OCCUPATION_CD", _S), ("DUTY_STATION", _S), ("JOB_IND", _S), ("REG_TEMP_CD", _S), ("SEPERATION_DATE", _D),
    ("PCA_CONTR_EFF_START_DATE", _D), ("PCA_CONTR_EFF_END_DATE", _D), ("MAX_ANNUAL_PAY", _A), ("PCA_BIWEEKLY_AMT", _A),
    ("PAY_PLAN_CD", _S), ("APPT_NTE_DTE", _D), ("SPECIAL_PROGRAM_CD", _S), ("TERM_ID", _S), ("EFFECTIVE_DATE", _D),
    ("EFFECTIVE_SEQ", "decimal(5,0)"), ("PAY_TABL_NO", _S), ("BUSINESS_UNIT", _S), ("DEPTID", _S),
    ("PCA_CONTR_LEN_YEAR", "decimal(4,0)"), ("TK_NUM", _S), ("PP_NUM", "decimal(2,0)"), ("PP_END_YEAR", "decimal(4,0)"),
]
PSEUDOSSN_DDL = ", ".join(f"{n} {t}" for n, t in PSEUDOSSN_COLUMNS)
ERROR_DDL = (
    "PROCESS_NAME string, ERROR_MESSAGE string, SOURCE_KEY string, ERROR_DATE timestamp, PP_END_YEAR decimal(4,0), "
    "PP_NUM decimal(2,0), CYCLE_ID decimal(3,0), ERROR_CODE string"
)

# exp_Conversions: target column <- source field, by conversion kind.
COPY = {
    "SSN": "SSN", "CAN_CD": "CAN_CD", "PSEUDOSSN": "PSEUDO_SSN", "EMPLID": "EIN", "EMPL_RCD": "EMP_REC_NO",
    "APPT_NUM": "LEGACY_APPT_NO", "EMP_FIRST_NAME": "FIRST_NAME", "EMP_MID_INIT": "MIDDLE_INIT",
    "EMP_LAST_NAME": "LAST_NAME", "SEX": "SEX", "VETERANS_PREFERENCE_CD": "VET_PREF", "TENURE_CD": "TENURE_CD",
    "POSITION_NUM": "POSITION_CD", "US_CITIZENSHIP_CD": "CITIZENSHIP_STATUS", "APPT_TYPE_CD": "APPT_TYPE_CD",
    "HANDICAP_CD": "HANDICAP_CODE", "UNF_ALLOW_CD": "UNIF_ALLOW_CODE", "RSSSDP": "RSSSDP",
    "CEIL_REPORTING_CD": "CEILING_CODE", "FUNCTNL_CLASSFCTN_CD": "FUNCTION_CODE", "EMP_STATUS": "EMPLOYEE_STATUS",
    "MANAGER_STATUS": "MANAGER_STATUS", "POSITION_SENSITIVITY_CD": "POSITION_SENS_CODE",
    "ABNORMAL_RATE_CD": "ABNORMAL_RATE_CODE", "CHARITY_AREA_CD": "CHARITY_AREA_CODE", "LAST_NOA_CD": "LAST_NOA",
    "PAY_BASIS_CD": "PAY_BASIS", "WORK_SCHEDULE_CD": "WORK_SCHEDULE", "OCCUPATION_CD": "OCCUPATION_CODE",
    "DUTY_STATION": "GEO_LOCATION", "JOB_IND": "JOB_INDICATOR", "REG_TEMP_CD": "REG_TEMP", "PAY_PLAN_CD": "PAY_PLAN_CD",
    "SPECIAL_PROGRAM_CD": "SPO_CD", "TERM_ID": "TERM_ID", "PAY_TABL_NO": "PAY_TABL_NO", "BUSINESS_UNIT": "BUSINESS_UNIT",
    "DEPTID": "DEPTID",
}
MMDDYYYY = {"HIRE_DATE": "HIRE_DATE", "CAREER_START_DATE": "CAREER_START_DATE", "CAREER_CONV_DATE": "CAREER_CONV_DATE",
            "PROBATION_DATE": "PROBATION_DATE", "LAST_PAY_CHANGE": "LAST_PAY_CHANGE"}
YYYYMMDD = {"UNIF_ALLOW_DATE": "UNIF_ALLOW_DATE", "CHARITY_EFF_DATE": "CHARITY_EFF_DATE",
            "SEPERATION_DATE": "SEPARATION_DATE", "PCA_CONTR_EFF_START_DATE": "PCA_CONTR_EFF_START_DATE",
            "PCA_CONTR_EFF_END_DATE": "PCA_CONTR_EFF_END_DATE", "APPT_NTE_DTE": "APP_LIMIT_DATE",
            "EFFECTIVE_DATE": "EFFECTIVE_DATE"}
# target <- (source, integer digits, sign position, port type)
AMOUNTS = {
    "UNIF_ALLOW_AMT": ("UNIF_ALLOW_AMT", 3, 6, "decimal(10,2)"),
    "APPT_LIMIT_HRS": ("APPT_LIMIT_HRS", 4, 7, "decimal(15,0)"),
    "APPT_LIMIT_PAY": ("APPT_LIMIT_PAY", 5, 8, "decimal(10,0)"),
    "CHARITY_DED_AMT": ("CHARITY_DED_AMT", 3, 6, "decimal(10,2)"),
    "QUARTERS_DED_AMT": ("QUARTERS_DEDUCTION", 5, 8, "decimal(10,2)"),
    "SUBSIST_DED_AMT": ("SUBSIST_DEDUCTION", 5, 8, "decimal(10,2)"),
    "MAX_ANNUAL_PAY": ("MAX_ANNUAL_PAY", 5, 8, "decimal(10,2)"),
    "PCA_BIWEEKLY_AMT": ("PCA_BIWEEKLY_AMOUNT", 5, 8, "decimal(10,2)"),
}
INTEGERS = {"EFFECTIVE_SEQ": "EFFECTIVE_SEQ", "PCA_CONTR_LEN_YEAR": "PCA_CONTR_YEAR"}


# --------------------------------------------------------------------------------------------- expression helpers
def _substr(c: str | Column, start: int, length: int) -> Column:
    c = F.col(c) if isinstance(c, str) else c
    return F.substring(c, start, length)


def _record_type(ssn: str = "SSN") -> Column:
    """``DECODE(TRUE, SUBSTR(SSN,1,6)='HEADER','H', SUBSTR(SSN,1,7)='TRAILER','T','D')``."""
    return (F.when(_substr(ssn, 1, 6) == "HEADER", "H").when(_substr(ssn, 1, 7) == "TRAILER", "T").otherwise("D"))


def _nonblank(c: str) -> Column:
    return F.coalesce(F.length(F.trim(F.col(c))) > 0, F.lit(False))


def _mmddyyyy(src: str, mm: tuple, dd: tuple, yyyy: tuple) -> Column:
    """``IIF(IS_DATE(SUBSTR(..)||'/'||SUBSTR(..)||'/'||SUBSTR(..), 'MM/DD/YYYY'), TO_DATE(...))``."""
    s = _pc.pc_concat(_substr(src, *mm), F.lit("/"), _substr(src, *dd), F.lit("/"), _substr(src, *yyyy))
    return F.to_timestamp(_pc.to_date(s, "MM/dd/yyyy"))


def _amount(src: str, digits: int, sign_pos: int, port: str, plus_src: str | None = None,
            plus_compares_value: bool = False) -> Column:
    """``v = SUBSTR(x,1,n)||'.'||SUBSTR(x,n+1,2)``; ``DECODE(TRUE, IS_NUMBER(v) AND sign='+', TO_DECIMAL(v,2),
    IS_NUMBER(v) AND sign='-', TO_DECIMAL(v,2)*-1, IS_NUMBER(v), TO_DECIMAL(v,2), 0)``."""
    def v(c):
        return _pc.pc_concat(_substr(c, 1, digits), F.lit("."), _substr(c, digits + 1, 2))

    val, sign = v(src), _substr(src, sign_pos, 1)
    pos_val = v(plus_src) if plus_src else val
    plus_test = val if plus_compares_value else sign
    num = _pc.is_number(val)
    dec = lambda c: F.trim(c).cast("decimal(12,2)")  # noqa: E731
    out = (F.when(num & (plus_test == "+"), dec(pos_val))
           .when(num & (sign == "-"), dec(val) * -1)
           .when(num, dec(pos_val))
           .otherwise(F.lit(0)))
    return out.cast(port).cast("decimal(10,2)")


def _to_integer(src: str, port: str) -> Column:
    """``IIF(IS_NUMBER(x), TO_INTEGER(x), 0)`` (TO_INTEGER truncates)."""
    c = F.col(src)
    return F.when(_pc.is_number(c), F.trim(c).cast("double").cast("int")).otherwise(F.lit(0)).cast(port)


def _conversions(ctx: RunContext, sda: bool) -> list[Column]:
    """exp_Conversions + exp_Final column list for PSEUDOSSN_TBL / PSEUDOSSN_FROM_SDA_TBL (PP_* added by caller)."""
    cols: dict[str, Column] = {}
    for t, s in COPY.items():
        cols[t] = F.col(s)
    for t, s in MMDDYYYY.items():
        cols[t] = _mmddyyyy(s, (1, 2), (3, 2), (5, 4))
    for t, s in YYYYMMDD.items():
        cols[t] = _mmddyyyy(s, (5, 2), (7, 2), (1, 4))
    for t, (s, n, sp, port) in AMOUNTS.items():
        cols[t] = _amount(
            s, n, sp, port,
            # o_SUBSIST_DED_AMT returns v_QUARTERS_DED_AMT on the '+' and unsigned branches (as exported).
            plus_src="QUARTERS_DEDUCTION" if t == "SUBSIST_DED_AMT" else None,
            # o_APPT_LIMIT_HRS tests ``v_APPT_LIMIT_HRS = '+'`` (value, not sign) on its first branch.
            plus_compares_value=t == "APPT_LIMIT_HRS",
        )
    for t, s in INTEGERS.items():
        cols[t] = _to_integer(s, "decimal(5,0)" if t == "EFFECTIVE_SEQ" else "decimal(4,0)")
    if sda:
        # m_Pseudossn_Load_Pseudossn_From_SDA_Tbl: DECODE(TRUE, IS_DATE(..), TO_DATE(..),
        #   EMP_FIRST_NAME = 'NOT FROM EH' AND EMP_MID_INIT = 'R', SESSSTARTTIME)
        cols["EFFECTIVE_DATE"] = F.coalesce(
            cols["EFFECTIVE_DATE"],
            F.when((F.col("FIRST_NAME") == "NOT FROM EH") & (F.col("MIDDLE_INIT") == "R"), _pc.session_start(ctx)),
        )
        cols["TK_NUM"] = F.col("TK_NUM")
    else:
        cols["TK_NUM"] = F.lit(None).cast("string")
    return [cols[n].cast(t).alias(n) for n, t in PSEUDOSSN_COLUMNS if n in cols]


def _read(ctx: RunContext, backup: bool = False) -> DataFrame:
    if backup:
        return _pc.read_flat(ctx, ctx.path("in", AREA, ctx.param("PSEUDOSSN_Backup_file")), FILE_TK_NUM_FIELDS)
    return _pc.read_flat(ctx, ctx.path("in", AREA, ctx.param("PSEUDOSSN_file")), FILE_FIELDS)


def _pay_period(ctx: RunContext) -> DataFrame:
    return ctx.table("PAY_PERIOD", "HISTDBA")


def _current_pp(ctx: RunContext) -> DataFrame:
    """lkp_Current_Pay_Period: ``CURR_PP_FLAG = in_CURR_PP_FLAG`` (in = 'Y'), Use Any Value -> 0 or 1 row."""
    return _pc.any_value(_pay_period(ctx).filter("CURR_PP_FLAG = 'Y'").select("PP_NUM", "PP_END_YEAR"), [])


def _with_current_pp(df: DataFrame, ctx: RunContext) -> DataFrame:
    return df.join(_current_pp(ctx), how="left") if _current_pp(ctx).count() else df.withColumn(
        "PP_NUM", F.lit(None).cast("decimal(2,0)")).withColumn("PP_END_YEAR", F.lit(None).cast("decimal(4,0)"))


def _table(ctx: RunContext, name: str, ddl: str = PSEUDOSSN_DDL) -> str:
    tbl = ctx.table_name(name)
    _pc.ensure_table(ctx, tbl, ddl)
    return tbl


# --------------------------------------------------------------------------------------------- sessions
def _verify_header(ctx: RunContext, src: DataFrame, date_string: Column, out_name: str) -> None:
    """Shared body of the two m_Pseudossn_Verify_Header_Date_Current_Pay_Period* mappings.

    exp_Initial/exp_Verify_Header_Date: header date string -> ``IS_DATE(.., 'YYYY-MM-DD')``;
    lkp_Current_Pay_Period (Use Any Value) gives ``v_CURRENT_PAY_PERIOD = YYYY-PP``;
    lkp_Pay_Period_Header_Date SQL override ``... FROM PAY_PERIOD WHERE CURR_PP_FLAG = 'Y'`` with condition
    ``PP_START_DTE <= in_HEADER_DATE AND PP_END_DTE >= in_HEADER_DATE`` (Use Any Value);
    o_BAD_DATE_FLAG ABORTs on an invalid header date or a header date outside the current pay period;
    fil_Header ``RECORD_TYPE_FLAG = 'H'``; exp_Final strips dashes -> PSEUDO_HDR_DATE_FILE (PSEUDO_DATE 10, no header).
    """
    df = src.select(_record_type().alias("RECORD_TYPE_FLAG"),
                    F.when(_record_type() == "H", date_string).alias("PSEUDO_DATE_STRING"))
    df = df.withColumn("HEADER_DATE", F.to_timestamp(_pc.to_date(F.col("PSEUDO_DATE_STRING"), "yyyy-MM-dd")))
    cur = _current_pp(ctx).first()
    current_pp = f"{int(cur.PP_END_YEAR)}-{int(cur.PP_NUM):02d}" if cur and cur.PP_NUM is not None else ""
    hdr_lkp = _pay_period(ctx).filter("CURR_PP_FLAG = 'Y'").select(
        F.col("PP_NUM").alias("lkp_PP_NUM"), "PP_START_DTE", "PP_END_DTE")
    hdr = df.filter("RECORD_TYPE_FLAG = 'H'").withColumn("_id", F.monotonically_increasing_id())
    hdr = hdr.join(hdr_lkp, (F.col("PP_START_DTE") <= F.col("HEADER_DATE")) & (F.col("PP_END_DTE") >= F.col("HEADER_DATE")), "left")
    hdr = _pc.any_value(hdr, ["_id"])
    for r in hdr.collect():
        if r.HEADER_DATE is None:
            raise RuntimeError(f"!!!! The value : {r.PSEUDO_DATE_STRING or ''} is not a valid date")
        if r.lkp_PP_NUM is None:
            raise RuntimeError(f"!!!! The date in the Header Record: {r.PSEUDO_DATE_STRING} is not in the current "
                               f"pay period: {current_pp}")
    s = F.col("PSEUDO_DATE_STRING")
    out = hdr.select(F.concat(_substr(s, 1, 4), _substr(s, 6, 2), _substr(s, 9, 2)).alias("PSEUDO_DATE"))
    _pc.write_flat(out, ctx.path("out", AREA, out_name), [Field("PSEUDO_DATE", 10)])


def s_Pseudossn_Verify_Header_Date_Current_Pay_Period(ctx: RunContext) -> None:
    """m_Pseudossn_Verify_Header_Date_Current_Pay_Period on PSEUDOSSN_FILE:
    header date = ``SUBSTR(CAN_CD,1,8) || SUBSTR(PSEUDO_SSN,1,2)``; writes pseudo_hdr_date.txt."""
    date_string = F.concat(_substr("CAN_CD", 1, 8), _substr("PSEUDO_SSN", 1, 2))
    _verify_header(ctx, _read(ctx), date_string, "pseudo_hdr_date.txt")


def s_Pseudossn_Verify_Record_Count(ctx: RunContext) -> None:
    """m_Pseudossn_Verify_Record_Count (PSEUDOSSN_FILE read twice).

    exp_DETERMINE_DETAIL ``IIF(v='H' OR v='D' AND LENGTH(LTRIM(RTRIM(SSN))) > 0, v, 'R')`` -> fil_DETAIL ``'D'`` ->
    agg_ALL_RECORDS ``COUNT(SSN)`` (no group-by); exp_DETERMINE_HEADER_TRAILER -> fil_TRAILER ``'T'`` with
    ``RECORD_COUNT = IIF(IS_NUMBER(SUBSTR(CAN_CD,1,6)), .., 'BAD_NUMBER')``; jnr_RECORD_CONSTANTS normal join on the
    constant '1'; exp_COMPARE ABORTs when ``TO_INTEGER(trailer count)`` <> detail count.
    Target PSEUDO_RECORD_COUNT is written by a File Writer (header = field names): pseudo_record_count.txt.
    """
    src = _read(ctx)
    t = _record_type()
    detail = src.filter((t == "H") | ((t == "D") & _nonblank("SSN"))).filter(t == "D")
    out_path = ctx.path("out", AREA, "pseudo_record_count.txt")
    fields = [Field("TRAILER_RECORD_COUNT", 10), Field("DETAIL_RECORD_COUNT", 10), Field("MESSAGE", 30)]
    empty = ctx.spark.createDataFrame([], "TRAILER_RECORD_COUNT string, DETAIL_RECORD_COUNT string, MESSAGE string")
    if not detail.limit(1).count():
        _pc.write_flat(empty, out_path, fields, header=True)
        return
    detail_count = detail.agg(F.count("SSN").alias("DETAIL_RECORD_COUNT"))
    rc = _substr("CAN_CD", 1, 6)
    trailer = src.filter(t == "T").select(
        F.when(_pc.is_number(rc), rc).otherwise(F.lit("BAD_NUMBER")).alias("TRAILER_RECORD_COUNT"))
    joined = trailer.crossJoin(detail_count)
    rows = joined.collect()
    for r in rows:
        trailer_n = int(float(r.TRAILER_RECORD_COUNT)) if r.TRAILER_RECORD_COUNT != "BAD_NUMBER" else 0
        if trailer_n != r.DETAIL_RECORD_COUNT:
            raise RuntimeError(f"!!!! The record count in the trailer: {r.TRAILER_RECORD_COUNT} does not match the "
                               f"detail record count of the file: {r.DETAIL_RECORD_COUNT}")
    out = joined.select("TRAILER_RECORD_COUNT", F.col("DETAIL_RECORD_COUNT").cast("string"),
                        F.lit("Record Counts Match").alias("MESSAGE"))
    _pc.write_flat(out, out_path, fields, header=True)


def s_Pseudossn_Verify_Header_Date_Current_Pay_Period_Pseudossn_From_SDA(ctx: RunContext) -> None:
    """m_Pseudossn_Verify_Header_Date_Current_Pay_Period_Pseudossn_From_SDA on PSEUDOSSN_FILE_TK_NUM (backup file):
    header date = ``SUBSTR(SSN,8,2) || SUBSTR(CAN_CD,1,8)``; writes pseudo_hdr_date_from_sda.txt."""
    date_string = F.concat(_substr("SSN", 8, 2), _substr("CAN_CD", 1, 8))
    _verify_header(ctx, _read(ctx, backup=True), date_string, "pseudo_hdr_date_from_sda.txt")


def s_Pseudossn_Current_Pay_Period(ctx: RunContext) -> dict:
    """m_Pseudossn_Current_Pay_Period: SQ filter ``PAY_PERIOD.CURR_PP_FLAG = 'Y'``; ``TO_CHAR(PP_END_YEAR) || v_PP_NUM``
    -> PAY_PERIOD_DATE_FILE (pay_period.txt, 6 bytes); SETVARIABLE $$MAP_PP_YEAR_NUM (MAX) -> $$WF_PP_YEAR_NUM."""
    pp = _pay_period(ctx).filter("CURR_PP_FLAG = 'Y'")
    out = pp.select(_pc.pc_concat(F.col("PP_END_YEAR").cast("int").cast("string"), _pc.pp2(F.col("PP_NUM")))
                    .alias("PAY_PERIOD"))
    _pc.write_flat(out, ctx.path("out", AREA, "pay_period.txt"), [Field("PAY_PERIOD", 6)])
    v = out.agg(F.max(F.substring("PAY_PERIOD", 1, 6))).first()[0]
    return {"WF_PP_YEAR_NUM": v} if v is not None else {}


def s_Pseudossn_Load_Archive_Pseudossn_Tbl(ctx: RunContext) -> None:
    """m_Pseudossn_Load_Archive_Pseudossn_Tbl_v1: PSEUDOSSN_TBL (all columns) -> HI_ARCH_PSEUDOSSN_TBL, normal insert."""
    src = ctx.spark.table(_table(ctx, "PSEUDOSSN_TBL"))
    tgt = _table(ctx, "HI_ARCH_PSEUDOSSN_TBL")
    delta.append(ctx, src.select(*[n for n, _ in PSEUDOSSN_COLUMNS]), tgt)


def _determine_errors(df: DataFrame) -> DataFrame:
    """exp_Determine_Errors after srt_PSEUDOSSN_EFF_DT (PSEUDO_SSN asc, EMPLOYEE_STATUS asc, EFFECTIVE_DATE_CONV desc).

    ``v_ERROR_FLAG_1`` compares PSEUDO_SSN with ``v_PREV_KEY`` (the previous row's value: the variable is assigned
    after the comparison), i.e. every row after the first in a PSEUDO_SSN group is a duplicate.
    """
    order = [F.col("PSEUDO_SSN").asc_nulls_first(), F.col("EMPLOYEE_STATUS").asc_nulls_first(),
             F.col("EFFECTIVE_DATE_CONV").desc_nulls_last(), F.col("_seq").asc()]
    w = Window.orderBy(*order)
    df = df.withColumn("_prev", F.lag("PSEUDO_SSN").over(w))
    ed = F.col("EFFECTIVE_DATE")
    blank = lambda c: F.col(c).isNull() | (F.length(F.ltrim(F.col(c))) == 0)  # noqa: E731
    flags = [
        F.coalesce(F.col("PSEUDO_SSN") == F.col("_prev"), F.lit(False)),
        blank("SSN"), blank("CAN_CD"), blank("PSEUDO_SSN"), blank("EFFECTIVE_DATE"),
        ~_pc.is_date(_pc.pc_concat(_substr(ed, 5, 2), F.lit("/"), _substr(ed, 7, 2), F.lit("/"), _substr(ed, 1, 4)),
                     "MM/dd/yyyy"),
    ]
    msgs = [
        _pc.pc_concat(F.lit("A duplicate has been detected for Social Security: "), F.col("SSN"),
                      F.lit(" and Can Code: "), F.col("CAN_CD"), F.lit(".")),
        F.lit("SSN is not found."), F.lit("CAN_CD is not found."), F.lit("PSEUDOSSN is not found."),
        F.lit("EFFECTIVE_DATE is not found."),
        _pc.pc_concat(F.lit("EFFECTIVE_DATE is not a valid date. Value: "), ed),
    ]
    for i, (f, m) in enumerate(zip(flags, msgs), 1):
        df = df.withColumn(f"ERROR_MESSAGE_{i}", F.when(f, m.substr(1, 200)))
    any_flag = flags[0]
    for f in flags[1:]:
        any_flag = any_flag | f
    return df.withColumn("ERROR_FLAG", any_flag).withColumn(
        "SRC_KEY",
        _pc.pc_concat(F.col("SSN"), F.lit(" || "), F.col("CAN_CD"), F.lit(" || "), ed, F.lit(" || ")).substr(1, 50),
    )


def _detail_records(src: DataFrame, sda: bool) -> DataFrame:
    """exp_Determine_Record_Type + fil_Detail_Records."""
    t = _record_type()
    flag = F.when((t == "D") & _nonblank("SSN"), F.lit("D")).otherwise(F.lit("R"))
    cond = flag == "D"
    if sda:
        cond = cond & _pc.is_number(F.col("SSN"))
    return src.filter(cond)


def s_Pseudossn_Load_Pseudossn_Tbl(ctx: RunContext) -> None:
    """m_Pseudossn_Load_Pseudossn_Tbl.

    SQ_PSEUDOSSN_FILE -> exp_Determine_Record_Type -> fil_Detail_Records ``o_RECORD_TYPE_FLAG = 'D'`` ->
    srt_PSEUDOSSN_EFF_DT -> exp_Determine_Errors -> rtr_Good_Bad_Records:
      GOOD_RECORDS (``ERROR_FLAG = FALSE``) -> exp_Conversions -> lkp_Current_Pay_Period1 -> PSEUDOSSN_TBL
        (session: Truncate target table = YES, insert);
      BAD_RECORDS (``ERROR_FLAG = TRUE``) -> nrm_Errors (6 ERROR_MESSAGE occurrences) -> fil_Error_Message
        ``NOT ISNULL(ERROR_MESSAGE) AND LENGTH(LTRIM(ERROR_MESSAGE)) > 0`` -> exp_Final_Errors
        (ERROR_DATE = SESSSTARTTIME, PP from lkp_Current_Pay_Period) -> ERROR_TBL (insert).
      DEFAULT1 is unconnected.
    """
    detail = _detail_records(_read(ctx), sda=False).withColumn("_seq", F.monotonically_increasing_id())
    detail = detail.withColumn("EFFECTIVE_DATE_CONV", _mmddyyyy("EFFECTIVE_DATE", (5, 2), (7, 2), (1, 4)))
    df = _determine_errors(detail)
    good = _with_current_pp(df.filter(~F.col("ERROR_FLAG")), ctx)
    out = good.select(*_conversions(ctx, sda=False), "PP_NUM", "PP_END_YEAR")
    delta.overwrite(ctx, out.select(*[F.col(n).cast(t).alias(n) for n, t in PSEUDOSSN_COLUMNS]),
                    _table(ctx, "PSEUDOSSN_TBL"))

    bad = df.filter(F.col("ERROR_FLAG"))
    errs = bad.select(
        "SRC_KEY",
        F.explode(F.array(*[F.col(f"ERROR_MESSAGE_{i}") for i in range(1, 7)])).alias("ERROR_MESSAGE"),
    ).filter(F.col("ERROR_MESSAGE").isNotNull() & (F.length(F.ltrim("ERROR_MESSAGE")) > 0))
    errs = _with_current_pp(errs, ctx).select(
        F.lit("m_Pseudossn_Load_Pseudossn_Tbl").alias("PROCESS_NAME"),
        "ERROR_MESSAGE",
        F.col("SRC_KEY").alias("SOURCE_KEY"),
        _pc.session_start(ctx).alias("ERROR_DATE"),
        F.col("PP_END_YEAR").cast("decimal(4,0)").alias("PP_END_YEAR"),
        F.col("PP_NUM").cast("decimal(2,0)").alias("PP_NUM"),
        F.lit(None).cast("decimal(3,0)").alias("CYCLE_ID"),
        F.lit(None).cast("string").alias("ERROR_CODE"),
    )
    delta.append(ctx, errs, _table(ctx, "ERROR_TBL", ERROR_DDL))


def s_Pseudossn_Load_Pseudossn_From_SDA_Tbl(ctx: RunContext) -> None:
    """m_Pseudossn_Load_Pseudossn_From_SDA_Tbl on PSEUDOSSN_FILE_TK_NUM (backup file).

    fil_Detail_Records ``RECORD_TYPE_FLAG = 'D' AND SSN_FLAG = TRUE`` (``IS_NUMBER(SSN)``); sorter/error ports do not
    reach a target; exp_Conversions (EFFECTIVE_DATE falls back to SESSSTARTTIME for 'NOT FROM EH'/'R' rows),
    TK_NUM passed through, lkp_Current_Pay_Period -> PSEUDOSSN_FROM_SDA_TBL (Truncate target table = YES).
    """
    detail = _with_current_pp(_detail_records(_read(ctx, backup=True), sda=True), ctx)
    out = detail.select(*_conversions(ctx, sda=True), "PP_NUM", "PP_END_YEAR")
    delta.overwrite(ctx, out.select(*[F.col(n).cast(t).alias(n) for n, t in PSEUDOSSN_COLUMNS]),
                    _table(ctx, "PSEUDOSSN_FROM_SDA_TBL"))


def s_Pseudossn_Update_Timekeeper_Number(ctx: RunContext) -> None:
    """m_Pseudossn_Update_Timekeeper_Number: SQ filter ``TK_NUM IS NULL`` on PSEUDOSSN_TBL;
    lkp_PSEUDOSSN_FROM_SDA_TBL ``PSEUDOSSN = in_PSEUDOSSN`` (Use Any Value);
    ``UPDATE_FLAG = NOT ISNULL(lkp_TK_NUM) AND NOT IS_SPACES(lkp_TK_NUM)``; fil_Updates; upd_Update_TK_NUM
    ``DD_UPDATE`` -> PSEUDOSSN_TBL (PSEUDOSSN, TK_NUM) keyed on PK PSEUDOSSN."""
    tbl = _table(ctx, "PSEUDOSSN_TBL")
    src = ctx.spark.table(tbl).filter("TK_NUM IS NULL").select("PSEUDOSSN")
    lkp = _pc.any_value(ctx.spark.table(_table(ctx, "PSEUDOSSN_FROM_SDA_TBL")).select(
        F.col("PSEUDOSSN").alias("in_PSEUDOSSN"), F.col("TK_NUM").alias("lkp_TK_NUM")), ["in_PSEUDOSSN"])
    df = src.join(lkp, F.col("PSEUDOSSN") == F.col("in_PSEUDOSSN"), "left")
    df = df.filter(F.col("lkp_TK_NUM").isNotNull() & (F.length(F.trim("lkp_TK_NUM")) > 0))
    _pc.update_rows(ctx, df.select("PSEUDOSSN", F.col("lkp_TK_NUM").alias("TK_NUM")), tbl, ["PSEUDOSSN"])


def s_Pseudossn_Load_SDA_Records_Pseudossn_Tbl(ctx: RunContext) -> None:
    """m_Pseudossn_Load_SDA_Records_Pseudossn_Tbl: PSEUDOSSN_FROM_SDA_TBL -> lkp_PSEUDOSSN_TBL ``PSEUDOSSN =
    in_PSEUDOSSN`` (static cache, Use Any Value) -> ``INSERT_FLAG = ISNULL(lkp_PSEUDOSSN)`` -> fil_Inserts ->
    PSEUDOSSN_TBL (insert, all columns)."""
    tbl = _table(ctx, "PSEUDOSSN_TBL")
    src = ctx.spark.table(_table(ctx, "PSEUDOSSN_FROM_SDA_TBL"))
    existing = ctx.spark.table(tbl).select(F.col("PSEUDOSSN").alias("lkp_PSEUDOSSN")).filter("lkp_PSEUDOSSN IS NOT NULL")
    new = src.join(existing, src["PSEUDOSSN"] == existing["lkp_PSEUDOSSN"], "left_anti")
    delta.append(ctx, new.select(*[n for n, _ in PSEUDOSSN_COLUMNS]), tbl)


def s_Pseudossn_Counters(ctx: RunContext) -> dict:
    """m_Pseudossn_Counters.

    Detail count: exp_Determine_Detail/fil_Detail ``'D'`` -> agg_ALL_RECORDS ``COUNT(SSN)`` (no group-by);
    lkp_Count_PSEUDOSSN_TBL ``SELECT COUNT(*) FROM PSEUDOSSN_TBL`` (condition COUNT_CURRENT >= 1);
    lkp_Count_PSEUDOSSN_Duplicates_ERROR_TABLE: ERROR_TBL rows of process m_Pseudossn_Load_Pseudossn_Tbl at the
    latest ERROR_DATE >= TRUNC(SYSDATE) that has a 'duplicate' message (condition COUNT_CURRENT >= 1);
    lkp_PSEUDOSSN_TBL_Pay_Period ``SELECT MIN(PP_NUM), MIN(PP_END_YEAR) FROM PSEUDOSSN_TBL`` (PP_END_YEAR >= 1).
    nrm_Counters -> 3 COUNTER_TBL rows; exp_Build_Message -> PSEUDOSSN_MESSAGE_FILE (header = field names) and
    SETVARIABLE $$MAP_SUBJECT/$$MAP_MESSAGE -> $$WF_SUBJECT/$$WF_MESSAGE. SYSDATE uses the session start time.
    """
    src = _read(ctx)
    t = _record_type()
    detail = src.filter((t == "H") | ((t == "D") & _nonblank("SSN"))).filter(t == "D")
    counter_tbl = _table(ctx, "COUNTER_TBL", _pc.COUNTER_DDL)
    msg_path = ctx.path("out", AREA, "pseudossn_message_file.txt")
    msg_fields = [Field("SUBJECT", 100), Field("MESSAGE", 300)]
    if not detail.limit(1).count():
        _pc.write_flat(ctx.spark.createDataFrame([], "SUBJECT string, MESSAGE string"), msg_path, msg_fields, header=True)
        return {}
    c1 = detail.agg(F.count("SSN")).first()[0]
    pseudossn = ctx.spark.table(_table(ctx, "PSEUDOSSN_TBL"))
    c2 = _pc.nullable_count(pseudossn)
    err = ctx.spark.table(_table(ctx, "ERROR_TBL", ERROR_DDL)).filter(
        "PROCESS_NAME = 'm_Pseudossn_Load_Pseudossn_Tbl'")
    start = _pc.session_start(ctx)
    latest = err.filter((F.col("ERROR_DATE") >= F.to_date(start).cast("timestamp"))
                        & (F.substring("ERROR_MESSAGE", 3, 9) == "duplicate")).agg(F.max("ERROR_DATE")).first()[0]
    c3 = _pc.nullable_count(err.filter(F.col("ERROR_DATE") == F.lit(latest))) if latest is not None else None
    pp = pseudossn.agg(F.min("PP_NUM").alias("PP_NUM"), F.min("PP_END_YEAR").alias("PP_END_YEAR")).first()
    pp_num, pp_year = (pp.PP_NUM, pp.PP_END_YEAR) if pp.PP_END_YEAR is not None and pp.PP_END_YEAR >= 1 else (None, None)

    rows = [
        ("Number of detail records from the PSEUDOSSN file.", c1),
        ("Number of detail records loaded to the PSEUDOSSN table.", c2),
        ("Number of duplicate records found.", c3),
    ]
    counters = ctx.spark.createDataFrame(rows, "COUNTER_DESCRIPTION string, COUNTER_VALUE long").select(
        start.alias("RUN_DATE"),
        F.lit("m_Pseudossn_Counters").alias("PROCESS_NAME"),
        "COUNTER_DESCRIPTION",
        F.col("COUNTER_VALUE").cast("decimal(15,0)").alias("COUNTER_VALUE"),
        F.lit(None).cast("decimal(4,0)").alias("PP_END_YEAR"),
        F.lit(None).cast("decimal(2,0)").alias("PP_NUM"),
        F.lit(None).cast("decimal(1,0)").alias("CYCLE_ID"),
    )
    delta.append(ctx, counters, counter_tbl)

    base = ctx.spark.createDataFrame([(pp_num, pp_year, c1, c2, c3)],
                                     "PP_NUM decimal(2,0), PP_END_YEAR decimal(4,0), C1 long, C2 long, C3 long")
    msg = base.select(
        _pc.pc_concat(_pc.environment(ctx), F.lit("Pseudo SSN loaded successfully for Pay Period: "),
                      F.col("PP_END_YEAR").cast("int").cast("string"), F.lit("-"), _pc.pp2(F.col("PP_NUM")))
        .substr(1, 100).alias("SUBJECT"),
        _pc.pc_concat(F.lit("Number of Detail Records from PseudoSSN file = "), F.col("C1").cast("string"), F.lit("\n"),
                      F.lit("Number of Records loaded = "), F.col("C2").cast("string"), F.lit("\n"),
                      F.lit("Number of Duplicate Records = "), F.col("C3").cast("string"))
        .substr(1, 300).alias("MESSAGE"),
    )
    _pc.write_flat(msg, msg_path, msg_fields, header=True)
    r = msg.first()
    return {k: v for k, v in {"WF_SUBJECT": r.SUBJECT, "WF_MESSAGE": r.MESSAGE}.items() if v is not None}


def Email_Pseudossn(ctx: RunContext) -> dict:
    """Email task: To ``$$WF_PSEUDOSSN_EMAIL_LIST``, Subject ``$$WF_SUBJECT``, Text ``$$WF_MESSAGE``."""
    return {
        "EMAIL_TO": ctx.params.get("WF_PSEUDOSSN_EMAIL_LIST", ""),
        "EMAIL_SUBJECT": ctx.params.get("WF_SUBJECT", "0"),
        "EMAIL_TEXT": ctx.params.get("WF_MESSAGE", "0"),
    }


TASKS = {
    "s_Pseudossn_Verify_Header_Date_Current_Pay_Period": s_Pseudossn_Verify_Header_Date_Current_Pay_Period,
    "s_Pseudossn_Verify_Record_Count": s_Pseudossn_Verify_Record_Count,
    "s_Pseudossn_Verify_Header_Date_Current_Pay_Period_Pseudossn_From_SDA":
        s_Pseudossn_Verify_Header_Date_Current_Pay_Period_Pseudossn_From_SDA,
    "s_Pseudossn_Current_Pay_Period": s_Pseudossn_Current_Pay_Period,
    "s_Pseudossn_Load_Archive_Pseudossn_Tbl": s_Pseudossn_Load_Archive_Pseudossn_Tbl,
    "s_Pseudossn_Load_Pseudossn_Tbl": s_Pseudossn_Load_Pseudossn_Tbl,
    "s_Pseudossn_Load_Pseudossn_From_SDA_Tbl": s_Pseudossn_Load_Pseudossn_From_SDA_Tbl,
    "s_Pseudossn_Update_Timekeeper_Number": s_Pseudossn_Update_Timekeeper_Number,
    "s_Pseudossn_Load_SDA_Records_Pseudossn_Tbl": s_Pseudossn_Load_SDA_Records_Pseudossn_Tbl,
    "s_Pseudossn_Counters": s_Pseudossn_Counters,
    "Email_Pseudossn": Email_Pseudossn,
}
