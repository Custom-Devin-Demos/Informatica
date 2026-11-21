"""wf_LES (XML/LES): load the NFC Leave and Earnings Statement (LES) extract into BIIS.

Every input file in ``$$WF_LES_INPUT_DIR`` (PowerCenter file-list command ``cd in/LES; ls -1|sort``)
is one LES transmission; each record is parsed with the copybook of the record type the mapping
reads (``EMP_REC_TYPE_0`` header, ``_1``..``_6``/``C``/``D``/``E``/``L``/``M``/``R``/``T``/``U`` detail).
Sessions run in workflow-link order (``TASKS`` preserves it); the LESRPT sessions have
``FAIL_PARENT_IF_INSTANCE_FAILS=NO`` in PowerCenter, which the orchestration unit reproduces.

Workflow variables: ``WF_PP_END_YEAR``, ``WF_PP_NUM``, ``WF_PP_YEAR_NUM`` (set by
s_LES_Current_Pay_Period / s_LES_Load_LES_HEADER_TBL), ``WF_SUBJECT``/``WF_MESSAGE`` (set by
s_LES_Build_Message_Counters, consumed by email_LES_Complete) and ``WF_LES_EMAIL_LIST``.
"""
from __future__ import annotations

from pyspark.sql import DataFrame, Window, functions as F

from biis_lakehouse.common import delta
from biis_lakehouse.context import RunContext
from biis_lakehouse.pipelines.les._io import (
    ORDER_COLS,
    carry_les_number,
    first_match,
    is_number,
    is_date,
    read_flat,
    pp_num_str,
    raise_abort,
    read_vsam,
    rr_date,
    write_flat,
)

OWNER = "INFO_TARGET_DEV"


def _t(ctx: RunContext, name: str) -> str:
    return ctx.table_name(name, OWNER)


def _src(ctx: RunContext, name: str) -> DataFrame:
    return ctx.table(name, OWNER)


def _var(ctx: RunContext, name: str):
    v = ctx.param(name, "")
    return None if v in (None, "") else v


def _setvariable_max(start, values) -> object:
    """SETVARIABLE on a mapping variable with aggregation MAX: max of the start value and every row value."""
    vals = [v for v in values if v is not None]
    if start not in (None, ""):
        vals.append(type(vals[0])(start) if vals and not isinstance(start, type(vals[0])) else start)
    return max(vals) if vals else start


def _mmddyyyy(yyyymmdd: str) -> F.Column:
    """``SUBSTR(d,5,2) || '/' || SUBSTR(d,7,2) || '/' || SUBSTR(d,1,4)``."""
    c = F.col(yyyymmdd)
    return F.concat_ws("/", F.substring(c, 5, 2), F.substring(c, 7, 2), F.substring(c, 1, 4))


def _to_date(c: F.Column, fmt: str = "MM/dd/yyyy") -> F.Column:
    """``IIF(IS_DATE(v, fmt), TO_DATE(v, fmt))``."""
    return F.when(is_date(c, fmt), F.to_timestamp(c, fmt))


def _pay_period_for(ctx: RunContext, df: DataFrame, date_col: str, out: dict[str, str]) -> DataFrame:
    """Unconnected-style range lookup on PAY_PERIOD: ``PP_START_DTE <= d AND PP_END_DTE >= d`` (Use Any Value)."""
    pp = _src(ctx, "PAY_PERIOD").select(F.col("PP_START_DTE").alias("__lkp_START"), F.col("PP_END_DTE").alias("__lkp_END"),
                                        *[F.col(k).alias(f"__lkp_{k}") for k in out])
    df = df.withColumn("__row", F.monotonically_increasing_id())
    j = df.join(pp, (F.col("__lkp_START") <= df[date_col]) & (F.col("__lkp_END") >= df[date_col]), "left")
    j = first_match(j, ["__row"], [F.col("__lkp_" + next(iter(out))).asc_nulls_last()])
    for k, alias in out.items():
        j = j.withColumnRenamed(f"__lkp_{k}", alias)
    return j.drop("__lkp_START", "__lkp_END", "__row")


def _current_pay_period(ctx: RunContext) -> dict | None:
    """lkp_Current_Pay_Period: PAY_PERIOD where ``CURR_PP_FLAG = 'Y'`` (Use Any Value)."""
    rows = _src(ctx, "PAY_PERIOD").where("CURR_PP_FLAG = 'Y'").limit(1).collect()
    return rows[0].asDict() if rows else None


def _headers(ctx: RunContext) -> DataFrame:
    """EMP_REC_TYPE_0 -> Norm_EMP_REC_TYPE_0 -> fil_LES_HEADER (``RECORD_TYPE = '0'``)."""
    return read_vsam(ctx, "EMP_REC_TYPE_0").where("RECORD_TYPE = '0'")


def _abort_invalid_pp_end(df: DataFrame) -> None:
    """exp_Convert.v_PP_END_DTE_FLAG: ``ABORT('The Pay Period End Date of '|| v || 'is not valid.')``."""
    bad = df.where(~is_date(F.col("v_PP_END_DTE"), "MM/dd/yyyy")).select("v_PP_END_DTE").limit(1).collect()
    raise_abort(bad, lambda r: f"The Pay Period End Date of {r[0]}is not valid.")


# --------------------------------------------------------------------------------------------
# s_LES_Current_Pay_Period / m_LES_Current_Pay_Period
# --------------------------------------------------------------------------------------------
def s_LES_Current_Pay_Period(ctx: RunContext) -> dict:
    """SQ_PAY_PERIOD (``CURR_PP_FLAG = 'Y'``) -> exp_Build_Pay_Period -> LES_PAY_PERIOD_DATE_FILE (les_pay_period.txt).

    Sets ``$$MAP_PP_END_YEAR``/``$$MAP_PP_NUM``/``$$MAP_PP_YEAR_NUM`` (aggregation MAX) and copies them to
    ``$$WF_PP_END_YEAR``/``$$WF_PP_NUM``/``$$WF_PP_YEAR_NUM`` on success.
    """
    sq = _src(ctx, "PAY_PERIOD").where("CURR_PP_FLAG = 'Y'")
    out = sq.select(
        F.concat(F.col("PP_END_YEAR").cast("int").cast("string"), pp_num_str(F.col("PP_NUM"))).alias("PAY_PERIOD_TXT"),
        F.col("PP_END_YEAR").cast("int").alias("PP_END_YEAR"),
        F.col("PP_NUM").cast("int").alias("PP_NUM"),
    ).cache()
    write_flat(ctx, out, "LES_PAY_PERIOD_DATE_FILE", "les_pay_period.txt")
    rows = out.collect()
    return {
        "WF_PP_END_YEAR": _setvariable_max(_var(ctx, "WF_PP_END_YEAR") and int(_var(ctx, "WF_PP_END_YEAR")), [r.PP_END_YEAR for r in rows]),
        "WF_PP_NUM": _setvariable_max(_var(ctx, "WF_PP_NUM") and int(_var(ctx, "WF_PP_NUM")), [r.PP_NUM for r in rows]),
        "WF_PP_YEAR_NUM": _setvariable_max(_var(ctx, "WF_PP_YEAR_NUM"), [r.PAY_PERIOD_TXT for r in rows]),
    }


# --------------------------------------------------------------------------------------------
# s_LES_Verify_Header / m_LES_Verify_Header
# --------------------------------------------------------------------------------------------
def s_LES_Verify_Header(ctx: RunContext) -> None:
    """Validate every file's header record against the current pay period; write LES_HEADER_FILE (les_header_file.txt).

    exp_Verify_Header_Date ABORTs when the header date is not a date or falls outside the current
    pay period; v_FILE_COUNTER numbers the header records (one per input file).
    """
    hdr = _headers(ctx).withColumn("v_PP_END_DTE", _mmddyyyy("PPD_END_DATE"))
    _abort_invalid_pp_end(hdr)
    hdr = hdr.withColumn("o_PP_END_DTE", _to_date(F.col("v_PP_END_DTE")))
    hdr = _pay_period_for(ctx, hdr, "o_PP_END_DTE", {"PP_NUM": "lkp_PP_NUM", "PP_END_YEAR": "lkp_PP_END_YEAR"})
    hdr = hdr.withColumn("FILE_COUNTER", F.row_number().over(Window.orderBy(*ORDER_COLS)))
    cur = _current_pay_period(ctx) or {}
    cur_num, cur_year = cur.get("PP_NUM"), cur.get("PP_END_YEAR")
    cur_pp_num = None if cur_num is None else f"{int(cur_num):02d}"
    rows = hdr.orderBy("FILE_COUNTER").collect()
    for r in rows:
        if not (r.lkp_PP_END_YEAR is not None and cur_year is not None and r.lkp_PP_END_YEAR == cur_year
                and r.lkp_PP_NUM == cur_num):
            raise RuntimeError(
                f"!!!! The date in the Header Record: {r.PPD_END_DATE} is not in the current pay period: "
                f"{'' if cur_year is None else int(cur_year)}-{cur_pp_num or ''} for file {r.FILE_COUNTER}"
            )
    out = hdr.select(
        F.lit(None if cur_year is None else f"{int(cur_year)}{cur_pp_num}").cast("string").alias("PP_YEAR_NUM"),
        F.col("FILE_COUNTER").cast("string").alias("INPUT_FILE_NUMBER"),
        F.col("PPD_END_DATE").alias("INPUT_PP_END_DATE"),
    )
    write_flat(ctx, out, "LES_HEADER_FILE", "les_header_file.txt")


# --------------------------------------------------------------------------------------------
# s_LES_Verify_Record_Count / m_LES_Verify_Record_Count
# --------------------------------------------------------------------------------------------
def s_LES_Verify_Record_Count(ctx: RunContext) -> None:
    """Reconcile each file's trailer count with its ``*E`` employee records; write LES_TOTALS_FILE (les_totals_file.txt).

    fil_LES_Records keeps header ('0'), employee ('*E') and trailer ('2') records. The trailer count is
    ``SUBSTR(type,2,1) || SSN3 || SSN2 || SUBSTR(SSN4,1,1)``; a mismatch ABORTs.
    """
    e = read_vsam(ctx, "EMP_REC_TYPE_E")
    t1 = F.substring("WS_EMP_REC_TYPE", 1, 1)
    e = e.where((t1 == "0") | (F.col("WS_EMP_REC_TYPE") == "*E") | (t1 == "2"))
    cnt_s = F.concat(F.substring("WS_EMP_REC_TYPE", 2, 1), "WS_SSN3", "WS_SSN2", F.substring("WS_SSN4", 1, 1))
    e = e.withColumn("LES_COUNT", F.when((t1 == "2") & cnt_s.rlike(r"^\s*[+-]?\d+\s*$"), cnt_s.cast("int")))
    w = Window.orderBy(*ORDER_COLS).rowsBetween(Window.unboundedPreceding, Window.currentRow)
    e = e.withColumn("FILE_COUNTER", F.sum(F.when(t1 == "0", 1).otherwise(0)).over(w))
    wf = Window.partitionBy("FILE_COUNTER").orderBy(*ORDER_COLS).rowsBetween(Window.unboundedPreceding, Window.currentRow)
    e = e.withColumn("LES_RECORD_COUNTER", F.sum(F.when(F.col("WS_EMP_REC_TYPE") == "*E", 1).otherwise(0)).over(wf))
    trailers = e.where(t1 == "2")
    bad = trailers.where(F.col("LES_COUNT").isNull() | (F.col("LES_COUNT") != F.col("LES_RECORD_COUNTER")))
    raise_abort(
        bad.orderBy(*ORDER_COLS).limit(1).collect(),
        lambda r: (f"!!!! The count in the Trailer Record: {'' if r.LES_COUNT is None else r.LES_COUNT} does not match "
                   f"the calculated count: {r.LES_RECORD_COUNTER} for file {r.FILE_COUNTER}"),
    )
    out = trailers.orderBy(*ORDER_COLS).select(
        F.col("FILE_COUNTER").cast("string").alias("INPUT_FILE_NUMBER"),
        F.col("LES_COUNT").cast("string").alias("INPUT_FILE_TOTAL"),
    )
    write_flat(ctx, out, "LES_TOTALS_FILE", "les_totals_file.txt")


# --------------------------------------------------------------------------------------------
# s_LES_Load_LES_HEADER_TBL / m_LES_Load_LES_HEADER_TBL
# --------------------------------------------------------------------------------------------
def s_LES_Load_LES_HEADER_TBL(ctx: RunContext) -> dict:
    """First header record (fil_First_Header_Record ``HEADER_COUNTER = 1``) -> PAY_PERIOD lookup -> LES_HEADER_TBL (insert).

    ``$$MAP_PP_END_YEAR``/``$$MAP_PP_NUM`` (MAX) are set from the looked-up pay period and copied to the WF variables.
    """
    hdr = _headers(ctx).withColumn("v_PP_END_DTE", _mmddyyyy("PPD_END_DATE"))
    _abort_invalid_pp_end(hdr)
    first = hdr.withColumn("HEADER_COUNTER", F.row_number().over(Window.orderBy(*ORDER_COLS))).where("HEADER_COUNTER = 1")
    tx = F.concat_ws("/", "TRANSMISSION_MONTH", "TRANSMISSION_DAY", F.concat("TRANSMISSION_CENTURY", "TRANSMISSION_YEAR"))
    first = first.select(
        _to_date(F.col("v_PP_END_DTE")).alias("PP_END_DTE"),
        F.col("HEADER_LOC_ID").alias("HDR_LOC_ID"),
        F.col("HEADER_LOC_NAME").alias("HDR_LOC_NAME"),
        "SYSTEM_ID",
        _to_date(tx).alias("TRANSMISSION_DTE"),
    )
    out = _pay_period_for(ctx, first, "PP_END_DTE", {"PP_END_YEAR": "PP_END_YEAR", "PP_NUM": "PP_NUM"}).select(
        F.col("PP_END_YEAR").cast("decimal(4,0)").alias("PP_END_YEAR"),
        F.col("PP_NUM").cast("decimal(2,0)").alias("PP_NUM"),
        "PP_END_DTE", "HDR_LOC_ID", "HDR_LOC_NAME", "SYSTEM_ID", "TRANSMISSION_DTE",
    ).cache()
    delta.append(ctx, out, _t(ctx, "LES_HEADER_TBL"))
    rows = out.collect()
    start_year, start_num = _var(ctx, "WF_PP_END_YEAR"), _var(ctx, "WF_PP_NUM")
    return {
        "WF_PP_END_YEAR": _setvariable_max(start_year and int(start_year), [r.PP_END_YEAR and int(r.PP_END_YEAR) for r in rows]),
        "WF_PP_NUM": _setvariable_max(start_num and int(start_num), [r.PP_NUM and int(r.PP_NUM) for r in rows]),
    }


def _map_pp(ctx: RunContext) -> tuple:
    """Pre-session assignment ``$$MAP_PP_END_YEAR = $$WF_PP_END_YEAR``, ``$$MAP_PP_NUM = $$WF_PP_NUM``."""
    y, n = _var(ctx, "WF_PP_END_YEAR"), _var(ctx, "WF_PP_NUM")
    return (None if y is None else int(y)), (None if n is None else int(n))


def _pp_filter(ctx: RunContext, df: DataFrame, year_col: str = "PP_END_YEAR", num_col: str = "PP_NUM") -> DataFrame:
    """Source Filter ``<tbl>.PP_END_YEAR = $$MAP_PP_END_YEAR AND <tbl>.PP_NUM = $$MAP_PP_NUM``."""
    y, n = _map_pp(ctx)
    if y is None or n is None:  # SQL ``col = NULL`` selects nothing
        return df.limit(0)
    return df.where((F.col(year_col) == y) & (F.col(num_col) == n))


def _emp_detail(ctx: RunContext) -> DataFrame:
    """SQ_LES_EMP_DETAIL_TBL restricted to the current pay period."""
    return _pp_filter(ctx, _src(ctx, "LES_EMP_DETAIL_TBL"))


def _write_errors(ctx: RunContext, bad: DataFrame, mapping: str) -> None:
    """rtr_GOOD_BAD_RECORDS.BAD_RECORDS -> exp_FINAL_ERRORS -> ERROR_TBL (insert, ``ERROR_DATE = SESSSTARTTIME``)."""
    delta.append(ctx, bad.select(
        F.lit(mapping).cast("string").alias("PROCESS_NAME"),
        F.col("ERROR_MESSAGE").cast("string"),
        F.col("SRC_KEY").cast("string").alias("SOURCE_KEY"),
        F.current_timestamp().alias("ERROR_DATE"),
        F.col("PP_END_YEAR").cast("decimal(4,0)"),
        F.col("PP_NUM").cast("decimal(2,0)"),
        F.lit(None).cast("decimal(1,0)").alias("CYCLE_ID"),
    ), _t(ctx, "ERROR_TBL"))


# --------------------------------------------------------------------------------------------
# s_LES_Load_LES_EMP_DETAIL_TBL / m_LES_Load_LES_EMP_DETAIL_TBL
# --------------------------------------------------------------------------------------------
def s_LES_Load_LES_EMP_DETAIL_TBL(ctx: RunContext) -> None:
    """``*E`` employee records x current LES_HEADER_TBL row -> LES_EMP_DETAIL_TBL (truncate + insert), errors -> ERROR_TBL.

    jnr_HEADER_EMP_DETAILS joins on constants (``1 = 1``); lkp_PSEUDSSN_TBL (``PSEUDOSSN = SSN``, Use Any Value)
    only flags unknown SSNs: rtr_GOOD_BAD_RECORDS.GOOD_RECORDS is ``TRUE`` so every record is loaded and
    BAD_RECORDS (``ERROR_FLAG = TRUE``) additionally goes to ERROR_TBL.
    """
    hdr = _pp_filter(ctx, _src(ctx, "LES_HEADER_TBL")).select("PP_END_YEAR", "PP_NUM")
    emp = read_vsam(ctx, "EMP_REC_TYPE_E").where("WS_EMP_REC_TYPE = '*E'").select(
        F.concat("WS_SSN3", "WS_SSN2", "WS_SSN4").alias("SSN"), "WS_EMP_LES_NUM", "WS_EMP_LES_DELIVERY_IDC"
    )
    j = emp.crossJoin(hdr)
    lkp = first_match(_src(ctx, "PSEUDOSSN_TBL").select("PSEUDOSSN"), ["PSEUDOSSN"])
    j = j.join(lkp.withColumnRenamed("PSEUDOSSN", "lkp_PSEUDOSSN"), F.col("SSN") == F.col("lkp_PSEUDOSSN"), "left")
    j = j.withColumn("ERROR_FLAG", F.col("lkp_PSEUDOSSN").isNull()).withColumn(
        "ERROR_MESSAGE",
        F.when(F.col("ERROR_FLAG"), F.concat(F.lit("The PSEUDOSSN: "), F.col("SSN"), F.lit(" is not found for this LES Number: "), F.col("WS_EMP_LES_NUM"))),
    ).withColumn(
        "SRC_KEY",
        F.concat(F.col("PP_END_YEAR").cast("int").cast("string"), F.lit(" || "), F.lpad(F.col("PP_NUM").cast("int").cast("string"), 2, "0"),
                 F.lit(" || "), F.col("WS_EMP_LES_NUM"), F.lit(" || "), F.col("SSN"), F.lit(" || ")),
    ).cache()
    good = j.select(
        F.col("PP_END_YEAR").cast("decimal(10,0)"), F.col("PP_NUM").cast("decimal(10,0)"),
        F.col("SSN").alias("PSEUDOSSN"), F.trim("WS_EMP_LES_NUM").cast("long").cast("decimal(15,0)").alias("LES_NUMBER"),
        F.col("WS_EMP_LES_DELIVERY_IDC").alias("LES_DELIVERY_IND"),
    )
    delta.overwrite(ctx, good, _t(ctx, "LES_EMP_DETAIL_TBL"))
    _write_errors(ctx, j.where("ERROR_FLAG"), "m_LES_Load_LES_EMP_DETAIL_TBL")


# --------------------------------------------------------------------------------------------
# Record-type detail loads (m_LES_Load_LES_EMP_DETAIL_RECTYPE_<x>_TBL)
# --------------------------------------------------------------------------------------------
def _load_rectype(ctx: RunContext, source: str, type_col: str, code: str, target: str, cols: dict,
                  id_col: str | None = None) -> None:
    """EMP_REC_TYPE_<x> -> Norm -> fil (``'*<x>'`` or ``'*E'``) -> exp_Convert (v_LES_NUMBER carried from ``*E``)
    -> fil (``'*<x>'``) -> jnr_LES_NUMBER (normal join with SQ_LES_EMP_DETAIL_TBL on LES_NUMBER) -> target (insert).

    ``cols`` maps target column -> VSAM field name, ``"D.<col>"`` (LES_EMP_DETAIL_TBL) or a Column.
    ``id_col``: ``v_<ID> = IIF(type = '*E', 0, v_<ID> + 1)`` -- line number of the record within its employee.
    """
    rec = read_vsam(ctx, source).where(F.col(type_col).isin(f"*{code}", "*E"))
    rec = carry_les_number(rec, type_col)
    if id_col:
        upto = Window.orderBy(*ORDER_COLS).rowsBetween(Window.unboundedPreceding, Window.currentRow)
        rec = rec.withColumn("__emp", F.sum(F.when(F.col(type_col) == "*E", 1).otherwise(0)).over(upto))
        per_emp = Window.partitionBy("__emp").orderBy(*ORDER_COLS).rowsBetween(Window.unboundedPreceding, Window.currentRow)
        rec = rec.withColumn(id_col, F.sum(F.when(F.col(type_col) == "*E", 0).otherwise(1)).over(per_emp).cast("decimal(15,0)"))
    rec = rec.where(F.col(type_col) == f"*{code}")
    det = _emp_detail(ctx).select(*[F.col(c).alias(f"D_{c}") for c in ("PP_END_YEAR", "PP_NUM", "PSEUDOSSN", "LES_NUMBER")])
    j = rec.join(det, F.col("D_LES_NUMBER") == F.col("LES_NUMBER"), "inner")
    sel = [F.col("LES_NUMBER").cast("decimal(15,0)").alias("LES_NUMBER")]
    for tcol, src in cols.items():
        if isinstance(src, str):
            src = F.col("D_" + src[2:]) if src.startswith("D.") else F.col(src)
        sel.append(src.alias(tcol))
    delta.append(ctx, j.select(*sel), _t(ctx, target))


_D = {"PSEUDOSSN": "D.PSEUDOSSN", "PP_END_YEAR": "D.PP_END_YEAR", "PP_NUM": "D.PP_NUM"}


def s_LES_Load_LES_EMP_DETAIL_RECTYPE_1_TBL(ctx: RunContext) -> None:
    """Type-1 (pay/grade/rate) records -> LES_EMP_DETAIL_RECTYPE_1_TBL; dates converted with ``TO_DATE(MM/DD/RR)``."""
    _load_rectype(ctx, "EMP_REC_TYPE_1", "WS_LES_REC_TYPE1", "1", "LES_EMP_DETAIL_RECTYPE_1_TBL", {
        **_D,
        "PP_END_DTE": rr_date(F.col("WS_LES_PPD_END_DT")),
        "PAY_DTE": rr_date(F.col("WS_LES_PAY_DT")),
        "EMPLOYEE_NAME": "WS_LES_EYE_NME", "PAY_PLAN_CD": "WS_LES_PAY_PLAN", "GRADE": "WS_LES_PAY_GRADE",
        "STEP": "WS_LES_PAY_STEP", "HRLY_RATE": "WS_LES_HRLY_RATE", "BASE_OT_RATE": "WS_LES_BASIC_OT_RATE",
        "BASE_PAY": "WS_LES_BASIC_PAY", "LOCALITY_ADJ": "WS_LES_LOCAL_ADJ", "ADJ_BASE_PAY": "WS_LES_ADJ_BASIC_PAY",
        "LOCALITY_PERCENTAGE": "WS_LES_LOCAL_PCT", "FLSA_STATUS": "WS_LES_FLSA",
        "LV_SCD_DTE": rr_date(F.col("WS_LES_SCD_LEAVE_DT")), "MIL_LV_CARRY": "WS_LES_MAX_LV_CRY",
        "LV_YR_END_DTE": rr_date(F.col("WS_LES_LV_YR_END_DT")), "LAST_INCREASE_DTE": rr_date(F.col("WS_LES_DATE_LST_INC")),
    })


def s_LES_Load_LES_EMP_DETAIL_RECTYPE_2_TBL(ctx: RunContext) -> None:
    """Type-2 (net-pay/allotment financial orgs, federal/state tax) -> LES_EMP_DETAIL_RECTYPE_2_TBL."""
    _load_rectype(ctx, "EMP_REC_TYPE_2", "WS_LES_REC_TYPE2", "2", "LES_EMP_DETAIL_RECTYPE_2_TBL", {
        **_D,
        "NET_PAY_FINANCIAL_ORG": "WS_LES_NETPAY_FINORG", "ALLOT1_FINANCIAL_ORG": "WS_LES_ALLT1_FINORG",
        "ALLOT2_FINANCIAL_ORG": "WS_LES_ALLT2_FINORG", "FEDERAL_TAX": "WS_LES_FED_TIT", "FED_MARITAL_STATUS": "WS_LES_FED_MS",
        "FED_ADDNL_WITHHOLDING": "WS_LES_FED_ADDTL", "FED_EXCEPTIONS": "WS_LES_FED_ITW_XMP", "STATE1_TAX": "WS_LES_ST_TX1_TIT",
        "STATE1_MARITAL_STATUS": "WS_LES_ST_TX1_MS", "STATE1_ADDNL_AMOUNT": "WS_LES_ST_TX1_ADDTL",
        "STATE1_EXCEPTIONS": "WS_LES_ST_TX1_XMP", "STATE2_TAX": "WS_LES_ST_TX2_TIT", "STATE2_MARITAL_STATUS": "WS_LES_ST_TX2_MS",
    })


def s_LES_Load_LES_EMP_DETAIL_RECTYPE_3_TBL(ctx: RunContext) -> None:
    """Type-3 (state-2/local taxes, CSRS, military deposit) -> LES_EMP_DETAIL_RECTYPE_3_TBL."""
    _load_rectype(ctx, "EMP_REC_TYPE_3", "WS_LES_REC_TYPE3", "3", "LES_EMP_DETAIL_RECTYPE_3_TBL", {
        **_D,
        "STATE2_EXCEPTIONS": "WS_LES_ST_TX2_XMP", "STATE2_ADDNL_AMOUNT": "WS_LES_ST_TX2_ADDTL",
        "LOCAL1_TAX": "WS_LES_LC_TX1_TIT", "LOCAL1_MARITAL_STATUS": "WS_LES_LC_TX1_MS", "LOCAL1_EXCEPTIONS": "WS_LES_LC_TX1_XMP",
        "LOCAL1_ADDNL_AMOUNT": "WS_LES_LC_TX1_ADDTL", "LOCAL1_TAX_NAME": "WS_LES_LC_TX1_NME",
        "LOCAL2_TAX": "WS_LES_LC_TX2_TIT", "LOCAL2_MARITAL_STATUS": "WS_LES_LC_TX2_MS", "LOCAL2_EXCEPTIONS": "WS_LES_LC_TX2_XMP",
        "LOCAL2_ADDNL_AMOUNT": "WS_LES_LC_TX2_ADDTL", "LOCAL2_TAX_NAME": "WS_LES_LC_TX2_NME",
        "CSRS": "WS_LES_CSRS_TIT", "CSRS_CUM_AMT": "WS_LES_CSRS_CUMM_AMT", "BIWEEKLY_AMT": "WS_LES_BIWK_TIT",
        "BIWEEKLY_ANNUITY": "WS_LES_BIWK_ANNTY", "MILITARY_DEPOSIT": "WS_LES_MILDPS_TIT",
    })


def s_LES_Load_LES_EMP_DETAIL_RECTYPE_4_TBL(ctx: RunContext) -> None:
    """Type-4 (military deposit, gross/taxable/non-taxable/deferred/deduction amounts) -> LES_EMP_DETAIL_RECTYPE_4_TBL."""
    _load_rectype(ctx, "EMP_REC_TYPE_4", "WS_LES_REC_TYPE4", "4", "LES_EMP_DETAIL_RECTYPE_4_TBL", {
        **_D,
        "MIL_DEPOSIT_PAID": "WS_LES_MILDPS_PD", "MIL_DEPOSIT_2": "WS_LES_MILDPS_TIT2", "MIL_DEPOSIT_BAL": "WS_LES_MILDPS_OWED",
        "GROSS_PAY_AMT": "WS_LES_GRS_PAY_AMT", "GROSS_PAY_AMT_YTD": "WS_LES_GRS_PAY_YTD",
        "TAXABLE_WAGE_AMT": "WS_LES_TAX_WGE_AMT", "TAXABLE_WAGE_AMT_YTD": "WS_LES_TAX_WGE_YTD",
        "NONTAXABLE_WAGE_AMT": "WS_LES_NTAX_WGE_AMT", "NONTAXABLE_WAGE_AMT_YTD": "WS_LES_NTAX_WGE_YTD",
        "TAX_DEFERRED_AMT": "WS_LES_DEF_TAX_AMT", "TAX_DEFERRED_AMT_YTD": "WS_LES_DEF_TAX_YTD",
        "DEDUCTION_AMT": "WS_LES_DED_TAX_AMT", "DEDUCTION_AMT_YTD": "WS_LES_DED_TAX_YTD",
    })


def s_LES_Load_LES_EMP_DETAIL_RECTYPE_5_TBL(ctx: RunContext) -> None:
    """Type-5 (AEIC, net pay, TSP/Roth elections) -> LES_EMP_DETAIL_RECTYPE_5_TBL."""
    _load_rectype(ctx, "EMP_REC_TYPE_5", "WS_LES_REC_TYPE5", "5", "LES_EMP_DETAIL_RECTYPE_5_TBL", {
        **_D,
        "ADV_ERND_INCOME_CREDIT_AMT": "WS_LES_AEI_AMT", "ADV_ERND_INCOME_CREDIT_AMT_YTD": "WS_LES_AEI_AMT_YTD",
        "NET_PAY_AMT": "WS_LES_NET_PAY_AMT", "NET_PAY_AMT_YTD": "WS_LES_NET_PAY_YTD",
        "TSP_SAVINGS": "WS_LES_XVX7_TITLE", "TSP_SAVINGS_PCT": "WS_LES_XVX7_PCT", "TITLEG_FUND": "WS_LES_GA_TIT",
        "TITLEG_FUND_PCT": "WS_LES_GA_PCT", "TITLEF_FUND": "WS_LES_FB_TIT", "TITLEF_FUND_PCT": "WS_LES_FB_PCT",
        "TITLEC_FUND": "WS_LES_CC_TIT", "TITLEC_FUND_PCT": "WS_LES_CC_PCT", "ROTH_SAVINGS": "WS_LES_ROTH_SV",
        "ROTH_SAVINGS_PCT": "WS_LES_ROTH_SV_PCT",
    })


def s_LES_Load_LES_EMP_DETAIL_RECTYPE_6_TBL(ctx: RunContext) -> None:
    """Type-6 (earnings for current/YTD TSP) -> LES_EMP_DETAIL_RECTYPE_6_TBL."""
    _load_rectype(ctx, "EMP_REC_TYPE_6", "WS_LES_REC_TYPE6", "6", "LES_EMP_DETAIL_RECTYPE_6_TBL", {
        **_D,
        "EARNINGS_FOR_CURR_TSP": "WS_LES_VHVM_TITLE", "EARNINGS_FOR_CURR_TSP_AMT": "WS_LES_VHVM_AMT",
        "EARNINGS_FOR_YTD_TSP": "WS_LES_VHVM_TITLE_YTD", "EARNINGS_FOR_YTD_TSP_AMT": "WS_LES_VHVM_AMT_YTD",
    })


def _trim(c: str) -> F.Column:
    """``LTRIM(RTRIM(c))``."""
    return F.trim(F.col(c))


def s_LES_Load_LES_EMP_DETAIL_RECTYPE_C_TBL(ctx: RunContext) -> None:
    """Type-C (current earnings, 3 per record) -> LES_EMP_DETAIL_RECTYPE_C_TBL; CURR_EARN_ID numbers the *C lines per employee."""
    _load_rectype(ctx, "EMP_REC_TYPE_C", "WS_CURR_REC_TYPE", "C", "LES_EMP_DETAIL_RECTYPE_C_TBL", {
        **_D, "CURR_EARN_ID": "CURR_EARN_ID",
        **{k: v for i in (1, 2, 3) for k, v in {
            f"CURR_EARN_NAME{i}": _trim(f"WS_CURR_ERN_NAME{i}"),
            f"CURR_EARN_NAME{i}_HOURS": f"WS_CURR_HRS_DAYS{i}",
            f"CURR_EARN_NAME{i}_AMT": f"WS_CURR_AMT{i}",
        }.items()},
    }, id_col="CURR_EARN_ID")


def s_LES_Load_LES_EMP_DETAIL_RECTYPE_R_TBL(ctx: RunContext) -> None:
    """Type-R (retroactive earnings, 3 per record) -> LES_EMP_DETAIL_RECTYPE_R_TBL; RETRO_ACTIVE_ID numbers the *R lines."""
    _load_rectype(ctx, "EMP_REC_TYPE_R", "WS_RETR_REC_TYPE", "R", "LES_EMP_DETAIL_RECTYPE_R_TBL", {
        **_D, "RETRO_ACTIVE_ID": "RETRO_ACTIVE_ID",
        **{k: v for i in (1, 2, 3) for k, v in {
            f"RETRO_ACTIVE_EARN_NAME{i}": _trim(f"WS_RETR_ERN_NAME{i}"),
            f"RETRO_ACTIVE_EARN_NAME{i}_HOURS": f"WS_RETR_HRS_DAYS{i}",
            f"RETRO_ACTIVE_EARN_NAME{i}_AMT": f"WS_RETR_AMT{i}",
        }.items()},
    }, id_col="RETRO_ACTIVE_ID")


def s_LES_Load_LES_EMP_DETAIL_RECTYPE_D_TBL(ctx: RunContext) -> None:
    """Type-D (deductions, 2 per record) -> LES_EMP_DETAIL_RECTYPE_D_TBL; DEDUCTION_ID numbers the *D lines."""
    _load_rectype(ctx, "EMP_REC_TYPE_D", "WS_DED_REC_TYPE", "D", "LES_EMP_DETAIL_RECTYPE_D_TBL", {
        **_D, "DEDUCTION_ID": "DEDUCTION_ID",
        **{k: v for i in (1, 2) for k, v in {
            f"DEDUCTION_{i}_NAME": _trim(f"WS_DED_NAME{i}"), f"DEDUCTION_{i}_CODE": f"WS_DED_CODE{i}",
            f"DEDUCTION_{i}_AMT": f"WS_DED_AMT{i}", f"DEDUCTION_{i}_AMT_YTD": f"WS_DED_AMT_YTD{i}",
        }.items()},
    }, id_col="DEDUCTION_ID")


def s_LES_Load_LES_EMP_DETAIL_RECTYPE_L_TBL(ctx: RunContext) -> None:
    """Type-L (leave balances) -> LES_EMP_DETAIL_RECTYPE_L_TBL.

    ``WS_LV_TERM_DATE`` carries either the use-or-lose termination date (``MM/DD/RR``) or the balance
    hours (``TO_DECIMAL(v, 2)``); both target columns are derived from it.
    """
    v = _trim("WS_LV_TERM_DATE")
    six = F.regexp_replace(v, "/", "")
    term_dte = F.when(v.rlike(r"^\d\d/\d\d/\d\d$") | v.rlike(r"^\d{6}$"), rr_date(six))
    _load_rectype(ctx, "EMP_REC_TYPE_L", "WS_LV_REC_TYPE", "L", "LES_EMP_DETAIL_RECTYPE_L_TBL", {
        **_D, "LEAVE_ID": "LEAVE_ID", "LEAVE_NAME": _trim("WS_LV_NAME"),
        "LEAVE_PRIOR_YEAR_BAL": "WS_LV_PRIOR_YR_BAL", "LEAVE_EARNED_CPP": "WS_LV_ACCR_PP", "LEAVE_EARNED_YTD": "WS_LV_ACCR_YTD",
        "LEAVE_USED_CPP": "WS_LV_USED_CURR_PP", "LEAVE_USED_YTD": "WS_LV_USED_YTD", "LEAVE_DONATED": "WS_LV_DONATE_RET",
        "LEAVE_CURR_BAL": "WS_LV_CURR_BAL",
        "LEAVE_USE_OR_LOSE_TERM_DTE": term_dte,
        "LEAVE_USE_OR_LOSE_TERM_BAL_HRS": F.when(is_number(v), v.cast("decimal(15,2)")),
    }, id_col="LEAVE_ID")


def s_LES_Load_LES_EMP_DETAIL_RECTYPE_U_TBL(ctx: RunContext) -> None:
    """Type-U (employer contributions, 2 per record) -> LES_EMP_DETAIL_RECTYPE_U_TBL."""
    _load_rectype(ctx, "EMP_REC_TYPE_U", "WS_EMPLR_REC_TYPE", "U", "LES_EMP_DETAIL_RECTYPE_U_TBL", {
        **_D, "EMPLOYER_CONTR_ID": "EMPLOYER_CONTR_ID",
        **{k: v for i in (1, 2) for k, v in {
            f"EMPLOYER_CONTR_{i}_NAME": _trim(f"WS_EMPLR_CONT_NAME{i}"),
            f"EMPLOYER_CONTR_{i}_AMT": f"WS_EMPLR_CONT_AMT{i}", f"EMPLOYER_CONTR_{i}_AMT_YTD": f"WS_EMPLR_CONT_AMT_YTD{i}",
        }.items()},
    }, id_col="EMPLOYER_CONTR_ID")


def s_LES_Load_LES_EMP_DETAIL_RECTYPE_M_TBL(ctx: RunContext) -> None:
    """Type-M (remarks) -> LES_EMP_DETAIL_RECTYPE_M_TBL; REMARK_ID numbers the *M lines per employee."""
    _load_rectype(ctx, "EMP_REC_TYPE_M", "WS_RMK_REC_TYPE", "M", "LES_EMP_DETAIL_RECTYPE_M_TBL", {
        **_D, "REMARK_ID": "REMARK_ID", "REMARK_TEXT": "WS_RMK_MSG",
    }, id_col="REMARK_ID")


def s_LES_Load_LES_EMP_DETAIL_RECTYPE_T_TBL(ctx: RunContext) -> None:
    """Type-T (time & attendance leave taken) -> LES_EMP_DETAIL_RECTYPE_T_TBL; TIME_ATTEND_ID numbers the *T lines."""
    _load_rectype(ctx, "EMP_REC_TYPE_T", "WS_TA_RECORD_TYPE", "T", "LES_EMP_DETAIL_RECTYPE_T_TBL", {
        **_D, "TIME_ATTEND_ID": "TIME_ATTEND_ID", "LEAVE_NAME": _trim("WS_TA_LV_NAME"),
        "LEAVE_TAKEN_DTE": rr_date(F.col("WS_TA_LV_DATE_TAKEN")), "LEAVE_HOURS": "WS_TA_LV_HOURS",
    }, id_col="TIME_ATTEND_ID")


# --------------------------------------------------------------------------------------------
# Normalised earnings / leave (read back the RECTYPE tables of the current pay period)
# --------------------------------------------------------------------------------------------
_KEYS = ["PP_END_YEAR", "PP_NUM", "LES_NUMBER", "PSEUDOSSN"]


def _normalize_earnings(ctx: RunContext, source: str, id_col: str, prefix: str, target: str,
                        out_prefix: str, trim_names: tuple) -> None:
    """SQ (SQL override, current pay period, ``TRIM`` on ``trim_names``) -> Normalizer (3 occurrences of
    NAME/HOURS/AMT, GCID 1..3) -> fil_Remove_Blank_* (``NOT ISNULL(NAME) AND LTRIM(RTRIM(NAME)) != ''``)
    -> exp_Convert (``v_ID = IIF(v_ID = 0, 1, v_ID + 1)``: running row number over the session) -> target (insert).
    """
    src = _pp_filter(ctx, _src(ctx, source))
    occ = []
    for i in (1, 2, 3):
        name = F.col(f"{prefix}{i}")
        occ.append(F.struct(
            F.lit(i).alias("GCID"),
            (F.trim(name) if i in trim_names else name).alias("NAME"),
            F.col(f"{prefix}{i}_HOURS").alias("HOURS"),
            F.col(f"{prefix}{i}_AMT").alias("AMT"),
        ))
    n = src.select(*_KEYS, F.col(id_col).alias("__src_id"), F.explode(F.array(*occ)).alias("o")).select(
        *_KEYS, "__src_id", "o.*")
    n = n.where(F.col("NAME").isNotNull() & (F.trim("NAME") != ""))
    w = Window.orderBy(*_KEYS, "__src_id", "GCID")
    out = n.select(
        *_KEYS, F.row_number().over(w).cast("decimal(15,0)").alias(id_col),
        F.col("NAME").alias(f"{out_prefix}_NAME"), F.col("HOURS").alias(f"{out_prefix}_HOURS"), F.col("AMT").alias(f"{out_prefix}_AMT"),
    )
    delta.append(ctx, out, _t(ctx, target))


def s_LES_Load_LES_EMP_DETAIL_CURR_EARN_TBL(ctx: RunContext) -> None:
    """LES_EMP_DETAIL_RECTYPE_C_TBL -> nrm_EMP_CURR_EARN (one row per non-blank earning) -> LES_EMP_DETAIL_CURR_EARN_TBL."""
    _normalize_earnings(ctx, "LES_EMP_DETAIL_RECTYPE_C_TBL", "CURR_EARN_ID", "CURR_EARN_NAME",
                        "LES_EMP_DETAIL_CURR_EARN_TBL", "CURR_EARN", trim_names=(1, 2))


def s_LES_Load_LES_EMP_DETAIL_RETRO_EARN_TBL(ctx: RunContext) -> None:
    """LES_EMP_DETAIL_RECTYPE_R_TBL -> nrm_EMP_RETRO_EARN (one row per non-blank earning) -> LES_EMP_DETAIL_RETRO_EARN_TBL."""
    src_id = "RETRO_ACTIVE_ID"
    _normalize_earnings(ctx, "LES_EMP_DETAIL_RECTYPE_R_TBL", src_id, "RETRO_ACTIVE_EARN_NAME",
                        "LES_EMP_DETAIL_RETRO_EARN_TBL", "RETRO_ACTIVE_EARN", trim_names=(1, 2, 3))


def s_LES_Load_LES_EMP_DETAIL_LEAVE_TBL(ctx: RunContext) -> None:
    """LES_EMP_DETAIL_RECTYPE_L_TBL (SQL override, current pay period, ``TRIM(LEAVE_NAME)``) -> LES_EMP_DETAIL_LEAVE_TBL.

    exp_Initial renumbers LEAVE_ID with a running counter (``IIF(v = 0, 1, v + 1)``) across the session.
    """
    src = _pp_filter(ctx, _src(ctx, "LES_EMP_DETAIL_RECTYPE_L_TBL"))
    cols = ["LEAVE_PRIOR_YEAR_BAL", "LEAVE_EARNED_CPP", "LEAVE_EARNED_YTD", "LEAVE_USED_CPP", "LEAVE_USED_YTD",
            "LEAVE_DONATED", "LEAVE_CURR_BAL", "LEAVE_USE_OR_LOSE_TERM_DTE", "LEAVE_USE_OR_LOSE_TERM_BAL_HRS"]
    w = Window.orderBy(*_KEYS, "LEAVE_ID")
    out = src.select(*_KEYS, F.row_number().over(w).cast("decimal(15,0)").alias("LEAVE_ID"),
                     F.trim("LEAVE_NAME").alias("LEAVE_NAME"), *cols)
    delta.append(ctx, out, _t(ctx, "LES_EMP_DETAIL_LEAVE_TBL"))


# --------------------------------------------------------------------------------------------
# s_LES_Load_LES_PRIMARY_DATA_TBL / m_LES_Load_LES_PRIMARY_DATA_TBL
# --------------------------------------------------------------------------------------------
_PRIMARY_COLS = {
    "lh": ["PP_END_DTE", ("HDR_LOC_ID", True), ("HDR_LOC_NAME", True), ("SYSTEM_ID", True), "TRANSMISSION_DTE"],
    "r1": ["PAY_DTE", ("EMPLOYEE_NAME", True), ("PAY_PLAN_CD", True), ("GRADE", True), ("STEP", True), "HRLY_RATE",
           "BASE_OT_RATE", "BASE_PAY", "LOCALITY_ADJ", "ADJ_BASE_PAY", "LOCALITY_PERCENTAGE", "FLSA_STATUS", "LV_SCD_DTE",
           "MIL_LV_CARRY", "LV_YR_END_DTE", "LAST_INCREASE_DTE"],
    "r2": ["NET_PAY_FINANCIAL_ORG", "ALLOT1_FINANCIAL_ORG", "ALLOT2_FINANCIAL_ORG", ("FEDERAL_TAX", True),
           "FED_MARITAL_STATUS", "FED_EXCEPTIONS", "FED_ADDNL_WITHHOLDING", ("STATE1_TAX", True), "STATE1_MARITAL_STATUS",
           "STATE1_EXCEPTIONS", "STATE1_ADDNL_AMOUNT", ("STATE2_TAX", True), "STATE2_MARITAL_STATUS"],
    "r3": [("STATE2_EXCEPTIONS", True), "STATE2_ADDNL_AMOUNT", ("LOCAL1_TAX", True), "LOCAL1_MARITAL_STATUS",
           ("LOCAL1_EXCEPTIONS", True), "LOCAL1_ADDNL_AMOUNT", ("LOCAL1_TAX_NAME", True), ("LOCAL2_TAX", True),
           "LOCAL2_MARITAL_STATUS", ("LOCAL2_EXCEPTIONS", True), "LOCAL2_ADDNL_AMOUNT", ("LOCAL2_TAX_NAME", True),
           ("CSRS", True), "CSRS_CUM_AMT", ("BIWEEKLY_AMT", True), "BIWEEKLY_ANNUITY", ("MILITARY_DEPOSIT", True)],
    "r4": ["MIL_DEPOSIT_PAID", ("MIL_DEPOSIT_2", True), "MIL_DEPOSIT_BAL", "GROSS_PAY_AMT", "GROSS_PAY_AMT_YTD",
           "TAXABLE_WAGE_AMT", "TAXABLE_WAGE_AMT_YTD", "NONTAXABLE_WAGE_AMT", "NONTAXABLE_WAGE_AMT_YTD", "TAX_DEFERRED_AMT",
           "TAX_DEFERRED_AMT_YTD", "DEDUCTION_AMT", "DEDUCTION_AMT_YTD"],
    # ADV_ERND_INCOME_CREDIT_AMT is selected by the SQL override but not connected to the target.
    "r5": ["ADV_ERND_INCOME_CREDIT_AMT_YTD", "NET_PAY_AMT", "NET_PAY_AMT_YTD", ("TSP_SAVINGS", True), "TSP_SAVINGS_PCT",
           ("TITLEG_FUND", True), "TITLEG_FUND_PCT", ("TITLEF_FUND", True), "TITLEF_FUND_PCT", ("TITLEC_FUND", True),
           "TITLEC_FUND_PCT", ("ROTH_SAVINGS", True), "ROTH_SAVINGS_PCT"],
    "r6": [("EARNINGS_FOR_CURR_TSP", True), "EARNINGS_FOR_CURR_TSP_AMT", ("EARNINGS_FOR_YTD_TSP", True),
           "EARNINGS_FOR_YTD_TSP_AMT"],
}


def s_LES_Load_LES_PRIMARY_DATA_TBL(ctx: RunContext) -> None:
    """SQ SQL override: inner join of LES_EMP_DETAIL_TBL (le), LES_HEADER_TBL (lh) and RECTYPE_1..6 (r1..r6) for the
    current pay period on PP_END_YEAR/PP_NUM(/LES_NUMBER/PSEUDOSSN), ``TRIM`` on the text columns, then
    lkp_PSEUDOSSN (PSEUDOSSN_TBL, ``PSEUDOSSN = in_PSEUDOSSN``, Use Any Value) for SSN/EMPLID/TK_NUM with
    ``o_SSN = IIF(ISNULL(lkp_SSN), PSEUDOSSN, lkp_SSN)`` -> LES_PRIMARY_DATA_TBL (insert).

    The override's ``TRIM(r5.TITLEC_FUND) TITLEF_FUND`` alias typo is harmless (SQ ports bind by position).
    """
    le = _pp_filter(ctx, _src(ctx, "LES_EMP_DETAIL_TBL")).alias("le")
    df = le.join(_pp_filter(ctx, _src(ctx, "LES_HEADER_TBL")).alias("lh"),
                 (F.col("lh.PP_END_YEAR") == F.col("le.PP_END_YEAR")) & (F.col("lh.PP_NUM") == F.col("le.PP_NUM")))
    for i in range(1, 7):
        r = f"r{i}"
        rt = _pp_filter(ctx, _src(ctx, f"LES_EMP_DETAIL_RECTYPE_{i}_TBL")).alias(r)
        cond = (F.col(f"{r}.PP_END_YEAR") == F.col("le.PP_END_YEAR")) & (F.col(f"{r}.LES_NUMBER") == F.col("le.LES_NUMBER")) \
            & (F.col(f"{r}.PSEUDOSSN") == F.col("le.PSEUDOSSN"))
        if i != 2:  # r2's predicate is ``r2.PP_NUM = r2.PP_NUM`` in the override (already pinned by $$MAP_PP_NUM)
            cond = cond & (F.col(f"{r}.PP_NUM") == F.col("le.PP_NUM"))
        df = df.join(rt, cond)
    sel = [F.col(f"le.{c}").alias(c) for c in ("PP_END_YEAR", "PP_NUM", "LES_NUMBER", "PSEUDOSSN", "LES_DELIVERY_IND")]
    for alias, cols in _PRIMARY_COLS.items():
        for c in cols:
            name, trim = (c, False) if isinstance(c, str) else c
            col = F.col(f"{alias}.{name}")
            sel.append((F.trim(col) if trim else col).alias(name))
    df = df.select(*sel)
    lkp = first_match(_src(ctx, "PSEUDOSSN_TBL").select(
        F.col("PSEUDOSSN").alias("__lkp_PSEUDOSSN"), F.col("SSN").alias("lkp_SSN"), "EMPLID", "TK_NUM"), ["__lkp_PSEUDOSSN"])
    df = df.join(lkp, F.col("PSEUDOSSN") == F.col("__lkp_PSEUDOSSN"), "left")
    df = df.withColumn("SSN", F.coalesce(F.col("lkp_SSN"), F.col("PSEUDOSSN"))).drop("__lkp_PSEUDOSSN", "lkp_SSN")
    delta.append(ctx, df, _t(ctx, "LES_PRIMARY_DATA_TBL"))


# --------------------------------------------------------------------------------------------
# LESRPT report loads (m_LESRPT_Load_LES<x>): current pay period RECTYPE rows -> LES<x> report tables
# --------------------------------------------------------------------------------------------
def _table_exists(ctx: RunContext, name: str) -> bool:
    return ctx.spark.catalog.tableExists(_t(ctx, name))


def _max_col(ctx: RunContext, table: str, col: str) -> int:
    """Unconnected lookup ``:LKP.LKP_<x>_KEY(1)`` with SQL override ``SELECT MAX(<x>.<x>_KEY) FROM <x>`` (NULL -> 0)."""
    if not _table_exists(ctx, table):
        return 0
    v = _src(ctx, table).agg(F.max(col)).collect()[0][0]
    return int(v or 0)


def _with_surrogate(ctx: RunContext, df: DataFrame, table: str, key: str, order: list) -> DataFrame:
    """exp_Final ``v_KEY = IIF(v_PREV_KEY = 0 OR ISNULL(v_PREV_KEY), MAX_KEY + 1, v_KEY + 1)``: MAX(existing) + row_number."""
    base = _max_col(ctx, table, key)
    return df.withColumn(key, (F.row_number().over(Window.orderBy(*order)) + F.lit(base)).cast("decimal(15,0)"))


def _sequence(ctx: RunContext, df: DataFrame, table: str, col: str, current_value: int, order: list) -> DataFrame:
    """Non-reusable Sequence Generator (increment 1, no cycle). PowerCenter persists ``Current Value`` in the
    repository; here it resumes from ``MAX(<col>)`` of the target (or the XML Current Value on first load)."""
    start = max(_max_col(ctx, table, col) + 1, current_value) if _table_exists(ctx, table) else current_value
    return df.withColumn(col, (F.row_number().over(Window.orderBy(*order)) - 1 + F.lit(start)).cast("decimal(15,0)"))


def _rpt_source(ctx: RunContext, source: str) -> DataFrame:
    """SQ_<source> with Source Filter ``PP_END_YEAR = $$MAP_PP_END_YEAR AND PP_NUM = $$MAP_PP_NUM``."""
    return _pp_filter(ctx, _src(ctx, source))


def s_LESRPT_Load_LESM(ctx: RunContext) -> None:
    """LES_EMP_DETAIL_RECTYPE_M_TBL (remarks) -> LESM (insert) with LESM_KEY = MAX(LESM_KEY) + n, LOAD_NBR = 1."""
    src = _rpt_source(ctx, "LES_EMP_DETAIL_RECTYPE_M_TBL")
    df = src.select(
        F.col("LES_NUMBER").alias("MASTER_ID"), F.col("PSEUDOSSN").alias("SSN"), F.col("PP_END_YEAR").alias("PYR"),
        F.col("PP_NUM").alias("PPN"), F.col("REMARK_ID").alias("SEQ"), F.col("REMARK_TEXT").alias("RMK_MSG"),
        F.col("REMARK_ID").alias("NEW_SEQ_NBR"), F.lit(1).cast("decimal(15,0)").alias("LOAD_NBR"),
    )
    df = _with_surrogate(ctx, df, "LESM", "LESM_KEY", ["PYR", "PPN", "MASTER_ID", "SSN", "SEQ"])
    delta.append(ctx, df, _t(ctx, "LESM"))


def s_LESRPT_Load_LEST(ctx: RunContext) -> None:
    """LES_EMP_DETAIL_RECTYPE_T_TBL (leave taken) -> LEST (insert); LEST_KEY = MAX + n, NEW_SEQ_NBR/LOAD_NBR = SEQTRANS.NEXTVAL,
    CREATE_DT = SESSSTARTTIME."""
    src = _rpt_source(ctx, "LES_EMP_DETAIL_RECTYPE_T_TBL")
    df = src.select(
        F.col("LES_NUMBER").alias("MASTER_ID"), F.col("PSEUDOSSN").alias("SSN"), F.col("PP_END_YEAR").alias("PYR"),
        F.col("PP_NUM").alias("PPN"), "LEAVE_NAME", F.col("LEAVE_TAKEN_DTE").alias("LEAVE_DATE_TAKEN"), "LEAVE_HOURS",
        F.current_timestamp().alias("CREATE_DT"), F.col("TIME_ATTEND_ID").alias("__ord"),
    )
    order = ["PYR", "PPN", "MASTER_ID", "SSN", "__ord"]
    df = _with_surrogate(ctx, df, "LEST", "LEST_KEY", order)
    df = _sequence(ctx, df, "LEST", "NEW_SEQ_NBR", 3686821, order).withColumn("LOAD_NBR", F.col("NEW_SEQ_NBR"))
    delta.append(ctx, df.drop("__ord"), _t(ctx, "LEST"))


def _lesrpt(ctx: RunContext, source: str, id_col: str, target: str, key: str, seq_current: int, cols: dict) -> None:
    """Shared shape of m_LESRPT_Load_LESC/LESD/LESU: SQL override (current pay period, ``ORDER BY 1, 2, 3, 4, 5``)
    -> exp_Initial (``:LKP.LKP_<x>_KEY(1)`` = ``MAX(<x>_KEY)``) -> exp_Final (``<x>_KEY = MAX + n``, ``LOAD_NBR = 1``,
    ``CREATE_DT = SESSSTARTTIME``) + SEQTRANS.NEXTVAL -> NEW_SEQ_NBR -> target (insert)."""
    src = _rpt_source(ctx, source)
    df = src.select(
        F.col("LES_NUMBER").alias("MASTER_ID"), F.col("PSEUDOSSN").alias("SSN"), F.col("PP_END_YEAR").alias("PYR"),
        F.col("PP_NUM").alias("PPN"), *[F.col(s).alias(t) for t, s in cols.items()],
        F.lit(1).cast("decimal(15,0)").alias("LOAD_NBR"), F.current_timestamp().alias("CREATE_DT"), F.col(id_col).alias("__ord"),
    )
    order = ["PYR", "PPN", "MASTER_ID", "SSN", "__ord"]
    df = _with_surrogate(ctx, df, target, key, order)
    df = _sequence(ctx, df, target, "NEW_SEQ_NBR", seq_current, order)
    delta.append(ctx, df.drop("__ord"), _t(ctx, target))


def s_LESRPT_Load_LESC(ctx: RunContext) -> None:
    """LES_EMP_DETAIL_RECTYPE_C_TBL (current earnings) -> LESC."""
    _lesrpt(ctx, "LES_EMP_DETAIL_RECTYPE_C_TBL", "CURR_EARN_ID", "LESC", "LESC_KEY", 1825331, {
        k: v for i in (1, 2, 3) for k, v in {
            f"CURR_ERN_NAME{i}": f"CURR_EARN_NAME{i}", f"CURR_HRS_DAYS{i}": f"CURR_EARN_NAME{i}_HOURS",
            f"CURR_AMT{i}": f"CURR_EARN_NAME{i}_AMT"}.items()})


def s_LESRPT_Load_LESD(ctx: RunContext) -> None:
    """LES_EMP_DETAIL_RECTYPE_D_TBL (deductions) -> LESD."""
    _lesrpt(ctx, "LES_EMP_DETAIL_RECTYPE_D_TBL", "DEDUCTION_ID", "LESD", "LESD_KEY", 10347492, {
        k: v for i in (1, 2) for k, v in {
            f"DED_NAME{i}": f"DEDUCTION_{i}_NAME", f"DED_CODE{i}": f"DEDUCTION_{i}_CODE",
            f"DED_AMT{i}": f"DEDUCTION_{i}_AMT", f"DED_AMT_YTD{i}": f"DEDUCTION_{i}_AMT_YTD"}.items()})


def s_LESRPT_Load_LESU(ctx: RunContext) -> None:
    """LES_EMP_DETAIL_RECTYPE_U_TBL (employer contributions) -> LESU."""
    _lesrpt(ctx, "LES_EMP_DETAIL_RECTYPE_U_TBL", "EMPLOYER_CONTR_ID", "LESU", "LESU_KEY", 6238046, {
        k: v for i in (1, 2) for k, v in {
            f"BEN_NAME{i}": f"EMPLOYER_CONTR_{i}_NAME", f"BEN_AMT{i}": f"EMPLOYER_CONTR_{i}_AMT",
            f"BEN_AMT_YTD{i}": f"EMPLOYER_CONTR_{i}_AMT_YTD"}.items()})


def s_LESRPT_Load_LESR(ctx: RunContext) -> None:
    """LES_EMP_DETAIL_RECTYPE_R_TBL (retroactive earnings) -> LESR; LESR_KEY = MAX + n, NEW_SEQ_NBR = SEQTRANS.NEXTVAL."""
    src = _rpt_source(ctx, "LES_EMP_DETAIL_RECTYPE_R_TBL")
    df = src.select(
        F.col("LES_NUMBER").alias("MASTER_ID"), F.col("PSEUDOSSN").alias("SSN"), F.col("PP_END_YEAR").alias("PYR"),
        F.col("PP_NUM").alias("PPN"),
        *[F.col(f"RETRO_ACTIVE_EARN_NAME{i}{s}").alias(f"{t}{i}") for i in (1, 2, 3)
          for s, t in (("", "RETR_ERN_NAME"), ("_HOURS", "RETR_HRS_DAYS"), ("_AMT", "RETR_AMT"))],
        F.col("RETRO_ACTIVE_ID").alias("__ord"),
    )
    order = ["PYR", "PPN", "MASTER_ID", "SSN", "__ord"]
    df = _with_surrogate(ctx, df, "LESR", "LESR_KEY", order)
    df = _sequence(ctx, df, "LESR", "NEW_SEQ_NBR", 180666, order)
    delta.append(ctx, df.drop("__ord"), _t(ctx, "LESR"))


def s_LESRPT_Load_LESL(ctx: RunContext) -> None:
    """LES_EMP_DETAIL_LEAVE_TBL -> LESL (insert).

    exp_Final: ``SEQ``/``NEW_SEQ_NBR = TO_CHAR(v_SEQ_NBR)`` where v_SEQ_NBR restarts at 1 for each LES_NUMBER
    (rows ordered ``ORDER BY 1, 2, 3, 4, 5``); ``LV_TERM_DATE = DECODE(TRUE, NOT ISNULL(TERM_DTE),
    TO_CHAR(TERM_DTE, 'MM/DD/YYYY'), NOT ISNULL(TERM_BAL_HRS), TO_CHAR(TERM_BAL_HRS), ' ')``; LESL_KEY = MAX + n.
    """
    src = _rpt_source(ctx, "LES_EMP_DETAIL_LEAVE_TBL")
    order = ["PP_END_YEAR", "PP_NUM", "LES_NUMBER", "PSEUDOSSN", "LEAVE_ID"]
    seq = F.row_number().over(Window.partitionBy("LES_NUMBER").orderBy(*order)).cast("string")
    term = (F.when(F.col("LEAVE_USE_OR_LOSE_TERM_DTE").isNotNull(), F.date_format("LEAVE_USE_OR_LOSE_TERM_DTE", "MM/dd/yyyy"))
            .when(F.col("LEAVE_USE_OR_LOSE_TERM_BAL_HRS").isNotNull(), F.col("LEAVE_USE_OR_LOSE_TERM_BAL_HRS").cast("string"))
            .otherwise(F.lit(" ")))
    df = src.select(
        F.col("LES_NUMBER").alias("MASTER_ID"), F.col("PSEUDOSSN").alias("SSN"), F.col("PP_END_YEAR").alias("PYR"),
        F.col("PP_NUM").alias("PPN"), seq.alias("SEQ"), seq.alias("NEW_SEQ_NBR"),
        F.col("LEAVE_NAME").alias("LV_NAME"), F.col("LEAVE_PRIOR_YEAR_BAL").alias("LV_PRIOR_YR_BAL"),
        F.col("LEAVE_EARNED_CPP").alias("LV_ACCR_PP"), F.col("LEAVE_EARNED_YTD").alias("LV_ACCR_YTD"),
        F.col("LEAVE_USED_CPP").alias("LV_USED_CURR_PP"), F.col("LEAVE_USED_YTD").alias("LV_USED_YTD"),
        F.col("LEAVE_DONATED").alias("LV_DONATE_RET"), F.col("LEAVE_CURR_BAL").alias("LV_CURR_BAL"),
        term.alias("LV_TERM_DATE"), F.col("LEAVE_ID").alias("__ord"),
    )
    df = _with_surrogate(ctx, df, "LESL", "LESL_KEY", ["PYR", "PPN", "MASTER_ID", "SSN", "__ord"])
    delta.append(ctx, df.drop("__ord"), _t(ctx, "LESL"))


_LESS_COLS = {
    "TKN": "TK_NUM", "EMPLID": "EMPLID", "PPD_END_DT": "PP_END_DTE", "PAY_DT": "PAY_DTE", "EYE_NME": "EMPLOYEE_NAME",
    "PAY_PLAN": "PAY_PLAN_CD", "PAY_GRADE": "GRADE", "PAY_STEP": "STEP", "HRLY_RATE": "HRLY_RATE",
    "BASIC_OT_RATE": "BASE_OT_RATE", "BASIC_PAY": "BASE_PAY", "LOCAL_ADJ": "LOCALITY_ADJ", "ADJ_BASIC_PAY": "ADJ_BASE_PAY",
    "LOCAL_PCT": "LOCALITY_PERCENTAGE", "FLSA": "FLSA_STATUS", "SCD_LEAVE_DT": "LV_SCD_DTE", "MAX_LV_CRY": "MIL_LV_CARRY",
    "LV_YR_END_DT": "LV_YR_END_DTE", "DATE_LST_INC": "LAST_INCREASE_DTE", "NETPAY_FINORG": "NET_PAY_FINANCIAL_ORG",
    "ALLT1_FINORG": "ALLOT1_FINANCIAL_ORG", "ALLT2_FINORG": "ALLOT2_FINANCIAL_ORG", "FED_TIT": "FEDERAL_TAX",
    "FED_MS": "FED_MARITAL_STATUS", "FED_ITW_XMP": "FED_EXCEPTIONS", "FED_ADDTL": "FED_ADDNL_WITHHOLDING",
    "ST_TX1_TIT": "STATE1_TAX", "ST_TX1_MS": "STATE1_MARITAL_STATUS", "ST_TX1_XMP": "STATE1_EXCEPTIONS",
    "ST_TX1_ADDTL": "STATE1_ADDNL_AMOUNT", "ST_TX2_TIT": "STATE2_TAX", "ST_TX2_MS": "STATE2_MARITAL_STATUS",
    "ST_TX2_XMP": "STATE2_EXCEPTIONS", "ST_TX2_ADDTL": "STATE2_ADDNL_AMOUNT", "LC_TX1_TIT": "LOCAL1_TAX",
    "LC_TX1_MS": "LOCAL1_MARITAL_STATUS", "LC_TX1_XMP": "LOCAL1_EXCEPTIONS", "LC_TX1_ADDTL": "LOCAL1_ADDNL_AMOUNT",
    "LC_TX1_NME": "LOCAL1_TAX_NAME", "LC_TX2_TIT": "LOCAL2_TAX", "LC_TX2_MS": "LOCAL2_MARITAL_STATUS",
    "LC_TX2_XMP": "LOCAL2_EXCEPTIONS", "LC_TX2_ADDTL": "LOCAL2_ADDNL_AMOUNT", "LC_TX2_NME": "LOCAL2_TAX_NAME",
    "CSRS_TIT": "CSRS", "CSRS_CUMM_AMT": "CSRS_CUM_AMT", "BIWK_TIT": "BIWEEKLY_AMT", "BIWK_ANNTY": "BIWEEKLY_ANNUITY",
    "MILDPS_TIT": "MILITARY_DEPOSIT", "MILDPS_PD": "MIL_DEPOSIT_PAID", "MILDPS_TIT2": "MIL_DEPOSIT_2",
    "MILDPS_OWED": "MIL_DEPOSIT_BAL", "GRS_PAY_AMT": "GROSS_PAY_AMT", "GRS_PAY_YTD": "GROSS_PAY_AMT_YTD",
    "TAX_WGE_AMT": "TAXABLE_WAGE_AMT", "TAX_WGE_YTD": "TAXABLE_WAGE_AMT_YTD", "NTAX_WGE_AMT": "NONTAXABLE_WAGE_AMT",
    "NTAX_WGE_YTD": "NONTAXABLE_WAGE_AMT_YTD", "DEF_TAX_AMT": "TAX_DEFERRED_AMT", "DEF_TAX_YTD": "TAX_DEFERRED_AMT_YTD",
    "DED_TAX_AMT": "DEDUCTION_AMT", "DED_TAX_YTD": "DEDUCTION_AMT_YTD", "AEI_AMT_YTD": "ADV_ERND_INCOME_CREDIT_AMT_YTD",
    "NET_PAY_AMT": "NET_PAY_AMT", "NET_PAY_YTD": "NET_PAY_AMT_YTD", "ROTH_SAVING": "ROTH_SAVINGS",
}


def s_LESRPT_Load_LESS(ctx: RunContext) -> None:
    """LES_PRIMARY_DATA_TBL (Source Filter: current pay period) -> LESS (insert); LESS_KEY = MAX + n, LOAD_NBR = 1,
    CREATE_DT = SESSSTARTTIME.

    exp_Final.AEI_AMT has no input link (always NULL); the TSP/fund columns of LESS are not connected in the mapping.
    """
    src = _rpt_source(ctx, "LES_PRIMARY_DATA_TBL")
    df = src.select(
        F.col("LES_NUMBER").alias("MASTER_ID"), F.col("PSEUDOSSN").alias("SSN"), F.col("PP_END_YEAR").alias("PYR"),
        F.col("PP_NUM").alias("PPN"), *[F.col(s).alias(t) for t, s in _LESS_COLS.items()],
        F.lit(None).cast("decimal(10,2)").alias("AEI_AMT"),
        F.lit(1).cast("decimal(15,0)").alias("LOAD_NBR"), F.current_timestamp().alias("CREATE_DT"),
    )
    df = _with_surrogate(ctx, df, "LESS", "LESS_KEY", ["PYR", "PPN", "MASTER_ID", "SSN"])
    delta.append(ctx, df, _t(ctx, "LESS"))


# --------------------------------------------------------------------------------------------
# s_LES_NIH_FILE / m_LES_NIH_FILE
# --------------------------------------------------------------------------------------------
def s_LES_NIH_FILE(ctx: RunContext) -> None:
    """LES_EMPLOYEE_DETAIL (flat file, every LES input file) -> fil_Header_Trailer (record type not ``0*``/``2*``)
    -> lkp_PSEUDOSSN_TBL (``PSEUDOSSN = SOCIAL_SECURITY_NUMBER``, Use Any Value: SSN, BUSINESS_UNIT)
    -> exp_Flag_NIH_Record:
      ``v_NIH_EMPLOYEE_FLAG = DECODE(TRUE, *E AND BU = 'NIH00', TRUE, *E AND BU != 'NIH00', FALSE,
      NOT *E AND v_PREV = TRUE, TRUE, FALSE)`` (detail records inherit their employee's flag),
      ``o_SSN = IIF(SSN != lkp_SSN, lkp_SSN, SSN)``
    -> fil_NIH_Records -> LES_NIH_EMPLOYEE_DETAIL file ($Param_LES_NIH_emp_detail, truncate)
    -> fil_NIH_EMPLOYEE_DETAIL (``*E`` only) + lkp_Current_Pay_Period -> LES_NIH_EMPLOYEE_SUMMARY_TBL (insert).
    """
    src = read_flat(ctx, "LES_EMPLOYEE_DETAIL")
    src = src.where(~F.substring("RECORD_TYPE", 1, 1).isin("0", "2"))
    lkp = first_match(_src(ctx, "PSEUDOSSN_TBL").select(
        F.col("PSEUDOSSN").alias("__lkp_PSEUDOSSN"), F.col("SSN").alias("lkp_SSN"),
        F.col("BUSINESS_UNIT").alias("lkp_BUSINESS_UNIT")), ["__lkp_PSEUDOSSN"])
    df = src.join(lkp, F.col("SOCIAL_SECURITY_NUMBER") == F.col("__lkp_PSEUDOSSN"), "left")
    is_e = F.col("RECORD_TYPE") == "*E"
    emp_flag = F.when(is_e, F.coalesce(F.col("lkp_BUSINESS_UNIT") == "NIH00", F.lit(False)))
    upto = Window.orderBy(*ORDER_COLS).rowsBetween(Window.unboundedPreceding, Window.currentRow)
    df = df.withColumn("NIH_RECORD_FLAG", F.coalesce(F.last(emp_flag, ignorenulls=True).over(upto), F.lit(False)))
    df = df.withColumn("SSN", F.when(F.col("SOCIAL_SECURITY_NUMBER") != F.col("lkp_SSN"), F.col("lkp_SSN"))
                       .otherwise(F.col("SOCIAL_SECURITY_NUMBER")))
    nih = df.where("NIH_RECORD_FLAG").cache()
    write_flat(ctx, nih.orderBy(*ORDER_COLS).select("RECORD_TYPE", "SSN", "LES_NUMBER", "LES_DELIVERY_INDICATOR", "FILLER"),
               "LES_NIH_EMPLOYEE_DETAIL", ctx.param("Param_LES_NIH_emp_detail", "les_nih_emp_detail.txt"))
    pp = first_match(_src(ctx, "PAY_PERIOD").where("CURR_PP_FLAG = 'Y'").select("PP_END_YEAR", "PP_NUM"), [])
    summary = nih.where(is_e).crossJoin(pp).select(
        F.col("PP_END_YEAR").cast("decimal(4,0)"), F.col("PP_NUM").cast("decimal(2,0)"),
        F.col("LES_NUMBER").cast("decimal(15,0)").alias("LES_NUMBER"), F.col("SSN"))
    delta.append(ctx, summary, _t(ctx, "LES_NIH_EMPLOYEE_SUMMARY_TBL"))


# --------------------------------------------------------------------------------------------
# s_LES_Build_Message_Counters / m_LES_Build_Message_Counters
# --------------------------------------------------------------------------------------------
_COUNTER_DESCRIPTIONS = ["Input Records", "Records Loaded", "NIH Records", "Records not found on Pseudo"]


def s_LES_Build_Message_Counters(ctx: RunContext) -> dict:
    """Four single-row aggregators full-outer-joined on a constant 1, then normalised into COUNTER_TBL and
    formatted into LES_MESSAGE_FILE (les_message_file.txt); ``SETVARIABLE($$MAP_SUBJECT/$$MAP_MESSAGE)`` is
    assigned back to ``$$WF_SUBJECT``/``$$WF_MESSAGE``.

    * COUNTER_1 ``COUNT(WS_EMP_LES_NUM)`` of ``*E`` records in the LES input (EMP_REC_TYPE_E)
    * COUNTER_2 ``COUNT(LES_NUMBER)`` of LES_PRIMARY_DATA_TBL for ``$$MAP_PP_END_YEAR``/``$$MAP_PP_NUM``
    * COUNTER_3 ``COUNT(LES_NUMBER)`` of LES_NIH_EMPLOYEE_SUMMARY_TBL (SQ has no filter: all pay periods)
    * COUNTER_4 ``COUNT(*)`` of the latest ERROR_TBL batch written today by m_LES_Load_LES_EMP_DETAIL_TBL
      for the pay period (SQL override with ``MAX(ERROR_DATE) ... >= TRUNC(SYSDATE)``)

    Post-session success commands (owned by the orchestration unit):
      1. ``cd $Param_Root_Directory/data/int/out/LES; cp $Param_LES_NIH_emp_detail NIHLES.P$$WF_PP_YEAR_NUM``
      2. ``/home/sa-infoadm/scripts/nih_les_transfer.sh NIHLES.P$$WF_PP_YEAR_NUM``
      3. ``$Param_ControlDir/scripts/archive_files.sh $Param_Root_Directory/data/int/in/LES
         $Param_Root_Directory/data/archive/LES $$WF_PP_YEAR_NUM``
    """
    year, num = _map_pp(ctx)
    c1 = read_vsam(ctx, "EMP_REC_TYPE_E").where("WS_EMP_REC_TYPE = '*E'").where(F.col("WS_EMP_LES_NUM").isNotNull()).count()
    c2 = _pp_filter(ctx, _src(ctx, "LES_PRIMARY_DATA_TBL")).where(F.col("LES_NUMBER").isNotNull()).count()
    c3 = (_src(ctx, "LES_NIH_EMPLOYEE_SUMMARY_TBL").where(F.col("LES_NUMBER").isNotNull()).count()
          if _table_exists(ctx, "LES_NIH_EMPLOYEE_SUMMARY_TBL") else 0)
    c4 = 0
    if _table_exists(ctx, "ERROR_TBL"):
        err = _pp_filter(ctx, _src(ctx, "ERROR_TBL").where(
            F.col("PROCESS_NAME") == "m_LES_Load_LES_EMP_DETAIL_TBL"))
        latest = err.where(F.col("ERROR_DATE") >= F.current_date()).agg(F.max("ERROR_DATE")).collect()[0][0]
        if latest is not None:
            c4 = err.where(F.col("ERROR_DATE") == F.lit(latest)).count()
    counts = [c1, c2, c3, c4]

    pp = _current_pay_period(ctx) or {}
    pp_year, pp_num = pp.get("PP_END_YEAR"), pp.get("PP_NUM")
    rows = [(d, v, i) for i, (d, v) in enumerate(zip(_COUNTER_DESCRIPTIONS, counts), start=1)]
    counters = ctx.spark.createDataFrame(rows, "COUNTER_DESCRIPTION string, COUNTER_VALUE long, GCID int").select(
        F.current_timestamp().alias("RUN_DATE"), F.lit("m_LES_Build_Message_Counters").alias("PROCESS_NAME"),
        "COUNTER_DESCRIPTION", F.col("COUNTER_VALUE").cast("decimal(15,0)"),
        F.lit(pp_year).cast("decimal(4,0)").alias("PP_END_YEAR"), F.lit(pp_num).cast("decimal(2,0)").alias("PP_NUM"))
    delta.append(ctx, counters, _t(ctx, "COUNTER_TBL"))

    env = {"Dev_": "Dev: ", "Test": "Test: ", "Prod": "Prod: "}.get((ctx.param("PMRepositoryServiceName", ""))[:4], "")
    pp_num_txt = "" if pp_num is None else f"{int(pp_num):02d}"
    subject = f"{env}LES Process Completed Successfully for: {'' if pp_year is None else int(pp_year)}-{pp_num_txt}"
    message = (f"Number of Input Records                    \t= {c1}\n"
               f"Number of Records loaded                    \t= {c2}\n"
               f"Number of NIH Records                           = {c3}\n"
               f"Number of Records Not on Pseudo                 = {c4}")
    msg_df = ctx.spark.createDataFrame([(subject, message)], "SUBJECT string, MESSAGE string")
    write_flat(ctx, msg_df, "LES_MESSAGE_FILE", "les_message_file.txt")
    return {"WF_SUBJECT": _setvariable_max(_var(ctx, "WF_SUBJECT"), [subject]),
            "WF_MESSAGE": _setvariable_max(_var(ctx, "WF_MESSAGE"), [message])}


def email_LES_Complete(ctx: RunContext) -> dict:
    """Email task: To ``$$WF_LES_EMAIL_LIST``, Subject ``$$WF_SUBJECT``, Text ``$$WF_MESSAGE``.

    Delivery is owned by the orchestration unit (job notification); the rendered email is returned.
    """
    return {"EMAIL_TO": ctx.param("WF_LES_EMAIL_LIST", ""), "EMAIL_SUBJECT": ctx.param("WF_SUBJECT", ""),
            "EMAIL_TEXT": ctx.param("WF_MESSAGE", "")}


TASKS = {
    "s_LES_Current_Pay_Period": s_LES_Current_Pay_Period,
    "s_LES_Verify_Header": s_LES_Verify_Header,
    "s_LES_Verify_Record_Count": s_LES_Verify_Record_Count,
    "s_LES_Load_LES_HEADER_TBL": s_LES_Load_LES_HEADER_TBL,
    "s_LES_Load_LES_EMP_DETAIL_TBL": s_LES_Load_LES_EMP_DETAIL_TBL,
    **{f"s_LES_Load_LES_EMP_DETAIL_RECTYPE_{x}_TBL": globals()[f"s_LES_Load_LES_EMP_DETAIL_RECTYPE_{x}_TBL"]
       for x in ("1", "2", "3", "4", "5", "6", "C", "R", "D", "L", "U", "M", "T")},
    "s_LES_Load_LES_PRIMARY_DATA_TBL": s_LES_Load_LES_PRIMARY_DATA_TBL,
    "s_LES_Load_LES_EMP_DETAIL_CURR_EARN_TBL": s_LES_Load_LES_EMP_DETAIL_CURR_EARN_TBL,
    "s_LES_Load_LES_EMP_DETAIL_RETRO_EARN_TBL": s_LES_Load_LES_EMP_DETAIL_RETRO_EARN_TBL,
    "s_LES_Load_LES_EMP_DETAIL_LEAVE_TBL": s_LES_Load_LES_EMP_DETAIL_LEAVE_TBL,
    **{f"s_LESRPT_Load_LES{x}": globals()[f"s_LESRPT_Load_LES{x}"] for x in "CDLMRSTU"},
    "s_LES_NIH_FILE": s_LES_NIH_FILE,
    "s_LES_Build_Message_Counters": s_LES_Build_Message_Counters,
    "email_LES_Complete": email_LES_Complete,
}
