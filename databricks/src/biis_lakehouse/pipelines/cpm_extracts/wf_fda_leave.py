"""wf_FDA_Leave (XML/FDA_Leave): FDA leave TATRAN extract (12200 file) with the "200" leave-balance records.

Session chain (workflow links): 0010 Verify_File -> 0020 Set_CPM_Calendar -> 0025 Set_Pay_Calendar
-> 0050 Update_CPM_CYCLE_TBL_FDA -> 0100 Load_TATRAN_To_DB -> 0150 Error_Counter -> 0200 Create_200_Rows
-> 0300 Create_Output_File -> 0500 IO_Counter -> 1100 Send_Email.

Files: input ``$Param_FDA_12100_file`` (``tatran*``) in ``in/CPM``; 0010 reads it as a file list
(``CurrentlyProcessedFileName``). Output ``$Param_FDA_12200_out_file`` in ``out/CPM``.

Session commands (owned by the orchestration unit, ``biis_lakehouse.ops``):

* 0010 source file-list command: ``ls -1 $Param_Root_Directory/data/int/in/CPM/tatran*``
* 1100 post: ``cp $Param_FDA_12200_out_file FDA.TATRAN.PP$$WF_PP_NUM.TXT``, ``fda_transfer.sh FDA.TATRAN.PP$$WF_PP_NUM.TXT``,
  archive (``mv``) of the ``tatran*`` input files.
"""
from __future__ import annotations

import glob
import os
import re
from functools import reduce

from pyspark.sql import DataFrame, Window, functions as F

from biis_lakehouse.common import delta
from biis_lakehouse.common.fixed_width import Field, write_fixed_width
from biis_lakehouse.context import RunContext
from biis_lakehouse.pipelines.cpm_extracts import _common
from biis_lakehouse.pipelines.cpm_extracts._common import _dec, _num_str
from biis_lakehouse.pipelines.cpm_extracts._spec_fda_leave import SPECS
from biis_lakehouse.pipelines.cpm_extracts.wf_cpm_afps import update_cycle

TATRAN_FILE = [Field("FDA_TK_NO", 5), Field("FDA_EMP_ID", 9), Field("FDA_PP_YEAR", 4), Field("FDA_PP_NUM", 2), Field("FDA_REC_TYPE", 2), Field("FDA_DATA", 118)]
TATRAN_OUT = [Field("FDA_TK_NO", 5), Field("FDA_EMP_ID", 9), Field("FDA_PP_YEAR", 4), Field("FDA_PP_NUM", 2), Field("FDA_REC_TYPE", 2), Field("FDA_DATA", 114)]
MESSAGE_FILE = [Field("COUNT_DESC", 50), Field("COUNT_VALUE", 10)]
TATRAN_COLS = ["FDA_BATCH_ID", "FDA_TK_NO", "FDA_EMP_ID", "FDA_PP_YEAR", "FDA_PP_NUM", "FDA_REC_TYPE", "FDA_SEQ", "FDA_DATA"]
ERROR_PROCESS = "m_0150_PM_FDA_Error_Counter"
REC_200 = (200, 201, 202, 203, 204, 205, 206, 207, 208, 209, 211, 212, 214, 215)
PORTS_200 = SPECS["m_0200_PM_FDA_Create_Insert_200_Rows"]["_ports"]
OWNER = "INFO_TARGET_DEV"


def _input_files(ctx: RunContext) -> list[str]:
    pattern = ctx.param("FDA_12100_file", "tatran*")
    return sorted(p for p in glob.glob(ctx.path("in", _common.AREA, pattern)) if os.path.isfile(p))


def _params_pp(ctx: RunContext):
    return _dec(ctx.param("WF_PP_END_YEAR", "")), _dec(ctx.param("WF_PP_NUM", ""))


def _pp_text(year, num) -> str:
    return f"{_num_str(year) or ''}-{(_num_str(num) or '').rjust(2, '0')}"


def _tatran(ctx: RunContext) -> DataFrame:
    return ctx.table("HI_PM_FDA_TATRAN_TBL", OWNER)


def _first(df: DataFrame, keys: list[str], order: list[str]) -> DataFrame:
    """Lookup multiple-match policy (*Use First Value* / *Use Any Value*): one deterministic row per key."""
    w = Window.partitionBy(*keys).orderBy(*[F.col(c).asc_nulls_last() for c in order])
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")


def s_0010_PM_FDA_Verify_File(ctx: RunContext) -> None:
    """srt_Distinct_File_Names + agg_Count_Number_of_Files over the file list; exp_Validate_Parameters ABORTs when
    there is not exactly one file or its ``P<y><pp>`` name suffix is not the current pay period."""
    files = [p for p in _input_files(ctx) if os.path.getsize(p) > 0]
    pp = _common.resolve_pay_period(ctx)
    cur = ctx.table("PAY_PERIOD", "HISTDBA").filter(F.col("CURR_PP_FLAG") == "Y").orderBy("PP_END_YEAR", "PP_NUM").limit(1).collect()
    cy, cn = (cur[0]["PP_END_YEAR"], cur[0]["PP_NUM"]) if cur else (None, None)
    if not files:
        raise RuntimeError("!!!! There are no FDA TATRAN files ready for processing")
    if len(files) > 1:
        raise RuntimeError(f"!!!! There are : {len(files)} FDA TATRAN files. There should only be 1.")
    name = os.path.basename(files[0])[:20]
    pos = name.rfind("P")
    in_name = name[pos + 1:pos + 4]
    current = (_num_str(cy) or "")[3:4] + (_num_str(cn) or "").rjust(2, "0")
    if in_name != current:
        raise RuntimeError(f"!!!! The file named: {name} does not match the current pay period. {_pp_text(cy, cn)}")
    _common.write_rows(ctx, [{"CPM_PAY_PERIOD": pp.cpm_pay_period}], _common.PAY_PERIOD_FILE, "cpm_fda_pay_period_file.txt")
    return None


def s_0020_PM_FDA_Set_CPM_Calendar(ctx: RunContext) -> dict:
    """Set_CPM_Calendar with ``$$MAP_PP_NUM := LPAD(TO_CHAR(v_PP_NUM), 2, '0')``."""
    pp = _common.resolve_pay_period(ctx)
    _common.validate(pp)
    _common.write_rows(ctx, [{"CPM_PAY_PERIOD": pp.cpm_pay_period}], _common.PAY_PERIOD_FILE, "cpm_fda_cpm_pay_period_file.txt")
    num2 = pp.num_str.rjust(2, "0")[:2] if pp.num_str is not None else None
    return {
        "WF_PP_END_YEAR": pp.end_year_str if pp.end_year_str is not None else ctx.param("WF_PP_END_YEAR", ""),
        "WF_PP_NUM": num2 if num2 is not None else ctx.param("WF_PP_NUM", ""),
    }


def s_0025_PM_FDA_Set_Pay_Calendar(ctx: RunContext) -> None:
    """lkp_CPM_NEWPAY_TBL (pay period, MP_POOL_DES = ' '); ABORT when there are no CPM records."""
    return _common.set_pay_calendar(ctx, "cpm_fda_pay_period_file.txt", F.lit(True), "CPM")


def s_0050_PM_FDA_Update_CPM_CYCLE_TBL_FDA(ctx: RunContext) -> dict:
    return update_cycle(ctx, "FDA")


def s_0100_PM_FDA_Load_TATRAN_To_DB(ctx: RunContext) -> None:
    """fil_Filter_Out_01_99 -> exp_Add_FDA_SEQ (batch id +1 on each TK||EMP||YEAR||NUM change, FDA_SEQ running
    1..n in file order) -> srt_Sort_By_BATCH_SEQ -> HI_PM_FDA_TATRAN_TBL (truncate + insert)."""
    files = _input_files(ctx)
    if files:
        raw = ctx.spark.read.text(files).withColumn("_file", F.input_file_name()).withColumn("_pos", F.monotonically_increasing_id())
        cols, pos = [], 1
        for f in TATRAN_FILE:
            cols.append(F.substring("value", pos, f.length).alias(f.name))
            pos += f.length
        src = raw.select("_file", "_pos", *cols)
    else:
        src = ctx.spark.createDataFrame([], "_file string, _pos bigint, " + ", ".join(f"{f.name} string" for f in TATRAN_FILE))
    rows = src.filter(~F.col("FDA_REC_TYPE").isin("01", "99") | F.col("FDA_REC_TYPE").isNull())
    w = Window.orderBy("_file", "_pos")
    key = F.concat_ws("", "FDA_TK_NO", "FDA_EMP_ID", "FDA_PP_YEAR", "FDA_PP_NUM")
    prev = F.lag(key).over(w)
    rows = rows.withColumn("_new", F.when(prev.isNull() | (key != prev), 1).otherwise(0))
    out = rows.select(
        F.sum("_new").over(w.rowsBetween(Window.unboundedPreceding, 0)).cast("decimal(19,0)").alias("FDA_BATCH_ID"),
        "FDA_TK_NO", "FDA_EMP_ID", "FDA_PP_YEAR", "FDA_PP_NUM", "FDA_REC_TYPE",
        F.row_number().over(w).cast("string").alias("_seq_str"),
        "FDA_DATA",
    )
    out = out.orderBy("FDA_BATCH_ID", "FDA_TK_NO", "FDA_EMP_ID", "FDA_PP_YEAR", "FDA_PP_NUM", "FDA_REC_TYPE", "_seq_str")
    out = out.select(*TATRAN_COLS[:6], F.col("_seq_str").cast("decimal(19,0)").alias("FDA_SEQ"), "FDA_DATA")
    delta.overwrite(ctx, out, ctx.table_name("HI_PM_FDA_TATRAN_TBL", OWNER))


ERROR_LOOKUPS = (
    # (lookup table, employee column, ERR_DESC, Lookup Source Filter on the $$MAP pay-period year)
    ("CPM_PAD_DETAIL_STG_TBL", "PAD_SOC_SEC_NO", "DOES NOT HAVE A PAD RECORD (P6331X01)", False),
    ("CPM_YTD_DETAIL_STG_TBL", "DYD_SSN_1", "DOES NOT HAVE A YTD RECORD (P6791X01)", False),
    ("CPM_NEWPAY_TBL", "DFAS_PSEUDO_SSN", "DOES NOT HAVE A PAYMASTER RECORD", True),
    ("CPM_MER_DETAIL_STG_TBL", "MER_SSN", "DOES NOT HAVE A MER RECORD (P6722D01)", False),
)


def s_0150_PM_FDA_Error_Counter(ctx: RunContext) -> None:
    """For each ``FDA_REC_TYPE = '02'`` TATRAN row, look up PAD / YTD / CPM (NEWPAY) / MER on employee + ``$$MAP``
    pay period; fil_Errors_* keep the misses -> four ERROR_TBL instances (append)."""
    y, n = _params_pp(ctx)
    src = _tatran(ctx).filter(F.col("FDA_REC_TYPE") == "02").select("FDA_EMP_ID")
    outs = []
    for table, emp_col, desc, year_filter in ERROR_LOOKUPS:
        lkp = ctx.table(table, OWNER)
        if year_filter:
            lkp = lkp.filter(F.col("PP_END_YEAR") == F.lit(y))
        keys = lkp.filter((F.col("PP_END_YEAR") == F.lit(y)) & (F.col("PP_NUM") == F.lit(n))).select(F.col(emp_col).alias("_emp")).distinct()
        miss = src.join(keys, src["FDA_EMP_ID"] == keys["_emp"], "left_anti")
        outs.append(miss.select(
            F.lit(ERROR_PROCESS).alias("PROCESS_NAME"), F.lit(desc).alias("ERROR_MESSAGE"),
            F.col("FDA_EMP_ID").substr(1, 9).alias("SOURCE_KEY"), F.current_timestamp().alias("ERROR_DATE"),
            F.lit(y).cast("decimal(4,0)").alias("PP_END_YEAR"), F.lit(n).cast("decimal(2,0)").alias("PP_NUM"),
            F.lit(ctx.param("WF_CYCLE_ID", "")).cast("decimal(3,0)").alias("CYCLE_ID"), F.lit(None).cast("string").alias("ERROR_CODE"),
        ))
    delta.append(ctx, reduce(DataFrame.unionByName, outs), ctx.table_name("ERROR_TBL", OWNER))


# lookup instance -> (table, lookup source filter, condition columns (lookup col, input col))
LOOKUPS_200 = {
    "lkp_CPM_MER_DETAIL_STG_TBL": ("CPM_MER_DETAIL_STG_TBL", "SUBSTR(MER_ORG, 1, 1) = 'D'", [("PP_END_YEAR", "FDA_PP_YEAR"), ("PP_NUM", "FDA_PP_NUM"), ("MER_SSN", "FDA_EMP_ID")]),
    "lkp_CPM_PAD_DETAIL_STG_TBL": ("CPM_PAD_DETAIL_STG_TBL", "SUBSTR(PAD_ORGANIZATION, 1, 1) = 'D'", [("PP_END_YEAR", "_PP_END_YEAR"), ("PP_NUM", "_PP_NUM"), ("PAD_SOC_SEC_NO", "FDA_EMP_ID")]),
    "lkp_CPM_YTD_DETAIL_STG_TBL": ("CPM_YTD_DETAIL_STG_TBL", "SUBSTR(DYD_ACTIVITY, 1, 1) = 'D'", [("PP_END_YEAR", "_PP_END_YEAR"), ("PP_NUM", "_PP_NUM"), ("DYD_SSN_1", "FDA_EMP_ID")]),
    "lkp_CPM_NEWPAY_TBL": ("CPM_NEWPAY_TBL", "PP_END_YEAR = {year}", [("PP_END_YEAR", "_PP_END_YEAR"), ("PP_NUM", "_PP_NUM"), ("DFAS_PSEUDO_SSN", "FDA_EMP_ID")]),
}
# fil_Filter_Out_NULL drops a row only when every lookup missed (return port per lookup).
FOUND_200 = {"lkp_CPM_MER_DETAIL_STG_TBL": "MER_SSN", "lkp_CPM_PAD_DETAIL_STG_TBL": "PAD_SOC_SEC_NO", "lkp_CPM_YTD_DETAIL_STG_TBL": "DYD_SSN_1", "lkp_CPM_NEWPAY_TBL": "SOC_SEC_NO"}


def _ports_columns() -> dict[str, list[str]]:
    used: dict[str, set] = {k: {v} for k, v in FOUND_200.items()}
    for sql in PORTS_200.values():
        for inst, col in re.findall(r"`(lkp_\w+?)__(\w+)`", sql):
            used.setdefault(inst, set()).add(col)
    return {k: sorted(v) for k, v in used.items()}


def s_0200_PM_FDA_Create_200_Rows(ctx: RunContext) -> None:
    """SQ (``FDA_REC_TYPE = '02'``) -> MER/PAD/YTD/NEWPAY lookups (*Use First Value*) -> fil_Filter_Out_NULL ->
    exp_Format_200_Records (``FDA_2xx_DATA`` from the generated spec, ``FDA_2xx_SEQ = 10010xx``, REC_TYPE '12')
    -> lkp_PSEUDOSSN (FDA_EMP_ID := SSN) -> nrm_Normalize_200_Records (14 occurrences) -> HI_PM_FDA_TATRAN_TBL (append)."""
    y, n = _params_pp(ctx)
    df = _tatran(ctx).filter(F.col("FDA_REC_TYPE") == "02").withColumn("_PP_END_YEAR", F.lit(y).cast("decimal(4,0)")).withColumn("_PP_NUM", F.lit(n).cast("decimal(2,0)"))
    for inst, cols in _ports_columns().items():
        table, src_filter, cond = LOOKUPS_200[inst]
        lk_cols = [c for c, _ in cond]
        lkp = ctx.table(table, OWNER).filter(F.expr(src_filter.format(year=y)))
        lkp = _first(lkp.select(*dict.fromkeys(lk_cols + cols)), lk_cols, lk_cols + cols)
        lkp = lkp.select(*[F.col(c).alias(f"{inst}__{c}") for c in dict.fromkeys(lk_cols + cols)])
        on = reduce(lambda a, b: a & b, [lkp[f"{inst}__{c}"] == df[i] for c, i in cond])
        df = df.join(lkp, on, "left").drop(*[f"{inst}__{c}" for c in lk_cols if c not in cols])
    df = df.filter(~reduce(lambda a, b: a & b, [F.col(f"{i}__{c}").isNull() for i, c in FOUND_200.items()]))
    ps = _first(ctx.table("PSEUDOSSN_TBL", OWNER).select("PSEUDOSSN", "SSN"), ["PSEUDOSSN"], ["PSEUDOSSN", "SSN"])
    df = df.join(ps, df["FDA_EMP_ID"] == ps["PSEUDOSSN"], "left")
    occ = F.array(*[F.struct(F.expr(PORTS_200[f"exp_Format_200_Records.FDA_{r}_DATA"]).alias("d"), F.lit(1001000 + r - 200).alias("s")) for r in REC_200])
    out = df.select("FDA_BATCH_ID", "FDA_TK_NO", F.col("SSN").alias("FDA_EMP_ID"), "FDA_PP_YEAR", "FDA_PP_NUM", F.explode(occ).alias("o")).select(
        "FDA_BATCH_ID", "FDA_TK_NO", "FDA_EMP_ID", "FDA_PP_YEAR", "FDA_PP_NUM", F.lit("12").alias("FDA_REC_TYPE"),
        F.col("o.s").cast("decimal(19,0)").alias("FDA_SEQ"), F.col("o.d").substr(1, 200).alias("FDA_DATA"),
    )
    delta.append(ctx, out, ctx.table_name("HI_PM_FDA_TATRAN_TBL", OWNER))


def s_0300_PM_FDA_Create_Output_File(ctx: RunContext) -> None:
    """SQ ORDER BY batch/tk/emp/year/num/rec type/seq; minutes ``.25|/.50|/.75|`` -> ``.15|/.30|/.45|`` except 215 rows."""
    t = _tatran(ctx).orderBy("FDA_BATCH_ID", "FDA_TK_NO", "FDA_EMP_ID", "FDA_PP_YEAR", "FDA_PP_NUM", "FDA_REC_TYPE", "FDA_SEQ")
    d = F.col("FDA_DATA")
    conv = F.replace(F.replace(F.replace(d, F.lit(".25|"), F.lit(".15|")), F.lit(".50|"), F.lit(".30|")), F.lit(".75|"), F.lit(".45|"))
    out = t.select("FDA_TK_NO", "FDA_EMP_ID", "FDA_PP_YEAR", "FDA_PP_NUM", "FDA_REC_TYPE",
                   F.when(F.substring(d, 1, 3) != "215", conv).otherwise(d).substr(1, 114).alias("FDA_DATA"))
    write_fixed_width(out, _common.out_file(ctx, ctx.param("FDA_12200_out_file", "fda_12200.txt")), TATRAN_OUT)


def _errors(ctx: RunContext) -> DataFrame:
    """SQ_ERROR_TBL override: rows of the latest 0150 run today for the pay period and ``$$MAP_CYCLE_ID``."""
    y, n = _params_pp(ctx)
    e = ctx.table("ERROR_TBL", OWNER).filter((F.col("PP_END_YEAR") == F.lit(y)) & (F.col("PP_NUM") == F.lit(n)) & (F.col("PROCESS_NAME") == ERROR_PROCESS))
    b = (
        e.filter((F.col("ERROR_DATE") >= F.date_trunc("DD", F.current_timestamp())) & (F.col("CYCLE_ID") == F.lit(ctx.param("WF_CYCLE_ID", "")).cast("decimal(3,0)")))
        .groupBy("PP_END_YEAR", "PP_NUM").agg(F.max("ERROR_DATE").alias("b_DATE"), F.max("CYCLE_ID").alias("b_CYCLE"))
    )
    return e.join(b, ["PP_END_YEAR", "PP_NUM"]).filter((F.col("ERROR_DATE") == F.col("b_DATE")) & (F.col("CYCLE_ID") == F.col("b_CYCLE")))


def s_0500_PM_FDA_IO_Counter(ctx: RunContext) -> dict:
    """agg_All_Leave_Recs (REC_TYPE '02') and agg_Error_Records joined on a constant (jnr_All_Counts) + count lookups
    -> nrm_Counters -> COUNTER_TBL (4 rows) and nrm_Counters_Message -> fda_extract_message_file.txt.

    The Aggregators have no group-by, so (as in PowerCenter) an empty input yields no row and the normal join then
    produces nothing: no counters, no message file rows and ``$$MAP_SUBJECT`` keeps its value."""
    y, n = _params_pp(ctx)
    t = _tatran(ctx)
    leave = t.filter(F.col("FDA_REC_TYPE") == "02").count()
    errors = _errors(ctx).count()
    keep = {"WF_SUBJECT": ctx.param("WF_SUBJECT", ""), "WF_MESSAGE": ctx.param("WF_MESSAGE", "")}
    if leave == 0 or errors == 0:
        write_fixed_width(ctx.spark.createDataFrame([], "COUNT_DESC string, COUNT_VALUE string"), _common.out_file(ctx, "fda_extract_message_file.txt"), MESSAGE_FILE)
        return keep
    read = t.filter(F.col("FDA_SEQ") < 1000000).count()
    written = t.count()
    cur = ctx.table("PAY_PERIOD", "HISTDBA").filter(F.col("CURR_PP_FLAG") == "Y").orderBy("PP_END_YEAR", "PP_NUM").limit(1).collect()
    pp = _pp_text(cur[0]["PP_END_YEAR"], cur[0]["PP_NUM"]) if cur else "-"
    counts = [("TATRAN Records Read", read), ("Leave Records Read", leave), ("Error Records", errors), ("New TATRAN Records Written", written)]
    msg = [(f"FDA Leave Counts for Pay Period: {pp}", None), (" ", None), *counts]
    data = [{"COUNT_DESC": d, "COUNT_VALUE": None if v is None else str(v).rjust(10)} for d, v in msg]
    write_fixed_width(ctx.spark.createDataFrame(data, "COUNT_DESC string, COUNT_VALUE string"), _common.out_file(ctx, "fda_extract_message_file.txt"), MESSAGE_FILE)
    ctr = ctx.spark.createDataFrame([(d, v) for d, v in counts], "COUNTER_DESCRIPTION string, COUNTER_VALUE bigint").select(
        F.current_timestamp().alias("RUN_DATE"), F.lit("FDA EXTRACT").alias("PROCESS_NAME"), "COUNTER_DESCRIPTION",
        F.col("COUNTER_VALUE").cast("decimal(15,0)").alias("COUNTER_VALUE"),
        F.lit(y).cast("decimal(4,0)").alias("PP_END_YEAR"), F.lit(n).cast("decimal(2,0)").alias("PP_NUM"), F.lit(None).cast("decimal(1,0)").alias("CYCLE_ID"),
    )
    delta.append(ctx, ctr, ctx.table_name("COUNTER_TBL", OWNER))
    subject = f"{_common.environment_prefix(ctx)}FDA Extract completed successfully for Pay Period:  {pp}"
    return {**keep, "WF_SUBJECT": subject}


def s_1100_PM_FDA_Send_Email(ctx: RunContext) -> None:
    """Placeholder mapping (HI_GENERIC_SRC_TBL -> generic_target_file1.out); the copy/transfer/archive work is in the
    post-session commands listed in the module docstring."""
    _common.generic_file(ctx, "generic_target_file1.out")


TASKS = {
    "s_0010_PM_FDA_Verify_File": s_0010_PM_FDA_Verify_File,
    "s_0020_PM_FDA_Set_CPM_Calendar": s_0020_PM_FDA_Set_CPM_Calendar,
    "s_0025_PM_FDA_Set_Pay_Calendar": s_0025_PM_FDA_Set_Pay_Calendar,
    "s_0050_PM_FDA_Update_CPM_CYCLE_TBL_FDA": s_0050_PM_FDA_Update_CPM_CYCLE_TBL_FDA,
    "s_0100_PM_FDA_Load_TATRAN_To_DB": s_0100_PM_FDA_Load_TATRAN_To_DB,
    "s_0150_PM_FDA_Error_Counter": s_0150_PM_FDA_Error_Counter,
    "s_0200_PM_FDA_Create_200_Rows": s_0200_PM_FDA_Create_200_Rows,
    "s_0300_PM_FDA_Create_Output_File": s_0300_PM_FDA_Create_Output_File,
    "s_0500_PM_FDA_IO_Counter": s_0500_PM_FDA_IO_Counter,
    "s_1100_PM_FDA_Send_Email": s_1100_PM_FDA_Send_Email,
}
