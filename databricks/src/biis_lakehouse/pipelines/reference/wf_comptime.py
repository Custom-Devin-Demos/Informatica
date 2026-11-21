"""wf_COMPTIME (folder COMP_TIME): load the daily comp-time file into COMP_TIME_DAILY_TBL.

Workflow links (all on ``Status = Succeeded``):
Start -> s_COMPTIME_Current_Pay_Period -> s_COMPTIME_Load_COMP_TIME_DAILY_TBL
      -> s_COMPTIME_Build_Message_Counters -> email_COMPTIME_Complete

Source file ``$Param_Root_Directory/data/int/in/COMPTIME/$Param_COMPTIME_filename`` -> ``ctx.path("in", "COMPTIME",
ctx.param("COMPTIME_filename"))``. Post-session success command of s_COMPTIME_Build_Message_Counters (orchestration
unit): ``mv .../in/COMPTIME/$Param_COMPTIME_filename .../archive/COMPTIME/u0827d01_P$$WF_PP_YEAR_NUM.txt``.
"""
from __future__ import annotations

from pyspark.sql import functions as F

from biis_lakehouse.common import delta
from biis_lakehouse.common.fixed_width import Field
from biis_lakehouse.context import RunContext
from biis_lakehouse.pipelines.reference import _pc

AREA = "COMPTIME"
SOURCE_FIELDS = [
    "SSN", "NAME", "CURRENT_ACCT", "CURRENT_ORG", "FLSA_STATUS", "COMP_TIME_CUR_BAL", "COMP_TIME_YEAR_EARNED",
    "PP_END_DATE", "DAILY_DATE_EARNED", "COMP_TIME_RATE", "COMP_TIME_HOURS", "COMP_TIME_UNDEF",
]
DATE_FILE = [Field("PAY_PERIOD", 6)]
MESSAGE_FILE = [Field("SUBJECT", 100), Field("MESSAGE", 300)]
COUNTER_DDL = _pc.COUNTER_DDL


def _source(ctx: RunContext):
    """U0287D01: comma-delimited, double-quoted (FLATFILE DELIMITED=YES)."""
    return _pc.read_delimited(ctx, ctx.path("in", AREA, ctx.param("COMPTIME_filename")), SOURCE_FIELDS)


def _current_pp(ctx: RunContext):
    """lkp_PAY_PERIOD: ``CURR_PP_FLAG = in_CURR_PP_FLAG`` with in = 'Y', Use Any Value."""
    pp = ctx.table("PAY_PERIOD", "HISTDBA").filter("CURR_PP_FLAG = 'Y'").select("PP_NUM", "PP_END_YEAR")
    return _pc.any_value(pp, [])


def s_COMPTIME_Current_Pay_Period(ctx: RunContext) -> dict:
    """m_COMPTIME_Current_Pay_Period: SQ filter ``CURR_PP_FLAG = 'Y'``; exp_Build_Pay_Period
    ``TO_CHAR(PP_END_YEAR) || v_PP_NUM`` -> COMP_TIME_DATE_FILE (PAY_PERIOD 6);
    SETVARIABLE $$MAP_PP_YEAR_NUM / $$MAP_PP_END_YEAR / $$MAP_PP_NUM (MAX) -> $$WF_PP_YEAR_NUM / _END_YEAR / _NUM."""
    pp = ctx.table("PAY_PERIOD", "HISTDBA").filter("CURR_PP_FLAG = 'Y'")
    out = pp.select(
        _pc.pc_concat(F.col("PP_END_YEAR").cast("int").cast("string"), _pc.pp2(F.col("PP_NUM"))).alias("PAY_PERIOD"),
        F.col("PP_END_YEAR").cast("int").alias("PP_END_YEAR"),
        F.col("PP_NUM").cast("int").alias("PP_NUM"),
    )
    _pc.write_flat(out.select("PAY_PERIOD"), ctx.path("out", AREA, "comp_time_date_file.txt"), DATE_FILE)
    row = out.agg(F.max(F.substring("PAY_PERIOD", 1, 6)).alias("y"), F.max("PP_END_YEAR").alias("e"), F.max("PP_NUM").alias("n")).first()
    return {k: v for k, v in {"WF_PP_YEAR_NUM": row.y, "WF_PP_END_YEAR": row.e, "WF_PP_NUM": row.n}.items() if v is not None}


def s_COMPTIME_Load_COMP_TIME_DAILY_TBL(ctx: RunContext) -> None:
    """m_COMPTIME_Load_COMP_TIME_DAILY_TBL.

    exp_Initial ``o_VALID_RECORD_FLAG = DECODE(TRUE, IS_NUMBER(SSN), 1, 0)``; fil_Valid_Records
    ``VALID_RECORD_FLAG = TRUE``; lkp_PAY_PERIOD (current pay period, Use Any Value); exp_Convert
    ``IIF(IS_DATE(x,'YYYYMMDD'), TO_DATE(x,'YYYYMMDD'))`` for PP_END_DATE / DAILY_DATE_EARNED,
    ``PP_YEAR_NUM = TO_DECIMAL(TO_CHAR(PP_END_YEAR) || LPAD(TO_CHAR(PP_NUM), 2, '0'))``.
    Target COMP_TIME_DAILY_TBL: normal insert (no truncate).
    """
    src = _source(ctx)
    valid = src.filter(_pc.is_number(F.col("SSN")))
    lkp = _current_pp(ctx).select(F.col("PP_NUM").alias("lkp_PP_NUM"), F.col("PP_END_YEAR").alias("lkp_PP_END_YEAR"))
    df = valid.join(lkp, how="left")

    def dec(c, p, s):
        return F.trim(F.col(c)).cast(f"decimal({p},{s})")

    out = df.select(
        F.col("lkp_PP_END_YEAR").cast("decimal(4,0)").alias("PP_END_YEAR"),
        F.col("lkp_PP_NUM").cast("decimal(2,0)").alias("PP_NUM"),
        F.concat(F.col("lkp_PP_END_YEAR").cast("int").cast("string"), F.lpad(F.col("lkp_PP_NUM").cast("int").cast("string"), 2, "0"))
        .cast("decimal(6,0)")
        .alias("PP_YEAR_NUM"),
        "SSN", "NAME", "CURRENT_ACCT", "CURRENT_ORG", "FLSA_STATUS",
        dec("COMP_TIME_CUR_BAL", 8, 2).alias("COMP_TIME_CUR_BAL"),
        dec("COMP_TIME_YEAR_EARNED", 4, 0).alias("COMP_TIME_YEAR_EARNED"),
        F.to_timestamp(_pc.to_date(F.col("PP_END_DATE"), "yyyyMMdd")).alias("PP_END_DATE"),
        F.to_timestamp(_pc.to_date(F.col("DAILY_DATE_EARNED"), "yyyyMMdd")).alias("DAILY_DATE_EARNED"),
        dec("COMP_TIME_RATE", 6, 2).alias("COMP_TIME_RATE"),
        dec("COMP_TIME_HOURS", 8, 2).alias("COMP_TIME_HOURS"),
        dec("COMP_TIME_UNDEF", 6, 0).alias("COMP_TIME_UNDEF"),
    )
    delta.append(ctx, out, ctx.table_name("COMP_TIME_DAILY_TBL"))


def s_COMPTIME_Build_Message_Counters(ctx: RunContext) -> dict:
    """m_COMPTIME_Build_Message_Counters.

    exp_Initial ``RECORD_TYPE_FLAG = DECODE(TRUE, IS_NUMBER(SSN), 'D', 'NO')``; fil_Detail ``RECORD_TYPE_FLAG = 'D'``;
    agg_ALL_RECORDS (no group-by) ``COUNT(SSN)``; lkp_PAY_PERIOD current period; exp_Final -> COUNTER_TBL
    (RUN_DATE = SESSSTARTTIME, PROCESS_NAME = $PMMappingName, PP_* not connected); exp_Build_Message ->
    COMPTIME_MESSAGE_FILE and SETVARIABLE $$MAP_SUBJECT/$$MAP_MESSAGE -> $$WF_SUBJECT/$$WF_MESSAGE.
    An Aggregator without group-by emits no row when it receives no rows, so an empty/all-invalid file
    writes no counter and leaves the workflow variables unchanged.
    """
    detail = _source(ctx).filter(_pc.is_number(F.col("SSN")))
    tbl = ctx.table_name("COUNTER_TBL")
    _pc.ensure_table(ctx, tbl, COUNTER_DDL)
    if not detail.limit(1).count():
        _pc.write_flat(ctx.spark.createDataFrame([], "SUBJECT string, MESSAGE string"),
                       ctx.path("out", AREA, "comptime_message_file.txt"), MESSAGE_FILE)
        return {}
    agg = detail.agg(F.count("SSN").alias("DETAIL_RECORD_COUNT"))
    df = agg.join(_current_pp(ctx), how="left")
    desc = "Number of detail records from the COMP TIME file."

    counters = df.select(
        _pc.session_start(ctx).alias("RUN_DATE"),
        F.lit("m_COMPTIME_Build_Message_Counters").alias("PROCESS_NAME"),
        F.lit(desc).alias("COUNTER_DESCRIPTION"),
        F.col("DETAIL_RECORD_COUNT").cast("decimal(15,0)").alias("COUNTER_VALUE"),
        F.lit(None).cast("decimal(4,0)").alias("PP_END_YEAR"),
        F.lit(None).cast("decimal(2,0)").alias("PP_NUM"),
        F.lit(None).cast("decimal(1,0)").alias("CYCLE_ID"),
    )
    delta.append(ctx, counters, tbl)

    msg = df.select(
        _pc.pc_concat(
            _pc.environment(ctx), F.lit("Comp Time File loaded successfully for Pay Period:  "),
            F.col("PP_END_YEAR").cast("int").cast("string"), F.lit("-"), _pc.pp2(F.col("PP_NUM")),
        ).substr(1, 100).alias("SUBJECT"),
        _pc.pc_concat(F.lit("Number of Detail Records from Comp Time file\t= "), F.col("DETAIL_RECORD_COUNT").cast("string"))
        .substr(1, 300)
        .alias("MESSAGE"),
    )
    _pc.write_flat(msg, ctx.path("out", AREA, "comptime_message_file.txt"), MESSAGE_FILE)
    row = msg.first()
    return {k: v for k, v in {"WF_SUBJECT": row.SUBJECT, "WF_MESSAGE": row.MESSAGE}.items() if v is not None}


def email_COMPTIME_Complete(ctx: RunContext) -> dict:
    """Email task: To ``$$WF_COMPTIME_EMAIL_LIST``, Subject ``$$WF_SUBJECT``, Text ``$$WF_MESSAGE``."""
    return {
        "EMAIL_TO": ctx.params.get("WF_COMPTIME_EMAIL_LIST", ""),
        "EMAIL_SUBJECT": ctx.params.get("WF_SUBJECT", ""),
        "EMAIL_TEXT": ctx.params.get("WF_MESSAGE", ""),
    }


TASKS = {
    "s_COMPTIME_Current_Pay_Period": s_COMPTIME_Current_Pay_Period,
    "s_COMPTIME_Load_COMP_TIME_DAILY_TBL": s_COMPTIME_Load_COMP_TIME_DAILY_TBL,
    "s_COMPTIME_Build_Message_Counters": s_COMPTIME_Build_Message_Counters,
    "email_COMPTIME_Complete": email_COMPTIME_Complete,
}
