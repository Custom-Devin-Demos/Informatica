"""wf_Pay_Calendar (folder Pay_Calendar): reset and set the current pay period in HISTDBA.PAY_PERIOD.

Workflow links (all on ``Status = Succeeded``):
Start -> s_Pay_Calendar_Reset_Pay_Calendar -> s_Pay_Calendar_Set_Pay_Calendar
      -> s_Pay_Calendar_Verify_Pay_Calendar -> s_Pay_Calendar_Build_Message -> Email_Pay_Calendar

Parameters: ``WF_PP_END_YEAR`` / ``WF_PP_NUM`` (optional override of the pay period to make current),
``PMRepositoryServiceName`` (subject prefix), ``SESSSTARTTIME`` (optional, defaults to now),
``WF_PAY_CALENDAR_EMAIL_LIST``. No pre/post session shell commands in this workflow.
"""
from __future__ import annotations

from pyspark.sql import functions as F

from biis_lakehouse.common.fixed_width import Field
from biis_lakehouse.context import RunContext
from biis_lakehouse.pipelines.reference import _pc

AREA = "PAY_CALENDAR"
KEYS = ["PP_NUM", "PP_END_YEAR"]
VERIFY_FILE = [Field("SUBJECT", 100)]
MESSAGE_FILE = [Field("SUBJECT", 100), Field("MESSAGE", 300)]


def _pay_period(ctx: RunContext) -> str:
    return ctx.table_name("PAY_PERIOD", "HISTDBA")


def s_Pay_Calendar_Reset_Pay_Calendar(ctx: RunContext) -> None:
    """m_Pay_Calendar_Reset_Pay_Calendar: SQ filter ``PAY_PERIOD.CURR_PP_FLAG = 'Y'``;
    exp_Initial ``o_CURR_PP_FLAG = NULL``; upd_Reset_Current_PP ``DD_UPDATE`` -> PAY_PERIOD."""
    tbl = _pay_period(ctx)
    df = (
        ctx.spark.table(tbl)
        .filter("CURR_PP_FLAG = 'Y'")
        .select(*KEYS, F.lit(None).cast("string").alias("CURR_PP_FLAG"))
    )
    _pc.update_rows(ctx, df, tbl, KEYS)


def s_Pay_Calendar_Set_Pay_Calendar(ctx: RunContext) -> dict:
    """m_Pay_Calendar_Set_Pay_Calendar.

    SQ: ``SELECT MAX(PP_NUM) PP_NUM, MAX(PP_END_YEAR) PP_END_YEAR FROM PAY_PERIOD`` (one row).
    exp_Initial: ``$$PP_END_YEAR``/``$$PP_NUM`` (pre-session := ``$$WF_PP_END_YEAR``/``$$WF_PP_NUM``),
    ``IIF(NOT IS_NUMBER(x), 0, TO_DECIMAL(x))``.
    lkp_Existing_Pay_Period (Use Any Value): ``PP_NUM = in_PARAM_PP_NUM AND PP_END_YEAR = in_PARAM_PP_END_YEAR``.
    exp_Determine_Parameters_Exist: ``PARAM_EXISTS_FLAG = NOT ISNULL(lkp_PP_NUM)``.
    rtr_Parameter_Non_Parameter:
      PARAMETERS_EXIST (``PARAM_EXISTS_FLAG = TRUE``) -> PP_NUM/PP_END_YEAR := parameter values, flag 'Y'
          -> upd_Set_Current_PP_Param (DD_UPDATE) -> PAY_PERIOD_PARAM;
      PARAMETERS_NOT_EXIST (``PARAM_EXISTS_FLAG = FALSE``) -> exp_Set_Date ``TRUNC(SESSSTARTTIME)``
          -> lkp_New_Current_Pay_Period (Use Any Value) ``PP_START_DTE <= d AND PP_END_DTE >= d`` -> flag 'Y'
          -> upd_Set_Current_PP_Non_Param (DD_UPDATE) -> PAY_PERIOD. DEFAULT1 is unconnected.
    Post-session: ``$$WF_PP_END_YEAR := $$PP_END_YEAR``, ``$$WF_PP_NUM := $$PP_NUM`` (unchanged pass-through).
    """
    spark, tbl = ctx.spark, _pay_period(ctx)
    pp = spark.table(tbl)
    p_year, p_num = ctx.params.get("WF_PP_END_YEAR", ""), ctx.params.get("WF_PP_NUM", "")

    def param(v: str):
        c = F.lit(v)
        return F.when(~_pc.is_number(c), F.lit(0)).otherwise(c.cast("decimal(28,0)")).cast("decimal(10,0)")

    sq = pp.agg(F.max("PP_NUM").alias("PP_NUM"), F.max("PP_END_YEAR").alias("PP_END_YEAR"))
    exp = sq.select(*KEYS, param(p_year).alias("PARAM_PP_END_YEAR"), param(p_num).alias("PARAM_PP_NUM"))
    lkp = _pc.any_value(pp.select("PP_NUM", "PP_END_YEAR"), KEYS).select(
        F.col("PP_NUM").alias("lkp_PP_NUM"), F.col("PP_END_YEAR").alias("lkp_PP_END_YEAR")
    )
    exp = exp.join(
        lkp,
        (F.col("lkp_PP_NUM") == F.col("PARAM_PP_NUM")) & (F.col("lkp_PP_END_YEAR") == F.col("PARAM_PP_END_YEAR")),
        "left",
    ).withColumn("PARAM_EXISTS_FLAG", F.col("lkp_PP_NUM").isNotNull())

    param_rows = exp.filter("PARAM_EXISTS_FLAG = TRUE").select(
        F.col("PARAM_PP_NUM").cast("decimal(2,0)").alias("PP_NUM"),
        F.col("PARAM_PP_END_YEAR").cast("decimal(4,0)").alias("PP_END_YEAR"),
        F.lit("Y").alias("CURR_PP_FLAG"),
    )
    non_param = exp.filter("PARAM_EXISTS_FLAG = FALSE").select(F.to_date(_pc.session_start(ctx)).alias("in_CURRENT_DATE"))
    # Every PAY_PERIOD row satisfies the lookup condition independently; Use Any Value keeps one match.
    new_current = (
        non_param.join(
            pp.select(*KEYS, "PP_START_DTE", "PP_END_DTE"),
            (F.col("PP_START_DTE") <= F.col("in_CURRENT_DATE")) & (F.col("PP_END_DTE") >= F.col("in_CURRENT_DATE")),
            "left",
        )
    )
    new_current = _pc.any_value(new_current, ["in_CURRENT_DATE"]).select(*KEYS, F.lit("Y").alias("CURR_PP_FLAG"))

    _pc.update_rows(ctx, param_rows, tbl, KEYS)
    _pc.update_rows(ctx, new_current, tbl, KEYS)
    return {"WF_PP_END_YEAR": p_year, "WF_PP_NUM": p_num}


def s_Pay_Calendar_Verify_Pay_Calendar(ctx: RunContext) -> None:
    """m_Pay_Calendar_Verify_Pay_Calendar.

    SQ ``SELECT MAX(PP_NUM), MAX(PP_END_YEAR) FROM PAY_PERIOD``; exp_Initial ``o_MIN_PP_NUM = 1``;
    lkp_Current_Pay_Period SQL override ``SELECT COUNT(*) COUNT_CURRENT FROM PAY_PERIOD WHERE CURR_PP_FLAG = 'Y'``
    with condition ``COUNT_CURRENT >= in_MIN_PP_NUM``; exp_Check_Current_Flag:
    count = 1 -> '!!!! Current flag has been properly set' (port precision 30, so truncated);
    NULL/0 -> ABORT('!!!! There is no pay period set.'); > 1 -> ABORT('!!!! There are more than one pay periods set to current.').
    Target PAY_PERIOD_VERIFY_FILE (SUBJECT 100, no header).
    """
    pp = ctx.spark.table(_pay_period(ctx))
    sq = pp.agg(F.max("PP_NUM").alias("PP_NUM"))
    cnt = pp.filter("CURR_PP_FLAG = 'Y'").agg(F.count(F.lit(1)).alias("COUNT_CURRENT")).filter("COUNT_CURRENT >= 1")
    df = sq.crossJoin(cnt.select(F.col("COUNT_CURRENT").alias("lkp_COUNT_CURRENT"))) if cnt.count() else sq.withColumn(
        "lkp_COUNT_CURRENT", F.lit(None).cast("long")
    )
    _pc.abort_if(df, "lkp_COUNT_CURRENT IS NULL OR lkp_COUNT_CURRENT = 0", "!!!! There is no pay period set.")
    _pc.abort_if(df, "lkp_COUNT_CURRENT > 1", "!!!! There are more than one pay periods set to current.")
    out = df.select(F.substring(F.lit("!!!! Current flag has been properly set"), 1, 30).alias("SUBJECT"))
    _pc.write_flat(out, ctx.path("out", AREA, "pay_period_verify_file.txt"), VERIFY_FILE)


def s_Pay_Calendar_Build_Message(ctx: RunContext) -> dict:
    """m_Pay_Calendar_Build_Message: SQ filter ``CURR_PP_FLAG = 'Y'``; exp_Initial builds subject/message and
    ``SETVARIABLE($$MAP_SUBJECT/$$MAP_MESSAGE)`` (MAX aggregation); post-session ``$$WF_SUBJECT/$$WF_MESSAGE``.
    Target PAY_PERIOD_MESSAGE_FILE (SUBJECT 100, MESSAGE 300, header = field names)."""
    pp = ctx.spark.table(_pay_period(ctx)).filter("CURR_PP_FLAG = 'Y'")
    v_pp = _pc.pp2(F.col("PP_NUM"))
    subject = _pc.pc_concat(
        _pc.environment(ctx),
        F.lit("Pay Calendar Process Completed Successfully for: "),
        F.col("PP_END_YEAR").cast("int").cast("string"),
        F.lit("-"),
        v_pp,
    ).substr(1, 100)
    message = _pc.pc_concat(
        F.lit("Current Pay Period = "), v_pp, F.lit("\n"),
        F.lit("Begin Date         = "), F.date_format("PP_START_DTE", "MM/dd/yyyy"), F.lit("\n"),
        F.lit("End Date           = "), F.date_format("PP_END_DTE", "MM/dd/yyyy"),
    ).substr(1, 300)
    out = pp.select(subject.alias("SUBJECT"), message.alias("MESSAGE"))
    _pc.write_flat(out, ctx.path("out", AREA, "pay_period_message_file.txt"), MESSAGE_FILE, header=True)
    row = out.agg(F.max("SUBJECT").alias("s"), F.max("MESSAGE").alias("m")).first()
    result = {}
    if row.s is not None:
        result["WF_SUBJECT"] = row.s
    if row.m is not None:
        result["WF_MESSAGE"] = row.m
    return result


def Email_Pay_Calendar(ctx: RunContext) -> dict:
    """Email task: To ``$$WF_PAY_CALENDAR_EMAIL_LIST``, Subject ``$$WF_SUBJECT``, Text ``$$WF_MESSAGE``.
    Sending is done by the job's notification step; this returns the rendered email."""
    return {
        "EMAIL_TO": ctx.params.get("WF_PAY_CALENDAR_EMAIL_LIST", ""),
        "EMAIL_SUBJECT": ctx.params.get("WF_SUBJECT", "0"),
        "EMAIL_TEXT": ctx.params.get("WF_MESSAGE", "0"),
    }


TASKS = {
    "s_Pay_Calendar_Reset_Pay_Calendar": s_Pay_Calendar_Reset_Pay_Calendar,
    "s_Pay_Calendar_Set_Pay_Calendar": s_Pay_Calendar_Set_Pay_Calendar,
    "s_Pay_Calendar_Verify_Pay_Calendar": s_Pay_Calendar_Verify_Pay_Calendar,
    "s_Pay_Calendar_Build_Message": s_Pay_Calendar_Build_Message,
    "Email_Pay_Calendar": Email_Pay_Calendar,
}
