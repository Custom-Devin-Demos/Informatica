"""Sessions shared (same mapping logic, different names/filters) by the CPM extract workflows.

``m_*_Set_CPM_Calendar`` / ``m_*_Set_Pay_Calendar`` resolve the pay period from ``$$WF_PP_END_YEAR``
/ ``$$WF_PP_NUM`` (blank -> current pay period), ``m_*_Build_Message`` builds the completion email
and ``m_*_Concatenate_Files`` / ``m_*_Send_Report`` are the dummy ``HI_GENERIC_SRC_TBL`` ->
``GENERIC_TARGET_FILE`` mappings whose real work is the session's post-command (orchestration unit).
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Optional

from pyspark.sql import DataFrame, functions as F

from biis_lakehouse.common.fixed_width import Field, write_fixed_width
from biis_lakehouse.context import RunContext

AREA = "CPM"
_IS_NUMBER = re.compile(r"^\s*[+-]?(\d+\.?\d*|\.\d+)([eE][+-]?\d+)?\s*$")

PAY_PERIOD_FILE = [Field("CPM_PAY_PERIOD", 10)]
GENERIC_TARGET_FILE = [Field("ID", 1, numeric=True)]


def is_number(v: Optional[str]) -> bool:
    return v is not None and bool(_IS_NUMBER.match(v))


def _blank(v: Optional[str]) -> bool:
    # ISNULL(x) OR IS_SPACES(x) OR LENGTH(LTRIM(x)) = 0
    return v is None or v.strip() == ""


def _dec(v: Optional[str]) -> Decimal:
    # IIF(NOT IS_NUMBER($$X), 0, TO_DECIMAL($$X))
    try:
        return Decimal(v.strip()) if is_number(v) else Decimal(0)
    except InvalidOperation:
        return Decimal(0)


def _num_str(v) -> Optional[str]:
    if v is None:
        return None
    return str(int(v)) if Decimal(v) == int(v) else str(v)


def out_file(ctx: RunContext, name: str) -> str:
    """``$PMTargetFileDir`` / ``$Param_Root_Directory/data/int/out/CPM`` target file."""
    return ctx.path("out", AREA, name)


def write_rows(ctx: RunContext, rows: list[dict], fields: list[Field], name: str) -> DataFrame:
    df = ctx.spark.createDataFrame(rows, ",".join(f"{f.name} string" for f in fields)) if rows else ctx.spark.createDataFrame([], ",".join(f"{f.name} string" for f in fields))
    write_fixed_width(df, out_file(ctx, name), fields)
    return df


def environment_prefix(ctx: RunContext) -> str:
    """``DECODE(SUBSTR($PMRepositoryServiceName, 1, 4), 'Dev_', 'Dev: ', 'Test', 'Test: ', 'Prod', 'Prod: ')``."""
    return {"Dev_": "Dev: ", "Test": "Test: ", "Prod": "Prod: "}.get(ctx.param("PMRepositoryServiceName", "")[:4], "")


@dataclass
class PayPeriod:
    init_end_year: Optional[str]
    init_num: Optional[str]
    end_year: Optional[Decimal]  # v_PP_END_YEAR
    num: Optional[Decimal]  # v_PP_NUM
    default_to_current: bool

    @property
    def end_year_str(self) -> Optional[str]:
        return _num_str(self.end_year)

    @property
    def num_str(self) -> Optional[str]:
        return _num_str(self.num)

    @property
    def year_num(self) -> Optional[str]:
        # TO_CHAR(v_PP_END_YEAR) || IIF(v_PP_NUM < 10, LPAD(TO_CHAR(v_PP_NUM), 2, '0'), TO_CHAR(v_PP_NUM))
        if self.end_year is None and self.num is None:
            return None
        return (self.end_year_str or "") + (self.num_str or "").rjust(2, "0")

    @property
    def cpm_pay_period(self) -> str:
        # exp_Final.o_CPM_PAY_PERIOD := PARAM_PP_END_YEAR || '-' || LPAD(PARAM_PP_NUM, 2, '0')
        num = self.num_str.rjust(2, "0")[:2] if self.num_str is not None else ""
        return (self.end_year_str or "") + "-" + num


def resolve_pay_period(ctx: RunContext) -> PayPeriod:
    """exp_Initial -> lkp_Existing_Pay_Period / lkp_Current_Pay_Period -> exp_Set_Parameters.

    ``SQ_PAY_PERIOD`` (``SELECT MAX(PP_NUM), MAX(PP_END_YEAR) FROM PAY_PERIOD``) always returns one
    row and only drives the single evaluation, so it is not read. Both lookups are cached
    PAY_PERIOD lookups with *Use Any Value*: the first matching row (by key order) is used.
    """
    init_year = ctx.param("WF_PP_END_YEAR", "")
    init_num = ctx.param("WF_PP_NUM", "")
    pp = ctx.table("PAY_PERIOD", "HISTDBA")
    p_year, p_num = _dec(init_year), _dec(init_num)
    existing = pp.filter((F.col("PP_NUM") == F.lit(p_num)) & (F.col("PP_END_YEAR") == F.lit(p_year))).select("PP_NUM").limit(1).collect()
    current = pp.filter(F.col("CURR_PP_FLAG") == "Y").orderBy("PP_END_YEAR", "PP_NUM").select("PP_NUM", "PP_END_YEAR").limit(1).collect()
    default = _blank(init_year) and _blank(init_num)
    if default:
        year = current[0]["PP_END_YEAR"] if current else None
        num = current[0]["PP_NUM"] if current else None
    elif existing:
        year, num = p_year, p_num
    else:
        year = num = None
    return PayPeriod(init_year, init_num, None if year is None else Decimal(year), None if num is None else Decimal(num), default)


def validate(pp: PayPeriod, no_records: bool = False, label: str = "") -> None:
    """exp_Validate_Parameters: ``ABORT`` -> raise, in the DECODE order of the mapping."""
    abort = (not is_number(pp.init_end_year) and not pp.default_to_current) or (
        not is_number(pp.init_num) and not pp.default_to_current
    ) or no_records
    if not abort:
        return
    if not is_number(pp.init_end_year):
        raise RuntimeError(f"!!!! The value : {pp.init_end_year or ''} is not a valid pay period year")
    if not is_number(pp.init_num):
        raise RuntimeError(f"!!!! The value : {pp.init_num or ''} is not a valid pay period number")
    raise RuntimeError(
        f"!!!! There were no {label} records found for this pay period: {pp.init_end_year}-{(pp.init_num or '').rjust(2, '0')}"
    )


def wf_vars(ctx: RunContext, pp: PayPeriod) -> dict:
    """Post-session success assignment ``$$WF_* = $$MAP_*``; a NULL SETVARIABLE keeps the start value."""
    return {
        "WF_PP_END_YEAR": pp.end_year_str if pp.end_year_str is not None else ctx.param("WF_PP_END_YEAR", ""),
        "WF_PP_NUM": pp.num_str if pp.num_str is not None else ctx.param("WF_PP_NUM", ""),
        "WF_PP_YEAR_NUM": pp.year_num if pp.year_num is not None else ctx.param("WF_PP_YEAR_NUM", ""),
    }


def set_cpm_calendar(ctx: RunContext, file_name: str) -> dict:
    """``m_*_Set_CPM_Calendar``: resolve the pay period, write ``CPM_PAY_PERIOD`` and publish WF_PP_* vars."""
    pp = resolve_pay_period(ctx)
    validate(pp)
    write_rows(ctx, [{"CPM_PAY_PERIOD": pp.cpm_pay_period}], PAY_PERIOD_FILE, file_name)
    return wf_vars(ctx, pp)


def newpay_lookup_filter(ctx: RunContext, pp: PayPeriod, extra) -> DataFrame:
    """lkp_CPM_NEWPAY_TBL: Lookup Source Filter on ``$$MAP_PP_END_YEAR``/``$$MAP_PP_NUM`` + condition on v_PP_* and MP_POOL_DES = ' '."""
    y, n = _dec(ctx.param("WF_PP_END_YEAR", "")), _dec(ctx.param("WF_PP_NUM", ""))
    t = ctx.table("CPM_NEWPAY_TBL", "INFO_TARGET_DEV")
    return t.filter(
        (F.col("PP_END_YEAR") == F.lit(y))
        & (F.col("PP_NUM") == F.lit(n))
        & extra
        & (F.col("PP_END_YEAR") == F.lit(pp.end_year))
        & (F.col("PP_NUM") == F.lit(pp.num))
        & (F.col("MP_POOL_DES") == F.lit(" "))
    )


def set_pay_calendar(ctx: RunContext, file_name: str, business_unit_filter, label: str) -> None:
    """``m_*_Set_Pay_Calendar``: as Set_CPM_Calendar plus an ABORT when the agency has no CPM_NEWPAY_TBL rows.

    No post-session variable assignment in these sessions, so nothing is returned.
    """
    pp = resolve_pay_period(ctx)
    no_records = pp.end_year is None or newpay_lookup_filter(ctx, pp, business_unit_filter).limit(1).count() == 0
    validate(pp, no_records=no_records, label=label)
    write_rows(ctx, [{"CPM_PAY_PERIOD": pp.cpm_pay_period}], PAY_PERIOD_FILE, file_name)
    return None


def newpay_period_rows(ctx: RunContext, extra) -> DataFrame:
    """SQ_CPM_NEWPAY_TBL Source Filter: ``PP_END_YEAR = TO_NUMBER('$$MAP_PP_END_YEAR') AND PP_NUM = ... AND MP_POOL_DES = ' ' AND <agency>``."""
    t = ctx.table("CPM_NEWPAY_TBL", "INFO_TARGET_DEV")
    return t.filter(
        (F.col("PP_END_YEAR") == F.lit(ctx.param("WF_PP_END_YEAR")).cast("decimal(4,0)"))
        & (F.col("PP_NUM") == F.lit(ctx.param("WF_PP_NUM")).cast("decimal(2,0)"))
        & (F.col("MP_POOL_DES") == F.lit(" "))
        & extra
    )


def pay_period_rows(ctx: RunContext) -> DataFrame:
    """SQ_PAY_PERIOD Source Filter: ``PP_END_YEAR = TO_NUMBER('$$MAP_PP_END_YEAR') AND PP_NUM = TO_NUMBER('$$MAP_PP_NUM')``."""
    return ctx.table("PAY_PERIOD", "HISTDBA").filter(
        (F.col("PP_END_YEAR") == F.lit(ctx.param("WF_PP_END_YEAR")).cast("decimal(4,0)"))
        & (F.col("PP_NUM") == F.lit(ctx.param("WF_PP_NUM")).cast("decimal(2,0)"))
    )


MESSAGE_FILE = [Field("SUBJECT", 100), Field("MESSAGE", 300)]


def build_message(ctx: RunContext, rows: DataFrame, label: str, file_name: str, count_header: bool) -> dict:
    """``m_CPM_<X>_Build_Message``: COUNT(*) of the extracted rows (+1 header row from
    lkp_Pay_Period_Total when ``count_header``), SETVARIABLE $$MAP_SUBJECT / $$MAP_MESSAGE."""
    agg = rows.agg(F.count(F.lit(1)).alias("TOTAL"), F.max("PP_END_YEAR").alias("PP_END_YEAR"), F.max("PP_NUM").alias("PP_NUM")).collect()[0]
    total = int(agg["TOTAL"])
    if count_header:
        # lkp_Pay_Period_Total: SELECT COUNT(*) COUNT_TOTAL FROM PAY_PERIOD WHERE <pay period>, condition COUNT_TOTAL >= 1
        hdr = pay_period_rows(ctx).count()
        total = total + hdr if hdr >= 1 else total
    pp_num = _num_str(agg["PP_NUM"])
    pp_num = (pp_num.rjust(2, "0") if agg["PP_NUM"] is not None and agg["PP_NUM"] < 10 else pp_num) or ""
    subject = f"{environment_prefix(ctx)}CPM {label} Process Completed Successfully for: {_num_str(agg['PP_END_YEAR']) or ''}-{pp_num}"
    message = f"Total number of CPM {label} records loaded is: {total}"
    write_rows(ctx, [{"SUBJECT": subject, "MESSAGE": message}], MESSAGE_FILE, file_name)
    return {"WF_SUBJECT": subject, "WF_MESSAGE": message}


def generic_file(ctx: RunContext, file_name: str = "generic_target_file.txt") -> DataFrame:
    """SQ_HI_GENERIC_SRC_TBL -> GENERIC_TARGET_FILE (ID number(1)). A placeholder mapping that exists
    so the session can run its post-session command."""
    df = ctx.table("HI_GENERIC_SRC_TBL", "INFO_TARGET_DEV").select(F.col("ID").cast("string").alias("ID"))
    write_fixed_width(df, out_file(ctx, file_name), GENERIC_TARGET_FILE)
    return df


def email(ctx: RunContext, to_param: str = "WF_CPM_EMAIL_LIST") -> dict:
    """Email task: ``To = $$<to_param>``, ``Subject = $$WF_SUBJECT``, ``Text = $$WF_MESSAGE``.

    Sending is done by the job's notification / ops task; this returns the resolved message."""
    return {"EMAIL_TO": ctx.param(to_param, ""), "EMAIL_SUBJECT": ctx.param("WF_SUBJECT", ""), "EMAIL_TEXT": ctx.param("WF_MESSAGE", "")}
