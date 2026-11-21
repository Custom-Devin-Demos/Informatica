"""wf_CPM — Civilian Pay Master (DOE payroll) staging and CPM_NEWPAY load, converted from ``XML/CPM``.

One task per PowerCenter session, in workflow-link order (each link is unconditional, so a task runs only
after the previous one succeeded).  Each task runs the session's mapping through ``_engine.run_mapping``
(which executes the exported mapping graph: expressions, lookups, routers, filters, aggregators, joiners,
normalizers and the ``mplt_Convert_Num_To_Prec7`` mapplet) after building the Source Qualifier inputs
below (SQL overrides / source filters / user-defined joins translated to Spark SQL, VSAM copybook files
parsed with ``_helpers.read_vsam``), then writes the targets:

* Oracle targets -> Delta tables (``ctx.table_name``); *Truncate target table option* = YES -> overwrite,
  otherwise append (all sessions treat source rows as Insert; there is no Update Strategy in wf_CPM).
* Flat-file targets -> fixed-width files under ``<landing_root>/out/CPM`` (``$PMTargetFileDir`` and
  ``$Param_Root_Directory/data/int/out/CPM`` both map there).

Workflow variables: every session's pre-session assignment is ``$$MAP_PP_END_YEAR = $$WF_PP_END_YEAR`` /
``$$MAP_PP_NUM = $$WF_PP_NUM``; ``s_CPM_Current_Pay_Period`` returns ``WF_PP_END_YEAR``, ``WF_PP_NUM``,
``WF_PP_YEAR_NUM`` and ``s_CPM_Build_Message_Counters`` returns ``WF_SUBJECT``, ``WF_MESSAGE`` (post-session
success assignments from the mapping variables set with SETVARIABLE).

Parameters (``BIIS_parms.iparms``): ``CPM_Biweekly_MER_filename``, ``CPM_PAD_filename``,
``CPM_Year_To_Date_filename``, ``CPM_Department_of_Energy_filename`` (input file names under
``<landing_root>/in/CPM``), ``WF_CPM_EMAIL_LIST``.

Owned by the orchestration unit (not executed here):

* ``s_CPM_Send_Counts`` post-session success commands::

    mv $Param_Root_Directory/data/int/in/CPM/$Param_CPM_Biweekly_MER_filename  $Param_Root_Directory/data/archive/CPM/p6722d01_P$$WF_PP_YEAR_NUM.txt
    mv $Param_Root_Directory/data/int/in/CPM/$Param_CPM_PAD_filename  $Param_Root_Directory/data/archive/CPM/p6331x01_P$$WF_PP_YEAR_NUM.txt
    mv $Param_Root_Directory/data/int/in/CPM/$Param_CPM_Department_of_Energy_filename  $Param_Root_Directory/data/archive/CPM/p6310de2_P$$WF_PP_YEAR_NUM.txt
    mv $Param_Root_Directory/data/int/in/CPM/$Param_CPM_Year_To_Date_filename  $Param_Root_Directory/data/archive/CPM/p6791x01_P$$WF_PP_YEAR_NUM.txt

* ``s_CPM_Send_Counts`` success e-mail to ``$$WF_CPM_EMAIL_LIST`` with subject ``$$WF_SUBJECT`` and the
  ``cpm_message_file.txt`` written by ``s_CPM_Build_Message_Counters`` as body; every session's failure
  e-mail ("Session Error") to ``$$WF_CPM_EMAIL_LIST``.
"""
from __future__ import annotations

from typing import Callable, Optional

from pyspark.sql import DataFrame

from biis_lakehouse.context import RunContext

from . import _engine as E
from ._helpers import load_insert, load_truncate, read_vsam, write_flat

MAPPINGS = {
    "s_CPM_Current_Pay_Period": "m_CPM_Current_Pay_Period",
    "s_CPM_Load_CPM_MER_Staging_Tables": "m_CPM_Load_CPM_MER_Staging_Tables",
    "s_CPM_Load_CPM_PAD_Staging_Tables": "m_CPM_Load_CPM_PAD_Staging_Tables",
    "s_CPM_Load_CPM_YTD_Staging_Tables": "m_CPM_Load_CPM_YTD_Staging_Tables",
    "s_CPM_Load_CPM_PMR_Staging_Tables": "m_CPM_Load_CPM_PMR_Staging_Tables",
    "s_CPM_Load_CPM_NEWPAY_STG_YTD_STATE_TBL": "m_CPM_Load_CPM_NEWPAY_STG_YTD_STATE_TBL",
    "s_CPM_Load_CPM_NEWPAY_STG_ALT_TBL": "m_CPM_Load_CPM_NEWPAY_STG_ALT_TBL",
    "s_CPM_Load_CPM_NEWPAY_STG_DETAIL_TBL": "m_CPM_Load_CPM_NEWPAY_STG_DETAIL_TBL",
    "s_CPM_Load_CPM_NEWPAY_STG_TYPE_1_2_TBL": "m_CPM_Load_CPM_NEWPAY_STG_TYPE_1_2_TBL",
    "s_CPM_Load_CPM_NEWPAY_STG_TYPE_3_FDR_TBL": "m_CPM_Load_CPM_NEWPAY_STG_TYPE_3_FDR_TBL",
    "s_CPM_Load_From_FDR_CPM_NEWPAY_STG_TYPE_3_TBL": "m_CPM_Load_CPM_NEWPAY_STG_TYPE_3_TBL",
    "s_CPM_Load_FDR_CPM_NEWPAY_TBL": "m_CPM_Load_FDR_CPM_NEWPAY_TBL",
    "s_CPM_Load_CPM_NEWPAY_TBL": "m_CPM_Load_PMR_To_CPM_NEWPAY_TBL",
    "s_CPM_Build_Message_Counters": "m_CPM_Build_Message_Counters",
    "s_CPM_Send_Counts": "m_Generic_Mapping",
}

# Oracle owner of each table read by a lookup / Source Qualifier (everything else is INFO_TARGET_DEV).
OWNERS = {"PAY_PERIOD": "HISTDBA"}


def _owner(table: str) -> Optional[str]:
    return OWNERS.get(table.upper())


def _params(ctx: RunContext, mapping: str) -> dict:
    """Mapping parameters/variables after the pre-session assignment ``$$MAP_* = $$WF_*``."""
    p = {
        "$PMMappingName": mapping,
        "$PMRepositoryServiceName": ctx.params.get("PMRepositoryServiceName", "BIIS_LAKEHOUSE"),
    }
    for m, w in (("MAP_PP_END_YEAR", "WF_PP_END_YEAR"), ("MAP_PP_NUM", "WF_PP_NUM"), ("MAP_PP_YEAR_NUM", "WF_PP_YEAR_NUM")):
        if ctx.params.get(w) not in (None, ""):
            p["$$" + m] = ctx.params[w]
    return p


def _t(ctx: RunContext, table: str) -> str:
    return ctx.table_name(table, _owner(table))


def _run(ctx: RunContext, session: str, inputs: Callable[[dict], dict[str, DataFrame]]) -> E.MappingResult:
    mapping = MAPPINGS[session]
    params = _params(ctx, mapping)
    return E.run_mapping(ctx, mapping, inputs(params), params, lookup_owner=_owner)


def _write(ctx: RunContext, res: E.MappingResult, truncate: tuple[str, ...] = (), insert: tuple[str, ...] = ()) -> None:
    for t in truncate:
        load_truncate(ctx, res.targets[t], t)
    for t in insert:
        load_insert(ctx, res.targets[t], t)


def _vsam(ctx: RunContext, session: str, normalizer: str, source: str, file_param: str) -> DataFrame:
    return E.vsam_input(ctx, MAPPINGS[session], normalizer, read_vsam(ctx, source, ctx.param(file_param)))


# --------------------------------------------------------------------------- sessions
def s_CPM_Current_Pay_Period(ctx: RunContext) -> dict:
    """PAY_PERIOD (CURR_PP_FLAG = 'Y') -> cpm_pay_period.txt; SETVARIABLE MAP_PP_* -> WF_PP_*."""
    s = "s_CPM_Current_Pay_Period"
    res = _run(ctx, s, lambda p: {"SQ_PAY_PERIOD": E.sq_default(
        ctx, MAPPINGS[s], "SQ_PAY_PERIOD", where="PAY_PERIOD.CURR_PP_FLAG = 'Y'", owners={"PAY_PERIOD": "HISTDBA"})})
    write_flat(ctx, res.targets["CPM_PAY_PERIOD_DATE_FILE"], "CPM_PAY_PERIOD_DATE_FILE", "cpm_pay_period.txt")
    v = res.setvars
    if v.get("$$MAP_PP_END_YEAR") is None:
        return {}
    return {
        "WF_PP_END_YEAR": str(int(v["$$MAP_PP_END_YEAR"])),
        "WF_PP_NUM": str(int(v["$$MAP_PP_NUM"])),
        "WF_PP_YEAR_NUM": str(v["$$MAP_PP_YEAR_NUM"]),
    }


def s_CPM_Load_CPM_MER_Staging_Tables(ctx: RunContext) -> None:
    """MER VSAM file (header record type 1, detail type 3) -> CPM_MER_HEADER/DETAIL_STG_TBL.
    ABORTs when the header date is invalid or outside the current pay period."""
    s = "s_CPM_Load_CPM_MER_Staging_Tables"
    res = _run(ctx, s, lambda p: {"Norm_MER_FILE": _vsam(ctx, s, "Norm_MER_FILE", "MER_FILE", "CPM_Biweekly_MER_filename")})
    _write(ctx, res, truncate=("CPM_MER_DETAIL_STG_TBL", "CPM_MER_HEADER_STG_TBL"))


def s_CPM_Load_CPM_PAD_Staging_Tables(ctx: RunContext) -> None:
    """PAD VSAM file (H/D records) -> CPM_PAD_HEADER/DETAIL_STG_TBL (ABORT on bad header date)."""
    s = "s_CPM_Load_CPM_PAD_Staging_Tables"
    res = _run(ctx, s, lambda p: {"Norm_PAD_FILE": _vsam(ctx, s, "Norm_PAD_FILE", "PAD_FILE", "CPM_PAD_filename")})
    _write(ctx, res, truncate=("CPM_PAD_DETAIL_STG_TBL", "CPM_PAD_HEADER_STG_TBL"))


def s_CPM_Load_CPM_YTD_Staging_Tables(ctx: RunContext) -> None:
    """YTD VSAM file (record types 0/1/2) -> CPM_YTD_HEADER/DETAIL/STATE_STG_TBL (ABORT on bad header date)."""
    s = "s_CPM_Load_CPM_YTD_Staging_Tables"
    res = _run(ctx, s, lambda p: {"Norm_YTD_FILE": _vsam(ctx, s, "Norm_YTD_FILE", "YTD_FILE", "CPM_Year_To_Date_filename")})
    _write(ctx, res, truncate=("CPM_YTD_HEADER_STG_TBL", "CPM_YTD_STATE_STG_TBL", "CPM_YTD_DETAIL_STG_TBL"))


def s_CPM_Load_CPM_PMR_Staging_Tables(ctx: RunContext) -> None:
    """DOE Paymaster VSAM file (H/1/2/3 records) -> CPM_PMH/PM1/PM2/PM3_STG_TBL (ABORT on bad header date)."""
    s = "s_CPM_Load_CPM_PMR_Staging_Tables"
    res = _run(ctx, s, lambda p: {"Norm_PAYMASTER_FILE": _vsam(ctx, s, "Norm_PAYMASTER_FILE", "PAYMASTER_FILE", "CPM_Department_of_Energy_filename")})
    _write(ctx, res, truncate=("CPM_PM1_STG_TBL", "CPM_PMH_STG_TBL", "CPM_PM2_STG_TBL", "CPM_PM3_STG_TBL"))


def s_CPM_Load_CPM_NEWPAY_STG_YTD_STATE_TBL(ctx: RunContext) -> None:
    """CPM_YTD_STATE_STG_TBL -> one row per employee with up to 4 state tax slots (stateful expression)."""
    s = "s_CPM_Load_CPM_NEWPAY_STG_YTD_STATE_TBL"
    res = _run(ctx, s, lambda p: {"SQ_CPM_YTD_STATE_STG_TBL": _override(ctx, s, "SQ_CPM_YTD_STATE_STG_TBL", p)})
    _write(ctx, res, truncate=("CPM_NEWPAY_STG_YTD_STATE_TBL",))


def s_CPM_Load_CPM_NEWPAY_STG_ALT_TBL(ctx: RunContext) -> None:
    """PM3 'WL' allotment lines -> CPM_NEWPAY_STG_ALT_TBL (up to 9 allotments per employee) + ERROR_TBL."""
    s = "s_CPM_Load_CPM_NEWPAY_STG_ALT_TBL"
    res = _run(ctx, s, lambda p: {"SQ_CPM_PM3_STG_TBL": _override(ctx, s, "SQ_CPM_PM3_STG_TBL", p)})
    _write(ctx, res, truncate=("CPM_NEWPAY_STG_ALT_TBL",), insert=("ERROR_TBL",))


def s_CPM_Load_CPM_NEWPAY_STG_DETAIL_TBL(ctx: RunContext) -> None:
    """PM3 tax/benefit detail lines -> CPM_NEWPAY_STG_DETAIL_TBL (one row per employee)."""
    s = "s_CPM_Load_CPM_NEWPAY_STG_DETAIL_TBL"
    res = _run(ctx, s, lambda p: {"SQ_CPM_PM3_STG_TBL": _override(ctx, s, "SQ_CPM_PM3_STG_TBL", p)})
    _write(ctx, res, truncate=("CPM_NEWPAY_STG_DETAIL_TBL",))


def _override(ctx: RunContext, session: str, sq: str, p: dict) -> DataFrame:
    """Source Qualifier SQL override, executed as exported (all CPM overrides are ANSI SQL): ``$$`` parameters
    expanded, ``--`` comments dropped, table names qualified to their lakehouse schema."""
    mapping = MAPPINGS[session]
    m = E.spec()["mappings"][mapping]
    tables = {m["instances"][a]["tx"] for a, _, b, _ in m["connectors"] if b == sq}
    return E.sq_query(ctx, mapping, sq, _oracle_override(ctx, m["transformations"][sq]["attrs"]["Sql Query"], p, tables))


def _oracle_override(ctx: RunContext, sql: str, p: dict, tables) -> str:
    """Use an exported SQL override verbatim when it is ANSI-compatible: expand ``$$`` parameters, qualify
    the table names and drop ``--`` comments."""
    import re

    out = E.sub_params(sql, p)
    out = re.sub(r"--[^\n]*", "", out)
    for table in tables:
        out = re.sub(rf"\b{table}\b(?!\.)", _t(ctx, table), out)
    return out


def s_CPM_Load_CPM_NEWPAY_STG_TYPE_1_2_TBL(ctx: RunContext) -> None:
    """PM1 x PM2 x PSEUDOSSN (+ YTD detail, MER, PAD, YTD-state lookups) -> CPM_NEWPAY_STG_TYPE_1_2_TBL + ERROR_TBL."""
    s = "s_CPM_Load_CPM_NEWPAY_STG_TYPE_1_2_TBL"
    m = MAPPINGS[s]

    def inputs(p):
        y, n = p["$$MAP_PP_END_YEAR"], p["$$MAP_PP_NUM"]
        return {
            "SQ_CPM_YTD_DETAIL_STG_TBL": E.sq_default(
                ctx, m, "SQ_CPM_YTD_DETAIL_STG_TBL",
                where=f"CPM_YTD_DETAIL_STG_TBL.PP_END_YEAR = {y} AND CPM_YTD_DETAIL_STG_TBL.PP_NUM = {n}"),
            # Oracle "PM1.PYF_EYE_ID_1 = PSEUDOSSN_TBL.PSEUDOSSN(+)" -> LEFT JOIN
            "SQ_CPM_PM1_STG_TBL": E.sq_default(
                ctx, m, "SQ_CPM_PM1_STG_TBL",
                from_sql=f"{_t(ctx, 'CPM_PM1_STG_TBL')} CPM_PM1_STG_TBL"
                         f" JOIN {_t(ctx, 'CPM_PM2_STG_TBL')} CPM_PM2_STG_TBL ON CPM_PM1_STG_TBL.PYF_EYE_ID_1 = CPM_PM2_STG_TBL.PYF_EYE_ID_2"
                         f" LEFT JOIN {_t(ctx, 'PSEUDOSSN_TBL')} PSEUDOSSN_TBL ON CPM_PM1_STG_TBL.PYF_EYE_ID_1 = PSEUDOSSN_TBL.PSEUDOSSN",
                where=f"CPM_PM1_STG_TBL.PP_END_YEAR = {y} AND CPM_PM1_STG_TBL.PP_NUM = {n}"
                      f" AND CPM_PM2_STG_TBL.PP_END_YEAR = {y} AND CPM_PM2_STG_TBL.PP_NUM = {n}"),
        }

    res = _run(ctx, s, inputs)
    _write(ctx, res, truncate=("CPM_NEWPAY_STG_TYPE_1_2_TBL",), insert=("ERROR_TBL",))


def s_CPM_Load_CPM_NEWPAY_STG_TYPE_3_FDR_TBL(ctx: RunContext) -> None:
    """PM3 pay lines -> CPM_NEWPAY_STG_TYPE_3_FDR_TBL (Feeder format, one row per employee/pool/TAC line)."""
    s = "s_CPM_Load_CPM_NEWPAY_STG_TYPE_3_FDR_TBL"
    res = _run(ctx, s, lambda p: {"SQ_CPM_PM3_STG_TBL": _override(ctx, s, "SQ_CPM_PM3_STG_TBL", p)})
    _write(ctx, res, truncate=("CPM_NEWPAY_STG_TYPE_3_FDR_TBL",))


def s_CPM_Load_From_FDR_CPM_NEWPAY_STG_TYPE_3_TBL(ctx: RunContext) -> None:
    """CPM_NEWPAY_STG_TYPE_3_FDR_TBL -> CPM_NEWPAY_STG_TYPE_3_TBL (Paymaster format, one row per employee)."""
    s = "s_CPM_Load_From_FDR_CPM_NEWPAY_STG_TYPE_3_TBL"
    res = _run(ctx, s, lambda p: {"SQ_CPM_NEWPAY_STG_TYPE_3_FDR_TBL": E.sq_default(
        ctx, MAPPINGS[s], "SQ_CPM_NEWPAY_STG_TYPE_3_FDR_TBL",
        where=f"CPM_NEWPAY_STG_TYPE_3_FDR_TBL.PP_END_YEAR = {p['$$MAP_PP_END_YEAR']}"
              f" AND CPM_NEWPAY_STG_TYPE_3_FDR_TBL.PP_NUM = {p['$$MAP_PP_NUM']}")})
    _write(ctx, res, truncate=("CPM_NEWPAY_STG_TYPE_3_TBL",))


def _type12_join(ctx: RunContext, session: str, other: str, p: dict) -> DataFrame:
    y, n = p["$$MAP_PP_END_YEAR"], p["$$MAP_PP_NUM"]
    a, b = "CPM_NEWPAY_STG_TYPE_1_2_TBL", other
    return E.sq_default(
        ctx, MAPPINGS[session], "SQ_CPM_NEWPAY_STG_TYPE_1_2_TBL",
        where=f"{a}.PP_END_YEAR = {y} AND {a}.PP_NUM = {n} AND {b}.PP_END_YEAR = {y} AND {b}.PP_NUM = {n}"
              f" AND {a}.PP_END_YEAR = {b}.PP_END_YEAR AND {a}.PP_NUM = {b}.PP_NUM"
              f" AND {a}.DFAS_PSEUDO_SSN = {b}.DFAS_PSEUDO_SSN")


def s_CPM_Load_FDR_CPM_NEWPAY_TBL(ctx: RunContext) -> None:
    """TYPE_1_2 x TYPE_3_FDR -> CPM_NEWPAY_TBL (Feeder rows, appended — truncate is off)."""
    s = "s_CPM_Load_FDR_CPM_NEWPAY_TBL"
    res = _run(ctx, s, lambda p: {"SQ_CPM_NEWPAY_STG_TYPE_1_2_TBL": _type12_join(ctx, s, "CPM_NEWPAY_STG_TYPE_3_FDR_TBL", p)})
    _write(ctx, res, insert=("CPM_NEWPAY_TBL",))


def s_CPM_Load_CPM_NEWPAY_TBL(ctx: RunContext) -> None:
    """TYPE_1_2 x TYPE_3 -> CPM_NEWPAY_TBL (Paymaster rows, appended — truncate is off)."""
    s = "s_CPM_Load_CPM_NEWPAY_TBL"
    res = _run(ctx, s, lambda p: {"SQ_CPM_NEWPAY_STG_TYPE_1_2_TBL": _type12_join(ctx, s, "CPM_NEWPAY_STG_TYPE_3_TBL", p)})
    _write(ctx, res, insert=("CPM_NEWPAY_TBL",))


def s_CPM_Build_Message_Counters(ctx: RunContext) -> dict:
    """Input/output/error counters -> COUNTER_TBL (append) + cpm_message_file.txt; sets WF_SUBJECT/WF_MESSAGE."""
    s = "s_CPM_Build_Message_Counters"
    m = MAPPINGS[s]

    def inputs(p):
        y, n = p["$$MAP_PP_END_YEAR"], p["$$MAP_PP_NUM"]
        return {
            "SQ_CPM_NEWPAY_TBL": E.sq_default(ctx, m, "SQ_CPM_NEWPAY_TBL",
                                              where=f"CPM_NEWPAY_TBL.PP_END_YEAR = {y} AND CPM_NEWPAY_TBL.PP_NUM = {n}"),
            "SQ_ERROR_TBL": _override(ctx, s, "SQ_ERROR_TBL", p),
            "SQ_CPM_NEWPAY_TBL1": _override(ctx, s, "SQ_CPM_NEWPAY_TBL1", p),
            "Norm_PAYMASTER_THREE": _vsam(ctx, s, "Norm_PAYMASTER_THREE", "PAYMASTER_THREE", "CPM_Department_of_Energy_filename"),
        }

    res = _run(ctx, s, inputs)
    _write(ctx, res, insert=("COUNTER_TBL",))
    write_flat(ctx, res.targets["CPM_MESSAGE_FILE"], "CPM_MESSAGE_FILE", "cpm_message_file.txt", numeric=("COUNT_VALUE",))
    out = {}
    if res.setvars.get("$$MAP_SUBJECT") is not None:
        out["WF_SUBJECT"] = str(res.setvars["$$MAP_SUBJECT"])
    if res.setvars.get("$$MAP_MESSAGE") is not None:
        out["WF_MESSAGE"] = str(res.setvars["$$MAP_MESSAGE"])
    return out


def s_CPM_Send_Counts(ctx: RunContext) -> None:
    """m_Generic_Mapping: HI_GENERIC_SRC_TBL -> generic_target_file1.out (exists to carry the success e-mail and
    the archive ``mv`` commands, which belong to orchestration)."""
    s = "s_CPM_Send_Counts"
    res = _run(ctx, s, lambda p: {"SQ_HI_GENERIC_SRC_TBL": E.sq_default(ctx, MAPPINGS[s], "SQ_HI_GENERIC_SRC_TBL")})
    write_flat(ctx, res.targets["GENERIC_TARGET_FILE"], "GENERIC_TARGET_FILE", "generic_target_file1.out")


TASKS = {name: globals()[name] for name in MAPPINGS}
