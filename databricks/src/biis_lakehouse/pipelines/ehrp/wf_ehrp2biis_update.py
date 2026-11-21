"""wf_EHRP2BIIS_UPDATE (PowerCenter folder EHRP2BIIS, export ``XML/EHRP2BIIS_UPDATE``).

Workflow links: ``Start -> s_m_EHRP2BIIS_UPDATE``. Scheduled daily from 2018-09-28 12:00
(schedule, the ``on_success_mail`` email task and the pre/post load SQL
``ehrp2biis_preload`` / ``ehrp2biis_afterload.sql`` are owned by the orchestration and SQL units).
The session has no pre/post session shell commands.

Session ``s_m_EHRP2BIIS_UPDATE`` runs mapping ``m_EHRP2BIIS_UPDATE``:

* ``SQ_PS_GVT_JOB``: SQL override joining ``EHRP.PS_GVT_JOB`` to
  ``NKNIGHT.NWK_NEW_EHRP_ACTIONS_TBL`` on EMPLID/EMPL_RCD/EFFDT/EFFSEQ, ``ORDER BY EFFDT``.
* nine connected, cached Lookups, all ``Lookup policy on multiple match = Use Any Value``
  (implemented as a deterministic pick of one row per lookup key, then a left join).
* ``exp_GET_EFFDT_YEAR`` / ``exp_MAIN2BIIS`` / ``exp_PERS_DATA`` expressions.
* targets ``EHRP_RECS_TRACKING_TBL``, ``NWK_ACTION_PRIMARY_TBL``, ``NWK_ACTION_SECONDARY_TBL``
  (Oracle, schema NKNIGHT). There is no Update Strategy and the session treats source rows as
  ``Insert``, so every row is a plain insert (``common.delta.append``), as in PowerCenter.
  The targets define no key columns.

The mapping sets no workflow variables, so the task returns an empty dict.
Optional parameter ``SYSDATE`` (``YYYY-MM-DD``) pins ``SYSDATE`` for ``o_LOAD_ID`` / ``o_LOAD_DATE``.
"""
from __future__ import annotations

import re

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

from biis_lakehouse.common import delta
from biis_lakehouse.context import RunContext

from . import _schema as S

SRC_OWNER = "EHRP"
NK_OWNER = "NKNIGHT"
JOB_KEYS = ["EMPLID", "EMPL_RCD", "EFFDT", "EFFSEQ"]
SQ = "SQ_PS_GVT_JOB"
EXP = "exp_MAIN2BIIS"


def _c(instance: str, port: str) -> str:
    """Column name holding ``instance.port`` in the flattened row."""
    return f"{instance}__{port}"


def _resolve(instance: str, port: str) -> str:
    """Flattened column for a connector source; exp_PERS_DATA ports are pure pass-throughs."""
    if instance == "exp_PERS_DATA":
        instance, port = S.EXP_PERS_DATA_INPUTS[port]
    return _c(instance, port)


def pc_concat(*cols: Column) -> Column:
    """PowerCenter ``||``: NULL operands are ignored; NULL only if every operand is NULL."""
    all_null = None
    for c in cols:
        all_null = c.isNull() if all_null is None else (all_null & c.isNull())
    return F.when(all_null, F.lit(None).cast("string")).otherwise(
        F.concat(*[F.coalesce(c.cast("string"), F.lit("")) for c in cols]))


def is_spaces(c: Column) -> Column:
    return F.trim(c) == ""


def null_if_zero(c: Column) -> Column:
    return F.when(c == 0, F.lit(None)).otherwise(c)


def any_value(df: DataFrame, keys: list[str]) -> DataFrame:
    """``Use Any Value``: keep one (deterministically chosen) row per lookup key."""
    order = [F.col(c).asc_nulls_last() for c in df.columns if c not in keys]
    w = Window.partitionBy(*keys).orderBy(*(order or [F.lit(1)]))
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")


def _lookup(df: DataFrame, name: str, lkp: DataFrame, cond: list[tuple[Column, str]],
            outputs: list[str]) -> DataFrame:
    """Connected cached lookup: left join on equality ``cond`` [(input expr, lookup port)]."""
    keys = [p for _, p in cond]
    lkp = any_value(lkp.select(*dict.fromkeys(keys + outputs)), keys)
    lkp = lkp.select(*[F.col(k).alias(f"_k{i}") for i, k in enumerate(keys)],
                     *[F.col(o).alias(_c(name, o)) for o in outputs])
    df = df.select("*", *[e.alias(f"_in{i}") for i, (e, _) in enumerate(cond)])
    on = [F.col(f"_in{i}") == lkp[f"_k{i}"] for i in range(len(cond))]
    return df.join(lkp, on, "left").drop(*[f"_in{i}" for i in range(len(cond))],
                                         *[f"_k{i}" for i in range(len(cond))])


def _source(ctx: RunContext) -> DataFrame:
    """SQ_PS_GVT_JOB SQL override (inner join; ORDER BY applied where row order matters)."""
    job = ctx.table("PS_GVT_JOB", SRC_OWNER).alias("j")
    act = ctx.table("NWK_NEW_EHRP_ACTIONS_TBL", NK_OWNER).alias("n")
    joined = job.join(act, [F.col(f"n.{k}") == F.col(f"j.{k}") for k in JOB_KEYS], "inner")
    return joined.select(*[F.col(f"j.{c}").alias(_c(SQ, c)) for c, _ in S.PS_GVT_JOB_SELECT])


def _job_key_cond() -> list[tuple[Column, str]]:
    return [(F.col(_c(SQ, k)), k) for k in JOB_KEYS]


def _lookups(ctx: RunContext, df: DataFrame) -> DataFrame:
    def ehrp(t: str) -> DataFrame:
        return ctx.table(t, SRC_OWNER)

    simple = {
        "lkp_PS_GVT_EMPLOYMENT": ("PS_GVT_EMPLOYMENT", [
            "BUSINESS_TITLE", "CMPNY_SENIORITY_DT", "GVT_APPT_EXPIR_DT", "GVT_APPT_LIMIT_DYS",
            "GVT_APPT_LIMIT_HRS", "GVT_CNV_BEGIN_DATE", "GVT_COMP_LVL_PERM", "GVT_CURR_APT_AUTH1",
            "GVT_CURR_APT_AUTH2", "GVT_DETAIL_EXPIRES", "GVT_RTND_GRADE_BEG", "GVT_RTND_GRADE_EXP",
            "GVT_SABBATIC_EXPIR", "GVT_SCD_LEO", "GVT_SCD_RETIRE", "GVT_SCD_TSP", "GVT_SPEP",
            "GVT_SUPV_PROB_DT", "GVT_TEMP_PRO_EXPIR", "GVT_TEMP_PSN_EXPIR", "GVT_TENURE",
            "GVT_WGI_DUE_DATE", "GVT_WGI_STATUS", "HIRE_DT", "LAST_INCREASE_DT", "PROBATION_DT",
            "SERVICE_DT"]),
        "lkp_PS_GVT_PERS_NID": ("PS_GVT_PERS_NID", ["NATIONAL_ID"]),
        "lkp_PS_GVT_AWD_DATA": ("PS_GVT_AWD_DATA", [
            "EARNINGS_END_DT", "ERNCD", "GOAL_AMT", "GVT_TANG_BEN_AMT", "OTH_HRS", "OTH_PAY"]),
        "lkp_PS_HE_FILL_POS": ("PS_HE_FILL_POS", ["HE_FILL_POSITION"]),
        "lkp_PS_GVT_CITIZENSHIP": ("PS_GVT_CITIZENSHIP", ["CITIZENSHIP_STATUS"]),
    }
    for name, (table, outputs) in simple.items():
        df = _lookup(df, name, ehrp(table), _job_key_cond(), outputs)

    # lkp_PS_GVT_EE_DATA_TRK: Lookup Sql Override = SELECT DISTINCT GVT_DATE_WRK, keys, GVT_WIP_STATUS
    trk = ehrp("PS_GVT_EE_DATA_TRK").select(
        "GVT_DATE_WRK", *JOB_KEYS, "GVT_WIP_STATUS").distinct()
    df = _lookup(df, "lkp_PS_GVT_EE_DATA_TRK", trk, _job_key_cond(), ["GVT_DATE_WRK"])

    # lkp_PS_GVT_PERS_DATA: override joins PS_GVT_PERS_DATA p to NWK_NEW_EHRP_ACTIONS_TBL n.
    pers_out = ["LAST_NAME", "FIRST_NAME", "MIDDLE_NAME", "ADDRESS1", "CITY", "STATE", "POSTAL",
                "GEO_CODE", "SEX", "BIRTHDATE", "MILITARY_STATUS", "GVT_CRED_MIL_SVCE",
                "GVT_MILITARY_COMP", "GVT_MIL_RESRVE_CAT", "GVT_VET_PREF_APPT", "ETHNIC_GROUP",
                "GVT_DISABILITY_CD"]
    p = ehrp("PS_GVT_PERS_DATA").alias("p")
    n = ctx.table("NWK_NEW_EHRP_ACTIONS_TBL", NK_OWNER).alias("n")
    pers = p.join(n, [F.col(f"p.{k}") == F.col(f"n.{k}") for k in JOB_KEYS]).select(
        *[F.col(f"p.{c}") for c in pers_out + JOB_KEYS])
    df = _lookup(df, "lkp_PS_GVT_PERS_DATA", pers, _job_key_cond(), pers_out)

    # lkp_PS_JPM_JP_ITEMS: override filters jpm_cat_type = 'DEG' AND eff_status = 'A';
    # condition JPM_PROFILE_ID = EMPLID_IN, where EMPLID_IN is a string(10) port.
    jpm = ehrp("PS_JPM_JP_ITEMS").filter((F.col("JPM_CAT_TYPE") == "DEG") & (F.col("EFF_STATUS") == "A"))
    df = _lookup(df, "lkp_PS_JPM_JP_ITEMS", jpm,
                 [(F.substring(F.col(_c(SQ, "EMPLID")), 1, 10), "JPM_PROFILE_ID")],
                 ["JPM_CAT_ITEM_ID", "JPM_INTEGER_2", "MAJOR_CODE"])

    # exp_GET_EFFDT_YEAR -> lkp_OLD_SEQUENCE_NUMBER (EHRP_YEAR = o_CURRENT_YEAR)
    df = df.withColumn(_c("exp_GET_EFFDT_YEAR", "o_CURRENT_YEAR"), F.year(_c(SQ, "EFFDT")))
    seq = ctx.table("SEQUENCE_NUM_TBL", NK_OWNER)
    return _lookup(df, "lkp_OLD_SEQUENCE_NUMBER", seq,
                   [(F.col(_c("exp_GET_EFFDT_YEAR", "o_CURRENT_YEAR")), "EHRP_YEAR")],
                   ["EHRP_SEQ_NUMBER"])


def _sysdate(ctx: RunContext) -> Column:
    value = ctx.params.get("SYSDATE")
    return F.to_date(F.lit(value)) if value else F.current_date()


def _exp_main2biis(ctx: RunContext, df: DataFrame) -> DataFrame:
    def i(port: str) -> Column:
        return F.col(_resolve(*S.EXP_MAIN2BIIS_INPUTS[port]))

    noa = i("GVT_NOA_CODE")
    award_noa = noa.isin("840", "841", "842", "843", "844", "845", "848", "849", "873", "874",
                         "875", "876", "877", "878", "879") & ~i("ERNCD").isin("RCR", "RLC")
    recruit = (noa == "815") | ((noa == "948") & (i("HE_NOA_EXT") == "0"))
    reloc = (noa == "816") | ((noa == "948") & (i("HE_NOA_EXT") == "1"))
    sever = noa.isin("304", "312", "356", "357")
    sysdate = _sysdate(ctx)
    std = i("STD_HOURS")

    # v_EVENT_ID / v_PREVIOUS_YEAR: rows arrive ORDER BY EFFDT, so each year is one contiguous run;
    # the first row of a run gets EHRP_SEQ_NUMBER + 1 and each later row the previous value + 1.
    year_run = Window.partitionBy(F.year(i("EFFDT"))).orderBy(
        i("EFFDT"), i("EMPLID"), i("EMPL_RCD"), F.col(_c(SQ, "EFFSEQ")))
    first_seq = F.first(i("EHRP_SEQ_NUMBER")).over(
        year_run.rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing))
    event_id = (first_seq + F.row_number().over(year_run)).cast("int")

    exprs: dict[str, Column] = {
        "o_EVENT_ID": event_id,
        "o_AGCY_ASSIGN_CD": pc_concat(i("COMPANY"), i("GVT_SUB_AGENCY")),
        "o_AGCY_SUBELEMENT_PRIOR_CD": F.when(
            is_spaces(i("GVT_XFER_FROM_AGCY")) | i("GVT_XFER_FROM_AGCY").isNull(), None
        ).otherwise(pc_concat(i("GVT_XFER_FROM_AGCY"), F.lit("00"))),
        "o_BASE_HOURS": F.when(i("GVT_WORK_SCHED") == "I", std).otherwise(std * 2),
        "o_LEG_AUTH_TXT_1": F.upper(pc_concat(i("GVT_PAR_AUTH_D1"), F.rtrim(i("GVT_PAR_AUTH_D1_2")))),
        "o_LEG_AUTH_TXT_2": F.upper(pc_concat(i("GVT_PAR_AUTH_D2"), F.rtrim(i("GVT_PAR_AUTH_D2_2")))),
        "o_PERM_TEMP_POSITION_CD": F.when(std < 40, 3).when(std >= 40, 1).otherwise(0).cast("string"),
        "o_UNION_CD": F.when(is_spaces(i("UNION_CD")), None).otherwise(i("UNION_CD")),
        "o_EMP_RESID_CITY_STATE_NAME": pc_concat(i("CITY"), F.lit(", "), i("STATE")),
        "o_EFFSEQ": i("EFFSEQ").cast("string"),
        "o_LOAD_ID": pc_concat(F.lit("NK"), F.date_format(sysdate, "yyyyMMdd")),
        "o_LOAD_DATE": sysdate.cast("timestamp"),
        "o_CASH_AWARD_AMT": F.when(award_noa, i("GOAL_AMT"))
                             .when(noa.isin("817", "889"), i("GOAL_AMT")),
        "o_CASH_AWARD_BNFT_AMT": F.when(award_noa, i("GVT_TANG_BEN_AMT")),
        "o_RECRUITMENT_BONUS_AMT": F.when(recruit, i("GOAL_AMT")),
        "o_RELOCATION_BONUS_AMT": F.when(noa == "816", i("GOAL_AMT")),
        "o_TIME_OFF_AWARD_AMT": F.when(noa.isin("846", "847"), i("OTH_HRS")),
        "o_TIME_OFF_GRANTED_HRS": F.when(noa.isin("846", "847"), i("OTH_HRS")),
        "o_CITIZENSHIP_STATUS": F.when(i("CITIZENSHIP_STATUS_IN").isin("1", "2"), "1").otherwise("8"),
        "o_TSP_VESTING_CD": F.when(
            i("GVT_RETIRE_PLAN").isin("K", "M"),
            F.when(i("GVT_TYPE_OF_APPT").isin("55", "34", "36", "46", "44"), 2).otherwise(3),
        ).otherwise(0).cast("string"),
        "o_EVENT_SUBMITTED_DTE": F.when(i("GVT_DATE_WRK").isNull(), i("ACTION_DT"))
                                  .otherwise(i("GVT_DATE_WRK")),
        "o_RECRUITMENT_EXP_DTE": F.when(recruit, i("EARNINGS_END_DT")),
        "o_RELOCATION_EXP_DTE": F.when(reloc, i("EARNINGS_END_DT")),
        "o_GVT_CURR_APT_AUTH1": F.when(is_spaces(i("GVT_CURR_APT_AUTH1")), None)
                                 .otherwise(i("GVT_CURR_APT_AUTH1")),
        "o_GVT_CURR_APT_AUTH2": F.when(is_spaces(i("GVT_CURR_APT_AUTH2")), None)
                                 .otherwise(i("GVT_CURR_APT_AUTH2")),
        "LWOP_START_DATE": F.when(noa.isin("460", "473"), i("EFFDT")),
        "BUYOUT_EFFDT": F.when(noa == "825", i("EFFDT")),
        "o_BUSINESS_TITLE": F.upper(i("BUSINESS_TITLE")),
        "o_BUYOUT_AMT": F.when((noa == "825") & (i("ACTION") == "BON"), i("GOAL_AMT")),
        "o_APPT_LIMIT_NTE_HRS": null_if_zero(i("GVT_APPT_LIMIT_HRS")),
        "YEAR_DEGREE": null_if_zero(i("JPM_INTEGER_2")),
        "o_SEVERANCE_PAY_AMT": F.when(sever, i("OTH_PAY")),
        "o_SEVERANCE_TOTAL_AMT": F.when(sever, i("GOAL_AMT")),
        "o_SEVERANCE_PAY_START_DTE": F.when(sever, i("EFFDT")),
        "o_PROBATION_DT": F.when(i("PROBATION_DT").isNull(), None)
                           .otherwise(i("PROBATION_DT") - F.expr("INTERVAL 12 MONTHS")),
    }
    for port in ["HE_AL_RED_CRED", "HE_AL_BALANCE", "HE_LUMP_HRS", "HE_AL_CARRYOVER",
                 "HE_RES_LASTYR", "HE_RES_BALANCE", "HE_RES_TWOYRS", "HE_RES_THREEYRS",
                 "HE_AL_ACCRUAL", "HE_AL_TOTAL", "HE_AWOP_SEP", "HE_AWOP_WIGI",
                 "GVT_ANNUITY_OFFSET", "HE_SL_RED_CRED", "HE_SL_BALANCE", "HE_FROZEN_SL",
                 "HE_SL_CARRYOVER", "HE_SL_ACCRUAL", "HE_SL_TOTAL"]:
        exprs[f"o_{port}"] = null_if_zero(i(port))

    used = {p for conns in (S.EHRP_RECS_TRACKING_TBL_CONNECTORS, S.NWK_ACTION_PRIMARY_TBL_CONNECTORS,
                            S.NWK_ACTION_SECONDARY_TBL_CONNECTORS)
            for inst, p in conns.values() if inst == EXP}
    missing = used - exprs.keys() - S.EXP_MAIN2BIIS_INPUTS.keys()
    if missing:
        raise RuntimeError(f"{EXP}: no implementation for connected ports {sorted(missing)}")
    cols = {_c(EXP, p): exprs[p] if p in exprs else i(p) for p in sorted(used)}
    return df.select("*", *[c.alias(n) for n, c in cols.items()])


_TYPE = re.compile(r"string\((\d+)\)")


def _conform(df: DataFrame, layout: list[tuple[str, str]], connectors: dict[str, tuple[str, str]]) -> DataFrame:
    """Project the row onto a target: connected ports by connector, others NULL, cast to the
    Oracle column type (strings truncated to the column length like the PowerCenter writer)."""
    out = []
    for col, typ in layout:
        src = _resolve(*connectors[col]) if col in connectors else None
        c = F.col(src) if src else F.lit(None)
        m = _TYPE.fullmatch(typ)
        if m:
            if src and dict(df.dtypes)[src] in ("timestamp", "date"):
                c = F.date_format(c, "MM/dd/yyyy HH:mm:ss")
            c = F.substring(c.cast("string"), 1, int(m.group(1)))
        else:
            c = c.cast(typ)
        out.append(c.alias(col))
    return df.select(*out)


TARGETS = {
    "EHRP_RECS_TRACKING_TBL": (S.EHRP_RECS_TRACKING_TBL, S.EHRP_RECS_TRACKING_TBL_CONNECTORS),
    "NWK_ACTION_PRIMARY_TBL": (S.NWK_ACTION_PRIMARY_TBL, S.NWK_ACTION_PRIMARY_TBL_CONNECTORS),
    "NWK_ACTION_SECONDARY_TBL": (S.NWK_ACTION_SECONDARY_TBL, S.NWK_ACTION_SECONDARY_TBL_CONNECTORS),
}


def build_targets(ctx: RunContext) -> dict[str, DataFrame]:
    """m_EHRP2BIIS_UPDATE: target name -> rows to insert."""
    row = _exp_main2biis(ctx, _lookups(ctx, _source(ctx))).cache()
    return {t: _conform(row, layout, conns) for t, (layout, conns) in TARGETS.items()}


def s_m_EHRP2BIIS_UPDATE(ctx: RunContext) -> dict[str, str]:
    for target, df in build_targets(ctx).items():
        table = ctx.table_name(target, NK_OWNER)
        delta.append(ctx, df, table)
    return {}


TASKS = {"s_m_EHRP2BIIS_UPDATE": s_m_EHRP2BIIS_UPDATE}
