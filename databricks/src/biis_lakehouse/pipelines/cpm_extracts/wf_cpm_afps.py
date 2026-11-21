"""wf_CPM_AFPS (XML/CPM_AFPS): AFPS accounting feeder, crossfoot error report and counters.

Session chain (workflow links): 0010 Set_CPM_Calendar -> 0025 Set_Pay_Calendar -> 0050 Update_CPM_CYCLE_TBL
-> 0100 Data_Seperate -> 0200 Debridge_To_FEEDER_FLAT -> 0300 Gross_Exp_Report -> 0400 Crossfoot_Errors
-> 0500 Crossfoot_Message_Header -> 0600 Crossfoot_Message_Details -> 0700 Crossfoot_Message_Summary_Counts
-> 0720 Crossfoot_Message_Gross_Expend -> 0760 Concatenate_Crossfoot_Files -> 0800 Build_Message_Counters
-> 0820 Build_Message_Totals -> 0860 Concatenate_Counts_Files -> 0900 Build_Message -> 1000 Send_Report.

Session commands (owned by the orchestration unit, ``biis_lakehouse.ops``):

* 0200 pre: ``remove_file.sh $Param_Root_Directory/data/int/out/CPM $Param_CPM_AFPS_out_filename``
* 0760 target command: ``cat afps_header.txt afps_details.txt afps_summary_counts.txt afps_summary_gross_expend.txt > crossfoot_report.txt``
* 0860 target command: ``cat cpm_afps_counts_header.txt cpm_afps_counts_total.txt > cpm_afps_counters.txt``
* 1000 post: copy ``$Param_CPM_AFPS_out_filename`` to ``AFPS_FLAT_PP$$WF_PP_NUM.txt``, GPRecon / DDForm files to
  ``P3306D04.GPRECON.P<d><pp>`` / ``P6615X01.P<d><pp>``, ``afps_transfer.sh`` each, archive the inputs.
"""
from __future__ import annotations

from decimal import Decimal

from pyspark.sql import DataFrame, functions as F

from biis_lakehouse.common import delta
from biis_lakehouse.common.fixed_width import Field
from biis_lakehouse.context import RunContext
from biis_lakehouse.pipelines.cpm_extracts import _common
from biis_lakehouse.pipelines.cpm_extracts._common import PayPeriod, _dec, _num_str
from biis_lakehouse.pipelines.cpm_extracts._pcexpr import bind_params
from biis_lakehouse.pipelines.cpm_extracts._records import project, write_records
from biis_lakehouse.pipelines.cpm_extracts._spec_cpm_afps import SPECS

FEEDER_TBL_FIELDS = SPECS["m_CPM_AFPS_0100_Data_Seperate"]["HI_AFPS_FEEDER_TBL"]["fields"]
FEEDER_RECORD = SPECS["m_CPM_AFPS_0200_Debridge_To_FEEDER_FLAT"]["feeder_FEEDER_RECORD"]["fields"]
ERROR_CODE_SQL = SPECS["m_CPM_AFPS_0400_Crossfoot_Errors"]["_ports"]["exp_Format_Message.ERROR_CODE"]
CROSSFOOT_PROCESS = "m_CPM_AFPS_0400_Crossfoot_Errors"

MESSAGE_FILE = [Field("SUBJECT", 500), Field("MESSAGE", 500)]
CROSSFOOT_FILE = [Field("SUBJECT", 90), Field("MESSAGE", 200)]
COUNTS_FILE = [Field("COUNT_DESCRIPTION", 50), Field("COUNT_VALUE", 19)]

CHECK_TOT_ADD = (
    "FBASE_PAY_AMOUNT FANNUITY_GROSS FOT_PAY FOLD_OT_PAY FND_PAY FPCA_PAY FHOL_PREM_PAY FSUN_PAY FON_CALL_PAY FPSP_PAY "
    "FLSLP_PAY FICENT_PAY FPOST_DIFF_PAY FSEV_PAY FSEP_INCENT_AMT FEMPL_AWDS_AMT FRECRUITBONUS FRELOCBONUS FRETENTALLOW "
    "FSTAFFINGDIFF FSUPVSRDIFF FSUBOBJ_GM_BONUSES FUNIF_ALLOW_AMT FCOLA_ALLOW_AMT FYOC_ADVANCE_REPAY FOTHER_BEN_PAY "
    "FTEA_EMPLR_TV_CONTR FTEA_EMPLR_PK_CONTR FTEA_CASH_OUT FCHILD_SUP_SUB FREL_COMP_LS_PAY FCR_HRS_PAY FCR_HRS_LS_PAY "
    "FTRVL_SAV_AWD FOTHER_PAY_AB"
).split()
CHECK_TOT_SUBTRACT = (
    "FANNUITY_GROSS FMILITARY_COLA FFEDERAL_TAX_DED FSTATE_TAX_DED FMTOLDFUNDTAX FCITY_TAX FCOUNTY_TAX FCITY_OP_TAX "
    "FFEGLI_DED_AMT FOPT_FEGLI_DED FBOND_DED_AMT FMEDICARE FFICA_DED_AMT FCSRA_DED_AMT FUNION_DED_AMT FNATL_UNION_DED "
    "FCHARITY_DED_AMT FFEHBA_DED_AMT FLEVY_DED_AMT FQTRS_DED_AMT FSUBST_DED_AMT FLAUNDRY_DED_AMT FREFUND_DED_AMT "
    "FDCS_DEBT_AMT FOTHER_DED_AMT FALLOTMENT1 FALLOTMENT2 FALLOTMENT4 FLOAN_1_DED FLOAN_2_DED FLOAN_OWED_TO_AGENCY "
    "FFERS_EMP_DED FTSPG_EADIST FTSPG_PRADIST FTSPG_UND_DED_EMP FTSPF_UND_DED_EMP FTSPC_EADIST FTSPC_PRADIST FTEA_EMP_DED "
    "FPREPD_HB FLTC_DED FFSAHC_DED FFSADC_DED FTSP_OVER50 FTSPC_UND_DED_EMP"
).split()
GROSS_EXPEND = (
    "FBASE_PAY_AMOUNT FOT_PAY FND_PAY FHOL_PREM_PAY FANNUITY_GROSS FSUN_PAY FON_CALL_PAY FPSP_PAY FLSLP_PAY FICENT_PAY "
    "FPOST_DIFF_PAY FOLD_OT_PAY FRECRUITBONUS FRELOCBONUS FRETENTALLOW FSTAFFINGDIFF FSUPVSRDIFF FPCA_PAY "
    "FSUBOBJ_GM_BONUSES FSEV_PAY FSEP_INCENT_AMT FEMPL_AWDS_AMT FUNIF_ALLOW_AMT FCOLA_ALLOW_AMT FOTHER_BEN_PAY "
    "FOTHER_PAY_AB FCSRA_CONTR_AMT FFICA_CONTR_AMT FFEGLI_CONTR_AMT FFEHBA_CONTR_AMT FYOC_ADVANCE_REPAY FFERS_AGY_DED "
    "FTSPG_GBADIST FTSPG_GMADIST FTSPG_UND_DED_GVT FTSPF_UND_DED_GVT FTSPC_GBADIST FTSPC_GMADIST FTSPC_UND_DED_GVT "
    "FTEA_EMPLR_TV_CONTR FTEA_EMPLR_PK_CONTR FTEA_CASH_OUT FCHILD_SUP_SUB FREL_COMP_LS_PAY FCR_HRS_PAY FCR_HRS_LS_PAY "
    "FTRVL_SAV_AWD"
).split()


def _pp2(num) -> str:
    s = _num_str(num) or ""
    return s.rjust(2, "0") if num is not None and Decimal(num) < 10 else s


def _year_num_params(ctx: RunContext):
    return _dec(ctx.param("WF_PP_END_YEAR", "")), _dec(ctx.param("WF_PP_NUM", ""))


def _feeder(ctx: RunContext) -> DataFrame:
    return ctx.table("HI_AFPS_FEEDER_TBL", "INFO_TARGET_DEV")


def _master(ctx: RunContext) -> DataFrame:
    # SQ_CPM_NEWPAY_TBL: PP_END_YEAR/PP_NUM = $$MAP_*, MP_POOL_DES != ' ' (payroll pools REG/CASH/SUPPL)
    y, n = _year_num_params(ctx)
    t = ctx.table("CPM_NEWPAY_TBL", "INFO_TARGET_DEV")
    return t.filter((F.col("PP_END_YEAR") == F.lit(y)) & (F.col("PP_NUM") == F.lit(n)) & (F.col("MP_POOL_DES") != " "))


def s_CPM_AFPS_0010_Set_CPM_Calendar(ctx: RunContext) -> dict:
    """As the shared Set_CPM_Calendar, but ``$$MAP_PP_NUM`` is set to ``LPAD(TO_CHAR(v_PP_NUM), 2, '0')`` and
    ``$$MAP_PP_YEAR_NUM := PARAM_PP_END_YEAR || PARAM_PP_NUM``."""
    pp = _common.resolve_pay_period(ctx)
    _common.validate(pp)
    _common.write_rows(ctx, [{"CPM_PAY_PERIOD": pp.cpm_pay_period}], _common.PAY_PERIOD_FILE, "cpm_afps_pay_period_cal_file.txt")
    num2 = pp.num_str.rjust(2, "0")[:2] if pp.num_str is not None else None
    return {
        "WF_PP_END_YEAR": pp.end_year_str if pp.end_year_str is not None else ctx.param("WF_PP_END_YEAR", ""),
        "WF_PP_NUM": num2 if num2 is not None else ctx.param("WF_PP_NUM", ""),
        "WF_PP_YEAR_NUM": (pp.end_year_str or "") + (num2 or "") if pp.end_year is not None else ctx.param("WF_PP_YEAR_NUM", ""),
    }


def s_CPM_AFPS_0025_Set_Pay_Calendar(ctx: RunContext) -> dict:
    """lkp_CPM_NEWPAY_TBL (source filter on the pay period, condition MP_POOL_DES = 'REG'); ABORT when no feeder rows.
    Publishes ``$$WF_PP_END_YEAR_LAST_DIGIT := SUBSTR(TO_CHAR(v_PP_END_YEAR), 4, 1)``."""
    pp = _common.resolve_pay_period(ctx)
    y, n = _year_num_params(ctx)
    t = ctx.table("CPM_NEWPAY_TBL", "INFO_TARGET_DEV")
    hit = pp.end_year is not None and t.filter(
        (F.col("PP_END_YEAR") == F.lit(y)) & (F.col("PP_NUM") == F.lit(n))
        & (F.col("PP_END_YEAR") == F.lit(pp.end_year)) & (F.col("PP_NUM") == F.lit(pp.num))
        & (F.col("MP_POOL_DES") == F.lit("REG"))
    ).limit(1).count() > 0
    if not hit and (pp.default_to_current or (_common.is_number(pp.init_end_year) and _common.is_number(pp.init_num))):
        num2 = pp.num_str.rjust(2, "0")[:2] if pp.num_str is not None else ""
        raise RuntimeError(f"!!!! There were no FEEDER records found for this pay period: {pp.end_year_str or ''}-{num2}")
    _common.validate(pp)
    _common.write_rows(ctx, [{"CPM_PAY_PERIOD": pp.cpm_pay_period}], _common.PAY_PERIOD_FILE, "cpm_afps_pay_period_file1.out")
    last = (pp.end_year_str or "")[3:4] or None
    return {"WF_PP_END_YEAR_LAST_DIGIT": last if last is not None else ctx.param("WF_PP_END_YEAR_LAST_DIGIT", "")}


def update_cycle(ctx: RunContext, process_name: str) -> dict:
    """SQ_CPM_CYCLE_TBL (PROCESS_NAME filter) + lkp_PAY_PERIOD on the parameter pay period ->
    ``CYCLE_ID := IIF(PP_END_DTE <> lkp_PP_END_DTE, 0, CYCLE_ID + 1)`` -> DD_UPDATE on CPM_CYCLE_TBL (key PROCESS_NAME)."""
    y, n = _year_num_params(ctx)
    cyc = ctx.table("CPM_CYCLE_TBL", "INFO_TARGET_DEV").filter(F.col("PROCESS_NAME") == process_name)
    lkp = ctx.table("PAY_PERIOD", "HISTDBA").filter((F.col("PP_END_YEAR") == F.lit(y)) & (F.col("PP_NUM") == F.lit(n))).limit(1)
    lkp = lkp.select(F.col("PP_END_YEAR").alias("lkp_PP_END_YEAR"), F.col("PP_NUM").alias("lkp_PP_NUM"), F.col("PP_END_DTE").alias("lkp_PP_END_DTE"))
    j = cyc.crossJoin(lkp) if lkp.count() else cyc.select("*", F.lit(None).cast("decimal(4,0)").alias("lkp_PP_END_YEAR"), F.lit(None).cast("decimal(2,0)").alias("lkp_PP_NUM"), F.lit(None).cast("timestamp").alias("lkp_PP_END_DTE"))
    new_pp = F.coalesce(F.col("PP_END_DTE") != F.col("lkp_PP_END_DTE"), F.lit(False))
    out = j.select(
        F.col("PROCESS_NAME"),
        F.col("lkp_PP_END_YEAR").alias("PP_END_YEAR"),
        F.col("lkp_PP_NUM").alias("PP_NUM"),
        F.col("lkp_PP_END_DTE").alias("PP_END_DTE"),
        F.when(new_pp, F.lit(0)).otherwise(F.col("CYCLE_ID") + 1).cast("decimal(3,0)").alias("CYCLE_ID"),
    )
    rows = out.collect()
    delta.merge(ctx, delta.update_strategy(out, "DD_UPDATE"), ctx.table_name("CPM_CYCLE_TBL", "INFO_TARGET_DEV"), ["PROCESS_NAME"])
    cycle = rows[-1]["CYCLE_ID"] if rows and rows[-1]["CYCLE_ID"] is not None else None
    return {"WF_CYCLE_ID": str(int(cycle)) if cycle is not None else ctx.param("WF_CYCLE_ID", "")}


def s_CPM_AFPS_0050_Update_CPM_CYCLE_TBL(ctx: RunContext) -> dict:
    return update_cycle(ctx, "AFPS")


def s_CPM_AFPS_0100_Data_Seperate(ctx: RunContext) -> None:
    """SQ_CPM_NEWPAY_TBL x PAY_PERIOD (User Defined Join on PP_END_YEAR/PP_NUM; filter pay period, MP_POOL_DES ^= ' ')
    -> exp_Format_AFPS_Feeder / exp_Convert_Calc_Fields / exp_Format_Text_Fields -> srt_Sort_By_Payment_Type
    (all ports ascending) -> HI_AFPS_FEEDER_TBL (truncate and load)."""
    pp = ctx.table("PAY_PERIOD", "HISTDBA").drop("CURR_PP_FLAG")
    keep = [c for c in pp.columns if c not in ("PP_END_YEAR", "PP_NUM")]
    master = _master(ctx).drop(*[c for c in keep if c in _master(ctx).columns])
    src = master.join(pp, ["PP_END_YEAR", "PP_NUM"])
    out = project(src, FEEDER_TBL_FIELDS, ctx.params)
    out = out.orderBy(*[F.col(f"`{f[0]}`").asc() for f in FEEDER_TBL_FIELDS])
    delta.overwrite(ctx, out, ctx.table_name("HI_AFPS_FEEDER_TBL", "INFO_TARGET_DEV"))


def s_CPM_AFPS_0200_Debridge_To_FEEDER_FLAT(ctx: RunContext) -> None:
    """SQ override ``SELECT ... FROM HI_AFPS_FEEDER_TBL ORDER BY FFEEDER_SORT`` -> feeder_FEEDER_RECORD (PWX copybook,
    1758-byte records; the session's LRECL = 1200 is shorter than the copybook and is not applied)."""
    rows = _feeder(ctx).orderBy("FFEEDER_SORT")
    path = _common.out_file(ctx, ctx.param("CPM_AFPS_out_filename", "CPM_AFPS_out.txt"))
    write_records(rows, path, FEEDER_RECORD, ctx.params)


def _sum(cols) -> F.Column:
    # PowerCenter '+' with a NULL operand yields NULL, as in Spark.
    out = F.col(cols[0])
    for c in cols[1:]:
        out = out + F.col(c)
    return out


def s_CPM_AFPS_0300_Gross_Exp_Report(ctx: RunContext) -> None:
    """SQ_HI_AFPS_FEEDER_TBL -> exp_Compute_Main (crossfoot check) -> HI_GROSS_EXP_TBL (truncate and load)."""
    y, n = _year_num_params(ctx)
    f = _feeder(ctx)
    add, sub = _sum(CHECK_TOT_ADD).cast("decimal(11,2)"), _sum(CHECK_TOT_SUBTRACT).cast("decimal(11,2)")
    test_net = (add - sub).cast("decimal(11,2)")
    test_net_tot = (F.col("FEARNED_INCOME_PAY") + test_net).cast("decimal(11,2)")
    out = f.select(
        F.lit(y).cast("decimal(4,0)").alias("PP_END_YEAR"),
        F.lit(n).cast("decimal(2,0)").alias("PP_NUM"),
        F.lpad(F.col("FSOC_SEC_NO").cast("string"), 9, "0").alias("EMPL_ID"),
        F.concat_ws("", "FFISCAL_YEAR", "FAGY", "FACCT_PT", "FSERIAL").alias("CAN"),
        F.col("FTIME_KEEPER_NO").cast("string").alias("TK_NO"),
        add.alias("CHECK_TOT_ADD"),
        sub.alias("CHECK_TOT_SUBTRACT"),
        test_net.alias("TEST_NET"),
        F.col("FEARNED_INCOME_PAY").cast("decimal(11,2)").alias("FEARNED_INCOME_PAY"),
        test_net_tot.alias("TEST_NET_TOT"),
        F.col("FNET_PAY").cast("decimal(11,2)").alias("FNET_PAY"),
        (F.col("FNET_PAY") - test_net_tot).cast("decimal(11,2)").alias("DIFF"),
    )
    delta.overwrite(ctx, out, ctx.table_name("HI_GROSS_EXP_TBL", "INFO_TARGET_DEV"))


def s_CPM_AFPS_0400_Crossfoot_Errors(ctx: RunContext) -> None:
    """Crossfoot differences (HI_GROSS_EXP_TBL.DIFF <> 0) -> ERROR_TBL (append).

    SQ_HI_GROSS_EXP_TBL joins CPM_PM3_STG_TBL pay detail lines whose amount explains the difference
    (``PYF_ACTUAL_AMT IN (DIFF, -DIFF, DIFF/2, -DIFF/2)``) and keeps ``MIN(PYF_GEN_SEQ_ID)``; uncached lkp_CPM3
    fetches that line's PYF_PAY_DET_CD; jnr_Check_For_Records_Not_In_CPM3 is a full outer join with all DIFF <> 0 rows.
    """
    g = ctx.table("HI_GROSS_EXP_TBL", "INFO_TARGET_DEV")
    pm3 = ctx.table("CPM_PM3_STG_TBL", "INFO_TARGET_DEV")
    d = F.col("g.DIFF")
    sq = (
        g.alias("g").filter(d != 0)
        .join(pm3.alias("p"), (F.col("p.PP_END_YEAR") == F.col("g.PP_END_YEAR")) & (F.col("p.PP_NUM") == F.col("g.PP_NUM"))
              & (F.col("p.PYF_REC_NO_PDT3") == "3") & (F.col("p.PYF_EYE_ID_PDT3") == F.col("g.EMPL_ID"))
              & F.col("p.PYF_ACTUAL_AMT").isin(d, d * -1, d / 2, (d / 2) * -1))
        .groupBy("g.PP_END_YEAR", "g.PP_NUM", "g.EMPL_ID", "g.CAN", "g.TK_NO", "g.DIFF")
        .agg(F.min("p.PYF_GEN_SEQ_ID").alias("PYF_GEN_SEQ_ID"))
    )
    lkp = pm3.filter(F.col("PYF_REC_NO_PDT3") == "3").select(
        F.col("PP_END_YEAR").alias("l_yr"), F.col("PP_NUM").alias("l_num"), F.col("PYF_EYE_ID_PDT3").alias("l_emp"),
        F.col("PYF_GEN_SEQ_ID").alias("l_seq"), "PYF_PAY_DET_CD", "PYF_ADJ_DATE_EFF",
    ).dropDuplicates(["l_yr", "l_num", "l_emp", "l_seq"])
    master = sq.join(lkp, (sq.PP_END_YEAR == lkp.l_yr) & (sq.PP_NUM == lkp.l_num) & (sq.EMPL_ID == lkp.l_emp) & (sq.PYF_GEN_SEQ_ID == lkp.l_seq), "left").select(
        F.col("l_yr").alias("lkp_PP_END_YEAR"), F.col("l_num").alias("lkp_PP_NUM"), F.col("l_emp").alias("lkp_EMPL_ID"),
        F.col("PYF_PAY_DET_CD").alias("jnr_Check_For_Records_Not_In_CPM3__PYF_PAY_DET_CD"),
        F.col("PYF_ADJ_DATE_EFF").alias("jnr_Check_For_Records_Not_In_CPM3__PYF_ADJ_DATE_EFF"),
    )
    detail = g.filter(F.col("DIFF") != 0).select("PP_END_YEAR", "PP_NUM", "EMPL_ID", "CAN", "TK_NO", "DIFF")
    j = master.join(detail, (master.lkp_PP_END_YEAR == detail.PP_END_YEAR) & (master.lkp_PP_NUM == detail.PP_NUM) & (master.lkp_EMPL_ID == detail.EMPL_ID), "full")
    msg = F.concat(
        F.lit("CROSSFOOT ERROR FOR SSN "), F.coalesce(F.col("lkp_EMPL_ID"), F.lit("")),
        F.lit(" CAN "), F.coalesce(F.col("CAN"), F.lit("")),
        F.lit(" TK "), F.coalesce(F.lpad(F.col("TK_NO"), 5, "0"), F.lit("")),
        F.lit(" AMOUNT IS "), F.coalesce(F.col("DIFF").cast("decimal(28,2)").cast("string"), F.lit("")),
    )
    out = j.select(
        F.lit(CROSSFOOT_PROCESS).alias("PROCESS_NAME"),
        msg.alias("ERROR_MESSAGE"),
        F.col("lkp_EMPL_ID").alias("SOURCE_KEY"),
        F.current_timestamp().alias("ERROR_DATE"),
        F.col("lkp_PP_END_YEAR").cast("decimal(4,0)").alias("PP_END_YEAR"),
        F.col("lkp_PP_NUM").cast("decimal(2,0)").alias("PP_NUM"),
        F.lit(ctx.param("WF_CYCLE_ID", "")).cast("decimal(3,0)").alias("CYCLE_ID"),
        F.expr(bind_params(ERROR_CODE_SQL, ctx.params)).alias("ERROR_CODE"),
    )
    delta.append(ctx, out, ctx.table_name("ERROR_TBL", "INFO_TARGET_DEV"))


def _crossfoot_errors(ctx: RunContext) -> DataFrame:
    """SQ_ERROR_TBL override: today's latest crossfoot run (MAX ERROR_DATE / CYCLE_ID) for the pay period and cycle."""
    y, n = _year_num_params(ctx)
    e = ctx.table("ERROR_TBL", "INFO_TARGET_DEV").filter(
        (F.col("PP_END_YEAR") == F.lit(y)) & (F.col("PP_NUM") == F.lit(n)) & (F.col("PROCESS_NAME") == CROSSFOOT_PROCESS)
    )
    b = (
        e.filter((F.col("ERROR_DATE") >= F.date_trunc("DD", F.current_timestamp())) & (F.col("CYCLE_ID") == F.lit(ctx.param("WF_CYCLE_ID", "")).cast("decimal(3,0)")))
        .groupBy("PP_END_YEAR", "PP_NUM").agg(F.max("ERROR_DATE").alias("b_DATE"), F.max("CYCLE_ID").alias("b_CYCLE"))
    )
    return e.join(b, ["PP_END_YEAR", "PP_NUM"]).filter((F.col("ERROR_DATE") == F.col("b_DATE")) & (F.col("CYCLE_ID") == F.col("b_CYCLE"))).drop("b_DATE", "b_CYCLE")


def _pay_period_row(ctx: RunContext):
    rows = _common.pay_period_rows(ctx).select("PP_END_YEAR", "PP_NUM").collect()
    return rows[0] if rows else None


def s_CPM_AFPS_0500_Crossfoot_Message_Header(ctx: RunContext) -> dict:
    """afps_header.txt: report title + CRLF CRLF; ``$$MAP_SUBJECT`` gets the completion subject."""
    r = _pay_period_row(ctx)
    if r is None:
        _common.write_rows(ctx, [], MESSAGE_FILE, "afps_header.txt")
        return {"WF_SUBJECT": ctx.param("WF_SUBJECT", "")}
    yr, pp = _num_str(r["PP_END_YEAR"]), _pp2(r["PP_NUM"])
    message = f"CROSSFOOT ERROR MESSAGE REPORT FOR PAY PERIOD: {yr}-{pp}\r\n\r\n"
    # Only MESSAGE is connected to the target; SUBJECT is written as an unconnected (blank) port.
    _common.write_rows(ctx, [{"SUBJECT": None, "MESSAGE": message}], MESSAGE_FILE, "afps_header.txt")
    return {"WF_SUBJECT": f"{_common.environment_prefix(ctx)}AFPS Process Completed Successfully for: {yr}-{pp}"}


def s_CPM_AFPS_0600_Crossfoot_Message_Details(ctx: RunContext) -> None:
    """afps_details.txt: one line per crossfoot error, SSN masked as ``XXX-XX-<last 4>``, ordered by SOURCE_KEY."""
    e = _crossfoot_errors(ctx).orderBy("SOURCE_KEY")
    mask = F.concat(F.lit("XXX-XX-"), F.substring("SOURCE_KEY", 6, 4))
    out = e.select(
        F.when(F.col("SOURCE_KEY").isNull() | (F.col("SOURCE_KEY") == ""), F.col("ERROR_MESSAGE"))
        .otherwise(F.expr("replace(ERROR_MESSAGE, SOURCE_KEY, concat('XXX-XX-', substring(SOURCE_KEY, 6, 4)))")).alias("SUBJECT"),
        F.col("ERROR_CODE").alias("MESSAGE"),
    )
    del mask
    from biis_lakehouse.common.fixed_width import write_fixed_width

    write_fixed_width(out, _common.out_file(ctx, "afps_details.txt"), CROSSFOOT_FILE)


def _write_counts(ctx: RunContext, rows: list[tuple], name: str, scale: int = 0) -> None:
    def fmt(v):
        if v is None:
            return None
        return str(int(v)) if scale == 0 else f"{Decimal(v):.{scale}f}"

    data = [{"COUNT_DESCRIPTION": d, "COUNT_VALUE": fmt(v)} for d, v in rows]
    df = ctx.spark.createDataFrame(data, "COUNT_DESCRIPTION string, COUNT_VALUE string")
    from biis_lakehouse.common.fixed_width import write_fixed_width

    # number targets are right-aligned in the fixed-width file
    df = df.select("COUNT_DESCRIPTION", F.lpad(F.coalesce("COUNT_VALUE", F.lit("")), 19, " ").alias("COUNT_VALUE"))
    write_fixed_width(df, _common.out_file(ctx, name), COUNTS_FILE)


def _gross_expend(ctx: RunContext):
    r = _feeder(ctx).agg(F.count(F.lit(1)).alias("n"), *[F.sum(c).alias(c) for c in GROSS_EXPEND]).collect()[0]
    vals = [r[c] for c in GROSS_EXPEND]
    total = None if any(v is None for v in vals) else sum(vals, Decimal(0))
    return int(r["n"]), total


def s_CPM_AFPS_0700_Crossfoot_Message_Summary_Counts(ctx: RunContext) -> None:
    """afps_summary_counts.txt: nrm_Counters emits 7 (description, value) rows from the feeder count and the crossfoot error count."""
    feeders, _ = _gross_expend(ctx)
    errors = _crossfoot_errors(ctx).count()
    rows = [(" ", None), ("FEEDER RECORDS READ", feeders), (" ", None), ("CROSS FOOT ERROR RECORDS", errors),
            (" ", None), ("GROSS-EXPEND RECORD COUNT", feeders), (" ", None)]
    _write_counts(ctx, rows, "afps_summary_counts.txt")


def s_CPM_AFPS_0720_Crossfoot_Message_Gross_Expend(ctx: RunContext) -> None:
    _, total = _gross_expend(ctx)
    _write_counts(ctx, [("GROSS-EXPEND TOTAL", total)], "afps_summary_gross_expend.txt", scale=2)


def s_CPM_AFPS_0760_Concatenate_Crossfoot_Files(ctx: RunContext) -> None:
    _common.generic_file(ctx, "generic_target_file1.out")


def _master_feeder_totals(ctx: RunContext) -> dict:
    m = _master(ctx).agg(
        F.count(F.lit(1)).alias("COUNT_CPM"), F.sum("REG_HRS_CPP").alias("SUM_MASTER_HOURS_WORKED"),
        F.sum("CPP_GROSS_PAY").alias("SUM_MASTER_GROSS_PAY"), F.sum("CPP_BASE_PAY").alias("SUM_MASTER_BASE_PAY"),
        F.sum(F.col("CPP_NET_PAY") + F.col("CPP_NET_ALT_DED")).alias("SUM_MASTER_NET_PAY"),
        F.max("PP_END_YEAR").alias("PP_END_YEAR"), F.max("PP_NUM").alias("PP_NUM"),
    ).collect()[0].asDict()
    f = _feeder(ctx).agg(
        F.count(F.lit(1)).alias("COUNT_FEEDER_RECORDS_CREATED"), F.sum("FBASE_HOURS_WORKED").alias("SUM_FEEDER_BASE_HOURS_WORKED"),
        F.sum("FBASE_PAY_AMOUNT").alias("SUM_FEEDER_BASE_PAY_AMOUNT"), F.sum("FNET_PAY").alias("SUM_FEEDER_NET_PAY"),
        F.sum("FGROSS_PAY").alias("SUM_FEEDER_GROSS_PAY"),
    ).collect()[0].asDict()
    return {**m, **f}


def _append_counters(ctx: RunContext, t: dict, rows: list[tuple], process: str) -> None:
    data = [(t["PP_END_YEAR"], t["PP_NUM"], d, None if v is None else Decimal(v)) for d, v in rows]
    df = ctx.spark.createDataFrame(data, "PP_END_YEAR decimal(4,0), PP_NUM decimal(2,0), COUNTER_DESCRIPTION string, COUNTER_VALUE decimal(19,2)")
    out = df.select(
        "PP_END_YEAR", "PP_NUM", F.current_timestamp().alias("RUN_DATE"), F.lit(process).alias("PROCESS_NAME"),
        "COUNTER_DESCRIPTION", "COUNTER_VALUE", F.lit(None).cast("decimal(1,0)").alias("CYCLE_ID"),
    )
    delta.append(ctx, out, ctx.table_name("AFPS_COUNTER_TBL", "INFO_TARGET_DEV"))


def s_CPM_AFPS_0800_Build_Message_Counters(ctx: RunContext) -> None:
    t = _master_feeder_totals(ctx)
    yr, pp = _num_str(t["PP_END_YEAR"]) or "", (_num_str(t["PP_NUM"]) or "").rjust(2, "0")
    run_date = ctx.spark.sql("SELECT date_format(current_timestamp(), 'MM/dd/yyyy') d").collect()[0]["d"]
    counts = [("MASTER RECORDS READ", t["COUNT_CPM"]), ("FEEDER RECORDS CREATED", t["COUNT_FEEDER_RECORDS_CREATED"])]
    rows = [(f"AFPS Counts for Pay Period: {yr}-{pp}", None), (" ", None), (f"The Run Date is: {run_date}", None), (" ", None), *counts]
    _write_counts(ctx, rows, "cpm_afps_counts_header.txt")
    _append_counters(ctx, t, counts, "m_CPM_AFPS_0800_Build_Message_Counters")


def s_CPM_AFPS_0820_Build_Message_Totals(ctx: RunContext) -> None:
    t = _master_feeder_totals(ctx)
    rows = [
        ("HOURS WORKED ON MASTER", t["SUM_MASTER_HOURS_WORKED"]), ("HOURS WORKED ON FEEDER", t["SUM_FEEDER_BASE_HOURS_WORKED"]),
        ("AMOUNT OF GROSS ON MASTER", t["SUM_MASTER_GROSS_PAY"]), ("AMOUNT OF GROSS ON FEEDER", t["SUM_FEEDER_GROSS_PAY"]),
        ("AMOUNT OF BASE PAY ON MASTER", t["SUM_MASTER_BASE_PAY"]), ("AMOUNT OF BASE PAY ON FEEDER", t["SUM_FEEDER_BASE_PAY_AMOUNT"]),
        ("AMOUNT OF NET PAY ON MASTER", t["SUM_MASTER_NET_PAY"]), ("AMOUNT OF NET PAY ON FEEDER", t["SUM_FEEDER_NET_PAY"]),
    ]
    _write_counts(ctx, rows, "cpm_afps_counts_total.txt", scale=2)
    _append_counters(ctx, t, rows, "m_CPM_AFPS_0820_Build_Message_Totals")


def s_CPM_AFPS_0860_Concatenate_Counts_Files(ctx: RunContext) -> None:
    _common.generic_file(ctx, "generic_target_file1.out")


def s_CPM_AFPS_0900_Build_Message(ctx: RunContext) -> dict:
    r = _pay_period_row(ctx)
    if r is None:
        _common.write_rows(ctx, [], MESSAGE_FILE, "cpm_afps_message_file1.out")
        return {"WF_SUBJECT": ctx.param("WF_SUBJECT", ""), "WF_MESSAGE": ctx.param("WF_MESSAGE", "")}
    yr, pp = _num_str(r["PP_END_YEAR"]), _pp2(r["PP_NUM"])
    d = yr[3:4]
    subject = f"{_common.environment_prefix(ctx)}AFPS Process Completed Successfully for: {yr}-{pp}"
    message = (
        f"The accounting feeder/GPRECON files for Pay Period  {pp} have been created as:\n\n"
        f"AFPS_FLAT_PP{pp}.txt\n\nP3306D04.GPRECON.P{d}{pp}\n\nP6615X01.P{d}{pp}"
    )
    _common.write_rows(ctx, [{"SUBJECT": None, "MESSAGE": message}], MESSAGE_FILE, "cpm_afps_message_file1.out")
    return {"WF_SUBJECT": subject, "WF_MESSAGE": message}


def s_CPM_AFPS_1000_Send_Report(ctx: RunContext) -> None:
    _common.generic_file(ctx, "generic_target_file1.out")


TASKS = {
    "s_CPM_AFPS_0010_Set_CPM_Calendar": s_CPM_AFPS_0010_Set_CPM_Calendar,
    "s_CPM_AFPS_0025_Set_Pay_Calendar": s_CPM_AFPS_0025_Set_Pay_Calendar,
    "s_CPM_AFPS_0050_Update_CPM_CYCLE_TBL": s_CPM_AFPS_0050_Update_CPM_CYCLE_TBL,
    "s_CPM_AFPS_0100_Data_Seperate": s_CPM_AFPS_0100_Data_Seperate,
    "s_CPM_AFPS_0200_Debridge_To_FEEDER_FLAT": s_CPM_AFPS_0200_Debridge_To_FEEDER_FLAT,
    "s_CPM_AFPS_0300_Gross_Exp_Report": s_CPM_AFPS_0300_Gross_Exp_Report,
    "s_CPM_AFPS_0400_Crossfoot_Errors": s_CPM_AFPS_0400_Crossfoot_Errors,
    "s_CPM_AFPS_0500_Crossfoot_Message_Header": s_CPM_AFPS_0500_Crossfoot_Message_Header,
    "s_CPM_AFPS_0600_Crossfoot_Message_Details": s_CPM_AFPS_0600_Crossfoot_Message_Details,
    "s_CPM_AFPS_0700_Crossfoot_Message_Summary_Counts": s_CPM_AFPS_0700_Crossfoot_Message_Summary_Counts,
    "s_CPM_AFPS_0720_Crossfoot_Message_Gross_Expend": s_CPM_AFPS_0720_Crossfoot_Message_Gross_Expend,
    "s_CPM_AFPS_0760_Concatenate_Crossfoot_Files": s_CPM_AFPS_0760_Concatenate_Crossfoot_Files,
    "s_CPM_AFPS_0800_Build_Message_Counters": s_CPM_AFPS_0800_Build_Message_Counters,
    "s_CPM_AFPS_0820_Build_Message_Totals": s_CPM_AFPS_0820_Build_Message_Totals,
    "s_CPM_AFPS_0860_Concatenate_Counts_Files": s_CPM_AFPS_0860_Concatenate_Counts_Files,
    "s_CPM_AFPS_0900_Build_Message": s_CPM_AFPS_0900_Build_Message,
    "s_CPM_AFPS_1000_Send_Report": s_CPM_AFPS_1000_Send_Report,
}
