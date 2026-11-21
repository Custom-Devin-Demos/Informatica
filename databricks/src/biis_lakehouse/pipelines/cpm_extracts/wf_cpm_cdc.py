"""wf_CPM_CDC (XML/CPM_CDC): CPM payroll extract for CDC / ATSDR.

Session chain: s_CPM_CDC_Set_CPM_Calendar -> s_CPM_CDC_Set_Pay_Calendar -> s_CPM_CDC_Load_CPM_CDC_Header_File
-> s_CPM_CDC_Load_CPM_CDC_Data_File -> s_CPM_CDC_Concatenate_Files -> s_CPM_CDC_Build_Message -> email_CPM_CDC.

Session commands (owned by the orchestration unit, ``biis_lakehouse.ops``):

* Header / Data / Concatenate pre: ``remove_file.sh`` of ``$Param_CPM_CDC_HDR_filename``, ``$Param_CPM_CDC_DAT_filename``,
  ``$Param_CPM_CDC_out_filename`` in ``data/int/out/CPM``
* s_CPM_CDC_Concatenate_Files post: ``cat $Param_CPM_CDC_HDR_filename $Param_CPM_CDC_DAT_filename > $Param_CPM_CDC_out_filename``
* s_CPM_CDC_Build_Message post: ``cp $Param_CPM_CDC_out_filename CDC.CPM.PP$$WF_PP_YEAR_NUM`` and
  ``cdc_transfer.sh CDC.CPM.PP$$WF_PP_YEAR_NUM``
"""
from __future__ import annotations

from pyspark.sql import functions as F

from biis_lakehouse.context import RunContext
from biis_lakehouse.pipelines.cpm_extracts import _common
from biis_lakehouse.pipelines.cpm_extracts._records import write_records
from biis_lakehouse.pipelines.cpm_extracts._spec_cpm_cdc import SPECS

LABEL = "CDC"
WS_CDC_HDR = SPECS["m_CPM_CDC_Load_CPM_CDC_Header_File"]["cdchdr_WS_CDC_HDR"]["fields"]
WS_PAY_OUT_REC = SPECS["m_CPM_CDC_Load_CPM_CDC_Data_File"]["cdcskel_WS_PAY_OUT_REC"]["fields"]
LRECL = 2858
ANC_ORGS = ["ANC34", "ANC341", "ANC342", "ANC343", "ANC344", "ANC345"]


def _cdc():
    # BUSINESS_UNIT IN ('CDC00', 'ATSDR') OR trim(ORG_CDE) in ('ANC34', ..., 'ANC345')
    return F.col("BUSINESS_UNIT").isin("CDC00", "ATSDR") | F.trim(F.col("ORG_CDE")).isin(*ANC_ORGS)


def s_CPM_CDC_Set_CPM_Calendar(ctx: RunContext) -> dict:
    return _common.set_cpm_calendar(ctx, "cpm_cdc_cpm_pay_period_file.txt")


def s_CPM_CDC_Set_Pay_Calendar(ctx: RunContext) -> None:
    # lkp_CPM_NEWPAY_TBL source filter only checks BUSINESS_UNIT = 'CDC00'
    return _common.set_pay_calendar(ctx, "cpm_cdc_pay_period_file1.out", F.col("BUSINESS_UNIT") == "CDC00", LABEL)


def s_CPM_CDC_Load_CPM_CDC_Header_File(ctx: RunContext) -> None:
    """SQ_PAY_PERIOD (current pay period) -> exp_Initial constants -> cdchdr_WS_CDC_HDR (LRECL 2858)."""
    path = _common.out_file(ctx, ctx.param("CPM_CDC_HDR_filename", "CPM_CDC_HDR.txt"))
    write_records(_common.pay_period_rows(ctx), path, WS_CDC_HDR, ctx.params, LRECL)


def s_CPM_CDC_Load_CPM_CDC_Data_File(ctx: RunContext) -> None:
    """SQ_CPM_NEWPAY_TBL (CDC filter) -> exp_Init / exp_Convert / exp_Set_Defaults -> cdcskel_WS_PAY_OUT_REC (LRECL 2858)."""
    path = _common.out_file(ctx, ctx.param("CPM_CDC_DAT_filename", "CPM_CDC_DAT.txt"))
    write_records(_common.newpay_period_rows(ctx, _cdc()), path, WS_PAY_OUT_REC, ctx.params, LRECL)


def s_CPM_CDC_Concatenate_Files(ctx: RunContext) -> None:
    _common.generic_file(ctx)


def s_CPM_CDC_Build_Message(ctx: RunContext) -> dict:
    rows = _common.newpay_period_rows(ctx, _cdc())
    return _common.build_message(ctx, rows, LABEL, "cpm_cdc_message_file.txt", count_header=True)


def email_CPM_CDC(ctx: RunContext) -> dict:
    return _common.email(ctx)


TASKS = {
    "s_CPM_CDC_Set_CPM_Calendar": s_CPM_CDC_Set_CPM_Calendar,
    "s_CPM_CDC_Set_Pay_Calendar": s_CPM_CDC_Set_Pay_Calendar,
    "s_CPM_CDC_Load_CPM_CDC_Header_File": s_CPM_CDC_Load_CPM_CDC_Header_File,
    "s_CPM_CDC_Load_CPM_CDC_Data_File": s_CPM_CDC_Load_CPM_CDC_Data_File,
    "s_CPM_CDC_Concatenate_Files": s_CPM_CDC_Concatenate_Files,
    "s_CPM_CDC_Build_Message": s_CPM_CDC_Build_Message,
    "email_CPM_CDC": email_CPM_CDC,
}
