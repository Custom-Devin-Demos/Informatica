"""wf_CPM_NIH (XML/CPM_NIH): CPM payroll master extract for NIH (BUSINESS_UNIT 'NIH00').

Session chain: s_CPM_NIH_Set_CPM_Calendar -> s_CPM_NIH_Set_Pay_Calendar -> s_CPM_NIH_Load_CPM_NIH_Header_File
-> s_CPM_NIH_Load_CPM_NIH_Data_File -> s_CPM_NIH_Concatenate_Files -> s_CPM_NIH_Build_Message -> email_CPM_NIH.

Session commands (owned by the orchestration unit, ``biis_lakehouse.ops``):

* Header / Data / Concatenate pre: ``remove_file.sh`` of ``$Param_CPM_NIH_HDR_filename``, ``$Param_CPM_NIH_DAT_filename``,
  ``$Param_CPM_NIH_out_filename`` in ``data/int/out/CPM``
* s_CPM_NIH_Concatenate_Files post: ``cat $Param_CPM_NIH_HDR_filename $Param_CPM_NIH_DAT_filename > $Param_CPM_NIH_out_filename``
* s_CPM_NIH_Build_Message post: ``cp $Param_CPM_NIH_out_filename NIHPAYMASTER.PP$$WF_PP_YEAR_NUM`` and
  ``nih_cpm_transfer.sh NIHPAYMASTER.PP$$WF_PP_YEAR_NUM``
"""
from __future__ import annotations

from pyspark.sql import functions as F

from biis_lakehouse.context import RunContext
from biis_lakehouse.pipelines.cpm_extracts import _common
from biis_lakehouse.pipelines.cpm_extracts._records import write_records
from biis_lakehouse.pipelines.cpm_extracts._spec_cpm_nih import SPECS

LABEL = "NIH"
WS_NIH_HDR = SPECS["m_CPM_NIH_Load_CPM_NIH_Header_File"]["nihhdr_WS_NIH_HDR"]["fields"]
NIH_PAYROLL_MASTER = SPECS["m_CPM_NIH_Load_CPM_NIH_Data_File"]["nihtest_NIH_PAYROLL_MASTER"]["fields"]
HDR_LRECL, DAT_LRECL = 6000, 3325


def _nih():
    return F.col("BUSINESS_UNIT") == "NIH00"


def s_CPM_NIH_Set_CPM_Calendar(ctx: RunContext) -> dict:
    return _common.set_cpm_calendar(ctx, "cpm_nih_cpm_pay_period_file.txt")


def s_CPM_NIH_Set_Pay_Calendar(ctx: RunContext) -> None:
    return _common.set_pay_calendar(ctx, "cpm_nih_pay_period_file.txt", _nih(), LABEL)


def s_CPM_NIH_Load_CPM_NIH_Header_File(ctx: RunContext) -> None:
    """SQ_PAY_PERIOD (current pay period) -> exp_Initial constants -> nihhdr_WS_NIH_HDR (LRECL 6000)."""
    path = _common.out_file(ctx, ctx.param("CPM_NIH_HDR_filename", "CPM_NIH_HDR.txt"))
    write_records(_common.pay_period_rows(ctx), path, WS_NIH_HDR, ctx.params, HDR_LRECL)


def s_CPM_NIH_Load_CPM_NIH_Data_File(ctx: RunContext) -> None:
    """SQ_CPM_NEWPAY_TBL -> exp_Init / exp_Convert / exp_Set_Defaults -> nihtest_NIH_PAYROLL_MASTER (LRECL 3325).

    The SQ's *User Defined Join* holds the filter (single source), applied as a filter here."""
    path = _common.out_file(ctx, ctx.param("CPM_NIH_DAT_filename", "CPM_NIH_DAT.txt"))
    write_records(_common.newpay_period_rows(ctx, _nih()), path, NIH_PAYROLL_MASTER, ctx.params, DAT_LRECL)


def s_CPM_NIH_Concatenate_Files(ctx: RunContext) -> None:
    _common.generic_file(ctx)


def s_CPM_NIH_Build_Message(ctx: RunContext) -> dict:
    rows = _common.newpay_period_rows(ctx, _nih())
    return _common.build_message(ctx, rows, LABEL, "cpm_nih_message_file.txt", count_header=True)


def email_CPM_NIH(ctx: RunContext) -> dict:
    return _common.email(ctx)


TASKS = {
    "s_CPM_NIH_Set_CPM_Calendar": s_CPM_NIH_Set_CPM_Calendar,
    "s_CPM_NIH_Set_Pay_Calendar": s_CPM_NIH_Set_Pay_Calendar,
    "s_CPM_NIH_Load_CPM_NIH_Header_File": s_CPM_NIH_Load_CPM_NIH_Header_File,
    "s_CPM_NIH_Load_CPM_NIH_Data_File": s_CPM_NIH_Load_CPM_NIH_Data_File,
    "s_CPM_NIH_Concatenate_Files": s_CPM_NIH_Concatenate_Files,
    "s_CPM_NIH_Build_Message": s_CPM_NIH_Build_Message,
    "email_CPM_NIH": email_CPM_NIH,
}
