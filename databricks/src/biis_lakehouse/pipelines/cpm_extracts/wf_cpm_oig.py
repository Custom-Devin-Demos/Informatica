"""wf_CPM_OIG (XML/CPM_OIG): CPM payroll extract for the Office of Inspector General (BUSINESS_UNIT 'OIG00').

Session chain: s_CPM_OIG_Set_CPM_Calendar -> s_CPM_OIG_Set_Pay_Calendar -> s_CPM_OIG_Load_CPM_OIG_File
-> s_CPM_OIG_Build_Message -> email_CPM_OIG.

Session commands (owned by the orchestration unit, ``biis_lakehouse.ops``):

* s_CPM_OIG_Load_CPM_OIG_File pre: ``remove_file.sh $Param_Root_Directory/data/int/out/CPM $Param_CPM_OIG_out_filename``
* s_CPM_OIG_Build_Message post: ``cp $Param_CPM_OIG_out_filename OIG.CPM.PP$$WF_PP_YEAR_NUM.TXT`` and
  ``oig_transfer.sh OIG.CPM.PP$$WF_PP_YEAR_NUM.TXT``
"""
from __future__ import annotations

from pyspark.sql import functions as F

from biis_lakehouse.context import RunContext
from biis_lakehouse.pipelines.cpm_extracts import _common
from biis_lakehouse.pipelines.cpm_extracts._records import write_records
from biis_lakehouse.pipelines.cpm_extracts._spec_cpm_oig import SPECS

LABEL = "OIG"
SKPAYROLL_MASTER = SPECS["m_CPM_OIG_Load_CPM_OIG_File"]["oigsgndec_SKPAYROLL_MASTER"]["fields"]
LRECL = 726


def _oig(c=F.col):
    # CPM_NEWPAY_TBL.BUSINESS_UNIT = 'OIG00'   (--CPM_NEWPAY_TBL.ORG_CDE LIKE 'AF%' is commented out)
    return c("BUSINESS_UNIT") == "OIG00"


def s_CPM_OIG_Set_CPM_Calendar(ctx: RunContext) -> dict:
    return _common.set_cpm_calendar(ctx, "cpm_oig_cpm_pay_period_file.txt")


def s_CPM_OIG_Set_Pay_Calendar(ctx: RunContext) -> None:
    # lkp_CPM_NEWPAY_TBL source filter: BUSINESS_UNIT = 'OIG00'
    return _common.set_pay_calendar(ctx, "cpm_oig_pay_period_file.txt", _oig(), LABEL)


def s_CPM_OIG_Load_CPM_OIG_File(ctx: RunContext) -> None:
    """SQ_CPM_NEWPAY_TBL -> exp_Init / exp_Set_Defaults / exp_Convert -> exp_Final -> UPDTRANS (``0`` = DD_INSERT)
    -> oigsgndec_SKPAYROLL_MASTER (PowerExchange sequential file, LRECL 726, RECFM FB).

    The Update Strategy is a constant DD_INSERT into a file target, so every row is written."""
    rows = _common.newpay_period_rows(ctx, _oig())
    path = _common.out_file(ctx, ctx.param("CPM_OIG_out_filename", "CPM_OIG_out.txt"))
    write_records(rows, path, SKPAYROLL_MASTER, ctx.params, LRECL)


def s_CPM_OIG_Build_Message(ctx: RunContext) -> dict:
    rows = _common.newpay_period_rows(ctx, _oig())
    return _common.build_message(ctx, rows, LABEL, "cpm_oig_message_file.txt", count_header=False)


def email_CPM_OIG(ctx: RunContext) -> dict:
    return _common.email(ctx)


TASKS = {
    "s_CPM_OIG_Set_CPM_Calendar": s_CPM_OIG_Set_CPM_Calendar,
    "s_CPM_OIG_Set_Pay_Calendar": s_CPM_OIG_Set_Pay_Calendar,
    "s_CPM_OIG_Load_CPM_OIG_File": s_CPM_OIG_Load_CPM_OIG_File,
    "s_CPM_OIG_Build_Message": s_CPM_OIG_Build_Message,
    "email_CPM_OIG": email_CPM_OIG,
}
