import pytest

from biis_lakehouse.pipelines.reference import wf_pay_calendar as wf

from ._fixtures import pay_periods, read_lines


def _current(ctx, tbl):
    return [(int(r.PP_NUM), int(r.PP_END_YEAR)) for r in ctx.spark.table(tbl).filter("CURR_PP_FLAG = 'Y'").collect()]


def test_task_order():
    assert list(wf.TASKS) == [
        "s_Pay_Calendar_Reset_Pay_Calendar",
        "s_Pay_Calendar_Set_Pay_Calendar",
        "s_Pay_Calendar_Verify_Pay_Calendar",
        "s_Pay_Calendar_Build_Message",
        "Email_Pay_Calendar",
    ]


def test_reset_clears_current_flag(ctx):
    tbl = pay_periods(ctx)
    wf.TASKS["s_Pay_Calendar_Reset_Pay_Calendar"](ctx)
    assert _current(ctx, tbl) == []
    assert ctx.spark.table(tbl).count() == 4


def test_set_with_parameters(ctx):
    tbl = pay_periods(ctx, current=None)
    ctx.params.update({"WF_PP_END_YEAR": "2024", "WF_PP_NUM": "3"})
    out = wf.TASKS["s_Pay_Calendar_Set_Pay_Calendar"](ctx)
    assert _current(ctx, tbl) == [(3, 2024)]
    assert out == {"WF_PP_END_YEAR": "2024", "WF_PP_NUM": "3"}


def test_set_without_parameters_uses_session_date(ctx):
    tbl = pay_periods(ctx, current=None)
    ctx.params["SESSSTARTTIME"] = "2024-01-25 06:00:00"  # inside pay period 2 (01-21..02-03)
    wf.TASKS["s_Pay_Calendar_Set_Pay_Calendar"](ctx)
    assert _current(ctx, tbl) == [(2, 2024)]


def test_set_with_unknown_parameters_falls_back_to_date(ctx):
    tbl = pay_periods(ctx, current=None)
    ctx.params.update({"WF_PP_END_YEAR": "2030", "WF_PP_NUM": "9", "SESSSTARTTIME": "2024-02-10 00:00:00"})
    wf.TASKS["s_Pay_Calendar_Set_Pay_Calendar"](ctx)
    assert _current(ctx, tbl) == [(3, 2024)]


def test_verify_writes_file_when_one_current(ctx):
    pay_periods(ctx)
    wf.TASKS["s_Pay_Calendar_Verify_Pay_Calendar"](ctx)
    lines = read_lines(ctx, ctx.path("out", wf.AREA, "pay_period_verify_file.txt"))
    assert len(lines) == 1 and lines[0].startswith("!!!! Current flag has been pro")


def test_verify_aborts_on_none_or_many(ctx):
    pay_periods(ctx, current=None)
    with pytest.raises(RuntimeError, match="no pay period set"):
        wf.TASKS["s_Pay_Calendar_Verify_Pay_Calendar"](ctx)
    from decimal import Decimal
    pay_periods(ctx, extra_current=[(Decimal(3), Decimal(2024))])
    with pytest.raises(RuntimeError, match="more than one"):
        wf.TASKS["s_Pay_Calendar_Verify_Pay_Calendar"](ctx)


def test_build_message_and_email(ctx):
    pay_periods(ctx)
    ctx.params["PMRepositoryServiceName"] = "Prod_Repo"
    out = wf.TASKS["s_Pay_Calendar_Build_Message"](ctx)
    assert out["WF_SUBJECT"] == "Prod: Pay Calendar Process Completed Successfully for: 2024-02"
    assert out["WF_MESSAGE"].splitlines() == [
        "Current Pay Period = 02",
        "Begin Date         = 01/21/2024",
        "End Date           = 02/03/2024",
    ]
    lines = read_lines(ctx, ctx.path("out", wf.AREA, "pay_period_message_file.txt"))
    assert lines[0].startswith("SUBJECT") and len(lines) >= 2
    ctx.params.update(out)
    ctx.params["WF_PAY_CALENDAR_EMAIL_LIST"] = "ops@example.com"
    assert wf.TASKS["Email_Pay_Calendar"](ctx)["EMAIL_SUBJECT"] == out["WF_SUBJECT"]
