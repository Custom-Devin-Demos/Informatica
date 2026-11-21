from biis_lakehouse.pipelines.reference import wf_comptime as wf

from ._fixtures import pay_periods, read_lines

ROWS = [
    '"123456789","DOE JOHN","AC0001","ORG0001","N",12.50,2024,"20240203","20240125",45.10,2.00,0',
    '"234567890","ROE JANE","AC0002","ORG0002","E",0,2024,"20240203","2024XX01",50.00,1.50,0',
    '"HEADER   ","FILE HDR","","","",,,"","",,,',
    '"345678901","POE ED","AC0003","ORG0003","N",3,2023,"20240203","20240130",40.00,4.25,1',
]


def _setup(ctx, rows=ROWS):
    pay_periods(ctx)
    ctx.params.update({"COMPTIME_filename": "u0287d01.txt", "SESSSTARTTIME": "2024-01-31 05:00:00",
                       "PMRepositoryServiceName": "Dev_Repo"})
    path = ctx.path("in", wf.AREA, "u0287d01.txt")
    ctx.spark.createDataFrame([(r,) for r in rows], "value string").coalesce(1).write.mode("overwrite").text(path)


def test_task_order():
    assert list(wf.TASKS) == [
        "s_COMPTIME_Current_Pay_Period",
        "s_COMPTIME_Load_COMP_TIME_DAILY_TBL",
        "s_COMPTIME_Build_Message_Counters",
        "email_COMPTIME_Complete",
    ]


def test_current_pay_period(ctx):
    _setup(ctx)
    out = wf.TASKS["s_COMPTIME_Current_Pay_Period"](ctx)
    assert out == {"WF_PP_YEAR_NUM": "202402", "WF_PP_END_YEAR": 2024, "WF_PP_NUM": 2}
    assert read_lines(ctx, ctx.path("out", wf.AREA, "comp_time_date_file.txt")) == ["202402"]


def test_load_daily_table(ctx):
    _setup(ctx)
    wf.TASKS["s_COMPTIME_Load_COMP_TIME_DAILY_TBL"](ctx)
    df = ctx.spark.table(ctx.table_name("COMP_TIME_DAILY_TBL"))
    assert df.count() == 3
    rows = {r.SSN: r for r in df.collect()}
    assert set(rows) == {"123456789", "234567890", "345678901"}
    r = rows["123456789"]
    assert int(r.PP_YEAR_NUM) == 202402 and int(r.PP_NUM) == 2 and str(r.COMP_TIME_CUR_BAL) == "12.50"
    assert str(r.DAILY_DATE_EARNED.date()) == "2024-01-25"
    assert rows["234567890"].DAILY_DATE_EARNED is None


def test_build_message_counters_and_email(ctx):
    _setup(ctx)
    out = wf.TASKS["s_COMPTIME_Build_Message_Counters"](ctx)
    c = ctx.spark.table(ctx.table_name("COUNTER_TBL")).collect()
    assert len(c) == 1 and int(c[0].COUNTER_VALUE) == 3
    assert c[0].PROCESS_NAME == "m_COMPTIME_Build_Message_Counters"
    assert out["WF_SUBJECT"] == "Dev: Comp Time File loaded successfully for Pay Period:  2024-02"
    assert out["WF_MESSAGE"] == "Number of Detail Records from Comp Time file\t= 3"
    assert len(read_lines(ctx, ctx.path("out", wf.AREA, "comptime_message_file.txt"))) == 1
    ctx.params.update(out)
    assert wf.TASKS["email_COMPTIME_Complete"](ctx)["EMAIL_TEXT"] == out["WF_MESSAGE"]


def test_build_message_counters_empty_file(ctx):
    _setup(ctx, rows=['"HEADER   ","x","","","",,,"","",,,'])
    assert wf.TASKS["s_COMPTIME_Build_Message_Counters"](ctx) == {}
    assert ctx.spark.table(ctx.table_name("COUNTER_TBL")).count() == 0
