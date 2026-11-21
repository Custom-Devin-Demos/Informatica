from decimal import Decimal

import pytest

from biis_lakehouse.pipelines.reference import wf_pseudossn as wf

from ._fixtures import pay_periods, read_lines


def line(fields=wf.FILE_FIELDS, **vals):
    return "".join(str(vals.get(f.name, "")).ljust(f.length)[: f.length] for f in fields)


DETAIL = dict(EIN="E0000001", FIRST_NAME="JOHN", LAST_NAME="DOE", HIRE_DATE="01152020", UNIF_ALLOW_AMT="01234+",
              APPT_LIMIT_HRS="001050-", QUARTERS_DEDUCTION="0010000+", SUBSIST_DEDUCTION="0005000+",
              EFFECTIVE_SEQ="002", PCA_CONTR_YEAR="03", EMPLOYEE_STATUS="A")


def main_file(trailer_count="000004"):
    return [
        line(SSN="HEADER", CAN_CD="2024-01-", PSEUDO_SSN="25"),
        line(SSN="111111111", CAN_CD="CAN00001", PSEUDO_SSN="P00000001", EFFECTIVE_DATE="20240110", **DETAIL),
        line(SSN="222222222", CAN_CD="CAN00002", PSEUDO_SSN="P00000001", EFFECTIVE_DATE="20240101", **DETAIL),
        line(SSN="333333333", CAN_CD="", PSEUDO_SSN="P00000003", EFFECTIVE_DATE="20240110", **DETAIL),
        line(SSN="444444444", CAN_CD="CAN00004", PSEUDO_SSN="P00000004", EFFECTIVE_DATE="20240112", **DETAIL),
        line(SSN="TRAILER", CAN_CD=trailer_count),
    ]


def backup_file():
    f = wf.FILE_TK_NUM_FIELDS
    sda_detail = {**DETAIL, "FIRST_NAME": "NOT FROM EH", "MIDDLE_INIT": "R"}
    return [
        line(f, SSN="HEADER 20", CAN_CD="24-01-25"),
        line(f, SSN="111111111", CAN_CD="CAN00001", PSEUDO_SSN="P00000001", EFFECTIVE_DATE="20240110", TK_NUM="T0001", **DETAIL),
        line(f, SSN="555555555", CAN_CD="CAN00005", PSEUDO_SSN="P00000005", TK_NUM="T0005", **sda_detail),
        line(f, SSN="ABCDEFGHI", CAN_CD="CAN00006", PSEUDO_SSN="P00000006", TK_NUM="T0006", **DETAIL),
        line(f, SSN="TRAILER", CAN_CD="000002"),
    ]


def _write(ctx, name, lines):
    path = ctx.path("in", wf.AREA, name)
    ctx.spark.createDataFrame([(x,) for x in lines], "value string").coalesce(1).write.mode("overwrite").text(path)


@pytest.fixture
def setup(ctx):
    pay_periods(ctx)
    ctx.params.update({"PSEUDOSSN_file": "pseudossn.txt", "PSEUDOSSN_Backup_file": "pseudossn_bkup.txt",
                       "SESSSTARTTIME": "2024-01-26 07:00:00", "PMRepositoryServiceName": "Test_Repo"})
    _write(ctx, "pseudossn.txt", main_file())
    _write(ctx, "pseudossn_bkup.txt", backup_file())
    return ctx


def _tbl(ctx, name):
    return ctx.spark.table(ctx.table_name(name))


def test_task_order():
    assert list(wf.TASKS) == [
        "s_Pseudossn_Verify_Header_Date_Current_Pay_Period",
        "s_Pseudossn_Verify_Record_Count",
        "s_Pseudossn_Verify_Header_Date_Current_Pay_Period_Pseudossn_From_SDA",
        "s_Pseudossn_Current_Pay_Period",
        "s_Pseudossn_Load_Archive_Pseudossn_Tbl",
        "s_Pseudossn_Load_Pseudossn_Tbl",
        "s_Pseudossn_Load_Pseudossn_From_SDA_Tbl",
        "s_Pseudossn_Update_Timekeeper_Number",
        "s_Pseudossn_Load_SDA_Records_Pseudossn_Tbl",
        "s_Pseudossn_Counters",
        "Email_Pseudossn",
    ]


def test_layout_lengths():
    assert sum(f.length for f in wf.FILE_FIELDS) == 500
    assert sum(f.length for f in wf.FILE_TK_NUM_FIELDS) == 505


def test_verify_header_dates(setup):
    ctx = setup
    wf.TASKS["s_Pseudossn_Verify_Header_Date_Current_Pay_Period"](ctx)
    assert read_lines(ctx, ctx.path("out", wf.AREA, "pseudo_hdr_date.txt")) == ["20240125  "]
    wf.TASKS["s_Pseudossn_Verify_Header_Date_Current_Pay_Period_Pseudossn_From_SDA"](ctx)
    assert read_lines(ctx, ctx.path("out", wf.AREA, "pseudo_hdr_date_from_sda.txt")) == ["20240125  "]


def test_verify_header_aborts(setup):
    ctx = setup
    _write(ctx, "pseudossn.txt", [line(SSN="HEADER", CAN_CD="2024-03-", PSEUDO_SSN="01")])
    with pytest.raises(RuntimeError, match="not in the current pay period: 2024-02"):
        wf.TASKS["s_Pseudossn_Verify_Header_Date_Current_Pay_Period"](ctx)
    _write(ctx, "pseudossn.txt", [line(SSN="HEADER", CAN_CD="2024-13-", PSEUDO_SSN="01")])
    with pytest.raises(RuntimeError, match="is not a valid date"):
        wf.TASKS["s_Pseudossn_Verify_Header_Date_Current_Pay_Period"](ctx)


def test_verify_record_count(setup):
    ctx = setup
    wf.TASKS["s_Pseudossn_Verify_Record_Count"](ctx)
    lines = read_lines(ctx, ctx.path("out", wf.AREA, "pseudo_record_count.txt"))
    assert lines[0].startswith("TRAILER_REDETAIL_RECMESSAGE") and lines[1].split() == ["000004", "4", "Record", "Counts", "Match"]
    _write(ctx, "pseudossn.txt", main_file(trailer_count="000009"))
    with pytest.raises(RuntimeError, match="trailer: 000009 does not match the detail record count of the file: 4"):
        wf.TASKS["s_Pseudossn_Verify_Record_Count"](ctx)


def test_current_pay_period(setup):
    ctx = setup
    assert wf.TASKS["s_Pseudossn_Current_Pay_Period"](ctx) == {"WF_PP_YEAR_NUM": "202402"}
    assert read_lines(ctx, ctx.path("out", wf.AREA, "pay_period.txt")) == ["202402"]


def test_load_archive(setup):
    ctx = setup
    wf.TASKS["s_Pseudossn_Load_Pseudossn_Tbl"](ctx)
    wf.TASKS["s_Pseudossn_Load_Archive_Pseudossn_Tbl"](ctx)
    wf.TASKS["s_Pseudossn_Load_Archive_Pseudossn_Tbl"](ctx)
    assert _tbl(ctx, "HI_ARCH_PSEUDOSSN_TBL").count() == 4


def test_load_pseudossn_tbl_and_errors(setup):
    ctx = setup
    wf.TASKS["s_Pseudossn_Load_Pseudossn_Tbl"](ctx)
    rows = {r.SSN: r for r in _tbl(ctx, "PSEUDOSSN_TBL").collect()}
    assert sorted(rows) == ["111111111", "444444444"]
    r = rows["111111111"]
    assert r.PSEUDOSSN == "P00000001" and r.EMPLID == "E0000001" and int(r.PP_NUM) == 2
    assert str(r.HIRE_DATE.date()) == "2020-01-15" and str(r.EFFECTIVE_DATE.date()) == "2024-01-10"
    assert r.UNIF_ALLOW_AMT == Decimal("12.34") and r.APPT_LIMIT_HRS == Decimal("-11.00")
    assert r.SUBSIST_DED_AMT == Decimal("100.00")  # '+' branch returns v_QUARTERS_DED_AMT as exported
    assert int(r.EFFECTIVE_SEQ) == 2 and int(r.PCA_CONTR_LEN_YEAR) == 3 and r.TK_NUM is None
    errs = sorted(r.ERROR_MESSAGE for r in _tbl(ctx, "ERROR_TBL").collect())
    assert errs == ["A duplicate has been detected for Social Security: 222222222 and Can Code: CAN00002.",
                    "CAN_CD is not found."]
    # truncate-and-load on rerun
    wf.TASKS["s_Pseudossn_Load_Pseudossn_Tbl"](ctx)
    assert _tbl(ctx, "PSEUDOSSN_TBL").count() == 2 and _tbl(ctx, "ERROR_TBL").count() == 4


def test_load_from_sda_update_tk_and_sda_records(setup):
    ctx = setup
    wf.TASKS["s_Pseudossn_Load_Pseudossn_Tbl"](ctx)
    wf.TASKS["s_Pseudossn_Load_Pseudossn_From_SDA_Tbl"](ctx)
    sda = {r.PSEUDOSSN: r for r in _tbl(ctx, "PSEUDOSSN_FROM_SDA_TBL").collect()}
    assert sorted(sda) == ["P00000001", "P00000005"]  # non-numeric SSN filtered
    assert sda["P00000005"].TK_NUM == "T0005"
    assert str(sda["P00000005"].EFFECTIVE_DATE) == "2024-01-26 07:00:00"  # 'NOT FROM EH'/'R' -> SESSSTARTTIME

    wf.TASKS["s_Pseudossn_Update_Timekeeper_Number"](ctx)
    tk = {r.PSEUDOSSN: r.TK_NUM for r in _tbl(ctx, "PSEUDOSSN_TBL").collect()}
    assert tk == {"P00000001": "T0001", "P00000004": None}

    wf.TASKS["s_Pseudossn_Load_SDA_Records_Pseudossn_Tbl"](ctx)
    tk = {r.PSEUDOSSN: r.TK_NUM for r in _tbl(ctx, "PSEUDOSSN_TBL").collect()}
    assert tk == {"P00000001": "T0001", "P00000004": None, "P00000005": "T0005"}


def test_counters_and_email(setup):
    ctx = setup
    wf.TASKS["s_Pseudossn_Load_Pseudossn_Tbl"](ctx)
    out = wf.TASKS["s_Pseudossn_Counters"](ctx)
    vals = {r.COUNTER_DESCRIPTION: int(r.COUNTER_VALUE) for r in _tbl(ctx, "COUNTER_TBL").collect()}
    assert vals == {"Number of detail records from the PSEUDOSSN file.": 4,
                    "Number of detail records loaded to the PSEUDOSSN table.": 2,
                    "Number of duplicate records found.": 2}
    assert out["WF_SUBJECT"] == "Test: Pseudo SSN loaded successfully for Pay Period: 2024-02"
    assert out["WF_MESSAGE"].splitlines() == ["Number of Detail Records from PseudoSSN file = 4",
                                              "Number of Records loaded = 2", "Number of Duplicate Records = 2"]
    lines = read_lines(ctx, ctx.path("out", wf.AREA, "pseudossn_message_file.txt"))
    assert lines[0].startswith("SUBJECT")
    ctx.params.update(out)
    assert wf.TASKS["Email_Pseudossn"](ctx)["EMAIL_SUBJECT"] == out["WF_SUBJECT"]


def test_full_workflow(setup):
    ctx = setup
    for name, task in wf.TASKS.items():
        ctx.params.update({k: str(v) for k, v in (task(ctx) or {}).items()})
    assert _tbl(ctx, "PSEUDOSSN_TBL").count() == 3
    assert ctx.params["WF_PP_YEAR_NUM"] == "202402"
    assert ctx.params["EMAIL_SUBJECT"].endswith("2024-02")
