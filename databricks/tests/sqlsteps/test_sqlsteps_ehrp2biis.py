import re
from datetime import date, timedelta
from pathlib import Path

import pytest

from biis_lakehouse import runner
from biis_lakehouse.sqlsteps import ehrp2biis
from biis_lakehouse.sqlsteps.engine import SqlStepError

REPO = Path(__file__).resolve().parents[3]
TODAY = date.today()
YESTERDAY = TODAY - timedelta(days=1)
ZERO_STEP = "0.0000000000000"

PRIMARY = "event_id BIGINT, emp_id STRING, event_eff_dte DATE, load_date DATE, gvt_wip_status STRING"
SECONDARY = "event_id BIGINT, retnd1_step_cd STRING, note STRING"
REMARKS = "event_id BIGINT, remark_txt STRING"
TRACKING = (
    "emplid STRING, empl_rcd INT, effdt DATE, effseq INT, event_submitted_dt DATE, gvt_wip_status STRING, "
    "noa_cd STRING, noa_suffix_cd STRING, biis_event_id BIGINT, load_date DATE, changed_wip_status STRING, "
    "biis_wip_status_changed_dt DATE"
)
GVT_JOB = "emplid STRING, empl_rcd INT, effdt DATE, effseq INT, gvt_wip_status STRING, gvt_noa_code STRING"
NEW_ACTIONS = "emplid STRING, empl_rcd INT, effdt DATE, effseq INT"
SEQ = "ehrp_year INT, ehrp_seq_number BIGINT"


@pytest.fixture(autouse=True)
def _small_delta_snapshots(spark):
    # Delta state reconstruction defaults to 50 partitions; tiny fixtures run much faster with 1.
    key = "spark.databricks.delta.snapshotPartitions"
    old = spark.conf.get(key, None)
    spark.conf.set(key, "1")
    yield
    if old is None:
        spark.conf.unset(key)
    else:
        spark.conf.set(key, old)


def _table(spark, name, schema, rows):
    spark.createDataFrame(rows, schema).write.format("delta").mode("overwrite").saveAsTable(name)


def _rows(spark, sql):
    return [tuple(r) for r in spark.sql(sql).collect()]


@pytest.fixture
def afterload_tables(ctx):
    spark = ctx.spark
    d24, d25 = date(2024, 3, 1), date(2025, 1, 15)
    _table(spark, "nknight.nwk_action_primary_tbl", PRIMARY, [
        (101, "E1", d24, TODAY, "PRO"),
        (102, "E2", d25, TODAY, "PRO"),
        (9000000001, "E9", d24, TODAY, "PRO"),
        (50, "E5-NEW", d24, YESTERDAY, "CAN"),  # cancelled in eHRP since it was loaded yesterday
        (40, "E4", d24, YESTERDAY, "PRO"),
    ])
    _table(spark, "nknight.nwk_action_secondary_tbl", SECONDARY, [
        (101, ZERO_STEP, "s101"),
        (102, "1", "s102"),
        (9000000001, ZERO_STEP, "s9"),
        (50, ZERO_STEP, "s50-new"),
        (40, ZERO_STEP, "s40"),
    ])
    _table(spark, "nknight.nwk_action_remarks_tbl", REMARKS, [(101, "r101"), (50, "r50-new")])
    _table(spark, "nknight.nwk_new_ehrp_actions_tbl", NEW_ACTIONS, [("E1", 0, d24, 1), ("E2", 0, d25, 1)])
    _table(spark, "nknight.ehrp_recs_tracking_tbl", TRACKING, [
        ("E1", 0, d24, 1, d24, "PRO", "100", "0", 101, TODAY, None, None),
        ("E5", 0, d24, 1, d24, "PRO", "100", "0", 50, YESTERDAY, None, None),
        ("E4", 0, date(2024, 2, 1), 1, d24, "PRO", "100", "0", 40, YESTERDAY, "CAN", YESTERDAY),
    ])
    _table(spark, "ehrp.ps_gvt_job", GVT_JOB, [
        ("E1", 0, d24, 1, "PRO", "100"),
        ("E5", 0, d24, 1, "CAN", "100"),
        ("E4", 0, date(2024, 2, 1), 1, "PRO", "100"),
    ])
    _table(spark, "nknight.sequence_num_tbl", SEQ, [(2024, 100)])
    _table(spark, "biis.process_table", "p_startdt DATE", [(date(2000, 1, 1),)])
    _table(spark, "biis.action_primary_all", PRIMARY, [(50, "E5-OLD", d24, YESTERDAY, "PRO"), (40, "E4", d24, YESTERDAY, "PRO")])
    _table(spark, "biis.action_secondary_all", SECONDARY, [(50, ZERO_STEP, "s50-old"), (40, ZERO_STEP, "s40")])
    _table(spark, "biis.action_remarks_all", REMARKS, [(50, "r50-old")])
    ctx.params["ON_MISSING_PROCEDURE"] = "skip"
    return ctx


def test_registered_in_runner():
    assert runner.WORKFLOW_MODULES["ehrp2biis_sql"] == "biis_lakehouse.sqlsteps.ehrp2biis"
    assert set(runner.load_tasks("ehrp2biis_sql")) == {"preload", "afterload", "actstage_load"}


def test_afterload_steps_cover_every_exec_in_original_script():
    original = (REPO / "ehrp2biis_afterload.sql").read_text()
    called = {
        m.group(1).upper()
        for m in re.finditer(r"^\s*(?:EXEC|execute)\s+([\w.]+)", original, re.IGNORECASE | re.MULTILINE)
    }
    ported = {s.name for s in ehrp2biis.AFTERLOAD_STEPS if isinstance(s, ehrp2biis.Proc)}
    assert called == ported
    for step in ehrp2biis.PRELOAD_STEPS + ehrp2biis.AFTERLOAD_STEPS + ehrp2biis.ACTSTAGE_STEPS:
        assert (ehrp2biis.DEFAULT_SQL_DIR / "ehrp2biis" / step.path).exists(), step


def test_afterload(afterload_tables):
    ctx = afterload_tables
    spark = ctx.spark
    result = ehrp2biis.afterload(ctx)

    assert result["EHRP2BIIS_AFTERLOAD_STATUS"] == "SUCCESS_WITH_SKIPS"
    assert result["EHRP2BIIS_AFTERLOAD_UNPORTED"].split(",") == [
        "HISTDBA.UPDT_ERP2BIIS_CRE8_REMARKS01_P",
        "HISTDBA.UPDATE_ERP2BIIS_NO900S01_P",
        "HISTDBA.ERP2BIIS_CRE8_REMARKS_900S01",
        "HISTDBA.UPDATE_ERP2BIIS_900SONLY01_P",
        "HISTDBA.UPDT_ORIG_CANCELLED_TRANS01_P",
    ]

    # Step 04: only today's non-900s events lose the '0.0000000000000' retained step.
    assert dict(_rows(spark, "SELECT event_id, retnd1_step_cd FROM nknight.nwk_action_secondary_tbl")) == {
        101: None, 102: "1", 9000000001: ZERO_STEP, 50: ZERO_STEP, 40: ZERO_STEP,
    }

    # update_sequence_number_tbl_p: max event id per EFFDT year, 900s excluded.
    assert sorted(_rows(spark, "SELECT * FROM nknight.sequence_num_tbl")) == [(2024, 101), (2025, 102)]

    # PROCESS_TABLE: first changed-status effdt (E5's 2024-03-01; E4 was already flagged).
    assert _rows(spark, "SELECT p_startdt FROM biis.process_table") == [(date(2024, 3, 1),)]

    # chk_ehrp2biis_wip_status_p flags E5 (PRO -> CAN) with today's date, leaves E4's earlier flag.
    tracking = {r[0]: r[1:] for r in _rows(
        spark, "SELECT biis_event_id, changed_wip_status, biis_wip_status_changed_dt FROM nknight.ehrp_recs_tracking_tbl"
    )}
    assert tracking == {101: (None, None), 50: ("CAN", TODAY), 40: ("CAN", YESTERDAY)}

    # _all inserts for today's load_date + cancelled-action delete/re-insert of event 50.
    assert sorted(_rows(spark, "SELECT event_id, emp_id FROM biis.action_primary_all")) == [
        (40, "E4"), (50, "E5-NEW"), (101, "E1"), (102, "E2"), (9000000001, "E9"),
    ]
    assert sorted(_rows(spark, "SELECT event_id, note FROM biis.action_secondary_all")) == [
        (40, "s40"), (50, "s50-new"), (101, "s101"), (102, "s102"), (9000000001, "s9"),
    ]
    assert sorted(_rows(spark, "SELECT event_id, remark_txt FROM biis.action_remarks_all")) == [
        (50, "r50-new"), (101, "r101"),
    ]

    # Truncate of nwk_new_ehrp_actions_tbl.
    assert spark.table("nknight.nwk_new_ehrp_actions_tbl").count() == 0

    # GATHER_EHRP2BIIS_RUNCOUNTS_P replacement (gathered before the cancelled-action block, as in the original).
    counts = dict(_rows(spark, f"SELECT table_name, row_count FROM histdba.ehrp2biis_runcounts WHERE load_date = DATE'{TODAY}'"))
    assert counts["NWK_ACTION_PRIMARY_TBL"] == 3
    assert counts["ACTION_PRIMARY_ALL"] == 3
    assert counts["NWK_NEW_EHRP_ACTIONS_TBL"] == 2

    log = Path(result["EHRP2BIIS_AFTERLOAD_LOG"]).read_text()
    assert "afterload SUCCESS" in log and "WARNING: skipped HISTDBA.UPDT_ORIG_CANCELLED_TRANS01_P" in log


def test_afterload_load_date_param_replaces_sysdate(afterload_tables):
    ctx = afterload_tables
    ctx.params["LOAD_DATE"] = YESTERDAY.isoformat()
    ehrp2biis.afterload(ctx)
    # Yesterday's events are appended, then both were status-changed "yesterday" (40 already flagged, 50 by
    # chk_ehrp2biis_wip_status_p) so the cancelled-action block replaces them with one current copy each.
    assert sorted(_rows(ctx.spark, "SELECT event_id, emp_id FROM biis.action_primary_all")) == [(40, "E4"), (50, "E5-NEW")]
    assert sorted(_rows(ctx.spark, "SELECT event_id FROM biis.action_secondary_all")) == [(40,), (50,)]
    assert dict(_rows(ctx.spark, "SELECT event_id, retnd1_step_cd FROM nknight.nwk_action_secondary_tbl"))[40] is None


def test_afterload_fails_up_front_when_procedures_not_ported(afterload_tables):
    ctx = afterload_tables
    ctx.params["ON_MISSING_PROCEDURE"] = "fail"
    with pytest.raises(SqlStepError, match="HISTDBA.UPDT_ERP2BIIS_CRE8_REMARKS01_P"):
        ehrp2biis.afterload(ctx)
    # Nothing ran: step 04 did not touch the secondary table and the driver table is intact.
    assert dict(_rows(ctx.spark, "SELECT event_id, retnd1_step_cd FROM nknight.nwk_action_secondary_tbl"))[101] == ZERO_STEP
    assert ctx.spark.table("nknight.nwk_new_ehrp_actions_tbl").count() == 2


def test_afterload_sql_error_fails_task(afterload_tables):
    ctx = afterload_tables
    ctx.spark.sql("DROP TABLE biis.action_remarks_all")
    with pytest.raises(SqlStepError, match="insert into action_remarks_all"):
        ehrp2biis.afterload(ctx)
    log_dir = Path(ctx.path("log", "EHRP2BIIS"))
    assert "ERROR at afterload: insert into action_remarks_all" in next(log_dir.glob("*AFTERLOAD*")).read_text()


@pytest.fixture
def preload_tables(ctx):
    spark = ctx.spark
    d24, d25 = date(2024, 3, 1), date(2025, 1, 15)
    _table(spark, "ehrp.ps_gvt_job", GVT_JOB, [
        ("E1", 0, d24, 1, "PRO", "100"),  # already tracked
        ("E2", 0, d24, 1, "PRO", "702"),
        ("E3", 0, d25, 2, "PRO", "100"),
        ("E4", 0, d25, 1, "CAN", "100"),  # not a new processed action
    ])
    _table(spark, "nknight.ehrp_recs_tracking_tbl", TRACKING, [
        ("E1", 0, d24, 1, d24, "PRO", "100", "0", 101, YESTERDAY, None, None),
    ])
    _table(spark, "nknight.nwk_new_ehrp_actions_tbl", NEW_ACTIONS, [("STALE", 0, d24, 9)])
    _table(spark, "nknight.sequence_num_tbl", SEQ, [(2024, 101), (2025, 0)])
    return ctx


def test_preload_builds_new_actions(preload_tables):
    ctx = preload_tables
    result = ehrp2biis.preload(ctx)
    assert result["EHRP2BIIS_PRELOAD_STATUS"] == "SUCCESS"
    assert result["EHRP2BIIS_PRELOAD_NEW_ACTIONS"] == "2"
    assert sorted(_rows(ctx.spark, "SELECT emplid, effseq FROM nknight.nwk_new_ehrp_actions_tbl")) == [("E2", 1), ("E3", 2)]
    assert "new actions by NOA code" in Path(result["EHRP2BIIS_PRELOAD_LOG"]).read_text()


def test_preload_wip_status_param(preload_tables):
    ctx = preload_tables
    ctx.params["NEW_ACTION_WIP_STATUSES"] = "PRO, CAN"
    assert ehrp2biis.preload(ctx)["EHRP2BIIS_PRELOAD_NEW_ACTIONS"] == "3"


def test_preload_fails_when_sequence_year_missing(preload_tables):
    ctx = preload_tables
    ctx.spark.sql("DELETE FROM nknight.sequence_num_tbl WHERE ehrp_year = 2025")
    with pytest.raises(SqlStepError, match="SEQUENCE_NUM_TBL has no EHRP_YEAR"):
        ehrp2biis.preload(ctx)


def test_actstage_load_not_ported(ctx):
    with pytest.raises(SqlStepError, match="actstage/action_stage_load.sql"):
        ehrp2biis.actstage_load(ctx)
    ctx.params["ON_MISSING_PROCEDURE"] = "skip"
    result = ehrp2biis.actstage_load(ctx)
    assert result["EHRP2BIIS_ACTSTAGE_LOAD_STATUS"] == "SUCCESS_WITH_SKIPS"


def test_actstage_load_runs_dropped_in_script(ctx, tmp_path):
    sql_dir = tmp_path / "sql"
    (sql_dir / "ehrp2biis" / "actstage").mkdir(parents=True)
    (sql_dir / "ehrp2biis" / "actstage" / "action_stage_load.sql").write_text(
        "CREATE TABLE ${biis}.action_stage (event_id BIGINT) USING DELTA;\n"
        "INSERT INTO ${biis}.action_stage VALUES (1), (2);\n"
        "-- @metric staged\nSELECT count(*) FROM ${biis}.action_stage;\n"
    )
    ctx.params.update({"SQL_DIR": str(sql_dir), "SPOOL": "false"})
    result = ehrp2biis.actstage_load(ctx)
    assert result["EHRP2BIIS_ACTSTAGE_LOAD_STATUS"] == "SUCCESS"
    assert result["EHRP2BIIS_ACTSTAGE_LOAD_STAGED"] == "2"
    assert "EHRP2BIIS_ACTSTAGE_LOAD_LOG" not in result


def test_catalog_and_schema_parameters(spark):
    from biis_lakehouse.context import RunContext

    ctx = RunContext(spark=spark, catalog="biis_prod", params={"EHRP2BIIS_DEFAULT_SCHEMA": "histdba", "LOAD_DATE": "2024-05-06"})
    v = ehrp2biis.sql_variables(ctx)
    assert (v["nknight"], v["ehrp"], v["histdba"], v["biis"]) == (
        "biis_prod.nknight", "biis_prod.ehrp", "biis_prod.histdba", "biis_prod.histdba",
    )
    assert v["load_date"] == "DATE'2024-05-06'"
    assert v["new_action_wip_statuses"] == "'PRO'"
