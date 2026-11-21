"""EHRP2BIIS pre/post-load SQL (``ehrp2biis_preload``, ``ehrp2biis_afterload.sql``, ``actstage_load``).

Run as ``biis-run --workflow ehrp2biis_sql --task preload|afterload|actstage_load``.

Job parameters (all optional):

- ``LOAD_DATE`` (``YYYY-MM-DD``): replaces ``trunc(sysdate)``; default ``current_date()`` in the session time zone.
- ``NEW_ACTION_WIP_STATUSES``: comma list of PS_GVT_JOB.GVT_WIP_STATUS values the preload picks up (default ``PRO``).
- ``ON_MISSING_PROCEDURE``: ``fail`` (default) or ``skip`` for Oracle procedures/scripts not ported yet.
- ``EHRP2BIIS_DEFAULT_SCHEMA``: schema for the afterload's unqualified tables (``*_ALL``, ``PROCESS_TABLE``); default ``biis``.
- ``SQL_DIR``: directory holding ``ehrp2biis/`` SQL (default: ``databricks/sql`` next to this package).
- ``SPOOL``: ``true`` (default) writes the SQL*Plus-style log to ``<landing_root>/log/EHRP2BIIS/``.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Union

from biis_lakehouse.context import DEFAULT_SCHEMA, OWNER_SCHEMAS, RunContext
from biis_lakehouse.sqlsteps.engine import Spool, SqlStepError, Statement, load_sql, run_statements

DEFAULT_SQL_DIR = Path(__file__).resolve().parents[3] / "sql"


@dataclass(frozen=True)
class Sql:
    """A ported SQL file, relative to ``<SQL_DIR>/ehrp2biis``."""

    path: str


@dataclass(frozen=True)
class Proc:
    """An Oracle procedure called with ``EXEC``; ported as ``procedures/<name lower-cased>.sql``."""

    name: str

    @property
    def path(self) -> str:
        return f"procedures/{self.name.lower()}.sql"


Step = Union[Sql, Proc]

PRELOAD_STEPS: list[Step] = [
    Sql("preload/01_clear_new_ehrp_actions.sql"),
    Sql("preload/02_select_new_ehrp_actions.sql"),
    Sql("preload/03_check_sequence_years.sql"),
    Sql("preload/04_prerun_report.sql"),
]

# Order of ehrp2biis_afterload.sql.
AFTERLOAD_STEPS: list[Step] = [
    Sql("afterload/01_reset_retained_step.sql"),
    Sql("afterload/02_show_sequence_num_tbl.sql"),
    Proc("UPDATE_SEQUENCE_NUMBER_TBL_P"),
    Sql("afterload/02_show_sequence_num_tbl.sql"),
    Proc("HISTDBA.UPDT_ERP2BIIS_CRE8_REMARKS01_P"),
    Proc("HISTDBA.UPDATE_ERP2BIIS_NO900S01_P"),
    Proc("HISTDBA.ERP2BIIS_CRE8_REMARKS_900S01"),
    Proc("HISTDBA.UPDATE_ERP2BIIS_900SONLY01_P"),
    Proc("HISTDBA.UPDT_ORIG_CANCELLED_TRANS01_P"),
    Sql("afterload/03_set_process_startdt.sql"),
    Proc("CHK_EHRP2BIIS_WIP_STATUS_P"),
    Sql("afterload/04_insert_todays_load.sql"),
    Proc("HISTDBA.GATHER_EHRP2BIIS_RUNCOUNTS_P"),
    Sql("afterload/05_replace_cancelled_actions.sql"),
    Sql("afterload/06_truncate_new_ehrp_actions.sql"),
]

ACTSTAGE_STEPS: list[Step] = [Sql("actstage/action_stage_load.sql")]


def _flag(value: str) -> bool:
    return value.strip().lower() in {"1", "true", "yes", "y"}


def _quote_list(csv: str) -> str:
    items = [s.strip() for s in csv.split(",") if s.strip()]
    if not items:
        raise SqlStepError("NEW_ACTION_WIP_STATUSES is empty")
    return ", ".join("'" + s.replace("'", "''") + "'" for s in items)


def _schema(ctx: RunContext, owner: str | None) -> str:
    return ctx.table_name("_", owner).rsplit(".", 1)[0]


def sql_variables(ctx: RunContext) -> dict[str, str]:
    variables = {owner.lower(): _schema(ctx, owner) for owner in OWNER_SCHEMAS}
    default_schema = ctx.param("EHRP2BIIS_DEFAULT_SCHEMA", DEFAULT_SCHEMA)
    variables["biis"] = f"{ctx.catalog}.{default_schema}" if ctx.catalog else default_schema
    load_date = ctx.param("LOAD_DATE", "")
    variables["load_date"] = f"DATE'{date.fromisoformat(load_date).isoformat()}'" if load_date else "current_date()"
    variables["new_action_wip_statuses"] = _quote_list(ctx.param("NEW_ACTION_WIP_STATUSES", "PRO"))
    return variables


def _sql_dir(ctx: RunContext) -> Path:
    return Path(ctx.param("SQL_DIR", str(DEFAULT_SQL_DIR))) / "ehrp2biis"


def _step_name(step: Step) -> str:
    return step.name if isinstance(step, Proc) else step.path


def run_steps(ctx: RunContext, task: str, steps: list[Step]) -> dict:
    """Run ``steps`` like the SQL*Plus wrapper: spool everything, fail the task on the first error."""
    on_missing = ctx.param("ON_MISSING_PROCEDURE", "fail").lower()
    if on_missing not in {"fail", "skip"}:
        raise SqlStepError(f"ON_MISSING_PROCEDURE must be fail or skip, got {on_missing!r}")

    base = _sql_dir(ctx)
    loaded: list[tuple[Step, list[Statement]]] = []
    unported: list[str] = []
    for step in steps:
        path = base / step.path
        statements = load_sql(path) if path.exists() else []
        if not statements:
            unported.append(_step_name(step))
        loaded.append((step, statements))
    # Fail before touching any table, unlike SQL*Plus which would have run up to the missing procedure.
    if unported and on_missing == "fail":
        raise SqlStepError(
            f"{task}: not ported to Databricks SQL yet: {', '.join(dict.fromkeys(unported))} "
            f"(see {base}; set ON_MISSING_PROCEDURE=skip to run without them)"
        )

    stamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    spool_path = ctx.path("log", "EHRP2BIIS", f"EHRP2BIIS_{task.upper()}_{stamp}.log") if _flag(ctx.param("SPOOL", "true")) else None
    spool = Spool(spool_path)
    variables = sql_variables(ctx)
    spool.write(f"{task} started {stamp}; load_date = {variables['load_date']}")
    metrics: dict[str, object] = {}
    try:
        for step, statements in loaded:
            name = _step_name(step)
            if not statements:
                spool.write(f"WARNING: skipped {name} (not ported)")
                continue
            spool.write(f"==== {name}")
            metrics.update(run_statements(ctx, statements, variables, spool, source=name))
        spool.write(f"{task} SUCCESS")
    finally:
        spool.close()

    prefix = f"EHRP2BIIS_{task.upper()}"
    result = {f"{prefix}_STATUS": "SUCCESS_WITH_SKIPS" if unported else "SUCCESS"}
    result[f"{prefix}_UNPORTED"] = ",".join(dict.fromkeys(unported))
    if spool_path:
        result[f"{prefix}_LOG"] = spool_path
    result.update({f"{prefix}_{k.upper()}": "" if v is None else str(v) for k, v in metrics.items()})
    return result


def preload(ctx: RunContext) -> dict:
    """``ehrp2biis_preload`` (SQL*Plus ``@step01``): build NWK_NEW_EHRP_ACTIONS_TBL and the pre-run report."""
    return run_steps(ctx, "preload", PRELOAD_STEPS)


def afterload(ctx: RunContext) -> dict:
    """``ehrp2biis_afterload.sql``: post-process today's wf_EHRP2BIIS_UPDATE load into the BIIS *_ALL tables."""
    return run_steps(ctx, "afterload", AFTERLOAD_STEPS)


def actstage_load(ctx: RunContext) -> dict:
    """``actstage_load`` (SQL*Plus ``@action_stage_load``)."""
    return run_steps(ctx, "actstage_load", ACTSTAGE_STEPS)


TASKS = {"preload": preload, "afterload": afterload, "actstage_load": actstage_load}
