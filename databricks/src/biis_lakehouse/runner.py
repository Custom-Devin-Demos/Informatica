"""Entry point used by Databricks Workflows tasks: ``biis-run --workflow wf_CPM --task s_CPM_Current_Pay_Period``."""
from __future__ import annotations

import argparse
import importlib
import json
from typing import Sequence

from biis_lakehouse.context import RunContext, Task

# PowerCenter workflow (or auxiliary step group) -> module exposing ``TASKS: dict[str, Task]``.
WORKFLOW_MODULES = {
    "wf_Pay_Calendar": "biis_lakehouse.pipelines.reference.wf_pay_calendar",
    "wf_Pseudossn": "biis_lakehouse.pipelines.reference.wf_pseudossn",
    "wf_COMPTIME": "biis_lakehouse.pipelines.reference.wf_comptime",
    "wf_CPM": "biis_lakehouse.pipelines.cpm.wf_cpm",
    "wf_CPM_AFPS": "biis_lakehouse.pipelines.cpm_extracts.wf_cpm_afps",
    "wf_CPM_CDC": "biis_lakehouse.pipelines.cpm_extracts.wf_cpm_cdc",
    "wf_CPM_NIH": "biis_lakehouse.pipelines.cpm_extracts.wf_cpm_nih",
    "wf_CPM_OIG": "biis_lakehouse.pipelines.cpm_extracts.wf_cpm_oig",
    "wf_FDA_Leave": "biis_lakehouse.pipelines.cpm_extracts.wf_fda_leave",
    "wf_LES": "biis_lakehouse.pipelines.les.wf_les",
    "wf_EHRP2BIIS_UPDATE": "biis_lakehouse.pipelines.ehrp.wf_ehrp2biis_update",
    "ehrp2biis_sql": "biis_lakehouse.sqlsteps.ehrp2biis",
    "ops": "biis_lakehouse.ops.tasks",
}


def load_tasks(workflow: str) -> dict[str, Task]:
    return importlib.import_module(WORKFLOW_MODULES[workflow]).TASKS


def _parse_params(items: Sequence[str]) -> dict[str, str]:
    out = {}
    for item in items:
        key, _, value = item.partition("=")
        out[key.removeprefix("$$").removeprefix("$Param_")] = value
    return out


def _dbutils(spark):
    try:
        from pyspark.dbutils import DBUtils  # type: ignore[import-not-found]

        return DBUtils(spark)
    except ImportError:
        return None


def main(argv: Sequence[str] | None = None) -> dict | None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--workflow", required=True)
    ap.add_argument("--task", required=True)
    ap.add_argument("--catalog")
    ap.add_argument("--landing-root", default="/Volumes/biis/landing/files")
    ap.add_argument("--param", action="append", default=[], help="KEY=VALUE, repeatable")
    ap.add_argument("--upstream-task", action="append", default=[], help="task key whose task values are merged into params")
    args = ap.parse_args(argv)

    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()
    params = _parse_params(args.param)
    dbutils = _dbutils(spark)
    if dbutils is not None:
        for upstream in args.upstream_task:
            raw = dbutils.jobs.taskValues.get(taskKey=upstream, key="workflow_vars", default="{}")
            params.update(json.loads(raw))

    ctx = RunContext(spark=spark, catalog=args.catalog, landing_root=args.landing_root, params=params)
    result = load_tasks(args.workflow)[args.task](ctx)
    if result and dbutils is not None:
        merged = {**params, **{k.removeprefix("$$"): str(v) for k, v in result.items()}}
        dbutils.jobs.taskValues.set(key="workflow_vars", value=json.dumps(merged))
    return result


if __name__ == "__main__":
    main()
