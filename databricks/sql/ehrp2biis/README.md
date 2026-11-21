# EHRP2BIIS pre/post-load SQL (Databricks)

Ports of the root-level SQL*Plus assets, run by `biis_lakehouse.sqlsteps.ehrp2biis`
(`biis-run --workflow ehrp2biis_sql --task preload|afterload|actstage_load`).
The original files (`ehrp2biis_preload`, `ehrp2biis_afterload.sql`, `actstage_load`) stay at the repo root, unchanged.

| PowerCenter / Oracle | Databricks |
|---|---|
| `ehrp2biis_preload` → SQL*Plus `@step01` | `preload/*.sql` (step01 is not in the repo; logic inferred from `wf_EHRP2BIIS_UPDATE` and the afterload) |
| `ehrp2biis_afterload.sql` | `afterload/*.sql` + `procedures/*.sql`, in the original statement order (`AFTERLOAD_STEPS`) |
| `actstage_load` → `action_stage_load` | `actstage/action_stage_load.sql` (script not in the repo: no statements, task fails unless `ON_MISSING_PROCEDURE=skip`) |
| `HISTDBA.`, `NKNIGHT.`, `EHRP.`, unqualified owner | `${histdba}`, `${nknight}`, `${ehrp}`, `${biis}` (catalog-qualified via `RunContext`) |
| `trunc(sysdate)` | `${load_date}` = job parameter `LOAD_DATE` or `current_date()` |
| `UPDATE ... WHERE x IN (SELECT ...)`, `DELETE ... IN (SELECT ...)` | Delta `MERGE INTO ... WHEN MATCHED THEN UPDATE/DELETE` |
| `DELETE FROM nwk_new_ehrp_actions_tbl` | Delta `DELETE` (keeps table history; `TRUNCATE` would also work) |
| `EXEC <procedure>` | `procedures/<name>.sql`; unported bodies are comment-only placeholders |
| `spool`, `set echo/feedback` | log file `<landing_root>/log/EHRP2BIIS/<task>_<ts>.log` + driver stdout |
| `whenever sqlerror exit`, `grep ERROR` on the spool | any Spark SQL error raises `SqlStepError` → task fails, downstream tasks do not run |
| `grep -c 'SUCCESS!!!'` (actstage) | task returns normally only when every statement and `-- @assert_empty` check passed |
| `mailx` / `uuencode` of the log | Databricks Workflows `email_notifications` / `webhook_notifications` (`on_failure`, `on_success`) on the job/task in `databricks.yml` (Unit 4); the log path is in the task's return value |
| `$HOME/.use`, `.pw`, `.use1`, `.pw1` | dropped: Unity Catalog / job identity authorisation |

## Directives (SQL comments read by `sqlsteps.engine`)

- `-- @label <text>`: heading written to the log before the statement.
- `-- @metric <name>`: first column of the first row is returned as a task metric.
- `-- @assert_empty <message>`: the query must return no rows, otherwise the task fails with `<message>`.

## Not converted faithfully

- `step01` (called by `ehrp2biis_preload`): source absent; `preload/02_select_new_ehrp_actions.sql` is an inference (PS_GVT_JOB rows with `GVT_WIP_STATUS` in `NEW_ACTION_WIP_STATUSES`, not yet in `EHRP_RECS_TRACKING_TBL`).
- `action_stage_load` (called by `actstage_load`): source absent; placeholder only.
- `HISTDBA.UPDT_ERP2BIIS_CRE8_REMARKS01_P`, `HISTDBA.UPDATE_ERP2BIIS_NO900S01_P`, `HISTDBA.ERP2BIIS_CRE8_REMARKS_900S01`, `HISTDBA.UPDATE_ERP2BIIS_900SONLY01_P`, `HISTDBA.UPDT_ORIG_CANCELLED_TRANS01_P`: PL/SQL source absent; placeholders.
- `UPDATE_SEQUENCE_NUMBER_TBL_P`, `CHK_EHRP2BIIS_WIP_STATUS_P`, `HISTDBA.GATHER_EHRP2BIIS_RUNCOUNTS_P`: PL/SQL source absent; behaviour inferred from names, the afterload and the mapping (see each file's header).
