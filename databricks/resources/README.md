# Orchestration: Databricks Workflows bundle

`databricks.yml` + `resources/*.yml` form a Databricks Asset Bundle that replaces the PowerCenter
workflows, scheduler, parameter file, session commands, `Maintenance Scripts/*` and `Transfer Scripts/*`.

```bash
cd databricks
python resources/generate.py            # regenerate resources/*.yml after changing XML/* or ops/tasks.py
databricks bundle validate -t dev
databricks bundle deploy -t dev         # builds the biis_lakehouse wheel (entry point biis-run) into dist/
databricks bundle run -t dev biis_pay_period
```

`pytest tests/test_bundle.py tests/ops` checks all of this offline (no workspace, no CLI).

## Jobs

| Job | Source | Trigger |
|---|---|---|
| `wf_<name>` (11) | one per PowerCenter workflow | on demand (PowerCenter `ONDEMAND`); no schedule |
| `biis_pay_period` | PLAN.md §4.1, `run_job_task` per workflow | on demand |
| `ehrp2biis_daily` | `preload` → `wf_EHRP2BIIS_UPDATE` → `afterload` → `actstage_load` | `0 0 12 * * ?` in `${var.schedule_timezone}`; `PAUSED` in dev, `UNPAUSED` in prod |

Each workflow job is a linear chain of `python_wheel_task`s, `depends_on` the previous task (`ALL_SUCCESS`, as every
PowerCenter link is `$<prev>.Status = SUCCEEDED`). Task keys are the PowerCenter task instance names, plus
ops tasks wired around them:

- `<session>__pre` — the session's *Pre-session command* (runs before it);
- `<session>__post` — its *Post-session success command* and *Success Email* (runs after it);
- workflow `Email` tasks (`email_*`, `Email_*`) — `biis-run --workflow ops --task <name>`.

`biis_pay_period` dependencies follow the writer → reader table edges in PLAN.md §4.1:
Pay_Calendar → {Pseudossn, COMPTIME}; Pseudossn → {CPM, LES}; CPM → {CPM_AFPS, CPM_CDC, CPM_NIH, CPM_OIG};
CPM_AFPS → FDA_Leave.

## Parameters and workflow variables

- Every `$Param_*` / `$$WF_*` in `XML/*` is a bundle variable (prefix stripped, `resources/variables.yml`,
  placeholder defaults) and is passed to every task of the workflows that reference it as `--param NAME=${var.NAME}`.
  Real values go in the `dev` / `prod` targets (or `--var` at deploy time).
- `$Param_Root_Directory/data/int/<in|out>/<AREA>` → `${var.landing_root}/<in|out>/<AREA>`,
  `$Param_Root_Directory/data/archive/<AREA>` → `${var.landing_root}/archive/<AREA>` (UC Volume).
- Workflow variables assigned at run time (post-session success variable assignment, e.g. `$$WF_PP_YEAR_NUM`) are
  returned by the session task, stored by `biis-run` as the `workflow_vars` task value, and pulled by every later
  task through `--upstream-task <session>` (later sessions override earlier ones, like PowerCenter).
- Email lists (`$$WF_*_EMAIL_LIST`) feed the ops email steps and the job `email_notifications.on_failure`
  (the sessions' *Failure Email*). Put one address or distribution list in each.
- Secrets live in secret scope `biis` (`${var.SECRET_SCOPE}`), never in the bundle:
  `sftp_private_key`, `sftp_known_hosts`, optional `sftp_private_key_passphrase`, optional `smtp_password`.
  The legacy `$HOME/.use*` / `$HOME/.pw*` database credentials are not needed (Unity Catalog replaces the connections).

## Ops tasks (`biis_lakehouse.ops`)

| Legacy | Replacement |
|---|---|
| `Maintenance Scripts/remove_file` | `ops.files.remove_file`; task `remove_file` (`--param INPUT_DIR=out/CPM --param FILENAME=...`) |
| `Maintenance Scripts/archive_files` | `ops.files.archive_files` (`<name minus .txt>_P<pp>.txt`); task `archive_files` |
| session `mv` / `cp` / `cat` / `mv tatran*` | `ops.tasks.SESSION_COMMANDS` (one step per XML command, in `EXECORDER`, original command kept in `xml_command`) |
| `Transfer Scripts/*` (7) | `ops.sftp.transfer` (paramiko, key + pinned host key from secret scope, `RejectPolicy`); tasks `afps_transfer`, `cdc_transfer`, ... (`--param FILENAME=...`) for manual re-sends |
| `mailx`, Email tasks, session Success Email | `ops.notify.send_email` via `${var.SMTP_HOST}` (logs only when unset) |

All PowerCenter command components have *Fail task if any command fails = NO*, so a failing step is logged and the
remaining steps still run; set `FAIL_TASK_ON_COMMAND_ERROR=true` to fail the task instead (recommended once cut
over, so that a failed transfer fails the job and triggers its failure notification).

## Where a managed file transfer is preferable

The paramiko tasks reproduce the scripts 1:1 (push one file to `m1csv301.hhs.gov:/opt/app/jail/<account>/outbound`),
but for production a managed file transfer (the agency MFT / Axway, AWS Transfer Family, Azure Blob SFTP, or a
Unity Catalog external location the partner reads directly) is preferable for the outbound partner feeds
(AFPS, CDC, FDA, NIH CPM / LES, OIG) because:

- **Network**: job clusters need egress to the on-prem SFTP host (firewall rules / NAT from the Databricks VPC);
  an MFT already sits in the DMZ.
- **Key custody**: the SSH key moves into a Databricks secret scope readable by whoever can run the job's identity;
  an MFT keeps partner credentials and PGP keys out of the analytics platform.
- **Delivery guarantees**: MFTs give retries, checkpoint restart, receipts / non-repudiation and audit trails that
  the scripts (and their emails) only approximated.
- **Partner changes**: new recipients or protocols become MFT configuration instead of code + redeploy.

With an MFT the `Transfer` steps become "copy to the MFT pickup Volume / external location"; the rest of the
ops tasks (copy/rename/archive on Volumes) stay as they are.
