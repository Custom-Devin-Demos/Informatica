# BIIS lakehouse — Databricks cutover runbook

This directory replaces the Informatica PowerCenter estate in this repo (`XML/`, `ehrp2biis_preload`,
`ehrp2biis_afterload.sql`, `actstage_load`, `Maintenance Scripts/`, `Transfer Scripts/`) with a
Databricks lakehouse. The design, inventory, transformation mapping and dependency graph are in
[`PLAN.md`](PLAN.md); this file is the operational runbook.

## 1. Architecture

| Piece | Path | Replaces |
|---|---|---|
| Run context, task runner (`biis-run`), Delta + fixed-width helpers | `src/biis_lakehouse/{context,runner}.py`, `src/biis_lakehouse/common/` | Integration Service, session target load types |
| Converted pipelines, one module per workflow, `TASKS = {"s_...": fn}` | `src/biis_lakehouse/pipelines/{reference,cpm,cpm_extracts,les,ehrp}/` | `XML/*` workflows/mappings |
| Pre/post-load SQL | `sql/`, `src/biis_lakehouse/sqlsteps/` | `ehrp2biis_preload`, `ehrp2biis_afterload.sql`, `actstage_load` |
| File ops and SFTP | `src/biis_lakehouse/ops/` | session `mv`/`cp` commands, `Maintenance Scripts/*`, `Transfer Scripts/*` |
| Jobs (Asset Bundle) | `databricks.yml`, `resources/*.yml` | PowerCenter workflows, schedules, `BIIS_parms.iparms` |
| Reconciliation | `src/biis_lakehouse/validation/` | — (cutover only) |
| XML → PySpark converter | `../tools/pc2dbx/` | — (regenerates skeletons from `XML/`) |
| CI | `../.github/workflows/ci.yml` | — |

Storage mapping (PLAN §1): Oracle `HISTDBA` / `INFO_TARGET_DEV` / `NKNIGHT` / `EHRP` → Unity Catalog
`<catalog>.histdba` / `.biis` / `.nknight` / `.ehrp`, table names lower-cased. Flat files live in the
Volume `/Volumes/<catalog>/landing/files/{in,out,archive}/<AREA>/` (`RunContext.landing_root`),
mirroring `$Param_Root_Directory/data/int`.

Jobs: one per workflow (`wf_Pay_Calendar`, `wf_Pseudossn`, `wf_COMPTIME`, `wf_CPM`, `wf_LES`,
`wf_CPM_AFPS`, `wf_CPM_CDC`, `wf_CPM_NIH`, `wf_CPM_OIG`, `wf_FDA_Leave`), the `biis_pay_period` master
job that runs them in the PLAN §4.1 order, and `ehrp2biis_daily` (preload SQL →
`wf_EHRP2BIIS_UPDATE` → afterload SQL → actstage load). Every task runs
`biis-run --workflow <wf> --task <session>`; `$$WF_*` variables flow between tasks as task values.

## 2. Local development and tests

Requires Python ≥ 3.10 (CI uses 3.11) and Java 17. No Databricks workspace is needed.

```bash
cd databricks
python -m venv .venv && . .venv/bin/activate
pip install -U pip            # editable installs need pip >= 21.3
pip install -e '.[dev]'       # pyspark 3.5.3, delta-spark 3.2.1, pytest
pytest                        # local Delta SparkSession from tests/conftest.py
pytest tests/validation -q    # just the reconciliation helpers
```

The first run downloads the Delta jars into `~/.ivy2`. `tests/conftest.py` resolves them from the
GCS Maven mirror because Maven Central sometimes returns HTTP 429; override with
`SPARK_JARS_REPOSITORIES=<url>[,<url>]`.

Run one converted session locally against tables you created in the test metastore:

```bash
biis-run --workflow wf_Pay_Calendar --task s_Pay_Calendar_Set_Pay_Calendar \
  --landing-root /tmp/landing --param WF_PP_NUM=12 --param WF_PP_END_YEAR=2019
```

CI (`.github/workflows/ci.yml`) runs on every push and pull request: Python 3.11 + Temurin 17, pip
and `~/.ivy2` caches, `pip install -e 'databricks[dev]'`, `pytest databricks`, and `pytest tools/pc2dbx`
when the converter is present. It uses no secrets and no workspace.

## 3. Workspace setup (once per environment)

Do this for `dev` first, then `prod`. Names below are the defaults; the real ones are bundle
variables in `databricks.yml`.

1. **Unity Catalog**

   ```sql
   CREATE CATALOG IF NOT EXISTS biis_dev;            -- biis_prod for prod
   USE CATALOG biis_dev;
   CREATE SCHEMA IF NOT EXISTS histdba;
   CREATE SCHEMA IF NOT EXISTS biis;                 -- INFO_TARGET_DEV
   CREATE SCHEMA IF NOT EXISTS nknight;
   CREATE SCHEMA IF NOT EXISTS ehrp;
   CREATE SCHEMA IF NOT EXISTS landing;
   CREATE VOLUME IF NOT EXISTS landing.files;        -- managed, or EXTERNAL on the existing storage
   GRANT USE CATALOG, USE SCHEMA, SELECT, MODIFY ON CATALOG biis_dev TO `biis-job-sp`;
   GRANT READ VOLUME, WRITE VOLUME ON VOLUME landing.files TO `biis-job-sp`;
   ```

   Create the folder tree the jobs expect: `in/<AREA>`, `out/<AREA>`, `archive/<AREA>` for
   `COMPTIME`, `CPM`, `LES`, `PSEUDO`, `PAY_CALENDAR`, `EHRP` (same areas as `/data/BIISINT/data/int`).

2. **Secret scope `biis`** (never put credentials in the bundle; PLAN §5)

   ```bash
   databricks secrets create-scope biis
   databricks secrets put-secret biis oracle_user        # replaces $HOME/.use / .use1
   databricks secrets put-secret biis oracle_password    # replaces $HOME/.pw / .pw1
   databricks secrets put-secret biis sftp_private_key   # replaces sa-cdirect's ssh key (Transfer Scripts)
   databricks secrets put-acls biis biis-job-sp READ
   ```

3. **Service principal** `biis-job-sp` owns and runs all jobs (`run_as` in the bundle).
4. **Network**: the job clusters need egress to the Oracle listener (history backfill and parallel
   run only) and to the SFTP targets used by `Transfer Scripts/*` (e.g. `m1csv301.hhs.gov`).
5. **Oracle JDBC driver**: install `com.oracle.database.jdbc:ojdbc8` as a cluster library on the
   reconciliation/backfill cluster only. Pipelines never talk to Oracle after cutover.

## 4. Deploy with the Asset Bundle

```bash
cd databricks
databricks auth login --host <workspace-url>        # or DATABRICKS_HOST/DATABRICKS_TOKEN for a CI SP
databricks bundle validate -t dev
databricks bundle deploy   -t dev
databricks bundle run      -t dev wf_Pay_Calendar   # smoke test one job
# prod: same commands with -t prod, run by the service principal
databricks bundle validate -t prod && databricks bundle deploy -t prod
```

Fill the per-target values that came from `BIIS_parms.iparms` (file names, e-mail lists,
directories) in the `targets.<env>.variables` block of `databricks.yml` before the first prod deploy;
the placeholders are listed in PLAN §2.1/§5. Pause schedules in prod until go-live
(`ehrp2biis_daily` is the only scheduled job; everything else is on demand like today).

## 5. Oracle history backfill

The Delta tables must start from the same state as Oracle at the cutover point.

1. Freeze PowerCenter loads for the window (or take the backfill right after a pay-period run).
2. For every Oracle table in PLAN §2.3 (sources, lookups and targets), copy it once over JDBC:

   ```python
   from biis_lakehouse.context import RunContext
   from biis_lakehouse.common import delta
   from biis_lakehouse.validation.sources import read_oracle

   ctx = RunContext(spark, catalog="biis_prod")
   url = "jdbc:oracle:thin:@//<host>:1521/<service>"
   for owner, table in [("HISTDBA", "PAY_PERIOD"), ("INFO_TARGET_DEV", "PSEUDOSSN_TBL"), ...]:
       df = read_oracle(spark, url, dbtable=f"{owner}.{table}")      # creds from secret scope biis
       delta.overwrite(ctx, df.toDF(*[c.lower() for c in df.columns]), ctx.table_name(table, owner))
   ```

   Partition large tables (`partitionColumn`/`lowerBound`/`upperBound`/`numPartitions` JDBC
   options, e.g. on pay period) and cast Oracle `NUMBER` columns to the precision the pipelines use.
   Archive tables (`HI_ARCH_*`) and `ERROR_TBL` history are copied the same way.
3. Copy control state: `SEQUENCE_NUM_TBL` / sequence high-water marks, `CPM_CYCLE_TBL`, and the
   current `PAY_PERIOD` flags, so Sequence Generator replacements and the AFPS ↔ FDA cycle id
   continue from the right values.
4. Copy any in-flight inbound files from `/data/BIISINT/data/int/in/**` and the last
   `archive/**` generation into the Volume.
5. Reconcile the backfill itself with a spec of `format: jdbc` vs Delta checks (section 6) — every
   table must be `OK` before the parallel run starts.

## 6. Parallel run and reconciliation (per pay period)

Run both systems on the same inputs for at least **two consecutive pay periods** (one of them a
year-end/first-period-of-year if the calendar allows), then compare.

1. Drop the same inbound files in `/data/BIISINT/data/int/in/<AREA>` and
   `/Volumes/<catalog>/landing/files/in/<AREA>`.
2. Run PowerCenter as usual, then `databricks bundle run -t prod biis_pay_period` with the same
   `WF_PP_NUM` / `WF_PP_END_YEAR` / `WF_PP_YEAR_NUM`. SFTP tasks must be disabled (or pointed at a
   test drop box) during the parallel run so partners receive only PowerCenter's files.
3. Copy PowerCenter's outbound files (`/data/BIISINT/data/int/out/**`) to
   `/Volumes/<catalog>/landing/files/legacy/<AREA>/`.
4. Run the reconciliation spec:

   ```bash
   python -m biis_lakehouse.validation --spec recon_pp.json --catalog biis_prod \
     --landing-root /Volumes/biis_prod/landing/files \
     --param WF_PP_NUM=12 --param WF_PP_END_YEAR=2019 --param WF_PP_YEAR_NUM=201912 \
     --param ORACLE_JDBC_URL=jdbc:oracle:thin:@//<host>:1521/<service>
   ```

   (or as a `spark_python_task` / notebook calling `run_spec(ctx, spec)`). It prints one line per
   check, appends results to `<catalog>.biis.recon_results` keyed by `pay_period` and `run_id`, and
   exits 1 on any mismatch.

   Example spec (format documented in `src/biis_lakehouse/validation/spec.py`):

   ```json
   {"checks": [
     {"name": "pay_period", "legacy": {"format": "jdbc", "dbtable": "HISTDBA.PAY_PERIOD"},
      "target": {"table": "PAY_PERIOD", "owner": "HISTDBA"}, "keys": ["pp_num", "pp_end_year"]},
     {"name": "cpm_newpay_tbl",
      "legacy": {"format": "jdbc", "query": "select * from INFO_TARGET_DEV.CPM_NEWPAY_TBL where PP_NUM = ${WF_PP_NUM}"},
      "target": {"table": "CPM_NEWPAY_TBL", "where": "pp_num = ${WF_PP_NUM}"},
      "keys": ["ssn", "pp_num"], "decimal_scale": 2, "exclude": ["load_dt"]},
     {"name": "afps_feeder", "type": "file",
      "legacy": "legacy/CPM/<afps file>", "target": "out/CPM/<afps file>"}
   ]}
   ```

What each check does (`biis_lakehouse.validation`):

| Helper | Compares | Typical use |
|---|---|---|
| `compare_row_counts` | `count()` of both sides, optional tolerance | every target table |
| `key_diff` | null-safe key set difference + duplicate keys each side, with samples | tables with a primary key / MERGE keys |
| `compare_columns` / `column_profile` | per column non-null count, distinct count, min, max, numeric sum (with tolerance) and an order-independent xxhash64 checksum | money/amount columns, flags |
| `row_diff` | matched-key rows whose values differ: `keys, column, legacy_value, target_value` | root-causing a mismatch |
| `reconcile` | all of the above after `normalize` (lower-case names, trim, `''` → NULL like Oracle) and casting legacy columns to the Delta types | one call per table |
| `diff_fixed_width_files` | bytes + SHA-256, then line diff (difflib-aligned) with first differing column → field name, wrong record lengths | AFPS/CDC/NIH/OIG/FDA/LES outbound files, message files |
| `write_results` | appends results to `recon_results` | parallel-run dashboard |

Track sign-off with:

```sql
SELECT pay_period, name, count_if(NOT ok) AS failed_checks
FROM biis_prod.biis.recon_results
WHERE run_id = '<latest>' GROUP BY ALL ORDER BY failed_checks DESC;
```

Acceptance criteria per pay period:

- All target tables: row count, key set and checksums equal. Allowed exclusions are load
  timestamps / `SYSDATE` columns and Sequence Generator surrogate keys (`exclude`); each exclusion is
  listed in the sign-off.
- Outbound files sent to partners (AFPS, CDC, NIH, OIG, FDA, LES): **byte-identical**
  (`byte_identical = true`). `sort_lines` / `ignore_trailing_spaces` are only acceptable for files
  the receiving partner confirms are order/padding-insensitive.
- Message/e-mail bodies: same counts; wording differences acceptable.
- Every mismatch has a ticket with a root cause (conversion bug fixed, or PowerCenter defect
  accepted and documented).

## 7. Cutover checklist and go/no-go

T-2 weeks
- [ ] CI green on `master`; all unit PRs merged.
- [ ] `databricks bundle validate -t prod` clean; prod deployed with schedules paused.
- [ ] Secret scope, grants, Volume folders, network egress verified (smoke run of `wf_Pay_Calendar`).
- [ ] Prod values for all `BIIS_parms.iparms` parameters filled in and reviewed.
- [ ] Partners/MFT team informed of the new SFTP source host; test file accepted by each.

T-1 pay period
- [ ] Two parallel-run pay periods reconciled with zero unexplained mismatches.
- [ ] Run time of `biis_pay_period` within the PowerCenter batch window.
- [ ] Job failure notifications reach the support list (replaces `mailx` in the scripts).
- [ ] Rollback (section 8) rehearsed in dev.

Go/no-go (cutover day, owner: BIIS lead + DBA + partner contact)

| Criterion | Go if |
|---|---|
| Latest parallel run reconciliation | 0 failed checks (or all exceptions signed off) |
| Outbound partner files | byte-identical for the last pay period |
| Delta state vs Oracle at freeze | final backfill/catch-up reconciled `OK` |
| Support | on-call for first two live pay periods |
| Rollback | Oracle still writable, PowerCenter workflows only *unscheduled*, not deleted |

Cutover
1. Disable PowerCenter schedulers/cron (`ehrp2biis_preload`, `actstage_load`, transfer and
   maintenance cron entries) and `wf_EHRP2BIIS_UPDATE`'s schedule.
2. Final catch-up copy of tables changed since the backfill; reconcile.
3. Unpause `ehrp2biis_daily`; enable SFTP tasks in prod; run `biis_pay_period` for the pay period.
4. Reconcile Databricks output against the *expected* files for the period (spot checks with
   the partners) and confirm receipt.

## 8. Rollback

Rollback is possible until PowerCenter is decommissioned (section 9).

1. Pause `ehrp2biis_daily` and stop any running `biis_pay_period` run.
2. If Databricks already sent bad files, notify partners and resend from PowerCenter.
3. Re-enable PowerCenter schedules and cron. Oracle is still the system of record during the
   rollback window, so no data copy back is required unless Delta-only runs happened; for those,
   write the affected tables back with `df.write.format("jdbc")` (append for history, MERGE via a
   staging table for update-strategy targets) after reconciling which pay periods ran where.
4. Delta tables can be reset to the pre-run state with `RESTORE TABLE ... TO VERSION AS OF <n>`
   (keep `delta.deletedFileRetentionDuration` ≥ 30 days during the rollback window).
5. Record the cause; repeat the parallel run for the failed area before the next attempt.

## 9. Decommissioning PowerCenter and the shell scripts

After **three** clean live pay periods on Databricks:

| Legacy asset | Replacement | Action |
|---|---|---|
| `XML/*` workflows in the PowerCenter repository | jobs in `resources/*.yml`, `pipelines/*` | export a final repository backup, then delete the folders `COMP_TIME`, `CPM`, `EHRP2BIIS`, `LES`, `Pay_Calendar`, `Pseudossn` |
| `BIIS_parms.iparms` | bundle variables per target | archive the file |
| `ehrp2biis_preload`, `ehrp2biis_afterload.sql`, `actstage_load` | `sql/` + `sqlsteps` tasks in `ehrp2biis_daily` | remove cron entries; archive scripts |
| `Maintenance Scripts/archive_files` (`mv dir/* dest/<file>_P<pp>.txt`) | `ops` archive task on the Volume | remove cron entries |
| `Maintenance Scripts/remove_file` | `ops` remove task | remove |
| `Transfer Scripts/*_transfer` (7 SFTP + `mailx` scripts) | `ops` SFTP task (key in secret scope `biis`) + job notifications, or the enterprise MFT hand-off documented by unit 4 | revoke the `sa-cdirect` key from the old host; remove cron |
| `$HOME/.use*`, `$HOME/.pw*` credential files | secret scope `biis` | rotate the Oracle passwords, delete the files |
| Oracle schemas `INFO_TARGET_DEV`, `HISTDBA`, `NKNIGHT` (BIIS objects) | UC schemas | keep read-only for the retention period, final export, then drop |
| PowerCenter Integration Service / license | — | shut down after the retention period |

Keep `recon_results` and the final reconciliation reports with the decommissioning record.
