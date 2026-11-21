# pc2dbx — PowerCenter XML → PySpark/Delta converter

Reads Informatica PowerCenter XML exports (`POWERMART/REPOSITORY/FOLDER/...`) into an in-memory model
(`model.py`) and emits:

* `inventory.json` — sources, targets, transformation instance counts (same counting rule as
  `databricks/PLAN.md` §2), mappings, sessions, workflow task order/links/schedules/variables, plus every
  `conversion_issues` entry (objects that could not be converted faithfully).
* one Python module per workflow, `<area>/<wf_name>.py` (areas per PLAN.md §2.1), exposing
  `TASKS = {"s_<session>": fn}` in workflow order and built on `biis_lakehouse.context.RunContext`,
  `biis_lakehouse.common.delta` and `biis_lakehouse.common.fixed_width`.

```bash
cd tools && python -m pc2dbx ../XML/* -o ../out/pc2dbx        # or: pip install -e tools/pc2dbx && pc2dbx ...
cd tools && python -m pytest pc2dbx/tests                     # Spark tests need `pip install -e 'databricks[dev]'`
```

## Rendering rules (PLAN.md §3)

Transformations are rendered in connector (topological) order, one DataFrame per instance. Passive
branches are re-aligned on a `_rid` row id assigned at each active origin (source, SQ, aggregator,
joiner, normalizer).

| PowerCenter | Generated code |
|---|---|
| Source Definition (Oracle) / (Flat File, VSAM, PWX) | `ctx.table(name, owner)` / `read_fixed_width(...)` or `spark.read.csv` |
| Source Qualifier | port selection; `Sql Query` / multi-source join → `spark.sql` over temp views; Source Filter, Select Distinct, sorted ports |
| Expression | `withColumn` per variable then output port (cast to port type) |
| Filter / Router | `.filter(...)` with the PowerCenter condition quoted; router DEFAULT = null-safe complement |
| Lookup (connected / `:LKP`) | left join + `_first_match` (one row per input row, ordered by condition ports) |
| Joiner | `detail.join(master, cond, how)` — Normal→inner, Master Outer→left, Detail Outer→right, Full→full |
| Aggregator | `groupBy(GROUPBY ports).agg(...)`; non-aggregate outputs → `last(...)` |
| Update Strategy → target | `delta.update_strategy` then `delta.merge` on target primary keys |
| Sequence | `row_number()` over `_rid` from a persisted `SEQ_<name>_CURRENT` job parameter |
| Normalizer | `posexplode(array(struct(...)))` per occurrence; GCID / GK ports |
| Mapplet | generated `_mplt_<name>(ctx, df)` function |
| Target | Delta `merge` / `overwrite` (truncate) / `append`; flat files via `write_fixed_width` / CSV |

Expressions go through `expr.py` (tokenizer + recursive-descent parser → Spark SQL). `$$VAR` / `$Param_X`
become `${KEY}` placeholders resolved at run time from `RunContext.params` (defaults in `PARAM_DEFAULTS`).
Anything untranslatable is emitted as `# TODO(pc2dbx): ...` quoting the original PowerCenter text and is
recorded in `inventory.json` → `conversion_issues`.
