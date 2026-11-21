"""JSON-driven reconciliation batch, e.g. one spec per pay period parallel run.

```json
{
  "results_table": "biis.recon_results",
  "checks": [
    {"name": "pay_period",
     "legacy": {"format": "jdbc", "dbtable": "HISTDBA.PAY_PERIOD"},
     "target": {"table": "PAY_PERIOD", "owner": "HISTDBA"},
     "keys": ["pp_num", "pp_end_year"], "exclude": ["load_dt"]},
    {"name": "cpm_newpay_tbl",
     "legacy": {"format": "jdbc", "query": "select * from INFO_TARGET_DEV.CPM_NEWPAY_TBL where PP_NUM = ${WF_PP_NUM}"},
     "target": {"table": "CPM_NEWPAY_TBL", "where": "pp_num = ${WF_PP_NUM}"},
     "keys": ["ssn", "pp_num"], "sum_tolerance": 0.01, "decimal_scale": 2},
    {"name": "afps_feeder_file", "type": "file",
     "legacy": "/Volumes/biis/landing/files/legacy/CPM/AFPS_${WF_PP_YEAR_NUM}.txt",
     "target": "out/CPM/afps_feeder.txt"}
  ]
}
```

Legacy ``format``: ``jdbc`` (``url`` or param ``ORACLE_JDBC_URL``; credentials from secret scope
``secret_scope``/``user_key``/``password_key``, default ``biis``/``oracle_user``/``oracle_password``),
``csv`` (``path``), ``table`` (``name``, fully qualified) or ``delta`` (``path``). ``${PARAM}``
placeholders are replaced from ``RunContext.params``. Relative file paths resolve under
``RunContext.landing_root``.
"""
from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from pathlib import PurePosixPath
from typing import Optional, Union

from pyspark.sql import DataFrame

from biis_lakehouse.common.fixed_width import Field
from biis_lakehouse.context import RunContext
from biis_lakehouse.validation.files import FileDiff, diff_fixed_width_files
from biis_lakehouse.validation.reconcile import ReconReport, reconcile, write_results
from biis_lakehouse.validation.sources import SecretGetter, read_csv_extract, read_oracle

_PLACEHOLDER = re.compile(r"\$\{(\w+)\}")


def substitute(value, params: dict[str, str]):
    if isinstance(value, str):
        def repl(m):
            if m.group(1) not in params:
                raise KeyError(f"missing parameter {m.group(1)}")
            return params[m.group(1)]

        return _PLACEHOLDER.sub(repl, value)
    if isinstance(value, list):
        return [substitute(v, params) for v in value]
    if isinstance(value, dict):
        return {k: substitute(v, params) for k, v in value.items()}
    return value


def _path(ctx: RunContext, p: str) -> str:
    return p if p.startswith(("/", "dbfs:", "s3:", "abfss:", "gs:")) else str(PurePosixPath(ctx.landing_root, p))


def load_legacy(ctx: RunContext, src: dict, get_secret: Optional[SecretGetter] = None) -> DataFrame:
    fmt = src.get("format", "table")
    if fmt == "jdbc":
        df = read_oracle(
            ctx.spark,
            src.get("url") or ctx.param("ORACLE_JDBC_URL"),
            dbtable=src.get("dbtable"),
            query=src.get("query"),
            secret_scope=src.get("secret_scope", "biis"),
            user_key=src.get("user_key", "oracle_user"),
            password_key=src.get("password_key", "oracle_password"),
            get_secret=get_secret,
        )
    elif fmt == "csv":
        df = read_csv_extract(ctx.spark, _path(ctx, src["path"]), sep=src.get("sep", ","), schema=src.get("schema"))
    elif fmt == "table":
        df = ctx.spark.table(src["name"])
    elif fmt == "delta":
        df = ctx.spark.read.format("delta").load(_path(ctx, src["path"]))
    else:
        raise ValueError(f"unknown legacy format {fmt!r}")
    return df.filter(src["where"]) if src.get("where") else df


def load_target(ctx: RunContext, tgt: dict) -> DataFrame:
    df = ctx.spark.table(tgt["name"]) if "name" in tgt else ctx.table(tgt["table"], tgt.get("owner"))
    return df.filter(tgt["where"]) if tgt.get("where") else df


@dataclass
class SpecResult:
    tables: list[ReconReport] = field(default_factory=list)
    files: list[FileDiff] = field(default_factory=list)
    results_table: Optional[str] = None

    @property
    def ok(self) -> bool:
        return all(r.ok for r in self.tables) and all(f.ok for f in self.files)

    def summary(self) -> str:
        return "\n".join([r.summary() for r in self.tables] + [f.summary() for f in self.files])


def run_spec(
    ctx: RunContext,
    spec: Union[dict, str],
    *,
    get_secret: Optional[SecretGetter] = None,
    write: bool = True,
) -> SpecResult:
    """Run every check in ``spec`` (dict or JSON text). Table results are appended to ``results_table``."""
    spec = json.loads(spec) if isinstance(spec, str) else spec
    spec = substitute(spec, ctx.params)
    out = SpecResult()
    for chk in spec["checks"]:
        if chk.get("type", "table") == "file":
            fields = [Field(**f) for f in chk.get("fields", [])] or None
            out.files.append(
                diff_fixed_width_files(
                    _path(ctx, chk["legacy"]),
                    _path(ctx, chk["target"]),
                    fields=fields,
                    ignore_trailing_spaces=chk.get("ignore_trailing_spaces", False),
                    sort_lines=chk.get("sort_lines", False),
                )
            )
            continue
        out.tables.append(
            reconcile(
                load_legacy(ctx, chk["legacy"], get_secret),
                load_target(ctx, chk["target"]),
                name=chk["name"],
                keys=chk.get("keys", []),
                columns=chk.get("columns"),
                exclude=chk.get("exclude", []),
                count_tolerance=chk.get("count_tolerance", 0),
                sum_tolerance=chk.get("sum_tolerance", 0.0),
                decimal_scale=chk.get("decimal_scale"),
            )
        )
    if write and out.tables:
        out.results_table = write_results(ctx, out.tables, spec.get("results_table"))
    return out
