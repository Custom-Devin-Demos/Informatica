"""``python -m biis_lakehouse.validation --spec recon.json --catalog biis_prod --param WF_PP_NUM=12``

Runs a reconciliation spec (see :mod:`biis_lakehouse.validation.spec`), prints the summary, appends
results to the audit table and exits non-zero on any mismatch (so a Databricks job task fails).
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Sequence

from biis_lakehouse.context import RunContext
from biis_lakehouse.runner import _parse_params
from biis_lakehouse.validation.spec import run_spec


def main(argv: Sequence[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="python -m biis_lakehouse.validation")
    ap.add_argument("--spec", required=True, help="path to a JSON reconciliation spec")
    ap.add_argument("--catalog")
    ap.add_argument("--landing-root", default="/Volumes/biis/landing/files")
    ap.add_argument("--param", action="append", default=[], help="KEY=VALUE, repeatable")
    ap.add_argument("--no-write", action="store_true", help="do not append to the results table")
    ap.add_argument("--warn-only", action="store_true", help="exit 0 even when checks mismatch")
    args = ap.parse_args(argv)

    from pyspark.sql import SparkSession

    ctx = RunContext(
        spark=SparkSession.builder.getOrCreate(),
        catalog=args.catalog,
        landing_root=args.landing_root,
        params=_parse_params(args.param),
    )
    result = run_spec(ctx, Path(args.spec).read_text(), write=not args.no_write)
    print(result.summary())
    if result.results_table:
        print(f"results appended to {result.results_table}")
    return 0 if result.ok or args.warn_only else 1


if __name__ == "__main__":
    sys.exit(main())
