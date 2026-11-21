"""Cutover reconciliation: compare legacy (Oracle / PowerCenter) outputs with the Delta lakehouse outputs.

- :mod:`.reconcile` — row counts, key-set diff, per-column aggregates/checksums and row-level diffs
  between two DataFrames, plus a combined :func:`reconcile` report.
- :mod:`.files` — byte/line diff of fixed-width outbound files.
- :mod:`.sources` — readers for the legacy side (Oracle JDBC, CSV extracts, tables).
- :mod:`.spec` — JSON-driven batch of checks, runnable as ``python -m biis_lakehouse.validation``.
"""
from biis_lakehouse.validation.files import FileDiff, diff_fixed_width_files
from biis_lakehouse.validation.reconcile import (
    ColumnCompare,
    CountCompare,
    KeyDiff,
    ReconReport,
    align_types,
    column_profile,
    compare_columns,
    compare_row_counts,
    key_diff,
    normalize,
    reconcile,
    row_diff,
    write_results,
)

__all__ = [
    "ColumnCompare",
    "CountCompare",
    "FileDiff",
    "KeyDiff",
    "ReconReport",
    "align_types",
    "column_profile",
    "compare_columns",
    "compare_row_counts",
    "diff_fixed_width_files",
    "key_diff",
    "normalize",
    "reconcile",
    "row_diff",
    "write_results",
]
