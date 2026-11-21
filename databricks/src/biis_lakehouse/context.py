from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import PurePosixPath
from typing import Callable, Optional

from pyspark.sql import DataFrame, SparkSession

# Oracle owner (PowerCenter OWNERNAME / DBDNAME) -> lakehouse schema.
OWNER_SCHEMAS = {
    "HISTDBA": "histdba",
    "INFO_TARGET_DEV": "biis",
    "NKNIGHT": "nknight",
    "EHRP": "ehrp",
}
DEFAULT_SCHEMA = "biis"


@dataclass
class RunContext:
    """Everything a converted session needs: Spark, table naming, file roots and parameters.

    ``params`` holds PowerCenter parameters with the ``$$`` / ``$Param_`` prefix stripped,
    e.g. ``$$WF_PP_NUM`` -> ``WF_PP_NUM`` and ``$Param_CPM_PAD_filename`` -> ``CPM_PAD_filename``.
    """

    spark: SparkSession
    catalog: Optional[str] = None
    landing_root: str = "/Volumes/biis/landing/files"
    params: dict[str, str] = field(default_factory=dict)

    def table_name(self, name: str, owner: str | None = None) -> str:
        schema = OWNER_SCHEMAS.get((owner or "").upper(), DEFAULT_SCHEMA)
        parts = [self.catalog, schema, name.lower()] if self.catalog else [schema, name.lower()]
        return ".".join(parts)

    def table(self, name: str, owner: str | None = None) -> DataFrame:
        return self.spark.table(self.table_name(name, owner))

    def path(self, *parts: str) -> str:
        """Landing-zone path, e.g. ``ctx.path("in", "CPM", fname)``; mirrors ``$Param_Root_Directory/data/int``."""
        return str(PurePosixPath(self.landing_root, *parts))

    def param(self, key: str, default: str | None = None) -> str:
        key = key.removeprefix("$$").removeprefix("$Param_")
        if key in self.params:
            return self.params[key]
        if default is not None:
            return default
        raise KeyError(f"missing parameter {key}")


# A converted PowerCenter session. Returns workflow-variable updates (SETVARIABLE) or None.
Task = Callable[[RunContext], Optional[dict]]
