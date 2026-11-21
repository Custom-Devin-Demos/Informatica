"""Fixed-width flat-file IO: PowerCenter flat-file sources/targets and PowerExchange (PWX_SEQ_NRDB2) copybook records."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence

from pyspark.sql import DataFrame, SparkSession, functions as F


@dataclass(frozen=True)
class Field:
    name: str
    length: int
    numeric: bool = False  # numeric fields are right-aligned and zero-padded on write


def read_fixed_width(spark: SparkSession, path: str, fields: Sequence[Field], trim: bool = True) -> DataFrame:
    raw = spark.read.text(path)
    cols, pos = [], 1
    for f in fields:
        c = F.substring("value", pos, f.length)
        cols.append((F.trim(c) if trim else c).alias(f.name))
        pos += f.length
    return raw.select(*cols)


def to_fixed_width(df: DataFrame, fields: Sequence[Field]) -> DataFrame:
    parts = []
    for f in fields:
        c = F.coalesce(F.col(f.name).cast("string"), F.lit(""))
        parts.append(F.lpad(c, f.length, "0") if f.numeric else F.rpad(c, f.length, " "))
    return df.select(F.concat(*parts).alias("value"))


def write_fixed_width(df: DataFrame, path: str, fields: Sequence[Field]) -> None:
    """Write a single-part text file directory at ``path`` (one file, like a PowerCenter flat-file target)."""
    to_fixed_width(df, fields).coalesce(1).write.mode("overwrite").text(path)
