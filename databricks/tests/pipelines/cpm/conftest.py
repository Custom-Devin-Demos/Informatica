"""Fixture builders for the wf_CPM tests: Delta tables typed from the XML target/source definitions and
VSAM records laid out with the XML copybook offsets."""
from __future__ import annotations

import os
from decimal import Decimal

import pytest
from pyspark.sql import types as T

from biis_lakehouse.pipelines.cpm._layouts import TABLES, VSAM_LAYOUTS
from biis_lakehouse.pipelines.cpm._helpers import vsam_fields, vsam_record_length

PP = {"WF_PP_END_YEAR": "2024", "WF_PP_NUM": "5", "WF_PP_YEAR_NUM": "202405"}
FILES = {
    "CPM_Biweekly_MER_filename": "p6722d01.txt",
    "CPM_PAD_filename": "p6331x01.txt",
    "CPM_Year_To_Date_filename": "p6791x01.txt",
    "CPM_Department_of_Energy_filename": "p6310de2.txt",
}


def _spark_type(t: str):
    return T._parse_datatype_string(t)


def make_table(ctx, table: str, rows: list[dict], owner: str | None = None):
    cols = TABLES[table]
    schema = T.StructType([T.StructField(c, _spark_type(t), True) for c, t, _ in cols])
    data = []
    for r in rows:
        vals = []
        for c, t, _ in cols:
            v = r.get(c)
            if v is not None and t.startswith("decimal"):
                v = Decimal(str(v))
            vals.append(v)
        data.append(tuple(vals))
    df = ctx.spark.createDataFrame(data, schema)
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(ctx.table_name(table, owner))
    return df


def vsam_line(source: str, values: dict) -> str:
    """Build one fixed-length record: strings left-justified, signed numerics as +/-digits (implied decimals)."""
    rec = [" "] * vsam_record_length(source)
    layout = {f[0]: f for f in vsam_fields(source)}
    for name, v in values.items():
        _, off, length, scale, numeric, _o, _l, _t, _r, pic = layout[name]
        off, length = int(off), int(length)
        if numeric:
            d = Decimal(str(v)).scaleb(int(scale)).to_integral_value()
            signed = pic.startswith(("+", "-", "S"))
            digits = str(abs(int(d))).rjust(length - (1 if signed else 0), "0")
            text = (("-" if d < 0 else "+") if signed else "") + digits
        else:
            text = str(v).ljust(length)[:length]
        rec[off:off + length] = list(text)
    return "".join(rec)


def write_vsam(ctx, param: str, lines: list[str]):
    path = ctx.path("in", "CPM", FILES[param])
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as fh:
        fh.write("\n".join(lines) + "\n")


@pytest.fixture
def cpm(ctx):
    ctx.params.update(PP)
    ctx.params.update(FILES)
    return ctx
