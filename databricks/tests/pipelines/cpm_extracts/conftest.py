"""Tiny in-test fixtures for the CPM extract workflows."""
from __future__ import annotations

import glob
from datetime import datetime
from decimal import Decimal

import pytest
from pyspark.sql import types as T

from biis_lakehouse.pipelines.cpm_extracts import _spec_cpm_afps, _spec_cpm_cdc, _spec_cpm_nih, _spec_cpm_oig

PP_YEAR, PP_NUM = 2019, 5


PAY_PERIOD_ONLY = {"PP_START_DTE", "PP_END_DTE", "LV_NUM", "LV_YEAR", "PAY_DTE", "CURR_PP_FLAG"}


def _newpay_columns() -> dict[str, str]:
    cols = {
        "PP_END_YEAR": "decimal(4,0)",
        "PP_NUM": "decimal(2,0)",
        "MP_POOL_DES": "string",
        "BUSINESS_UNIT": "string",
        "ORG_CDE": "string",
        "DFAS_PSEUDO_SSN": "string",
        "SOC_SEC_NO": "decimal(11,0)",
    }
    for mod, mapping in (
        (_spec_cpm_oig, "m_CPM_OIG_Load_CPM_OIG_File"),
        (_spec_cpm_cdc, "m_CPM_CDC_Load_CPM_CDC_Data_File"),
        (_spec_cpm_nih, "m_CPM_NIH_Load_CPM_NIH_Data_File"),
        (_spec_cpm_afps, "m_CPM_AFPS_0100_Data_Seperate"),
    ):
        for name, typ in mod.SPECS[mapping]["_inputs"].items():
            if name not in PAY_PERIOD_ONLY:
                cols.setdefault(name, typ)
    for name in ("REG_HRS_CPP", "CPP_GROSS_PAY", "CPP_BASE_PAY", "CPP_NET_PAY", "CPP_NET_ALT_DED"):
        cols.setdefault(name, "decimal(11,2)")
    return cols


NEWPAY_COLUMNS = _newpay_columns()


def _value(typ: str, v):
    if v is None:
        return None
    if typ.startswith("decimal"):
        return Decimal(str(v))
    if typ == "timestamp":
        return v
    if typ in ("int", "bigint"):
        return int(v)
    if typ == "double":
        return float(v)
    return str(v)


def make_table(ctx, name: str, owner: str, columns: dict[str, str], rows: list[dict]) -> None:
    unknown = {k for r in rows for k in r} - set(columns)
    assert not unknown, unknown
    schema = T._parse_datatype_string(", ".join(f"`{c}` {t}" for c, t in columns.items()))
    data = [tuple(_value(t, r.get(c)) for c, t in columns.items()) for r in rows]
    ctx.spark.createDataFrame(data, schema).write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(ctx.table_name(name, owner))


PAY_PERIOD_COLUMNS = {
    "PP_NUM": "decimal(2,0)",
    "PP_END_YEAR": "decimal(4,0)",
    "PP_START_DTE": "timestamp",
    "PP_END_DTE": "timestamp",
    "LV_NUM": "decimal(2,0)",
    "LV_YEAR": "decimal(4,0)",
    "PAY_DTE": "timestamp",
    "CURR_PP_FLAG": "string",
}


@pytest.fixture
def pay_period(ctx):
    rows = [
        {"PP_NUM": 4, "PP_END_YEAR": PP_YEAR, "CURR_PP_FLAG": "N", "PP_START_DTE": datetime(2019, 2, 10), "PP_END_DTE": datetime(2019, 2, 23)},
        {"PP_NUM": PP_NUM, "PP_END_YEAR": PP_YEAR, "CURR_PP_FLAG": "Y", "PP_START_DTE": datetime(2019, 2, 24), "PP_END_DTE": datetime(2019, 3, 9)},
    ]
    make_table(ctx, "PAY_PERIOD", "HISTDBA", PAY_PERIOD_COLUMNS, rows)
    make_table(ctx, "HI_GENERIC_SRC_TBL", "INFO_TARGET_DEV", {"ID": "decimal(10,0)"}, [{"ID": 1}])
    return rows


def newpay_row(i: int, business_unit: str, **kw) -> dict:
    row = {
        "PP_END_YEAR": PP_YEAR,
        "PP_NUM": PP_NUM,
        "MP_POOL_DES": " ",
        "BUSINESS_UNIT": business_unit,
        "ORG_CDE": "XX",
        "DFAS_PSEUDO_SSN": f"9000000{i:02d}",
        "SOC_SEC_NO": 123450000 + i,
        "FIR_NAME_1": "J",
        "SURNAME_3": "DOE",
        "HRLY_RATE": "25.75",
        "CPP_OT_PAY": "-12.34",
        "YTD_GROSS_PAY": "12345.67",
        "BASE_HRS": 80,
    }
    row.update(kw)
    return {k: v for k, v in row.items() if k in NEWPAY_COLUMNS}


@pytest.fixture
def newpay(ctx):
    def _make(rows):
        make_table(ctx, "CPM_NEWPAY_TBL", "INFO_TARGET_DEV", NEWPAY_COLUMNS, rows)

    return _make


def read_lines(path: str) -> list[str]:
    lines = []
    for part in sorted(glob.glob(f"{path}/part-*")):
        with open(part, encoding="latin1") as fh:
            lines.extend(fh.read().split("\n")[:-1] if fh else [])
    return lines


@pytest.fixture
def params():
    return {"WF_PP_END_YEAR": str(PP_YEAR), "WF_PP_NUM": str(PP_NUM), "WF_PP_YEAR_NUM": f"{PP_YEAR}{PP_NUM:02d}", "WF_CPM_EMAIL_LIST": "cpm@example.gov", "PMRepositoryServiceName": "Prod_Repo"}
