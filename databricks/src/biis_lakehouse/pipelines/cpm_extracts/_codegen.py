"""Generate the record specs (``_spec_<wf>.py``) for the wide flat-file / PowerExchange / Oracle targets.

The CPM extract mappings build 200-700 column fixed-width records with one Expression port per
target field. Rather than retype those ports, this script reads the PowerCenter export, inlines
every target field's lineage back to Source Qualifier ports (through Expression, Sorter, Filter
and Update Strategy transformations, variable ports included) and translates it to Spark SQL with
``_pcexpr``. The hand-written pipelines own the Source Qualifier SQL, lookups, aggregations and
writes; the specs only carry the field layout and per-field expressions.

    python -m biis_lakehouse.pipelines.cpm_extracts._codegen ../XML
"""
from __future__ import annotations

import pprint
import re
import sys
import xml.etree.ElementTree as ET
from pathlib import Path

from biis_lakehouse.pipelines.cpm_extracts import _pcexpr

PASS_THROUGH = {"Sorter", "Filter", "Update Strategy"}

# workflow export -> {mapping: [target instances]}
WANTED = {
    "CPM_OIG": {"m_CPM_OIG_Load_CPM_OIG_File": ["oigsgndec_SKPAYROLL_MASTER"]},
    "CPM_CDC": {
        "m_CPM_CDC_Load_CPM_CDC_Header_File": ["cdchdr_WS_CDC_HDR"],
        "m_CPM_CDC_Load_CPM_CDC_Data_File": ["cdcskel_WS_PAY_OUT_REC"],
    },
    "CPM_NIH": {
        "m_CPM_NIH_Load_CPM_NIH_Header_File": ["nihhdr_WS_NIH_HDR"],
        "m_CPM_NIH_Load_CPM_NIH_Data_File": ["nihtest_NIH_PAYROLL_MASTER"],
    },
    "CPM_AFPS": {
        "m_CPM_AFPS_0100_Data_Seperate": ["HI_AFPS_FEEDER_TBL"],
        "m_CPM_AFPS_0200_Debridge_To_FEEDER_FLAT": ["feeder_FEEDER_RECORD"],
    },
}


def spark_type(dt: str, precision: str, scale: str) -> str:
    """Spark SQL type for a PowerCenter transformation datatype (used to type test fixtures)."""
    dt = (dt or "string").lower()
    if dt == "decimal":
        return f"decimal({precision},{scale})"
    if dt in ("integer", "small integer"):
        return "int"
    if dt == "bigint":
        return "bigint"
    if dt in ("double", "real"):
        return "double"
    if dt.startswith("date"):
        return "timestamp"
    return "string"


def load(path: Path) -> ET.Element:
    text = path.read_bytes().decode("latin1")
    return ET.fromstring(re.sub(r"<!DOCTYPE[^>]*>", "", text)).find(".//FOLDER")


def mapping_specs(folder: ET.Element, mapping: str, target_instances: list[str]) -> dict:
    m = next(x for x in folder.findall("MAPPING") if x.get("NAME") == mapping)
    reusable = {t.get("NAME"): t for t in folder.findall("TRANSFORMATION")}
    local = {t.get("NAME"): t for t in m.findall("TRANSFORMATION")}
    targets = {t.get("NAME"): t for t in folder.findall("TARGET")}
    inst = {i.get("NAME"): i for i in m.findall("INSTANCE")}
    incoming = {(c.get("TOINSTANCE"), c.get("TOFIELD").upper()): (c.get("FROMINSTANCE"), c.get("FROMFIELD")) for c in m.findall("CONNECTOR")}
    memo: dict = {}
    used_inputs: dict = {}

    def ports(iname):
        i = inst[iname]
        tr = local.get(i.get("TRANSFORMATION_NAME")) or reusable.get(i.get("TRANSFORMATION_NAME"))
        return tr, {f.get("NAME").upper(): f for f in tr.findall("TRANSFORMFIELD")} if tr is not None else {}

    def resolve(iname, port):
        key = (iname, port.upper())
        if key in memo:
            return memo[key]
        i = inst[iname]
        itype = i.get("TRANSFORMATION_TYPE")
        if itype in ("Source Qualifier", "Source Definition"):
            _, fields = ports(iname)
            f = fields.get(port.upper())
            used_inputs[port.upper()] = spark_type(f.get("DATATYPE"), f.get("PRECISION"), f.get("SCALE")) if f is not None else "string"
            out = f"`{port.upper()}`"
        else:
            tr, fields = ports(iname)
            f = fields.get(port.upper())
            ptype = f.get("PORTTYPE") if f is not None else "INPUT"
            expr = (f.get("EXPRESSION") or "") if f is not None else ""
            if itype == "Expression" and ("OUTPUT" in ptype and "INPUT" not in ptype or ptype == "LOCAL VARIABLE"):
                out = "(" + _pcexpr.to_sql(expr, lambda n: resolve(iname, n)) + ")"
            elif itype == "Expression" or itype in PASS_THROUGH:
                src = incoming.get(key)
                out = resolve(*src) if src else "NULL"
            else:
                out = f"`{iname}__{port.upper()}`"
        memo[key] = out
        return out

    specs = {}
    for tname in target_instances:
        t = targets[inst[tname].get("TRANSFORMATION_NAME")]
        fields = []
        for f in t.findall("TARGETFIELD"):
            src = incoming.get((tname, f.get("NAME").upper()))
            sql = resolve(*src) if src else None
            fields.append((f.get("NAME"), f.get("DATATYPE"), int(f.get("PRECISION")), int(f.get("SCALE") or 0), sql))
        specs[tname] = {"table": t.get("NAME"), "dbtype": t.get("DATABASETYPE"), "fields": fields}
    specs["_inputs"] = dict(sorted(used_inputs.items()))
    return specs


def generate(xml_dir: Path, out_dir: Path) -> None:
    for export, mappings in WANTED.items():
        folder = load(xml_dir / export)
        specs = {m: mapping_specs(folder, m, tgts) for m, tgts in mappings.items()}
        body = pprint.pformat(specs, width=120, sort_dicts=False)
        (out_dir / f"_spec_{export.lower()}.py").write_text(
            f'"""Generated by ``_codegen`` from ``XML/{export}``; do not edit by hand."""\n'
            "# fmt: off\n"
            f"SPECS = {body}\n"
        )


if __name__ == "__main__":
    generate(Path(sys.argv[1] if len(sys.argv) > 1 else "../XML"), Path(__file__).parent)
