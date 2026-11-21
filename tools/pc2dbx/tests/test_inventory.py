"""Parse every real export and check counts against the PLAN.md §2 inventory."""
import hashlib
import json
import pytest

from pc2dbx.inventory import ABBREV, build_inventory, folder_summary
from pc2dbx.parser import parse_file

from .conftest import EXPORTS, PLAN


def plan_rows() -> dict[str, dict]:
    lines = [l for l in PLAN.read_text().splitlines() if l.startswith("| `XML/")]
    header = next(l for l in PLAN.read_text().splitlines() if l.startswith("| Export |"))
    cols = [c.strip() for c in header.strip("|").split("|")]
    rows = {}
    for line in lines:
        cells = [c.strip() for c in line.strip("|").split("|")]
        row = dict(zip(cols, cells))
        rows[row["Export"].strip("`").split("/")[1]] = row
    return rows


def by_type(cell: str) -> dict[str, int]:
    return {k: int(v) for k, v in (part.strip().rsplit(" ", 1) for part in cell.split(",") if part.strip())}


def test_all_eleven_exports_present():
    assert len(EXPORTS) == 11
    assert set(plan_rows()) == {p.name for p in EXPORTS}


@pytest.mark.parametrize("path", EXPORTS, ids=lambda p: p.name)
def test_counts_match_plan(path):
    row = plan_rows()[path.name]
    exp = parse_file(path)
    assert len(exp.folders) == 1
    summary = folder_summary(exp.folders[0])
    assert summary["folder"] == row["Folder"]
    assert summary["mappings"] == int(row["Mappings"])
    assert summary["sessions"] == int(row["Sessions"])
    assert summary["workflows"] == 1
    assert summary["sources_by_type"] == by_type(row["Sources (by type)"])
    assert summary["targets_by_type"] == by_type(row["Targets (by type)"])
    for abbrev in ABBREV.values():
        assert summary["transformation_counts"].get(abbrev, 0) == int(row[abbrev]), abbrev


def test_totals():
    exports = [parse_file(p) for p in EXPORTS]
    inv = build_inventory(exports)
    assert inv["totals"]["mappings"] == 108
    assert inv["totals"]["sessions"] == 108
    assert inv["totals"]["workflows"] == 11
    json.dumps(inv)


def test_model_details():
    exp = parse_file(next(p for p in EXPORTS if p.name == "Pay_Calendar"))
    folder = exp.folders[0]
    wf = folder.workflows["wf_Pay_Calendar"]
    assert [s.name for s in wf.ordered_sessions()][:2] == [
        "s_Pay_Calendar_Reset_Pay_Calendar", "s_Pay_Calendar_Set_Pay_Calendar"]
    m = folder.mappings["m_Pay_Calendar_Set_Pay_Calendar"]
    lkp = m.transformations["lkp_Existing_Pay_Period"]
    assert lkp.attrs["Lookup table name"] == "PAY_PERIOD"
    assert lkp.attrs["Lookup policy on multiple match"] == "Use Any Value"
    rtr = next(t for t in m.transformations.values() if t.type == "Router")
    assert {g.type for g in rtr.groups} >= {"INPUT", "OUTPUT", "OUTPUT/DEFAULT"}
    assert any(c.to_instance == lkp.name for c in m.connectors)
    sess = wf.sessions["s_Pay_Calendar_Set_Pay_Calendar"]
    assert sess.mapping_name == m.name and sess.pre_assignments


def test_xml_untouched(tmp_path):
    before = {p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in EXPORTS}
    from pc2dbx.cli import main

    assert main([str(p) for p in EXPORTS] + ["-o", str(tmp_path)]) == 0
    after = {p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in EXPORTS}
    assert before == after
