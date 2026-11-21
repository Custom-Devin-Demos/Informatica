"""Generated workflow modules compile and follow PLAN.md conventions."""
import ast
import importlib.util
import json
import subprocess
import sys

import pytest

from pc2dbx.codegen import AREAS, generate_workflow
from pc2dbx.parser import parse_file

from .conftest import EXPORTS, REPO


def _workflows():
    for path in EXPORTS:
        exp = parse_file(path)
        for folder in exp.folders:
            for wf in folder.workflows.values():
                yield pytest.param(folder, wf, id=wf.name)


@pytest.mark.parametrize("folder,wf", list(_workflows()))
def test_module_compiles_and_has_tasks(folder, wf):
    code, issues = generate_workflow(folder, wf, "XML")
    tree = ast.parse(code)
    compile(code, f"{wf.name}.py", "exec")
    imports = {n.module for n in ast.walk(tree) if isinstance(n, ast.ImportFrom)}
    assert {"biis_lakehouse.context", "biis_lakehouse.common.delta", "biis_lakehouse.common.fixed_width"} - imports <= {
        "biis_lakehouse.common.delta"}  # delta is imported as `from biis_lakehouse.common import delta`
    assert "biis_lakehouse.common" in imports
    tasks = next(n for n in tree.body if isinstance(n, ast.Assign) and n.targets[0].id == "TASKS")
    keys = [k.value for k in tasks.value.keys]
    assert keys == [s.name for s in wf.ordered_sessions()]
    assert len(keys) == len(wf.sessions)
    fns = {n.name for n in tree.body if isinstance(n, ast.FunctionDef)}
    assert all(v.id in fns for v in tasks.value.values)
    for i in issues:
        assert i["object"] and i["reason"]


def test_transformations_rendered_in_topological_order():
    exp = parse_file(next(p for p in EXPORTS if p.name == "Pay_Calendar"))
    folder = exp.folders[0]
    code, _ = generate_workflow(folder, folder.workflows["wf_Pay_Calendar"])
    body = code[code.index("def s_Pay_Calendar_Set_Pay_Calendar"):]
    body = body[: body.index("\ndef ", 10)]
    order = ["SQ_PAY_PERIOD", "exp_Initial", "lkp_Existing_Pay_Period", "exp_Determine_Parameters_Exist",
             "rtr_Parameter_Non_Parameter"]
    positions = [body.index(f": {name}") for name in order]
    assert positions == sorted(positions)
    assert "delta.update_strategy(" in body and "delta.merge(" in body
    assert "_first_match(" in body  # lookup as left join with multiple-match handling


@pytest.mark.parametrize("folder,wf", list(_workflows()))
def test_module_imports(folder, wf, tmp_path):
    pytest.importorskip("pyspark")
    code, _ = generate_workflow(folder, wf)
    path = tmp_path / f"{wf.name.lower()}.py"
    path.write_text(code)
    spec = importlib.util.spec_from_file_location(f"gen_{wf.name.lower()}", path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    assert list(mod.TASKS) == [s.name for s in wf.ordered_sessions()]
    assert all(callable(f) for f in mod.TASKS.values())


def test_cli(tmp_path):
    xml = [str(p) for p in EXPORTS]
    res = subprocess.run([sys.executable, "-m", "pc2dbx", *xml, "-o", str(tmp_path)],
                         cwd=REPO / "tools", capture_output=True, text=True, check=True)
    assert "inventory" in res.stdout
    inv = json.loads((tmp_path / "inventory.json").read_text())
    assert inv["exports"] == 11 and inv["totals"]["sessions"] == 108
    for wf, area in AREAS.items():
        assert (tmp_path / area / f"{wf.lower()}.py").exists(), wf
    assert inv["conversion_issues"]
