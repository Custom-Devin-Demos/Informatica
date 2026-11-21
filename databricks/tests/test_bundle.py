"""Static checks of the Asset Bundle against the PowerCenter XML (no workspace, no databricks CLI)."""
from __future__ import annotations

import glob
import re
import subprocess
import sys
import xml.etree.ElementTree as ET
from pathlib import Path

import pytest

yaml = pytest.importorskip("yaml")

from biis_lakehouse.ops.tasks import EMAIL_TASKS, SESSION_COMMANDS, TASKS as OPS_TASKS  # noqa: E402
from biis_lakehouse.runner import WORKFLOW_MODULES  # noqa: E402

BUNDLE = Path(__file__).resolve().parents[1]
XML_FILES = sorted(p for p in (BUNDLE.parent / "XML").iterdir() if p.is_file())
EHRP_SQL_TASKS = ["preload", "afterload", "actstage_load"]


@pytest.fixture(scope="module")
def bundle() -> dict:
    root = yaml.safe_load((BUNDLE / "databricks.yml").read_text())
    merged = {"variables": dict(root["variables"]), "jobs": {}}
    for pattern in root["include"]:
        for path in sorted(glob.glob(str(BUNDLE / pattern))):
            doc = yaml.safe_load(Path(path).read_text())
            merged["variables"].update(doc.get("variables", {}))
            for name, job in doc.get("resources", {}).get("jobs", {}).items():
                assert name not in merged["jobs"], f"duplicate job {name}"
                merged["jobs"][name] = job
    merged["root"] = root
    return merged


def xml_workflow(path: Path):
    root = ET.parse(path).getroot()
    wf = next(root.iter("WORKFLOW"))
    inst = {t.get("NAME"): t for t in wf.iter("TASKINSTANCE")}
    nxt = {}
    for link in wf.iter("WORKFLOWLINK"):
        assert link.get("FROMTASK") not in nxt, "PowerCenter chains are linear"
        nxt[link.get("FROMTASK")] = link.get("TOTASK")
        cond = (link.get("CONDITION") or "").replace(" ", "").upper()
        assert cond in ("", f"${link.get('FROMTASK')}.STATUS=SUCCEEDED".upper()), cond
    cur = next(n for n, t in inst.items() if t.get("TASKTYPE") == "Start")
    chain = []
    while cur in nxt:
        cur = nxt[cur]
        chain.append((cur, inst[cur].get("TASKTYPE"), inst[cur].get("TASKNAME")))
    return root, wf, chain


def args_of(task: dict) -> dict:
    params = task["python_wheel_task"]["parameters"]
    out = {"--param": [], "--upstream-task": []}
    for flag, value in zip(params[::2], params[1::2]):
        if flag in out:
            out[flag].append(value)
        else:
            out[flag] = value
    return out


def deps(task: dict) -> list[str]:
    return [d["task_key"] for d in task.get("depends_on", [])]


def is_ops_glue(task: dict) -> bool:
    return "python_wheel_task" in task and args_of(task)["--workflow"] == "ops" and re.search(r"__(pre|post)$", task["task_key"])


def test_generated_resources_are_current():
    assert subprocess.run([sys.executable, str(BUNDLE / "resources" / "generate.py"), "--check"]).returncode == 0


def test_bundle_basics(bundle):
    root = bundle["root"]
    assert root["artifacts"]["biis_lakehouse"]["type"] == "whl"
    assert set(root["targets"]) == {"dev", "prod"}
    assert root["targets"]["dev"]["mode"] == "development"
    assert root["targets"]["prod"]["mode"] == "production"
    assert bundle["variables"]["SECRET_SCOPE"]["default"] == "biis"
    text = "\n".join(Path(p).read_text() for p in [BUNDLE / "databricks.yml", *glob.glob(str(BUNDLE / "resources/*.yml"))])
    assert not re.search(r"BEGIN [A-Z ]*PRIVATE KEY|\.pw1?\b|\.use1?\b|password\s*:", text, re.I)


@pytest.mark.parametrize("path", XML_FILES, ids=lambda p: p.name)
def test_job_matches_workflow_links(bundle, path):
    root, wf, chain = xml_workflow(path)
    name = wf.get("NAME")
    job = bundle["jobs"][name]
    tasks = job["tasks"]
    # Strictly linear, every edge ALL_SUCCESS (the default; PowerCenter links are all "$prev.Status = Succeeded").
    for prev, task in zip([None] + tasks, tasks):
        assert deps(task) == ([prev["task_key"]] if prev else []), task["task_key"]
        assert task.get("run_if", "ALL_SUCCESS") == "ALL_SUCCESS"
    # Without the ops glue tasks the job is exactly the WORKFLOWLINK chain.
    assert [t["task_key"] for t in tasks if not is_ops_glue(t)] == [n for n, _, _ in chain]

    sessions = {s.get("NAME"): s for s in root.iter("SESSION")}
    setters: list[str] = []
    by_key = {t["task_key"]: t for t in tasks}
    for task in tasks:
        a = args_of(task)
        assert a["--catalog"] == "${var.catalog}"
        assert task["python_wheel_task"]["entry_point"] == "biis-run"
        assert a["--workflow"] in WORKFLOW_MODULES
        if a["--workflow"] == "ops":
            assert a["--task"] in OPS_TASKS
        else:
            assert a["--workflow"] == name and a["--task"] == task["task_key"]
        # Workflow variables assigned by earlier sessions reach every later task.
        assert a["--upstream-task"] == setters, task["task_key"]
        params = {p.split("=", 1)[0] for p in a["--param"]}
        assert params >= {p.removeprefix("$$").removeprefix("$Param_") for p in re.findall(r"\$(?:Param_|\$WF_)\w+", path.read_text(errors="replace"))}
        if task["task_key"] in sessions or any(task["task_key"] == n for n, t, _ in chain if t == "Session"):
            sess = sessions[next(tn for n, _, tn in chain if n == task["task_key"])]
            if any(vp.get("NAME", "").startswith("$$WF_")
                   for c in sess.iter("SESSIONCOMPONENT") if c.get("REFOBJECTNAME") == "postsession_success_variable_assignment"
                   for vp in c.iter("VALUEPAIR")):
                setters.append(task["task_key"])
    for n, ttype, _ in chain:
        if ttype == "Email":
            assert args_of(by_key[n])["--workflow"] == "ops" and EMAIL_TASKS[n][0] == name

    if name == "wf_EHRP2BIIS_UPDATE":
        assert "schedule" not in job  # scheduled through ehrp2biis_daily
    else:
        sched = next(root.iter("SCHEDULEINFO"))
        assert sched.get("SCHEDULETYPE") == "ONDEMAND"
        assert "schedule" not in job and "trigger" not in job and "continuous" not in job


def test_session_commands_match_xml():
    """Every pre/post-session command in the XML is reproduced, in EXECORDER, by exactly one ops task."""
    expected = {}
    for path in XML_FILES:
        root, wf, chain = xml_workflow(path)
        sessions = {s.get("NAME"): s for s in root.iter("SESSION")}
        for inst, ttype, tname in chain:
            if ttype != "Session":
                continue
            for comp in sessions[tname].iter("SESSIONCOMPONENT"):
                phase = {"pre_session_command": "pre", "post_session_success_command": "post"}.get(comp.get("REFOBJECTNAME"))
                cmds = sorted(comp.iter("VALUEPAIR"), key=lambda v: int(v.get("EXECORDER")))
                if phase and cmds:
                    expected[(wf.get("NAME"), inst, phase)] = [v.get("VALUE") for v in cmds]
    actual = {(c.workflow, c.session, c.phase): [s.xml_command for s in c.steps if s.xml_command != "Success Email"]
              for c in SESSION_COMMANDS}
    assert actual == expected


def test_success_emails_match_xml():
    found = {}
    for path in XML_FILES:
        root, wf, chain = xml_workflow(path)
        sessions = {s.get("NAME"): s for s in root.iter("SESSION")}
        for inst, ttype, tname in chain:
            if ttype != "Session":
                continue
            for comp in sessions[tname].iter("SESSIONCOMPONENT"):
                attrs = {a.get("NAME"): a.get("VALUE") for a in comp.iter("ATTRIBUTE")}
                if comp.get("REFOBJECTNAME") == "on_success_mail" and attrs.get("Email User Name", "").startswith("$$WF_"):
                    found[inst] = attrs
    ops = {c.session: s for c in SESSION_COMMANDS for s in c.steps if s.xml_command == "Success Email"}
    assert set(ops) == set(found)
    for session, email in ops.items():
        assert "{%s}" % found[session]["Email User Name"].removeprefix("$$") == email.to
        assert "{%s}" % found[session]["Email Subject"].removeprefix("$$") == email.subject
        attached = re.findall(r"%a<[^>]*/([^/>]+)>", found[session]["Email Text"])
        assert [p[-1] for p in email.attachments] == attached


def test_every_param_is_a_bundle_variable(bundle):
    refs = set()
    for path in XML_FILES:
        refs |= set(re.findall(r"\$(?:Param_|\$WF_)\w+", path.read_text(errors="replace")))
    names = {r.removeprefix("$$").removeprefix("$Param_") for r in refs}
    assert names and names <= set(bundle["variables"])
    for job in bundle["jobs"].values():
        used = set(re.findall(r"\$\{var\.([\w-]+)\}", yaml.safe_dump(job)))
        assert used <= set(bundle["variables"]), used - set(bundle["variables"])


def test_failure_notifications(bundle):
    for path in XML_FILES:
        root, wf, _ = xml_workflow(path)
        lists = {a.get("VALUE") for c in root.iter("SESSIONCOMPONENT") if c.get("REFOBJECTNAME") == "on_failure_mail"
                 for a in c.iter("ATTRIBUTE") if a.get("NAME") == "Email User Name" and a.get("VALUE")}
        if lists:
            job = bundle["jobs"][wf.get("NAME")]
            assert job["email_notifications"]["on_failure"] == ["${var.%s}" % sorted(lists)[0].removeprefix("$$")]


def test_ehrp2biis_daily(bundle):
    root = ET.parse(BUNDLE.parent / "XML" / "EHRP2BIIS_UPDATE").getroot()
    start = next(root.iter("STARTOPTIONS"))
    assert (start.get("STARTDATE"), start.get("STARTTIME")) == ("9/28/2018", "12:00")
    assert next(root.iter("SCHEDULEOPTIONS")).get("SCHEDULETYPE") == "RECURRING"
    rec = next(root.iter("RECURRING"))
    assert (rec.get("DAYS"), rec.get("HOURS"), rec.get("MINUTES")) == ("1", "0", "0")

    job = bundle["jobs"]["ehrp2biis_daily"]
    assert job["schedule"]["quartz_cron_expression"] == "0 0 12 * * ?"
    assert job["schedule"]["pause_status"] == "${var.ehrp_schedule_pause_status}"
    assert bundle["root"]["targets"]["prod"]["variables"]["ehrp_schedule_pause_status"] == "UNPAUSED"
    keys = [t["task_key"] for t in job["tasks"]]
    assert keys == ["preload", "wf_EHRP2BIIS_UPDATE", "afterload", "actstage_load"]
    assert [deps(t) for t in job["tasks"]] == [[], ["preload"], ["wf_EHRP2BIIS_UPDATE"], ["afterload"]]
    assert job["tasks"][1]["run_job_task"]["job_id"] == "${resources.jobs.wf_EHRP2BIIS_UPDATE.id}"
    for t in job["tasks"]:
        if t["task_key"] in EHRP_SQL_TASKS:
            assert args_of(t)["--workflow"] == "ehrp2biis_sql" and args_of(t)["--task"] == t["task_key"]


def test_biis_pay_period(bundle):
    job = bundle["jobs"]["biis_pay_period"]
    assert "schedule" not in job
    graph = {t["task_key"]: set(deps(t)) for t in job["tasks"]}
    for t in job["tasks"]:
        assert t["run_job_task"]["job_id"] == "${resources.jobs.%s.id}" % t["task_key"]
    assert graph == {
        "wf_Pay_Calendar": set(),
        "wf_Pseudossn": {"wf_Pay_Calendar"},
        "wf_COMPTIME": {"wf_Pay_Calendar"},
        "wf_CPM": {"wf_Pseudossn"},
        "wf_LES": {"wf_Pseudossn"},
        "wf_CPM_AFPS": {"wf_CPM"},
        "wf_CPM_CDC": {"wf_CPM"},
        "wf_CPM_NIH": {"wf_CPM"},
        "wf_CPM_OIG": {"wf_CPM"},
        "wf_FDA_Leave": {"wf_CPM_AFPS"},
    }
    every_wf = {xml_workflow(p)[1].get("NAME") for p in XML_FILES}
    assert set(graph) == every_wf - {"wf_EHRP2BIIS_UPDATE"}
