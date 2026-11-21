"""Parse PowerCenter XML exports (POWERMART DTD) into the pc2dbx model."""
from __future__ import annotations

import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Iterable

from .model import (
    Connector,
    Export,
    Folder,
    Group,
    Instance,
    Mapping,
    Port,
    Session,
    SessionExtension,
    Source,
    Target,
    Task,
    TaskInstance,
    Transformation,
    Variable,
    Workflow,
    WorkflowLink,
)


def _int(value: str | None) -> int:
    try:
        return int(value or 0)
    except ValueError:
        return 0


def _attrs(el: ET.Element, tag: str = "ATTRIBUTE") -> dict[str, str]:
    return {a.get("NAME", ""): a.get("VALUE", "") for a in el.findall(tag)}


def _port(el: ET.Element) -> Port:
    g = el.get
    return Port(
        name=g("NAME", ""),
        datatype=g("DATATYPE", "string"),
        precision=_int(g("PRECISION")),
        scale=_int(g("SCALE")),
        porttype=g("PORTTYPE", ""),
        expression=g("EXPRESSION", ""),
        default=g("DEFAULTVALUE", ""),
        group=g("GROUP", ""),
        ref_field=g("REF_FIELD", ""),
        ref_source_field=g("REF_SOURCE_FIELD", ""),
        keytype=g("KEYTYPE", ""),
        nullable=g("NULLABLE", "NULL") != "NOTNULL",
        offset=_int(g("OFFSET")),
        physical_offset=_int(g("PHYSICALOFFSET")),
        physical_length=_int(g("PHYSICALLENGTH")),
        length=_int(g("LENGTH")),
        occurs=_int(g("OCCURS")),
        level=_int(g("LEVEL")),
        fieldtype=g("FIELDTYPE", ""),
        picture=g("PICTURETEXT", ""),
        usage=g("USAGE_FLAGS", "") or g("USAGE", ""),
        is_sortkey=g("ISSORTKEY", "NO") == "YES",
        sort_direction=g("SORTDIRECTION", ""),
        expression_type=g("EXPRESSIONTYPE", "GENERAL"),
        attrs={k: v for k, v in el.attrib.items() if k in ("REDEFINES", "FIELDPROPERTY", "MAPPLETGROUP")},
    )


def _flatfile(el: ET.Element) -> dict:
    ff = el.find("FLATFILE")
    return dict(ff.attrib) if ff is not None else {}


def parse_source(el: ET.Element) -> Source:
    return Source(
        name=el.get("NAME", ""),
        dbtype=el.get("DATABASETYPE", ""),
        dbdname=el.get("DBDNAME", ""),
        owner=el.get("OWNERNAME", ""),
        description=el.get("DESCRIPTION", ""),
        fields=[_port(f) for f in el.findall("SOURCEFIELD")],
        flatfile=_flatfile(el),
        attrs=_attrs(el, "TABLEATTRIBUTE"),
    )


def parse_target(el: ET.Element) -> Target:
    return Target(
        name=el.get("NAME", ""),
        dbtype=el.get("DATABASETYPE", ""),
        description=el.get("DESCRIPTION", ""),
        fields=[_port(f) for f in el.findall("TARGETFIELD")],
        flatfile=_flatfile(el),
        attrs=_attrs(el, "TABLEATTRIBUTE"),
    )


def parse_transformation(el: ET.Element) -> Transformation:
    return Transformation(
        name=el.get("NAME", ""),
        type=el.get("TYPE", ""),
        description=el.get("DESCRIPTION", ""),
        reusable=el.get("REUSABLE", "NO") == "YES",
        ports=[_port(f) for f in el.findall("TRANSFORMFIELD")],
        attrs=_attrs(el, "TABLEATTRIBUTE"),
        groups=[
            Group(g.get("NAME", ""), g.get("TYPE", ""), g.get("EXPRESSION", ""), _int(g.get("ORDER")))
            for g in el.findall("GROUP")
        ],
        source_fields=[_port(f) for f in el.findall("SOURCEFIELD")],
        extra={k: el.get(k, "") for k in ("ISVSAM_NORMALIZER", "REF_SOURCE_NAME", "REF_DBD_NAME") if el.get(k)},
    )


def _variable(el: ET.Element) -> Variable:
    return Variable(
        name=el.get("NAME", ""),
        datatype=el.get("DATATYPE", "string"),
        precision=_int(el.get("PRECISION")),
        scale=_int(el.get("SCALE")),
        is_param=el.get("ISPARAM", "NO") == "YES",
        default=el.get("DEFAULTVALUE", ""),
        aggfunction=el.get("AGGFUNCTION", ""),
        description=el.get("DESCRIPTION", ""),
    )


def parse_mapping(el: ET.Element, folder_transformations: dict[str, Transformation] | None = None) -> Mapping:
    m = Mapping(name=el.get("NAME", ""), description=el.get("DESCRIPTION", ""), is_mapplet=el.tag == "MAPPLET")
    if folder_transformations:
        m.transformations.update(folder_transformations)
    for t in el.findall("TRANSFORMATION"):
        tr = parse_transformation(t)
        m.transformations[tr.name] = tr
    for i in el.findall("INSTANCE"):
        inst = Instance(
            name=i.get("NAME", ""),
            transformation_name=i.get("TRANSFORMATION_NAME", ""),
            transformation_type=i.get("TRANSFORMATION_TYPE", ""),
            type=i.get("TYPE", "TRANSFORMATION"),
            reusable=i.get("REUSABLE", "NO") == "YES",
            dbdname=i.get("DBDNAME", ""),
            associated_sources=[a.get("NAME", "") for a in i.findall("ASSOCIATED_SOURCE_INSTANCE")],
        )
        m.instances[inst.name] = inst
    for c in el.findall("CONNECTOR"):
        m.connectors.append(
            Connector(
                c.get("FROMINSTANCE", ""), c.get("FROMFIELD", ""), c.get("FROMINSTANCETYPE", ""),
                c.get("TOINSTANCE", ""), c.get("TOFIELD", ""), c.get("TOINSTANCETYPE", ""),
            )
        )
    m.variables = [_variable(v) for v in el.findall("MAPPINGVARIABLE")]
    m.target_load_order = [
        t.get("TARGETINSTANCE", "") for t in sorted(el.findall("TARGETLOADORDER"), key=lambda x: _int(x.get("ORDER")))
    ]
    return m


def parse_session(el: ET.Element) -> Session:
    s = Session(
        name=el.get("NAME", ""),
        mapping_name=el.get("MAPPINGNAME", ""),
        description=el.get("DESCRIPTION", ""),
        attrs=_attrs(el),
    )
    for st in el.findall("SESSTRANSFORMATIONINST"):
        a = _attrs(st)
        if a:
            s.transformation_overrides[st.get("SINSTANCENAME", "")] = a
    for comp in el.findall("SESSIONCOMPONENT"):
        ctype = comp.get("TYPE", "")
        pairs = [(v.get("NAME", ""), v.get("VALUE", "")) for v in comp.iter("VALUEPAIR")]
        s.components.append({"type": ctype, "ref": comp.get("REFOBJECTNAME", ""), "values": pairs})
        if ctype == "Pre-session variable assignment":
            s.pre_assignments.extend(pairs)
        elif ctype.startswith("Post-session") and ctype.endswith("variable assignment"):
            s.post_assignments.extend(pairs)
        elif ctype == "Pre-session command":
            s.pre_commands.extend(v for _, v in pairs)
        elif ctype.startswith("Post-session") and ctype.endswith("command"):
            s.post_commands.extend(v for _, v in pairs)
    for ext in el.findall("SESSIONEXTENSION"):
        conn = ext.find("CONNECTIONREFERENCE")
        s.extensions.append(
            SessionExtension(
                instance=ext.get("SINSTANCENAME", ""),
                type=ext.get("TYPE", ""),
                subtype=ext.get("SUBTYPE", ""),
                transformation_type=ext.get("TRANSFORMATIONTYPE", ""),
                attrs=_attrs(ext),
                connection=dict(conn.attrib) if conn is not None else {},
            )
        )
    return s


def _task(el: ET.Element) -> Task:
    return Task(
        name=el.get("NAME", ""),
        type=el.get("TYPE", ""),
        description=el.get("DESCRIPTION", ""),
        attrs=_attrs(el),
        commands=[(v.get("NAME", ""), v.get("VALUE", "")) for v in el.iter("VALUEPAIR")],
    )


def _schedule(el: ET.Element | None) -> dict:
    if el is None:
        return {"type": "ONDEMAND"}
    info = el.find("SCHEDULEINFO")
    out: dict = {"name": el.get("NAME", ""), "type": info.get("SCHEDULETYPE", "") if info is not None else ""}
    if info is not None:
        for child in info.iter():
            if child is info:
                continue
            if child.attrib:
                out[child.tag.lower()] = dict(child.attrib)
    return out


def parse_workflow(el: ET.Element) -> Workflow:
    wf = Workflow(
        name=el.get("NAME", ""),
        description=el.get("DESCRIPTION", ""),
        server=el.get("SERVERNAME", ""),
        schedule=_schedule(el.find("SCHEDULER")),
        attrs=_attrs(el),
    )
    for t in el.findall("TASK"):
        task = _task(t)
        wf.tasks[task.name] = task
    for s in el.findall("SESSION"):
        sess = parse_session(s)
        wf.sessions[sess.name] = sess
    for ti in el.findall("TASKINSTANCE"):
        wf.task_instances.append(
            TaskInstance(
                name=ti.get("NAME", ""),
                task_name=ti.get("TASKNAME", ""),
                task_type=ti.get("TASKTYPE", ""),
                reusable=ti.get("REUSABLE", "NO") == "YES",
                fail_parent=ti.get("FAIL_PARENT_IF_INSTANCE_FAILS", "NO") == "YES",
            )
        )
    wf.links = [
        WorkflowLink(l.get("FROMTASK", ""), l.get("TOTASK", ""), l.get("CONDITION", ""))
        for l in el.findall("WORKFLOWLINK")
    ]
    wf.variables = [
        Variable(
            name=v.get("NAME", ""),
            datatype=v.get("DATATYPE", "string"),
            default=v.get("DEFAULTVALUE", ""),
            description=v.get("DESCRIPTION", ""),
            is_param=False,
            aggfunction="",
        )
        for v in el.findall("WORKFLOWVARIABLE")
        if v.get("USERDEFINED", "NO") == "YES"
    ]
    return wf


def parse_folder(el: ET.Element) -> Folder:
    f = Folder(name=el.get("NAME", ""), description=el.get("DESCRIPTION", ""))
    for s in el.findall("SOURCE"):
        src = parse_source(s)
        f.sources[src.name] = src
    for t in el.findall("TARGET"):
        tgt = parse_target(t)
        f.targets[tgt.name] = tgt
    for t in el.findall("TRANSFORMATION"):
        tr = parse_transformation(t)
        f.transformations[tr.name] = tr
    for m in el.findall("MAPPLET"):
        mp = parse_mapping(m, f.transformations)
        f.mapplets[mp.name] = mp
    for m in el.findall("MAPPING"):
        mp = parse_mapping(m, f.transformations)
        f.mappings[mp.name] = mp
    for t in el.findall("TASK"):
        task = _task(t)
        f.tasks[task.name] = task
    for s in el.findall("SESSION"):
        sess = parse_session(s)
        f.sessions[sess.name] = sess
    for w in el.findall("WORKFLOW"):
        wf = parse_workflow(w)
        for ti in wf.task_instances:  # reusable sessions defined at folder level
            if ti.task_type == "Session" and ti.task_name not in wf.sessions and ti.task_name in f.sessions:
                wf.sessions[ti.task_name] = f.sessions[ti.task_name]
            if ti.task_name not in wf.tasks and ti.task_name in f.tasks:
                wf.tasks[ti.task_name] = f.tasks[ti.task_name]
        f.workflows[wf.name] = wf
    return f


def parse_file(path: str | Path) -> Export:
    root = ET.parse(str(path)).getroot()
    if root.tag != "POWERMART":
        raise ValueError(f"{path}: not a PowerCenter export (root <{root.tag}>)")
    exp = Export(path=str(path), creation_date=root.get("CREATION_DATE", ""))
    for repo in root.findall("REPOSITORY"):
        exp.repository = repo.get("NAME", "")
        exp.database_type = repo.get("DATABASETYPE", "")
        exp.folders.extend(parse_folder(f) for f in repo.findall("FOLDER"))
    return exp


def parse_files(paths: Iterable[str | Path]) -> list[Export]:
    return [parse_file(p) for p in paths]
