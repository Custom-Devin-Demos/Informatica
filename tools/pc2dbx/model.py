"""In-memory model of a PowerCenter XML export (POWERMART)."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional


@dataclass
class Port:
    name: str
    datatype: str = "string"
    precision: int = 0
    scale: int = 0
    porttype: str = ""
    expression: str = ""
    default: str = ""
    group: str = ""
    ref_field: str = ""
    ref_source_field: str = ""
    keytype: str = ""
    nullable: bool = True
    offset: int = 0
    physical_offset: int = 0
    physical_length: int = 0
    length: int = 0
    occurs: int = 0
    level: int = 0
    fieldtype: str = ""
    picture: str = ""
    usage: str = ""
    is_sortkey: bool = False
    sort_direction: str = ""
    expression_type: str = "GENERAL"
    attrs: dict = field(default_factory=dict)

    def has(self, kind: str) -> bool:
        return kind in self.porttype.split("/")

    @property
    def is_input(self) -> bool:
        return self.has("INPUT")

    @property
    def is_output(self) -> bool:
        return self.has("OUTPUT")

    @property
    def is_variable(self) -> bool:
        return self.has("LOCAL VARIABLE") or self.porttype == "VARIABLE"


@dataclass
class Source:
    name: str
    dbtype: str
    dbdname: str = ""
    owner: str = ""
    description: str = ""
    fields: list[Port] = field(default_factory=list)
    flatfile: dict = field(default_factory=dict)
    attrs: dict = field(default_factory=dict)


@dataclass
class Target:
    name: str
    dbtype: str
    owner: str = ""
    description: str = ""
    fields: list[Port] = field(default_factory=list)
    flatfile: dict = field(default_factory=dict)
    attrs: dict = field(default_factory=dict)

    @property
    def keys(self) -> list[str]:
        return [f.name for f in self.fields if "PRIMARY" in f.keytype.upper()]


@dataclass
class Group:
    name: str
    type: str
    expression: str = ""
    order: int = 0


@dataclass
class Transformation:
    name: str
    type: str
    description: str = ""
    reusable: bool = False
    ports: list[Port] = field(default_factory=list)
    attrs: dict = field(default_factory=dict)
    groups: list[Group] = field(default_factory=list)
    source_fields: list[Port] = field(default_factory=list)
    extra: dict = field(default_factory=dict)

    def port(self, name: str) -> Optional[Port]:
        for p in self.ports:
            if p.name == name:
                return p
        return None


@dataclass
class Instance:
    name: str
    transformation_name: str
    transformation_type: str
    type: str = "TRANSFORMATION"
    reusable: bool = False
    dbdname: str = ""
    associated_sources: list[str] = field(default_factory=list)


@dataclass
class Connector:
    from_instance: str
    from_field: str
    from_type: str
    to_instance: str
    to_field: str
    to_type: str


@dataclass
class Variable:
    name: str
    datatype: str = "string"
    precision: int = 0
    scale: int = 0
    is_param: bool = False
    default: str = ""
    aggfunction: str = ""
    description: str = ""


@dataclass
class Mapping:
    name: str
    description: str = ""
    is_mapplet: bool = False
    transformations: dict[str, Transformation] = field(default_factory=dict)
    instances: dict[str, Instance] = field(default_factory=dict)
    connectors: list[Connector] = field(default_factory=list)
    variables: list[Variable] = field(default_factory=list)
    target_load_order: list[str] = field(default_factory=list)

    def transformation_for(self, inst: Instance) -> Optional[Transformation]:
        return self.transformations.get(inst.transformation_name)

    def instance_counts(self) -> dict[str, int]:
        counts: dict[str, int] = {}
        for inst in self.instances.values():
            counts[inst.transformation_type] = counts.get(inst.transformation_type, 0) + 1
        return counts


@dataclass
class SessionExtension:
    instance: str
    type: str
    subtype: str = ""
    transformation_type: str = ""
    attrs: dict = field(default_factory=dict)
    connection: dict = field(default_factory=dict)


@dataclass
class Session:
    name: str
    mapping_name: str
    description: str = ""
    attrs: dict = field(default_factory=dict)
    extensions: list[SessionExtension] = field(default_factory=list)
    pre_assignments: list[tuple[str, str]] = field(default_factory=list)
    post_assignments: list[tuple[str, str]] = field(default_factory=list)
    pre_commands: list[str] = field(default_factory=list)
    post_commands: list[str] = field(default_factory=list)
    components: list[dict] = field(default_factory=list)
    transformation_overrides: dict[str, dict] = field(default_factory=dict)

    def extension(self, instance: str, type_: str | None = None) -> Optional[SessionExtension]:
        for ext in self.extensions:
            if ext.instance == instance and (type_ is None or ext.type == type_):
                return ext
        return None


@dataclass
class Task:
    name: str
    type: str
    description: str = ""
    attrs: dict = field(default_factory=dict)
    commands: list[tuple[str, str]] = field(default_factory=list)


@dataclass
class TaskInstance:
    name: str
    task_name: str
    task_type: str
    reusable: bool = False
    fail_parent: bool = False


@dataclass
class WorkflowLink:
    from_task: str
    to_task: str
    condition: str = ""


@dataclass
class Workflow:
    name: str
    description: str = ""
    server: str = ""
    schedule: dict = field(default_factory=dict)
    variables: list[Variable] = field(default_factory=list)
    tasks: dict[str, Task] = field(default_factory=dict)
    sessions: dict[str, Session] = field(default_factory=dict)
    task_instances: list[TaskInstance] = field(default_factory=list)
    links: list[WorkflowLink] = field(default_factory=list)
    attrs: dict = field(default_factory=dict)

    def ordered_tasks(self) -> list[str]:
        """Task instance names in dependency (topological) order, ties by XML order."""
        names = [t.name for t in self.task_instances]
        pos = {n: i for i, n in enumerate(names)}
        preds: dict[str, set[str]] = {n: set() for n in names}
        for link in self.links:
            preds.setdefault(link.to_task, set()).add(link.from_task)
            preds.setdefault(link.from_task, set())
            pos.setdefault(link.to_task, len(pos))
            pos.setdefault(link.from_task, len(pos))
        done: list[str] = []
        remaining = dict(preds)
        while remaining:
            ready = sorted((n for n, p in remaining.items() if not (p - set(done))), key=pos.get)
            if not ready:  # cycle: fall back to XML order
                ready = sorted(remaining, key=pos.get)[:1]
            for n in ready:
                done.append(n)
                remaining.pop(n)
        return done

    def ordered_sessions(self) -> list[Session]:
        by_inst = {t.name: t.task_name for t in self.task_instances}
        out = []
        for name in self.ordered_tasks():
            sess = self.sessions.get(by_inst.get(name, name))
            if sess is not None and sess not in out:
                out.append(sess)
        for sess in self.sessions.values():
            if sess not in out:
                out.append(sess)
        return out

    def predecessors(self, task_instance: str) -> list[tuple[str, str]]:
        return [(l.from_task, l.condition) for l in self.links if l.to_task == task_instance]


@dataclass
class Folder:
    name: str
    description: str = ""
    sources: dict[str, Source] = field(default_factory=dict)
    targets: dict[str, Target] = field(default_factory=dict)
    transformations: dict[str, Transformation] = field(default_factory=dict)
    mapplets: dict[str, Mapping] = field(default_factory=dict)
    mappings: dict[str, Mapping] = field(default_factory=dict)
    sessions: dict[str, Session] = field(default_factory=dict)
    workflows: dict[str, Workflow] = field(default_factory=dict)
    tasks: dict[str, Task] = field(default_factory=dict)

    def all_sessions(self) -> dict[str, Session]:
        out = dict(self.sessions)
        for wf in self.workflows.values():
            out.update(wf.sessions)
        return out

    def source_owner(self, name: str) -> str:
        for src in self.sources.values():
            if src.name.upper() == name.upper() and src.owner:
                return src.owner
        return ""


@dataclass
class Export:
    path: str
    repository: str = ""
    database_type: str = ""
    creation_date: str = ""
    folders: list[Folder] = field(default_factory=list)

    @property
    def name(self) -> str:
        from pathlib import Path

        return Path(self.path).name
