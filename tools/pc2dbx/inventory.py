"""JSON inventory of PowerCenter exports (counts match databricks/PLAN.md §2)."""
from __future__ import annotations

from collections import Counter

from .model import Export, Folder

ABBREV = {
    "Source Qualifier": "SQ", "Expression": "EXP", "Lookup Procedure": "LKP", "Joiner": "JNR",
    "Aggregator": "AGG", "Router": "RTR", "Filter": "FIL", "Update Strategy": "UPD", "Sequence": "SEQ",
    "Sorter": "SRT", "Normalizer": "NRM", "Mapplet": "MPLT",
}
NON_TRANSFORMATION = {"Source Definition", "Target Definition"}


def transformation_counts(folder: Folder) -> Counter:
    """Transformation instances used directly by mappings (mapplet internals are not expanded)."""
    counts: Counter = Counter()
    for m in folder.mappings.values():
        for kind, n in m.instance_counts().items():
            if kind not in NON_TRANSFORMATION:
                counts[kind] += n
    return counts


def folder_summary(folder: Folder) -> dict:
    counts = transformation_counts(folder)
    return {
        "folder": folder.name,
        "workflows": len(folder.workflows),
        "mappings": len(folder.mappings),
        "mapplets": len(folder.mapplets),
        "sessions": len(folder.all_sessions()),
        "transformations": sum(counts.values()),
        "transformation_counts": {ABBREV.get(k, k): v for k, v in sorted(counts.items())},
        "sources_by_type": dict(Counter(s.dbtype for s in folder.sources.values())),
        "targets_by_type": dict(Counter(t.dbtype for t in folder.targets.values())),
    }


def folder_inventory(folder: Folder) -> dict:
    out = folder_summary(folder)
    out["sources"] = [
        {"name": s.name, "type": s.dbtype, "owner": s.owner, "dbd": s.dbdname, "fields": len(s.fields)}
        for s in folder.sources.values()
    ]
    out["targets"] = [
        {"name": t.name, "type": t.dbtype, "keys": t.keys, "fields": len(t.fields)} for t in folder.targets.values()
    ]
    out["mappings_detail"] = [
        {
            "name": m.name,
            "instances": {i.name: i.transformation_type for i in m.instances.values()},
            "connectors": len(m.connectors),
            "variables": [{"name": v.name, "datatype": v.datatype, "is_param": v.is_param, "default": v.default}
                          for v in m.variables],
            "target_load_order": m.target_load_order,
        }
        for m in list(folder.mapplets.values()) + list(folder.mappings.values())
    ]
    out["workflows_detail"] = [
        {
            "name": wf.name,
            "schedule": wf.schedule,
            "variables": [{"name": v.name, "datatype": v.datatype, "default": v.default} for v in wf.variables],
            "tasks": [{"name": t.name, "task": t.task_name, "type": t.task_type} for t in wf.task_instances],
            "order": wf.ordered_tasks(),
            "links": [{"from": l.from_task, "to": l.to_task, "condition": l.condition} for l in wf.links],
            "sessions": [
                {
                    "name": s.name,
                    "mapping": s.mapping_name,
                    "treat_source_rows_as": s.attrs.get("Treat source rows as", ""),
                    "pre_assignments": s.pre_assignments,
                    "post_assignments": s.post_assignments,
                    "pre_commands": s.pre_commands,
                    "post_commands": s.post_commands,
                }
                for s in wf.ordered_sessions()
            ],
        }
        for wf in folder.workflows.values()
    ]
    return out


def build_inventory(exports: list[Export], issues: list[dict] | None = None) -> dict:
    folders = []
    for exp in exports:
        for f in exp.folders:
            item = folder_inventory(f)
            item["export"] = exp.name
            item["repository"] = exp.repository
            folders.append(item)
    totals = Counter()
    for f in folders:
        for key in ("workflows", "mappings", "mapplets", "sessions", "transformations"):
            totals[key] += f[key]
    return {"exports": len(exports), "totals": dict(totals), "folders": folders, "conversion_issues": issues or []}
