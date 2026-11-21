"""``python -m pc2dbx <xml...> -o <dir>``"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from .codegen import area_for, generate_workflow, module_name
from .inventory import build_inventory
from .parser import parse_file


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="pc2dbx", description="PowerCenter XML -> PySpark/Delta skeletons")
    ap.add_argument("xml", nargs="+", help="PowerCenter XML export(s)")
    ap.add_argument("-o", "--out", required=True, help="output directory")
    ap.add_argument("--flat", action="store_true", help="write modules directly in OUT (no area packages)")
    args = ap.parse_args(argv)

    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    exports, issues = [], []
    for path in args.xml:
        exp = parse_file(path)
        exports.append(exp)
        for folder in exp.folders:
            for wf in folder.workflows.values():
                code, wf_issues = generate_workflow(folder, wf, Path(path).as_posix())
                target_dir = out if args.flat else out / area_for(wf.name, folder)
                target_dir.mkdir(parents=True, exist_ok=True)
                if not args.flat:
                    (target_dir / "__init__.py").touch()
                target = target_dir / f"{module_name(wf.name)}.py"
                target.write_text(code)
                issues.extend(wf_issues)
                print(f"{wf.name} -> {target} ({len(wf_issues)} conversion notes)")
    inv = out / "inventory.json"
    inv.write_text(json.dumps(build_inventory(exports, issues), indent=2))
    print(f"inventory -> {inv}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
