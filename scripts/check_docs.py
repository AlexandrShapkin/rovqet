#!/usr/bin/env python3
"""Fast, dependency-free documentation and task-graph consistency check."""
from __future__ import annotations
import json
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
FAILURES: list[str] = []
REQUIRED = [
    "README.md", "AGENTS.md",
    "docs/requirements/BROKER_REQUIREMENTS_v1.1.1.en.md",
    "docs/requirements/CHANGELOG_1.0_to_1.1.1.en.md",
    "docs/roadmap/README.md", "docs/roadmap/work-packages.md",
    "docs/roadmap/tasks.json", "docs/roadmap/agent-task-protocol.md",
    "docs/architecture/diagrams/workspace.dsl",
    "docs/architecture/diagrams/flows.puml",
    "docs/architecture/diagrams/states.puml",
]
REQUIRED += [f"docs/architecture/{n:02d}-" for n in range(17)]

def issue(message: str) -> None:
    FAILURES.append(message)

for path in REQUIRED:
    if path.endswith("-"):
        if not list((ROOT / "docs/architecture").glob(Path(path).name + "*.md")):
            issue(f"missing architecture chapter {path}*.md")
    elif not (ROOT / path).is_file():
        issue(f"missing required file {path}")

files = [*ROOT.rglob("*.md")]
link_re = re.compile(r"(?<!!)\[[^\]]+\]\(([^)]+)\)")
for source in files:
    text = source.read_text(encoding="utf-8")
    in_fence = False
    for line in text.splitlines():
        stripped = line.lstrip()
        if stripped.startswith(("```", "~~~")):
            in_fence = not in_fence
            continue
        if in_fence:
            continue
        for href in link_re.findall(line):
            href = href.split("#", 1)[0].strip().strip("<>")
            if not href or href.startswith(("https://", "http://", "mailto:", "data:")):
                continue
            if " " in href:
                issue(f"{source.relative_to(ROOT)}: whitespace/unsupported local link {href}")
                continue
            dest = (source.parent / href).resolve()
            if not dest.is_relative_to(ROOT.resolve()) or not dest.exists():
                issue(f"{source.relative_to(ROOT)}: broken local link {href}")

norm = ROOT / "docs/requirements/BROKER_REQUIREMENTS_v1.1.1.en.md"
if norm.exists():
    src = norm.read_text(encoding="utf-8")
    for item in ("FR-QOS-007", "FR-GRP-006", "FR-CLU-004",
                 "INV-13", "INV-16", "TEST-017"):
        if item not in src:
            issue(f"normative contract missing {item}")

ledger_path = ROOT / "docs/roadmap/tasks.json"
if ledger_path.exists():
    try:
        tasks = json.loads(ledger_path.read_text(encoding="utf-8"))["tasks"]
        ids = [t["id"] for t in tasks]
        if len(ids) != len(set(ids)):
            issue("duplicate task IDs")
        mapping = {t["id"]: t for t in tasks}
        allowed = {"TODO", "IN_PROGRESS", "DONE", "BLOCKED"}
        for t in tasks:
            if t["status"] not in allowed:
                issue(f"{t['id']}: invalid status")
            if t["status"] == "DONE":
                e = t.get("evidence")
                if not isinstance(e, dict) or not e.get("commit") or not e.get("verification"):
                    issue(f"{t['id']}: DONE without commit and verification evidence")
            for dep in t["depends_on"]:
                if dep not in mapping:
                    issue(f"{t['id']}: missing dependency {dep}")
                elif t["status"] == "DONE" and mapping[dep]["status"] != "DONE":
                    issue(f"{t['id']}: DONE with unfinished dependency {dep}")
        colors: dict[str, int] = {}
        def visit(i: str) -> None:
            if colors.get(i) == 1:
                issue(f"dependency cycle at {i}")
                return
            if colors.get(i) == 2:
                return
            colors[i] = 1
            for dep in mapping[i]["depends_on"]:
                if dep in mapping:
                    visit(dep)
            colors[i] = 2
        for i in ids:
            visit(i)
        package_text = (ROOT / "docs/roadmap/work-packages.md").read_text()
        for i in ids:
            if i not in package_text:
                issue(f"roadmap task {i} not described in work-packages.md")
    except (ValueError, KeyError, TypeError) as exc:
        issue(f"invalid task ledger: {exc}")

for name in ("flows.puml", "states.puml"):
    f = ROOT / "docs/architecture/diagrams" / name
    if f.exists():
        s = f.read_text()
        if s.count("@startuml") != s.count("@enduml"):
            issue(f"{name}: unmatched @startuml/@enduml")

if FAILURES:
    print("documentation checks FAILED")
    for item in FAILURES:
        print(" -", item)
    sys.exit(1)
print(f"documentation checks PASS: {len(files)} markdown files, {len(tasks) if ledger_path.exists() else 0} roadmap tasks")
