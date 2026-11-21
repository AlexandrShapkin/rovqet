#!/usr/bin/env python3
"""Reproducible evidence manifest and SHA-256 index for the C-02 safety campaign.

Writes exactly two files under ``benchmarks/evidence/c02-safety-matrix/``:

* ``manifest.json`` (schema ``rovqet-c02-evidence-v1``): the current build and
  machine identity, the exact ``commands`` of the safety campaigns (fault
  injection builds, checks/tests, campaign scripts — every campaign line is
  verified to exist under ``scripts/``), the normal-build
  ``production_commands`` (the same cargo test lines plus the production
  build), one entry per retained safety
  scenario (per safety journal, per crash-matrix verdict and per bundle-campaign
  verdict) with its barrier, kill/restart observations and the independent
  ``q1_oracle.reconcile_safety`` verdict, plus the bundle verdict recomputed
  with ``q1_oracle.reconcile_bundle``.
* ``index.sha256``: a ``sha256sum``-compatible index of every retained file
  under ``c02-safety-matrix``, ``c02-crash-matrix`` and ``c02-bundle-campaign``
  (paths relative to ``benchmarks/evidence``, sorted by path), so that
  ``sha256sum -c c02-safety-matrix/index.sha256`` verifies from inside
  ``benchmarks/evidence``.

Rules honoured here:

* A journal that cannot be loaded or reconciled is recorded with an explicit
  INVALID or NOT_RUN verdict and its reason; it is never reported as a pass.
* A scenario that never reached its barrier is visible as ``barrier_reached:
  false`` in its manifest entry.
* Nothing under ``benchmarks/evidence/`` is modified except the two files this
  tool writes.  ``manifest.json`` and ``index.sha256`` are excluded from the
  index so that two runs over the same inputs produce identical index content
  (only the manifest timestamp changes).

Python 3.12 standard library only.
"""
from __future__ import annotations

import hashlib
import json
import os
import platform
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import q1_oracle  # noqa: E402  (sibling script on sys.path)

ROOT = Path(__file__).resolve().parent.parent
EVIDENCE_DIR = ROOT / "benchmarks" / "evidence"
SAFETY_DIR = EVIDENCE_DIR / "c02-safety-matrix"
CRASH_DIR = EVIDENCE_DIR / "c02-crash-matrix"
BUNDLE_DIR = EVIDENCE_DIR / "c02-bundle-campaign"
MANIFEST_PATH = SAFETY_DIR / "manifest.json"
INDEX_PATH = SAFETY_DIR / "index.sha256"
BUNDLE_JOURNAL = BUNDLE_DIR / "journal.jsonl"
BUNDLE_VERDICT = BUNDLE_DIR / "verdict.json"
CRASH_VERDICTS = CRASH_DIR / "verdicts.json"
SCHEMA = "rovqet-c02-evidence-v1"


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace(
        "+00:00", "Z"
    )


def _repo_relative(path: Path) -> str:
    return path.resolve().relative_to(ROOT).as_posix()


def _run(argv: list[str]) -> subprocess.CompletedProcess | None:
    try:
        return subprocess.run(
            argv, cwd=ROOT, capture_output=True, text=True, check=False
        )
    except OSError:
        return None


def _sha256_hex(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _sha16(path: Path) -> str | None:
    """The 16 hex character digest prefix of a build artifact, null if absent."""
    if not path.is_file():
        return None
    try:
        return _sha256_hex(path)[:16]
    except OSError:
        return None


def _git_build() -> dict:
    build: dict = {"commit": None, "status": [], "binaries": {}}

    head = _run(["git", "rev-parse", "HEAD"])
    if head is not None and head.returncode == 0:
        build["commit"] = head.stdout.strip()
    else:
        reason = (
            head.stderr.strip()
            if head is not None and head.stderr.strip()
            else "git rev-parse HEAD unavailable"
        )
        build["commit_error"] = reason

    status = _run(["git", "status", "--porcelain"])
    if status is not None and status.returncode == 0:
        build["status"] = status.stdout.splitlines()
    else:
        build["status"] = []
        build["status_error"] = "git status --porcelain unavailable"

    binaries: dict[str, str | None] = {}
    for relative in ("target/debug/rovqet", "target/debug/metadata-raft-node"):
        binaries[relative] = _sha16(ROOT / relative)
    examples = ROOT / "target" / "debug" / "examples"
    if examples.is_dir():
        try:
            entries = sorted(examples.iterdir())
        except OSError:
            entries = []
        for entry in entries:
            # Skip cargo's *.d dependency files: only the example binaries count.
            if entry.is_file() and entry.suffix != ".d":
                binaries[entry.relative_to(ROOT).as_posix()] = _sha16(entry)
    build["binaries"] = binaries
    return build


def _machine() -> dict:
    info: dict = {
        "platform": platform.platform(),
        "python_version": platform.python_version(),
        "cpu_count": os.cpu_count(),
        "filesystem_type": "unknown",
        "filesystem_total_bytes": None,
        "filesystem_available_bytes": None,
    }
    stat = _run(["stat", "-f", "-c", "%T", str(EVIDENCE_DIR)])
    if stat is not None and stat.returncode == 0 and stat.stdout.strip():
        info["filesystem_type"] = stat.stdout.strip()
    try:
        vfs = os.statvfs(EVIDENCE_DIR)
        info["filesystem_total_bytes"] = vfs.f_blocks * vfs.f_frsize
        info["filesystem_available_bytes"] = vfs.f_bavail * vfs.f_frsize
    except (AttributeError, OSError) as error:
        info["filesystem_statvfs_error"] = str(error)
    return info


# The exact build commands of the safety campaigns: these are the builds that
# produce the binaries the campaigns run (target/debug/rovqet and
# target/debug/metadata-raft-node).  Nothing here is reconstructed or guessed.
CAMPAIGN_BUILD_COMMANDS = [
    "cargo build -p rovqet-server --features fault-injection --locked",
    "cargo build -p rovqet-metadata-raft --features fault-injection --bins --locked",
]

# The exact check/test commands of the safety campaigns.
CAMPAIGN_TEST_COMMANDS = [
    "cargo fmt --all -- --check",
    "cargo check --workspace --locked",
    "cargo test --workspace --locked",
    "cargo test -p rovqet-metadata-raft --locked",
    "cargo test -p rovqet-durable --locked",
    "cargo test -p rovqet-server --locked",
]

# The normal (non fault-injection) build commands recorded separately.
PRODUCTION_BUILD_COMMANDS = [
    "cargo build -p rovqet-server -p rovqet-sdk --examples --bins --locked",
]

# The campaign scripts, in reader order.  An entry is only emitted as a command
# line when the script actually exists under ``scripts/``; a missing script is
# skipped and recorded in the manifest ``errors`` list instead of being
# reported as a command this repository runs.
CAMPAIGN_SCRIPTS = [
    "scripts/test_q1_migration_crash.py",
    "scripts/test_q1_migration_bundle.py",
    "scripts/test_q1_migration_partition.py",
    "scripts/test_q1_dlq_uncertainty.py",
    "scripts/test_q1_oracle.py",
    "scripts/check_docs.py",
    "scripts/q0_smoke.py",
    "scripts/q1_smoke.py --check-hooks-disabled",
    "scripts/test_metadata_raft.py",
    "scripts/test_cluster_routing.py",
    "scripts/test_q1_replay.py",
    "scripts/test_q1_consumer.py",
    "scripts/test_q1_terminal.py",
    "scripts/test_q1_durability.py",
    "scripts/test_q1_faults.py",
]


def _campaign_commands(errors: list[str]) -> list[str]:
    """The exact campaign command lines: builds, then tests, then campaigns.

    The list is deterministic and sorted by role; it never depends on the
    current working directory contents.  Campaign entries whose script does
    not exist in ``scripts/`` are skipped and recorded in ``errors``.
    """
    commands = list(CAMPAIGN_BUILD_COMMANDS) + list(CAMPAIGN_TEST_COMMANDS)
    for entry in CAMPAIGN_SCRIPTS:
        script = entry.split(" ", 1)[0]
        if not (ROOT / script).is_file():
            errors.append(f"campaign script not found: {script}")
            continue
        commands.append(f"python3 {entry}")
    return commands


def _production_commands() -> list[str]:
    """The normal-build command lines: the test commands plus the production build."""
    return list(CAMPAIGN_TEST_COMMANDS) + list(PRODUCTION_BUILD_COMMANDS)


def _read_json(path: Path, errors: list[str]) -> dict | None:
    try:
        loaded = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        errors.append(f"missing campaign file: {_repo_relative(path)}")
        return None
    except (OSError, json.JSONDecodeError) as error:
        errors.append(f"unreadable campaign file {_repo_relative(path)}: {error}")
        return None
    if not isinstance(loaded, dict):
        errors.append(f"campaign file {_repo_relative(path)} is not a JSON object")
        return None
    return loaded


def _load_journal(
    path: Path,
) -> tuple[list[dict] | None, str | None, str | None]:
    """Loads a journal; on failure returns (None, verdict, reason).

    A missing journal is NOT_RUN, an unreadable or malformed one is INVALID.
    """
    if not path.is_file():
        return None, q1_oracle.NOT_RUN, f"journal not found: {_repo_relative(path)}"
    try:
        return q1_oracle.load(path), None, None
    except q1_oracle.JournalError as error:
        return None, q1_oracle.INVALID, str(error)
    except OSError as error:
        return None, q1_oracle.INVALID, f"unreadable journal: {error}"
    except Exception as error:  # never fail silently on unexpected corruption
        return (
            None,
            q1_oracle.INVALID,
            f"journal load failed: {type(error).__name__}: {error}",
        )


def _scenario_entry(
    scenario: str | None,
    source: str,
    journal: Path,
    stored: dict | None,
    campaign: dict | None = None,
) -> dict:
    """One manifest entry: barrier, cases, kills/restarts and the oracle verdict."""
    entry: dict = {
        "scenario": scenario,
        "source": source,
        "journal": _repo_relative(journal),
        "barrier": None,
        "barrier_reached": None,
        "executed_cases": 0,
        "process_killed": [],
        "process_restarted": [],
        "oracle": {
            "verdict": q1_oracle.NOT_RUN,
            "reasons": ["journal not associated with this entry"],
            "counts": {},
        },
        "journal_conclusion": None,
        "stored_verdict": stored,
    }
    if campaign is not None:
        entry["campaign"] = campaign

    records, failure, reason = _load_journal(journal)
    if records is None:
        entry["oracle"] = {"verdict": failure, "reasons": [reason], "counts": {}}
    else:
        try:
            entry["oracle"] = q1_oracle.reconcile_safety(records)
        except Exception as error:
            entry["oracle"] = {
                "verdict": q1_oracle.INVALID,
                "reasons": [
                    f"reconcile_safety failed: {type(error).__name__}: {error}"
                ],
                "counts": {},
            }

    if records is not None:
        scenarios = [
            record for record in records if record.get("event") == "scenario_started"
        ]
        if scenario is None:
            entry["scenario"] = (
                scenarios[0].get("scenario") if scenarios else journal.stem
            )
        reached = {
            record.get("barrier")
            for record in records
            if record.get("event") == "barrier_reached"
        }
        required = next(
            (item.get("barrier") for item in scenarios if item.get("barrier")), None
        )
        if required is not None:
            entry["barrier"] = required
            entry["barrier_reached"] = required in reached
        entry["executed_cases"] = len(scenarios)
        entry["process_killed"] = [
            record for record in records if record.get("event") == "process_killed"
        ]
        entry["process_restarted"] = [
            record for record in records if record.get("event") == "process_restarted"
        ]
        # The harness's own closing verdict, kept next to the independent one so
        # that a disagreement between the two is visible instead of hidden.
        conclusion = (
            records[-1] if records and records[-1].get("event") == "conclusion" else None
        )
        if conclusion is not None:
            entry["journal_conclusion"] = {
                "verdict": conclusion.get("verdict"),
                "reason": conclusion.get("reason"),
            }

    # A stored campaign verdict fills in what the journal cannot show, but it
    # never overrides an independently computed barrier observation.
    if stored:
        if entry["barrier"] is None:
            entry["barrier"] = stored.get("barrier")
        if entry["barrier_reached"] is None:
            entry["barrier_reached"] = stored.get("barrier_reached")
        if entry["executed_cases"] == 0:
            entry["executed_cases"] = 1
    return entry


def _bundle_oracle(errors: list[str]) -> dict:
    result: dict = {
        "journal": _repo_relative(BUNDLE_JOURNAL),
        "recomputed": None,
        "stored": None,
        "agrees": None,
    }
    stored = _read_json(BUNDLE_VERDICT, errors)
    result["stored"] = stored
    records, failure, reason = _load_journal(BUNDLE_JOURNAL)
    if records is None:
        result["recomputed"] = {
            "verdict": failure,
            "reasons": [reason],
            "counts": {},
        }
    else:
        try:
            result["recomputed"] = q1_oracle.reconcile_bundle(records)
        except Exception as error:
            result["recomputed"] = {
                "verdict": q1_oracle.INVALID,
                "reasons": [
                    f"reconcile_bundle failed: {type(error).__name__}: {error}"
                ],
                "counts": {},
            }
    if stored is not None and result["recomputed"] is not None:
        result["agrees"] = result["recomputed"].get("verdict") == stored.get("verdict")
    return result


def _index_lines(errors: list[str]) -> list[str]:
    """The sha256sum-format index lines, sorted by path relative to the evidence dir."""
    files: list[Path] = []
    for directory in (SAFETY_DIR, CRASH_DIR, BUNDLE_DIR):
        if not directory.is_dir():
            errors.append(f"missing evidence directory: {_repo_relative(directory)}")
            continue
        for path in directory.rglob("*"):
            if not path.is_file():
                continue
            # The index never lists itself; the manifest changes on every run
            # and is excluded so the index content stays idempotent.
            if path == INDEX_PATH or path.name == "index.sha256":
                continue
            if path == MANIFEST_PATH:
                continue
            files.append(path)
    files.sort(key=lambda item: item.relative_to(EVIDENCE_DIR).as_posix())
    lines: list[str] = []
    for path in files:
        relative = path.relative_to(EVIDENCE_DIR).as_posix()
        try:
            digest = _sha256_hex(path)
        except OSError as error:
            errors.append(f"unreadable evidence file {relative}: {error}")
            print(f"warning: unreadable evidence file {relative}: {error}",
                  file=sys.stderr)
            continue
        lines.append(f"{digest}  {relative}")
    return lines


def main() -> int:
    errors: list[str] = []

    scenarios: list[dict] = []
    for journal in sorted(SAFETY_DIR.glob("*.jsonl")):
        scenarios.append(_scenario_entry(None, "safety-journal", journal, None))

    crash = _read_json(CRASH_VERDICTS, errors)
    if crash is not None:
        verdicts = crash.get("verdicts")
        if not isinstance(verdicts, list):
            errors.append("c02-crash-matrix/verdicts.json has no verdicts list")
            verdicts = []
        campaign = {
            key: crash.get(key)
            for key in ("schema", "executed", "broker_version", "build")
        }
        for index, item in enumerate(verdicts):
            if not isinstance(item, dict):
                errors.append(f"verdicts.json verdict {index} is not an object")
                continue
            scenario = item.get("scenario") or f"crash-verdict-{index}"
            scenarios.append(
                _scenario_entry(
                    scenario,
                    "crash-verdict",
                    SAFETY_DIR / f"{scenario}.jsonl",
                    item,
                    campaign,
                )
            )

    stored_bundle = _read_json(BUNDLE_VERDICT, errors)
    scenarios.append(
        _scenario_entry(
            "c02-bundle-campaign", "bundle-verdict", BUNDLE_JOURNAL, stored_bundle
        )
    )

    manifest = {
        "schema": SCHEMA,
        "generated_utc": _utc_now(),
        "build": _git_build(),
        "machine": _machine(),
        "commands": _campaign_commands(errors),
        "production_commands": _production_commands(),
        "scenarios": scenarios,
        "oracle": _bundle_oracle(errors),
        "errors": errors,
    }

    # Index first (its failures are recorded in the manifest), then write the
    # manifest, then the index: the index excludes both written files.
    index_lines = _index_lines(errors)
    MANIFEST_PATH.parent.mkdir(parents=True, exist_ok=True)
    MANIFEST_PATH.write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    INDEX_PATH.write_text(
        ("\n".join(index_lines) + "\n") if index_lines else "", encoding="utf-8"
    )

    print(f"manifest: {_repo_relative(MANIFEST_PATH)}")
    print(f"index:    {_repo_relative(INDEX_PATH)} ({len(index_lines)} files)")
    print("per-scenario oracle verdicts (q1_oracle.reconcile_safety):")
    for entry in scenarios:
        print(
            f"  {entry['scenario']}: {entry['oracle']['verdict']} "
            f"[source={entry['source']} barrier={entry['barrier']} "
            f"reached={entry['barrier_reached']}]"
        )
    bundle = manifest["oracle"]["recomputed"]
    stored = manifest["oracle"]["stored"]
    print(
        "bundle oracle (q1_oracle.reconcile_bundle): "
        f"{bundle['verdict'] if bundle else 'n/a'} "
        f"(stored verdict: {stored.get('verdict') if stored else 'n/a'})"
    )
    if errors:
        for message in errors:
            print(f"warning: {message}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
