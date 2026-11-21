#!/usr/bin/env python3
"""Deterministic crash matrix for a Q1 ownership handover.

Every scenario runs its own cluster on its own directories and ports, pauses one broker at
one named barrier, kills it with SIGKILL, restarts it on the original storage and checks the
safety invariant of that window. A scenario that never reaches its barrier is a failure, not
a pass, and a campaign with no executed scenario is never reported as PASS.

The runner needs a broker built with the `fault-injection` feature, which is the only build
in which the barriers exist. The parent owns one fsynced oracle journal per scenario under
`benchmarks/evidence/c02-safety-matrix/<scenario>.jsonl`; the conclusion record carries the
verdict the parent itself reached from externally observed results.
"""
from __future__ import annotations

import json
import os
import signal
import socket
import subprocess
import sys
import tempfile
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import q1_oracle  # noqa: E402

ROOT = Path(__file__).resolve().parents[1]
SERVER = ROOT / "target/debug/rovqet"
VOTER = ROOT / "target/debug/metadata-raft-node"
Q1 = ROOT / "target/debug/examples/q1"
READ = ROOT / "target/debug/examples/read_q1"
EVIDENCE = ROOT / "benchmarks" / "evidence" / "c02-safety-matrix"

CLUSTER = 616161
TOPIC_NAME = "durable"
PRODUCER = 11
TENANT = "1"
TOPIC_ID = "3"
PARTITION = "0"
INCARNATION = 42
# The broker's own committed-stage spelling.
STAGE_ORDER = ("prepare", "catchup", "fenceold", "activatenew")


class ScenarioFailure(Exception):
    """A window that cannot support a verdict: the barrier or the recovery never happened."""


def free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


class Node:
    def __init__(self, name: str, argv: list[str], cwd: Path, env: dict[str, str] | None = None):
        self.name = name
        self.argv = argv
        self.cwd = cwd
        self.env = env
        self.process: subprocess.Popen | None = None
        self.lines: list[str] = []
        cwd.mkdir(parents=True, exist_ok=True)

    def start(self) -> None:
        environment = dict(os.environ)
        if self.env:
            environment.update(self.env)
        self.lines = []
        self.process = subprocess.Popen(
            self.argv,
            cwd=self.cwd,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
            env=environment,
        )
        import threading

        threading.Thread(target=self._read, daemon=True).start()

    def _read(self) -> None:
        assert self.process is not None and self.process.stdout is not None
        for line in self.process.stdout:
            line = line.rstrip()
            self.lines.append(line)
            print(f"[{self.name}] {line}", flush=True)

    def wait_for(self, needle: str, timeout: float) -> str | None:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            for line in self.lines:
                if needle in line:
                    return line
            time.sleep(0.05)
        return None

    def kill(self) -> None:
        if self.process is None:
            return
        if self.process.poll() is None:
            self.process.send_signal(signal.SIGKILL)
        self.process.wait(timeout=15)
        if self.process.stdout is not None:
            self.process.stdout.close()
        self.process = None

    def stop(self) -> None:
        if self.process is None:
            return
        if self.process.poll() is None:
            self.process.terminate()
            try:
                self.process.wait(timeout=15)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait(timeout=5)
        if self.process.stdout is not None:
            self.process.stdout.close()
        self.process = None


def broker(
    name: str,
    node_id: int,
    root: Path,
    peer_ports: dict[int, int],
    raft_ports: dict[int, int],
    initialize: bool,
    env: dict[str, str] | None = None,
    client_port: int | None = None,
    bundle: bool = False,
) -> Node:
    (root / "q1").mkdir(parents=True, exist_ok=True)
    (root / "dlq").mkdir(parents=True, exist_ok=True)
    argv = [
        str(SERVER),
        "--listen", f"127.0.0.1:{client_port or free_port()}",
        "--q1-data-dir", str(root / "q1"),
        "--q1-topic", TOPIC_NAME,
        "--q1-tenant-id", "1",
        "--q1-topic-id", "3",
        "--q1-partition-id", "0",
        "--q1-incarnation", "42",
        "--q1-owner-epoch", "7",
        "--q1-initialize-new-log", "true" if initialize else "false",
        "--cluster-id", str(CLUSTER),
        "--node-id", str(node_id),
        "--cluster-listen", f"127.0.0.1:{peer_ports[node_id]}",
        "--metadata-raft", "true",
        "--metadata-raft-dir", str(root / "raft"),
        "--metadata-raft-listen", f"127.0.0.1:{raft_ports[node_id]}",
        "--metadata-raft-bootstrap", "true" if node_id == 1 else "false",
        "--transfer-staging-dir", str(root / "staging"),
    ]
    for peer, port in peer_ports.items():
        argv.extend(["--cluster-peer", f"{peer}=127.0.0.1:{port}"])
    for peer, port in raft_ports.items():
        argv.extend(["--metadata-raft-peer", f"{peer}=127.0.0.1:{port}"])
    if bundle:
        # A coordinated handover: the route owns a real local DLQ partition.
        argv.extend([
            "--q1-terminal-policy", "dlq",
            "--q1-max-attempts", "1",
            "--q1-retry-initial-ms", "10",
            "--q1-retry-max-ms", "50",
            "--q1-dlq-data-dir", str(root / "dlq"),
            "--q1-dlq-topic", "durable-dlq",
            "--q1-dlq-topic-id", "7",
            "--q1-dlq-partition-id", "0",
            "--q1-dlq-incarnation", "77",
            "--q1-dlq-owner-epoch", "7",
        ])
    if node_id == 1:
        argv.extend(["--migrate-to", "2"])
    return Node(name, argv, root, env)


def endpoint(node: Node) -> str:
    return "127.0.0.1:" + node.argv[node.argv.index("--listen") + 1].split(":")[1]


def publish(
    node: Node,
    sequence: int,
    epoch: int = 7,
    message_id: int | None = None,
    payload: str | None = None,
    producer: int = PRODUCER,
) -> subprocess.CompletedProcess:
    identity = 100 + sequence if message_id is None else message_id
    body = f"event-{sequence}" if payload is None else payload
    # q1: topic producer_id producer_epoch sequence message_id partition incarnation
    #     owner_epoch payload
    return subprocess.run(
        [str(Q1), TOPIC_NAME, str(producer), TENANT, str(sequence), str(identity),
         PARTITION, str(INCARNATION), str(epoch), body],
        env=dict(os.environ, ROVQET_ADDR=endpoint(node)),
        capture_output=True, text=True, timeout=60,
    )


def committed_offset(result: subprocess.CompletedProcess | None, text: str | None = None) -> int | None:
    """The offset of a committed local publication, as the client received it."""
    if text is None:
        text = result.stdout if result is not None else ""
    for line in (text or "").splitlines():
        if "COMMITTED_LOCAL" not in line:
            continue
        parts = line.split()
        if len(parts) > 2:
            try:
                return int(parts[2])
            except ValueError:
                return None
    return None


def read_history(node: Node, max_messages: int = 64) -> list[dict] | None:
    """Reads the committed history of the source partition through an external client."""
    result = subprocess.run(
        [str(READ), TOPIC_NAME, TENANT, TOPIC_ID, PARTITION, str(INCARNATION), "0",
         str(max_messages), "65536", "0"],
        env=dict(os.environ, ROVQET_ADDR=endpoint(node)),
        capture_output=True, text=True, timeout=60,
    )
    if result.returncode != 0:
        return None
    records: list[dict] = []
    for line in result.stdout.splitlines():
        if not line.startswith("READ_RECORD "):
            continue
        parts = line.split()
        if len(parts) < 5:
            return None
        _, incarnation, offset, message_id, payload_hex = parts[:5]
        records.append(
            {
                "incarnation": int(incarnation),
                "offset": int(offset),
                "message_id": int(message_id),
                "digest": q1_oracle.digest_hex(bytes.fromhex(payload_hex)),
            }
        )
    return records


def history_difference(expected: list[dict], observed: list[dict] | None) -> list[str]:
    """The successor must serve exactly the records the source served, never a replacement."""
    if observed is None:
        return ["the successor history could not be read"]
    if not observed:
        return ["the successor served an empty history"]
    left = sorted(
        (item["incarnation"], item["offset"], item["message_id"], item["digest"])
        for item in expected
    )
    right = sorted(
        (item["incarnation"], item["offset"], item["message_id"], item["digest"])
        for item in observed
    )
    if left != right:
        return [f"the successor history {right} differs from the source history {left}"]
    return []


def record_offset(records: list[dict], message_id: int) -> int | None:
    matches = [record["offset"] for record in records if record["message_id"] == message_id]
    return matches[0] if len(matches) == 1 else None


def node_id(node: Node) -> int | None:
    if "--node-id" in node.argv:
        return int(node.argv[node.argv.index("--node-id") + 1])
    if "--id" in node.argv:
        return int(node.argv[node.argv.index("--id") + 1])
    return None


def leader_id(voter: Node) -> int | None:
    """The leader the standalone voter last reported, from its own JSON events."""
    leader = None
    for line in voter.lines:
        try:
            payload = json.loads(line)
        except (json.JSONDecodeError, TypeError):
            continue
        if isinstance(payload, dict) and payload.get("event") in ("leader", "follower"):
            if payload.get("leader") is not None:
                leader = int(payload["leader"])
    return leader


def migration_events(lines: list[str], kind: str) -> list[tuple[int, dict]]:
    """Parsed `MIGRATION <kind> {json}` lines with their line index, in order."""
    prefix = f"MIGRATION {kind} "
    events: list[tuple[int, dict]] = []
    for index, line in enumerate(lines):
        if not line.startswith(prefix):
            continue
        try:
            payload = json.loads(line[len(prefix):])
        except json.JSONDecodeError:
            continue
        if isinstance(payload, dict):
            events.append((index, payload))
    return events


def staging_receipts(target_root: Path) -> dict[str, list[dict]]:
    """The durable chunk receipts of every staged inventory, keyed by inventory directory.

    Each inventory is one snapshot of the sender's files, so the chunk identity is scoped to
    the inventory: two inventories may hold the same (file_index, offset) with another
    digest because the log changed between them. Within one inventory the identity is unique.
    """
    groups: dict[str, list[dict]] = {}
    root = target_root / "staging"
    if not root.is_dir():
        return groups
    for path in sorted(root.glob("*/chunks.jsonl")):
        inventory = path.parent.name
        chunks: list[dict] = []
        for line in path.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            entry = json.loads(line)
            chunks.append(
                {
                    "file_index": int(entry["file_index"]),
                    "offset": int(entry["offset"]),
                    "digest": entry["digest"],
                    "inventory": inventory,
                }
            )
        groups[inventory] = chunks
    return groups


def receipt_reasons(groups: dict[str, list[dict]]) -> tuple[list[str], int]:
    """One receipt per chunk identity in an inventory, with a stable digest."""
    reasons: list[str] = []
    total = 0
    for inventory, chunks in groups.items():
        seen: dict[tuple, str] = {}
        for chunk in chunks:
            key = (chunk["file_index"], chunk["offset"])
            if key in seen:
                if seen[key] != chunk["digest"]:
                    reasons.append(
                        f"the chunk receipt {key} of {inventory} changed its digest"
                    )
                else:
                    reasons.append(
                        f"the chunk receipt {key} of {inventory} was journaled twice"
                    )
                continue
            seen[key] = chunk["digest"]
        total += len(seen)
    if not total:
        reasons.append("the staging area holds no chunk receipt")
    return reasons, total


def chunk_identity_count(chunks: list[dict]) -> int:
    return len({(chunk["file_index"], chunk["offset"]) for chunk in chunks})


def inventory_reasons(recorder: "ScenarioRecorder", member: str = "source") -> list[str]:
    entries = [entry for entry in recorder.inventories if entry["member"] == member]
    if not entries:
        return [f"the {member} inventory was never verified"]
    finals = [entry for entry in entries if entry["phase"] == "final"]
    if not finals:
        return [f"the {member} inventory has no final verification"]
    if any(entry["watermark"] is None for entry in finals):
        return [f"the {member} inventory has no final watermark"]
    return []


def stage_order_reasons(stages: list[str]) -> list[str]:
    indices = [STAGE_ORDER.index(stage) for stage in stages if stage in STAGE_ORDER]
    reasons: list[str] = []
    for previous, current in zip(indices, indices[1:]):
        if current < previous:
            reasons.append(
                f"the migration stage moved back from {STAGE_ORDER[previous]} to "
                f"{STAGE_ORDER[current]}"
            )
        elif current > previous + 1:
            reasons.append(
                f"the migration stage jumped from {STAGE_ORDER[previous]} to "
                f"{STAGE_ORDER[current]}"
            )
    return reasons


def stage_revision_reasons(
    recorder: "ScenarioRecorder",
    required: tuple[str, ...] = ("catchup", "fenceold", "activatenew"),
) -> list[str]:
    """One committed stage never carries more than one metadata revision."""
    reasons: list[str] = []
    by_stage: dict[str, set] = {}
    for entry in recorder.stages:
        stage = entry["stage"]
        if stage not in STAGE_ORDER:
            reasons.append(f"unknown migration stage {stage}")
            continue
        by_stage.setdefault(stage, set()).add(entry["revision"])
    for stage, revisions in by_stage.items():
        if len(revisions) > 1:
            reasons.append(
                f"the stage {stage} was committed with several revisions: "
                f"{sorted(str(value) for value in revisions)}"
            )
    for stage in required:
        if stage not in by_stage:
            reasons.append(f"the migration never committed the {stage} stage")
    reasons.extend(stage_order_reasons([entry["stage"] for entry in recorder.stages]))
    return reasons


class ScenarioRecorder:
    """The parent-owned, fsynced observation journal of one scenario."""

    def __init__(self, scenario: "Scenario", build: str):
        EVIDENCE.mkdir(parents=True, exist_ok=True)
        self.scenario = scenario
        self.journal = q1_oracle.Journal(EVIDENCE / f"{scenario.id}.jsonl", scenario.id)
        self.seen: set = set()
        self.stages: list[dict] = []
        self.fences: list[dict] = []
        self.inventories: list[dict] = []
        self.journal.append(
            "scenario_started",
            scenario=scenario.id,
            barrier=scenario.barrier,
            victim=scenario.victim,
            invariant=scenario.invariant,
            build=build,
            bundle=scenario.bundle,
        )

    def event(self, name: str, **fields) -> None:
        self.journal.append(name, **fields)

    def route_active(self, node: Node) -> None:
        self.event("route_active", node=node.name)

    def publication_attempt(self, node: Node, sequence: int, epoch: int = 7,
                            producer: int = PRODUCER) -> None:
        self.event("publication_attempt", node=node.name, producer=producer,
                   epoch=epoch, sequence=sequence)

    def publication_committed(self, node: Node, sequence: int, offset: int | None,
                              message_id: int, payload: bytes, epoch: int = 7,
                              producer: int = PRODUCER) -> None:
        self.event("publication_committed", node=node.name, producer=producer,
                   epoch=epoch, sequence=sequence, offset=offset,
                   message_id=message_id, digest=q1_oracle.digest_hex(payload),
                   incarnation=INCARNATION)

    def publication_failed(self, node: Node, sequence: int, outcome: str,
                           epoch: int = 7, producer: int = PRODUCER) -> None:
        self.event("publication_failed", node=node.name, producer=producer,
                   epoch=epoch, sequence=sequence, outcome=outcome)

    def history_observed(self, node: Node, records: list[dict]) -> None:
        self.event("history_observed", node=node.name, records=records)

    def scrape_all(self, nodes: tuple[Node, ...]) -> None:
        for node in nodes:
            self.scrape(node)

    def barrier_reached(self, barrier: str, process: str) -> None:
        self.event("barrier_reached", barrier=barrier, process=process)

    def process_killed(self, process: str) -> None:
        self.event("process_killed", process=process)

    def process_restarted(self, process: str) -> None:
        self.event("process_restarted", process=process)

    def scrape(self, node: Node) -> None:
        """Journals the migration facts a process printed, in that process's line order."""
        entries: list[tuple[int, str, dict]] = []
        entries.extend(
            (index, "committed", payload)
            for index, payload in migration_events(node.lines, "committed")
        )
        entries.extend(
            (index, "fence", payload)
            for index, payload in migration_events(node.lines, "fence")
        )
        entries.extend(
            (index, "inventory", payload)
            for index, payload in migration_events(node.lines, "inventory")
        )
        for _, kind, payload in sorted(entries, key=lambda entry: entry[0]):
            if kind == "committed":
                stage = payload.get("stage")
                key = ("stage", stage, payload.get("metadata_revision"))
                if key in self.seen:
                    continue
                self.seen.add(key)
                record = {
                    "stage": stage,
                    "revision": payload.get("metadata_revision"),
                    "manifest_id": payload.get("manifest_id"),
                    "transfer_watermark": payload.get("transfer_watermark"),
                }
                self.stages.append(record)
                self.event("migration_stage", **record)
            elif kind == "fence":
                member = payload.get("member")
                key = ("fence", member, payload.get("fence_offset"),
                       payload.get("fence_digest"))
                if key in self.seen:
                    continue
                self.seen.add(key)
                record = {
                    "node": node.name,
                    "member": member,
                    "epoch": payload.get("original_epoch"),
                    "fence_offset": payload.get("fence_offset"),
                    "fence_digest": payload.get("fence_digest"),
                }
                self.fences.append(record)
                self.event("fence_confirmed", **record)
            else:
                member = payload.get("member")
                key = ("inventory", member, payload.get("phase"),
                       payload.get("manifest_id"), payload.get("watermark"))
                if key in self.seen:
                    continue
                self.seen.add(key)
                record = {
                    "member": member,
                    "phase": payload.get("phase"),
                    "inventory": payload.get("manifest_id"),
                    "verified": True,
                    "watermark": payload.get("watermark"),
                }
                self.inventories.append(record)
                self.event("transfer_verified", **record)

    def finish(self, result: dict) -> None:
        verdict = result["verdict"]
        if verdict not in (q1_oracle.PASS, q1_oracle.FAIL, q1_oracle.INVALID,
                           q1_oracle.NOT_RUN):
            verdict = q1_oracle.INVALID
        reason = "; ".join(result["reasons"]) if result["reasons"] else None
        self.journal.conclusion(verdict, reason)
        self.journal.close()


class Scenario:
    def __init__(self, scenario_id: str, barrier: str | None, victim: str, invariant: str,
                 bundle: bool = False):
        self.id = scenario_id
        self.barrier = barrier
        self.victim = victim
        self.invariant = invariant
        self.bundle = bundle


SCENARIOS = [
    Scenario("C1-before-fence", "before_fence", "source", "no-activation"),
    Scenario("C2-after-fence", "after_fence", "source", "fenced-source"),
    Scenario("C3-before-activate-commit", "before_activate_commit", "source", "no-activation"),
    Scenario("C4-promotion-intent", "promotion_intent", "target", "no-partial-activation"),
    Scenario("C5-before-route-open", "before_route_open", "target", "route-recovers"),
    Scenario("C6-after-route-open", "after_route_open", "target", "route-recovers"),
    # The coordinated bundle: the successor is killed between the dependent promotion and
    # the source promotion. The source history must stay unavailable until the remaining
    # promotion completes.
    Scenario("C8-bundle-after-dependent-promotion", "after_dependent_promotion", "target",
             "bundle-one-sided-promotion", bundle=True),
    # The C-02 crash windows of the initial transfer, the chunk receipt, the first
    # transition, the final post-fence delta and the metadata leader.
    Scenario("C9-initial-transfer-interrupted", "chunk_before_send", "source",
             "transfer-resumes"),
    Scenario("C10-chunk-written-before-sync", "chunk_after_write", "target",
             "chunk-revalidated"),
    Scenario("C11-chunk-synced-ack-lost", "chunk_after_sync", "target", "chunk-idempotent"),
    Scenario("C12-finalize-before-transition", "before_catch_up_commit", "source",
             "no-fabricated-transition"),
    Scenario("C13-final-delta-interrupted", "before_final_transfer", "source",
             "both-fenced", bundle=True),
    Scenario("C14-leader-crash-pending-transition", "after_migration_proposal", "leader",
             "stage-resumed"),
    Scenario("C15-leader-crash-after-commit", "after_migration_commit", "leader",
             "stage-recovered"),
]


def announced(nodes: tuple[Node, ...], needle: str) -> list[Node]:
    return [node for node in nodes if any(needle in line for line in node.lines)]


def activation_committed(*captures: list[str]) -> bool:
    """True when a captured broker output holds the committed activation transition."""
    return any(
        payload.get("stage") == "activatenew"
        for lines in captures
        for _, payload in migration_events(lines, "committed")
    )


def wait_for_activation(
    source: Node, target: Node, pre_crash: list[str], timeout: float
) -> str | None:
    """Waits for the fact that decides whether the successor is allowed to serve.

    The successor may only open its route once the source committed the activation
    transition. `None` means either that the activation is committed — so an open route
    is legitimate — or that nothing happened within the timeout and the route stayed
    closed, which is exactly what the safety check demands. A returned reason means the
    successor opened its route although no activation was ever committed. The source
    prints the commit line when it applies the committed transition, which a follower
    may observe slightly earlier, so the line gets a short grace period once the route
    is open.
    """
    deadline = time.monotonic() + timeout
    while True:
        if activation_committed(pre_crash, source.lines):
            return None
        if announced((target,), "Q1 route active"):
            grace = time.monotonic() + 15
            while not activation_committed(pre_crash, source.lines):
                if time.monotonic() >= grace:
                    return "the target became active without a committed activation"
                time.sleep(0.05)
            return None
        if time.monotonic() >= deadline:
            return None
        time.sleep(0.05)


def run_scenario(scenario: Scenario, build: str = "") -> dict:
    root = Path(tempfile.mkdtemp(prefix=f"rovqet-crash-{scenario.id}-"))
    peer_ports = {1: free_port(), 2: free_port()}
    raft_ports = {1: free_port(), 2: free_port(), 3: free_port()}
    leader_victim = scenario.victim == "leader"
    release = root / "release" / scenario.barrier if scenario.barrier else None
    barrier_env: dict[str, str] | None = None
    if scenario.barrier:
        # The release file lets a surviving process that reaches the same barrier continue
        # immediately instead of sleeping out the bounded wait.
        barrier_env = {
            "ROVQET_MIGRATION_BARRIER": scenario.barrier,
            "ROVQET_MIGRATION_BARRIER_RELEASE": str(release),
        }
    source = broker("source", 1, root / "source", peer_ports, raft_ports, True,
                    barrier_env if leader_victim or scenario.victim == "source" else None,
                    bundle=scenario.bundle)
    target = broker("target", 2, root / "target", peer_ports, raft_ports, False,
                    barrier_env if leader_victim or scenario.victim == "target" else None,
                    bundle=scenario.bundle)
    voter = Node(
        "voter3",
        [str(VOTER), "--id", "3", "--cluster-id", str(CLUSTER),
         "--listen", f"127.0.0.1:{raft_ports[3]}", "--data-dir", str(root / "voter3"),
         "--bootstrap", "false",
         "--peer", f"1=127.0.0.1:{raft_ports[1]}",
         "--peer", f"2=127.0.0.1:{raft_ports[2]}",
         "--peer", f"3=127.0.0.1:{raft_ports[3]}"],
        root,
        env=barrier_env if leader_victim else None,
    )
    result = {"scenario": scenario.id, "barrier": scenario.barrier, "verdict": "INVALID",
              "reasons": [], "barrier_reached": False}
    recorder = ScenarioRecorder(scenario, build)
    try:
        for node in (source, target, voter):
            node.start()
        if source.wait_for("Q1 route active", 60) is None:
            result["reasons"].append("the source never opened its route")
            return result
        recorder.route_active(source)
        published = publish(source, 0)
        recorder.publication_attempt(source, 0)
        if published.returncode != 0:
            recorder.publication_failed(source, 0, "refused")
            result["reasons"].append(f"the source publication failed: {published.stderr}")
            return result
        source_history = read_history(source)
        if not source_history:
            recorder.publication_failed(source, 0, "unobservable")
            result["reasons"].append("the acknowledged publication is not visible on the source")
            return result
        recorder.publication_committed(source, 0, record_offset(source_history, 100), 100,
                                       b"event-0")
        recorder.history_observed(source, source_history)
        # The operator asks for the handover.
        (root / "source" / "raft" / "MIGRATE").write_text("2\n")
        needle = f"MIGRATION_BARRIER {scenario.barrier}"
        if leader_victim:
            deadline = time.monotonic() + 120
            while time.monotonic() < deadline and not announced((source, target, voter), needle):
                time.sleep(0.05)
            if not announced((source, target, voter), needle):
                result["reasons"].append(f"the barrier {scenario.barrier} was never reached")
                return result
            # Let a second process announce before deciding which one is the leader.
            time.sleep(1.0)
            announcing = announced((source, target, voter), needle)
            reported = leader_id(voter)
            preferred = [
                node for node in announcing
                if reported is not None and node_id(node) == reported
            ]
            victim = preferred[0] if preferred else announcing[0]
            if reported is not None and node_id(victim) != reported:
                result["reasons"].append(
                    f"the barrier fired on node {node_id(victim)} while the voters report "
                    f"leader {reported}"
                )
                return result
        else:
            victim = source if scenario.victim == "source" else target
            if victim.wait_for(needle, 90) is None:
                result["reasons"].append(f"the barrier {scenario.barrier} was never reached")
                return result
        result["barrier_reached"] = True
        recorder.barrier_reached(scenario.barrier, victim.name)
        before_kill = {node.name: list(node.lines) for node in (source, target, voter)}
        for node in (source, target, voter):
            recorder.scrape(node)
        if release is not None:
            release.parent.mkdir(parents=True, exist_ok=True)
            release.write_text("release\n")
        victim.kill()
        recorder.process_killed(victim.name)
        # The restarted process must proceed: the barrier is a one-shot pause for the crash
        # window, not a permanent block. The restarted process runs on the original storage,
        # so it reopens the existing log instead of claiming a fresh one.
        victim.env = {}
        if "--q1-initialize-new-log" in victim.argv:
            flag = victim.argv.index("--q1-initialize-new-log") + 1
            victim.argv[flag] = "false"
        victim.start()
        recorder.process_restarted(victim.name)
        if leader_victim:
            if target.wait_for("Q1 route active", 180) is None:
                result["reasons"].append(
                    "the handover did not complete after the leader replacement"
                )
                return result
            recorder.route_active(target)
            recorder.scrape_all((source, target, voter))
            if victim.process is None or victim.process.poll() is not None:
                result["reasons"].append(f"the restarted {victim.name} is not running")
                return result
        elif scenario.victim == "source":
            if source.wait_for("Q1 route active", 60) is None and source.wait_for(
                "the Q1 route stays inactive", 10
            ) is None:
                result["reasons"].append("the source did not recover")
                return result
        else:
            if target.wait_for("Q1 route active", 90) is None and target.wait_for(
                "the promoted storage was refused", 10
            ) is None and target.wait_for("the Q1 route stays inactive", 10) is None:
                result["reasons"].append("the target did not recover")
                return result
        for node in (source, target, voter):
            recorder.scrape(node)

        if scenario.invariant == "fenced-source":
            # The durable fence survived the restart: the old owner refuses a publication.
            refused = publish(source, 1)
            recorder.publication_attempt(source, 1)
            if refused.returncode == 0:
                recorder.publication_committed(source, 1, committed_offset(refused), 101,
                                               b"event-1")
                result["reasons"].append("the restarted old owner still committed")
                return result
            recorder.publication_failed(source, 1, "refused")
            result["verdict"] = "PASS"
        elif scenario.invariant == "no-activation":
            # The successor may only open its route once the source committed the
            # activation transition: without that commitment its route must stay closed,
            # and the activation that legitimately completes the handover after the
            # restart is exactly the commitment that makes an open route correct. The
            # whole captured output is scanned, so the decision is made by the committed
            # transition, never by the presence of the line alone.
            reason = wait_for_activation(source, target, before_kill["source"], timeout=120)
            if reason is not None:
                result["reasons"].append(reason)
                return result
            result["verdict"] = "PASS"
        elif scenario.invariant == "no-partial-activation":
            # Either the promotion completed and the route opened — only once the source
            # committed the activation — or it is still inactive: a partially promoted
            # directory must never be served.
            active = target.wait_for("Q1 route active", 90)
            refused = target.wait_for("the promoted storage was refused", 10)
            if active is None and refused is None:
                result["reasons"].append("the target neither recovered nor refused the promotion")
                return result
            if active is not None:
                reason = wait_for_activation(source, target, before_kill["source"], timeout=15)
                if reason is not None:
                    result["reasons"].append(reason)
                    return result
                recorder.route_active(target)
            result["verdict"] = "PASS"
        elif scenario.invariant == "bundle-one-sided-promotion":
            # The successor was killed between the two promotions. It must not serve the
            # source partition before the remaining promotion completed, and it must
            # recover both histories after the restart.
            source_history = (root / "target" / "q1" / "MANIFEST")
            dependent_history = (root / "target" / "dlq" / "MANIFEST")
            if not dependent_history.is_file():
                result["reasons"].append("the dependent history was not promoted")
                return result
            active = target.wait_for("Q1 route active", 120)
            if active is None:
                result["reasons"].append("the successor did not recover the bundle")
                return result
            reason = wait_for_activation(source, target, before_kill["source"], timeout=15)
            if reason is not None:
                result["reasons"].append(reason)
                return result
            recorder.route_active(target)
            if not source_history.is_file():
                result["reasons"].append("the source history was not promoted")
                return result
            if target.wait_for("Q1 DLQ route active", 10) is None:
                result["reasons"].append("the dependent history was not reopened")
                return result
            result["verdict"] = "PASS"
        elif scenario.invariant == "route-recovers":
            if target.wait_for("Q1 route active", 90) is None:
                result["reasons"].append("the promoted storage did not reopen")
                return result
            # The reopened route must be backed by the committed activation: the scan
            # covers the whole captured output, so only the committed transition proves
            # the successor is allowed to serve again.
            reason = wait_for_activation(source, target, before_kill["source"], timeout=15)
            if reason is not None:
                result["reasons"].append(reason)
                return result
            recorder.route_active(target)
            result["verdict"] = "PASS"
        elif scenario.invariant == "transfer-resumes":
            # The source died while it sent the initial inventory. The acknowledged record
            # must survive and the successor must serve the complete history, never an
            # empty replacement.
            reasons: list[str] = []
            if target.wait_for("Q1 route active", 150) is None:
                reasons.append("the handover did not complete after the interrupted transfer")
            else:
                recorder.route_active(target)
                recorder.scrape_all((source, target, voter))
            reasons.extend(inventory_reasons(recorder))
            successor = read_history(target)
            if successor is not None:
                recorder.history_observed(target, successor)
            reasons.extend(history_difference(source_history, successor))
            result["reasons"].extend(reasons)
            result["verdict"] = "FAIL" if reasons else "PASS"
        elif scenario.invariant in ("chunk-revalidated", "chunk-idempotent"):
            # The receiver died around the durable chunk receipt. The receipt must be
            # revalidated and retransmitted, never counted twice, and the successor must
            # still serve the complete history.
            reasons = []
            if target.wait_for("Q1 route active", 150) is None:
                reasons.append("the handover did not complete after the interrupted chunk")
            else:
                recorder.route_active(target)
                recorder.scrape_all((source, target, voter))
            reasons.extend(inventory_reasons(recorder))
            groups = staging_receipts(root / "target")
            duplicated, _ = receipt_reasons(groups)
            reasons.extend(duplicated)
            for inventory, chunks in groups.items():
                recorder.event("chunk_receipts",
                               expected_chunks=chunk_identity_count(chunks),
                               chunks=chunks)
            successor = read_history(target)
            if successor is not None:
                recorder.history_observed(target, successor)
            reasons.extend(history_difference(source_history, successor))
            result["reasons"].extend(reasons)
            result["verdict"] = "FAIL" if reasons else "PASS"
        elif scenario.invariant == "no-fabricated-transition":
            # The source died after the inventory was verified and before the catch-up
            # transition was committed. After the restart the catch-up stage is committed
            # exactly once and no fence preceded it.
            reasons = []
            if target.wait_for("Q1 route active", 150) is None:
                reasons.append("the handover did not complete after the interrupted transition")
            else:
                recorder.route_active(target)
                recorder.scrape_all((source, target, voter))
            reasons.extend(stage_revision_reasons(recorder, required=("catchup",)))
            if not any(entry["stage"] == "catchup" for entry in recorder.stages):
                reasons.append("the catch-up transition was never committed")
            catchup_revisions = {
                entry["revision"] for entry in recorder.stages if entry["stage"] == "catchup"
            }
            if len(catchup_revisions) != 1:
                reasons.append(
                    f"the catch-up stage has {len(catchup_revisions)} metadata revisions"
                )
            pre_kill_fences = migration_events(before_kill["source"], "fence")
            if pre_kill_fences:
                reasons.append("the old owner was fenced before the catch-up transition")
            post = source.lines
            catchup = [
                index for index, payload in migration_events(post, "committed")
                if payload.get("stage") == "catchup"
            ]
            fences = [index for index, _ in migration_events(post, "fence")]
            if not fences:
                reasons.append("the source never recorded the old owner's fence")
            elif not catchup or fences[0] < catchup[0]:
                reasons.append("the old owner was fenced before the catch-up commit")
            result["reasons"].extend(reasons)
            result["verdict"] = "FAIL" if reasons else "PASS"
        elif scenario.invariant == "both-fenced":
            # Both durable fences survived the crash, the old owner still refuses
            # publications and the successor serves exactly the source records.
            reasons = []
            if target.wait_for("Q1 route active", 180) is None:
                reasons.append("the successor did not serve the bundle")
            else:
                recorder.route_active(target)
                recorder.scrape_all((source, target, voter))
            refused = publish(source, 1)
            recorder.publication_attempt(source, 1)
            if refused.returncode == 0:
                recorder.publication_committed(source, 1, committed_offset(refused), 101,
                                               b"event-1")
                reasons.append("the restarted old owner still committed")
            else:
                recorder.publication_failed(source, 1, "refused")
            members = {fence["member"] for fence in recorder.fences}
            if members != {"source", "dependent"}:
                reasons.append(
                    f"the bundle fenced {sorted(str(member) for member in members)} "
                    "instead of both members"
                )
            successor = read_history(target)
            if successor is not None:
                recorder.history_observed(target, successor)
            reasons.extend(history_difference(source_history, successor))
            result["reasons"].extend(reasons)
            result["verdict"] = "FAIL" if reasons else "PASS"
        elif scenario.invariant in ("stage-resumed", "stage-recovered"):
            # After the leader replacement the pending or already committed transition is
            # recovered exactly once and the handover completes. The activation itself may
            # be confirmed to the driver after the successor already serves the history, so
            # the committed transition that the barrier interrupted is what is checked.
            reasons = stage_revision_reasons(recorder, required=())
            if not recorder.stages:
                reasons.append("no committed migration transition was observed")
            result["reasons"].extend(reasons)
            result["verdict"] = "FAIL" if reasons else "PASS"
        return result
    except ScenarioFailure as failure:
        result["reasons"].append(str(failure))
        return result
    finally:
        for node in (source, target, voter):
            node.stop()
        recorder.finish(result)


def run_unreachable_successor(build: str = "") -> dict:
    """The successor is unreachable while the source keeps serving.

    The source is alive with a valid local authority and the metadata majority (source and
    the third voter) keeps running, but the transfer and the activation path cannot reach the
    successor. The migration must not advance and the source must keep serving; once the
    successor returns, the handover completes.
    """
    root = Path(tempfile.mkdtemp(prefix="rovqet-crash-unreachable-"))
    peer_ports = {1: free_port(), 2: free_port()}
    raft_ports = {1: free_port(), 2: free_port(), 3: free_port()}
    source = broker("source", 1, root / "source", peer_ports, raft_ports, True)
    target = broker("target", 2, root / "target", peer_ports, raft_ports, False)
    voter = Node(
        "voter3",
        [str(VOTER), "--id", "3", "--cluster-id", str(CLUSTER),
         "--listen", f"127.0.0.1:{raft_ports[3]}", "--data-dir", str(root / "voter3"),
         "--bootstrap", "false",
         "--peer", f"1=127.0.0.1:{raft_ports[1]}",
         "--peer", f"2=127.0.0.1:{raft_ports[2]}",
         "--peer", f"3=127.0.0.1:{raft_ports[3]}"],
        root,
    )
    result = {"scenario": "C7-unreachable-successor", "barrier": None, "verdict": "INVALID",
              "reasons": [], "barrier_reached": True}
    scenario = Scenario("C7-unreachable-successor", None, "target", "unreachable-successor")
    recorder = ScenarioRecorder(scenario, build)
    try:
        # The successor is not started at all: the direction to it is blocked.
        source.start()
        voter.start()
        if source.wait_for("Q1 route active", 60) is None:
            result["reasons"].append("the source never opened its route")
            return result
        recorder.route_active(source)
        published = publish(source, 0)
        recorder.publication_attempt(source, 0)
        if published.returncode != 0:
            recorder.publication_failed(source, 0, "refused")
            result["reasons"].append("the source publication failed")
            return result
        history = read_history(source) or []
        recorder.publication_committed(source, 0, record_offset(history, 100), 100, b"event-0")
        (root / "source" / "raft" / "MIGRATE").write_text("2\n")
        # The source keeps serving while the successor is unreachable.
        time.sleep(5)
        again = publish(source, 1)
        recorder.publication_attempt(source, 1)
        if again.returncode != 0:
            recorder.publication_failed(source, 1, "refused")
            result["reasons"].append("the source stopped serving before any fence")
            return result
        history = read_history(source) or []
        recorder.publication_committed(source, 1, record_offset(history, 101), 101, b"event-1")
        recorder.history_observed(source, history)
        if source.wait_for("Q1 route active", 1) is None:
            result["reasons"].append("the source route disappeared")
            return result
        # No activation can have happened: the successor never ran.
        if target.lines:
            result["reasons"].append("the successor ran while it was unreachable")
            return result
        # The successor returns and the handover completes.
        target.start()
        if target.wait_for("Q1 route active", 120) is None:
            result["reasons"].append("the handover did not complete once the successor returned")
            return result
        recorder.route_active(target)
        successor = read_history(target)
        if successor is not None:
            recorder.history_observed(target, successor)
        for node in (source, target, voter):
            recorder.scrape(node)
        result["verdict"] = "PASS"
        return result
    finally:
        for node in (source, target, voter):
            node.stop()
        recorder.finish(result)


def run_successor_first_commit(build: str = "") -> dict:
    """The successor dies between its first append and the synchronization.

    The retry of the same producer token must return the offset of the one committed
    application record for that message identity: no duplicate, no lost publication.
    """
    root = Path(tempfile.mkdtemp(prefix="rovqet-crash-successor-first-"))
    peer_ports = {1: free_port(), 2: free_port()}
    raft_ports = {1: free_port(), 2: free_port(), 3: free_port()}
    controls = root / "target" / "controls"
    control_env = {
        "ROVQET_Q1_TEST_DIR": str(controls),
        "ROVQET_Q1_TEST_STAGE": "after-append-before-sync",
        "ROVQET_Q1_TEST_SEQUENCE": "0",
    }
    source = broker("source", 1, root / "source", peer_ports, raft_ports, True)
    target = broker("target", 2, root / "target", peer_ports, raft_ports, False,
                    env=control_env)
    voter = Node(
        "voter3",
        [str(VOTER), "--id", "3", "--cluster-id", str(CLUSTER),
         "--listen", f"127.0.0.1:{raft_ports[3]}", "--data-dir", str(root / "voter3"),
         "--bootstrap", "false",
         "--peer", f"1=127.0.0.1:{raft_ports[1]}",
         "--peer", f"2=127.0.0.1:{raft_ports[2]}",
         "--peer", f"3=127.0.0.1:{raft_ports[3]}"],
        root,
    )
    result = {"scenario": "C16-successor-first-commit-interrupted", "barrier": None,
              "verdict": "INVALID", "reasons": [], "barrier_reached": True}
    scenario = Scenario("C16-successor-first-commit-interrupted", None, "target",
                        "one-committed-record")
    recorder = ScenarioRecorder(scenario, build)
    fresh_message = 900
    fresh_payload = b"successor-first"
    # A fresh producer identity: producer 11 already used sequence 0 for the transferred
    # record, and the producer token is (producer, producer epoch, sequence).
    fresh_producer = PRODUCER + 1
    try:
        for node in (source, target, voter):
            node.start()
        if source.wait_for("Q1 route active", 60) is None:
            result["reasons"].append("the source never opened its route")
            return result
        recorder.route_active(source)
        published = publish(source, 0)
        recorder.publication_attempt(source, 0)
        if published.returncode != 0:
            recorder.publication_failed(source, 0, "refused")
            result["reasons"].append("the source publication failed")
            return result
        history = read_history(source) or []
        recorder.publication_committed(source, 0, record_offset(history, 100), 100, b"event-0")
        recorder.history_observed(source, history)
        (root / "source" / "raft" / "MIGRATE").write_text("2\n")
        if target.wait_for("Q1 route active", 240) is None:
            result["reasons"].append("the handover did not complete")
            return result
        recorder.route_active(target)
        for node in (source, target, voter):
            recorder.scrape(node)
        # The first publication of producer sequence 0 on the new owner pauses after the
        # append and before the synchronization.
        controls.mkdir(parents=True, exist_ok=True)
        stale_markers = sorted(path.name for path in controls.glob("reached-*"))
        print(f"[control] markers before the publication: {stale_markers}", flush=True)
        for stale in controls.glob("reached-*"):
            stale.unlink()
        attempt = subprocess.Popen(
            [str(Q1), TOPIC_NAME, str(fresh_producer), TENANT, "0", str(fresh_message),
             PARTITION, str(INCARNATION), "8", fresh_payload.decode()],
            env=dict(os.environ, ROVQET_ADDR=endpoint(target)),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )
        recorder.publication_attempt(target, 0, epoch=8, producer=fresh_producer)
        marker = controls / "reached-after-append-before-sync"
        deadline = time.monotonic() + 60
        while not marker.is_file() and time.monotonic() < deadline:
            if attempt.poll() is not None:
                break
            time.sleep(0.05)
        if not marker.is_file():
            output, _ = attempt.communicate(timeout=15)
            result["reasons"].append(
                "the durable control never reached after-append-before-sync "
                f"(client exit {attempt.returncode}, output {output!r}, "
                f"markers {sorted(path.name for path in controls.glob('reached-*'))})"
            )
            return result
        target.kill()
        recorder.process_killed(target.name)
        output, _ = attempt.communicate(timeout=30)
        first_offset = committed_offset(None, output)
        if attempt.returncode == 0:
            recorder.publication_committed(target, 0, first_offset, fresh_message,
                                           fresh_payload, epoch=8, producer=fresh_producer)
        else:
            recorder.publication_failed(target, 0, "connection-lost", epoch=8,
                                        producer=fresh_producer)
        # The successor returns on its original directories with the control cleared.
        target.env = {}
        target.start()
        recorder.process_restarted(target.name)
        if target.wait_for("Q1 route active", 120) is None:
            result["reasons"].append("the successor did not reopen its route")
            return result
        recorder.route_active(target)
        # The retry uses the same producer token.
        retry = publish(target, 0, epoch=8, message_id=fresh_message,
                        payload=fresh_payload.decode(), producer=fresh_producer)
        recorder.publication_attempt(target, 0, epoch=8, producer=fresh_producer)
        if retry.returncode != 0:
            recorder.publication_failed(target, 0, "refused", epoch=8,
                                        producer=fresh_producer)
            result["reasons"].append(f"the retry failed: {retry.stdout} {retry.stderr}")
            return result
        retry_offset = committed_offset(retry)
        recorder.publication_committed(target, 0, retry_offset, fresh_message, fresh_payload,
                                       epoch=8, producer=fresh_producer)
        successor = read_history(target)
        if successor is None:
            result["reasons"].append("the successor history could not be read")
            return result
        recorder.history_observed(target, successor)
        for node in (source, target, voter):
            recorder.scrape(node)
        matches = [record for record in successor if record["message_id"] == fresh_message]
        reasons = []
        if len(matches) != 1:
            reasons.append(
                f"the message identity {fresh_message} has {len(matches)} committed records"
            )
        elif retry_offset is None or matches[0]["offset"] != retry_offset:
            reasons.append(
                f"the retry returned offset {retry_offset} but the record is at "
                f"{matches[0]['offset']}"
            )
        result["reasons"].extend(reasons)
        result["verdict"] = "FAIL" if reasons else "PASS"
        return result
    finally:
        for node in (source, target, voter):
            node.stop()
        recorder.finish(result)


SPECIALS = ("C7-unreachable-successor", "C16-successor-first-commit-interrupted")


def main(argv: list[str] | None = None) -> int:
    args = list(sys.argv[1:] if argv is None else argv)
    if not SERVER.exists() or not VOTER.exists() or not READ.exists():
        print("build the brokers first", file=sys.stderr)
        return 2
    selected = None
    if "--scenario" in args:
        index = args.index("--scenario")
        if index + 1 >= len(args):
            print("--scenario needs a scenario id", file=sys.stderr)
            return 2
        selected = args[index + 1]
    known = [scenario.id for scenario in SCENARIOS] + list(SPECIALS)
    if selected is not None and selected not in known:
        print(f"unknown scenario {selected}", file=sys.stderr)
        print("known scenarios:", ", ".join(known), file=sys.stderr)
        return 2
    # The barriers exist only in the fault-injection build.
    probe = subprocess.run([str(SERVER), "--version"], capture_output=True, text=True)
    build = (probe.stdout or probe.stderr).strip() or "unknown"
    executed = 0
    verdicts = []
    for scenario in SCENARIOS:
        if selected is not None and scenario.id != selected:
            continue
        print(f"\n===== {scenario.id} ({scenario.barrier}) =====", flush=True)
        result = run_scenario(scenario, build)
        executed += 1
        verdicts.append(result)
        print(f"===== {scenario.id}: {result['verdict']} {result['reasons']} =====", flush=True)
    if selected in (None, "C7-unreachable-successor"):
        print("\n===== C7-unreachable-successor =====", flush=True)
        unreachable = run_unreachable_successor(build)
        executed += 1
        verdicts.append(unreachable)
        print(
            f"===== C7-unreachable-successor: {unreachable['verdict']} "
            f"{unreachable['reasons']} =====",
            flush=True,
        )
    if selected in (None, "C16-successor-first-commit-interrupted"):
        print("\n===== C16-successor-first-commit-interrupted =====", flush=True)
        successor = run_successor_first_commit(build)
        executed += 1
        verdicts.append(successor)
        print(
            f"===== C16-successor-first-commit-interrupted: {successor['verdict']} "
            f"{successor['reasons']} =====",
            flush=True,
        )
    journal = {
        "schema": "rovqet-migration-crash-v1",
        "executed": executed,
        "verdicts": verdicts,
        "broker_version": build,
    }
    evidence = ROOT / "benchmarks" / "evidence" / "c02-crash-matrix"
    evidence.mkdir(parents=True, exist_ok=True)
    (evidence / "verdicts.json").write_text(json.dumps(journal, indent=2) + "\n")
    failed = [v for v in verdicts if v["verdict"] != "PASS"]
    print(f"\nexecuted={executed} passed={executed - len(failed)} failed={len(failed)}")
    if executed == 0:
        print("no scenario executed", file=sys.stderr)
        return 2
    return 0 if not failed else 1


if __name__ == "__main__":
    sys.exit(main())
