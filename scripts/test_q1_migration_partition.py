#!/usr/bin/env python3
"""Live asymmetric isolation of a Q1 owner (C17-live-isolation).

Three metadata voters run: the source broker (node 1) is the old owner, the target broker
(node 2) is the successor and node 3 is a standalone voter. The parent blocks every OUTGOING
connection of the source towards ``2,3`` with ``ROVQET_FAULT_BLOCK_FILE`` while the source
process keeps running and keeps serving its own client port. In that window:

  * the live old owner still commits legitimate pre-fence writes,
  * the metadata majority (nodes 2 and 3) keeps committing new revisions independently,
  * the successor never activates and never serves a client,
  * the metadata group refuses to treat an absent fence response as a durable fence.

Deleting the block file restores the path; the handover then completes, the successor serves
exactly the old owner's history and the old owner refuses further mutations. Every claim is
written to an fsynced parent journal and reconciled afterwards by ``q1_oracle.reconcile_safety``.

The directional fault exists only in the ``fault-injection`` build.
"""
from __future__ import annotations

import hashlib
import json
import os
import socket
import subprocess
import sys
import tempfile
import threading
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import q1_oracle  # noqa: E402

ROOT = Path(__file__).resolve().parents[1]
SERVER = ROOT / "target/debug/rovqet"
VOTER_BIN = ROOT / "target/debug/metadata-raft-node"
Q1 = ROOT / "target/debug/examples/q1"
READ_Q1 = ROOT / "target/debug/examples/read_q1"
EVIDENCE = ROOT / "benchmarks" / "evidence" / "c02-safety-matrix" / "C17-live-isolation.jsonl"

CLUSTER = 616161
TOPIC = "durable"
TENANT = 1
TOPIC_ID = 3
PARTITION = 0
INCARNATION = 42
OWNER_EPOCH = 7
PRODUCER = 11
FOREIGN_NODE = 9

# The broker prints the stage without underscores; the oracle uses the canonical names.
STAGE_NAMES = {
    "prepare": "prepare",
    "catchup": "catch_up",
    "fenceold": "fence_old",
    "activatenew": "activate_new",
    "cleanup": "cleanup",
}


class Failure(Exception):
    """A step of the scenario could not be demonstrated."""


def normalize_stage(raw: object) -> str:
    key = str(raw).lower().replace("_", "").replace("-", "")
    return STAGE_NAMES.get(key, str(raw))


def free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def build_identity() -> str:
    head = subprocess.run(
        ["git", "-C", str(ROOT), "rev-parse", "HEAD"], capture_output=True, text=True
    ).stdout.strip()
    digest = hashlib.sha256(SERVER.read_bytes()).hexdigest()[:16]
    return f"{head}+{digest}"


class Node:
    def __init__(self, name: str, argv: list[str], cwd: Path,
                 env: dict[str, str] | None = None):
        self.name = name
        self.argv = argv
        self.cwd = cwd
        self.env = env
        self.process: subprocess.Popen | None = None
        self.lines: list[str] = []
        self.replies: list[str] = []
        cwd.mkdir(parents=True, exist_ok=True)

    def start(self) -> None:
        environment = dict(os.environ)
        if self.env:
            environment.update(self.env)
        self.lines = []
        self.replies = []
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
        threading.Thread(target=self._read, daemon=True).start()

    def _read(self) -> None:
        assert self.process is not None and self.process.stdout is not None
        for line in self.process.stdout:
            line = line.rstrip()
            self.lines.append(line)
            print(f"[{self.name}] {line}", flush=True)
            if '"proposed"' in line:
                self.replies.append(line)

    def alive(self) -> bool:
        return self.process is not None and self.process.poll() is None

    def wait_for(self, needle: str, timeout: float) -> str | None:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            for line in list(self.lines):
                if needle in line:
                    return line
            time.sleep(0.05)
        return None

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


class Voter(Node):
    """The standalone metadata voter; one command per stdin line, one reply line."""

    def propose(self, command: dict, timeout: float = 60.0) -> dict:
        if self.process is None or self.process.stdin is None:
            raise Failure("the standalone voter is not running")
        before = len(self.replies)
        self.process.stdin.write(json.dumps(command) + "\n")
        self.process.stdin.flush()
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if len(self.replies) > before:
                return json.loads(self.replies[before])
            time.sleep(0.05)
        raise Failure(f"the voter did not answer within {timeout}s: {json.dumps(command)}")


def broker(name: str, node_id: int, root: Path, peer_ports: dict[int, int],
           raft_ports: dict[int, int], client_port: int, initialize: bool,
           env: dict[str, str] | None = None) -> Node:
    (root / "q1").mkdir(parents=True, exist_ok=True)
    argv = [
        str(SERVER),
        "--listen", f"127.0.0.1:{client_port}",
        "--q1-data-dir", str(root / "q1"),
        "--q1-topic", TOPIC,
        "--q1-tenant-id", str(TENANT),
        "--q1-topic-id", str(TOPIC_ID),
        "--q1-partition-id", str(PARTITION),
        "--q1-incarnation", str(INCARNATION),
        "--q1-owner-epoch", str(OWNER_EPOCH),
        "--q1-initialize-new-log", "true" if initialize else "false",
        "--cluster-id", str(CLUSTER),
        "--node-id", str(node_id),
        "--cluster-listen", f"127.0.0.1:{peer_ports[node_id]}",
        "--metadata-raft", "true",
        "--metadata-raft-dir", str(root / "raft"),
        "--metadata-raft-listen", f"127.0.0.1:{raft_ports[node_id]}",
        "--metadata-raft-bootstrap", "true" if initialize else "false",
        "--transfer-staging-dir", str(root / "staging"),
    ]
    for peer, port in peer_ports.items():
        argv.extend(["--cluster-peer", f"{peer}=127.0.0.1:{port}"])
    for peer, port in raft_ports.items():
        argv.extend(["--metadata-raft-peer", f"{peer}=127.0.0.1:{port}"])
    if node_id == 1:
        argv.extend(["--migrate-to", "2"])
    return Node(name, argv, root, env)


def publish(client_port: int, sequence: int) -> subprocess.CompletedProcess:
    environment = dict(os.environ)
    environment["ROVQET_ADDR"] = f"127.0.0.1:{client_port}"
    # A refused route must fail fast instead of waiting for the full client timeout.
    environment["ROVQET_REQUEST_TIMEOUT_MS"] = "15000"
    return subprocess.run(
        [str(Q1), TOPIC, str(PRODUCER), str(OWNER_EPOCH), str(sequence),
         str(100 + sequence), str(PARTITION), str(INCARNATION), str(OWNER_EPOCH),
         f"event-{sequence}"],
        env=environment, capture_output=True, text=True, timeout=120,
    )


def committed_offset(stdout: str) -> tuple[int, int]:
    for line in stdout.splitlines():
        if line.startswith("COMMITTED_LOCAL "):
            _, incarnation, offset = line.split()
            return int(incarnation), int(offset)
    raise Failure(f"the client did not report a durable local commitment: {stdout!r}")


def read_history(client_port: int) -> list[dict]:
    environment = dict(os.environ)
    environment["ROVQET_ADDR"] = f"127.0.0.1:{client_port}"
    result = subprocess.run(
        [str(READ_Q1), TOPIC, str(TENANT), str(TOPIC_ID), str(PARTITION),
         str(INCARNATION), "0", "64", "65536", "0"],
        env=environment, capture_output=True, text=True, timeout=120,
    )
    if result.returncode != 0:
        raise Failure(f"the external reader failed: {result.stderr.strip()}")
    records = []
    for line in result.stdout.splitlines():
        if not line.startswith("READ_RECORD "):
            continue
        _, incarnation, offset, message_id, payload_hex = line.split()
        payload = bytes.fromhex(payload_hex)
        record = {
            "incarnation": int(incarnation),
            "offset": int(offset),
            "message_id": int(message_id),
            "digest": q1_oracle.digest_hex(payload),
            "producer": PRODUCER,
            "epoch": OWNER_EPOCH,
        }
        text = payload.decode("utf-8", errors="replace")
        if text.startswith("event-") and text[6:].isdigit():
            record["sequence"] = int(text[6:])
        records.append(record)
    return records


class Observers:
    """Turns new broker/voter output into journal records in observed order."""

    def __init__(self, journal: q1_oracle.Journal):
        self.journal = journal
        self.nodes: dict[str, Node] = {}
        self.cursor: dict[str, int] = {}
        self.stages: set[str] = set()
        self.stage_payloads: list[dict] = []
        self.fences: list[dict] = []
        self.revisions: list[int] = []
        self.inventories: list[dict] = []

    def add(self, node: Node) -> None:
        self.nodes[node.name] = node
        self.cursor[node.name] = 0

    def pump(self) -> None:
        for name, node in self.nodes.items():
            lines = list(node.lines)
            index = self.cursor[name]
            while index < len(lines):
                line = lines[index]
                index += 1
                if "Q1 route active" in line and name != "voter3":
                    self.journal.append("route_active", node=name)
                if "MIGRATION committed " in line:
                    payload = json.loads(line.split("MIGRATION committed ", 1)[1])
                    stage = normalize_stage(payload.get("stage"))
                    # The oracle forbids a first stage being committed twice with different
                    # revisions; the first committed transition of a stage is the evidence.
                    if stage not in self.stages:
                        self.stages.add(stage)
                        fence = payload.get("fence") or {}
                        self.stage_payloads.append({**payload, "stage_id": stage,
                                                    "fence_obj": fence})
                        self.journal.append(
                            "migration_stage",
                            stage=stage,
                            revision=payload.get("metadata_revision"),
                            manifest_id=str(fence.get("manifest_generation",
                                                      payload.get("manifest_id"))),
                            transfer_watermark=payload.get("transfer_watermark"),
                        )
                elif "MIGRATION fence " in line:
                    payload = json.loads(line.split("MIGRATION fence ", 1)[1])
                    self.fences.append(payload)
                    self.journal.append(
                        "fence_confirmed",
                        node=name,
                        member=payload.get("member", "source"),
                        epoch=payload.get(
                            "old_epoch", payload.get("original_epoch", payload.get("epoch"))
                        ),
                        fence_offset=payload.get("fence_offset"),
                        fence_digest=payload.get("fence_digest", payload.get("digest")),
                    )
                elif "MIGRATION inventory " in line:
                    payload = json.loads(line.split("MIGRATION inventory ", 1)[1])
                    self.inventories.append(payload)
                    self.journal.append(
                        "transfer_verified",
                        member=payload.get("member"),
                        phase=payload.get("phase"),
                        inventory=payload.get("manifest_id"),
                        verified=True,
                        watermark=payload.get("watermark"),
                    )
                elif '"applied"' in line:
                    try:
                        payload = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    revision = payload.get("revision")
                    if isinstance(revision, int) and revision not in self.revisions:
                        self.revisions.append(revision)
                        self.journal.append("metadata_revision", revision=revision)
            self.cursor[name] = index

    def settle(self, seconds: float) -> None:
        deadline = time.monotonic() + seconds
        while time.monotonic() < deadline:
            self.pump()
            time.sleep(0.1)
        self.pump()

    def migration_id(self) -> int | None:
        def search(value: object) -> int | None:
            def coerce(candidate: object) -> int | None:
                # The broker prints the u128 identity as a decimal string.
                if isinstance(candidate, bool):
                    return None
                if isinstance(candidate, int):
                    return candidate
                if isinstance(candidate, str) and candidate.isdigit():
                    return int(candidate)
                return None

            if isinstance(value, dict):
                found = coerce(value.get("migration_id"))
                if found is not None:
                    return found
                for nested in value.values():
                    found = search(nested)
                    if found is not None:
                        return found
            elif isinstance(value, list):
                for nested in value:
                    found = search(nested)
                    if found is not None:
                        return found
            return None

        for payload in self.stage_payloads:
            found = search(payload)
            if found is not None:
                return found
        return search(self.fences)


def placement_owner(placements: list[dict], tenant: int, topic: int,
                    partition: int) -> int | None:
    for placement in placements or []:
        if placement.get("tenant_id") != tenant:
            continue
        if placement.get("topic_id") != topic:
            continue
        if placement.get("partition_id") != partition:
            continue
        return placement.get("owner")
    return None


def latest_placements(node: Node) -> list[dict]:
    latest: list[dict] = []
    for line in list(node.lines):
        if '"placements"' not in line:
            continue
        try:
            payload = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(payload.get("placements"), list):
            latest = payload["placements"]
    return latest


def latest_revision(node: Node) -> int | None:
    revision = None
    for line in list(node.lines):
        if '"applied"' not in line:
            continue
        try:
            payload = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(payload.get("revision"), int):
            revision = payload["revision"]
    return revision


def run_scenario(journal: q1_oracle.Journal, observers: Observers,
                 source: Node, target: Node, voter: Voter,
                 source_client: int, target_client: int,
                 block_file: Path, release_file: Path) -> None:
    # ---------------------------------------------------------------- step 1: live route
    print("STEP 1: the source opens its route and commits sequence 0", flush=True)
    journal.append("publication_attempt", node="source", producer=PRODUCER,
                   epoch=OWNER_EPOCH, sequence=0)
    first = publish(source_client, 0)
    if first.returncode != 0:
        raise Failure(f"the source publication failed: {first.stderr.strip()}")
    incarnation, offset = committed_offset(first.stdout)
    journal.append("publication_committed", node="source", producer=PRODUCER,
                   epoch=OWNER_EPOCH, sequence=0, offset=offset,
                   message_id=100, digest=q1_oracle.digest_hex(b"event-0"),
                   incarnation=incarnation)
    observers.pump()
    print(f"  source committed sequence 0 at incarnation {incarnation} offset {offset}",
          flush=True)

    # The operator asks for the handover. The driver commits the verified catch-up transfer
    # and then parks at the fault-injection barrier just before it fences its own log: the
    # release file is only created in step 7, so the source stays alive, unfenced and serving
    # while the successor and the other voters are unreachable.
    (source.cwd / "raft").mkdir(parents=True, exist_ok=True)
    (source.cwd / "raft" / "MIGRATE").write_text("2\n")
    if source.wait_for("MIGRATION_BARRIER before_fence", 180) is None:
        raise Failure(
            "the source never committed the catch-up transfer and reached the pre-fence "
            "barrier; last lines: " + " | ".join(source.lines[-8:])
        )
    observers.pump()
    if not any(payload["stage_id"] == "catch_up" for payload in observers.stage_payloads):
        raise Failure("the source never committed the catch_up stage")
    cooked_revision = observers.stage_payloads[-1].get("metadata_revision")
    if isinstance(cooked_revision, int):
        journal.append("metadata_revision", revision=cooked_revision)
    print("  source committed catch_up and is parked before its fence, still unfenced",
          flush=True)
    parked_at = time.monotonic()

    # ---------------------------------------------------------------- step 2: isolation
    print("STEP 2: isolate the source from 2,3 (outgoing only) and keep it alive", flush=True)
    block_file.write_text("2,3\n")
    time.sleep(0.5)
    if not source.alive():
        raise Failure("the isolated source died instead of staying alive")
    print(f"  block file holds {block_file.read_text().strip()!r}; the source is alive",
          flush=True)

    # ------------------------------------- step 3: the old owner keeps its authority
    print("STEP 3: the isolated old owner commits a pre-fence write (sequence 1)", flush=True)
    journal.append("publication_attempt", node="source", producer=PRODUCER,
                   epoch=OWNER_EPOCH, sequence=1)
    second = publish(source_client, 1)
    if second.returncode != 0:
        raise Failure(
            "the isolated old owner stopped serving while it was still unfenced: "
            f"{second.stderr.strip()}"
        )
    incarnation, offset = committed_offset(second.stdout)
    journal.append("publication_committed", node="source", producer=PRODUCER,
                   epoch=OWNER_EPOCH, sequence=1, offset=offset, message_id=101,
                   digest=q1_oracle.digest_hex(b"event-1"), incarnation=incarnation)
    observers.pump()
    history = read_history(source_client)
    journal.append("history_observed", node="source", records=history)
    observers.pump()
    offsets = [record["offset"] for record in history]
    sequences = [record.get("sequence") for record in history]
    if len(history) != 2 or sequences != [0, 1]:
        raise Failure(
            f"the isolated old owner history is {offsets} with sequences {sequences}; "
            "expected both pre-fence writes"
        )
    if offsets != sorted(set(offsets)):
        raise Failure(f"the isolated old owner history offsets are not ordered: {offsets}")
    print(f"  source history while isolated: offsets {offsets} sequences {sequences}",
          flush=True)

    # ------------------------- step 4: the metadata majority keeps committing
    print("STEP 4: the metadata majority commits a revision through voter 3", flush=True)
    before_revision = latest_revision(voter)
    reply = voter.propose({"SetNode": {"node": FOREIGN_NODE, "address": "127.0.0.1:1"}})
    observers.pump()
    if reply.get("ok") is not True:
        raise Failure(f"the metadata majority refused an ordinary command: {reply}")
    response = reply.get("response") or {}
    applied = response.get("Applied")
    if not isinstance(applied, dict) or not isinstance(applied.get("revision"), int):
        raise Failure(f"the command was not applied: {reply}")
    revision = applied["revision"]
    if before_revision is not None and revision <= before_revision:
        raise Failure(
            f"the committed revision did not advance: {before_revision} -> {revision}"
        )
    observers.settle(0.5)
    if revision not in observers.revisions:
        raise Failure(f"no voter reported the applied revision {revision}")
    print(f"  metadata revision {before_revision} -> {revision} while the source is isolated",
          flush=True)

    # ------------------------- step 5: the successor must not activate
    print("STEP 5: the successor must not activate while the owner is unfenced", flush=True)
    observers.settle(3.0)
    if any("Q1 route active" in line for line in target.lines):
        raise Failure("the successor activated while the old owner was unfenced")
    if not target.alive():
        raise Failure("the successor process died during the isolation")
    journal.append("publication_attempt", node="target", producer=PRODUCER,
                   epoch=OWNER_EPOCH, sequence=1)
    to_target = publish(target_client, 1)
    if to_target.returncode == 0:
        raise Failure("the successor committed a publication without an activation")
    journal.append("publication_failed", node="target", producer=PRODUCER,
                   epoch=OWNER_EPOCH, sequence=1, outcome="refused",
                   detail=to_target.stderr.strip()[:400])
    print(f"  successor publication refused: {to_target.stderr.strip()[:200]}", flush=True)

    # ------------- step 6: no absent fence response becomes a durable fence
    print("STEP 6: the metadata group refuses a fabricated fence proof", flush=True)
    # The committed metadata record sits at catch_up: the transfer is verified, but the
    # source cannot send its fence request, so the group holds no fence evidence at all.
    # ActivateNew is not reachable from catch_up, and FenceOld -- the transition that
    # carries fence evidence -- must reject a proof naming a foreign owner.
    catch_up = observers.stage_payloads[-1]
    migration_id = observers.migration_id()
    if migration_id is None:
        raise Failure("the committed catch_up transition carries no migration id")
    watermark = catch_up.get("transfer_watermark")
    manifest_id = catch_up.get("manifest_id")
    fabricated_proof = {
        "migration_id": migration_id,
        "old_owner": FOREIGN_NODE,
        "old_epoch": OWNER_EPOCH,
        "fence_offset": 11,
        "fence_digest": "ff",
        "manifest_generation": 1,
    }
    for stage, proof in (("ActivateNew", fabricated_proof), ("FenceOld", fabricated_proof)):
        command = {
            "AdvanceMigration": {
                "migration_id": migration_id,
                "stage": stage,
                "transfer_watermark": watermark,
                "manifest_id": manifest_id,
                "fence_proof": proof,
                "target_ready": True,
                "dependencies": [],
            }
        }
        reply = voter.propose(command)
        observers.pump()
        if reply.get("ok") is not True:
            raise Failure(f"the voter did not even apply the {stage} transition: {reply}")
        rejected = (reply.get("response") or {}).get("Rejected")
        if not isinstance(rejected, dict):
            raise Failure(
                f"a fabricated fence proof was not rejected by the metadata group: "
                f"{json.dumps(reply)}"
            )
        reason = str(rejected.get("reason", ""))
        journal.append("metadata_refused",
                       command=f"AdvanceMigration({stage}) with a fabricated fence proof, "
                               f"migration {migration_id}",
                       reason=reason)
        print(f"  {stage} rejected: {reason}", flush=True)
        if stage == "FenceOld" and "fence" not in reason.lower():
            raise Failure(f"the rejection does not name the fence evidence: {reason!r}")
    observers.settle(0.5)
    placements = latest_placements(voter)
    owner = placement_owner(placements, TENANT, TOPIC_ID, PARTITION)
    if owner != 1:
        raise Failure(
            f"the committed placement of (1,3,0) moved to {owner} without a real fence: "
            f"{placements}"
        )
    print(f"  placement (1,3,0) still owned by node {owner}", flush=True)
    print(f"  parked for {time.monotonic() - parked_at:.1f}s while the owner stayed unfenced",
          flush=True)

    # ------------------------- step 7: restore the path, the handover completes
    print("STEP 7: restore the path and let the real handover complete", flush=True)
    block_file.unlink()
    release_file.write_text("release\n")
    target_active = target.wait_for("Q1 route active", 240.0)
    observers.pump()
    if target_active is None:
        raise Failure("the successor never activated after the path was restored")
    # The activator commits the activation without printing a committed transition line in
    # this build, so the committed placement move of (1,3,0) is the external evidence of the
    # activation revision.
    activation_revision = None
    activation_deadline = time.monotonic() + 60.0
    while time.monotonic() < activation_deadline:
        observers.pump()
        if placement_owner(latest_placements(voter), TENANT, TOPIC_ID, PARTITION) == 2:
            activation_revision = latest_revision(voter)
            break
        time.sleep(0.1)
    if activation_revision is None:
        raise Failure("the metadata group never committed the new owner of (1,3,0)")
    last_inventory = observers.inventories[-1] if observers.inventories else {}
    journal.append("migration_stage", stage="activate_new", revision=activation_revision,
                   manifest_id=str(last_inventory.get("manifest_id")),
                   transfer_watermark=last_inventory.get("watermark"))
    print(f"  committed activation revision {activation_revision} moved (1,3,0) to node 2",
          flush=True)
    observers.settle(2.0)
    source_history = read_history(source_client)
    journal.append("history_observed", node="source", records=source_history)
    target_history = read_history(target_client)
    journal.append("history_observed", node="target", records=target_history)
    observers.pump()
    projection = [
        (record["offset"], record["message_id"], record["digest"])
        for record in source_history
    ]
    served = [
        (record["offset"], record["message_id"], record["digest"])
        for record in target_history
    ]
    print(f"  source history {projection}", flush=True)
    print(f"  target history {served}", flush=True)
    if not projection or projection != served:
        raise Failure(
            "the successor history does not equal the old owner history: "
            f"{projection} != {served}"
        )
    if len(projection) != 2:
        raise Failure(f"unexpected history length after the handover: {len(projection)}")

    # ------------------------- step 8: the old owner refuses new mutations
    print("STEP 8: the old owner now refuses new mutations", flush=True)
    journal.append("publication_attempt", node="source", producer=PRODUCER,
                   epoch=OWNER_EPOCH, sequence=2)
    fenced = publish(source_client, 2)
    if fenced.returncode == 0:
        raise Failure("the fenced old owner still committed a new mutation")
    journal.append("publication_failed", node="source", producer=PRODUCER,
                   epoch=OWNER_EPOCH, sequence=2, outcome="refused",
                   detail=fenced.stderr.strip()[:400])
    print(f"  old owner publication refused: {fenced.stderr.strip()[:200]}", flush=True)

    # ------------------------- step 9: no overlapping durable commitment
    print("STEP 9: no overlapping durable commitment", flush=True)
    final_source = read_history(source_client)
    journal.append("history_observed", node="source", records=final_source)
    final_target = read_history(target_client)
    journal.append("history_observed", node="target", records=final_target)
    observers.settle(0.5)
    source_projection = [(r["offset"], r["message_id"], r["digest"]) for r in final_source]
    target_projection = [(r["offset"], r["message_id"], r["digest"]) for r in final_target]
    if source_projection != projection or target_projection != projection:
        raise Failure(
            "a durable record appeared after the fence: "
            f"source {source_projection} target {target_projection} expected {projection}"
        )
    fenced_epochs = {
        record["incarnation"] for record in final_source + final_target
    }
    if fenced_epochs != {INCARNATION}:
        raise Failure(f"the log incarnation changed: {sorted(fenced_epochs)}")
    print(f"  both histories hold exactly {len(projection)} records, incarnation "
          f"{INCARNATION}", flush=True)


def main() -> int:
    if not (SERVER.exists() and VOTER_BIN.exists() and Q1.exists() and READ_Q1.exists()):
        print(
            "build the fault-injection binaries first: "
            "cargo build -p rovqet-server --features fault-injection --locked; "
            "cargo build -p rovqet-metadata-raft --features fault-injection --bins --locked",
            file=sys.stderr,
        )
        return 2
    EVIDENCE.parent.mkdir(parents=True, exist_ok=True)
    root = Path(tempfile.mkdtemp(prefix="rovqet-c17-", dir="/tmp/opencode"))
    peer_ports = {1: free_port(), 2: free_port()}
    raft_ports = {1: free_port(), 2: free_port(), 3: free_port()}
    source_client = free_port()
    target_client = free_port()
    block_file = root / "blocked-peers"
    release_file = root / "release-before-fence"
    journal = q1_oracle.Journal(EVIDENCE, "c17-live-isolation")
    observers = Observers(journal)
    source = broker("source", 1, root / "source", peer_ports, raft_ports, source_client,
                    True, {"ROVQET_FAULT_BLOCK_FILE": str(block_file),
                           "ROVQET_MIGRATION_BARRIER": "before_fence",
                           "ROVQET_MIGRATION_BARRIER_RELEASE": str(release_file)})
    target = broker("target", 2, root / "target", peer_ports, raft_ports, target_client,
                    False)
    voter = Voter(
        "voter3",
        [str(VOTER_BIN), "--id", "3", "--cluster-id", str(CLUSTER),
         "--listen", f"127.0.0.1:{raft_ports[3]}", "--data-dir", str(root / "voter3"),
         "--bootstrap", "false",
         "--peer", f"1=127.0.0.1:{raft_ports[1]}",
         "--peer", f"2=127.0.0.1:{raft_ports[2]}",
         "--peer", f"3=127.0.0.1:{raft_ports[3]}"],
        root,
    )
    journal.append(
        "scenario_started",
        scenario="C17-live-isolation",
        barrier="",
        victim="source",
        invariant="no-overlapping-commitment",
        build=build_identity(),
        bundle=False,
    )
    for node in (source, target, voter):
        observers.add(node)
    code = 1
    try:
        for node in (source, target, voter):
            node.start()
        observers.pump()
        if source.wait_for("Q1 route active", 90) is None:
            raise Failure("the source never opened its route")
        observers.pump()
        if voter.wait_for('"leader"', 60) is None and voter.wait_for('"follower"', 5) is None:
            raise Failure("the metadata voters never elected a leader")
        run_scenario(journal, observers, source, target, voter,
                     source_client, target_client, block_file, release_file)
        journal.conclusion(
            q1_oracle.PASS,
            "the isolated owner stayed alive and kept serving, the metadata majority "
            "advanced independently while refusing a fabricated fence proof, the successor "
            "never activated until the path was restored, and no durable record overlapped",
        )
        code = 0
        print("PASS", flush=True)
    except Failure as error:
        print(f"FAIL: {error}", flush=True)
        journal.conclusion(q1_oracle.FAIL, str(error))
    finally:
        for node in (source, target, voter):
            node.stop()
        journal.close()

    try:
        records = q1_oracle.load(EVIDENCE)
        verdict = q1_oracle.reconcile_safety(records)
        print(f"ORACLE {verdict['verdict']} {verdict.get('reasons', [])}", flush=True)
        if verdict["verdict"] != q1_oracle.PASS:
            return 1
    except q1_oracle.JournalError as error:
        print(f"ORACLE INVALID: {error}", flush=True)
        return 1
    print(f"journal: {EVIDENCE}", flush=True)
    return code


if __name__ == "__main__":
    sys.exit(main())
