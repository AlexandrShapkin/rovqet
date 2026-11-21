#!/usr/bin/env python3
"""D1/D2/D3: DLQ uncertainty cases across a coordinated source-plus-DLQ handover.

Three real multi-process campaigns, each with a source broker that owns a Q1 partition
plus a real local DLQ partition, a successor broker with its own initially inactive
directories, and a third independent metadata voter.  Every observation is an externally
received client result or an external read; the parent owns the fsynced journal.

* D1 -- a duplicate terminal disposition across the fencing boundary: the saved outcome is
  returned before and after the handover, and the closed disposition never publishes a
  second dependent message.
* D2 -- a durable PENDING terminal during the migration: the DLQ publication fails at the
  WAL, the client sees an uncertain/pending outcome, the quiescence barrier reports the
  pending terminal and the successor completes it against the promoted DLQ.
* D3 -- a crash after the DLQ publication committed but before the source terminal was
  durable: recovery reconciles the stable disposition identity without a second
  publication.

SIGKILL is process-crash recovery, not physical power-loss certification.  Disposable
storage only; the brokers share one machine, so this proves protocol behaviour, not
independent failure domains.
"""
from __future__ import annotations

import contextlib
import json
import os
import shutil
import signal
import subprocess
import sys
import tempfile
import time
import traceback
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

import q1_oracle as oracle  # noqa: E402
import test_q1_faults as faults  # noqa: E402
import test_q1_migration as harness  # noqa: E402

ROOT = HERE.parent
DEFAULT_SERVER = ROOT / "target/debug/rovqet"
# The campaign runs the broker from target/debug by default; ROVQET_SERVER may point at
# an isolated build so target/debug itself never has to be rewritten.
SERVER = Path(os.environ.get("ROVQET_SERVER", str(DEFAULT_SERVER)))
VOTER = ROOT / "target/debug/metadata-raft-node"
Q1 = ROOT / "target/debug/examples/q1"
READ = ROOT / "target/debug/examples/read_q1"
CONSUME = ROOT / "target/debug/examples/consume_q1"
NACK = ROOT / "target/debug/examples/nack_q1"
EVIDENCE = ROOT / "benchmarks/evidence/c02-safety-matrix"

CLUSTER = 737373
TENANT = "1"
TOPIC = "3"
PARTITION = "0"
INCARNATION = "42"
TOPIC_NAME = "durable"
GROUP = "9"
SOURCE_EPOCH = 7
SUCCESSOR_EPOCH = 8
PRODUCER = 11
PRODUCER_EPOCH = 1

DLQ_TOPIC = "7"
DLQ_TOPIC_NAME = "durable-dlq"
DLQ_PARTITION = "0"
DLQ_INCARNATION = "77"

D1 = "D1-duplicate-disposition-across-fencing"
D2 = "D2-pending-terminal-at-migration"
D3 = "D3-crash-after-dlq-commit-before-terminal-closure"

BUILD = (
    subprocess.run(
        ["git", "rev-parse", "HEAD"], cwd=ROOT, capture_output=True, text=True
    ).stdout.strip()
    or "unknown"
)
BUNDLE = [
    {"partition": [int(TENANT), int(TOPIC), int(PARTITION)], "incarnation": int(INCARNATION)},
    {"partition": [int(TENANT), int(DLQ_TOPIC), int(DLQ_PARTITION)],
     "incarnation": int(DLQ_INCARNATION)},
]

STAGE_MAP = {"catchup": "catch_up", "fenceold": "fence_old", "activatenew": "activate_new"}

ENV_ATTRS = ("env", "environment", "_env", "child_env", "popen_env")


def oracle_stage(stage: str) -> str:
    return STAGE_MAP.get(stage.replace("_", ""), stage)


@contextlib.contextmanager
def env_scope(mapping: dict[str, str]):
    """Expose fault-control variables to the child process and restore afterwards."""
    saved = {key: os.environ.get(key) for key in mapping}
    os.environ.update(mapping)
    try:
        yield
    finally:
        for key, value in saved.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value


class UncertaintyBroker(harness.Broker):
    """A broker whose Q1 route owns a real local DLQ partition."""

    def __init__(self, name, node_id, root, peer_ports, raft_ports, initialize,
                 migrate_to=None, extra_env=None):
        super().__init__(name, node_id, root, peer_ports, raft_ports, initialize, migrate_to)
        for index, argument in enumerate(self.argv):
            if argument == str(DEFAULT_SERVER):
                self.argv[index] = str(SERVER)
        (root / "dlq").mkdir(parents=True, exist_ok=True)
        dlq_args = [
            "--q1-terminal-policy", "dlq",
            "--q1-max-attempts", "1",
            "--q1-retry-initial-ms", "10",
            "--q1-retry-max-ms", "50",
            "--q1-dlq-data-dir", str(root / "dlq"),
            "--q1-dlq-topic", DLQ_TOPIC_NAME,
            "--q1-dlq-topic-id", DLQ_TOPIC,
            "--q1-dlq-partition-id", DLQ_PARTITION,
            "--q1-dlq-incarnation", DLQ_INCARNATION,
            "--q1-dlq-owner-epoch", str(SOURCE_EPOCH),
        ]
        index = self.argv.index("--cluster-id")
        self.argv[index:index] = dlq_args
        self.argv[self.argv.index("--cluster-id") + 1] = str(CLUSTER)
        self.dlq_root = root / "dlq"
        self.metadata_dir = root / "raft"
        self.extra_env = dict(extra_env or {})
        for attribute in ENV_ATTRS:
            value = self.__dict__.get(attribute)
            if isinstance(value, dict):
                value.update(self.extra_env)

    def start(self):
        with env_scope(self.extra_env):
            super().start()
        return self

    @property
    def endpoint(self) -> str:
        return f"127.0.0.1:{self.client_port}"

    def _addr(self) -> dict[str, str]:
        return dict(os.environ, ROVQET_ADDR=self.endpoint)

    def read_source(self):
        return subprocess.run(
            [str(READ), TOPIC_NAME, TENANT, TOPIC, PARTITION, INCARNATION,
             "0", "64", "65536", "0"],
            env=self._addr(), capture_output=True, text=True, timeout=60,
        )

    def read_dlq(self):
        return subprocess.run(
            [str(READ), DLQ_TOPIC_NAME, TENANT, DLQ_TOPIC, DLQ_PARTITION,
             DLQ_INCARNATION, "0", "64", "65536", "0"],
            env=self._addr(), capture_output=True, text=True, timeout=60,
        )

    def nack(self, action: str = "nack-first", generation: int = 0):
        return subprocess.run(
            [str(NACK), TOPIC_NAME, TENANT, TOPIC, PARTITION, INCARNATION, GROUP,
             str(generation), "16", "65536", "0", action],
            env=self._addr(), capture_output=True, text=True, timeout=90,
        )


def nack_argv(action: str, generation: int = 0) -> list[str]:
    return [str(NACK), TOPIC_NAME, TENANT, TOPIC, PARTITION, INCARNATION, GROUP,
            str(generation), "16", "65536", "0", action]


def raw_nack(broker: UncertaintyBroker, token: dict, epoch: int) -> dict:
    """Replay a consumed token as a raw NACK frame.

    A repeated terminal token is a read-only saved outcome: the server never publishes a
    second dependent message for it.  The ownership epoch must be the one that owns the
    partition (7 on the source, 8 on the successor) or the request is refused stale.
    """
    fields = {
        9: TOPIC_NAME.encode(), 19: (0).to_bytes(4, "big"),
        24: (int(GROUP)).to_bytes(8, "big"),
        25: token["generation"].to_bytes(8, "big"),
        26: token["lease"].to_bytes(16, "big"),
        27: epoch.to_bytes(8, "big"),
        20: (int(INCARNATION)).to_bytes(16, "big"),
        21: token["offset"].to_bytes(8, "big"),
        37: token["attempt"].to_bytes(4, "big"),
        45: (1).to_bytes(2, "big"),
    }
    with faults.connect(broker) as connection:
        connection.sendall(faults.frame(15, 900, fields))
        kind, correlation, response = faults.read_frame(connection)
        if kind == 20:
            return {"error": int.from_bytes(response[28], "big")}
        assert (kind, correlation) == (27, 900), (kind, correlation)
        return {"outcome": int.from_bytes(response[51], "big"),
                "disposition": int.from_bytes(response[38], "big")}


def parse_deliveries(stdout: str) -> list[dict]:
    deliveries = []
    for line in stdout.splitlines():
        parts = line.split()
        if parts and parts[0] == "DELIVERY":
            deliveries.append({
                "incarnation": int(parts[1]), "offset": int(parts[2]),
                "message_id": int(parts[3]), "generation": int(parts[4]),
                "lease": int(parts[5]), "attempt": int(parts[6]),
                "payload": bytes.fromhex(parts[8]),
            })
    return deliveries


def parse_nack(stdout: str) -> dict:
    results, acks = [], []
    for line in stdout.splitlines():
        parts = line.split()
        if not parts:
            continue
        if parts[0] == "NACK_RESULT":
            results.append({"offset": int(parts[1]), "code": int(parts[2]),
                            "delay": int(parts[3]), "disposition_id": int(parts[4]),
                            "generation": int(parts[5])})
        elif parts[0] == "ACK_COMMITTED":
            acks.append({"incarnation": int(parts[1]), "offset": int(parts[2]),
                         "generation": int(parts[3])})
    return {"results": results, "acks": acks,
            "deliveries": parse_deliveries(stdout)}


def records(stdout: str) -> list[dict]:
    """READ_RECORD <incarnation> <offset> <message_id> <hex payload>."""
    parsed = []
    for line in stdout.splitlines():
        parts = line.split()
        if not parts or parts[0] != "READ_RECORD":
            continue
        payload = bytes.fromhex(parts[4])
        parsed.append({"incarnation": int(parts[1]), "offset": int(parts[2]),
                       "message_id": int(parts[3]), "payload": payload,
                       "digest": oracle.digest_hex(payload)})
    return parsed


def parse_dlq_payload(payload: bytes) -> dict:
    assert payload[:6] == b"RQDL\0\x01", payload[:6]
    at = 6

    def take(count):
        nonlocal at
        value = payload[at:at + count]
        at += count
        return value

    return {
        "disposition_id": int.from_bytes(take(16), "big"),
        "group_id": int.from_bytes(take(8), "big"),
        "tenant_id": int.from_bytes(take(8), "big"),
        "topic_id": int.from_bytes(take(8), "big"),
        "partition_id": int.from_bytes(take(4), "big"),
        "source_incarnation": int.from_bytes(take(16), "big"),
        "source_offset": int.from_bytes(take(8), "big"),
        "original_message_id": int.from_bytes(take(16), "big"),
        "reason": take(1)[0],
        "attempts": int.from_bytes(take(4), "big"),
        "payload": payload[at:],
    }


def consume_cursor(broker: UncertaintyBroker) -> dict:
    result = broker.consume(ack_count=0)
    assert result.returncode == 0, (result.stdout, result.stderr)
    cursor, deliveries = None, []
    for line in result.stdout.splitlines():
        parts = line.split()
        if not parts:
            continue
        if parts[0] == "GROUP_BATCH":
            cursor = int(parts[3])
        elif parts[0] == "DELIVERY":
            deliveries.append(int(parts[2]))
    assert cursor is not None, result.stdout
    return {"cursor": cursor, "deliveries": deliveries}


def assert_no_resolved_redelivery(deliveries: list[int], resolved: set[int],
                                  unresolved: list[int]) -> None:
    """A resolved (acknowledged or closed) offset is never delivered again.

    ``GROUP_BATCH next`` is a continuation position of the group log, not an application
    offset (three records at application offsets 0, 2 and 4 report next=11 once the group
    has resolved them), so the no-skip property is asserted on the offset set the client
    can actually observe: every redelivered offset must still be unresolved and no
    resolved offset may reappear.
    """
    resurrected = sorted(set(deliveries) & resolved)
    assert not resurrected, (
        f"resolved offsets {resurrected} were delivered again (resolved {sorted(resolved)})"
    )
    unknown = sorted(set(deliveries) - set(unresolved))
    assert not unknown, (
        f"offsets {unknown} were delivered although they are neither unresolved nor "
        f"committed (unresolved {sorted(unresolved)})"
    )


def wait_broker_line(broker, needle: str, timeout: float = 90) -> str | None:
    """Wait until the broker's captured output contains ``needle``.

    The already captured lines are scanned too: a committed metadata revision is applied
    asynchronously, so the broker may print the marker before the caller starts waiting.
    """
    deadline = time.monotonic() + timeout
    while True:
        for line in broker.lines:
            if needle in line:
                return line
        process = getattr(broker, "process", None)
        if process is not None and process.poll() is not None:
            return None
        if time.monotonic() >= deadline:
            return None
        time.sleep(0.05)


def bundle_json(process, event: str) -> list[dict]:
    prefix = f"MIGRATION {event} "
    payloads = []
    for line in process.lines:
        if not line.startswith(prefix):
            continue
        try:
            payloads.append(json.loads(line.split(prefix, 1)[1]))
        except json.JSONDecodeError:
            continue
    return payloads


def wait_marker(root: Path, stage: str, process, seconds: float = 90) -> Path:
    name = f"reached-{stage}"
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        for path in Path(root).rglob(name):
            return path
        if process is not None and process.poll() is not None:
            break
        time.sleep(0.02)
    raise AssertionError(f"the barrier {stage} was never reached under {root}")


class Campaign:
    """One scenario journal, owned by the parent and fsynced after every record."""

    def __init__(self, scenario: str, *, barrier, victim: str, invariant: str):
        EVIDENCE.mkdir(parents=True, exist_ok=True)
        self.scenario = scenario
        self.path = EVIDENCE / f"{scenario}.jsonl"
        self.journal = oracle.Journal(self.path, scenario)
        self.event("scenario_started", scenario=scenario, barrier=barrier,
                   victim=victim, invariant=invariant, build=BUILD, bundle=BUNDLE)

    def event(self, name: str, **fields) -> None:
        self.journal.append(name, **fields)

    def observe_history(self, node: str, items: list[dict]) -> None:
        self.event("history_observed", node=node, records=[
            {"incarnation": item["incarnation"], "offset": item["offset"],
             "message_id": item["message_id"], "digest": item["digest"]}
            for item in items
        ])

    def conclude(self, verdict: str, reason: str | None, counts: dict) -> None:
        self.journal.conclusion(verdict, reason=reason, counts=counts)
        self.journal.close()


class Cluster:
    """Source broker (node 1), successor (node 2) and an independent third voter."""

    def __init__(self, temporary: Path, name: str):
        self.root = temporary / name
        self.root.mkdir(parents=True, exist_ok=True)
        self.controls = self.root / "controls"
        self.controls.mkdir(exist_ok=True)
        self.peer_ports = {1: harness.free_port(), 2: harness.free_port()}
        self.raft_ports = {1: harness.free_port(), 2: harness.free_port(),
                           3: harness.free_port()}
        self.source = None
        self.target = None
        self.voter = None
        self.processes: list = []

    def start(self, source_env: dict[str, str] | None = None,
              timeout: float = 120) -> "Cluster":
        self.source = UncertaintyBroker(
            "source", 1, self.root / "source", self.peer_ports, self.raft_ports,
            True, migrate_to=2, extra_env=source_env,
        )
        self.target = UncertaintyBroker(
            "target", 2, self.root / "target", self.peer_ports, self.raft_ports, False,
        )
        self.voter = harness.Process("voter3", [
            str(VOTER),
            "--id", "3",
            "--cluster-id", str(CLUSTER),
            "--listen", f"127.0.0.1:{self.raft_ports[3]}",
            "--data-dir", str(self.root / "voter3"),
            "--bootstrap", "false",
            "--peer", f"1=127.0.0.1:{self.raft_ports[1]}",
            "--peer", f"2=127.0.0.1:{self.raft_ports[2]}",
            "--peer", f"3=127.0.0.1:{self.raft_ports[3]}",
        ], self.root)
        for process in (self.source, self.target, self.voter):
            process.start()
            self.processes.append(process)
        assert self.source.wait_for("Q1 route active", timeout), (
            "the source never opened its route: " + "\n".join(self.source.lines)
        )
        return self

    def restart_source(self, timeout: float = 120) -> UncertaintyBroker:
        """Restart the source on its original directories, without initializing a log."""
        restarted = UncertaintyBroker(
            "source", 1, self.root / "source", self.peer_ports, self.raft_ports,
            False, migrate_to=2,
        )
        restarted.start()
        self.processes.append(restarted)
        self.source = restarted
        assert restarted.wait_for("Q1 route active", timeout), (
            "the recovered source never opened its route: " + "\n".join(restarted.lines)
        )
        return restarted

    def close(self) -> None:
        for process in self.processes:
            try:
                process.stop()
            except Exception:  # teardown must not mask the campaign result
                pass


def publish_records(campaign: Campaign, broker: UncertaintyBroker, count: int,
                    *, node: str, epoch: int) -> dict[int, dict]:
    committed = {}
    for sequence in range(count):
        campaign.event("publication_attempt", node=node, producer=PRODUCER,
                       epoch=PRODUCER_EPOCH, sequence=sequence, owner_epoch=epoch)
        result = broker.publish(sequence, 100 + sequence, epoch=epoch)
        assert result.returncode == 0, (result.stdout, result.stderr)
        lines = [line for line in result.stdout.splitlines() if line.startswith("COMMITTED_LOCAL")]
        assert lines, f"publication {sequence} did not commit: {result.stdout}"
        parts = lines[0].split()
        entry = {"incarnation": int(parts[1]), "offset": int(parts[2]),
                 "message_id": 100 + sequence,
                 "digest": oracle.digest_hex(f"event-{sequence}".encode())}
        assert entry["incarnation"] == int(INCARNATION), entry
        campaign.event("publication_committed", node=node, producer=PRODUCER,
                       epoch=PRODUCER_EPOCH, sequence=sequence,
                       offset=entry["offset"], message_id=entry["message_id"],
                       digest=entry["digest"], incarnation=entry["incarnation"],
                       owner_epoch=epoch)
        committed[sequence] = entry
    assert len({entry["offset"] for entry in committed.values()}) == count
    return committed


def handover(campaign: Campaign, cluster: Cluster, *, timeout: float = 300) -> dict:
    """Write the operator trigger and journal the committed transitions."""
    (cluster.source.metadata_dir / "MIGRATE").write_text("2\n")
    line = cluster.target.wait_for("Q1 route active", timeout)
    assert line is not None, (
        "the successor never opened the recovered history: "
        + "\n".join(cluster.target.lines)
    )
    committed = bundle_json(cluster.source, "committed")
    assert committed, "the source reported no committed transition: " + "\n".join(
        cluster.source.lines
    )
    quiesced = bundle_json(cluster.source, "quiesced")
    assert len(quiesced) == 1, f"the bundle must quiesce exactly once: {quiesced}"
    fences = bundle_json(cluster.source, "fence")
    assert sorted(fence["member"] for fence in fences) == ["dependent", "source"], fences
    inventories = bundle_json(cluster.source, "inventory")

    # The fences and the verified inventories precede the committed activation in the
    # journal: the activation may only be accepted behind verified transfer evidence.
    for fence in fences:
        campaign.event("fence_confirmed", node="source", member=fence["member"],
                       epoch=fence["original_epoch"],
                       fence_offset=fence["fence_offset"],
                       fence_digest=fence["fence_digest"],
                       migration_id=fence["migration_id"])
    for inventory in inventories:
        if inventory["phase"] != "final":
            continue
        campaign.event("transfer_verified", member=inventory["member"],
                       phase=inventory["phase"], inventory=inventory["manifest_id"],
                       verified=True, watermark=inventory["watermark"],
                       migration_id=inventory["migration_id"])
    for entry in committed:
        campaign.event("migration_stage", stage=oracle_stage(entry["stage"]),
                       revision=entry["metadata_revision"],
                       manifest_id=entry.get("manifest_id"),
                       transfer_watermark=entry.get("transfer_watermark"),
                       migration_id=entry["migration_id"])
    return {"committed": committed, "quiesced": quiesced[0],
            "fences": fences, "inventories": inventories}


def one_dependent_record(broker: UncertaintyBroker, expected: dict | None = None,
                         *, size: int = 1) -> dict:
    """Exactly `size` committed DLQ records, optionally pinned to a known identity."""
    result = broker.read_dlq()
    assert result.returncode == 0, (result.stdout, result.stderr)
    parsed = records(result.stdout)
    assert len(parsed) == size, (
        f"the dependent partition must hold {size} committed record(s): {parsed}"
    )
    if expected is not None:
        record = parsed[0]
        assert record["incarnation"] == expected["incarnation"], (record, expected)
        assert record["offset"] == expected["offset"], (record, expected)
        assert record["message_id"] == expected["message_id"], (record, expected)
        assert record["digest"] == expected["digest"], (record, expected)
    return parsed[0]


# -- D1 -----------------------------------------------------------------------------
def scenario_d1(temporary: Path) -> dict:
    campaign = Campaign(D1, barrier=None, victim="none",
                        invariant="one committed DLQ disposition across the fence")
    cluster = Cluster(temporary, "d1")
    observations: dict = {}
    try:
        cluster.start()
        campaign.event("route_active", node="source")
        offsets = publish_records(campaign, cluster.source, 3,
                                  node="source", epoch=SOURCE_EPOCH)
        observations["source_offsets"] = {str(k): v["offset"] for k, v in offsets.items()}

        rejected = cluster.source.nack("nack-first")
        assert rejected.returncode == 0, (rejected.stdout, rejected.stderr)
        parsed = parse_nack(rejected.stdout)
        assert parsed["deliveries"], rejected.stdout
        token = parsed["deliveries"][0]
        assert token["offset"] == 0, token
        terminal = parsed["results"][0]
        assert terminal["code"] == 3, (
            f"the first NACK must commit the terminal disposition: {rejected.stdout}"
        )
        disposition = terminal["disposition_id"]
        assert disposition != 0
        assert disposition != 100, "the disposition must be its own identity"

        dlq = one_dependent_record(cluster.source)
        assert dlq["message_id"] == disposition, (dlq, disposition)
        metadata = parse_dlq_payload(dlq["payload"])
        assert metadata["disposition_id"] == disposition, metadata
        assert metadata["original_message_id"] == 100, metadata
        assert metadata["source_offset"] == 0, metadata
        assert metadata["group_id"] == int(GROUP), metadata
        assert metadata["payload"] == b"event-0", metadata
        observations["dlq_before"] = {
            "incarnation": dlq["incarnation"], "offset": dlq["offset"],
            "message_id": dlq["message_id"], "digest": dlq["digest"],
        }

        source_history = records(cluster.source.read_source().stdout)
        assert len(source_history) == 3, source_history
        campaign.observe_history("source", source_history)

        committed_offsets = sorted(item["offset"] for item in offsets.values())
        resolved = {0} | {ack["offset"] for ack in parsed["acks"]}
        unresolved = [offset for offset in committed_offsets if offset not in resolved]
        cursor_before = consume_cursor(cluster.source)
        campaign.event("cursor_observed", node="source", cursor=cursor_before["cursor"])
        assert_no_resolved_redelivery(cursor_before["deliveries"], resolved, unresolved)
        observations["source_cursor_before"] = cursor_before["cursor"]
        observations["source_acked_rest"] = sorted(
            ack["offset"] for ack in parsed["acks"]
        )

        # Before the handover the duplicate disposition answers with the saved outcome.
        before = raw_nack(cluster.source, token, SOURCE_EPOCH)
        assert before.get("error") is None, before
        assert before["outcome"] == 5, before
        assert before["disposition"] == disposition, (before, disposition)
        campaign.event("terminal_lookup", node="source", phase="before_handover",
                       outcome=before["outcome"], disposition_id=before["disposition"],
                       dlq_records=len(records(cluster.source.read_dlq().stdout)))
        one_dependent_record(cluster.source, {
            "incarnation": dlq["incarnation"], "offset": dlq["offset"],
            "message_id": dlq["message_id"], "digest": dlq["digest"],
        })

        # The operator trigger.  A stale pre-fence replay during the migration is allowed
        # to be refused: the point is that every path leaves exactly one publication.
        (cluster.source.metadata_dir / "MIGRATE").write_text("2\n")
        during = raw_nack(cluster.source, token, SOURCE_EPOCH)
        campaign.event("terminal_lookup", node="source", phase="during_handover",
                       outcome=during.get("outcome"), error=during.get("error"),
                       disposition_id=during.get("disposition"))
        observations["source_replay_during_handover"] = during
        one_dependent_record(cluster.source, {
            "incarnation": dlq["incarnation"], "offset": dlq["offset"],
            "message_id": dlq["message_id"], "digest": dlq["digest"],
        })

        line = cluster.target.wait_for("Q1 route active", 300)
        assert line is not None, (
            "the successor never opened the recovered history: "
            + "\n".join(cluster.target.lines)
        )
        committed = bundle_json(cluster.source, "committed")
        assert committed, cluster.source.lines
        quiesced = bundle_json(cluster.source, "quiesced")
        assert len(quiesced) == 1, quiesced
        fences = bundle_json(cluster.source, "fence")
        assert sorted(fence["member"] for fence in fences) == ["dependent", "source"], fences
        inventories = bundle_json(cluster.source, "inventory")
        for fence in fences:
            campaign.event("fence_confirmed", node="source", member=fence["member"],
                           epoch=fence["original_epoch"],
                           fence_offset=fence["fence_offset"],
                           fence_digest=fence["fence_digest"],
                           migration_id=fence["migration_id"])
        for inventory in inventories:
            if inventory["phase"] != "final":
                continue
            campaign.event("transfer_verified", member=inventory["member"],
                           phase=inventory["phase"],
                           inventory=inventory["manifest_id"], verified=True,
                           watermark=inventory["watermark"],
                           migration_id=inventory["migration_id"])
        for entry in committed:
            campaign.event("migration_stage", stage=oracle_stage(entry["stage"]),
                           revision=entry["metadata_revision"],
                           manifest_id=entry.get("manifest_id"),
                           transfer_watermark=entry.get("transfer_watermark"),
                           migration_id=entry["migration_id"])
        observations["migration"] = {
            "stages": [entry["stage"] for entry in committed],
            "fences": sorted(fence["member"] for fence in fences),
            "source_watermark": quiesced[0]["source_watermark"],
            "pending_terminal": quiesced[0]["pending_terminal"],
        }
        assert quiesced[0]["pending_terminal"] == 0, quiesced[0]

        target_history = records(cluster.target.read_source().stdout)
        assert len(target_history) == 3, target_history
        campaign.observe_history("target", target_history)
        for item in target_history:
            observed = offsets[int(item["message_id"]) - 100]
            assert (item["offset"], item["message_id"], item["digest"]) == (
                observed["offset"], observed["message_id"], observed["digest"]
            ), (item, observed)

        after = raw_nack(cluster.target, token, SUCCESSOR_EPOCH)
        assert after.get("error") is None, after
        assert after["outcome"] == 5, after
        assert after["disposition"] == disposition, (after, disposition)
        served = one_dependent_record(cluster.target, {
            "incarnation": dlq["incarnation"], "offset": dlq["offset"],
            "message_id": dlq["message_id"], "digest": dlq["digest"],
        })
        campaign.event("dlq_disposition", disposition_id=disposition, verified=True,
                       incarnation=served["incarnation"], offset=served["offset"],
                       digest=served["digest"], node="target")
        campaign.event("terminal_lookup", node="target", phase="after_handover",
                       outcome=after["outcome"], disposition_id=after["disposition"],
                       dlq_records=1)
        observations["successor_replay"] = after
        observations["dlq_after"] = {
            "incarnation": served["incarnation"], "offset": served["offset"],
            "message_id": served["message_id"], "digest": served["digest"],
        }

        cursor_after = consume_cursor(cluster.target)
        campaign.event("cursor_observed", node="target", cursor=cursor_after["cursor"])
        assert_no_resolved_redelivery(cursor_after["deliveries"], resolved, unresolved)
        assert cursor_after["deliveries"] == [], (
            "every offset was acknowledged or closed, so nothing may be redelivered: "
            f"{cursor_after}"
        )
        observations["successor_cursor"] = cursor_after["cursor"]
        observations["successor_deliveries"] = cursor_after["deliveries"]
        assert cursor_after["cursor"] >= cursor_before["cursor"], (
            cursor_before, cursor_after
        )

        campaign.conclude(oracle.PASS, reason=None, counts={
            "dlq_publications": 1, "dlq_records": 1,
            "cursor_before": cursor_before["cursor"],
            "cursor_after": cursor_after["cursor"],
        })
    except BaseException as error:
        campaign.conclude(oracle.FAIL, reason=f"{type(error).__name__}: {error}"[:800],
                          counts={})
        raise
    finally:
        cluster.close()
    return observations


# -- D2 -----------------------------------------------------------------------------
def propose_metadata(voter, command: dict, timeout: float = 60) -> dict:
    """Commit one metadata command through the standalone voter's stdin protocol.

    The voter prints one JSON event per line and answers every command with a
    ``{"event": "proposed", ...}`` record, so the campaign can commit a placement before
    the handover without touching either broker's metadata directory.
    """
    process = voter.process
    assert process is not None and process.stdin is not None, (
        "the standalone metadata voter is not running: "
        + "\n".join(voter.lines[-20:])
    )
    seen = len(voter.lines)
    process.stdin.write(json.dumps(command) + "\n")
    process.stdin.flush()
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        for line in list(voter.lines[seen:]):
            if not line.startswith("{"):
                continue
            try:
                payload = json.loads(line)
            except json.JSONDecodeError:
                continue
            if payload.get("event") == "proposed":
                return payload
        if process.poll() is not None:
            break
        time.sleep(0.02)
    raise AssertionError(
        "the metadata voter never answered the placement proposal: "
        + "\n".join(voter.lines[-20:])
    )


def scenario_d2(temporary: Path) -> dict:
    campaign = Campaign(D2, barrier=None, victim="none",
                        invariant="no terminal completion before the destination exists")
    cluster = Cluster(temporary, "d2")
    observations: dict = {}
    try:
        cluster.start()
        campaign.event("route_active", node="source")
        offsets = publish_records(campaign, cluster.source, 3,
                                  node="source", epoch=SOURCE_EPOCH)
        observations["source_offsets"] = {str(k): v["offset"] for k, v in offsets.items()}
        committed_offsets = sorted(item["offset"] for item in offsets.values())
        assert committed_offsets == [0, 2, 4], (
            "a Q1 publication occupies the message record and its producer outcome record, "
            f"so three publications sit at application offsets 0, 2 and 4: {committed_offsets}"
        )

        # An EXPIRED dependent authority in front of a HEALTHY dependent actor.  The
        # placement is committed through the independent voter with a 1 ms lease only
        # AFTER the source's route is open, so the committed-metadata watcher installs the
        # dependent authority on the live route; the source counts the lease locally, so it
        # lapses almost immediately.  The source partition keeps its normal 600000 ms
        # lease and stays fully authorized, and the handover coordinator finds the
        # committed dependent placement (owner 1, epoch 7) already present, so it is not
        # overwritten with a long lease.
        placement = propose_metadata(cluster.voter, {"SetPlacement": {
            "tenant_id": int(TENANT),
            "topic_id": int(DLQ_TOPIC),
            "partition_id": int(DLQ_PARTITION),
            "owner": 1,
            "ownership_epoch": SOURCE_EPOCH,
            "lease_valid_for_ms": 1,
        }})
        assert placement.get("ok") is True, placement
        campaign.event("placement_committed", node=placement.get("node"),
                       tenant=int(TENANT), topic=int(DLQ_TOPIC),
                       partition=int(DLQ_PARTITION), owner=1,
                       ownership_epoch=SOURCE_EPOCH, lease_valid_for_ms=1)
        observations["dependent_placement"] = {
            "node": placement.get("node"), "owner": 1,
            "ownership_epoch": SOURCE_EPOCH, "lease_valid_for_ms": 1,
        }

        # The proof that the expired dependent authority is in place: the source prints the
        # dependent authority on the committed revision that carries the placement.  Without
        # that line the placement never reached the live route, so the scenario stops
        # instead of pretending the fence exists.
        authority_line = wait_broker_line(
            cluster.source,
            f"Q1 DLQ authority: epoch={SOURCE_EPOCH} owner=1 lease_ms=1",
            timeout=90,
        )
        assert authority_line is not None, (
            "BLOCKED: the source never printed "
            f"'Q1 DLQ authority: epoch={SOURCE_EPOCH} owner=1 lease_ms=1' after the "
            "dependent placement commit, so the expired dependent authority cannot be "
            "proven; the broker output ends with:\n" + "\n".join(cluster.source.lines[-40:])
        )
        campaign.event("dlq_authority_installed", node="source",
                       line=authority_line.strip())
        observations["dlq_authority_line"] = authority_line.strip()
        time.sleep(3.0)  # let the 1 ms lease lapse on the source's local installation

        rejected = cluster.source.nack("nack-first")
        parsed = parse_nack(rejected.stdout)
        assert parsed["deliveries"], rejected.stdout
        token = parsed["deliveries"][0]
        assert token["offset"] == 0, token
        first = parsed["results"][0] if parsed["results"] else None
        code = first["code"] if first else None
        observations["client_outcome"] = {
            "code": code, "returncode": rejected.returncode,
            "errors": [line for line in rejected.stdout.splitlines()
                       if line.startswith("NACK_ERROR")],
        }

        # The short dependent lease is the whole point of this scenario.  If it does not
        # fence the publication -- the source still returns a committed disposition and the
        # dependent log holds its record -- there is no durable PENDING terminal to report,
        # so the campaign is BLOCKED instead of pretending the uncertainty exists.
        if code in (3, 5):
            dependent = cluster.source.read_dlq()
            present = records(dependent.stdout) if dependent.returncode == 0 else []
            blocked = {
                "reason": "the committed dependent placement (owner=1, epoch=7, "
                          "lease_valid_for_ms=1) does not fence the dependent publication: "
                          "the source still commits the disposition and the dependent log "
                          "holds its record, so no durable PENDING terminal exists at the "
                          "quiescence barrier",
                "client_outcome": observations["client_outcome"],
                "client_reply": [line for line in rejected.stdout.splitlines()
                                 if line.startswith(("NACK_RESULT", "NACK_ERROR"))],
                "dependent_records_before_handover": len(present),
            }
            try:
                migration = handover(campaign, cluster)
                quiesced = migration["quiesced"]
                blocked["quiesced"] = {
                    "source_watermark": quiesced["source_watermark"],
                    "pending_terminal": quiesced["pending_terminal"],
                }
                campaign.event("pending_terminal", disposition_id=0,
                               observed=quiesced["pending_terminal"] >= 1,
                               reported=quiesced["pending_terminal"],
                               stage="quiescence_barrier")
            except BaseException as error:
                blocked["handover_error"] = f"{type(error).__name__}: {error}"[:400]
            observations["blocked"] = blocked
            reported = blocked.get("quiesced", {}).get("pending_terminal", 0)
            campaign.conclude(oracle.FAIL, reason="BLOCKED: " + blocked["reason"][:700],
                              counts={"pending_terminal": reported})
            return observations

        assert code not in (3, 5), (
            "the dependent publication failed, so the client must not see a committed "
            f"disposition: {rejected.stdout}"
        )
        assert code == 4 or rejected.returncode != 0 or "NACK_ERROR" in rejected.stdout, (
            f"the client must see a pending or uncertain outcome: {rejected.stdout}"
        )
        if first:
            campaign.event("publication_failed", node="source", producer=PRODUCER,
                           epoch=PRODUCER_EPOCH, sequence=0,
                           outcome="pending_terminal_durable",
                           disposition_id=first["disposition_id"])
            campaign.event("pending_terminal", disposition_id=first["disposition_id"],
                           observed=True, client_code=first["code"],
                           stage="source_before_handover")

        # No completion may be invented before the destination exists: the source keeps all
        # three application records and the healthy dependent log holds no committed record
        # for the pending disposition.
        empty = cluster.source.read_dlq()
        assert empty.returncode == 0, (empty.stdout, empty.stderr)
        assert records(empty.stdout) == [], (
            f"the dependent partition must hold no committed record yet: {empty.stdout}"
        )
        observations["dlq_before"] = []
        source_history = records(cluster.source.read_source().stdout)
        assert len(source_history) == 3, source_history
        campaign.observe_history("source", source_history)

        resolved = {ack["offset"] for ack in parsed["acks"]}
        unresolved = [offset for offset in committed_offsets if offset not in resolved]
        assert 0 not in resolved, parsed["acks"]
        assert 0 in unresolved, (committed_offsets, resolved)
        cursor_before = consume_cursor(cluster.source)
        campaign.event("cursor_observed", node="source", cursor=cursor_before["cursor"])
        assert_no_resolved_redelivery(cursor_before["deliveries"], resolved, unresolved)
        observations["source_cursor_before"] = cursor_before["cursor"]

        migration = handover(campaign, cluster)
        quiesced = migration["quiesced"]
        observations["quiesced"] = {
            "source_watermark": quiesced["source_watermark"],
            "pending_terminal": quiesced["pending_terminal"],
        }
        campaign.event(
            "pending_terminal",
            disposition_id=(first["disposition_id"] if first else 0),
            observed=quiesced["pending_terminal"] >= 1,
            reported=quiesced["pending_terminal"],
            stage="quiescence_barrier",
        )
        if quiesced["pending_terminal"] < 1:
            observations["blocked"] = (
                "the expired dependent lease did not leave a durable PENDING terminal: "
                f"client {observations['client_outcome']}, quiesced {quiesced}, "
                "dependent records before the handover "
                f"{len(observations['dlq_before'])}"
            )
            campaign.conclude(oracle.FAIL, reason="BLOCKED: " + observations["blocked"],
                              counts={"pending_terminal": quiesced["pending_terminal"]})
            return observations

        # The handover coordinator finds the committed dependent placement (owner 1,
        # epoch 7, 1 ms lease) already present and must not overwrite it with a long
        # lease: every dependent authority line the source reports after our placement
        # keeps that short lease, and the source still cannot close the pending
        # disposition now that the coordinator ran.
        successor_line = wait_broker_line(
            cluster.source, f"Q1 DLQ authority: epoch={SUCCESSOR_EPOCH} owner=2",
            timeout=60,
        )
        proof_at = cluster.source.lines.index(authority_line)
        post_placement = [
            line.strip() for line in cluster.source.lines[proof_at + 1:]
            if line.startswith("Q1 DLQ authority:")
        ]
        long_leases = [
            line for line in post_placement
            if line.split("lease_ms=")[-1].strip() != "1"
        ]
        assert not long_leases, (
            "the handover coordinator must not overwrite the committed dependent "
            f"placement with a long lease: {long_leases}"
        )
        campaign.event("dlq_authority_after_handover", node="source",
                       lines=post_placement,
                       successor_line=(successor_line.strip()
                                       if successor_line else None))
        try:
            source_replay = raw_nack(cluster.source, token, SOURCE_EPOCH)
        except BaseException as error:
            source_replay = {"error": f"{type(error).__name__}: {error}"[:200]}
        campaign.event("terminal_lookup", node="source", phase="after_handover",
                       outcome=source_replay.get("outcome"),
                       error=source_replay.get("error"),
                       disposition_id=source_replay.get("disposition"))
        assert source_replay.get("outcome") != 5, (
            "the source must not close the pending disposition after the handover "
            f"coordinator ran: {source_replay}"
        )

        # The successor completes the pending disposition against the promoted dependent
        # log: either its own recovery pass or the client's retry may drive it, but exactly
        # one publication may exist.
        served = None
        retries: list[dict] = []
        deadline = time.monotonic() + 120
        while time.monotonic() < deadline and served is None:
            result = cluster.target.read_dlq()
            if result.returncode == 0:
                found = records(result.stdout)
                if len(found) == 1:
                    served = found[0]
                    break
            if not retries:
                retries.append(raw_nack(cluster.target, token, SUCCESSOR_EPOCH))
            time.sleep(0.1)
        assert served is not None, (
            "the successor never completed the pending disposition against the promoted "
            f"dependent log: retries {retries}"
        )
        served = one_dependent_record(cluster.target)
        disposition = served["message_id"]
        metadata = parse_dlq_payload(served["payload"])
        assert metadata["disposition_id"] == disposition, metadata
        assert metadata["original_message_id"] == 100, metadata
        assert metadata["source_offset"] == 0, metadata
        assert metadata["payload"] == b"event-0", metadata
        observations["dependent_record"] = {
            "incarnation": served["incarnation"], "offset": served["offset"],
            "message_id": served["message_id"], "digest": served["digest"],
        }
        campaign.event("dlq_disposition", disposition_id=disposition, verified=True,
                       incarnation=served["incarnation"], offset=served["offset"],
                       digest=served["digest"], node="target",
                       completed_after_handover=True)
        campaign.event("pending_terminal", disposition_id=disposition, observed=True,
                       source_reported=quiesced["pending_terminal"],
                       resolution="completed_on_successor")

        # The closed disposition answers with the saved outcome, and a repeated lookup never
        # publishes a second dependent message.
        closed = raw_nack(cluster.target, token, SUCCESSOR_EPOCH)
        assert closed.get("error") is None, closed
        assert closed["outcome"] == 5, closed
        assert closed["disposition"] == disposition, (closed, disposition)
        campaign.event("terminal_lookup", node="target", phase="after_handover",
                       outcome=closed["outcome"], disposition_id=closed["disposition"],
                       dlq_records=1)
        again = raw_nack(cluster.target, token, SUCCESSOR_EPOCH)
        assert again.get("error") is None, again
        assert again["outcome"] == 5, again
        assert again["disposition"] == disposition, (again, disposition)
        one_dependent_record(cluster.target, {
            "incarnation": served["incarnation"], "offset": served["offset"],
            "message_id": served["message_id"], "digest": served["digest"],
        })
        observations["closed_lookup"] = closed
        observations["repeat_lookup"] = again

        # The recovered source history advanced to the successor and the cursor moved over
        # the closed disposition.  A Q1 publication occupies two WAL records, so the closed
        # application offset 0 advances the cursor by two records, never by a dense range.
        target_history = records(cluster.target.read_source().stdout)
        assert len(target_history) == 3, target_history
        campaign.observe_history("target", target_history)
        for item in target_history:
            observed = offsets[int(item["message_id"]) - 100]
            assert (item["offset"], item["message_id"], item["digest"]) == (
                observed["offset"], observed["message_id"], observed["digest"]
            ), (item, observed)
        cursor_after = consume_cursor(cluster.target)
        campaign.event("cursor_observed", node="target", cursor=cursor_after["cursor"])
        resolved_after = resolved | {0}
        unresolved_after = [
            offset for offset in committed_offsets if offset not in resolved_after
        ]
        assert_no_resolved_redelivery(cursor_after["deliveries"], resolved_after,
                                       unresolved_after)
        assert cursor_after["deliveries"] == [], (
            "the pending disposition was closed and every acknowledged offset must never be "
            f"redelivered: {cursor_after}"
        )
        assert cursor_after["cursor"] >= max(cursor_before["cursor"], 2), (
            "closing application offset 0 must advance the cursor over its two WAL records: "
            f"{cursor_before} -> {cursor_after}"
        )
        observations["successor"] = {
            "disposition_id": disposition,
            "cursor": cursor_after["cursor"],
            "deliveries": cursor_after["deliveries"],
        }
        campaign.conclude(oracle.PASS, reason=None, counts={
            "dlq_publications": 1, "dlq_records": 1,
            "pending_reported": quiesced["pending_terminal"],
            "cursor_before": cursor_before["cursor"],
            "cursor_after": cursor_after["cursor"],
        })
    except BaseException as error:
        campaign.conclude(oracle.FAIL, reason=f"{type(error).__name__}: {error}"[:800],
                          counts={})
        raise
    finally:
        cluster.close()
    return observations


# -- D3 -----------------------------------------------------------------------------
def scenario_d3(temporary: Path) -> dict:
    stage = "dlq-after-publish-before-terminal"
    campaign = Campaign(D3, barrier=stage, victim="source",
                        invariant="one DLQ publication; terminal closure only after commit")
    cluster = Cluster(temporary, "d3")
    observations: dict = {}
    staged_env = {
        "ROVQET_Q1_TEST_DIR": str(cluster.controls),
        "ROVQET_Q1_TEST_STAGE": stage,
        "ROVQET_Q1_TEST_SEQUENCE": "0",
    }
    try:
        cluster.start(staged_env)
        campaign.event("route_active", node="source")
        offsets = publish_records(campaign, cluster.source, 3,
                                  node="source", epoch=SOURCE_EPOCH)
        observations["source_offsets"] = {str(k): v["offset"] for k, v in offsets.items()}

        pending = subprocess.Popen(
            nack_argv("nack-first"), env=cluster.source._addr(),
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
        )
        marker = wait_marker(cluster.root, stage, cluster.source.process)
        campaign.event("barrier_reached", barrier=stage, process="source",
                       marker=str(marker))
        process = cluster.source.process
        assert process is not None and process.poll() is None
        process.send_signal(signal.SIGKILL)
        code = process.wait(timeout=15)
        assert code == -signal.SIGKILL, (code, cluster.source.lines)
        campaign.event("process_killed", process="source", signal="SIGKILL")
        stdout, _ = pending.communicate(timeout=60)
        observations["killed_client_deliveries"] = [
            {"offset": d["offset"], "message_id": d["message_id"]}
            for d in parse_deliveries(stdout)
        ]
        cluster.source.stop()

        restarted = cluster.restart_source()
        campaign.event("process_restarted", process="source")
        campaign.event("route_active", node="source")

        # Recovery reconciles the committed destination without a second publication.
        recovered = one_dependent_record(restarted)
        metadata = parse_dlq_payload(recovered["payload"])
        disposition = recovered["message_id"]
        assert metadata["disposition_id"] == disposition, metadata
        assert metadata["original_message_id"] == 100, metadata
        assert metadata["source_offset"] == 0, metadata
        assert metadata["payload"] == b"event-0", metadata
        recovered_history = records(restarted.read_source().stdout)
        assert len(recovered_history) == 3, recovered_history
        campaign.observe_history("source", recovered_history)
        committed_offsets = sorted(item["offset"] for item in offsets.values())
        resolved = {0}
        unresolved = [offset for offset in committed_offsets if offset not in resolved]
        cursor_recovered = consume_cursor(restarted)
        campaign.event("cursor_observed", node="source", cursor=cursor_recovered["cursor"])
        assert 0 not in cursor_recovered["deliveries"], (
            "the recovered terminal must close offset 0 instead of redelivering it"
        )
        assert_no_resolved_redelivery(cursor_recovered["deliveries"], resolved, unresolved)
        observations["recovered"] = {
            "display": {"incarnation": recovered["incarnation"],
                        "offset": recovered["offset"],
                        "digest": recovered["digest"]},
            "cursor": cursor_recovered["cursor"],
            "deliveries": cursor_recovered["deliveries"],
        }

        migration = handover(campaign, cluster)
        quiesced = migration["quiesced"]
        assert quiesced["pending_terminal"] == 0, (
            f"the recovered terminal must not stay pending: {quiesced}"
        )
        observations["quiesced"] = {
            "source_watermark": quiesced["source_watermark"],
            "pending_terminal": quiesced["pending_terminal"],
        }

        target_history = records(cluster.target.read_source().stdout)
        assert len(target_history) == 3, target_history
        campaign.observe_history("target", target_history)
        served = one_dependent_record(cluster.target, {
            "incarnation": recovered["incarnation"], "offset": recovered["offset"],
            "message_id": recovered["message_id"], "digest": recovered["digest"],
        })
        campaign.event("dlq_disposition", disposition_id=disposition, verified=True,
                       incarnation=served["incarnation"], offset=served["offset"],
                       digest=served["digest"], node="target",
                       survived_crash=True)
        cursor_after = consume_cursor(cluster.target)
        campaign.event("cursor_observed", node="target", cursor=cursor_after["cursor"])
        assert 0 not in cursor_after["deliveries"], (
            "the successor must not redeliver the closed disposition"
        )
        assert_no_resolved_redelivery(cursor_after["deliveries"], resolved, unresolved)
        assert cursor_after["cursor"] >= cursor_recovered["cursor"], (
            cursor_recovered, cursor_after
        )
        observations["successor"] = {
            "disposition_id": disposition,
            "dlq": {"incarnation": served["incarnation"], "offset": served["offset"],
                    "digest": served["digest"]},
            "cursor": cursor_after["cursor"],
            "deliveries": cursor_after["deliveries"],
        }
        campaign.conclude(oracle.PASS, reason=None, counts={
            "dlq_publications": 1, "dlq_records": 1,
            "kills": 1, "restarts": 1,
            "cursor_recovered": cursor_recovered["cursor"],
            "cursor_after": cursor_after["cursor"],
        })
    except BaseException as error:
        campaign.conclude(oracle.FAIL, reason=f"{type(error).__name__}: {error}"[:800],
                          counts={})
        raise
    finally:
        cluster.close()
    return observations


SCENARIOS = (
    (D1, scenario_d1),
    (D2, scenario_d2),
    (D3, scenario_d3),
)


def main() -> int:
    if not SERVER.exists():
        print(f"missing build artifact: {SERVER}", file=sys.stderr)
        return 2
    requested = {value for value in sys.argv[1:] if not value.startswith("-")}
    selected = [
        (name, function) for name, function in SCENARIOS
        if not requested or name in requested or name.split("-")[0] in requested
    ]
    if not selected:
        print(f"no scenario matches {sorted(requested)}", file=sys.stderr)
        return 2
    temporary = Path(tempfile.mkdtemp(prefix="rovqet-dlq-uncertainty-"))
    failures: list[str] = []
    observations: dict[str, dict] = {}
    try:
        for name, function in selected:
            print(f"=== {name} ===", flush=True)
            try:
                observations[name] = function(temporary)
            except BaseException:
                traceback.print_exc()
                failures.append(name)
            value = observations.get(name)
            if value is not None:
                print(json.dumps(value, indent=2, sort_keys=True, default=str),
                      flush=True)
    finally:
        shutil.rmtree(temporary, ignore_errors=True)

    verdicts: dict[str, dict] = {}
    journals = [name for name, _ in selected]
    for name in journals:
        path = EVIDENCE / f"{name}.jsonl"
        if not path.exists():
            verdicts[name] = {"verdict": oracle.INVALID,
                              "reasons": [f"missing journal {path}"], "counts": {}}
            continue
        try:
            loaded = oracle.load(path)
        except oracle.JournalError as error:
            verdicts[name] = {"verdict": oracle.INVALID, "reasons": [str(error)],
                              "counts": {}}
            continue
        verdicts[name] = oracle.reconcile_safety(loaded)
    for name, verdict in verdicts.items():
        print(f"ORACLE {name} {verdict['verdict']} counts={verdict['counts']} "
              f"reasons={verdict['reasons']}", flush=True)

    ok = not failures and all(
        verdict["verdict"] == oracle.PASS for verdict in verdicts.values()
    )
    blocked = [
        name for name, value in observations.items() if value.get("blocked")
    ]
    if blocked:
        for name in blocked:
            print(f"BLOCKED {name}: {observations[name]['blocked']}", flush=True)
        ok = False
    if failures:
        print(f"FAILED SCENARIOS: {failures}", flush=True)
    print("PASS" if ok else ("BLOCKED" if blocked and not failures else "FAIL"),
          flush=True)
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
