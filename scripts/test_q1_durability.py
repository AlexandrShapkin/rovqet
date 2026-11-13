#!/usr/bin/env python3
"""B-07 independent Q1 durability and crash-recovery verification.

Every campaign writes an fsynced, append-only journal outside the broker data directory,
deliberately kills or faults the broker, restarts on the same storage, and reconciles the
externally observed TCP acknowledgements against post-recovery committed history and
consumer progress. PASS/FAIL/INVALID/NOT_RUN are distinct outcomes.

Requires the fault-injection broker build and the example binaries. SIGKILL is process
crash recovery, not physical power-loss certification. Destructive work is confined to
disposable temporary directories; journals are written to benchmarks/evidence.
"""
from __future__ import annotations

import hashlib
import json
import os
import platform
import re
import statistics
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from pathlib import Path

import q1_oracle as oracle
from q1_smoke import Broker, Q0, READ, SERVER, q1_command
from test_q1_consumer import CONSUME
from test_q1_faults import connect, frame, read_frame

ROOT = Path(__file__).resolve().parents[1]
MEASURE = ROOT / "target/debug/examples/q1_measure"
FSYNC_PROFILE = ROOT / "target/debug/examples/fsync_profile"
Q1 = ROOT / "target/debug/examples/q1"
EVIDENCE = ROOT / "benchmarks/evidence" / f"q1-durability-{subprocess.run(
    ['git', 'rev-parse', '--short', 'HEAD'], cwd=ROOT, capture_output=True, text=True
).stdout.strip()}"
RESULTS: list[dict] = []
RECOVERY_SAMPLES: list[dict] = []


def digest(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def payload_for(seed: int, sequence: int, size: int = 24) -> bytes:
    base = f"seed{seed:04d}-event-{sequence}".encode()
    return (base + b"." * size)[: max(size, len(base))]


def classify(result: subprocess.CompletedProcess) -> dict:
    output = (result.stdout or "") + (result.stderr or "")
    match = re.search(r"COMMITTED_LOCAL (\d+) (\d+)", result.stdout or "")
    if result.returncode == 0 and match:
        return {"outcome": "committed", "incarnation": int(match.group(1)),
                "offset": int(match.group(2))}
    if "OutcomeUnknown" in output or "Timeout" in output:
        return {"outcome": "uncertain"}
    return {"outcome": "failed", "diagnostic": output.strip()[:400]}


def parse_consume(stdout: str) -> dict:
    deliveries, batch, acks, errors = [], None, [], []
    for line in stdout.splitlines():
        parts = line.split()
        if not parts:
            continue
        if parts[0] == "DELIVERY":
            deliveries.append({
                "incarnation": int(parts[1]), "offset": int(parts[2]),
                "message_id": int(parts[3]), "generation": int(parts[4]),
                "lease": int(parts[5]), "attempt": int(parts[6]),
                "deadline_ms": int(parts[7]), "payload": bytes.fromhex(parts[8]),
            })
        elif parts[0] == "GROUP_BATCH":
            batch = {"group": int(parts[1]), "generation": int(parts[2]),
                     "next": int(parts[3]), "has_more": int(parts[4]),
                     "end": int(parts[5])}
        elif parts[0] == "ACK_COMMITTED":
            acks.append({"incarnation": int(parts[1]), "offset": int(parts[2]),
                         "generation": int(parts[3])})
        elif parts[0] == "ACK_ERROR":
            errors.append(line)
    return {"deliveries": deliveries, "batch": batch, "acks": acks, "errors": errors}


def consume(broker: Broker, *, group: int = 9, generation: int = 0,
            count: int = 256, size: int = 65536, wait: int = 0,
            ack_count: int = 0, match: int | None = None, timeout: int = 30) -> dict:
    arguments = ["durable", "1", "3", "0", "42", str(group), str(generation),
                 str(count), str(size), str(wait), str(ack_count)]
    if match is not None:
        arguments.append(str(match))
    result = subprocess.run([str(CONSUME), *arguments], env=broker.environment(),
                            capture_output=True, text=True, timeout=timeout)
    parsed = parse_consume(result.stdout)
    parsed["returncode"] = result.returncode
    parsed["stderr"] = result.stderr
    return parsed


def read_history(broker: Broker, *, count: int = 512) -> list[dict]:
    records, offset, guard = [], 0, 0
    while True:
        guard += 1
        assert guard < 64
        result = subprocess.run(
            [str(READ), "durable", "1", "3", "0", "42", str(offset), str(count),
             "65536", "0"], env=broker.environment(), capture_output=True,
            text=True, timeout=30)
        if result.returncode != 0:
            print("READ FAILURE", result.stdout, result.stderr, broker.lines[-8:])
        assert result.returncode == 0, (result.stdout, result.stderr)
        end = None
        for line in result.stdout.splitlines():
            parts = line.split()
            if parts[0] == "READ_RECORD":
                records.append({"incarnation": int(parts[1]), "offset": int(parts[2]),
                                "message_id": int(parts[3]),
                                "digest": digest(bytes.fromhex(parts[4]))})
            elif parts[0] == "READ_END":
                end = parts
        assert end is not None, result.stdout
        if end[3] == "0" or len(records) >= count:
            return records
        offset = int(end[2])


def raw_ack(broker: Broker, token: dict) -> dict:
    fields = {
        9: b"durable", 19: (0).to_bytes(4, "big"),
        24: (9).to_bytes(8, "big"),
        25: token["generation"].to_bytes(8, "big"),
        26: token["lease"].to_bytes(16, "big"),
        27: (7).to_bytes(8, "big"),
        20: (42).to_bytes(16, "big"),
        21: token["offset"].to_bytes(8, "big"),
        37: token["attempt"].to_bytes(4, "big"),
    }
    with connect(broker) as connection:
        connection.sendall(frame(13, 777, fields))
        kind, correlation, response = read_frame(connection)
        assert (kind, correlation) == (14, 777), (kind, correlation)
        return {"outcome": int.from_bytes(response[36], "big")}


class Campaign:
    """One oracle run: fsynced journal plus aggregated verdict."""

    def __init__(self, name: str):
        self.name = name
        path = EVIDENCE / "journals" / f"{name}.jsonl"
        path.parent.mkdir(parents=True, exist_ok=True)
        if path.exists():
            try:
                previous = oracle.load(path)[-1]
            except oracle.JournalError:
                previous = {"verdict": oracle.INVALID}
            if previous.get("verdict") != oracle.PASS:
                archive = path.with_suffix(
                    f".failed-{int(time.time())}.jsonl")
                path.rename(archive)
        self.journal = oracle.Journal(path, name)
        self.records: list[dict] = []
        self.artifacts = []

    def event(self, event: str, **fields) -> dict:
        record = self.journal.append(event, **fields)
        self.records.append(record)
        return record

    def conclude(self, result: dict, **extra) -> dict:
        verdict = result["verdict"]
        self.journal.conclusion(
            verdict, reason="; ".join(result["reasons"]) or None,
            counts=result["counts"], **extra)
        entry = {"run": self.name, "verdict": verdict, "reasons": result["reasons"],
                 "counts": result["counts"], **extra}
        RESULTS.append(entry)
        self.journal.close()
        return entry

    def prospective(self) -> list[dict]:
        return self.records + [{"event": "conclusion", "verdict": oracle.PASS}]


def assert_pass(entry: dict) -> None:
    assert entry["verdict"] == oracle.PASS, entry


def record_deliveries(campaign: Campaign, parsed: dict, *, phase: str) -> None:
    assert parsed["batch"] is not None, parsed
    campaign.event("generation_observed", group=9,
                   generation=parsed["batch"]["generation"], phase=phase)
    for delivery in parsed["deliveries"]:
        campaign.event("delivery", group=9, generation=delivery["generation"],
                       offset=delivery["offset"], message_id=delivery["message_id"],
                       lease=delivery["lease"], attempt=delivery["attempt"],
                       digest=digest(delivery["payload"]), phase=phase)


def record_acks(campaign: Campaign, parsed: dict) -> None:
    for ack in parsed["acks"]:
        campaign.event("ack_committed", group=9, generation=ack["generation"],
                       offset=ack["offset"], incarnation=ack["incarnation"])


def publish(broker: Broker, sequence: int, *, seed: int, payload: bytes | None = None,
            producer: int = 11, epoch: int = 0, message: int | None = None,
            timeout: int = 30) -> subprocess.CompletedProcess:
    payload = payload_for(seed, sequence) if payload is None else payload
    message = 1000 + sequence if message is None else message
    return subprocess.run(
        q1_command(sequence, producer=producer, epoch=epoch, message=message,
                   payload=payload.decode()),
        env=broker.environment(), capture_output=True, text=True, timeout=timeout)


class Q1Durability(unittest.TestCase):
    maxDiff = None

    def publication_crash(self, stage: str, seed: int) -> None:
        name = f"publication-{stage}-seed{seed}"
        campaign = Campaign(name)
        with tempfile.TemporaryDirectory(prefix=f"rovqet-b07-{name}-") as temporary:
            root = Path(temporary)
            broker = Broker(root).start()
            for sequence in range(3):
                payload = payload_for(seed, sequence)
                campaign.event("publication_attempt", producer=11, epoch=0,
                               sequence=sequence, message_id=1000 + sequence,
                               digest=digest(payload), payload=payload.decode())
                result = publish(broker, sequence, seed=seed)
                outcome = classify(result)
                if outcome["outcome"] == "committed":
                    campaign.event("publication_committed", producer=11, epoch=0,
                                   sequence=sequence, message_id=1000 + sequence,
                                   digest=digest(payload),
                                   incarnation=outcome["incarnation"],
                                   offset=outcome["offset"])
                else:
                    campaign.event("publication_failed", producer=11, epoch=0,
                                   sequence=sequence, **outcome)
            broker.stop()
            staged = Broker(root, stage=stage, sequence=3).start()
            payload = payload_for(seed, 3)
            campaign.event("publication_attempt", producer=11, epoch=0, sequence=3,
                           message_id=1003, digest=digest(payload),
                           payload=payload.decode())
            pending = staged.pending(3, payload=payload.decode(), message=1003)
            staged.barrier(stage)
            campaign.event("fault", kind="process_kill", at=stage, seed=seed)
            staged.kill()
            stdout, stderr = pending.communicate(timeout=20)
            observed = {"returncode": pending.returncode, "stdout": stdout or "",
                        "stderr": stderr or ""}
            match = re.search(r"COMMITTED_LOCAL (\d+) (\d+)", observed["stdout"])
            if match:
                campaign.event("publication_committed", producer=11, epoch=0,
                               sequence=3, message_id=1003, digest=digest(payload),
                               incarnation=int(match.group(1)),
                               offset=int(match.group(2)))
            else:
                campaign.event("publication_uncertain", producer=11, epoch=0,
                               sequence=3, message_id=1003,
                               diagnostic=(observed["stdout"] + observed["stderr"])[:400])
            started = time.monotonic()
            broker = Broker(root).start()
            recovery_seconds = time.monotonic() - started
            RECOVERY_SAMPLES.append({"scenario": name, "seconds": recovery_seconds})
            history = read_history(broker)
            for item in history:
                campaign.event("recovered_record", **item)
            result = oracle.reconcile_publications(campaign.prospective(), history)
            if result["verdict"] != oracle.PASS:
                entry = campaign.conclude(result, recovery_seconds=recovery_seconds,
                                          fault=stage, seed=seed)
                assert_pass(entry)
            # Retry every externally acknowledged token; the durable offset is immutable.
            ledger = oracle.publication_ledger(campaign.records)
            for key, ack in ledger["acknowledged"].items():
                retry = publish(broker, key[2], seed=seed,
                                payload=payload_for(seed, key[2]),
                                message=ack["message_id"])
                outcome = classify(retry)
                campaign.event("retry", producer=key[0], epoch=key[1], sequence=key[2],
                               first_offset=ack["offset"], outcome=outcome)
                assert outcome["outcome"] == "committed", (key, outcome)
                assert outcome["offset"] == ack["offset"], (key, ack, outcome)
            # Conflicting content for an already committed token cannot overwrite it.
            conflict = publish(broker, 0, seed=seed, payload=b"changed-content",
                               message=9999)
            campaign.event("conflict_attempt", sequence=0,
                           diagnostic=(conflict.stdout + conflict.stderr)[:300])
            assert conflict.returncode != 0, conflict.stdout
            assert "SequenceConflict" in (conflict.stdout + conflict.stderr)
            # Producer epoch fencing survives restart. A new epoch starts at sequence 0.
            elevated = publish(broker, 0, seed=seed, epoch=1, message=5000)
            assert classify(elevated)["outcome"] == "committed"
            fenced = publish(broker, 1, seed=seed, epoch=0, message=6001)
            campaign.event("epoch_fenced", diagnostic=(fenced.stdout + fenced.stderr)[:200])
            assert fenced.returncode != 0
            assert "ProducerFenced" in (fenced.stdout + fenced.stderr)
            broker.stop()
            entry = campaign.conclude(result, recovery_seconds=recovery_seconds,
                                      fault=stage, seed=seed)
            assert_pass(entry)

    def test_publication_crash_windows(self):
        for stage in ("before-append", "after-append-before-sync",
                      "after-sync-before-reply"):
            for seed in (42, 43):
                with self.subTest(stage=stage, seed=seed):
                    self.publication_crash(stage, seed)

    def consumer_crash(self, stage: str, seed: int, *, checkpoint: bool) -> None:
        name = f"consumer-{stage}-seed{seed}"
        campaign = Campaign(name)
        with tempfile.TemporaryDirectory(prefix=f"rovqet-b07-{name}-") as temporary:
            root = Path(temporary)
            broker = Broker(root).start()
            for sequence in range(3):
                assert classify(publish(broker, sequence, seed=seed))["outcome"] == "committed"
            first = consume(broker, ack_count=1)
            assert first["returncode"] == 0, first
            record_deliveries(campaign, first, phase="pre-crash")
            record_acks(campaign, first)
            token = first["deliveries"][0]
            acked = {ack["offset"] for ack in first["acks"]}
            first_unacked = next(
                delivery["offset"] for delivery in first["deliveries"]
                if delivery["offset"] not in acked)
            broker.stop()
            options = {"sequence": first_unacked}
            if checkpoint:
                options["checkpoint_after"] = 1
            staged = Broker(root, stage=stage, **options).start()
            pending = subprocess.Popen(
                [str(CONSUME), "durable", "1", "3", "0", "42", "9",
                 str(first["batch"]["generation"]), "8", "65536", "0", "3"],
                env=staged.environment(), stdout=subprocess.PIPE,
                stderr=subprocess.PIPE, text=True)
            staged.barrier(stage)
            campaign.event("fault", kind="process_kill", at=stage, seed=seed)
            staged.kill()
            stdout, stderr = pending.communicate(timeout=20)
            pre = parse_consume(stdout or "")
            record_deliveries(campaign, pre, phase="crash-attempt")
            record_acks(campaign, pre)
            started = time.monotonic()
            broker = Broker(root).start()
            recovery_seconds = time.monotonic() - started
            RECOVERY_SAMPLES.append({"scenario": name, "seconds": recovery_seconds})
            history = read_history(broker)
            observed = consume(broker, ack_count=0)
            assert observed["returncode"] == 0, observed
            record_deliveries(campaign, observed, phase="recovered")
            application_offsets = [item["offset"] for item in history]
            delivered_offsets = [d["offset"] for d in observed["deliveries"]]
            result = oracle.reconcile_consumers(
                campaign.prospective(),
                {"delivered_offsets": delivered_offsets,
                 "cursor": observed["batch"]["next"]},
                application_offsets)
            if result["verdict"] != oracle.PASS:
                entry = campaign.conclude(result, recovery_seconds=recovery_seconds,
                                          fault=stage, seed=seed, checkpoint=checkpoint)
                assert_pass(entry)
            # A previously committed ACK remains a read-only lookup after the fence.
            lookup = raw_ack(broker, token)
            campaign.event("ack_lookup_committed", group=9, offset=token["offset"],
                           outcome=lookup["outcome"])
            assert lookup["outcome"] == 1, lookup
            # The old generation cannot perform new mutations after restart.
            stale = consume(broker, generation=first["batch"]["generation"], ack_count=0)
            campaign.event("stale_generation_attempt",
                           generation=first["batch"]["generation"],
                           returncode=stale["returncode"],
                           diagnostic=(stale.get("stderr") or "")[:200])
            assert stale["returncode"] != 0
            assert "StaleGeneration" in (stale.get("stderr") or "")
            # Repeated restart must not renumber identities or offsets.
            broker.stop()
            broker = Broker(root).start()
            again = read_history(broker)
            assert again == history, (history, again)
            broker.stop()
            entry = campaign.conclude(result, recovery_seconds=recovery_seconds,
                                      fault=stage, seed=seed, checkpoint=checkpoint)
            assert_pass(entry)

    def test_consumer_crash_windows(self):
        for stage in ("before-append", "after-append-before-sync",
                      "after-sync-before-reply", "checkpoint-after-append-before-sync"):
            for seed in (44, 45):
                with self.subTest(stage=stage, seed=seed):
                    self.consumer_crash(
                        stage, seed,
                        checkpoint=stage.startswith("checkpoint-"))

    def test_sparse_acks_and_generation_fencing(self):
        campaign = Campaign("sparse-generation")
        with tempfile.TemporaryDirectory(prefix="rovqet-b07-sparse-") as temporary:
            root = Path(temporary)
            broker = Broker(root).start()
            for sequence in range(3):
                assert classify(publish(broker, sequence, seed=46))["outcome"] == "committed"
            sparse = consume(broker, ack_count=8, match=2)
            assert sparse["returncode"] == 0, sparse
            record_deliveries(campaign, sparse, phase="sparse")
            record_acks(campaign, sparse)
            assert [a["offset"] for a in sparse["acks"]] == [2]
            broker.stop()
            broker = Broker(root).start()
            recovered = consume(broker, ack_count=0)
            assert recovered["returncode"] == 0
            record_deliveries(campaign, recovered, phase="recovered")
            history = read_history(broker)
            result = oracle.reconcile_consumers(
                campaign.prospective(),
                {"delivered_offsets": [d["offset"] for d in recovered["deliveries"]],
                 "cursor": recovered["batch"]["next"]},
                [item["offset"] for item in history])
            if result["verdict"] != oracle.PASS:
                entry = campaign.conclude(result, scenario="sparse")
                assert_pass(entry)
            assert recovered["batch"]["next"] == 0, recovered
            assert set(d["offset"] for d in recovered["deliveries"]) == {0, 4}, recovered
            # Close the gap under a fresh claim; leftover leases from the observation
            # process are cleared by the durable generation change.
            closing = consume(broker, generation=0, ack_count=8)
            record_acks(campaign, closing)
            assert closing["returncode"] == 0, closing
            end = consume(broker, generation=closing["batch"]["generation"], ack_count=0)
            assert end["batch"]["end"] == 1 and not end["deliveries"], end
            # The observation generation is now stale and cannot mutate.
            stale = consume(broker, generation=recovered["batch"]["generation"],
                            ack_count=0)
            campaign.event("stale_generation_attempt",
                           generation=recovered["batch"]["generation"],
                           diagnostic=(stale.get("stderr") or "")[:200])
            assert stale["returncode"] != 0
            assert "StaleGeneration" in (stale.get("stderr") or "")
            broker.stop()
            entry = campaign.conclude(result, scenario="sparse")
            assert_pass(entry)

    def test_wal_fault_and_rotation_matrix(self):
        faults = ("append", "partial-append", "segment-sync", "manifest-write",
                  "manifest-sync", "manifest-rename", "directory-sync")
        for fault in faults:
            with self.subTest(fault=fault):
                campaign = Campaign(f"wal-{fault}")
                with tempfile.TemporaryDirectory(prefix=f"rovqet-b07-{fault}-") as temporary:
                    root = Path(temporary)
                    segments = {"segment_bytes": 512, "record_bytes": 256}
                    broker = Broker(root, **segments).start()
                    for sequence in range(3):
                        payload = payload_for(47, sequence)
                        campaign.event("publication_attempt", producer=11, epoch=0,
                                       sequence=sequence, message_id=1000 + sequence,
                                       digest=digest(payload), payload=payload.decode())
                        result = publish(broker, sequence, seed=47,
                                         message=1000 + sequence)
                        outcome = classify(result)
                        assert outcome["outcome"] == "committed", outcome
                        campaign.event("publication_committed", producer=11, epoch=0,
                                       sequence=sequence, message_id=1000 + sequence,
                                       digest=digest(payload),
                                       incarnation=outcome["incarnation"],
                                       offset=outcome["offset"])
                    broker.stop()
                    staged = Broker(root, fault=fault, sequence=3, **segments).start()
                    payload = payload_for(47, 3)
                    campaign.event("publication_attempt", producer=11, epoch=0,
                                   sequence=3, message_id=1003, digest=digest(payload))
                    result = publish(staged, 3, seed=47, message=1003)
                    outcome = classify(result)
                    campaign.event("wal_fault", fault=fault, outcome=outcome)
                    staged.close()
                    broker = Broker(root, **segments).start()
                    history = read_history(broker)
                    for item in history:
                        campaign.event("recovered_record", **item)
                    reconcile = oracle.reconcile_publications(
                        campaign.prospective(), history)
                    segment_files = sorted(broker.wal.glob("segment-*.wal"))
                    if reconcile["verdict"] != oracle.PASS:
                        entry = campaign.conclude(
                            reconcile, fault=fault, segments=len(segment_files),
                            uncertain=outcome["outcome"] == "uncertain")
                        assert_pass(entry)
                    assert len(segment_files) > 1, (
                        fault, [p.name for p in segment_files])
                    # The same producer token is safe to retry after restart.
                    retry = publish(broker, 3, seed=47, message=1003)
                    retry_outcome = classify(retry)
                    campaign.event("retry", sequence=3, outcome=retry_outcome)
                    assert retry_outcome["outcome"] == "committed", retry_outcome
                    broker.stop()
                    entry = campaign.conclude(
                        reconcile, fault=fault, segments=len(segment_files),
                        uncertain=outcome["outcome"] == "uncertain")
                    assert_pass(entry)

    def test_generation_claim_crash_never_activates_an_unsynced_fence(self):
        campaign = Campaign("generation-claim")
        with tempfile.TemporaryDirectory(prefix="rovqet-b07-claim-") as temporary:
            root = Path(temporary)
            broker = Broker(root).start()
            assert classify(publish(broker, 0, seed=49))["outcome"] == "committed"
            broker.stop()
            staged = Broker(root, stage="checkpoint-after-append-before-sync",
                            sequence=0).start()
            pending = subprocess.Popen(
                [str(CONSUME), "durable", "1", "3", "0", "42", "9", "0",
                 "8", "65536", "0", "0"],
                env=staged.environment(), stdout=subprocess.PIPE,
                stderr=subprocess.PIPE, text=True)
            staged.barrier("checkpoint-after-append-before-sync")
            campaign.event("fault", kind="process_kill", at="generation-claim")
            staged.kill()
            pending.communicate(timeout=20)
            broker = Broker(root).start()
            claimed = consume(broker, ack_count=0)
            assert claimed["returncode"] == 0, claimed
            # The unsynced fence was discarded: the first durable claim is generation 1.
            campaign.event("generation_observed", group=9,
                           generation=claimed["batch"]["generation"])
            assert claimed["batch"]["generation"] == 1, claimed
            history = read_history(broker)
            for item in history:
                campaign.event("recovered_record", **item)
            broker.stop()
        campaign.journal.conclusion(oracle.PASS, reason=None,
                                    counts={"generation": 1, "records": len(history)})
        RESULTS.append({"run": campaign.name, "verdict": oracle.PASS, "reasons": [],
                        "counts": {"generation": 1, "records": len(history)}})
        campaign.journal.close()

    def test_corrupt_and_missing_storage_fail_closed(self):
        campaign = Campaign("storage-fail-closed")
        with tempfile.TemporaryDirectory(prefix="rovqet-b07-storage-") as temporary:
            root = Path(temporary)
            broker = Broker(root).start()
            assert classify(publish(broker, 0, seed=48))["outcome"] == "committed"
            broker.stop()
            segment = sorted(broker.wal.glob("segment-*.wal"))[0]
            data = bytearray(segment.read_bytes())
            data[70] ^= 0xFF  # inside the first committed record
            segment.write_bytes(data)
            corrupt = Broker(root)
            diagnostics = corrupt.reject_startup()
            campaign.event("corruption_rejected", diagnostics=diagnostics[:400])
            assert "recovery failed" in diagnostics
            assert segment.exists()
            # An absent storage path fails closed and creates nothing.
            absent_root = root / "absent-root"
            absent_root.mkdir()
            absent_path = absent_root / "never-created"
            missing = Broker(absent_root, data_directory=absent_path)
            diagnostics = missing.reject_startup()
            campaign.event("missing_storage_rejected", diagnostics=diagnostics[:400])
            assert not absent_path.exists(), "failed startup must not initialize storage"
            # An existing but empty replacement directory is REPLAY_NOT_AVAILABLE, not a
            # silently initialized new incarnation.
            replacement_root = root / "replacement"
            replacement_root.mkdir()
            empty_wal = replacement_root / "wal"
            empty_wal.mkdir()
            replacement = Broker(replacement_root, data_directory=empty_wal)
            diagnostics = replacement.reject_startup()
            campaign.event("empty_replacement_rejected", diagnostics=diagnostics[:400])
            assert "REPLAY_NOT_AVAILABLE" in diagnostics
            assert list(empty_wal.iterdir()) == []
        campaign.journal.conclusion(
            oracle.PASS, reason=None,
            counts={"corruption": 1, "missing": 1})
        RESULTS.append({"run": campaign.name, "verdict": oracle.PASS,
                        "reasons": [], "counts": {"corruption": 1, "missing": 1}})
        campaign.journal.close()

    def test_q0_isolation_fixture(self):
        campaign = Campaign("q0-isolation")
        result = subprocess.run(
            [sys.executable, str(ROOT / "scripts/test_q1_faults.py"),
             "Q1ProcessFaults.test_q0_publishes_while_q1_waits_for_storage_barrier"],
            cwd=ROOT, capture_output=True, text=True, timeout=120)
        campaign.event("q0_isolation", returncode=result.returncode,
                       output=(result.stdout + result.stderr)[-500:])
        verdict = oracle.PASS if result.returncode == 0 else oracle.FAIL
        campaign.journal.conclusion(verdict, reason=None, counts={"fixtures": 1})
        RESULTS.append({"run": campaign.name, "verdict": verdict,
                        "reasons": [] if verdict == oracle.PASS else ["fixture failed"],
                        "counts": {"fixtures": 1}})
        campaign.journal.close()
        self.assertEqual(verdict, oracle.PASS)

    def test_zz_measurements(self):
        measurements = {"schema": "rovqet-q1-durability-measurements-v1",
                        "source_sha": subprocess.run(
                            ["git", "rev-parse", "HEAD"], cwd=ROOT,
                            capture_output=True, text=True).stdout.strip(),
                        "machine": machine()}
        with tempfile.TemporaryDirectory(prefix="rovqet-b07-measure-") as temporary:
            root = Path(temporary)
            broker = Broker(root).start()
            sampler = Sampler(broker.process.pid)
            publish_measure = run_json([
                str(MEASURE), "publish", "durable", "1", "3", "0", "42",
                "11", "0", "0", "128", "200"], broker.environment())
            sampler.stop()
            measurements["publish"] = publish_measure
            measurements["broker_peak_rss_kib"] = sampler.peak_rss_kib
            measurements["broker_cpu_jiffies"] = sampler.cpu_jiffies()
            consume_measure = run_json([
                str(MEASURE), "consume", "durable", "1", "3", "0", "42",
                "9", "0", "200", "0"], broker.environment())
            measurements["consume"] = consume_measure
            read_measure = run_json([
                str(MEASURE), "read", "durable", "1", "3", "0", "42", "0",
                "256", "65536"], broker.environment())
            measurements["read"] = read_measure
            broker.stop()
            metrics = [line for line in broker.lines if line.startswith("BROKER_METRICS")]
            measurements["final_metrics"] = metrics[-1] if metrics else "UNAVAILABLE"
        with tempfile.TemporaryDirectory(prefix="rovqet-b07-fsync-") as directory:
            fsync = run_json([str(FSYNC_PROFILE), directory, "200", "128"], os.environ.copy())
            measurements["wal_fsync"] = fsync
        measurements["recovery_seconds"] = RECOVERY_SAMPLES
        measurements["unavailable"] = {
            "q1_queue_utilization": "UNAVAILABLE: no Q1 queue metric endpoint",
            "active_producers_and_outcomes": "UNAVAILABLE: internal actor state",
            "active_groups_and_leases": "UNAVAILABLE: internal actor state",
            "sparse_ack_entries": "UNAVAILABLE: internal actor state",
            "p99_9_interpretation": "n=200 nearest-rank; p99.9 is not statistically stable",
        }
        path = EVIDENCE / "measurements.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(measurements, indent=2, sort_keys=True) + "\n")
        RESULTS.append({"run": "measurements", "verdict": oracle.PASS, "reasons": [],
                        "counts": {"publish": 200, "consume": 200}})

    def test_zz_write_matrix(self):
        EVIDENCE.mkdir(parents=True, exist_ok=True)
        payload = {
            "schema": "rovqet-q1-durability-matrix-v1",
            "source_sha": subprocess.run(["git", "rev-parse", "HEAD"], cwd=ROOT,
                                         capture_output=True, text=True).stdout.strip(),
            "generated_at_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "runs": RESULTS,
        }
        (EVIDENCE / "matrix.json").write_text(
            json.dumps(payload, indent=2, sort_keys=True) + "\n")
        for entry in RESULTS:
            self.assertIn(entry["verdict"], (oracle.PASS, oracle.NOT_RUN), entry)


class Sampler:
    def __init__(self, pid: int):
        self.pid = pid
        self.peak_rss_kib = 0
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._sample, daemon=True)
        self._initial_cpu = self.cpu_jiffies()
        self._thread.start()

    def _sample(self) -> None:
        while not self._stop.is_set():
            try:
                status = Path(f"/proc/{self.pid}/status").read_text()
                for line in status.splitlines():
                    if line.startswith("VmRSS:"):
                        self.peak_rss_kib = max(
                            self.peak_rss_kib, int(line.split()[1]))
            except OSError:
                return
            self._stop.wait(0.02)

    def cpu_jiffies(self) -> int:
        try:
            fields = Path(f"/proc/{self.pid}/stat").read_text().split()
            return int(fields[13]) + int(fields[14])
        except (OSError, IndexError):
            return 0

    def stop(self) -> None:
        self._stop.set()
        self._thread.join(timeout=2)


def run_json(command: list[str], environment: dict) -> dict:
    result = subprocess.run(command, env=environment, capture_output=True,
                            text=True, timeout=300)
    assert result.returncode == 0, (command, result.stdout, result.stderr)
    return json.loads(result.stdout.strip().splitlines()[-1])


def machine() -> dict:
    release = {}
    try:
        for line in Path("/etc/os-release").read_text().splitlines():
            if "=" in line:
                key, value = line.split("=", 1)
                release[key] = value.strip('"')
    except OSError:
        pass
    filesystem = "UNAVAILABLE"
    try:
        mounts = Path("/proc/mounts").read_text().splitlines()
        target = str(ROOT)
        best = ""
        for line in mounts:
            parts = line.split()
            if len(parts) >= 3 and target.startswith(parts[1]) and len(parts[1]) > len(best):
                best = parts[1]
                filesystem = parts[2]
    except OSError:
        pass
    return {
        "platform": platform.platform(),
        "kernel": platform.release(),
        "machine": platform.machine(),
        "python": sys.version.split()[0],
        "rustc": subprocess.run(["rustc", "--version"], capture_output=True,
                                text=True).stdout.strip(),
        "os_release": release,
        "filesystem": filesystem,
        "cpu_count": os.cpu_count(),
    }


if __name__ == "__main__":
    unittest.main(verbosity=2)
