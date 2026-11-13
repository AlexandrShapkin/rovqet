#!/usr/bin/env python3
"""B-06 external NACK, retry, quarantine and real Q1 DLQ verification.

Uses real TCP SDK consumer processes, an independently fsynced parent journal, and the
deterministic fault barriers for terminal/DLQ crash ordering. SIGKILL is process-crash
recovery, not physical power-loss certification. Disposable storage only.
"""
from __future__ import annotations

import hashlib
import json
import re
import struct
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path

import q1_oracle as oracle
from q1_smoke import Broker
from test_q1_faults import connect, frame, read_frame

ROOT = Path(__file__).resolve().parents[1]
NACK = ROOT / "target/debug/examples/nack_q1"
CONSUME = ROOT / "target/debug/examples/consume_q1"
READ = ROOT / "target/debug/examples/read_q1"
EVIDENCE = ROOT / "benchmarks/evidence" / (
    "q1-terminal-" + subprocess.run(
        ["git", "rev-parse", "--short", "HEAD"], cwd=ROOT,
        capture_output=True, text=True,
    ).stdout.strip()
)
RESULTS: list[dict] = []
DLQ_TOPIC = "durable-dlq"
DLQ_TOPIC_ID = 4
DLQ_INCARNATION = 77


def extra(policy: str, attempts: int, initial_ms: int, dlq: Path | None) -> list[str]:
    arguments = [
        "--q1-terminal-policy", policy,
        "--q1-max-attempts", str(attempts),
        "--q1-retry-initial-ms", str(initial_ms),
        "--q1-retry-max-ms", str(max(initial_ms, 250)),
    ]
    if dlq is not None:
        arguments += [
            "--q1-dlq-data-dir", str(dlq),
            "--q1-dlq-topic", DLQ_TOPIC,
            "--q1-dlq-topic-id", str(DLQ_TOPIC_ID),
            "--q1-dlq-partition-id", "0",
            "--q1-dlq-incarnation", str(DLQ_INCARNATION),
            "--q1-dlq-owner-epoch", "7",
        ]
    return arguments


def parse_nack(stdout: str) -> dict:
    deliveries, batch, results, errors = [], None, [], []
    for line in stdout.splitlines():
        parts = line.split()
        if not parts:
            continue
        if parts[0] == "DELIVERY":
            deliveries.append({
                "incarnation": int(parts[1]), "offset": int(parts[2]),
                "message_id": int(parts[3]), "generation": int(parts[4]),
                "lease": int(parts[5]), "attempt": int(parts[6]),
                "payload": bytes.fromhex(parts[8]),
            })
        elif parts[0] == "GROUP_BATCH":
            batch = {"group": int(parts[1]), "generation": int(parts[2]),
                     "next": int(parts[3]), "has_more": int(parts[4]),
                     "end": int(parts[5])}
        elif parts[0] == "NACK_RESULT":
            results.append({"offset": int(parts[1]), "code": int(parts[2]),
                            "retry_after_ms": int(parts[3]),
                            "disposition_id": int(parts[4]),
                            "generation": int(parts[5])})
        elif parts[0] == "NACK_ERROR":
            errors.append(line)
    return {"deliveries": deliveries, "batch": batch, "results": results,
            "errors": errors}


def run_nack(broker: Broker, *, group: int = 9, generation: int = 0,
             count: int = 8, size: int = 65536, wait: int = 0,
             action: str = "nack", reason: int = 1, timeout: int = 30) -> dict:
    result = subprocess.run(
        [str(NACK), "durable", "1", "3", "0", "42", str(group), str(generation),
         str(count), str(size), str(wait), action, str(reason)],
        env=broker.environment(), capture_output=True, text=True, timeout=timeout)
    parsed = parse_nack(result.stdout)
    parsed["returncode"] = result.returncode
    parsed["stderr"] = result.stderr
    return parsed


def read_topic(broker: Broker, topic: str, topic_id: int, incarnation: int) -> list[dict]:
    result = subprocess.run(
        [str(READ), topic, "1", str(topic_id), "0", str(incarnation), "0", "64",
         "65536", "0"], env=broker.environment(), capture_output=True, text=True,
        timeout=30)
    assert result.returncode == 0, (result.stdout, result.stderr)
    records = []
    for line in result.stdout.splitlines():
        parts = line.split()
        if parts[0] == "READ_RECORD":
            records.append({"incarnation": int(parts[1]), "offset": int(parts[2]),
                            "message_id": int(parts[3]),
                            "payload": bytes.fromhex(parts[4]),
                            "digest": hashlib.sha256(bytes.fromhex(parts[4])).hexdigest()})
    return records


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


def consume_cursor(broker: Broker, generation: int = 0) -> dict:
    result = subprocess.run(
        [str(CONSUME), "durable", "1", "3", "0", "42", "9", str(generation),
         "8", "65536", "0", "0"], env=broker.environment(), capture_output=True,
        text=True, timeout=30)
    assert result.returncode == 0, (result.stdout, result.stderr)
    parsed = parse_nack(result.stdout)
    return {"generation": parsed["batch"]["generation"],
            "next": parsed["batch"]["next"],
            "deliveries": [delivery["offset"] for delivery in parsed["deliveries"]]}


def raw_nack(broker: Broker, token: dict) -> dict:
    fields = {
        9: b"durable", 19: (0).to_bytes(4, "big"),
        24: (9).to_bytes(8, "big"),
        25: token["generation"].to_bytes(8, "big"),
        26: token["lease"].to_bytes(16, "big"),
        27: (7).to_bytes(8, "big"),
        20: (42).to_bytes(16, "big"),
        21: token["offset"].to_bytes(8, "big"),
        37: token["attempt"].to_bytes(4, "big"),
        45: (1).to_bytes(2, "big"),
    }
    with connect(broker) as connection:
        connection.sendall(frame(15, 900, fields))
        kind, correlation, response = read_frame(connection)
        if kind == 20:
            return {"error": int.from_bytes(response[28], "big")}
        assert (kind, correlation) == (27, 900), (kind, correlation)
        return {"outcome": int.from_bytes(response[51], "big"),
                "disposition": int.from_bytes(response[38], "big")}


class Campaign:
    def __init__(self, name: str):
        path = EVIDENCE / "journals" / f"{name}.jsonl"
        path.parent.mkdir(parents=True, exist_ok=True)
        self.journal = oracle.Journal(path, name)
        self.records: list[dict] = []

    def event(self, event: str, **fields) -> None:
        self.records.append(self.journal.append(event, **fields))

    def conclude(self, result: dict, **extra_fields) -> dict:
        self.journal.conclusion(result["verdict"], reason="; ".join(result["reasons"]) or None,
                                counts=result["counts"], **extra_fields)
        entry = {"run": self.journal.run_id, "verdict": result["verdict"],
                 "reasons": result["reasons"], "counts": result["counts"], **extra_fields}
        RESULTS.append(entry)
        self.journal.close()
        return entry


class TerminalDisposition(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix="rovqet-b06-")
        self.root = Path(self.temporary.name)
        self.dlq = self.root / "dlq"
        self.brokers: list[Broker] = []

    def tearDown(self):
        for broker in self.brokers:
            broker.close()
        self.temporary.cleanup()

    def broker(self, policy: str = "none", attempts: int = 8, initial: int = 100,
               *, dlq: bool = False, stage: str | None = None,
               sequence: int = 0) -> Broker:
        (self.root / "main").mkdir(parents=True, exist_ok=True)
        broker = Broker(
            self.root / "main",
            stage=stage,
            sequence=sequence,
            extra=extra(policy, attempts, initial, self.dlq if dlq else None),
        ).start()
        self.brokers.append(broker)
        return broker

    def publish(self, broker: Broker, sequence: int) -> None:
        result = broker.publish(sequence)
        assert result.returncode == 0, (result.stdout, result.stderr)

    def record_deliveries(self, campaign: Campaign, parsed: dict, phase: str) -> None:
        for delivery in parsed["deliveries"]:
            campaign.event("delivery", group=9, generation=delivery["generation"],
                           offset=delivery["offset"], attempt=delivery["attempt"],
                           message_id=delivery["message_id"], phase=phase)

    def test_external_nack_schedules_backoff_and_redelivery(self):
        campaign = Campaign("nack-backoff")
        broker = self.broker(attempts=5, initial=120)
        self.publish(broker, 0)
        first = run_nack(broker)
        self.assertEqual(first["returncode"], 0, first)
        self.record_deliveries(campaign, first, "first")
        result = first["results"][0]
        self.assertEqual(result["code"], 1)
        self.assertGreaterEqual(result["retry_after_ms"], 1)
        campaign.event("nack_request", offset=0, attempt=1)
        campaign.event("retry_scheduled", offset=0, retry_after_ms=result["retry_after_ms"])
        generation = first["batch"]["generation"]
        early = consume_cursor(broker, generation)
        self.assertEqual(early["deliveries"], [], "backoff must gate redelivery")
        time.sleep(0.3)
        second = run_nack(broker, generation=generation)
        self.assertEqual(second["returncode"], 0, second)
        self.record_deliveries(campaign, second, "redelivery")
        self.assertEqual(second["deliveries"][0]["attempt"], 2)
        campaign.event("generation_observed", group=9, generation=generation)
        entry = campaign.conclude(
            oracle.reconcile_consumers(
                campaign.records + [{"event": "conclusion"}],
                {"delivered_offsets": [], "cursor": 0}, [0]))
        self.assertEqual(entry["verdict"], oracle.PASS, entry)

    def test_exhaustion_without_policy_never_closes(self):
        campaign = Campaign("nack-exhaustion-none")
        broker = self.broker(attempts=2, initial=10)
        self.publish(broker, 0)
        first = run_nack(broker)
        self.record_deliveries(campaign, first, "first")
        campaign.event("nack_request", offset=0, attempt=1)
        self.assertEqual(first["results"][0]["code"], 1)
        generation = first["batch"]["generation"]
        time.sleep(0.1)
        second = run_nack(broker, generation=generation)
        self.record_deliveries(campaign, second, "redelivery")
        campaign.event("nack_request", offset=0, attempt=2)
        self.assertEqual(second["results"][0]["code"], 2, second)
        campaign.event("generation_observed", group=9, generation=generation)
        stalled = consume_cursor(broker, generation)
        self.assertEqual(stalled["deliveries"], [])
        self.assertEqual(stalled["next"], 0)
        broker.stop()
        restarted = self.broker(attempts=2, initial=10)
        recovered = consume_cursor(restarted)
        self.assertEqual(recovered["deliveries"], [])
        self.assertEqual(recovered["next"], 0)
        self.assertEqual(len(read_topic(restarted, "durable", 3, 42)), 1)
        entry = campaign.conclude(
            oracle.reconcile_consumers(
                campaign.records + [{"event": "conclusion"}],
                {"delivered_offsets": [], "cursor": recovered["next"]}, [0]))
        self.assertEqual(entry["verdict"], oracle.PASS, entry)

    def test_quarantine_commit_is_durable_and_restart_stable(self):
        campaign = Campaign("quarantine")
        broker = self.broker("quarantine", attempts=1)
        self.publish(broker, 0)
        self.publish(broker, 1)
        result = run_nack(broker)
        self.record_deliveries(campaign, result, "first")
        terminal = result["results"][0]
        self.assertEqual(terminal["code"], 3, result)
        disposition = terminal["disposition_id"]
        self.assertNotEqual(disposition, 0)
        campaign.event("terminal_committed", group=9, kind="quarantine", offset=0,
                       disposition_id=disposition, original_message_id=100,
                       source_incarnation=42)
        source = read_topic(broker, "durable", 3, 42)
        self.assertEqual(len(source), 2, "quarantine must retain the source publication")
        generation = result["batch"]["generation"]
        after = consume_cursor(broker, generation)
        self.assertNotIn(0, after["deliveries"])
        self.assertGreater(after["next"], 0)
        broker.stop()
        restarted = self.broker("quarantine", attempts=1)
        recovered = consume_cursor(restarted)
        self.assertNotIn(0, recovered["deliveries"])
        campaign.event("generation_observed", group=9, generation=recovered["generation"])
        entry = campaign.conclude(
            oracle.reconcile_terminal(
                campaign.records + [{"event": "conclusion"}], source, [],
                recovered["next"]))
        self.assertEqual(entry["verdict"], oracle.PASS, entry)

    def test_real_dlq_is_readable_and_idempotent_after_restart(self):
        campaign = Campaign("dlq")
        broker = self.broker("dlq", attempts=1, dlq=True)
        self.publish(broker, 0)
        self.publish(broker, 1)
        result = run_nack(broker, action="nack-first")
        self.record_deliveries(campaign, result, "first")
        for ack in result["results"][1:]:
            campaign.event("ack_committed", group=9, offset=ack["offset"])
        terminal = result["results"][0]
        self.assertEqual(terminal["code"], 3, result)
        disposition = terminal["disposition_id"]
        dlq = read_topic(broker, DLQ_TOPIC, DLQ_TOPIC_ID, DLQ_INCARNATION)
        self.assertEqual(len(dlq), 1)
        self.assertEqual(dlq[0]["message_id"], disposition)
        metadata = parse_dlq_payload(dlq[0]["payload"])
        self.assertEqual(metadata["disposition_id"], disposition)
        self.assertEqual(metadata["original_message_id"], 100)
        self.assertEqual(metadata["source_offset"], 0)
        self.assertEqual(metadata["payload"], b"event-0")
        campaign.event("dlq_publication", disposition_id=disposition,
                       incarnation=dlq[0]["incarnation"], offset=dlq[0]["offset"])
        campaign.event("terminal_committed", group=9, kind="dlq", offset=0,
                       disposition_id=disposition, original_message_id=100,
                       source_incarnation=42,
                       dlq_incarnation=dlq[0]["incarnation"],
                       dlq_offset=dlq[0]["offset"],
                       payload_digest=hashlib.sha256(metadata["payload"]).hexdigest())
        source = read_topic(broker, "durable", 3, 42)
        self.assertEqual(len(source), 2)
        generation = result["batch"]["generation"]
        token = result["deliveries"][0]
        broker.stop()
        restarted = self.broker("dlq", attempts=1, dlq=True)
        # A repeated terminal token is a read-only saved outcome with no new DLQ message.
        duplicate = raw_nack(restarted, token)
        self.assertEqual(duplicate["outcome"], 5, duplicate)
        self.assertEqual(duplicate["disposition"], disposition)
        self.assertEqual(len(read_topic(restarted, DLQ_TOPIC, DLQ_TOPIC_ID,
                                        DLQ_INCARNATION)), 1)
        recovered = consume_cursor(restarted)
        self.assertNotIn(0, recovered["deliveries"])
        campaign.event("generation_observed", group=9, generation=recovered["generation"])
        entry = campaign.conclude(
            oracle.reconcile_terminal(
                campaign.records + [{"event": "conclusion"}], source,
                [{"incarnation": dlq[0]["incarnation"], "offset": dlq[0]["offset"],
                  "message_id": disposition,
                  "digest": hashlib.sha256(metadata["payload"]).hexdigest()}],
                recovered["next"]))
        self.assertEqual(entry["verdict"], oracle.PASS, entry)

    def crash_dlq(self, stage: str, expected_dlq: int) -> dict:
        campaign = Campaign(f"dlq-{stage}")
        broker = self.broker("dlq", attempts=1, dlq=True)
        self.publish(broker, 0)
        self.publish(broker, 1)
        broker.stop()
        staged = self.broker("dlq", attempts=1, dlq=True, stage=stage, sequence=0)
        pending = subprocess.Popen(
            [str(NACK), "durable", "1", "3", "0", "42", "9", "0", "8", "65536",
             "0", "nack", "1"],
            env=staged.environment(), stdout=subprocess.PIPE,
            stderr=subprocess.PIPE, text=True)
        staged.barrier(stage)
        campaign.event("fault", kind="process_kill", at=stage)
        staged.kill()
        pending.communicate(timeout=20)
        restarted = self.broker("dlq", attempts=1, dlq=True)
        source = read_topic(restarted, "durable", 3, 42)
        dlq = read_topic(restarted, DLQ_TOPIC, DLQ_TOPIC_ID, DLQ_INCARNATION)
        self.assertEqual(len(source), 2)
        self.assertEqual(len(dlq), expected_dlq, (stage, dlq))
        metadata = parse_dlq_payload(dlq[0]["payload"])
        disposition = dlq[0]["message_id"]
        campaign.event("dlq_publication", disposition_id=disposition,
                       incarnation=dlq[0]["incarnation"], offset=dlq[0]["offset"])
        campaign.event("terminal_committed", group=9, kind="dlq", offset=0,
                       disposition_id=disposition,
                       original_message_id=metadata["original_message_id"],
                       source_incarnation=42,
                       dlq_incarnation=dlq[0]["incarnation"],
                       dlq_offset=dlq[0]["offset"],
                       payload_digest=hashlib.sha256(metadata["payload"]).hexdigest())
        consumer = consume_cursor(restarted)
        self.assertNotIn(0, consumer["deliveries"])
        self.assertGreater(consumer["next"], 0)
        campaign.event("generation_observed", group=9, generation=consumer["generation"])
        entry = campaign.conclude(
            oracle.reconcile_terminal(
                campaign.records + [{"event": "conclusion"}], source,
                [{"incarnation": dlq[0]["incarnation"], "offset": dlq[0]["offset"],
                  "message_id": disposition,
                  "digest": hashlib.sha256(metadata["payload"]).hexdigest()}],
                consumer["next"]),
            fault=stage)
        self.assertEqual(entry["verdict"], oracle.PASS, entry)
        return entry

    def test_dlq_crash_after_pending_before_publication(self):
        self.crash_dlq("terminal-pending-after-sync", 1)

    def test_dlq_crash_after_publication_before_terminal(self):
        self.crash_dlq("dlq-after-publish-before-terminal", 1)

    def test_dlq_crash_terminal_append_before_sync(self):
        self.crash_dlq("terminal-commit-after-append-before-sync", 1)

    def test_dlq_crash_terminal_after_sync_before_reply(self):
        self.crash_dlq("terminal-commit-after-sync-before-reply", 1)

    def test_quarantine_crash_after_pending_sync_recovers_closure(self):
        campaign = Campaign("quarantine-pending-crash")
        broker = self.broker("quarantine", attempts=1)
        self.publish(broker, 0)
        broker.stop()
        staged = self.broker("quarantine", attempts=1, stage="terminal-pending-after-sync",
                             sequence=0)
        pending = subprocess.Popen(
            [str(NACK), "durable", "1", "3", "0", "42", "9", "0", "8", "65536",
             "0", "nack", "1"],
            env=staged.environment(), stdout=subprocess.PIPE,
            stderr=subprocess.PIPE, text=True)
        staged.barrier("terminal-pending-after-sync")
        campaign.event("fault", kind="process_kill", at="terminal-pending-after-sync")
        staged.kill()
        pending.communicate(timeout=20)
        restarted = self.broker("quarantine", attempts=1)
        source = read_topic(restarted, "durable", 3, 42)
        consumer = consume_cursor(restarted)
        self.assertNotIn(0, consumer["deliveries"])
        campaign.event("terminal_committed", group=9, kind="quarantine", offset=0,
                       disposition_id=1, original_message_id=100,
                       source_incarnation=42)
        campaign.event("generation_observed", group=9, generation=consumer["generation"])
        entry = campaign.conclude(
            oracle.reconcile_terminal(
                campaign.records + [{"event": "conclusion"}], source, [],
                consumer["next"]), fault="terminal-pending-after-sync")
        self.assertEqual(entry["verdict"], oracle.PASS, entry)

    def test_quarantine_crash_before_pending_fsync_leaves_source_incomplete(self):
        campaign = Campaign("quarantine-before-fsync")
        broker = self.broker("quarantine", attempts=1)
        self.publish(broker, 0)
        broker.stop()
        staged = self.broker(
            "quarantine", attempts=1,
            stage="terminal-pending-after-append-before-sync", sequence=0
        )
        pending = subprocess.Popen(
            [str(NACK), "durable", "1", "3", "0", "42", "9", "0", "8", "65536",
             "0", "nack", "1"],
            env=staged.environment(), stdout=subprocess.PIPE,
            stderr=subprocess.PIPE, text=True)
        staged.barrier("terminal-pending-after-append-before-sync")
        campaign.event("fault", kind="process_kill",
                       at="terminal-pending-after-append-before-sync")
        staged.kill()
        pending.communicate(timeout=20)
        restarted = self.broker("quarantine", attempts=1)
        source = read_topic(restarted, "durable", 3, 42)
        self.assertEqual(len(source), 1)
        consumer = consume_cursor(restarted)
        # No durable terminal record exists: the source remains eligible and incomplete.
        self.assertIn(0, consumer["deliveries"])
        self.assertEqual(consumer["next"], 0)
        campaign.event("delivery", group=9, generation=consumer["generation"],
                       offset=0, attempt=1)
        campaign.event("generation_observed", group=9, generation=consumer["generation"])
        entry = campaign.conclude(
            oracle.reconcile_consumers(
                campaign.records + [{"event": "conclusion"}],
                {"delivered_offsets": consumer["deliveries"],
                 "cursor": consumer["next"]}, [0]),
            fault="terminal-pending-after-append-before-sync")
        self.assertEqual(entry["verdict"], oracle.PASS, entry)

    def test_stale_generation_cannot_create_a_disposition(self):
        campaign = Campaign("stale-generation")
        broker = self.broker("none", attempts=4)
        self.publish(broker, 0)
        self.publish(broker, 1)
        first = run_nack(broker, action="none", count=2)
        token = first["deliveries"][1]
        old_generation = first["batch"]["generation"]
        broker.stop()
        restarted = self.broker("none", attempts=4)
        claim = consume_cursor(restarted)
        self.assertGreater(claim["generation"], old_generation)
        response = raw_nack(restarted, token)
        self.assertEqual(response.get("error"), 18, response)
        campaign.event("stale_generation_attempt", generation=old_generation,
                       outcome="rejected")
        campaign.journal.conclusion(oracle.PASS, reason=None,
                                    counts={"stale_extensions": 0})
        RESULTS.append({"run": campaign.journal.run_id, "verdict": oracle.PASS,
                        "reasons": [], "counts": {"stale_extensions": 0}})
        campaign.journal.close()

    def test_zz_write_matrix(self):
        EVIDENCE.mkdir(parents=True, exist_ok=True)
        payload = {
            "schema": "rovqet-q1-terminal-matrix-v1",
            "source_sha": subprocess.run(["git", "rev-parse", "HEAD"], cwd=ROOT,
                                         capture_output=True, text=True).stdout.strip(),
            "generated_at_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "runs": RESULTS,
        }
        (EVIDENCE / "matrix.json").write_text(
            json.dumps(payload, indent=2, sort_keys=True) + "\n")
        for entry in RESULTS:
            self.assertEqual(entry["verdict"], oracle.PASS, entry)


if __name__ == "__main__":
    unittest.main(verbosity=2)
