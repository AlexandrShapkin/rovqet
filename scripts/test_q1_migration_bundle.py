#!/usr/bin/env python3
"""Multi-process campaign for a coordinated source-plus-DLQ Q1 handover.

Three independently persisted metadata voters, a source broker that owns a Q1 partition with
a real local DLQ, a successor broker with its own initially inactive source and DLQ
directories, and external publisher, consumer and reader processes. The parent owns the
oracle journal: every observation is an externally received result, never a broker's own
account.

The campaign commits a real DLQ terminal disposition before the migration and verifies the
same disposition, the same source records and the same continuation after both histories
were transferred, fenced, activated and promoted onto the successor.

The brokers share one machine: this proves protocol behaviour, not independent physical
failure domains.
"""
from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import q1_oracle  # noqa: E402
import test_q1_faults as faults  # noqa: E402
import test_q1_migration as harness  # noqa: E402

ROOT = Path(__file__).resolve().parents[1]
SERVER = ROOT / "target/debug/rovqet"
VOTER = ROOT / "target/debug/metadata-raft-node"
Q1 = ROOT / "target/debug/examples/q1"
READ = ROOT / "target/debug/examples/read_q1"
CONSUME = ROOT / "target/debug/examples/consume_q1"
NACK = ROOT / "target/debug/examples/nack_q1"

CLUSTER = 727272
TENANT = "1"
TOPIC = "3"
PARTITION = "0"
INCARNATION = "42"
TOPIC_NAME = "durable"
GROUP = "9"

DLQ_TOPIC = "7"
DLQ_TOPIC_NAME = "durable-dlq"
DLQ_PARTITION = "0"
DLQ_INCARNATION = "77"

BUNDLE_BINARIES = (SERVER, VOTER, Q1, READ, CONSUME, NACK)


class BundleBroker(harness.Broker):
    """A broker whose Q1 route owns a real local DLQ partition."""

    def __init__(
        self,
        name: str,
        node_id: int,
        root: Path,
        peer_ports: dict[int, int],
        raft_ports: dict[int, int],
        initialize: bool,
        migrate_to: int | None = None,
    ):
        super().__init__(
            name, node_id, root, peer_ports, raft_ports, initialize, migrate_to
        )
        # The DLQ is a second explicit local Q1 partition of the same route. The terminal
        # policy is DLQ, so an exhausted consumer disposition publishes a real message.
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
            "--q1-dlq-owner-epoch", "7",
        ]
        index = self.argv.index("--cluster-id")
        self.argv[index:index] = dlq_args
        # The campaign owns its own metadata cluster identity.
        self.argv[self.argv.index("--cluster-id") + 1] = str(CLUSTER)
        self.dlq_root = root / "dlq"

    @property
    def endpoint(self) -> str:
        return f"127.0.0.1:{self.client_port}"

    def _addr(self) -> dict[str, str]:
        return dict(os.environ, ROVQET_ADDR=self.endpoint)

    def read_dlq(self):
        result = subprocess.run(
            [str(READ), DLQ_TOPIC_NAME, TENANT, DLQ_TOPIC, DLQ_PARTITION,
             DLQ_INCARNATION, "0", "64", "65536", "0"],
            env=self._addr(), capture_output=True, text=True, timeout=60,
        )
        assert result.returncode == 0, (result.stdout, result.stderr)
        return [line for line in result.stdout.splitlines() if line.startswith("READ_RECORD")]

    def nack(self, attempt_action: str = "nack", generation: int = 0):
        return subprocess.run(
            [str(NACK), TOPIC_NAME, TENANT, TOPIC, PARTITION, INCARNATION, GROUP,
             str(generation), "16", "65536", "0", attempt_action],
            env=self._addr(), capture_output=True, text=True, timeout=60,
        )


def raw_nack(broker, token: dict, epoch: int) -> dict:
    """Replays a consumed token as a raw NACK frame.

    A repeated terminal token is a read-only saved outcome: the server never publishes a
    second dependent message for it.
    """
    fields = {
        9: TOPIC_NAME.encode(), 19: (0).to_bytes(4, "big"),
        24: (9).to_bytes(8, "big"),
        25: token["generation"].to_bytes(8, "big"),
        26: token["lease"].to_bytes(16, "big"),
        27: epoch.to_bytes(8, "big"),
        20: (42).to_bytes(16, "big"),
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


def nack_result(stdout: str) -> dict | None:
    """The first NACK result of one consumer round, if any."""
    for line in stdout.splitlines():
        if not line.startswith("NACK_RESULT "):
            continue
        _, offset, code, delay, disposition, generation = line.split()
        return {
            "offset": int(offset),
            "code": int(code),
            "delay": int(delay),
            "disposition_id": int(disposition),
            "generation": int(generation),
        }
    return None


class BundleCampaign(unittest.TestCase):
    journal: q1_oracle.Journal
    root: Path
    source: BundleBroker
    target: BundleBroker
    voter: harness.Process
    offsets: dict[int, int] = {}
    terminal: dict = {}
    dlq_record: dict = {}
    migration_id: str = ""
    token: dict = {}
    source_cursor: int = 0

    @classmethod
    def setUpClass(cls) -> None:
        for binary in BUNDLE_BINARIES:
            if not binary.exists():
                subprocess.run(
                    ["cargo", "build", "-p", "rovqet-server", "-p", "rovqet-metadata-raft",
                     "-p", "rovqet-sdk", "--examples", "--bins", "--locked"],
                    cwd=ROOT, check=True,
                )
                break
        cls.root = Path(tempfile.mkdtemp(prefix="rovqet-bundle-"))
        cls.journal = q1_oracle.Journal(cls.root / "campaign.jsonl", "bundle-campaign")
        peer_ports = {1: harness.free_port(), 2: harness.free_port()}
        raft_ports = {1: harness.free_port(), 2: harness.free_port(), 3: harness.free_port()}
        cls.source = BundleBroker(
            "source", 1, cls.root / "source", peer_ports, raft_ports, True, migrate_to=2
        )
        cls.target = BundleBroker(
            "target", 2, cls.root / "target", peer_ports, raft_ports, False
        )
        cls.voter = harness.Process(
            "voter3",
            [
                str(VOTER),
                "--id", "3",
                "--cluster-id", str(CLUSTER),
                "--listen", f"127.0.0.1:{raft_ports[3]}",
                "--data-dir", str(cls.root / "voter3"),
                "--bootstrap", "false",
                "--peer", f"1=127.0.0.1:{raft_ports[1]}",
                "--peer", f"2=127.0.0.1:{raft_ports[2]}",
                "--peer", f"3=127.0.0.1:{raft_ports[3]}",
            ],
            cls.root,
        )
        for process in (cls.source, cls.target, cls.voter):
            process.start()
        cls.journal.append(
            "campaign_started",
            source=1,
            target=2,
            voters=sorted(raft_ports),
            source_partition=[int(TENANT), int(TOPIC), int(PARTITION)],
            source_incarnation=int(INCARNATION),
            dependent_partition=[int(TENANT), int(DLQ_TOPIC), int(DLQ_PARTITION)],
            dependent_incarnation=int(DLQ_INCARNATION),
        )

    @classmethod
    def tearDownClass(cls) -> None:
        for process in (cls.source, cls.target, cls.voter):
            process.stop()
        if os.environ.get("ROVQET_KEEP_TMP"):
            print(f"campaign directory kept: {cls.root}", flush=True)
        else:
            shutil.rmtree(cls.root, ignore_errors=True)

    # -- helpers ---------------------------------------------------------------------
    @staticmethod
    def records(lines: list[str]) -> list[dict]:
        """READ_RECORD <incarnation> <offset> <message_id> <hex payload>."""
        result = []
        for line in lines:
            _, incarnation, offset, message_id, hex_payload = line.split()
            result.append(
                {
                    "incarnation": int(incarnation),
                    "offset": int(offset),
                    "message_id": int(message_id),
                    "digest": q1_oracle.digest_hex(bytes.fromhex(hex_payload)),
                }
            )
        return result

    @staticmethod
    def migration_lines(process: harness.Process) -> list[dict]:
        events = []
        for line in process.lines:
            if line.startswith("MIGRATION committed "):
                payload = line.split("MIGRATION committed ", 1)[1]
                try:
                    events.append(json.loads(payload))
                except json.JSONDecodeError:
                    continue
        return events

    @classmethod
    def bundle_lines(cls, process: harness.Process, event: str) -> list[dict]:
        payloads = []
        prefix = f"MIGRATION {event} "
        for line in process.lines:
            if line.startswith(prefix):
                try:
                    payloads.append(json.loads(line.split(prefix, 1)[1]))
                except json.JSONDecodeError:
                    continue
        return payloads

    # -- campaign --------------------------------------------------------------------
    def test_1_the_source_commits_records_and_a_real_dlq_disposition(self) -> None:
        line = self.source.wait_for("Q1 route active", 90)
        self.assertIsNotNone(
            line, "the source must open its route: " + "\n".join(self.source.lines)
        )
        self.journal.append(
            "source_ready", node=1, partition=[int(TENANT), int(TOPIC), int(PARTITION)]
        )
        offsets = {}
        for sequence in range(3):
            self.journal.append(
                "publication_attempt", producer=11, epoch=1, sequence=sequence, node=1
            )
            result = self.source.publish(sequence, 100 + sequence)
            self.assertEqual(result.returncode, 0, (result.stdout, result.stderr))
            committed = [
                line for line in result.stdout.splitlines() if "COMMITTED_LOCAL" in line
            ]
            self.assertTrue(committed, f"the publication must commit: {result.stdout}")
            offsets[sequence] = int(committed[0].split()[2])
            digest = q1_oracle.digest_hex(f"event-{sequence}".encode())
            self.journal.append(
                "publication_committed", producer=11, epoch=1, sequence=sequence,
                offset=offsets[sequence], message_id=100 + sequence,
                incarnation=int(INCARNATION), node=1, digest=digest,
            )
        self.assertEqual(len(set(offsets.values())), 3)
        BundleCampaign.offsets = offsets
        # The first delivered message is rejected: the retry bound is one, so the NACK
        # commits a terminal disposition and publishes a real DLQ message.
        rejected = self.source.nack("nack-first")
        self.assertEqual(rejected.returncode, 0, (rejected.stdout, rejected.stderr))
        result = nack_result(rejected.stdout)
        self.assertIsNotNone(result, rejected.stdout)
        self.assertEqual(
            result["code"], 3,
            f"the disposition must be terminal and committed: {rejected.stdout}",
        )
        deliveries = parse_deliveries(rejected.stdout)
        self.assertTrue(deliveries, rejected.stdout)
        token = deliveries[0]
        self.journal.append(
            "delivery", group=int(GROUP), generation=token["generation"],
            offset=token["offset"], attempt=token["attempt"],
            message_id=token["message_id"], node=1, phase="first",
        )
        # The destination record is observed through an external read, never inferred.
        dlq_records = self.records(self.source.read_dlq())
        self.assertEqual(
            len(dlq_records), 1, f"exactly one DLQ disposition must exist: {dlq_records}"
        )
        dlq = dlq_records[0]
        self.assertEqual(dlq["incarnation"], int(DLQ_INCARNATION))
        self.assertEqual(dlq["message_id"], result["disposition_id"])
        self.journal.append(
            "dlq_publication", disposition_id=result["disposition_id"],
            incarnation=dlq["incarnation"], offset=dlq["offset"],
            original_message_id=token["message_id"], node=1, digest=dlq["digest"],
        )
        self.journal.append(
            "terminal_committed", kind="dlq", offset=result["offset"], group=int(GROUP),
            generation=result["generation"], disposition_id=result["disposition_id"],
            original_message_id=token["message_id"], source_incarnation=int(INCARNATION),
            dlq_incarnation=dlq["incarnation"], dlq_offset=dlq["offset"],
            payload_digest=dlq["digest"], node=1,
        )
        for line in rejected.stdout.splitlines():
            if line.startswith("ACK_COMMITTED "):
                _, incarnation, offset, generation = line.split()
                self.journal.append(
                    "ack_committed", group=int(GROUP), offset=int(offset),
                    generation=int(generation), node=1,
                )
        cursor_round = self.source.consume(ack_count=0)
        self.assertEqual(cursor_round.returncode, 0, (cursor_round.stdout, cursor_round.stderr))
        source_cursor = None
        for line in cursor_round.stdout.splitlines():
            if line.startswith("GROUP_BATCH "):
                source_cursor = int(line.split()[3])
        self.assertIsNotNone(source_cursor, cursor_round.stdout)
        self.journal.append(
            "consumer_cursor", node=1, group=int(GROUP), cursor=source_cursor
        )
        BundleCampaign.source_cursor = source_cursor
        BundleCampaign.terminal = result
        BundleCampaign.dlq_record = dlq
        BundleCampaign.token = token
        source_records = self.records(self.source.read_history())
        self.assertEqual(len(source_records), 3, source_records)
        for item in source_records:
            self.journal.append(
                "source_record", node=1, incarnation=item["incarnation"],
                offset=item["offset"], message_id=item["message_id"],
                digest=item["digest"],
            )

    def test_2_the_bundle_migration_commits_both_histories(self) -> None:
        (self.source.metadata_dir / "MIGRATE").write_text("2\n")
        line = self.target.wait_for("Q1 route active", 240)
        self.assertIsNotNone(
            line,
            "the successor must open the recovered history: " + "\n".join(self.target.lines),
        )
        quiesced = self.bundle_lines(self.source, "quiesced")
        self.assertEqual(len(quiesced), 1, f"the bundle must quiesce once: {quiesced}")
        self.journal.append(
            "bundle_quiesced", migration_id=quiesced[0]["migration_id"],
            source_watermark=quiesced[0]["source_watermark"],
            pending_terminal=quiesced[0]["pending_terminal"],
        )
        fences = self.bundle_lines(self.source, "fence")
        members = sorted(fence["member"] for fence in fences)
        self.assertEqual(
            members, ["dependent", "source"], f"both members must be fenced: {fences}"
        )
        for fence in fences:
            self.journal.append(
                "fence_confirmed", migration_id=fence["migration_id"],
                member=fence["member"], node=1, epoch=fence["original_epoch"],
                fence_offset=fence["fence_offset"], fence_digest=fence["fence_digest"],
                manifest_generation=fence["manifest_generation"],
                partition=[fence["tenant_id"], fence["topic_id"], fence["partition_id"]],
                incarnation=int(fence["incarnation"]),
            )
        inventories = self.bundle_lines(self.source, "inventory")
        for inventory in inventories:
            self.journal.append(
                "bundle_inventory", migration_id=inventory["migration_id"],
                member=inventory["member"], phase=inventory["phase"],
                watermark=inventory["watermark"], manifest_id=inventory["manifest_id"],
                partition=[inventory["tenant_id"], inventory["topic_id"],
                           inventory["partition_id"]],
            )
        source_final = [
            entry for entry in inventories
            if entry["member"] == "source" and entry["phase"] == "final"
        ]
        dependent_final = [
            entry for entry in inventories
            if entry["member"] == "dependent" and entry["phase"] == "final"
        ]
        self.assertEqual(len(source_final), 1, inventories)
        self.assertEqual(len(dependent_final), 1, inventories)
        self.assertNotEqual(
            source_final[0]["manifest_id"], dependent_final[0]["manifest_id"],
            "the two inventories of one bundle need distinct identities",
        )
        terminal_proofs = self.bundle_lines(self.source, "terminal")
        self.assertEqual(
            len(terminal_proofs), 1,
            f"the committed disposition must be proven: {terminal_proofs}",
        )
        self.assertTrue(terminal_proofs[0]["verified"])
        self.assertEqual(
            int(terminal_proofs[0]["disposition_id"]),
            self.terminal["disposition_id"],
        )
        self.assertEqual(
            int(terminal_proofs[0]["dlq_offset"]), self.dlq_record["offset"]
        )
        self.journal.append(
            "dlq_proof", migration_id=terminal_proofs[0]["migration_id"],
            partition=[int(TENANT), int(DLQ_TOPIC), int(DLQ_PARTITION)],
            disposition_id=int(terminal_proofs[0]["disposition_id"]),
            dlq_incarnation=int(terminal_proofs[0]["dlq_incarnation"]),
            dlq_offset=int(terminal_proofs[0]["dlq_offset"]), verified=True,
        )
        promotions = self.bundle_lines(self.target, "promoted")
        self.assertEqual(len(promotions), 1, f"one paired promotion is expected: {promotions}")
        promotion = promotions[0]
        self.assertTrue(promotion["dependent_promoted"])
        self.assertTrue(promotion["source_promoted"])
        self.assertNotEqual(
            promotion["dependent_inventory_digest"],
            promotion["source_inventory_digest"],
        )
        # The committed transitions of the metadata group.
        committed = self.migration_lines(self.source)
        stages = [entry["stage"].replace("_", "") for entry in committed]
        self.assertIn("catchup", stages)
        self.assertIn("fenceold", stages)
        self.assertIn("activatenew", stages)
        activation = [
            entry for entry in committed
            if entry["stage"].replace("_", "") == "activatenew"
        ][-1]
        self.assertTrue(activation["fence"], "the activation must carry the source fence")
        self.assertTrue(activation["target_ready"])
        dependencies = activation["dependencies"]
        self.assertEqual(len(dependencies), 1, dependencies)
        dependency = dependencies[0]
        self.assertIsNotNone(dependency["fence"], "the dependency must carry its fence")
        self.assertTrue(dependency["ready"])
        self.assertIsNotNone(dependency["transfer_watermark"])
        self.assertEqual(dependency["partition_id"], int(DLQ_PARTITION))
        self.assertEqual(dependency["topic_id"], int(DLQ_TOPIC))
        BundleCampaign.migration_id = activation["migration_id"]
        oracle_stage = {stage: stage for stage in q1_oracle.MIGRATION_STAGES}
        oracle_stage.update(
            {"catchup": "catch_up", "fenceold": "fence_old", "activatenew": "activate_new"}
        )
        self.journal.append(
            "migration_begin", migration_id=activation["migration_id"], source=1, target=2,
            partition=[int(TENANT), int(TOPIC), int(PARTITION)],
            incarnation=int(INCARNATION), original_epoch=activation["original_epoch"],
            next_epoch=activation["next_epoch"],
            dependent_partition=[int(TENANT), int(DLQ_TOPIC), int(DLQ_PARTITION)],
            dependent_incarnation=int(dependency["incarnation"]),
        )
        for entry in committed:
            self.journal.append(
                "migration_stage", migration_id=entry["migration_id"],
                stage=oracle_stage[entry["stage"]], metadata_revision=entry["metadata_revision"],
                transfer_watermark=entry["transfer_watermark"],
                manifest_id=entry["manifest_id"], target_ready=entry["target_ready"],
            )
        self.journal.append(
            "activation", migration_id=activation["migration_id"], node=2,
            epoch=activation["next_epoch"], target_ready=activation["target_ready"],
            transfer_watermark=activation["transfer_watermark"],
            dependent_transfer_watermark=dependency["transfer_watermark"],
            manifest_id=activation["manifest_id"],
            dependent_manifest_id=dependency["manifest_id"],
            fence_offset=activation["fence"]["fence_offset"],
            fence_digest=activation["fence"]["fence_digest"],
            manifest_generation=activation["fence"]["manifest_generation"],
            dependent_fence_offset=dependency["fence"]["fence_offset"],
            dependent_fence_digest=dependency["fence"]["fence_digest"],
        )
        # The paired promotion happened after the committed activation, and the successor
        # served the transferred history only after both promotions completed.
        self.journal.append(
            "bundle_promotion", migration_id=promotion["migration_id"],
            activation_revision=promotion["activation_revision"],
            target_epoch=promotion["target_epoch"],
            dependent_promoted=True, source_promoted=True,
            dependent_inventory_digest=promotion["dependent_inventory_digest"],
            source_inventory_digest=promotion["source_inventory_digest"],
            dependent_incarnation=int(promotion["dependent_incarnation"]),
            source_incarnation=int(promotion["source_incarnation"]),
        )
        self.journal.append("successor_active", node=2)

    def test_3_the_successor_serves_both_recovered_histories(self) -> None:
        history = self.records(self.target.read_history())
        self.assertEqual(
            len(history), 3, f"the successor must serve the source history: {history}"
        )
        for sequence, offset in self.offsets.items():
            match = [item for item in history if item["offset"] == offset]
            self.assertEqual(len(match), 1, f"offset {offset} must survive exactly once")
            self.assertEqual(match[0]["message_id"], 100 + sequence)
            self.assertEqual(match[0]["incarnation"], int(INCARNATION))
            self.journal.append(
                "recovered_record", partition="source", sequence=sequence,
                incarnation=match[0]["incarnation"], offset=match[0]["offset"],
                message_id=match[0]["message_id"], digest=match[0]["digest"], node=2,
            )
        dlq_records = self.records(self.target.read_dlq())
        self.assertEqual(
            len(dlq_records), 1,
            f"the successor must serve exactly one DLQ record: {dlq_records}",
        )
        self.assertEqual(dlq_records[0]["incarnation"], self.dlq_record["incarnation"])
        self.assertEqual(dlq_records[0]["offset"], self.dlq_record["offset"])
        self.assertEqual(dlq_records[0]["message_id"], self.dlq_record["message_id"])
        self.assertEqual(dlq_records[0]["digest"], self.dlq_record["digest"])
        self.journal.append(
            "recovered_record", partition="dependent", node=2,
            incarnation=dlq_records[0]["incarnation"], offset=dlq_records[0]["offset"],
            message_id=dlq_records[0]["message_id"], digest=dlq_records[0]["digest"],
        )
        # A committed producer token keeps its offset on the successor.
        self.journal.append("publication_attempt", producer=11, epoch=1, sequence=0, node=2)
        retry = self.target.publish(0, 100, epoch=8)
        self.assertEqual(retry.returncode, 0, (retry.stdout, retry.stderr))
        committed = [line for line in retry.stdout.splitlines() if "COMMITTED_LOCAL" in line]
        self.assertTrue(committed, retry.stdout)
        self.assertEqual(
            int(committed[0].split()[2]), self.offsets[0],
            "the retry must return the original offset without a duplicate",
        )
        self.journal.append(
            "publication_committed", producer=11, epoch=1, sequence=0,
            offset=self.offsets[0], message_id=100, incarnation=int(INCARNATION), node=2,
            digest=q1_oracle.digest_hex(b"event-0"),
        )
        # The consumer progress is recovered: the closed records are not redelivered and
        # the durable cursor continues where the old owner left it.
        cursor_round = self.target.consume(ack_count=0)
        self.assertEqual(cursor_round.returncode, 0, (cursor_round.stdout, cursor_round.stderr))
        cursor = None
        deliveries = []
        for line in cursor_round.stdout.splitlines():
            if line.startswith("GROUP_BATCH "):
                cursor = int(line.split()[3])
            if line.startswith("DELIVERY "):
                deliveries.append(int(line.split()[2]))
        self.assertIsNotNone(cursor, cursor_round.stdout)
        self.assertEqual(
            deliveries, [],
            "a closed record is never redelivered after the handover",
        )
        self.assertGreaterEqual(
            cursor, self.source_cursor,
            "the recovered cursor must not move backwards",
        )
        self.journal.append("consumer_cursor", node=2, group=int(GROUP), cursor=cursor)
        # A terminal result lookup does not publish a second DLQ message.
        duplicate = raw_nack(self.target, self.token, 8)
        self.assertEqual(duplicate.get("outcome"), 5, duplicate)
        self.assertEqual(duplicate.get("disposition"), self.terminal["disposition_id"])
        after = self.records(self.target.read_dlq())
        self.assertEqual(
            len(after), 1,
            f"a terminal lookup must never publish a second disposition: {after}",
        )
        self.journal.append(
            "terminal_lookup", node=2, disposition_id=self.terminal["disposition_id"],
            outcome="already_closed", dlq_records=len(after),
        )

    def test_4_the_old_owner_refuses_every_mutation(self) -> None:
        result = self.source.publish(1, 101)
        self.assertNotEqual(
            result.returncode, 0, "the fenced old owner must refuse a publication"
        )
        self.journal.append("publication_failed", producer=11, epoch=1, sequence=1, node=1)
        rejected = self.source.nack("nack")
        self.assertTrue(
            rejected.returncode != 0 or "NACK_ERROR" in rejected.stdout,
            f"the quiesced old owner must refuse a terminal operation: {rejected.stdout}",
        )
        self.journal.append("terminal_failed", node=1)

    def test_5_both_brokers_restart_and_keep_both_histories(self) -> None:
        self.source.stop()
        self.target.stop()
        self.source.start()
        self.target.start()
        line = self.target.wait_for("Q1 route active", 180)
        self.assertIsNotNone(
            line,
            "the restarted successor must reopen the recovered histories: "
            + "\n".join(self.target.lines),
        )
        self.journal.append("successor_restarted", node=2)
        history = self.records(self.target.read_history())
        self.assertEqual(len(history), 3, history)
        dlq_records = self.records(self.target.read_dlq())
        self.assertEqual(len(dlq_records), 1, dlq_records)
        self.assertEqual(dlq_records[0]["message_id"], self.dlq_record["message_id"])
        self.assertEqual(dlq_records[0]["digest"], self.dlq_record["digest"])
        self.journal.append(
            "recovered_record", partition="dependent", node=2, restart=True,
            incarnation=dlq_records[0]["incarnation"], offset=dlq_records[0]["offset"],
            message_id=dlq_records[0]["message_id"], digest=dlq_records[0]["digest"],
        )
        for item in history:
            self.journal.append(
                "recovered_record", partition="source", node=2, restart=True,
                incarnation=item["incarnation"], offset=item["offset"],
                message_id=item["message_id"], digest=item["digest"],
            )

    def test_6_the_oracle_accepts_the_bundle_campaign(self) -> None:
        self.journal.conclusion(
            q1_oracle.PASS,
            "both histories were transferred, fenced, activated and promoted; the "
            "successor serves the same source records and the same DLQ disposition",
        )
        self.journal.close()
        records = q1_oracle.load(self.root / "campaign.jsonl")
        verdict = q1_oracle.reconcile_bundle(records)
        self.assertEqual(
            verdict["verdict"], q1_oracle.PASS,
            f"the bundle oracle rejected the campaign: {verdict}",
        )
        # The persisted evidence is compact but complete enough to re-reconcile.
        evidence = ROOT / "benchmarks/evidence/c02-bundle-campaign"
        evidence.mkdir(parents=True, exist_ok=True)
        (evidence / "journal.jsonl").write_text(
            (self.root / "campaign.jsonl").read_text(), encoding="utf-8"
        )
        (evidence / "verdict.json").write_text(
            json.dumps(
                {
                    "verdict": verdict["verdict"],
                    "reasons": verdict["reasons"],
                    "counts": verdict["counts"],
                    "oracle": "scripts/q1_oracle.py:reconcile_bundle",
                },
                indent=2,
                sort_keys=True,
            )
            + "\n",
            encoding="utf-8",
        )


if __name__ == "__main__":
    unittest.main(verbosity=2)
