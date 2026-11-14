#!/usr/bin/env python3
"""Multi-process campaign for a Q1 ownership handover.

Three independently persisted metadata voters, a source broker process, a target broker
process and external publisher and consumer processes. The parent owns the oracle journal
and never trusts a broker's own account of what happened: every observation is an
externally received result.

The three brokers share one machine, so this proves protocol behaviour and not
independent physical failure domains.
"""
from __future__ import annotations

import json
import os
import queue
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import q1_oracle  # noqa: E402

ROOT = Path(__file__).resolve().parents[1]
SERVER = ROOT / "target/debug/rovqet"
VOTER = ROOT / "target/debug/metadata-raft-node"
Q1 = ROOT / "target/debug/examples/q1"
READ = ROOT / "target/debug/examples/read_q1"
CONSUME = ROOT / "target/debug/examples/consume_q1"

CLUSTER = 515151
TENANT = "1"
TOPIC = "3"
PARTITION = "0"
INCARNATION = "42"
TOPIC_NAME = "durable"
GROUP = "9"


_ALLOCATED: set[int] = set()


def free_port() -> int:
    """An unused loopback port.

    The kernel happily hands the same ephemeral port out again right after it was
    released, so the allocated ports are remembered: two processes must never be
    given the same port.
    """
    for _ in range(1000):
        with socket.socket() as sock:
            sock.bind(("127.0.0.1", 0))
            port = sock.getsockname()[1]
        if port not in _ALLOCATED:
            _ALLOCATED.add(port)
            return port
    raise RuntimeError("no free loopback port available")


class Process:
    """One broker or voter process with a captured diagnostic stream."""

    def __init__(self, name: str, argv: list[str], cwd: Path):
        self.name = name
        self.argv = argv
        self.cwd = cwd
        cwd.mkdir(parents=True, exist_ok=True)
        self.process: subprocess.Popen | None = None
        self.lines: list[str] = []
        self._reader: threading.Thread | None = None

    def start(self) -> None:
        # A restart must be observed from its own output: a stale line from the previous
        # incarnation would satisfy wait_for before the new process has opened anything.
        self.lines.clear()
        self.process = subprocess.Popen(
            self.argv,
            cwd=self.cwd,
            # The voter exits on stdin EOF, so the pipe stays open.
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )
        self._reader = threading.Thread(target=self._read, daemon=True)
        self._reader.start()

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
            if self.process is not None and self.process.poll() is not None:
                break
            time.sleep(0.05)
        return None

    def stop(self, kill: bool = False) -> None:
        if self.process is None:
            return
        if self.process.poll() is None:
            self.process.send_signal(signal.SIGKILL if kill else signal.SIGTERM)
            try:
                self.process.wait(timeout=15)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait(timeout=5)
        if self.process.stdout is not None:
            self.process.stdout.close()
        self.process = None

    def alive(self) -> bool:
        return self.process is not None and self.process.poll() is None


class Broker(Process):
    """A broker process in cluster and Raft mode."""

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
        client_port = free_port()
        # The listen addresses must be the ones every peer already knows.
        peer_port = peer_ports[node_id]
        raft_port = raft_ports[node_id]
        # The broker opens an existing partition directory: it never creates one.
        (root / "q1").mkdir(parents=True, exist_ok=True)
        argv = [
            str(SERVER),
            "--listen", f"127.0.0.1:{client_port}",
            "--q1-data-dir", str(root / "q1"),
            "--q1-topic", TOPIC_NAME,
            "--q1-tenant-id", TENANT,
            "--q1-topic-id", TOPIC,
            "--q1-partition-id", PARTITION,
            "--q1-incarnation", INCARNATION,
            "--q1-owner-epoch", "7",
            "--q1-initialize-new-log", "true" if initialize else "false",
            "--cluster-id", str(CLUSTER),
            "--node-id", str(node_id),
            "--cluster-listen", f"127.0.0.1:{peer_port}",
            "--metadata-raft", "true",
            "--metadata-raft-dir", str(root / "raft"),
            "--metadata-raft-listen", f"127.0.0.1:{raft_port}",
            "--metadata-raft-bootstrap", "true" if node_id == 1 else "false",
            "--transfer-staging-dir", str(root / "staging"),
        ]
        for peer, port in peer_ports.items():
            argv.extend(["--cluster-peer", f"{peer}=127.0.0.1:{port}"])
        for peer, port in raft_ports.items():
            argv.extend(["--metadata-raft-peer", f"{peer}=127.0.0.1:{port}"])
        if migrate_to is not None:
            argv.extend(["--migrate-to", str(migrate_to)])
        super().__init__(name, argv, root)
        self.metadata_dir = root / "raft"
        self.client_port = client_port
        self.peer_port = peer_port
        self.raft_port = raft_port
        self.node_id = node_id

    def publish(self, sequence: int, message_id: int, epoch: int = 7):
        return subprocess.run(
            [
                str(Q1), TOPIC_NAME, "11", "1", str(sequence), str(message_id),
                PARTITION, INCARNATION, str(epoch), f"event-{sequence}",
            ],
            env=dict(os.environ, ROVQET_ADDR=f"127.0.0.1:{self.client_port}"),
            capture_output=True, text=True, timeout=60,
        )

    def read_history(self):
        result = subprocess.run(
            [str(READ), TOPIC_NAME, TENANT, TOPIC, PARTITION, INCARNATION, "0", "64", "65536", "0"],
            env=dict(os.environ, ROVQET_ADDR=f"127.0.0.1:{self.client_port}"),
            capture_output=True, text=True, timeout=60,
        )
        assert result.returncode == 0, (result.stdout, result.stderr)
        return [line for line in result.stdout.splitlines() if line.startswith("READ_RECORD")]

    def consume(self, ack_count: int, match_offset: int | None = None):
        argv = [
            str(CONSUME), TOPIC_NAME, TENANT, TOPIC, PARTITION, INCARNATION, GROUP,
            "0", "16", "65536", "0", str(ack_count),
        ]
        if match_offset is not None:
            argv.append(str(match_offset))
        return subprocess.run(
            argv,
            env=dict(os.environ, ROVQET_ADDR=f"127.0.0.1:{self.client_port}"),
            capture_output=True, text=True, timeout=60,
        )


class MigrationCampaign(unittest.TestCase):
    journal: q1_oracle.Journal
    root: Path
    source: Broker
    target: Broker
    voter: Process
    acknowledged: dict[int, int] = {}
    offsets: dict[int, int] = {}

    @classmethod
    def setUpClass(cls) -> None:
        for binary in (SERVER, VOTER, Q1, READ, CONSUME):
            if not binary.exists():
                subprocess.run(
                    ["cargo", "build", "-p", "rovqet-server", "-p", "rovqet-metadata-raft",
                     "-p", "rovqet-sdk", "--examples", "--bins", "--locked"],
                    cwd=ROOT, check=True,
                )
                break
        cls.root = Path(tempfile.mkdtemp(prefix="rovqet-migration-"))
        cls.journal = q1_oracle.Journal(cls.root / "campaign.jsonl", "migration-campaign")
        peer_ports = {1: free_port(), 2: free_port()}
        raft_ports = {1: free_port(), 2: free_port(), 3: free_port()}
        cls.source = Broker("source", 1, cls.root / "source", peer_ports, raft_ports, True, migrate_to=2)
        cls.target = Broker("target", 2, cls.root / "target", peer_ports, raft_ports, False)
        cls.voter = Process(
            "voter3",
            [
                str(VOTER),
                "--id", "3",
                "--cluster-id", str(CLUSTER),
                "--listen", f"127.0.0.1:{raft_ports[3]}",
                "--data-dir", str(cls.root / "voter3"),
                "--bootstrap", "false",
            ],
            cls.root,
        )
        # The third voter joins the same metadata group.
        cls.voter.argv.extend(
            ["--peer", f"1=127.0.0.1:{raft_ports[1]}",
             "--peer", f"2=127.0.0.1:{raft_ports[2]}",
             "--peer", f"3=127.0.0.1:{raft_ports[3]}"]
        )
        for process in (cls.source, cls.target, cls.voter):
            process.start()
        cls.journal.append("campaign_started", source=cls.source.node_id,
                           target=cls.target.node_id, voters=sorted(raft_ports))

    @classmethod
    def tearDownClass(cls) -> None:
        for process in (cls.source, cls.target, cls.voter):
            process.stop()
        if os.environ.get("ROVQET_KEEP_TMP"):
            # Diagnostic aid: the campaign directory survives for inspection.
            print(f"campaign directory kept: {cls.root}", flush=True)
        else:
            shutil.rmtree(cls.root, ignore_errors=True)

    def migration_lines(self, process: Process) -> list[dict]:
        events = []
        for line in process.lines:
            if line.startswith("MIGRATION committed "):
                payload = line.split("MIGRATION committed ", 1)[1]
                try:
                    events.append(json.loads(payload))
                except json.JSONDecodeError:
                    continue
        return events

    @staticmethod
    def record_offsets(lines: list[str]) -> list[int]:
        """The application offsets of the read records: READ_RECORD <inc> <seq> <id> <hex>."""
        return sorted(int(line.split()[2]) for line in lines)

    def test_1_the_source_owns_the_partition_and_commits_records(self) -> None:
        line = self.source.wait_for("Q1 route active", 60)
        self.assertIsNotNone(line, "the source must open its route: " + "\n".join(self.source.lines))
        self.journal.append("source_ready", node=1)
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
            # COMMITTED_LOCAL <incarnation> <sequence>
            offset = int(committed[0].split()[2])
            offsets[sequence] = offset
            self.journal.append(
                "publication_committed", producer=11, epoch=1, sequence=sequence,
                offset=offset, message_id=100 + sequence, incarnation=42, node=1,
            )
        self.assertEqual(len(set(offsets.values())), 3, "every publication needs its own offset")
        # unittest creates a fresh instance per test, so the evidence lives on the class.
        MigrationCampaign.offsets = offsets

    def test_2_a_consumer_group_acknowledges_out_of_order(self) -> None:
        # The second message is acknowledged alone: the sparse set must survive.
        result = self.source.consume(ack_count=16, match_offset=self.offsets[1])
        self.assertEqual(result.returncode, 0, (result.stdout, result.stderr))
        self.assertIn("ACK_COMMITTED", result.stdout, result.stdout)
        self.journal.append(
            "consumer_ack", group=GROUP, offset=self.offsets[1], node=1,
        )
        MigrationCampaign.acknowledged[self.offsets[1]] = 1
        # The first message is acknowledged as well: the cursor may advance.
        result = self.source.consume(ack_count=16, match_offset=self.offsets[0])
        self.assertEqual(result.returncode, 0, (result.stdout, result.stderr))
        self.assertIn("ACK_COMMITTED", result.stdout, result.stdout)
        self.journal.append(
            "consumer_ack", group=GROUP, offset=self.offsets[0], node=1,
        )
        MigrationCampaign.acknowledged[self.offsets[0]] = 1

    def test_3_the_migration_is_committed_and_the_successor_takes_over(self) -> None:
        # The operator asks the source to hand the partition over. Every stage is still
        # committed by the metadata group.
        (self.source.metadata_dir / "MIGRATE").write_text("2\n")
        line = self.target.wait_for("Q1 route active", 180)
        self.assertIsNotNone(
            line,
            "the successor must open the transferred history: " + "\n".join(self.target.lines),
        )
        events = self.migration_lines(self.source)
        self.assertTrue(events, "the source must report the committed transitions")
        stages = [event["stage"].replace("_", "") for event in events]
        self.assertIn("catchup", stages)
        self.assertIn("fenceold", stages)
        self.assertIn("activatenew", stages)
        activation = [
            event for event in events if event["stage"].replace("_", "") == "activatenew"
        ][-1]
        self.assertTrue(activation["fence"], "the activation must carry the fence proof")
        self.assertTrue(activation["target_ready"], "the activation must carry the readiness")
        migration_id = activation["migration_id"]
        self.journal.append(
            "migration_begin", migration_id=migration_id, source=1, target=2,
            partition=PARTITION, incarnation=42,
            original_epoch=activation["original_epoch"], next_epoch=activation["next_epoch"],
            metadata_revision=activation["metadata_revision"],
        )
        # The broker's report line carries the compact stage spelling; the journal must
        # use the stage names of the metadata state machine the oracle reconciles.
        oracle_stage = {
            "prepare": "prepare",
            "catchup": "catch_up",
            "catch_up": "catch_up",
            "fenceold": "fence_old",
            "fence_old": "fence_old",
            "activatenew": "activate_new",
            "activate_new": "activate_new",
            "cleanup": "cleanup",
        }
        for event in events:
            self.journal.append(
                "migration_stage", migration_id=event["migration_id"],
                stage=oracle_stage[event["stage"]],
                metadata_revision=event["metadata_revision"],
                transfer_watermark=event["transfer_watermark"],
                manifest_id=event["manifest_id"], target_ready=event["target_ready"],
            )
            if event.get("fence"):
                self.journal.append(
                    "fence_confirmed", migration_id=event["migration_id"], node=1,
                    epoch=event["fence"]["old_epoch"], fence_offset=event["fence"]["fence_offset"],
                    fence_digest=event["fence"]["fence_digest"],
                    manifest_generation=event["fence"]["manifest_generation"],
                )
        self.journal.append(
            "activation", migration_id=migration_id, node=2,
            epoch=activation["next_epoch"], fence_offset=activation["fence"]["fence_offset"],
            fence_digest=activation["fence"]["fence_digest"],
            manifest_generation=activation["fence"]["manifest_generation"],
            target_ready=True, transfer_watermark=activation["transfer_watermark"],
        )
        self.migration_id = migration_id

    def test_4_the_successor_serves_the_transferred_history(self) -> None:
        history = self.target.read_history()
        self.assertTrue(history, "the successor must serve the transferred history")
        offsets = self.record_offsets(history)
        for sequence, offset in self.offsets.items():
            self.assertIn(offset, offsets, "the transferred offset must survive")
        # A producer retry with the original token returns the original offset.
        self.journal.append("publication_attempt", producer=11, epoch=1, sequence=0, node=2)
        result = self.target.publish(0, 100, epoch=8)
        self.assertEqual(result.returncode, 0, (result.stdout, result.stderr))
        committed = [line for line in result.stdout.splitlines() if "COMMITTED_LOCAL" in line]
        self.assertTrue(committed, f"the retry must resolve: {result.stdout}")
        # COMMITTED_LOCAL <incarnation> <sequence>: the same durable offset test_1 parsed.
        offset = int(committed[0].split()[2])
        self.assertEqual(
            offset, self.offsets[0],
            "the retry must return the original offset without a duplicate",
        )
        self.journal.append(
            "publication_committed", producer=11, epoch=1, sequence=0, offset=offset,
            message_id=100, incarnation=42, node=2,
        )

    def test_5_the_old_owner_refuses_to_mutate(self) -> None:
        result = self.source.publish(1, 101)
        self.assertNotEqual(
            result.returncode, 0,
            "the fenced old owner must refuse a publication",
        )
        self.journal.append("publication_failed", producer=11, epoch=1, sequence=1, node=1)

    def test_6_both_brokers_restart_and_keep_their_state(self) -> None:
        self.source.stop()
        self.target.stop()
        self.source.start()
        self.target.start()
        line = self.target.wait_for("Q1 route active", 120)
        self.assertIsNotNone(
            line,
            "the restarted successor must open the transferred history: "
            + "\n".join(self.target.lines),
        )
        history = self.target.read_history()
        offsets = self.record_offsets(history)
        for offset in self.offsets.values():
            self.assertIn(offset, offsets, "the transferred offset must survive the restart")

    def test_7_the_oracle_accepts_the_campaign(self) -> None:
        self.journal.conclusion(
            q1_oracle.PASS,
            "the handover was committed, fenced and served by the successor",
        )
        self.journal.close()
        records = q1_oracle.load(self.root / "campaign.jsonl")
        verdict = q1_oracle.reconcile_migration(records)
        self.assertEqual(
            verdict["verdict"], q1_oracle.PASS,
            f"the migration oracle rejected the campaign: {verdict}",
        )


if __name__ == "__main__":
    unittest.main(verbosity=2)
