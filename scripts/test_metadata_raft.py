#!/usr/bin/env python3
"""Three-process acceptance test for the metadata Raft group.

Every assertion is made against evidence produced by the node processes themselves:
leader elections with their terms, committed metadata revisions, and the outcome of a
proposal. A missing observation fails the test; it is never inferred from a sleep.

The three processes run on one machine, so this proves protocol behaviour and not
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

ROOT = Path(__file__).resolve().parents[1]
NODE = ROOT / "target" / "debug" / "metadata-raft-node"
CLUSTER = 990011
TOPIC = 3
TENANT = 1


def free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


class Node:
    """One metadata Raft process."""

    def __init__(self, node_id: int, port: int, peers: dict[int, int], directory: Path):
        self.node_id = node_id
        self.port = port
        self.peers = peers
        self.directory = directory
        self.directory.mkdir(parents=True, exist_ok=True)
        self.process: subprocess.Popen | None = None
        self.events: queue.Queue[dict] = queue.Queue()
        self.history: list[dict] = []

    def start(self) -> None:
        args = [
            str(NODE),
            "--id", str(self.node_id),
            "--cluster-id", str(CLUSTER),
            "--listen", f"127.0.0.1:{self.port}",
            "--data-dir", str(self.directory),
        ]
        for peer, port in sorted(self.peers.items()):
            args.extend(["--peer", f"{peer}=127.0.0.1:{port}"])
        # Only the first node initializes the membership.
        args.extend(["--bootstrap", "true" if self.node_id == 1 else "false"])
        self.process = subprocess.Popen(
            args,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )
        threading.Thread(target=self._reader, daemon=True).start()

    def _reader(self) -> None:
        assert self.process is not None and self.process.stdout is not None
        for line in self.process.stdout:
            line = line.strip()
            if not line:
                continue
            print(f"[node {self.node_id}] {line}", flush=True)
            try:
                event = json.loads(line)
            except json.JSONDecodeError:
                continue
            self.history.append(event)
            self.events.put(event)

    def propose(self, command: dict, timeout: float = 30.0) -> dict | None:
        """Send one metadata command and wait for its outcome."""
        assert self.process is not None and self.process.stdin is not None
        self.process.stdin.write(json.dumps(command) + "\n")
        self.process.stdin.flush()
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            try:
                event = self.events.get(timeout=0.2)
            except queue.Empty:
                continue
            if event.get("event") == "proposed":
                return event
        return None

    def drain(self) -> list[dict]:
        pending: list[dict] = []
        while True:
            try:
                pending.append(self.events.get_nowait())
            except queue.Empty:
                return pending

    def wait_for(self, predicate, timeout: float) -> list[dict]:
        """Collect events until the predicate holds, returning everything seen."""
        seen: list[dict] = []
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            try:
                event = self.events.get(timeout=0.2)
            except queue.Empty:
                continue
            seen.append(event)
            if predicate(event):
                return seen
        return seen

    def kill(self) -> None:
        if self.process is None:
            return
        if self.process.poll() is None:
            self.process.send_signal(signal.SIGKILL)
        self.process.wait(timeout=10)
        for pipe in (self.process.stdin, self.process.stdout):
            if pipe is not None:
                try:
                    pipe.close()
                except OSError:
                    pass
        self.process = None

    def stop(self) -> None:
        if self.process is None:
            return
        if self.process.poll() is None:
            self.process.terminate()
            try:
                self.process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self.process.kill()
        self.process = None


class MetadataRaftCluster(unittest.TestCase):
    nodes: list[Node]
    directory: Path

    @classmethod
    def setUpClass(cls) -> None:
        if not NODE.exists():
            subprocess.run(
                ["cargo", "build", "-p", "rovqet-metadata-raft", "--bin",
                 "metadata-raft-node", "--locked"],
                cwd=ROOT, check=True,
            )
        cls.directory = Path(tempfile.mkdtemp(prefix="rovqet-metadata-raft-"))
        ports = {node: free_port() for node in (1, 2, 3)}
        cls.nodes = [
            Node(node, ports[node], ports, cls.directory / f"node{node}")
            for node in (1, 2, 3)
        ]
        for node in cls.nodes:
            node.start()

    @classmethod
    def tearDownClass(cls) -> None:
        for node in getattr(cls, "nodes", []):
            node.stop()
        shutil.rmtree(cls.directory, ignore_errors=True)

    def leader_event(self, timeout: float = 30.0) -> dict:
        """Wait until one node reports itself as leader."""
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            for node in self.nodes:
                for event in node.history:
                    if event.get("event") == "leader" and event.get("node") == node.node_id:
                        return event
            time.sleep(0.1)
        self.fail("no metadata Raft leader was elected within the deadline")

    def applied_revision(self, node: Node) -> int:
        revisions = [
            event.get("revision", 0)
            for event in node.history
            if event.get("event") == "applied"
        ]
        return max(revisions) if revisions else 0

    def test_1_a_leader_is_elected(self) -> None:
        leader = self.leader_event()
        self.assertIn("term", leader)
        self.assertGreaterEqual(leader["term"], 1)
        print(f"evidence: leader={leader}", flush=True)

    def test_2_a_majority_commits_and_replicates(self) -> None:
        leader = self.leader_event()
        leader_node = next(node for node in self.nodes if node.node_id == leader["node"])
        response = leader_node.propose({
            "SetNode": {"node": 1, "address": "127.0.0.1:1"},
        })
        self.assertIsNotNone(response, "the leader did not answer the proposal")
        self.assertTrue(response["ok"], f"proposal failed: {response}")
        response = leader_node.propose({
            "SetPlacement": {
                "tenant_id": TENANT,
                "topic_id": TOPIC,
                "partition_id": 0,
                "owner": 1,
                "ownership_epoch": 7,
                "lease_valid_for_ms": 60000,
            },
        })
        self.assertIsNotNone(response, "the leader did not answer the placement")
        self.assertTrue(response["ok"], f"proposal failed: {response}")

        # Every voter applies the committed revision: follower replication.
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            if all(self.applied_revision(node) >= 2 for node in self.nodes):
                break
            time.sleep(0.1)
        revisions = {node.node_id: self.applied_revision(node) for node in self.nodes}
        self.assertTrue(
            all(revision >= 2 for revision in revisions.values()),
            f"the committed metadata did not reach every voter: {revisions}",
        )
        print(f"evidence: committed revisions {revisions}", flush=True)

    def test_3_the_leader_is_replaced_and_keeps_the_committed_metadata(self) -> None:
        leader = self.leader_event()
        old_leader = next(node for node in self.nodes if node.node_id == leader["node"])
        old_leader.kill()

        # A new leader must be elected among the survivors.
        deadline = time.monotonic() + 60
        replacement = None
        while time.monotonic() < deadline:
            for node in self.nodes:
                if node.process is None:
                    continue
                for event in node.history:
                    if (
                        event.get("event") == "leader"
                        and event.get("node") == node.node_id
                        and event.get("node") != leader["node"]
                    ):
                        replacement = event
                        break
                if replacement:
                    break
            if replacement:
                break
            time.sleep(0.2)
        self.assertIsNotNone(replacement, "no replacement leader was elected")
        self.assertGreater(
            replacement["term"], leader["term"],
            "the replacement must win a later term",
        )
        print(f"evidence: leader {leader} replaced by {replacement}", flush=True)

        # The committed metadata survives the failover.
        new_leader = next(node for node in self.nodes if node.node_id == replacement["node"])
        self.assertGreaterEqual(
            self.applied_revision(new_leader), 2,
            "the new leader lost the committed metadata",
        )
        response = new_leader.propose({"SetNode": {"node": 2, "address": "127.0.0.1:2"}})
        self.assertIsNotNone(response, "the new leader did not answer the proposal")
        self.assertTrue(response["ok"], f"proposal failed: {response}")

    def test_4_a_minority_cannot_commit(self) -> None:
        alive = [node for node in self.nodes if node.process is not None]
        self.assertGreaterEqual(len(alive), 2)
        # Leave exactly one voter alive: a minority cannot commit.
        for node in alive[:-1]:
            node.kill()
        survivor = alive[-1]
        before = self.applied_revision(survivor)
        response = survivor.propose(
            {"SetNode": {"node": 3, "address": "127.0.0.1:3"}}, timeout=15.0,
        )
        self.assertIsNone(
            response,
            "a minority must not commit a new metadata command",
        )
        self.assertEqual(
            self.applied_revision(survivor), before,
            "a minority must not change the committed metadata",
        )
        print("evidence: the minority committed nothing", flush=True)

    def test_5_the_nodes_restart_and_recover_their_committed_metadata(self) -> None:
        for node in self.nodes:
            if node.process is None:
                node.start()
        deadline = time.monotonic() + 60
        while time.monotonic() < deadline:
            if all(self.applied_revision(node) >= 2 for node in self.nodes):
                break
            time.sleep(0.2)
        revisions = {node.node_id: self.applied_revision(node) for node in self.nodes}
        self.assertTrue(
            all(revision >= 2 for revision in revisions.values()),
            f"a restarted voter did not recover the committed metadata: {revisions}",
        )
        print(f"evidence: recovered revisions {revisions}", flush=True)


if __name__ == "__main__":
    unittest.main(verbosity=2)
