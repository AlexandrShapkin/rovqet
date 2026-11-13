#!/usr/bin/env python3
"""C-01 multi-process cluster routing: a non-owner node forwards one Q1 publication.

Two locally running brokers with distinct node IDs, cluster identity, peer ports, metadata
fixtures and data directories. The client publishes to the non-owner, the owner performs the
durable commit and returns the original durable outcome. Disposable storage only.
"""
from __future__ import annotations

import json
import os
import socket
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SERVER = ROOT / "target/debug/rovqet"
Q1 = ROOT / "target/debug/examples/q1"
READ = ROOT / "target/debug/examples/read_q1"
PROBE = ROOT / "target/debug/examples/cluster_probe"
CLUSTER = 4242


def free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def snapshot_file(path: Path, owner: int) -> None:
    path.write_text(json.dumps({
        "cluster_id": CLUSTER,
        "revision": 1,
        "issued_at_ms": 1,
        "revalidate_after_ms": 600_000,
        "acl_revision": 1,
        "policy_revision": 1,
        "capability_revision": 1,
        "nodes": [
            {"node": 1, "address": "127.0.0.1:1"},
            {"node": 2, "address": "127.0.0.1:2"},
        ],
        "topics": [{"topic_id": 3, "durable_class": 1, "policy_revision": 1}],
        "partitions": [{
            "tenant_id": 1,
            "topic_id": 3,
            "partition_id": 0,
            "owner": owner,
            "ownership_epoch": 9,
            "lease_valid_for_ms": 600_000,
            "desired_replicas": 1,
            "shard_id": 0,
            "required_policy_revision": 1,
        }],
    }, indent=2), encoding="utf-8")


class Node:
    """One broker process in cluster mode."""

    def __init__(self, directory: Path, node_id: int, peer: int, peer_port: int,
                 owner: int, peer_port_self: int):
        self.directory = directory
        self.node_id = node_id
        self.data = directory / f"wal{node_id}"
        self.data.mkdir(parents=True, exist_ok=True)
        self.metadata = directory / f"metadata{node_id}.json"
        snapshot_file(self.metadata, owner)
        self.client_port = free_port()
        self.cluster_port = peer_port_self
        self.process = subprocess.Popen(
            [
                str(SERVER), "--listen", f"127.0.0.1:{self.client_port}",
                "--q1-data-dir", str(self.data),
                "--q1-topic", "durable",
                "--q1-tenant-id", "1",
                "--q1-topic-id", "3",
                "--q1-partition-id", "0",
                "--q1-incarnation", "42",
                "--q1-owner-epoch", "7",
                "--q1-initialize-new-log", "true",
                "--cluster-id", str(CLUSTER),
                "--node-id", str(node_id),
                "--cluster-listen", f"127.0.0.1:{self.cluster_port}",
                "--cluster-peer", f"{peer}=127.0.0.1:{peer_port}",
                "--cluster-metadata", str(self.metadata),
            ],
            stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, text=True)
        self.lines: list[str] = []
        self._read()

    def _read(self) -> None:
        assert self.process.stderr is not None
        for line in self.process.stderr:
            self.lines.append(line.rstrip())
            if "listening on" in line:
                return

    def wait_ready(self, seconds: int = 20) -> None:
        deadline = time.monotonic() + seconds
        while time.monotonic() < deadline:
            if any("listening on" in line for line in self.lines):
                return
            if self.process.poll() is not None:
                raise AssertionError("node exited: " + "\n".join(self.lines))
            time.sleep(0.02)
        raise AssertionError("node not ready: " + "\n".join(self.lines))

    def publish(self, sequence: int = 0, payload: str = "event-0"):
        return subprocess.run(
            [str(Q1), "durable", "11", "0", str(sequence), str(100 + sequence),
             "0", "42", "7", payload],
            env=dict(os.environ, ROVQET_ADDR=f"127.0.0.1:{self.client_port}"),
            capture_output=True, text=True, timeout=30)

    def read_history(self):
        result = subprocess.run(
            [str(READ), "durable", "1", "3", "0", "42", "0", "64", "65536", "0"],
            env=dict(os.environ, ROVQET_ADDR=f"127.0.0.1:{self.client_port}"),
            capture_output=True, text=True, timeout=30)
        assert result.returncode == 0, (result.stdout, result.stderr)
        return [line for line in result.stdout.splitlines() if line.startswith("READ_RECORD")]

    def stop(self) -> None:
        if self.process.poll() is None:
            self.process.terminate()
            try:
                self.process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait(timeout=5)


class ClusterRouting(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix="rovqet-cluster-")
        self.directory = Path(self.temporary.name)
        self.port_a = free_port()
        self.port_b = free_port()
        # Node 1 owns the partition; node 2 forwards to it.
        self.owner = Node(self.directory, 1, 2, self.port_b, 1, self.port_a)
        self.proxy = Node(self.directory, 2, 1, self.port_a, 1, self.port_b)
        self.owner.wait_ready()
        self.proxy.wait_ready()

    def tearDown(self):
        self.owner.stop()
        self.proxy.stop()
        self.temporary.cleanup()

    def test_a_non_owner_forwards_one_publication_to_the_owner(self):
        result = self.proxy.publish(0)
        self.assertEqual(result.returncode, 0, (result.stdout, result.stderr))
        self.assertIn("COMMITTED_LOCAL 42 0", result.stdout, result.stdout)
        # The durable record exists only at the owner.
        self.assertEqual(len(self.owner.read_history()), 1)
        self.assertEqual(self.proxy.read_history(), [])
        # The producer token is preserved: a retry returns the same durable offset.
        retry = self.proxy.publish(0)
        self.assertEqual(retry.returncode, 0, (retry.stdout, retry.stderr))
        self.assertIn("COMMITTED_LOCAL 42 0", retry.stdout, retry.stdout)
        self.assertEqual(len(self.owner.read_history()), 1)

    def test_inter_node_health_and_ownership_inspection(self):
        probe = subprocess.run(
            [str(PROBE), str(CLUSTER), "2", "1", f"1=127.0.0.1:{self.port_a}", "health"],
            capture_output=True, text=True, timeout=20)
        self.assertEqual(probe.returncode, 0, (probe.stdout, probe.stderr))
        self.assertIn("HEALTH node=1", probe.stdout, probe.stdout)
        ownership = subprocess.run(
            [str(PROBE), str(CLUSTER), "2", "1", f"1=127.0.0.1:{self.port_a}",
             "ownership", "0"],
            capture_output=True, text=True, timeout=20)
        self.assertEqual(ownership.returncode, 0, (ownership.stdout, ownership.stderr))
        self.assertIn("owner=1", ownership.stdout, ownership.stdout)
        # A peer of another cluster is refused.
        foreign = subprocess.run(
            [str(PROBE), "9999", "2", "1", f"1=127.0.0.1:{self.port_a}", "health"],
            capture_output=True, text=True, timeout=20)
        self.assertNotEqual(foreign.returncode, 0)


if __name__ == "__main__":
    unittest.main(verbosity=2)
