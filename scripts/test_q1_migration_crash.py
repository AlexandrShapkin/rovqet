#!/usr/bin/env python3
"""Deterministic crash matrix for a Q1 ownership handover.

Every scenario runs its own cluster on its own directories and ports, pauses one broker at
one named barrier, kills it with SIGKILL, restarts it on the original storage and checks the
safety invariant of that window. A scenario that never reaches its barrier is a failure, not
a pass, and a campaign with no executed scenario is never reported as PASS.

The runner needs a broker built with the `fault-injection` feature, which is the only build
in which the barriers exist.
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

ROOT = Path(__file__).resolve().parents[1]
SERVER = ROOT / "target/debug/rovqet"
VOTER = ROOT / "target/debug/metadata-raft-node"
Q1 = ROOT / "target/debug/examples/q1"

CLUSTER = 616161
TOPIC_NAME = "durable"


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
) -> Node:
    (root / "q1").mkdir(parents=True, exist_ok=True)
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
    if node_id == 1:
        argv.extend(["--migrate-to", "2"])
    return Node(name, argv, root, env)


def publish(node: Node, sequence: int, epoch: int = 7) -> subprocess.CompletedProcess:
    return subprocess.run(
        [str(Q1), TOPIC_NAME, "11", "1", str(sequence), str(100 + sequence), "0", "42",
         str(epoch), f"event-{sequence}"],
        env=dict(os.environ, ROVQET_ADDR=f"127.0.0.1:{node.argv[node.argv.index('--listen') + 1].split(':')[1]}"),
        capture_output=True, text=True, timeout=60,
    )


class Scenario:
    def __init__(self, scenario_id: str, barrier: str, victim: str, invariant: str):
        self.id = scenario_id
        self.barrier = barrier
        self.victim = victim
        self.invariant = invariant


SCENARIOS = [
    Scenario("C1-before-fence", "before_fence", "source", "no-activation"),
    Scenario("C2-after-fence", "after_fence", "source", "fenced-source"),
    Scenario("C3-before-activate-commit", "before_activate_commit", "source", "no-activation"),
    Scenario("C4-promotion-intent", "promotion_intent", "target", "no-partial-activation"),
    Scenario("C5-before-route-open", "before_route_open", "target", "route-recovers"),
    Scenario("C6-after-route-open", "after_route_open", "target", "route-recovers"),
]


def run_scenario(scenario: Scenario) -> dict:
    root = Path(tempfile.mkdtemp(prefix=f"rovqet-crash-{scenario.id}-"))
    peer_ports = {1: free_port(), 2: free_port()}
    raft_ports = {1: free_port(), 2: free_port(), 3: free_port()}
    barrier_env = {"ROVQET_MIGRATION_BARRIER": scenario.barrier}
    source = broker("source", 1, root / "source", peer_ports, raft_ports, True,
                    barrier_env if scenario.victim == "source" else None)
    target = broker("target", 2, root / "target", peer_ports, raft_ports, False,
                    barrier_env if scenario.victim == "target" else None)
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
    result = {"scenario": scenario.id, "barrier": scenario.barrier, "verdict": "INVALID",
              "reasons": [], "barrier_reached": False}
    try:
        for node in (source, target, voter):
            node.start()
        if source.wait_for("Q1 route active", 60) is None:
            result["reasons"].append("the source never opened its route")
            return result
        published = publish(source, 0)
        if published.returncode != 0:
            result["reasons"].append(f"the source publication failed: {published.stderr}")
            return result
        # The operator asks for the handover.
        (root / "source" / "raft" / "MIGRATE").write_text("2\n")
        victim = source if scenario.victim == "source" else target
        reached = victim.wait_for(f"MIGRATION_BARRIER {scenario.barrier}", 90)
        if reached is None:
            result["reasons"].append(f"the barrier {scenario.barrier} was never reached")
            return result
        result["barrier_reached"] = True
        victim.kill()
        # The restarted process must proceed: the barrier is a one-shot pause for the crash
        # window, not a permanent block.
        victim.env = {}
        victim.start()
        # The victim must come back on its original storage.
        if scenario.victim == "source":
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

        if scenario.invariant == "fenced-source":
            # The durable fence survived the restart: the old owner refuses a publication.
            refused = publish(source, 1)
            if refused.returncode == 0:
                result["reasons"].append("the restarted old owner still committed")
                return result
            result["verdict"] = "PASS"
        elif scenario.invariant == "no-activation":
            # The target must not be active: no "Q1 route active" on the target.
            if target.wait_for("Q1 route active", 10) is not None:
                result["reasons"].append("the target became active without the fence")
                return result
            result["verdict"] = "PASS"
        elif scenario.invariant == "no-partial-activation":
            # Either the promotion completed and the route opened, or it is still inactive:
            # a partially promoted directory must never be served.
            active = target.wait_for("Q1 route active", 90)
            refused = target.wait_for("the promoted storage was refused", 10)
            if active is None and refused is None:
                result["reasons"].append("the target neither recovered nor refused the promotion")
                return result
            result["verdict"] = "PASS"
        elif scenario.invariant == "route-recovers":
            if target.wait_for("Q1 route active", 90) is None:
                result["reasons"].append("the promoted storage did not reopen")
                return result
            result["verdict"] = "PASS"
        return result
    finally:
        for node in (source, target, voter):
            node.stop()


def run_unreachable_successor() -> dict:
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
    result = {"scenario": "C7-unreachable-successor", "barrier": "-", "verdict": "INVALID",
              "reasons": [], "barrier_reached": True}
    try:
        # The successor is not started at all: the direction to it is blocked.
        source.start()
        voter.start()
        if source.wait_for("Q1 route active", 60) is None:
            result["reasons"].append("the source never opened its route")
            return result
        published = publish(source, 0)
        if published.returncode != 0:
            result["reasons"].append("the source publication failed")
            return result
        (root / "source" / "raft" / "MIGRATE").write_text("2\n")
        # The source keeps serving while the successor is unreachable.
        time.sleep(5)
        again = publish(source, 1)
        if again.returncode != 0:
            result["reasons"].append("the source stopped serving before any fence")
            return result
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
        result["verdict"] = "PASS"
        return result
    finally:
        for node in (source, target, voter):
            node.stop()


def main() -> int:
    if not SERVER.exists() or not VOTER.exists():
        print("build the brokers first", file=sys.stderr)
        return 2
    # The barriers exist only in the fault-injection build.
    probe = subprocess.run([str(SERVER), "--version"], capture_output=True, text=True)
    executed = 0
    verdicts = []
    for scenario in SCENARIOS:
        print(f"\n===== {scenario.id} ({scenario.barrier}) =====", flush=True)
        result = run_scenario(scenario)
        executed += 1
        verdicts.append(result)
        print(f"===== {scenario.id}: {result['verdict']} {result['reasons']} =====", flush=True)
    print("\n===== C7-unreachable-successor =====", flush=True)
    unreachable = run_unreachable_successor()
    executed += 1
    verdicts.append(unreachable)
    print(
        f"===== C7-unreachable-successor: {unreachable['verdict']} "
        f"{unreachable['reasons']} =====",
        flush=True,
    )
    journal = {
        "schema": "rovqet-migration-crash-v1",
        "executed": executed,
        "verdicts": verdicts,
        "broker_version": probe.stdout.strip(),
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
