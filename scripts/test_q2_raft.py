#!/usr/bin/env python3
"""Three-process Q2 Raft acceptance harness for work package C-03 (scenarios 1-18).

Scenarios 1-10 cover election, replication, failover and divergent tails; scenarios
11-18 cover snapshot compaction, snapshot installation, whole-cluster restart, torn
and committed storage corruption, an interrupted append, snapshot rejection and the
on-disk separation from the Q1 WAL and the metadata Raft log.

The independent oracle arrives with a later dispatch, so every state comparison in
this file is written here directly: an assert-style check that appends an `evidence`
record to the scenario journal and fails the scenario with a precise reason. A
scenario that cannot reach its required state concludes with an explicit reason and a
non-PASS verdict; nothing passes silently.

Contract exercised (crates/consensus/src/bin/q2-raft-node.rs):
  * stdout: one JSON object per line - `leader`/`follower`, `applied`, `snapshot`,
    `authority` and `proposed` events;
  * stdin: one JSON object per line - `propose`, `authority`, `snapshot`,
    `shutdown`.

`proposal_result.ok` reports whether the proposal produced a committed `Applied`
result (the node's own `proposed.ok` plus an `Applied` response); a timeout, an
error or a deterministic rejection is recorded with `ok=false` and the reason.

Every wait is bounded by a deadline over the captured event history; every child
process is terminated in a `finally`; every temporary cluster directory is deleted
in a `finally` with `shutil.rmtree(ignore_errors=True)`.
"""
from __future__ import annotations

import json
import os
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import time
import traceback
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from q1_oracle import (  # noqa: E402
    FAIL as JOURNAL_FAIL,
)
from q1_oracle import (  # noqa: E402
    INVALID as JOURNAL_INVALID,
)
from q1_oracle import (  # noqa: E402
    PASS as JOURNAL_PASS,
)
from q1_oracle import (  # noqa: E402
    Journal,
    digest_hex,
    load as load_journal,
    reconcile_q2_raft,
)

ROOT = Path(__file__).resolve().parents[1]
NODE = ROOT / "target" / "debug" / "q2-raft-node"
Q1_BROKER = ROOT / "target" / "debug" / "rovqet"
Q1_CLIENT = ROOT / "target" / "debug" / "examples" / "q1"
EVIDENCE_DIR = ROOT / "benchmarks" / "evidence" / "c03-q2-raft"

PASS = "PASS"
FAIL = "FAIL"
BLOCKED = "BLOCKED"

# The single Q2 partition every scenario writes to.
PARTITION = {"tenant_id": 1, "topic_id": 3, "partition_id": 0}
EPOCH = 1
VOTERS = (1, 2, 3)
MAJORITY = 2

T_LEADER = 30.0
T_APPLIED = 20.0
T_PROPOSE = 15.0
T_PROPOSE_NO_QUORUM = 12.0
T_RESTART = 40.0
T_POLL = 0.05

_ALLOCATED: set[int] = set()


def free_port() -> int:
    """An unused loopback port that no other voter in this run may reuse."""
    for _ in range(1000):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.bind(("127.0.0.1", 0))
            port = sock.getsockname()[1]
        if port not in _ALLOCATED:
            _ALLOCATED.add(port)
            return port
    raise ScenarioBlocked("no free loopback port available")


def snapshot(node: "Node") -> list[dict]:
    return list(node.history)


def as_int(value) -> int | None:
    """A request id that serialises as either a JSON number or a decimal string."""
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        try:
            return int(value)
        except ValueError:
            return None
    return None


def applied_of(result: dict | None) -> dict | None:
    """The `Applied` body of a `proposed` event, or None when there is none."""
    if not isinstance(result, dict):
        return None
    response = result.get("response")
    if not isinstance(response, dict):
        return None
    body = response.get("Applied")
    return body if isinstance(body, dict) else None


def json_safe(value):
    return json.loads(json.dumps(value, default=str))


Q2_LOG_MAGIC = b"QRL1"
METADATA_LOG_MAGIC = b"MRL1"
MAX_RECORD = 16 * 1024 * 1024


def file_magic(path: Path, size: int = 4) -> bytes:
    """The first bytes of a file, or b"" when it cannot be read."""
    try:
        with open(path, "rb") as handle:
            return handle.read(size)
    except OSError:
        return b""


def inspect_raft_log(path: Path) -> dict:
    """Parses the QRL1 record framing of a Q2 Raft log without trusting checksums.

    The harness only needs the stored index range and the file size to prove that a
    snapshot compacted the log; the node itself verifies every checksum at open.
    """
    info: dict = {
        "path": str(path),
        "exists": path.exists(),
        "bytes": 0,
        "magic": None,
        "first_index": None,
        "last_index": None,
        "entries": [],
        "truncated": False,
    }
    if not path.exists():
        return info
    data = path.read_bytes()
    info["bytes"] = len(data)
    info["magic"] = data[:4].decode("latin-1")
    if data[:4] != Q2_LOG_MAGIC:
        return info
    offset = len(Q2_LOG_MAGIC)
    while offset + 8 <= len(data):
        length = int.from_bytes(data[offset:offset + 4], "big")
        payload = data[offset + 8:offset + 8 + length]
        if length > MAX_RECORD or len(payload) != length:
            info["truncated"] = True
            break
        try:
            entry = json.loads(payload)
            index = entry["log_id"]["index"]
        except (ValueError, KeyError, TypeError):
            info["undecodable_at"] = offset
            break
        info["entries"].append({"index": index, "offset": offset, "length": length})
        offset += 8 + length
    if info["entries"]:
        info["first_index"] = info["entries"][0]["index"]
        info["last_index"] = info["entries"][-1]["index"]
    info["end_offset"] = offset
    return info


def latest_term(harness: "Harness") -> int:
    """The highest leader/follower term any node reported so far."""
    terms = [
        event.get("term", 0)
        for node in harness.nodes.values()
        for event in snapshot(node)
        if event.get("event") in ("leader", "follower")
    ]
    return max(terms, default=0)


def snapshot_events(node: "Node") -> list[dict]:
    return [event for event in snapshot(node) if event.get("event") == "snapshot"]


class ScenarioFailure(Exception):
    """The scenario did not reach a required state; verdict FAIL."""


class ScenarioBlocked(Exception):
    """The environment cannot run the scenario; verdict BLOCKED."""


class Node:
    """One `q2-raft-node` process with its captured stdout history."""

    def __init__(self, node_id: int, port: int, ports: dict[int, int],
                 directory: Path, cluster_id: int, shard_id: int) -> None:
        self.node_id = node_id
        self.port = port
        self.ports = ports
        self.directory = directory
        self.cluster_id = cluster_id
        self.shard_id = shard_id
        self.history: list[dict] = []
        self.raw_lines: list[str] = []
        self.started_at = 0
        self.outstanding: list[int] = []
        self.propose_cursor = 0
        self.process: subprocess.Popen | None = None

    @property
    def alive(self) -> bool:
        return self.process is not None and self.process.poll() is None

    def start(self, bootstrap: bool) -> None:
        if self.alive:
            raise RuntimeError(f"node {self.node_id} is already running")
        self.directory.mkdir(parents=True, exist_ok=True)
        args = [
            str(NODE),
            "--id", str(self.node_id),
            "--cluster-id", str(self.cluster_id),
            "--shard-id", str(self.shard_id),
            "--listen", f"127.0.0.1:{self.port}",
            "--data-dir", str(self.directory),
        ]
        for peer in sorted(self.ports):
            args.extend(["--peer", f"{peer}=127.0.0.1:{self.ports[peer]}"])
        # Only the very first start bootstraps the membership; a restart loads it
        # from the durable storage the node already wrote.
        args.extend(["--bootstrap", "true" if bootstrap else "false"])
        self.process = subprocess.Popen(
            args,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )
        self.outstanding.clear()
        self.propose_cursor = len(self.history)
        self.started_at = len(self.history)
        threading.Thread(target=self._reader, daemon=True).start()

    def _reader(self) -> None:
        process = self.process
        if process is None or process.stdout is None:
            return
        for line in process.stdout:
            line = line.strip()
            if not line:
                continue
            self.raw_lines.append(line)
            print(f"[n{self.node_id}] {line}", flush=True)
            try:
                event = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(event, dict):
                self.history.append(event)

    def send(self, command: dict) -> None:
        if not self.alive or self.process is None or self.process.stdin is None:
            raise RuntimeError(f"node {self.node_id} is not running")
        self.process.stdin.write(json.dumps(command) + "\n")
        self.process.stdin.flush()

    def wait_exit(self, timeout: float) -> bool:
        if self.process is None:
            return True
        try:
            self.process.wait(timeout=timeout)
            return True
        except subprocess.TimeoutExpired:
            return False

    def error_lines(self, start: int = 0) -> list[str]:
        """Every captured line that mentions an error, from a raw-line cursor."""
        return [line for line in self.raw_lines[start:] if "error" in line.lower()]

    def kill(self) -> None:
        """SIGKILL the process; no graceful path, no state flushing."""
        if self.process is None:
            return
        if self.process.poll() is None:
            self.process.send_signal(signal.SIGKILL)
        self.wait_exit(10.0)
        for pipe in (self.process.stdin, self.process.stdout):
            if pipe is not None:
                try:
                    pipe.close()
                except OSError:
                    pass

    def terminate(self) -> None:
        if self.process is None:
            return
        if self.process.poll() is None:
            self.process.send_signal(signal.SIGTERM)
            if not self.wait_exit(5.0):
                self.kill()
                return
        for pipe in (self.process.stdin, self.process.stdout):
            if pipe is not None:
                try:
                    pipe.close()
                except OSError:
                    pass


class Harness:
    """One scenario: a fresh three-node cluster and its fsynced journal."""

    def __init__(self, journal: Journal, number: int, name: str, barrier: str) -> None:
        self.journal = journal
        self.number = number
        self.name = name
        self.barrier = barrier
        self.cluster_id = 31000 + number
        self.shard_id = 7
        ports = {node: free_port() for node in VOTERS}
        try:
            self.root = Path(tempfile.mkdtemp(prefix=f"rovqet-q2-{number:02d}-"))
        except OSError as error:
            raise ScenarioBlocked(f"cannot create the cluster directory: {error}") from error
        try:
            self.nodes: dict[int, Node] = {
                node: Node(node, ports[node], ports, self.root / f"n{node}",
                           self.cluster_id, self.shard_id)
                for node in VOTERS
            }
        except Exception:
            shutil.rmtree(self.root, ignore_errors=True)
            raise
        self._leader_seen: set[tuple] = set()
        self._snapshot_seen: set[tuple] = set()

    # -- lifecycle ---------------------------------------------------------

    def start_cluster(self) -> None:
        for node in VOTERS:
            self.nodes[node].start(bootstrap=(node == 1))
        self.journal.append(
            "cluster_started",
            cluster_id=self.cluster_id,
            shard_id=self.shard_id,
            nodes=list(VOTERS),
        )
        self.record_quorum()

    def close(self) -> None:
        """Terminate every child and delete the cluster directory."""
        try:
            for node in self.nodes.values():
                try:
                    if node.alive:
                        node.send({"shutdown": True})
                        node.wait_exit(3.0)
                    if node.alive:
                        node.terminate()
                    if node.alive:
                        node.kill()
                except Exception as error:  # cleanup must never mask the verdict
                    print(f"cleanup: node {node.node_id}: {error}", file=sys.stderr)
        finally:
            shutil.rmtree(self.root, ignore_errors=True)

    # -- journaling -------------------------------------------------------

    def sync(self) -> None:
        """Journal every leader/follower and snapshot observation not yet recorded."""
        for node_id, node in self.nodes.items():
            for event in snapshot(node):
                kind = event.get("event")
                if kind in ("leader", "follower"):
                    key = (node_id, kind, event.get("leader"), event.get("term"))
                    if key not in self._leader_seen:
                        self._leader_seen.add(key)
                        self.journal.append(
                            "leader_observed",
                            node=node_id,
                            leader=event.get("leader"),
                            term=event.get("term"),
                        )
                elif kind == "snapshot":
                    key = (node_id, event.get("included_index"), event.get("included_term"))
                    if key not in self._snapshot_seen:
                        self._snapshot_seen.add(key)
                        self.journal.append(
                            "snapshot_observed",
                            node=node_id,
                            included_index=event.get("included_index"),
                        )

    def check(self, condition, reason: str, **details) -> None:
        """Append an evidence record and fail the scenario when the check is false."""
        self.journal.append(
            "evidence",
            passed=bool(condition),
            reason=reason,
            **{key: json_safe(value) for key, value in details.items()},
        )
        if not condition:
            raise ScenarioFailure(reason)

    def wait_until(self, description: str, predicate, timeout: float) -> None:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            self.sync()
            if predicate():
                return
            time.sleep(T_POLL)
        self.sync()
        self.check(
            False,
            f"timed out after {timeout:.0f}s waiting for {description}; "
            f"alive nodes: {self.alive_nodes()}",
        )

    def alive_nodes(self) -> list[int]:
        return [node_id for node_id, node in self.nodes.items() if node.alive]

    def record_quorum(self) -> None:
        available = len(self.alive_nodes())
        self.journal.append(
            "quorum_observed",
            available_nodes=available,
            quorum=MAJORITY,
        )

    # -- observations -----------------------------------------------------

    def applied_view(self, node_id: int) -> dict | None:
        events = [e for e in snapshot(self.nodes[node_id]) if e.get("event") == "applied"]
        if not events:
            return None
        return self._project_applied(node_id, events[-1])

    def applied_view_from(self, node_id: int, start: int) -> dict | None:
        """The latest applied state that arrived after a history cursor."""
        events = [
            event
            for event in self.nodes[node_id].history[start:]
            if event.get("event") == "applied"
        ]
        if not events:
            return None
        return self._project_applied(node_id, events[-1])

    @staticmethod
    def _project_applied(node_id: int, event: dict) -> dict:
        partitions = event.get("partitions") or []
        return {
            "node": node_id,
            "applied_index": event.get("applied_index"),
            "revision": event.get("revision"),
            "partition": partitions[0] if partitions else None,
        }

    def record_applied(self, node_id: int) -> dict | None:
        view = self.applied_view(node_id)
        if view is None:
            self.journal.append(
                "applied_state",
                node=node_id,
                cluster_id=self.cluster_id,
                shard_id=self.nodes[node_id].shard_id,
                applied_index=None,
                revision=None,
                partitions=[],
            )
            return None
        partition = view["partition"]
        self.journal.append(
            "applied_state",
            node=node_id,
            cluster_id=self.cluster_id,
            shard_id=self.nodes[node_id].shard_id,
            applied_index=view["applied_index"],
            revision=view["revision"],
            partitions=[
                {
                    "partition": partition.get("partition"),
                    "count": partition.get("count"),
                    "request_id": partition.get("request_id"),
                    "chain": partition.get("chain"),
                }
            ]
            if partition
            else [],
        )
        return view

    def record_recovered(self, node_id: int) -> dict | None:
        view = self.applied_view(node_id)
        partition = view["partition"] if view else None
        self.journal.append(
            "recovered_state",
            node=node_id,
            cluster_id=self.cluster_id,
            shard_id=self.nodes[node_id].shard_id,
            partitions=[
                {
                    "partition": partition.get("partition"),
                    "count": partition.get("count"),
                    "request_id": partition.get("request_id"),
                    "chain": partition.get("chain"),
                }
            ]
            if partition
            else [],
        )
        return view

    # -- cluster control --------------------------------------------------

    def wait_leader(self, timeout: float = T_LEADER, *, after_term: int | None = None,
                    exclude: int | None = None) -> dict:
        """Wait for a self-declared leader event (`node == leader`)."""
        deadline = time.monotonic() + timeout
        candidates: list[dict] = []
        while time.monotonic() < deadline:
            self.sync()
            candidates = []
            for node_id, node in self.nodes.items():
                if not node.alive or node_id == exclude:
                    continue
                for event in snapshot(node):
                    if (
                        event.get("event") == "leader"
                        and event.get("node") == event.get("leader")
                        and (after_term is None or event.get("term", 0) > after_term)
                    ):
                        candidates.append(event)
            if candidates:
                return max(candidates, key=lambda event: event.get("term", 0))
            time.sleep(T_POLL)
        context = []
        if after_term is not None:
            context.append(f"term > {after_term}")
        if exclude is not None:
            context.append(f"node != {exclude}")
        self.check(
            False,
            f"timed out after {timeout:.0f}s waiting for a leader "
            f"({', '.join(context) or 'any'}); alive nodes: {self.alive_nodes()}",
            observed=candidates,
        )
        raise AssertionError("unreachable")  # check() always raises

    def wait_leader_agreement(self, timeout: float = T_LEADER) -> dict[int, dict]:
        """All three nodes' latest leader/follower events name one leader and term."""
        deadline = time.monotonic() + timeout
        latest: dict[int, dict] = {}
        while time.monotonic() < deadline:
            self.sync()
            latest = {}
            for node_id, node in self.nodes.items():
                events = [
                    event
                    for event in snapshot(node)
                    if event.get("event") in ("leader", "follower")
                ]
                if events:
                    latest[node_id] = events[-1]
            if len(latest) == len(VOTERS):
                leaders = {event.get("leader") for event in latest.values()}
                terms = {event.get("term") for event in latest.values()}
                if len(leaders) == 1 and len(terms) == 1 and next(iter(terms)) >= 1:
                    return latest
            time.sleep(T_POLL)
        self.sync()
        self.check(
            False,
            "the three nodes never agreed on one leader and a non-zero term "
            f"within {timeout:.0f}s; alive nodes: {self.alive_nodes()}",
            observed=latest,
        )
        raise AssertionError("unreachable")

    def install_authority(self, owner: int, epoch: int = EPOCH,
                          node_ids: tuple[int, ...] | list[int] | None = None) -> None:
        targets = [n for n in (node_ids if node_ids is not None else self.alive_nodes())
                   if self.nodes[n].alive]
        for node_id in targets:
            node = self.nodes[node_id]
            start = len(node.history)
            node.send({"authority": {"owner": owner, "epoch": epoch}})
            self.wait_until(
                f"node {node_id} to install authority owner={owner} epoch={epoch}",
                lambda: any(
                    event.get("event") == "authority"
                    and event.get("owner") == owner
                    and event.get("epoch") == epoch
                    for event in node.history[start:]
                ),
                T_APPLIED,
            )

    def kill_node(self, node_id: int) -> None:
        self.nodes[node_id].kill()
        self.journal.append("process_killed", node=node_id)
        self.record_quorum()

    def clean_stop(self, node_id: int, timeout: float = T_RESTART) -> None:
        """Stop one node through its graceful `shutdown` command."""
        node = self.nodes[node_id]
        if node.alive:
            node.send({"shutdown": True})
            self.check(
                node.wait_exit(timeout),
                f"node {node_id} did not stop cleanly within {timeout:.0f}s",
                alive=self.alive_nodes(),
            )
        self.journal.append("process_stopped", node=node_id)
        self.record_quorum()

    def restart_node(self, node_id: int) -> None:
        self.nodes[node_id].start(bootstrap=False)
        self.journal.append("process_restarted", node=node_id)
        self.record_quorum()

    # -- proposals --------------------------------------------------------

    def propose(self, node_id: int, request_id: int, payload: dict,
                timeout: float = T_PROPOSE) -> dict | None:
        """Send one propose command and wait, bounded, for its `proposed` event."""
        node = self.nodes[node_id]
        command = {
            "propose": {
                "request_id": request_id,
                "partition": PARTITION,
                "payload": payload,
                "metadata_epoch": EPOCH,
            }
        }
        self.journal.append(
            "proposal_attempt",
            node=node_id,
            request_id=request_id,
            partition=PARTITION,
            payload_digest=digest_hex(
                json.dumps(payload, sort_keys=True).encode("utf-8")
            ),
        )
        node.outstanding.append(request_id)
        node.send(command)

        deadline = time.monotonic() + timeout
        cursor = node.propose_cursor
        result: dict | None = None
        while time.monotonic() < deadline and result is None:
            while cursor < len(node.history):
                event = node.history[cursor]
                cursor += 1
                if event.get("event") != "proposed":
                    continue
                response = event.get("response")
                response_id = None
                if isinstance(response, dict) and isinstance(response.get("Applied"), dict):
                    response_id = as_int(response["Applied"].get("request_id"))
                expected = node.outstanding[0] if node.outstanding else None
                if expected is None:
                    continue
                if response_id is not None and response_id != expected:
                    # A response for a request we are not waiting for (should not
                    # happen: the node answers stdin commands in order).
                    continue
                node.outstanding.pop(0)
                result = event
                break
            if result is None:
                time.sleep(T_POLL)
        node.propose_cursor = cursor

        applied = applied_of(result)
        ok = applied is not None
        index = applied.get("index") if applied else None
        applied_count = applied.get("applied_count") if applied else None
        if result is None:
            reason = f"no proposed response within {timeout:.0f}s"
        elif not result.get("ok"):
            reason = str(result.get("error") or "the proposal failed")
        elif applied is not None:
            reason = None
        else:
            reason = f"response was not Applied: {json.dumps(result.get('response'))}"
        self.journal.append(
            "proposal_result",
            node=node_id,
            request_id=request_id,
            ok=ok,
            index=index,
            applied_count=applied_count,
            reason=reason,
        )
        return result

    def record_committed_states(self, count: int, request_id: int,
                                context: str = "") -> dict[int, dict]:
        """Journal every live voter's own applied state for a committed request.

        The independent oracle certifies a commit only when a majority of distinct
        nodes report the same committed state and carry the committed request id, so
        the harness records those states immediately after each accepted proposal.
        """
        node_ids = self.alive_nodes()
        if len(node_ids) < MAJORITY:
            return {}
        self.wait_applied_by(
            count,
            request_id,
            node_ids,
            T_APPLIED,
            context=f"the committed state{(' ' + context) if context else ''}",
        )
        for node_id in node_ids:
            self.record_applied(node_id)
        return {node_id: self.applied_view(node_id) for node_id in node_ids}

    def check_applied(self, result: dict | None, request_id: int,
                      count: int | None = None, context: str = "") -> dict:
        label = f"{context} proposal {request_id}".strip()
        self.check(
            result is not None,
            f"{label}: no proposed response within the deadline",
            result=result,
        )
        self.check(
            bool(result.get("ok")),
            f"{label}: the node did not accept the proposal: "
            f"{result.get('error') or json.dumps(result.get('response'))}",
            result=result,
        )
        applied = applied_of(result)
        self.check(
            applied is not None,
            f"{label}: no committed Applied result: {json.dumps(result.get('response'))}",
            result=result,
        )
        self.check(
            as_int(applied.get("request_id")) == request_id,
            f"{label}: the Applied result echoes request id "
            f"{applied.get('request_id')!r} instead of {request_id}",
            applied=applied,
        )
        self.check(
            applied.get("partition") == PARTITION,
            f"{label}: the Applied result names another partition: "
            f"{applied.get('partition')}",
            applied=applied,
        )
        if count is not None:
            self.check(
                applied.get("applied_count") == count,
                f"{label}: applied_count is {applied.get('applied_count')} "
                f"instead of {count}",
                applied=applied,
            )
            self.record_committed_states(count, request_id, context=label)
        return applied

    def require_no_commit(self, result: dict | None, context: str) -> None:
        """A proposal without quorum must time out or error, never succeed."""
        self.check(
            result is None or not result.get("ok"),
            f"{context}: expected a timeout or an error, but the proposal returned "
            f"ok=true ({json.dumps(result.get('response') if isinstance(result, dict) else None)})",
            result=result,
        )

    # -- applied-state waits ---------------------------------------------

    def wait_applied_by(self, count: int, request_id: int, node_ids, timeout: float,
                        context: str = "") -> dict[int, dict]:
        """Wait until every named node's latest applied state carries the command."""

        def done() -> bool:
            for node_id in node_ids:
                view = self.applied_view(node_id)
                partition = view["partition"] if view else None
                if not partition:
                    return False
                if partition.get("count") != count:
                    return False
                if str(partition.get("request_id")) != str(request_id):
                    return False
            return True

        self.wait_until(
            f"nodes {list(node_ids)} to apply command {request_id} "
            f"(count={count}){(' ' + context) if context else ''}",
            done,
            timeout,
        )
        return {node_id: self.applied_view(node_id) for node_id in node_ids}


# -- scenarios ------------------------------------------------------------


def scenario_01(h: Harness) -> None:
    """Fresh bootstrap: a leader is elected and all three agree on leader and term."""
    latest = h.wait_leader_agreement()
    observations = {
        str(node_id): {"leader": event.get("leader"), "term": event.get("term")}
        for node_id, event in sorted(latest.items())
    }
    term = next(iter({event.get("term") for event in latest.values()}))
    h.check(
        term is not None and term >= 1,
        f"the agreed term must be non-zero, observed {term}",
        observations=observations,
    )
    h.check(
        True,
        "all three nodes agree on one leader and one non-zero term",
        observations=observations,
    )
    for node_id in VOTERS:
        h.record_applied(node_id)


def scenario_02(h: Harness) -> None:
    """Three-voter replication: one committed entry reaches all applied states."""
    leader = h.wait_leader()
    leader_id = leader["node"]
    h.install_authority(leader_id)
    result = h.propose(leader_id, 1, {"Inline": [1, 2, 3]})
    h.check_applied(result, 1, count=1, context="the replicated command")
    views = h.wait_applied_by(1, 1, VOTERS, T_APPLIED)
    partitions = {
        str(view["partition"].get("partition")) for view in views.values()
    }
    h.check(
        len(partitions) == 1,
        f"the three applied states must name one partition, observed {sorted(partitions)}",
        partitions=sorted(partitions),
    )
    for node_id in VOTERS:
        view = h.record_applied(node_id)
        h.check(
            view is not None and view["partition"] is not None
            and view["partition"].get("request_id") == "1"
            and view["partition"].get("count") == 1,
            f"node {node_id} applied state must carry request id 1 with count 1",
            view=view,
        )


def scenario_03(h: Harness) -> None:
    """A majority committed the same entry: same index and same chain digest."""
    leader = h.wait_leader()
    leader_id = leader["node"]
    h.install_authority(leader_id)
    result = h.propose(leader_id, 1, {"Inline": [1, 2, 3]})
    h.check_applied(result, 1, count=1, context="the majority command")

    def majority_same() -> bool:
        keys: list[tuple] = []
        for node_id in VOTERS:
            view = h.applied_view(node_id)
            partition = view["partition"] if view else None
            if not partition or partition.get("count") != 1:
                continue
            if str(partition.get("request_id")) != "1":
                continue
            keys.append((node_id, view["applied_index"], partition.get("chain")))
        if len(keys) < MAJORITY:
            return False
        best = 0
        for _, index, chain in keys:
            same = sum(
                1
                for _, other_index, other_chain in keys
                if (other_index, other_chain) == (index, chain)
            )
            best = max(best, same)
        return best >= MAJORITY

    h.wait_until(
        f"{MAJORITY} nodes to share one applied_index and one chain digest",
        majority_same,
        T_APPLIED,
    )
    observed = []
    for node_id in VOTERS:
        view = h.record_applied(node_id)
        partition = view["partition"] if view else None
        if partition and partition.get("count") == 1:
            observed.append(
                {
                    "node": node_id,
                    "applied_index": view["applied_index"],
                    "chain": partition.get("chain"),
                }
            )
    groups: dict[tuple, list[int]] = {}
    for item in observed:
        groups.setdefault((item["applied_index"], item["chain"]), []).append(item["node"])
    best_group = max(groups.items(), key=lambda entry: len(entry[1]))
    h.check(
        len(best_group[1]) >= MAJORITY,
        f"only {len(best_group[1])} nodes share one applied_index and chain",
        observed=observed,
    )
    h.check(
        True,
        "a majority committed the same entry with the same chain digest",
        observed=observed,
        applied_index=best_group[0][0],
        chain=best_group[0][1],
        nodes=best_group[1],
    )


def scenario_04(h: Harness) -> None:
    """Leader SIGKILL after durable commitment: survivors keep the committed prefix."""
    leader = h.wait_leader()
    leader_id = leader["node"]
    h.install_authority(leader_id)
    result = h.propose(leader_id, 1, {"Inline": [1, 2, 3]})
    h.check_applied(result, 1, count=1, context="the command before the kill")

    survivors = [node_id for node_id in VOTERS if node_id != leader_id]
    before = h.wait_applied_by(1, 1, survivors, T_APPLIED, context="before the kill")
    for node_id in survivors:
        h.record_applied(node_id)

    h.kill_node(leader_id)
    h.check(
        all(h.nodes[node_id].alive for node_id in survivors),
        "both followers must survive the leader's SIGKILL",
        alive=h.alive_nodes(),
    )

    # A short bounded observation window: nothing new may appear or disappear.
    deadline = time.monotonic() + 1.5
    while time.monotonic() < deadline:
        h.sync()
        time.sleep(T_POLL)

    for node_id in survivors:
        after = h.applied_view(node_id)
        h.check(
            after is not None and after["partition"] == before[node_id]["partition"],
            f"node {node_id} lost the committed prefix after the leader was killed: "
            f"before={before[node_id]['partition']} after={after and after['partition']}",
            before=before[node_id],
            after=after,
        )
        h.check(
            after is not None
            and after["applied_index"] >= before[node_id]["applied_index"],
            f"node {node_id} applied index went backwards after the kill",
            before_index=before[node_id]["applied_index"],
            after_index=after and after["applied_index"],
        )
        h.record_applied(node_id)
    h.check(
        True,
        "both survivors kept the committed prefix after the leader SIGKILL",
        before={str(n): before[n] for n in survivors},
        after={str(n): h.applied_view(n) for n in survivors},
    )


def scenario_05(h: Harness) -> None:
    """Replacement leader retains the committed prefix through a second command."""
    leader = h.wait_leader()
    old_leader = leader["node"]
    old_term = leader["term"]
    h.install_authority(old_leader)
    result = h.propose(old_leader, 1, {"Inline": [1, 2, 3]})
    h.check_applied(result, 1, count=1, context="the command before the failover")

    survivors = [node_id for node_id in VOTERS if node_id != old_leader]
    prefix = h.wait_applied_by(1, 1, survivors, T_APPLIED, context="before the kill")
    for node_id in survivors:
        h.record_applied(node_id)

    h.kill_node(old_leader)
    replacement = h.wait_leader(after_term=old_term, exclude=old_leader)
    new_leader = replacement["node"]
    h.check(
        replacement.get("term", 0) > old_term,
        f"the replacement must win a later term than {old_term}",
        replacement=replacement,
    )
    alive = h.alive_nodes()
    h.check(
        new_leader in alive,
        f"the replacement leader {new_leader} must be a survivor",
        alive=alive,
    )

    prefix_state = h.wait_applied_by(1, 1, alive, T_APPLIED, context="after the failover")
    h.install_authority(new_leader)
    second = h.propose(new_leader, 2, {"Inline": [4, 5, 6]})
    h.check_applied(second, 2, count=2, context="the post-failover command")
    views = h.wait_applied_by(2, 2, alive, T_APPLIED, context="the second command")

    for node_id in alive:
        partition = views[node_id]["partition"]
        first_chain = prefix_state[node_id]["partition"].get("chain")
        h.check(
            partition.get("count") == 2,
            f"node {node_id} must retain the first command (count 2), "
            f"observed {partition.get('count')}",
            partition=partition,
        )
        h.check(
            partition.get("chain") != first_chain,
            f"node {node_id} chain did not advance past the first command",
            first_chain=first_chain,
            second_chain=partition.get("chain"),
        )
        h.record_applied(node_id)
    h.check(
        True,
        "the new committed state contains the first command and the second",
        prefix={str(n): prefix_state[n]["partition"] for n in alive},
        current={str(n): views[n]["partition"] for n in alive},
        replacement=replacement,
    )


def scenario_06(h: Harness) -> None:
    """One follower unavailable: 2/3 majority still commits and replicates."""
    leader = h.wait_leader()
    leader_id = leader["node"]
    h.install_authority(leader_id)
    stopped = next(node_id for node_id in VOTERS if node_id != leader_id)
    h.kill_node(stopped)
    h.check(
        MAJORITY <= len(h.alive_nodes()),
        "the cluster must still hold a majority after one follower stops",
        alive=h.alive_nodes(),
    )

    result = h.propose(leader_id, 1, {"Inline": [1, 2, 3]})
    h.check_applied(result, 1, count=1, context="the command with one follower down")
    alive = h.alive_nodes()
    views = h.wait_applied_by(1, 1, alive, T_APPLIED, context="with one follower down")
    for node_id in alive:
        h.record_applied(node_id)
    h.check(
        views[leader_id]["partition"] == views[
            next(n for n in alive if n != leader_id)
        ]["partition"],
        "the leader and the surviving follower must share one applied state",
        views={str(n): views[n]["partition"] for n in alive},
    )
    h.check(
        True,
        "a 2/3 majority committed the command while one follower was unavailable",
        stopped_node=stopped,
        views={str(n): views[n]["partition"] for n in alive},
    )


def scenario_07(h: Harness) -> None:
    """Two voters unavailable: no new committed entry, applied index unchanged."""
    leader = h.wait_leader()
    leader_id = leader["node"]
    h.install_authority(leader_id)
    baseline = h.propose(leader_id, 1, {"Inline": [1, 2, 3]})
    h.check_applied(baseline, 1, count=1, context="the baseline command")
    h.wait_applied_by(1, 1, VOTERS, T_APPLIED, context="the baseline on all voters")

    for node_id in [n for n in VOTERS if n != leader_id]:
        h.kill_node(node_id)
    h.check(
        h.alive_nodes() == [leader_id],
        f"only the leader must remain alive, observed {h.alive_nodes()}",
        alive=h.alive_nodes(),
    )
    h.check(
        len(h.alive_nodes()) < MAJORITY,
        "a lone voter must not hold a majority",
        alive=h.alive_nodes(),
        quorum=MAJORITY,
    )

    before = h.applied_view(leader_id)
    h.check(
        before is not None and before["partition"] is not None,
        "the leader must have an applied state before the minority proposal",
        before=before,
    )
    result = h.propose(leader_id, 2, {"Inline": [4, 5, 6]},
                       timeout=T_PROPOSE_NO_QUORUM)
    h.require_no_commit(result, "the lone leader")

    after = h.applied_view(leader_id)
    h.check(
        after is not None and after["applied_index"] == before["applied_index"],
        f"the leader's applied index advanced without a quorum: "
        f"{before['applied_index']} -> {after and after['applied_index']}",
        before=before,
        after=after,
    )
    h.check(
        after["partition"] == before["partition"],
        "the leader's applied partition state changed without a quorum",
        before=before["partition"],
        after=after["partition"],
    )
    for node_id in VOTERS:
        bad = [
            event
            for event in snapshot(h.nodes[node_id])
            if event.get("event") == "applied"
            and any(
                str(partition.get("request_id")) == "2"
                for partition in (event.get("partitions") or [])
            )
        ]
        h.check(
            not bad,
            f"node {node_id} applied the uncommitted command 2",
            violations=bad,
        )
    h.check(
        h.nodes[leader_id].alive,
        "the leader process must survive the minority window",
        alive=h.alive_nodes(),
    )
    h.check(
        True,
        "no new committed entry appeared while two voters were unavailable",
        before=before,
        after=after,
        proposal=result,
    )


def _establish_divergence(h: Harness) -> dict:
    """Build the scenario-8/9 state and return the observed facts.

    Committed prefix, an uncommitted tail on a SIGKILLed old leader, a restarted
    majority that elects a new leader and commits a further command, then the old
    leader rejoining and converging onto that committed state.
    """
    leader = h.wait_leader()
    old_leader = leader["node"]
    old_term = leader["term"]
    h.install_authority(old_leader)
    first = h.propose(old_leader, 1, {"Inline": [1, 2, 3]})
    h.check_applied(first, 1, count=1, context="the committed prefix")
    h.wait_applied_by(1, 1, VOTERS, T_APPLIED, context="the prefix on all voters")
    for node_id in VOTERS:
        h.record_applied(node_id)

    followers = [node_id for node_id in VOTERS if node_id != old_leader]
    for node_id in followers:
        h.kill_node(node_id)
    h.check(
        h.alive_nodes() == [old_leader],
        "only the old leader must remain for the divergent tail",
        alive=h.alive_nodes(),
    )

    # The lone leader appends a command it can never commit.
    divergent = h.propose(old_leader, 2, {"Inline": [4, 5, 6]},
                          timeout=T_PROPOSE_NO_QUORUM)
    h.require_no_commit(divergent, "the lone old leader")

    h.kill_node(old_leader)
    for node_id in followers:
        h.restart_node(node_id)

    replacement = h.wait_leader(after_term=old_term, exclude=old_leader,
                                timeout=T_RESTART)
    new_leader = replacement["node"]
    h.check(
        new_leader in followers,
        f"the new leader {new_leader} must come from the restarted followers",
        followers=followers,
        replacement=replacement,
    )
    recovered = h.wait_applied_by(1, 1, followers, T_APPLIED,
                                  context="the restarted majority")
    for node_id in followers:
        h.record_recovered(node_id)
        h.check(
            recovered[node_id]["partition"].get("count") == 1,
            f"node {node_id} must recover the committed prefix after restart",
            view=recovered[node_id],
        )

    h.install_authority(new_leader, node_ids=followers)
    third = h.propose(new_leader, 3, {"Inline": [7, 8, 9]})
    h.check_applied(third, 3, count=2, context="the post-failover command")
    h.wait_applied_by(2, 3, followers, T_APPLIED, context="on the new majority")

    # The old leader rejoins with its uncommitted tail still on disk.
    h.restart_node(old_leader)
    h.install_authority(new_leader, node_ids=[old_leader])
    h.wait_applied_by(2, 3, [old_leader], T_RESTART, context="the old leader's catch-up")
    h.record_recovered(old_leader)

    h.wait_until(
        "all three nodes to share one committed state",
        lambda: _identical_committed_state(h),
        T_RESTART,
    )
    views = {node_id: h.applied_view(node_id) for node_id in VOTERS}
    return {
        "old_leader": old_leader,
        "old_term": old_term,
        "new_leader": new_leader,
        "replacement": replacement,
        "views": views,
        "divergent": divergent,
    }


def _identical_committed_state(h: Harness) -> bool:
    views = [h.applied_view(node_id) for node_id in VOTERS]
    if any(view is None or view["partition"] is None for view in views):
        return False
    partitions = {json.dumps(view["partition"], sort_keys=True) for view in views}
    indices = {view["applied_index"] for view in views}
    return len(partitions) == 1 and len(indices) == 1


def _committed_request_ids(h: Harness) -> list[tuple[int, str]]:
    """Every request id that ever appeared in any node's applied state."""
    seen: list[tuple[int, str]] = []
    for node_id in VOTERS:
        for event in snapshot(h.nodes[node_id]):
            if event.get("event") != "applied":
                continue
            for partition in event.get("partitions") or []:
                request_id = partition.get("request_id")
                if request_id is not None:
                    seen.append((node_id, str(request_id)))
    return seen


def scenario_08(h: Harness) -> None:
    """Old leader reconnects with an uncommitted divergent tail."""
    facts = _establish_divergence(h)
    applied_ids = _committed_request_ids(h)
    violations = [item for item in applied_ids if item[1] == "2"]
    h.check(
        not violations,
        "the old leader's uncommitted request 2 appeared in a committed applied state",
        violations=violations,
    )
    old_view = facts["views"][facts["old_leader"]]
    h.check(
        old_view["partition"].get("request_id") == "3"
        and old_view["partition"].get("count") == 2,
        "the rejoined old leader must serve the committed state, "
        f"observed {old_view['partition']}",
        view=old_view,
    )
    h.check(
        True,
        "the old leader rejoined without its uncommitted entry ever committing",
        old_leader=facts["old_leader"],
        new_leader=facts["new_leader"],
        views={str(n): facts["views"][n] for n in VOTERS},
        applied_request_ids=sorted({rid for _, rid in applied_ids}),
    )


def scenario_09(h: Harness) -> None:
    """Divergent uncommitted tail never becomes committed; chains stay identical."""
    facts = _establish_divergence(h)
    views = facts["views"]
    chains = {node_id: views[node_id]["partition"].get("chain") for node_id in VOTERS}
    counts = {node_id: views[node_id]["partition"].get("count") for node_id in VOTERS}
    request_ids = {
        node_id: str(views[node_id]["partition"].get("request_id")) for node_id in VOTERS
    }
    h.check(
        len(set(chains.values())) == 1,
        f"the committed chain must be identical on all three nodes, observed {chains}",
        chains={str(k): v for k, v in chains.items()},
    )
    h.check(
        len({views[n]["applied_index"] for n in VOTERS}) == 1,
        "the committed applied index must be identical on all three nodes",
        indices={str(n): views[n]["applied_index"] for n in VOTERS},
    )
    h.check(
        all(count == 2 for count in counts.values()),
        f"every node must carry exactly the two committed commands, observed {counts}",
        counts={str(k): v for k, v in counts.items()},
    )
    applied_ids = _committed_request_ids(h)
    violations = [item for item in applied_ids if item[1] == "2"]
    h.check(
        not violations,
        "the old leader's uncommitted request id 2 must never appear in an "
        "applied state",
        violations=violations,
    )
    h.check(
        all(rid != "2" for rid in request_ids.values()),
        f"an applied state still names the uncommitted request 2: {request_ids}",
        request_ids={str(k): v for k, v in request_ids.items()},
    )
    h.check(
        True,
        "after the old leader rejoined all three committed chains are identical "
        "and exclude the uncommitted tail",
        chains={str(k): v for k, v in chains.items()},
        counts={str(k): v for k, v in counts.items()},
        request_ids={str(k): v for k, v in request_ids.items()},
        applied_index=views[1]["applied_index"],
        old_leader=facts["old_leader"],
        new_leader=facts["new_leader"],
    )


def scenario_10(h: Harness) -> None:
    """Follower restart and log catch-up onto the leader's committed state."""
    leader = h.wait_leader()
    leader_id = leader["node"]
    h.install_authority(leader_id)
    stopped = next(node_id for node_id in VOTERS if node_id != leader_id)
    h.kill_node(stopped)

    first = h.propose(leader_id, 1, {"Inline": [1, 2, 3]})
    h.check_applied(first, 1, count=1, context="the first command")
    second = h.propose(leader_id, 2, {"Inline": [4, 5, 6]})
    h.check_applied(second, 2, count=2, context="the second command")

    leader_view = h.wait_applied_by(2, 2, [leader_id], T_APPLIED,
                                    context="the leader's committed state")[leader_id]
    h.record_applied(leader_id)
    surviving = [n for n in VOTERS if n not in (leader_id, stopped)]
    h.wait_applied_by(2, 2, surviving, T_APPLIED, context="the surviving follower")
    for node_id in surviving:
        h.record_applied(node_id)

    h.restart_node(stopped)
    h.install_authority(leader_id, node_ids=[stopped])
    h.wait_applied_by(2, 2, [stopped], T_RESTART, context="the restarted follower's catch-up")
    caught_up = h.record_recovered(stopped)
    h.record_applied(leader_id)

    h.check(
        caught_up["partition"] == leader_view["partition"],
        "the restarted follower must reach the leader's committed state: "
        f"leader={leader_view['partition']} follower="
        f"{caught_up['partition']}",
        leader=leader_view,
        follower=caught_up,
    )
    h.check(
        caught_up["applied_index"] == leader_view["applied_index"],
        "the restarted follower's applied index must match the leader's",
        leader_index=leader_view["applied_index"],
        follower_index=caught_up["applied_index"],
    )
    h.check(
        True,
        "the restarted follower caught up to the leader's committed counts and chains",
        leader=leader_view,
        follower=caught_up,
        restarted_node=stopped,
    )


def scenario_11(h: Harness) -> None:
    """Snapshot creation compacts the leader's committed log prefix."""
    leader = h.wait_leader()
    leader_id = leader["node"]
    h.install_authority(leader_id)
    for request_id in (1, 2, 3):
        result = h.propose(leader_id, request_id, {"Inline": [request_id]})
        h.check_applied(result, request_id, count=request_id,
                        context="the pre-snapshot command")
    views = h.wait_applied_by(3, 3, VOTERS, T_APPLIED, context="before the snapshot")
    for node_id in VOTERS:
        h.record_applied(node_id)

    log_path = h.nodes[leader_id].directory / "raft" / "raft-log.bin"
    before = inspect_raft_log(log_path)
    h.check(
        before["magic"] == Q2_LOG_MAGIC.decode("latin-1"),
        f"the leader's Raft log must carry the QRL1 magic, observed {before['magic']!r}",
        log={key: before[key] for key in ("magic", "bytes", "first_index", "last_index")},
    )
    h.check(
        before["first_index"] is not None,
        f"the leader's Raft log has no stored entry to compact: {before}",
        log=before,
    )

    h.nodes[leader_id].send({"snapshot": True})
    h.wait_until(
        "the leader to publish a snapshot that includes the committed prefix",
        lambda: any(
            (event.get("included_index") or 0) >= 3
            for event in snapshot_events(h.nodes[leader_id])
        ),
        T_APPLIED,
    )
    h.wait_until(
        "the leader's Raft log to compact past its first stored index",
        lambda: (inspect_raft_log(log_path)["first_index"] or 0)
        > (before["first_index"] or 0),
        T_APPLIED,
    )
    after = inspect_raft_log(log_path)
    h.journal.append(
        "compaction_observed",
        node=leader_id,
        log_bytes_before=before["bytes"],
        log_bytes_after=after["bytes"],
        first_index_hint=after["first_index"],
    )
    h.check(
        after["first_index"] > before["first_index"],
        f"the leader's log first stored index did not advance: "
        f"{before['first_index']} -> {after['first_index']}",
        first_index_before=before["first_index"],
        first_index_after=after["first_index"],
    )
    h.check(
        after["bytes"] <= before["bytes"],
        f"the compacted log grew from {before['bytes']} to {after['bytes']} bytes",
        bytes_before=before["bytes"],
        bytes_after=after["bytes"],
    )

    # The compacted state is durable: a voter that restarts recovers it unchanged.
    voter = next(node_id for node_id in VOTERS if node_id != leader_id)
    cursor = len(h.nodes[voter].history)
    h.clean_stop(voter)
    h.restart_node(voter)

    def recovered() -> bool:
        view = h.applied_view_from(voter, cursor)
        partition = view["partition"] if view else None
        return (
            bool(partition)
            and partition.get("count") == 3
            and partition.get("chain") == views[leader_id]["partition"].get("chain")
        )

    h.wait_until(
        f"node {voter} to recover the snapshotted committed state",
        recovered,
        T_RESTART,
    )
    recovered_view = h.record_recovered(voter)
    h.check(
        True,
        "the snapshot compacted the leader's committed prefix and a restarted "
        "voter recovered the same state",
        node=leader_id,
        first_index_before=before["first_index"],
        first_index_after=after["first_index"],
        log_bytes_before=before["bytes"],
        log_bytes_after=after["bytes"],
        restarted_voter=voter,
        recovered=recovered_view,
        chain=views[leader_id]["partition"].get("chain"),
    )


def scenario_12(h: Harness) -> None:
    """A lagging follower catches up through snapshot installation, not log replay."""
    leader = h.wait_leader()
    leader_id = leader["node"]
    h.install_authority(leader_id)
    first = h.propose(leader_id, 1, {"Inline": [1]})
    h.check_applied(first, 1, count=1, context="the pre-stop command")
    h.wait_applied_by(1, 1, VOTERS, T_APPLIED, context="before the follower stops")

    stopped = next(node_id for node_id in VOTERS if node_id != leader_id)
    baseline_view = h.applied_view(stopped)
    baseline = baseline_view["applied_index"]
    h.kill_node(stopped)
    h.check(
        MAJORITY <= len(h.alive_nodes()),
        "a majority must remain for the leader to commit and compact",
        alive=h.alive_nodes(),
    )
    h.check(
        baseline is not None,
        f"the stopped follower must have an applied index before it is stopped: "
        f"{baseline_view}",
        view=baseline_view,
    )

    log_path = h.nodes[leader_id].directory / "raft" / "raft-log.bin"
    count = 1
    request_id = 1
    compacted: dict | None = None
    for _ in range(40):
        request_id += 1
        count += 1
        result = h.propose(leader_id, request_id, {"Inline": [request_id]})
        h.check_applied(result, request_id, count=count,
                        context="the lagging-follower command")
        info = inspect_raft_log(log_path)
        if info["first_index"] is not None and info["first_index"] > baseline:
            compacted = info
            break
    h.check(
        compacted is not None,
        f"the leader never compacted past the stopped follower's applied index "
        f"{baseline}; last first stored index "
        f"{inspect_raft_log(log_path)['first_index']}",
        baseline=baseline,
        log=inspect_raft_log(log_path),
    )
    included = max(
        (event.get("included_index") or 0 for event in snapshot_events(h.nodes[leader_id])),
        default=0,
    )
    h.check(
        included > baseline,
        f"the leader's snapshot (included_index={included}) must include the entries "
        f"the stopped follower is missing (applied index {baseline})",
        included_index=included,
        baseline=baseline,
    )
    leader_view = h.applied_view(leader_id)
    h.journal.append(
        "compaction_observed",
        node=leader_id,
        log_bytes_before=None,
        log_bytes_after=compacted["bytes"],
        first_index_hint=compacted["first_index"],
    )

    cursor = len(h.nodes[stopped].history)
    h.restart_node(stopped)
    h.install_authority(leader_id, node_ids=[stopped])
    h.wait_applied_by(count, request_id, [stopped], T_RESTART,
                      context="the lagging follower's catch-up")
    h.wait_until(
        "a snapshot installation on the lagging follower",
        lambda: any(
            event.get("event") == "snapshot"
            for event in h.nodes[stopped].history[cursor:]
        ),
        T_RESTART,
    )
    installed = [
        event
        for event in h.nodes[stopped].history[cursor:]
        if event.get("event") == "snapshot"
    ][-1]
    h.journal.append(
        "snapshot_installed",
        node=stopped,
        included_index=installed.get("included_index"),
    )
    caught_up = h.applied_view(stopped)
    h.check(
        caught_up["partition"] == leader_view["partition"],
        "the restarted follower must reach the leader's committed partition "
        f"(count and chain): leader={leader_view['partition']} "
        f"follower={caught_up['partition']}",
        leader=leader_view,
        follower=caught_up,
    )
    h.check(
        caught_up["applied_index"] == leader_view["applied_index"],
        "the restarted follower's applied index must match the leader's",
        leader_index=leader_view["applied_index"],
        follower_index=caught_up["applied_index"],
    )
    h.check(
        True,
        "the lagging follower recovered through a snapshotted prefix the leader had "
        "already compacted, and a snapshot install event was observed",
        baseline=baseline,
        leader_first_index=compacted["first_index"],
        snapshot_included_index=installed.get("included_index"),
        leader=leader_view,
        follower=caught_up,
    )


def scenario_13(h: Harness) -> None:
    """All three nodes restart from their original directories and keep one state."""
    leader = h.wait_leader()
    leader_id = leader["node"]
    h.install_authority(leader_id)
    for request_id in (1, 2, 3):
        result = h.propose(leader_id, request_id, {"Inline": [request_id]})
        h.check_applied(result, request_id, count=request_id,
                        context="the restart-durability command")
    h.wait_applied_by(3, 3, VOTERS, T_APPLIED, context="before the restart")
    before = {node_id: h.applied_view(node_id) for node_id in VOTERS}
    for node_id in VOTERS:
        h.record_applied(node_id)
    prior_term = latest_term(h)

    for node_id in VOTERS:
        h.clean_stop(node_id)
    h.check(
        not any(node.alive for node in h.nodes.values()),
        "all three nodes must be stopped before the restart",
        alive=h.alive_nodes(),
    )

    cursors = {node_id: len(h.nodes[node_id].history) for node_id in VOTERS}
    for node_id in VOTERS:
        h.restart_node(node_id)

    def agreement_after_restart() -> bool:
        latest: dict[int, dict] = {}
        for node_id in VOTERS:
            events = [
                event
                for event in h.nodes[node_id].history[cursors[node_id]:]
                if event.get("event") in ("leader", "follower")
            ]
            if not events:
                return False
            latest[node_id] = events[-1]
        leaders = {event.get("leader") for event in latest.values()}
        terms = {event.get("term") for event in latest.values()}
        return len(leaders) == 1 and len(terms) == 1 and next(iter(terms)) >= 1

    h.wait_until(
        "all three restarted nodes to agree on one leader and one term",
        agreement_after_restart,
        T_RESTART,
    )
    agreement = {}
    for node_id in VOTERS:
        events = [
            event
            for event in h.nodes[node_id].history[cursors[node_id]:]
            if event.get("event") in ("leader", "follower")
        ]
        agreement[str(node_id)] = {
            "leader": events[-1].get("leader"),
            "term": events[-1].get("term"),
        }

    def recovered_all() -> bool:
        for node_id in VOTERS:
            view = h.applied_view_from(node_id, cursors[node_id])
            partition = view["partition"] if view else None
            if not partition or partition != before[node_id]["partition"]:
                return False
            if view["applied_index"] != before[node_id]["applied_index"]:
                return False
        return True

    h.wait_until(
        "every restarted node to recover the pre-restart committed state",
        recovered_all,
        T_RESTART,
    )
    after = {node_id: h.applied_view_from(node_id, cursors[node_id])
             for node_id in VOTERS}
    for node_id in VOTERS:
        h.record_recovered(node_id)
        h.check(
            after[node_id]["partition"] == before[node_id]["partition"],
            f"node {node_id} changed its committed state across the restart: "
            f"before={before[node_id]['partition']} after={after[node_id]['partition']}",
            before=before[node_id],
            after=after[node_id],
        )
    h.check(
        True,
        "all three nodes restarted from their original directories and kept "
        "identical committed counts, request ids and chain digests",
        prior_term=prior_term,
        agreement=agreement,
        before={str(k): v for k, v in before.items()},
        after={str(k): v for k, v in after.items()},
    )


def scenario_14(h: Harness) -> None:
    """A torn uncommitted tail is either refused or recovered without changing history."""
    leader = h.wait_leader()
    leader_id = leader["node"]
    h.install_authority(leader_id)
    for request_id in (1, 2):
        result = h.propose(leader_id, request_id, {"Inline": [request_id]})
        h.check_applied(result, request_id, count=request_id,
                        context="the pre-corruption command")
    h.wait_applied_by(2, 2, VOTERS, T_APPLIED, context="before the corruption")
    leader_view = h.applied_view(leader_id)

    target = next(node_id for node_id in VOTERS if node_id != leader_id)
    survivor = next(node_id for node_id in VOTERS
                    if node_id not in (leader_id, target))
    h.kill_node(target)

    log_path = h.nodes[target].directory / "raft" / "raft-log.bin"
    before = inspect_raft_log(log_path)
    h.check(
        before["magic"] == Q2_LOG_MAGIC.decode("latin-1") and bool(before["entries"]),
        f"the stopped follower's Raft log must hold the committed prefix: {before}",
        log=before,
    )
    offset = log_path.stat().st_size
    with open(log_path, "ab") as handle:
        handle.write(bytes.fromhex("deadbeef") + b"\x00" * 64)
    h.journal.append(
        "corruption_injected",
        node=target,
        file="raft/raft-log.bin",
        offset=offset,
        kind="uncommitted-tail-garbage",
    )

    cursor = len(h.nodes[target].history)
    raw_cursor = len(h.nodes[target].raw_lines)
    h.restart_node(target)
    deadline = time.monotonic() + 25.0
    outcome: str | None = None
    recovered: dict | None = None
    while time.monotonic() < deadline:
        h.sync()
        process = h.nodes[target].process
        if process is not None and process.poll() is not None:
            outcome = "refused"
            break
        view = h.applied_view_from(target, cursor)
        partition = view["partition"] if view else None
        if partition and partition.get("count") == 2:
            recovered = view
            same = partition.get("chain") == leader_view["partition"].get("chain")
            outcome = "recovered" if same else "altered"
            break
        time.sleep(T_POLL)
    h.check(
        outcome is not None,
        "the torn follower neither refused to serve nor recovered the committed "
        f"prefix within 25s; alive={h.alive_nodes()}",
    )
    h.check(
        outcome != "altered",
        "the torn follower started and served a committed state that differs from "
        f"the leader's: {recovered and recovered['partition']}",
        recovered=recovered,
    )

    errors = h.nodes[target].error_lines(raw_cursor)
    if outcome == "refused":
        code = h.nodes[target].process.returncode
        h.check(
            code not in (None, 0),
            f"a refusing node must exit non-zero, observed {code}",
            exit_code=code,
            errors=errors,
        )
        h.check(
            bool(errors) and any("shard log" in line for line in errors),
            f"the refusal must carry an explicit shard-log error, observed {errors}",
            errors=errors,
        )
        classification = "refused_to_serve"
    else:
        h.check(
            recovered["partition"] == leader_view["partition"],
            "the recovered follower must match the leader's committed counts and chain",
            leader=leader_view,
            recovered=recovered,
        )
        classification = "recovered_and_matched_committed_prefix"

    after = inspect_raft_log(log_path)
    h.check(
        [entry["index"] for entry in after["entries"]]
        == [entry["index"] for entry in before["entries"]],
        "the committed prefix of the torn log changed: "
        f"{[e['index'] for e in before['entries']]} -> "
        f"{[e['index'] for e in after['entries']]}",
        before_entries=before["entries"],
        after_entries=after["entries"],
    )
    for node_id in (leader_id, survivor):
        live = h.applied_view(node_id)
        h.check(
            live["partition"] == leader_view["partition"],
            f"node {node_id} lost the committed prefix while the torn follower "
            f"recovered: {live['partition']}",
            live=live,
        )
    h.journal.append(
        "outcome_classified", scenario=14, classification=classification
    )
    h.check(
        True,
        f"garbage on the uncommitted tail was handled as {classification} and the "
        "committed prefix never changed",
        classification=classification,
        exit_code=h.nodes[target].process.returncode,
        error_lines=errors[:6],
        committed_prefix_indexes=[entry["index"] for entry in after["entries"]],
    )


def scenario_15(h: Harness) -> None:
    """Corruption inside a committed record fails closed and never serves altered history."""
    leader = h.wait_leader()
    leader_id = leader["node"]
    h.install_authority(leader_id)
    for request_id in (1, 2):
        result = h.propose(leader_id, request_id, {"Inline": [request_id]})
        h.check_applied(result, request_id, count=request_id,
                        context="the pre-corruption command")
    h.wait_applied_by(2, 2, VOTERS, T_APPLIED, context="before the corruption")
    committed = h.applied_view(leader_id)

    target = next(node_id for node_id in VOTERS if node_id != leader_id)
    h.clean_stop(target)
    log_path = h.nodes[target].directory / "raft" / "raft-log.bin"
    before = inspect_raft_log(log_path)
    h.check(
        bool(before["entries"]),
        f"the stopped node must hold committed log records: {before}",
        log=before,
    )
    record = before["entries"][0]
    data = bytearray(log_path.read_bytes())
    position = record["offset"] + 8 + 5
    if position >= record["offset"] + 8 + record["length"]:
        position = record["offset"] + 8
    data[position] ^= 0xFF
    log_path.write_bytes(bytes(data))
    h.journal.append(
        "corruption_injected",
        node=target,
        file="raft/raft-log.bin",
        offset=position,
        kind="committed-record-payload",
    )

    cursor = len(h.nodes[target].history)
    raw_cursor = len(h.nodes[target].raw_lines)
    h.restart_node(target)
    h.check(
        h.nodes[target].wait_exit(25.0),
        "the node did not stop after a checksum failure inside a committed record; "
        f"alive={h.alive_nodes()}",
        alive=h.alive_nodes(),
    )
    poll_deadline = time.monotonic() + 2.0
    while time.monotonic() < poll_deadline and not h.nodes[target].error_lines(raw_cursor):
        time.sleep(T_POLL)
    code = h.nodes[target].process.returncode
    errors = h.nodes[target].error_lines(raw_cursor)
    h.check(
        code not in (None, 0),
        f"a node with a corrupted committed record must exit non-zero, observed {code}",
        exit_code=code,
        errors=errors,
    )
    h.check(
        bool(errors) and any("shard log" in line for line in errors),
        f"the refusal must carry an explicit shard-log error, observed {errors}",
        errors=errors,
    )
    presented = [
        event
        for event in h.nodes[target].history[cursor:]
        if event.get("event") == "applied"
    ]
    h.check(
        not presented,
        f"the refusing node presented an applied state after the corruption: "
        f"{presented}",
        presented=presented,
    )
    for node_id in VOTERS:
        if node_id == target:
            continue
        live = h.applied_view(node_id)
        h.check(
            live["partition"] == committed["partition"],
            f"node {node_id} lost committed history while the corrupt node refused",
            live=live,
        )
    h.journal.append(
        "outcome_classified", scenario=15, classification="committed_corruption_failed_closed"
    )
    h.check(
        True,
        "a byte corrupted inside the first committed record made the node refuse to "
        "start with an explicit checksum error and present no altered history",
        exit_code=code,
        error_line=errors[0] if errors else None,
        corrupted_record_index=record["index"],
        corrupted_offset=position,
        survivors={str(n): h.applied_view(n) for n in VOTERS if n != target},
    )


def scenario_16(h: Harness) -> None:
    """A SIGKILLed leader can never fabricate a committed result that only it saw."""
    leader = h.wait_leader()
    leader_id = leader["node"]
    h.install_authority(leader_id)
    first = h.propose(leader_id, 1, {"Inline": [1, 2, 3]})
    h.check_applied(first, 1, count=1, context="the prefix before the interruption")
    h.wait_applied_by(1, 1, VOTERS, T_APPLIED, context="before the interruption")
    prior_term = latest_term(h)

    request_id = 2
    payload = {"Inline": [4, 5, 6]}
    node = h.nodes[leader_id]
    h.journal.append(
        "proposal_attempt",
        node=leader_id,
        request_id=request_id,
        partition=PARTITION,
        payload_digest=digest_hex(json.dumps(payload, sort_keys=True).encode("utf-8")),
    )
    node.send(
        {
            "propose": {
                "request_id": request_id,
                "partition": PARTITION,
                "payload": payload,
                "metadata_epoch": EPOCH,
            }
        }
    )
    # The command may have been consumed by the node, but its result is never read.
    time.sleep(0.005)
    h.kill_node(leader_id)
    h.check(not node.alive, "the leader must be dead after the SIGKILL",
            alive=h.alive_nodes())

    old_cursor = len(node.history)
    h.restart_node(leader_id)

    h.wait_until(
        f"the restarted node {leader_id} to report a leader after rejoining",
        lambda: any(
            event.get("event") in ("leader", "follower")
            for event in node.history[old_cursor:]
        ),
        T_RESTART,
    )

    def unanimous() -> bool:
        views = {}
        for node_id in VOTERS:
            if node_id == leader_id:
                views[node_id] = h.applied_view_from(node_id, old_cursor)
            else:
                views[node_id] = h.applied_view(node_id)
        if any(view is None or view["partition"] is None for view in views.values()):
            return False
        partitions = {
            json.dumps(view["partition"], sort_keys=True) for view in views.values()
        }
        indices = {view["applied_index"] for view in views.values()}
        return len(partitions) == 1 and len(indices) == 1

    h.wait_until(
        "all three nodes to expose one committed state after the interrupted append",
        unanimous,
        T_RESTART,
    )
    views = {
        node_id: (
            h.applied_view_from(node_id, old_cursor)
            if node_id == leader_id
            else h.applied_view(node_id)
        )
        for node_id in VOTERS
    }
    for node_id in VOTERS:
        h.record_applied(node_id)
    counts = {node_id: views[node_id]["partition"].get("count") for node_id in VOTERS}
    names = {
        node_id: str(views[node_id]["partition"].get("request_id")) for node_id in VOTERS
    }
    present = [
        node_id
        for node_id in VOTERS
        if names[node_id] == str(request_id) and counts[node_id] == 2
    ]
    reported = [
        (node_id, event)
        for node_id in VOTERS
        for event in snapshot(h.nodes[node_id])
        if event.get("event") == "proposed"
        and (body := applied_of(event)) is not None
        and str(body.get("request_id")) == str(request_id)
    ]
    if len(present) == len(VOTERS):
        classification = "committed"
    elif not present:
        classification = "definitely-absent"
        h.check(
            all(counts[node_id] == 1 for node_id in VOTERS),
            f"the command is absent but a node's count advanced: {counts}",
            counts={str(k): v for k, v in counts.items()},
        )
        h.check(
            not reported,
            "the command was reported as committed although it is absent from every "
            f"committed state: {reported}",
            reported=reported,
        )
    else:
        h.check(
            False,
            f"the interrupted command is present on only {present} of {list(VOTERS)} "
            f"(counts={counts}, request_ids={names})",
            present=present,
            counts={str(k): v for k, v in counts.items()},
            request_ids={str(k): v for k, v in names.items()},
        )
        raise AssertionError("unreachable")  # check() always raises
    h.journal.append(
        "outcome_classified", scenario=16, classification=classification
    )
    h.check(
        True,
        f"the interrupted command was {classification} on every node, never on one "
        "node only and never reported committed while absent",
        classification=classification,
        prior_term=prior_term,
        counts={str(k): v for k, v in counts.items()},
        request_ids={str(k): v for k, v in names.items()},
        counts_by_node={str(k): v for k, v in views.items()},
        reported_responses=len(reported),
    )


def scenario_17(h: Harness) -> None:
    """A corrupted snapshot and a foreign snapshot are both refused without state loss."""
    leader = h.wait_leader()
    leader_id = leader["node"]
    h.install_authority(leader_id)
    for request_id in (1, 2, 3):
        result = h.propose(leader_id, request_id, {"Inline": [request_id]})
        h.check_applied(result, request_id, count=request_id,
                        context="the snapshotted command")
    h.wait_applied_by(3, 3, VOTERS, T_APPLIED, context="before the snapshot")
    h.nodes[leader_id].send({"snapshot": True})
    h.wait_until(
        "the leader to publish a snapshot for the corruption test",
        lambda: bool(snapshot_events(h.nodes[leader_id])),
        T_APPLIED,
    )
    target = leader_id
    state_dir = h.nodes[target].directory / "state"
    snapshot_path = state_dir / "snapshot.json"
    meta_path = state_dir / "snapshot-meta.json"
    h.check(
        snapshot_path.exists() and meta_path.exists(),
        f"the snapshot document and its metadata must exist under {state_dir}",
        state_files=sorted(path.name for path in state_dir.iterdir()),
    )
    snapshot_bytes = snapshot_path.read_bytes()
    meta_bytes = meta_path.read_bytes()
    before = {node_id: h.applied_view(node_id) for node_id in VOTERS}
    h.clean_stop(target)

    data = bytearray(snapshot_bytes)
    position = max(1, len(data) // 2)
    data[position] ^= 0x01
    snapshot_path.write_bytes(bytes(data))
    h.journal.append(
        "corruption_injected",
        node=target,
        file="state/snapshot.json",
        offset=position,
        kind="snapshot-document-byte",
    )
    cursor = len(h.nodes[target].history)
    raw_cursor = len(h.nodes[target].raw_lines)
    h.restart_node(target)
    deadline = time.monotonic() + 25.0
    outcome: str | None = None
    recovered: dict | None = None
    while time.monotonic() < deadline:
        h.sync()
        process = h.nodes[target].process
        if process is not None and process.poll() is not None:
            outcome = "refused"
            break
        view = h.applied_view_from(target, cursor)
        partition = view["partition"] if view else None
        if partition and partition.get("count") == 3:
            recovered = view
            same = partition == before[target]["partition"]
            outcome = "recovered" if same else "altered"
            break
        time.sleep(T_POLL)
    h.check(
        outcome is not None,
        "the node with a corrupted snapshot neither refused nor recovered within 25s",
    )
    h.check(
        outcome != "altered",
        f"the node recovered a different committed state from a corrupted snapshot: "
        f"{recovered and recovered['partition']}",
        recovered=recovered,
    )
    errors: list[str] = []
    if outcome == "refused":
        code = h.nodes[target].process.returncode
        poll_deadline = time.monotonic() + 2.0
        while time.monotonic() < poll_deadline:
            errors = h.nodes[target].error_lines(raw_cursor)
            if errors:
                break
            time.sleep(T_POLL)
        h.check(
            code not in (None, 0),
            f"a corrupted snapshot must make the node exit non-zero, observed {code}",
            exit_code=code,
            errors=errors,
        )
        h.check(
            bool(errors) and any("snapshot" in line or "shard" in line for line in errors),
            f"the refusal must carry an explicit snapshot error, observed {errors}",
            errors=errors,
        )
        classification = "snapshot_corruption_refused"
    else:
        h.check(
            recovered["partition"] == before[target]["partition"]
            and recovered["applied_index"] == before[target]["applied_index"],
            "recovery from the log must not change committed history",
            before=before[target],
            recovered=recovered,
        )
        classification = "snapshot_corruption_recovered_from_log"
    h.journal.append(
        "outcome_classified", scenario=17, classification=classification
    )

    # A foreign snapshot (another shard id) must be refused by identity.
    second = Harness(h.journal, 17, "foreign-snapshot", "foreign_identity")
    try:
        second.cluster_id = h.cluster_id
        for node in second.nodes.values():
            node.shard_id = 8
        second.start_cluster()
        second.wait_leader()
        foreign = second.nodes[1]
        second.clean_stop(1)
        destination = foreign.directory / "state"
        destination.mkdir(parents=True, exist_ok=True)
        (destination / "snapshot.json").write_bytes(snapshot_bytes)
        (destination / "snapshot-meta.json").write_bytes(meta_bytes)
        h.journal.append(
            "corruption_injected",
            node=1,
            file="state/snapshot.json",
            offset=0,
            kind="foreign-snapshot-copy",
            shard_id=8,
        )
        second_raw = len(foreign.raw_lines)
        foreign.start(bootstrap=False)
        h.check(
            foreign.wait_exit(25.0),
            "the node must refuse a snapshot that belongs to another shard id",
            alive=second.alive_nodes(),
        )
        second_errors = foreign.error_lines(second_raw)
        poll_deadline = time.monotonic() + 2.0
        while time.monotonic() < poll_deadline and not second_errors:
            second_errors = foreign.error_lines(second_raw)
            time.sleep(T_POLL)
        h.check(
            foreign.process.returncode not in (None, 0),
            f"the foreign-snapshot node must exit non-zero, observed "
            f"{foreign.process.returncode}",
            exit_code=foreign.process.returncode,
            errors=second_errors,
        )
        h.check(
            any("identity mismatch" in line for line in second_errors),
            f"the refusal must name the identity mismatch, observed {second_errors}",
            errors=second_errors,
        )
        h.journal.append(
            "outcome_classified",
            scenario=17,
            classification="foreign_snapshot_refused",
        )
        h.check(
            True,
            "a corrupted snapshot was handled without changing committed history and "
            "a snapshot copied from another shard was refused by identity",
            corruption_classification=classification,
            corruption_errors=errors[:4],
            foreign_errors=second_errors[:4],
            copied_shard_id=8,
            source_shard_id=h.shard_id,
            committed=before[target],
        )
    finally:
        second.close()


def scenario_18(h: Harness) -> None:
    """The Q2 Raft log, the Q1 WAL and the metadata Raft log stay separate domains."""
    h.check(
        Q1_BROKER.exists() and Q1_CLIENT.exists(),
        f"the Q1 broker or its SDK client is missing: {Q1_BROKER}, {Q1_CLIENT}",
        broker=str(Q1_BROKER),
        client=str(Q1_CLIENT),
    )
    leader = h.wait_leader()
    leader_id = leader["node"]
    h.install_authority(leader_id)
    result = h.propose(leader_id, 1, {"Inline": [1, 2, 3]})
    h.check_applied(result, 1, count=1, context="the separation probe")
    h.wait_applied_by(1, 1, VOTERS, T_APPLIED, context="before the Q1 partition")
    before = {node_id: h.applied_view(node_id) for node_id in VOTERS}

    data_dir = h.nodes[leader_id].directory
    log_path = data_dir / "raft" / "raft-log.bin"
    state_dir = data_dir / "state"
    h.check(
        file_magic(log_path) == Q2_LOG_MAGIC,
        f"the Q2 Raft log must start with QRL1, observed {file_magic(log_path)!r}",
        path=str(log_path),
        magic=file_magic(log_path).decode("latin-1"),
    )
    h.check(
        log_path == data_dir / "raft" / "raft-log.bin",
        "the Q2 Raft log must live under <data>/raft/raft-log.bin",
        path=str(log_path),
    )
    h.check(
        state_dir.is_dir() and (state_dir / "state.json").exists(),
        f"the Q2 applied state must live under {state_dir}",
        state_files=sorted(path.name for path in state_dir.iterdir()),
    )
    q2_files = sorted(
        str(path.relative_to(data_dir)) for path in data_dir.rglob("*") if path.is_file()
    )
    q1_named = [
        name
        for name in q2_files
        if Path(name).name == "MANIFEST" or Path(name).name.startswith("segment-")
    ]
    h.check(
        not q1_named,
        f"the Q2 data directory contains Q1 WAL files: {q1_named}",
        files=q2_files,
    )
    metadata_logs = [
        name for name in q2_files
        if file_magic(data_dir / name) == METADATA_LOG_MAGIC
    ]
    h.check(
        not metadata_logs,
        f"the Q2 data directory contains a metadata-raft MRL1 log: {metadata_logs}",
        files=q2_files,
    )

    q1_dir = h.root / "q1-partition"
    q1_dir.mkdir()
    broker = subprocess.Popen(
        [
            str(Q1_BROKER),
            "--listen", "127.0.0.1:0",
            "--q1-data-dir", str(q1_dir),
            "--q1-topic", "q2-separation",
            "--q1-tenant-id", "1",
            "--q1-topic-id", "9",
            "--q1-partition-id", "0",
            "--q1-incarnation", "42",
            "--q1-owner-epoch", "7",
            "--q1-initialize-new-log", "true",
        ],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
        text=True,
    )
    lines: list[str] = []
    holder: dict[str, str | None] = {"endpoint": None}

    def read_broker() -> None:
        assert broker.stderr is not None
        for line in broker.stderr:
            text = line.rstrip()
            lines.append(text)
            if "listening on 127.0.0.1:" in text:
                holder["endpoint"] = text.rsplit(" ", 1)[-1]

    threading.Thread(target=read_broker, daemon=True).start()
    try:
        deadline = time.monotonic() + 20.0
        while (
            holder["endpoint"] is None
            and time.monotonic() < deadline
            and broker.poll() is None
        ):
            time.sleep(T_POLL)
        h.check(
            holder["endpoint"] is not None,
            f"the Q1 broker did not report readiness: {lines}",
            lines=lines[:10],
        )
        client = subprocess.run(
            [
                str(Q1_CLIENT), "q2-separation", "11", "0", "0", "101", "0",
                "42", "7", "event-0", "30000",
            ],
            env=dict(os.environ, ROVQET_ADDR=holder["endpoint"]),
            capture_output=True,
            text=True,
            timeout=30,
        )
        h.check(
            client.returncode == 0,
            f"the Q1 publisher failed with exit {client.returncode}: "
            f"{client.stdout} {client.stderr}",
            stdout=client.stdout,
            stderr=client.stderr,
        )
        h.check(
            bool((client.stdout or "").strip()),
            "the Q1 publisher produced no acknowledged record",
            stdout=client.stdout,
        )

        after = {node_id: h.applied_view(node_id) for node_id in VOTERS}
        for node_id in VOTERS:
            h.check(
                after[node_id]["partition"] == before[node_id]["partition"]
                and after[node_id]["applied_index"] == before[node_id]["applied_index"],
                f"the Q1 partition publish changed node {node_id}'s Q2 committed state: "
                f"before={before[node_id]} after={after[node_id]}",
                before=before[node_id],
                after=after[node_id],
            )
        for node_id in VOTERS:
            h.record_applied(node_id)
        q1_files = sorted(path.name for path in q1_dir.iterdir() if path.is_file())
        q1_qrl = [
            name for name in q1_files if file_magic(q1_dir / name) == Q2_LOG_MAGIC
        ]
        h.check(
            not q1_qrl,
            f"the Q1 partition directory contains a QRL1 Q2 Raft log: {q1_qrl}",
            files=q1_files,
        )
        h.check(
            "MANIFEST" in q1_files,
            f"the Q1 partition directory must hold its own WAL MANIFEST: {q1_files}",
            files=q1_files,
        )
        h.check(
            True,
            "the Q2 log (QRL1), the Q2 state directory and a live Q1 WAL occupy "
            "separate directories and a Q1 publish left the Q2 committed state alone",
            q2_files=q2_files,
            q2_log_magic=file_magic(log_path).decode("latin-1"),
            q1_files=q1_files,
            q1_has_qrl1=bool(q1_qrl),
            q2_has_q1_wal=bool(q1_named),
            q2_has_mrl1=bool(metadata_logs),
            committed={str(k): v for k, v in after.items()},
            q1_ack=client.stdout.strip(),
            q1_endpoint=holder["endpoint"],
        )
    finally:
        if broker.poll() is None:
            broker.send_signal(signal.SIGINT)
            try:
                broker.wait(timeout=15)
            except subprocess.TimeoutExpired:
                broker.kill()
                broker.wait(timeout=10)
        h.journal.append("q1_broker_exit", exit_code=broker.returncode)


SCENARIOS = [
    (1, "fresh-bootstrap-leader-election", "leader_elected", scenario_01),
    (2, "three-voter-replication", "committed_replication", scenario_02),
    (3, "majority-committed-entry", "majority_commit", scenario_03),
    (4, "leader-sigkill-after-commit", "committed_prefix_survives_kill", scenario_04),
    (5, "replacement-leader-prefix", "replacement_keeps_prefix", scenario_05),
    (6, "one-follower-unavailable", "majority_with_one_down", scenario_06),
    (7, "two-voters-unavailable", "no_quorum_no_commit", scenario_07),
    (8, "old-leader-divergent-tail", "divergent_tail_rejoin", scenario_08),
    (9, "divergent-tail-never-committed", "divergent_tail_excluded", scenario_09),
    (10, "follower-restart-catch-up", "follower_catch_up", scenario_10),
    (11, "snapshot-compaction", "snapshot_compaction", scenario_11),
    (12, "lagging-follower-snapshot-install", "snapshot_install", scenario_12),
    (13, "all-nodes-restart", "restart_durability", scenario_13),
    (14, "torn-uncommitted-tail", "uncommitted_corruption", scenario_14),
    (15, "committed-corruption-fails-closed", "committed_corruption", scenario_15),
    (16, "killed-leader-uncertain-commit", "interrupted_append", scenario_16),
    (17, "corrupt-and-foreign-snapshot", "snapshot_rejection", scenario_17),
    (18, "durability-domain-separation", "domain_separation", scenario_18),
]


def run_scenario(number: int, name: str, barrier: str, function, build: str):
    path = EVIDENCE_DIR / f"scenario-{number:02d}.jsonl"
    try:
        journal = Journal(path, f"c03-q2-raft-{number:02d}")
    except OSError as error:
        return BLOCKED, f"cannot open the journal: {error}", path, None

    harness: Harness | None = None
    verdict = FAIL
    reason: str | None = "the scenario did not conclude"
    oracle: dict | None = None
    try:
        journal.append(
            "scenario_started",
            scenario=number,
            barrier=barrier,
            build=build,
        )
        harness = Harness(journal, number, name, barrier)
        harness.start_cluster()
        function(harness)
        harness.sync()

        # The harness' own checks and the independent oracle must both accept the
        # journal. The oracle runs on the records written so far (no conclusion yet),
        # then its verdict is journaled and the single final conclusion reflects the
        # combined result.
        try:
            records = load_journal(path, require_conclusion=False)
            oracle = reconcile_q2_raft(records, require_conclusion=False)
        except Exception as error:  # noqa: BLE001 - an unreadable journal is not a pass
            oracle = {
                "verdict": JOURNAL_INVALID,
                "reasons": [f"the oracle could not read the journal: {error}"],
                "counts": {},
            }
        journal.append(
            "oracle_verdict",
            verdict=oracle["verdict"],
            reasons=oracle["reasons"],
            counts=oracle["counts"],
        )
        if oracle["verdict"] == JOURNAL_PASS:
            journal.conclusion(JOURNAL_PASS)
            verdict, reason = PASS, None
        else:
            reason = "the independent oracle rejected the journal: " + "; ".join(
                oracle["reasons"] or ["no reason given"]
            )
            verdict = FAIL
            journal.conclusion(JOURNAL_FAIL, reason)
    except ScenarioFailure as error:
        verdict, reason = FAIL, str(error)
        if harness is not None:
            try:
                harness.sync()
            except Exception:  # noqa: BLE001 - best effort before the conclusion
                pass
        try:
            journal.conclusion(JOURNAL_FAIL, reason)
        except Exception as error:  # noqa: BLE001
            print(f"journal conclusion failed: {error}", file=sys.stderr)
    except ScenarioBlocked as error:
        verdict, reason = BLOCKED, str(error)
        try:
            journal.conclusion(JOURNAL_INVALID, reason)
        except Exception:  # noqa: BLE001
            pass
    except Exception as error:  # noqa: BLE001 - any unexpected defect is a failure
        verdict = FAIL
        reason = f"unexpected {type(error).__name__}: {error}"
        traceback.print_exc()
        if harness is not None:
            try:
                harness.sync()
            except Exception:  # noqa: BLE001
                pass
        try:
            journal.conclusion(JOURNAL_FAIL, reason)
        except Exception:  # noqa: BLE001
            pass
    finally:
        if harness is not None:
            try:
                harness.close()
            except Exception as error:  # noqa: BLE001 - never mask the verdict
                print(f"cleanup failed: {error}", file=sys.stderr)
        journal.close()
    return verdict, reason, path, oracle


def main() -> int:
    if not NODE.exists():
        print(
            f"BLOCKED  harness: the node binary is missing: {NODE} "
            "(build target/debug/q2-raft-node first)",
            file=sys.stderr,
        )
        return 2
    version = subprocess.run(
        [str(NODE), "--version"], capture_output=True, text=True, timeout=30
    )
    build = (version.stdout or "").strip() or f"exit {version.returncode}"
    print(f"build: {build}", flush=True)

    EVIDENCE_DIR.mkdir(parents=True, exist_ok=True)
    results: list[str] = []
    for number, name, barrier, function in SCENARIOS:
        verdict, reason, path, oracle = run_scenario(number, name, barrier, function, build)
        results.append(verdict)
        detail = "ok" if reason is None else reason
        print(f"{verdict:>7}  scenario {number:02d} {name}: {detail}", flush=True)
        print(f"        journal: {path}", flush=True)
        if oracle is None:
            print("        oracle: not run (the harness checks failed first)", flush=True)
        else:
            counts = oracle.get("counts") or {}
            print(
                f"        oracle: {oracle.get('verdict')} "
                f"majority_proofs={counts.get('majority_proofs')} "
                f"observations={counts.get('observations')} "
                f"counts={counts}",
                flush=True,
            )
            for item in oracle.get("reasons") or []:
                print(f"        oracle reason: {item}", flush=True)

    counts = {value: results.count(value) for value in (PASS, FAIL, BLOCKED)}
    print(
        f"summary: {counts[PASS]} PASS, {counts[FAIL]} FAIL, "
        f"{counts[BLOCKED]} BLOCKED of {len(results)} scenarios",
        flush=True,
    )
    print(f"evidence: {EVIDENCE_DIR}", flush=True)
    if counts[BLOCKED]:
        return 2
    return 0 if counts[FAIL] == 0 and counts[PASS] == len(SCENARIOS) else 1


if __name__ == "__main__":
    sys.exit(main())
