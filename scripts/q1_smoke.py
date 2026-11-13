#!/usr/bin/env python3
"""Independent Q1 SDK processes and received-ACK journal; no power-loss claim.

Build rovqet-server and the SDK q1 example first. This script uses only disposable
storage, writes its oracle outside the WAL directory, and never removes user data.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import queue
import signal
import subprocess
import tempfile
import threading
import time

ROOT = Path(__file__).resolve().parents[1]
SERVER = ROOT / "target/debug/rovqet"
Q1 = ROOT / "target/debug/examples/q1"
READ = ROOT / "target/debug/examples/read_q1"
Q0 = ROOT / "target/debug/examples/q0"


def q1_command(sequence=0, *, producer=11, epoch=0, message=None,
               partition=0, incarnation=42, owner=7, topic="durable",
               payload=None, ttl_ms=30000):
    message = 100 + sequence if message is None else message
    payload = f"event-{sequence}" if payload is None else payload
    return [str(Q1), topic, str(producer), str(epoch), str(sequence), str(message),
            str(partition), str(incarnation), str(owner), payload, str(ttl_ms)]


class Broker:
    """A broker subprocess with bounded readiness/exit waits and complete cleanup."""
    def __init__(self, directory: Path, *, stage=None, fault=None, incarnation=42,
                 configure=True, data_directory=None, checkpoint_after=None,
                 segment_bytes=None, record_bytes=None, sequence=0, extra=()):
        self.directory = directory
        self.wal = Path(data_directory) if data_directory else directory / "wal"
        self.controls = directory / "controls"
        self.controls.mkdir(exist_ok=True)
        # Explicit test authorization only for a newly allocated disposable directory.
        initialize_new_log = data_directory is None and not self.wal.exists()
        if data_directory is None:
            self.wal.mkdir(exist_ok=True)
        self.lines = []
        self.condition = threading.Condition()
        self.ready = queue.Queue()
        env = {k: v for k, v in os.environ.items()
               if not k.startswith("ROVQET_Q1_TEST_")}
        env["TOKIO_WORKER_THREADS"] = "1"
        if stage or fault:
            env["ROVQET_Q1_TEST_DIR"] = str(self.controls)
            env["ROVQET_Q1_TEST_SEQUENCE"] = str(sequence)
        if stage:
            env["ROVQET_Q1_TEST_STAGE"] = stage
        if fault:
            env["ROVQET_Q1_TEST_WAL_FAULT"] = fault
        if checkpoint_after is not None:
            env["ROVQET_Q1_TEST_CHECKPOINT_AFTER"] = str(checkpoint_after)
        if segment_bytes is not None:
            env["ROVQET_Q1_TEST_SEGMENT_BYTES"] = str(segment_bytes)
        if record_bytes is not None:
            env["ROVQET_Q1_TEST_RECORD_BYTES"] = str(record_bytes)
        command = [str(SERVER), "--listen", "127.0.0.1:0"]
        if configure:
            command += ["--q1-data-dir", str(self.wal), "--q1-topic", "durable",
                        "--q1-tenant-id", "1", "--q1-topic-id", "3",
                        "--q1-partition-id", "0", "--q1-incarnation", str(incarnation),
                        "--q1-owner-epoch", "7"]
            if initialize_new_log:
                command += ["--q1-initialize-new-log", "true"]
            command += [str(value) for value in extra]
        self.process = subprocess.Popen(command, env=env, stdout=subprocess.DEVNULL,
                                        stderr=subprocess.PIPE, text=True)
        self.reader = threading.Thread(target=self._read, daemon=True)
        self.reader.start()
        self.endpoint = None

    def _read(self):
        assert self.process.stderr is not None
        for line in self.process.stderr:
            with self.condition:
                self.lines.append(line.rstrip())
                self.condition.notify_all()
            if "listening on 127.0.0.1:" in line:
                self.ready.put(line.rstrip().rsplit(" ", 1)[1])
        self.ready.put(None)
        self.process.stderr.close()

    def start(self):
        try:
            self.endpoint = self.ready.get(timeout=15)
            if self.endpoint is None:
                self.process.wait(timeout=5)
                raise AssertionError("broker failed before readiness: " + "\n".join(self.lines))
            return self
        except BaseException:
            self.close()
            raise

    def reject_startup(self):
        try:
            code = self.process.wait(timeout=15)
            self.reader.join(timeout=5)
            assert code != 0, self.lines
            assert self.endpoint is None
            assert not any("listening on" in line for line in self.lines), self.lines
            return "\n".join(self.lines)
        finally:
            self.close()

    def environment(self, **extra):
        assert self.endpoint
        return dict(os.environ, ROVQET_ADDR=self.endpoint, **extra)

    def publish(self, sequence=0, *, timeout=15, **options):
        return subprocess.run(q1_command(sequence, **options), env=self.environment(),
                              capture_output=True, text=True, timeout=timeout)

    def pending(self, sequence=0, **options):
        return subprocess.Popen(q1_command(sequence, **options), env=self.environment(),
                                stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

    def barrier(self, stage, seconds=15):
        marker = self.controls / f"reached-{stage}"
        deadline = time.monotonic() + seconds
        while not marker.exists():
            assert self.process.poll() is None, self.lines
            if time.monotonic() >= deadline:
                raise AssertionError(f"barrier {stage} not reached: {self.lines}")
            time.sleep(0.01)  # Poll an explicit event, not an assumed crash window.

    def admitted(self, producer, sequence, seconds=15):
        expected = f"Q1_TEST_ADMITTED {producer} {sequence}"
        with self.condition:
            self.condition.wait_for(lambda: expected in self.lines or self.process.poll() is not None,
                                    timeout=seconds)
            assert expected in self.lines, (expected, self.lines)

    def release(self, stage):
        (self.controls / f"release-{stage}").write_text("release\n")

    def stop(self):
        if self.process.poll() is None:
            self.process.send_signal(signal.SIGINT)
            self.process.wait(timeout=15)
        self.reader.join(timeout=5)
        assert self.process.returncode == 0, self.lines

    def kill(self):
        self.process.kill()
        self.process.wait(timeout=10)
        self.reader.join(timeout=5)
        assert self.process.returncode == -signal.SIGKILL

    def close(self):
        if self.process.poll() is None:
            self.process.kill()
        self.process.wait(timeout=10)
        self.reader.join(timeout=5)

    def __enter__(self):
        return self.start()

    def __exit__(self, *_):
        self.close()


def committed(result, expected):
    assert result.returncode == 0, (result.stdout, result.stderr)
    assert result.stdout.strip() == f"COMMITTED_LOCAL 42 {expected}", result.stdout


def journal_ack(path: Path, sequence: int, offset: int):
    """The oracle is controlled by the parent, outside all broker-owned files."""
    payload = f"event-{sequence}".encode()
    entry = dict(producer=11, epoch=0, sequence=sequence, message_id=100 + sequence,
                 payload_sha256=hashlib.sha256(payload).hexdigest(),
                 incarnation=42, offset=offset, outcome="COMMITTED_LOCAL")
    with path.open("a", encoding="utf-8") as journal:
        journal.write(json.dumps(entry, sort_keys=True) + "\n")
        journal.flush()
        os.fsync(journal.fileno())


def replay_journal(broker, journal: Path):
    """External SDK client verifies payloads/identities against the independent ACK journal."""
    result = subprocess.run([str(READ), "durable", "1", "3", "0", "42", "0", "64", "65536", "0"],
                            env=broker.environment(), capture_output=True, text=True, timeout=15)
    assert result.returncode == 0, (result.stdout, result.stderr)
    records = {}
    end = None
    for line in result.stdout.splitlines():
        parts = line.split()
        if parts[0] == "READ_RECORD":
            _, incarnation, offset, message, payload = parts
            records[int(offset)] = (int(incarnation), int(message), hashlib.sha256(bytes.fromhex(payload)).hexdigest())
        elif parts[0] == "READ_END":
            end = parts
    entries = list(map(json.loads, journal.read_text().splitlines()))
    assert len(records) == len(entries), (records, entries)
    for entry in entries:
        assert records[entry["offset"]] == (entry["incarnation"], entry["message_id"], entry["payload_sha256"])
    assert end and end[1:5] == ["42", str(2 * len(entries)), "0", "2"], end


def manifest_record_count(wal: Path):
    # Frozen v1 encoding: count is u64 BE at bytes69..77, not inferred from tails.
    manifest = (wal / "MANIFEST").read_bytes()
    assert manifest[:8] == b"RQWM\x00\x01\x00\x00"
    return int.from_bytes(manifest[69:77], "big")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check-hooks-disabled", action="store_true",
                        help="also prove the normal server build rejects test-control environment")
    arguments = parser.parse_args()
    if arguments.check_hooks_disabled:
        with tempfile.TemporaryDirectory(prefix="rovqet-q1-no-hooks-") as temporary:
            rejected = Broker(Path(temporary), stage="before-append")
            diagnostics = rejected.reject_startup()
            assert "fault" in diagnostics.lower() or "test" in diagnostics.lower(), diagnostics
            assert not (rejected.wal / "MANIFEST").exists()
    with tempfile.TemporaryDirectory(prefix="rovqet-q1-smoke-") as temporary:
        directory = Path(temporary)
        journal = directory / "received-acks.jsonl"
        with Broker(directory) as broker:
            for sequence in range(3):
                committed(broker.publish(sequence), sequence * 2)
                journal_ack(journal, sequence, sequence * 2)
            replay_journal(broker, journal)
            broker.stop()
        with Broker(directory) as broker:
            replay_journal(broker, journal)
            for entry in map(json.loads, journal.read_text().splitlines()):
                committed(broker.publish(entry["sequence"]), entry["offset"])
            assert manifest_record_count(broker.wal) == 6, "retry inserted duplicates"
            committed(broker.publish(3), 6)
            journal_ack(journal, 3, 6)
            broker.kill()  # Actual process crash, after independently journaled PUB_ACK.
        with Broker(directory) as broker:
            replay_journal(broker, journal)
            for entry in map(json.loads, journal.read_text().splitlines()):
                committed(broker.publish(entry["sequence"]), entry["offset"])
            assert manifest_record_count(broker.wal) == 8
            broker.stop()
        print("PASS: Q1 SDK/TCP COMMITTED_LOCAL; clean restart and SIGKILL recovered all4 independently journaled ACKs with stable retry offsets, SDK/TCP payload replay and no duplicate insertion")


if __name__ == "__main__":
    main()
