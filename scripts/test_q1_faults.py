#!/usr/bin/env python3
"""Deterministic Q1 TCP/process fault tests; build server with fault-injection.

These exercise process crashes on intact storage. They are not power-loss tests.
All broker data and the independent parent-owned received-ACK journal are temporary.
"""
from __future__ import annotations

from pathlib import Path
import signal
import socket
import subprocess
import tempfile
import time
import unittest

from q1_smoke import Broker, Q0, committed, journal_ack, manifest_record_count


def varint(number):
    encoded = bytearray()
    while number >= 128:
        encoded.append((number & 127) | 128)
        number >>= 7
    encoded.append(number)
    return bytes(encoded)


def frame(kind, request, fields, payload=b""):
    header = varint(kind) + request.to_bytes(8, "big")
    for field, value in fields.items():
        header += varint((field << 1) | 1) + varint(len(value)) + value
    return b"RQ\x01\0" + varint(len(header)) + varint(len(payload)) + header + payload


def exact(connection, count):
    data = bytearray()
    while len(data) < count:
        part = connection.recv(count - len(data))
        if not part:
            raise EOFError("broker closed before response")
        data.extend(part)
    return bytes(data)


def read_varint(connection):
    number = 0
    for position in range(10):
        value = exact(connection, 1)[0]
        number |= (value & 127) << (7 * position)
        if value < 128:
            return number
    raise AssertionError("overlong broker varint")


def read_frame(connection):
    assert exact(connection, 4) == b"RQ\x01\0"
    header_len, payload_len = read_varint(connection), read_varint(connection)
    assert header_len <= 4096 and payload_len <= 262144
    header = exact(connection, header_len)
    exact(connection, payload_len)
    position = 0

    def take_varint():
        nonlocal position
        number = 0
        for shift in range(0, 70, 7):
            value = header[position]
            position += 1
            number |= (value & 127) << shift
            if value < 128:
                return number
        raise AssertionError("invalid response header")

    kind = take_varint()
    request = int.from_bytes(header[position:position + 8], "big")
    position += 8
    fields = {}
    while position < len(header):
        tag, size = take_varint(), take_varint()
        fields[tag >> 1] = header[position:position + size]
        position += size
    return kind, request, fields


def connect(broker, authenticated=True):
    host, port = broker.endpoint.rsplit(":", 1)
    connection = socket.create_connection((host, int(port)), timeout=10)
    if authenticated:
        connection.sendall(frame(1, 1, {1: b"\0\0", 3: (1048576).to_bytes(4, "big")}))
        assert read_frame(connection)[:2] == (2, 1)
        connection.sendall(frame(3, 2, {6: b"\0", 7: b"dev"}))
        kind, request, fields = read_frame(connection)
        assert (kind, request) == (4, 2)
        assert fields[8] == (1).to_bytes(8, "big")
    return connection


def publish_fields(sequence=0, *, qos=1, topic="durable", partition=0,
                   incarnation=42, owner=7, ttl_ms=30000):
    return {
        9: topic.encode(), 11: b"", 12: bytes([qos]),
        14: ttl_ms.to_bytes(4, "big"), 15: (100 + sequence).to_bytes(16, "big"),
        16: (11).to_bytes(16, "big"), 17: (0).to_bytes(8, "big"),
        18: sequence.to_bytes(8, "big"), 19: partition.to_bytes(4, "big"),
        20: incarnation.to_bytes(16, "big"), 27: owner.to_bytes(8, "big"),
    }


class Q1ProcessFaults(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix="rovqet-q1-fault-")
        self.directory = Path(self.temporary.name)
        self.processes = []
        self.brokers = []

    def tearDown(self):
        for process in self.processes:
            if process.poll() is None:
                process.kill()
            process.communicate(timeout=10)
        for broker in self.brokers:
            broker.close()
        self.temporary.cleanup()

    def broker(self, **options):
        broker = Broker(self.directory, **options)
        self.brokers.append(broker)
        return broker.start()

    def pending(self, broker, sequence=0, **options):
        process = broker.pending(sequence, **options)
        self.processes.append(process)
        return process

    def assert_unknown(self, process):
        output, error = process.communicate(timeout=15)
        self.assertNotEqual(process.returncode, 0, (output, error))
        self.assertNotIn("COMMITTED_LOCAL", output)
        self.assertIn("OutcomeUnknown", error)

    def assert_error(self, broker, fields, expected, *, authenticated=True):
        with connect(broker, authenticated) as connection:
            connection.sendall(frame(7, 123, fields, b"event-0"))
            kind, request, response = read_frame(connection)
            self.assertEqual((kind, request), (20, 123))
            code = int.from_bytes(response[28], "big")
            self.assertIn(code, expected)

    def test_wire_ack_contains_identity_and_request_correlation(self):
        broker = self.broker()
        with connect(broker) as connection:
            connection.sendall(frame(7, 987654, publish_fields(), b"event-0"))
            kind, request, response = read_frame(connection)
            self.assertEqual((kind, request), (8, 987654))
            self.assertEqual(response[12], b"\x01")
            self.assertEqual(response[22], b"\x02")
            self.assertEqual(response[19], (0).to_bytes(4, "big"))
            self.assertEqual(response[20], (42).to_bytes(16, "big"))
            self.assertEqual(response[21], (0).to_bytes(8, "big"))
        broker.stop()

    def crash_window(self, stage, proven_count):
        broker = self.broker(stage=stage)
        publisher = self.pending(broker)
        broker.barrier(stage)
        self.assertEqual(manifest_record_count(broker.wal), proven_count)
        broker.kill()
        self.assert_unknown(publisher)
        reopened = self.broker()
        committed(reopened.publish(), 0)
        self.assertEqual(manifest_record_count(reopened.wal), 2)
        committed(reopened.publish(1), 2)
        reopened.stop()

    def test_process_crash_before_append(self):
        self.crash_window("before-append", 0)

    def test_process_crash_after_append_before_sync(self):
        self.crash_window("after-append-before-sync", 0)

    def test_process_crash_after_sync_before_pub_ack(self):
        self.crash_window("after-sync-before-reply", 2)

    def test_every_externally_acknowledged_record_recovers_after_sigkill(self):
        broker = self.broker()
        oracle = self.directory / "received-acks.jsonl"
        for sequence in range(6):
            committed(broker.publish(sequence), sequence * 2)
            journal_ack(oracle, sequence, sequence * 2)
        broker.kill()
        reopened = self.broker()
        for sequence in range(6):
            committed(reopened.publish(sequence), sequence * 2)
        self.assertEqual(manifest_record_count(reopened.wal), 12)
        self.assertEqual(len(oracle.read_text().splitlines()), 6)
        reopened.stop()

    def test_disconnected_client_does_not_cancel_admitted_work(self):
        stage = "after-sync-before-reply"
        broker = self.broker(stage=stage)
        publisher = self.pending(broker)
        broker.barrier(stage)
        publisher.kill()  # Drop a real SDK receiver after the operation was admitted.
        publisher.communicate(timeout=10)
        broker.release(stage)
        committed(broker.publish(), 0)
        self.assertEqual(manifest_record_count(broker.wal), 2)
        broker.stop()

    def test_disconnect_while_accepted_write_is_still_pending(self):
        stage = "before-append"
        broker = self.broker(stage=stage)
        publisher = self.pending(broker)
        broker.barrier(stage)
        publisher.kill()
        publisher.communicate(timeout=10)
        broker.release(stage)
        committed(broker.publish(), 0)
        self.assertEqual(manifest_record_count(broker.wal), 2)
        broker.stop()

    def test_q0_publishes_while_q1_waits_for_storage_barrier(self):
        stage = "after-append-before-sync"
        broker = self.broker(stage=stage)
        publisher = self.pending(broker)
        broker.barrier(stage)
        q0 = subprocess.run([str(Q0), "pub", "demo", "independent-q0"],
                            env=broker.environment(), capture_output=True, text=True, timeout=5)
        self.assertEqual(q0.returncode, 0, q0.stderr)
        self.assertEqual(q0.stdout.strip(), "Admitted")
        self.assertIsNone(publisher.poll(), "Q1 must remain behind the unreleased barrier")
        broker.release(stage)
        output, error = publisher.communicate(timeout=15)
        self.assertEqual(publisher.returncode, 0, error)
        self.assertEqual(output.strip(), "COMMITTED_LOCAL 42 0")
        broker.stop()

    def test_standalone_replay_cannot_expose_an_unsynced_tcp_publication(self):
        stage = "after-append-before-sync"
        broker = self.broker(stage=stage)
        publisher = self.pending(broker)
        broker.barrier(stage)
        with connect(broker) as reader:
            request = {9: b"durable", 12: b"\x01", 19: bytes(4),
                       20: (42).to_bytes(16, "big"), 21: bytes(8),
                       31: (8).to_bytes(4, "big"), 32: (65536).to_bytes(4, "big")}
            reader.sendall(frame(23, 998, request))
            reader.settimeout(0.02)
            with self.assertRaises(socket.timeout):
                reader.recv(1)
            self.assertEqual(manifest_record_count(broker.wal), 0)
            broker.release(stage)
            output, error = publisher.communicate(timeout=15)
            self.assertEqual(publisher.returncode, 0, error)
            self.assertEqual(output.strip(), "COMMITTED_LOCAL 42 0")
            journal_ack(self.directory / "received-acks.jsonl", 0, 0)
            reader.settimeout(5)
            kind, correlation, record = read_frame(reader)
            self.assertEqual((kind, correlation), (24, 998))
            self.assertEqual(record[15], (100).to_bytes(16, "big"))
            self.assertEqual(record[21], bytes(8))
            kind, correlation, end = read_frame(reader)
            self.assertEqual((kind, correlation), (25, 998))
            self.assertEqual(end[21], (2).to_bytes(8, "big"))
            self.assertEqual(end[49], b"\x02")
        broker.stop()

    def test_admitted_queued_expiry_rejects_before_append_without_consuming_sequence(self):
        stage = "before-append"
        broker = self.broker(stage=stage)
        first = self.pending(broker)
        broker.barrier(stage)
        with connect(broker) as connection:
            fields = publish_fields(ttl_ms=100)
            fields[16] = (22).to_bytes(16, "big")
            fields[15] = (200).to_bytes(16, "big")
            connection.sendall(frame(7, 444, fields, b"second"))
            broker.admitted(22, 0)
            # Clock expiry after an explicit admission event, not a guessed crash window.
            time.sleep(0.15)
            broker.release(stage)
            kind, request, response = read_frame(connection)
            self.assertEqual((kind, request), (20, 444))
            self.assertEqual(int.from_bytes(response[28], "big"), 24)
        output, error = first.communicate(timeout=15)
        self.assertEqual(first.returncode, 0, error)
        self.assertEqual(output.strip(), "COMMITTED_LOCAL 42 0")
        self.assertEqual(manifest_record_count(broker.wal), 2)
        committed(broker.publish(producer=22, message=200, payload="second"), 2)
        self.assertEqual(manifest_record_count(broker.wal), 4)
        broker.stop()

    def test_clean_shutdown_drains_accepted_pending_write(self):
        stage = "after-append-before-sync"
        broker = self.broker(stage=stage)
        publisher = self.pending(broker)
        broker.barrier(stage)
        broker.process.send_signal(signal.SIGINT)
        broker.release(stage)
        broker.process.wait(timeout=15)
        self.assertEqual(broker.process.returncode, 0, broker.lines)
        output, error = publisher.communicate(timeout=15)
        self.assertTrue(output.strip() == "COMMITTED_LOCAL 42 0" or "OutcomeUnknown" in error,
                        (output, error))
        reopened = self.broker()
        committed(reopened.publish(), 0)
        self.assertEqual(manifest_record_count(reopened.wal), 2)
        reopened.stop()

    def test_invalid_class_partition_incarnation_owner_identity_and_ttl(self):
        broker = self.broker()
        for fields, errors in [
            (publish_fields(qos=0), {14}),
            (publish_fields(qos=2), {14}),
            (publish_fields(topic="demo"), {14}),
            (publish_fields(partition=1), {14}),
            (publish_fields(incarnation=43), {26}),
            (publish_fields(owner=8), {17}),
            (publish_fields(ttl_ms=0), {23}),
        ]:
            self.assert_error(broker, fields, errors)
        forged = publish_fields()
        forged[8] = (99).to_bytes(8, "big")
        self.assert_error(broker, forged, {6})
        forged_tenant = publish_fields()
        forged_tenant[43] = (99).to_bytes(8, "big")
        self.assert_error(broker, forged_tenant, {6})
        self.assert_error(broker, publish_fields(), {4}, authenticated=False)
        self.assertEqual(manifest_record_count(broker.wal), 0)
        broker.stop()

    def test_q1_disabled_is_explicitly_rejected(self):
        broker = self.broker(configure=False)
        self.assert_error(broker, publish_fields(), {14})
        broker.stop()

    def test_failed_or_uncertain_sync_never_sends_commit_and_storage_fails_closed(self):
        for fault in ["append", "partial-append", "segment-sync", "manifest-write",
                      "manifest-sync", "manifest-rename", "directory-sync"]:
            with self.subTest(fault=fault):
                # Each fault has its own pristine log and one-shot injection.
                directory = self.directory / fault
                directory.mkdir()
                broker = Broker(directory, fault=fault)
                self.brokers.append(broker)
                broker.start()
                result = broker.publish()
                self.assertNotEqual(result.returncode, 0, (result.stdout, result.stderr))
                self.assertNotIn("COMMITTED_LOCAL", result.stdout)
                self.assertTrue("OutcomeUnknown" in result.stderr or "StorageUnavailable" in result.stderr,
                                result.stderr)
                second = broker.publish()
                self.assertNotEqual(second.returncode, 0)
                self.assertIn("StorageUnavailable", second.stderr)
                broker.stop()

    def test_corruption_in_committed_boundary_fails_startup_without_reset(self):
        broker = self.broker()
        committed(broker.publish(), 0)
        broker.stop()
        manifest = (broker.wal / "MANIFEST").read_bytes()
        segment = next(broker.wal.glob("segment-*.wal"))
        data = bytearray(segment.read_bytes())
        data[52 + 84] ^= 0x80  # Message payload, covered by its committed CRC32C.
        segment.write_bytes(data)
        rejected = Broker(self.directory)
        self.brokers.append(rejected)
        diagnostics = rejected.reject_startup()
        self.assertTrue("checksum" in diagnostics.lower() or "corrupt" in diagnostics.lower(), diagnostics)
        self.assertEqual((broker.wal / "MANIFEST").read_bytes(), manifest)
        self.assertEqual(segment.read_bytes(), data)

    def test_second_server_cannot_mutate_an_active_wal_directory(self):
        broker = self.broker()
        before = (broker.wal / "MANIFEST").read_bytes()
        second = Broker(self.directory)
        self.brokers.append(second)
        diagnostics = second.reject_startup()
        self.assertTrue("lock" in diagnostics.lower() or "owned" in diagnostics.lower(), diagnostics)
        self.assertEqual((broker.wal / "MANIFEST").read_bytes(), before)
        committed(broker.publish(), 0)
        broker.stop()

    def test_timeout_is_unknown_and_same_token_retry_settles_commit(self):
        stage = "after-sync-before-reply"
        broker = self.broker(stage=stage)
        from q1_smoke import q1_command
        publisher = subprocess.Popen(q1_command(),
                                     env=broker.environment(ROVQET_REQUEST_TIMEOUT_MS="500"),
                                     stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        self.processes.append(publisher)
        broker.barrier(stage)
        self.assert_unknown(publisher)
        broker.release(stage)
        committed(broker.publish(), 0)
        self.assertEqual(manifest_record_count(broker.wal), 2)
        broker.stop()

    def test_lost_entire_history_is_not_silently_recreated(self):
        broker = self.broker()
        committed(broker.publish(), 0)
        journal_ack(self.directory / "received-acks.jsonl", 0, 0)
        broker.stop()
        # Isolated disposable loss fixture. The broker itself never renames/deletes storage.
        broker.wal.rename(self.directory / "unavailable-wal")
        broker.wal.mkdir()
        rejected = Broker(self.directory)
        self.brokers.append(rejected)
        diagnostics = rejected.reject_startup()
        self.assertIn("REPLAY_NOT_AVAILABLE", diagnostics)
        self.assertEqual(list(broker.wal.iterdir()), [])
        self.assertTrue((self.directory / "unavailable-wal" / "MANIFEST").exists())

    def test_group_fetch_and_ack_are_explicitly_unsupported_without_fake_leases(self):
        broker = self.broker()
        committed(broker.publish(), 0)
        with connect(broker) as connection:
            fetch = {9: b"durable", 19: (0).to_bytes(4, "big"),
                     24: (1).to_bytes(8, "big"), 20: (42).to_bytes(16, "big"),
                     21: (0).to_bytes(8, "big"), 31: (8).to_bytes(4, "big"),
                     32: (2048).to_bytes(4, "big")}
            ack = {9: b"durable", 19: (0).to_bytes(4, "big"),
                   24: (1).to_bytes(8, "big"), 25: (1).to_bytes(8, "big"),
                   26: (1).to_bytes(16, "big"), 27: (7).to_bytes(8, "big"),
                   20: (42).to_bytes(16, "big"), 21: (0).to_bytes(8, "big")}
            for request, (kind, fields) in enumerate([(17, fetch), (13, ack), (15, ack), (16, ack)], 10):
                connection.sendall(frame(kind, request, fields))
                response, correlation, fields = read_frame(connection)
                self.assertEqual((response, correlation), (20, request))
                self.assertEqual(int.from_bytes(fields[28], "big"), 3)
        self.assertEqual(manifest_record_count(broker.wal), 2)
        broker.stop()

    def test_missing_storage_and_wrong_incarnation_fail_startup(self):
        missing = Broker(self.directory, data_directory=self.directory / "missing")
        self.brokers.append(missing)
        missing.reject_startup()
        broker = self.broker()
        committed(broker.publish(), 0)
        broker.stop()
        manifest = (broker.wal / "MANIFEST").read_bytes()
        rejected = Broker(self.directory, incarnation=43)
        self.brokers.append(rejected)
        rejected.reject_startup()
        self.assertEqual((broker.wal / "MANIFEST").read_bytes(), manifest)


if __name__ == "__main__":
    unittest.main(verbosity=2)
