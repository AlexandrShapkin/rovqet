#!/usr/bin/env python3
"""Standalone replay over real TCP and external Rust SDK processes. Disposable storage only."""
import subprocess
import tempfile
import unittest
from pathlib import Path

from q1_smoke import Broker, READ, committed, manifest_record_count
from test_q1_faults import connect, exact, frame, read_frame, varint


def fields(offset=0, count=8, size=65536, wait=0, **claims):
    result = {9: b"durable", 12: b"\x01", 19: bytes(4), 20: (42).to_bytes(16, "big"),
              21: offset.to_bytes(8, "big"), 31: count.to_bytes(4, "big"),
              32: size.to_bytes(4, "big"), 33: wait.to_bytes(4, "big")}
    for field, value in claims.items():
        result[int(field)] = value
    return result


def sdk(broker, offset=0, count=8, size=65536, wait=0):
    return subprocess.run([str(READ), "durable", "1", "3", "0", "42", str(offset), str(count), str(size), str(wait)],
                          env=broker.environment(), capture_output=True, text=True, timeout=15)


def completed(result, *, offsets, next_offset, more, status, byte_count=None):
    assert result.returncode == 0, (result.stdout, result.stderr)
    lines = [line.split() for line in result.stdout.splitlines()]
    assert len(lines) == len(offsets) + 1, lines
    assert [int(line[2]) for line in lines[:-1]] == offsets, lines
    for line in lines[:-1]:
        offset = int(line[2])
        assert line[0] == "READ_RECORD" and line[1] == "42"
        assert int(line[3]) == 100 + offset // 2
        assert bytes.fromhex(line[4]) == f"event-{offset // 2}".encode()
    assert lines[-1][:5] == ["READ_END", "42", str(next_offset), str(more), str(status)], lines
    if byte_count is not None:
        assert int(lines[-1][5]) == byte_count, lines


class StandaloneReplay(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix="rovqet-read-tcp-")
        self.directory = Path(self.temporary.name)
        self.broker = Broker(self.directory).start()

    def tearDown(self):
        self.broker.close()
        self.temporary.cleanup()

    def test_external_sdk_sparse_continuations_and_exact_encoded_limits(self):
        for sequence in range(5):
            committed(self.broker.publish(sequence), sequence * 2)
        manifest = (self.broker.wal / "MANIFEST").read_bytes()
        completed(sdk(self.broker, count=2), offsets=[0, 2], next_offset=4, more=1, status=3)
        completed(sdk(self.broker, offset=4, size=175), offsets=[4], next_offset=6, more=1, status=4, byte_count=175)
        failed = sdk(self.broker, size=174)
        self.assertNotEqual(failed.returncode, 0)
        self.assertIn("ResourceExhausted", failed.stderr)
        self.assertEqual(failed.stdout, "")
        completed(sdk(self.broker, offset=6), offsets=[6, 8], next_offset=10, more=0, status=2)
        completed(sdk(self.broker, offset=10, size=78), offsets=[], next_offset=10, more=0, status=2, byte_count=78)
        self.assertEqual((self.broker.wal / "MANIFEST").read_bytes(), manifest)
        committed(self.broker.publish(0), 0)
        self.assertEqual(manifest_record_count(self.broker.wal), 10)
        committed(self.broker.publish(5), 10)

    def test_empty_and_bounded_wait_expiration(self):
        completed(sdk(self.broker, size=78), offsets=[], next_offset=0, more=0, status=1, byte_count=78)
        completed(sdk(self.broker, wait=10), offsets=[], next_offset=0, more=0, status=5, byte_count=78)

    def test_concurrent_publish_wakes_long_poll_and_disconnected_client_does_not_block(self):
        with connect(self.broker) as waiting:
            waiting.sendall(frame(23, 100, fields(wait=30000)))
            committed(self.broker.publish(), 0)
            self.assertEqual(read_frame(waiting)[:2], (24, 100))
            kind, request, terminal = read_frame(waiting)
            self.assertEqual((kind, request), (25, 100))
            self.assertEqual(terminal[49], b"\x02")
        with connect(self.broker) as disconnected:
            disconnected.sendall(frame(23, 101, fields(offset=2, wait=30000)))
        # Independent SDK session remains functional while cancellation is collected.
        committed(self.broker.publish(1), 2)
        completed(sdk(self.broker), offsets=[0, 2], next_offset=4, more=0, status=2)

    def test_disconnect_mid_batch_never_completes_and_history_stays_replayable(self):
        payload = "x" * 16384
        for sequence in range(6):
            committed(self.broker.publish(sequence, payload=payload), sequence * 2)
        manifest = (self.broker.wal / "MANIFEST").read_bytes()
        with connect(self.broker) as connection:
            connection.sendall(frame(23, 400, fields(count=6, size=262144)))
            kind, request, record = read_frame(connection)
            self.assertEqual((kind, request), (24, 400))
            self.assertEqual(int.from_bytes(record[21], "big"), 0)
        # Closing before READ_END abandons the batch; it never becomes a success result.
        result = sdk(self.broker, count=6, size=262144)
        self.assertEqual(result.returncode, 0, result.stderr)
        lines = [line.split() for line in result.stdout.splitlines()]
        self.assertEqual([int(line[2]) for line in lines[:-1]], [0, 2, 4, 6, 8, 10])
        for line in lines[:-1]:
            self.assertEqual(line[0], "READ_RECORD")
            self.assertEqual(bytes.fromhex(line[4]), payload.encode())
        self.assertEqual(lines[-1][:5], ["READ_END", "42", "12", "0", "2"])
        self.assertEqual((self.broker.wal / "MANIFEST").read_bytes(), manifest)

    def test_large_record_uses_lossless_bounded_reply_path_not_control_queue(self):
        payload = "x" * 8192
        committed(self.broker.publish(payload=payload), 0)
        result = sdk(self.broker)
        self.assertEqual(result.returncode, 0, result.stderr)
        lines = result.stdout.splitlines()
        self.assertEqual(bytes.fromhex(lines[0].split()[4]), payload.encode())
        self.assertEqual(lines[-1].split()[:5], ["READ_END", "42", "2", "0", "2"])

    def test_authorization_incarnation_partition_and_unavailable_offsets(self):
        for replacement, code in [({43: (2).to_bytes(8, "big")}, 6),
                                  ({8: (2).to_bytes(8, "big")}, 6),
                                  ({10: (99).to_bytes(8, "big")}, 6),
                                  ({19: (1).to_bytes(4, "big")}, 26),
                                  ({20: (41).to_bytes(16, "big")}, 26),
                                  ({21: (1).to_bytes(8, "big")}, 26),
                                  ({21: (2).to_bytes(8, "big")}, 26)]:
            with connect(self.broker) as connection:
                request = fields(); request.update(replacement)
                connection.sendall(frame(23, 200, request))
                kind, correlation, result = read_frame(connection)
                self.assertEqual((kind, correlation), (20, 200))
                self.assertEqual(int.from_bytes(result[28], "big"), code)
        with connect(self.broker, authenticated=False) as connection:
            connection.sendall(frame(23, 201, fields()))
            self.assertEqual(int.from_bytes(read_frame(connection)[2][28], "big"), 4)

    def test_duplicate_wrong_width_unknown_frame_and_group_fields_are_rejected(self):
        valid = fields()
        for bad_kind, extra, code in [(23, [(19, bytes(4))], 1), (23, [(24, bytes(8))], 1),
                                      (126, [], 3), (23, [(127, b"\x01")], 3)]:
            header = varint(bad_kind) + (300).to_bytes(8, "big")
            for field, value in list(valid.items()) + extra:
                header += varint((field << 1) | 1) + varint(len(value)) + value
            encoded = b"RQ\x01\0" + varint(len(header)) + b"\0" + header
            with connect(self.broker) as connection:
                connection.sendall(encoded)
                response = read_frame(connection)
                self.assertEqual(response[0], 20)
                self.assertEqual(int.from_bytes(response[2][28], "big"), code)
        with connect(self.broker) as connection:
            wrong = fields(); wrong[19] = bytes(8)
            connection.sendall(frame(23, 301, wrong))
            self.assertEqual(int.from_bytes(read_frame(connection)[2][28], "big"), 1)
        self.assertEqual(manifest_record_count(self.broker.wal), 0)

    def test_negotiated_frame_limit_rejects_oversized_first_record(self):
        import socket
        committed(self.broker.publish(payload="x" * 1024), 0)
        host, port = self.broker.endpoint.rsplit(":", 1)
        with socket.create_connection((host, int(port)), timeout=5) as connection:
            connection.sendall(frame(1, 1, {1: bytes(2), 3: (256).to_bytes(4, "big")}))
            self.assertEqual(read_frame(connection)[0], 2)
            connection.sendall(frame(3, 2, {6: b"\0", 7: b"dev"}))
            self.assertEqual(read_frame(connection)[0], 4)
            connection.sendall(frame(23, 302, fields()))
            response = read_frame(connection)
            self.assertEqual((response[0], response[1]), (20, 302))
            self.assertEqual(int.from_bytes(response[2][28], "big"), 10)
        self.assertEqual(sdk(self.broker).returncode, 0)


if __name__ == "__main__":
    unittest.main(verbosity=2)
