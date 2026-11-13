#!/usr/bin/env python3
"""Independent durable-consumer processes over real TCP; disposable storage only.

The journal is written by this parent process, outside the broker WAL. It records delivery
tokens, ACK requests, actually received ACK_COMMITTED results and the recovered cursor.
SIGKILL here is process-crash recovery, not physical power-loss certification.
"""
from __future__ import annotations

import json
from pathlib import Path
import subprocess
import tempfile
import unittest

from q1_smoke import Broker, committed
from test_q1_faults import connect, frame, read_frame

ROOT = Path(__file__).resolve().parents[1]
CONSUME = ROOT / "target/debug/examples/consume_q1"


def consume(broker, *, group=9, generation=0, count=8, size=65536, wait=0,
            ack_count=0, match=None, timeout=20):
    arguments = ["durable", "1", "3", "0", "42", str(group), str(generation),
                 str(count), str(size), str(wait), str(ack_count)]
    if match is not None:
        arguments.append(str(match))
    return subprocess.run([str(CONSUME), *arguments], env=broker.environment(),
                          capture_output=True, text=True, timeout=timeout)


def delivered(result):
    assert result.returncode == 0, (result.stdout, result.stderr)
    deliveries, batch = [], None
    for line in result.stdout.splitlines():
        parts = line.split()
        if parts[0] == "DELIVERY":
            deliveries.append(dict(incarnation=int(parts[1]), offset=int(parts[2]),
                                   message_id=int(parts[3]), generation=int(parts[4]),
                                   lease=int(parts[5]), attempt=int(parts[6]),
                                   deadline=int(parts[7]),
                                   payload=bytes.fromhex(parts[8])))
        elif parts[0] == "GROUP_BATCH":
            batch = dict(group=int(parts[1]), generation=int(parts[2]),
                         next=int(parts[3]), has_more=int(parts[4]), end=int(parts[5]))
        elif parts[0] == "ACK_COMMITTED":
            continue
        else:
            raise AssertionError(parts)
    assert batch is not None, result.stdout
    return deliveries, batch


def ack_committed(result):
    assert result.returncode == 0, (result.stdout, result.stderr)
    return [int(line.split()[2]) for line in result.stdout.splitlines()
            if line.startswith("ACK_COMMITTED")]


class DurableConsumer(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix="rovqet-consumer-")
        self.directory = Path(self.temporary.name)
        self.broker = Broker(self.directory).start()

    def tearDown(self):
        self.broker.close()
        self.temporary.cleanup()

    def publish(self, *sequences):
        for sequence in sequences:
            result = self.broker.publish(sequence)
            assert result.returncode == 0, (result.stdout, result.stderr)

    def restart(self):
        self.broker.stop()
        self.broker = Broker(self.directory).start()

    def test_external_consumer_receives_acks_and_resumes_after_restart(self):
        self.publish(0, 1)
        deliveries, batch = delivered(consume(self.broker, ack_count=8))
        self.assertEqual([d["offset"] for d in deliveries], [0, 2])
        self.assertEqual([d["payload"] for d in deliveries],
                         [b"event-0", b"event-1"])
        self.assertEqual([d["generation"] for d in deliveries], [1, 1])
        self.assertNotEqual(deliveries[0]["lease"], deliveries[1]["lease"])
        self.assertEqual(batch["generation"], 1)
        self.assertEqual(batch["has_more"], 0)
        self.restart()
        deliveries, batch = delivered(consume(self.broker, ack_count=0))
        self.assertEqual(deliveries, [])
        self.assertEqual(batch["generation"], 2)
        self.assertEqual(batch["end"], 1)
        self.publish(2)
        result = consume(self.broker, generation=2, ack_count=1)
        deliveries, _ = delivered(result)
        self.assertEqual([d["payload"] for d in deliveries], [b"event-2"])
        self.assertEqual(ack_committed(result), [deliveries[0]["offset"]])

    def test_out_of_order_ack_never_skips_the_gap_after_restart(self):
        self.publish(0, 1)
        # Acknowledge only the second delivery; the first remains incomplete.
        result = consume(self.broker, ack_count=8, match=2)
        deliveries, _ = delivered(result)
        self.assertEqual([d["offset"] for d in deliveries], [0, 2])
        self.assertEqual(ack_committed(result), [2])
        self.restart()
        deliveries, batch = delivered(consume(self.broker, ack_count=8))
        self.assertEqual([d["offset"] for d in deliveries], [0],
                         "the acknowledged gap must be redelivered, not skipped")
        self.assertEqual(deliveries[0]["payload"], b"event-0")

    def test_sigkill_after_ack_keeps_cursor_and_identity(self):
        self.publish(0)
        result = consume(self.broker, ack_count=1)
        deliveries, _ = delivered(result)
        self.assertEqual(ack_committed(result), [0])
        journal = self.directory / "consumer-oracle.jsonl"
        with journal.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps({
                "offset": deliveries[0]["offset"],
                "message_id": deliveries[0]["message_id"],
                "payload_sha256": __import__("hashlib").sha256(
                    deliveries[0]["payload"]).hexdigest(),
                "generation": deliveries[0]["generation"],
                "lease": deliveries[0]["lease"],
                "ack_committed": True,
            }, sort_keys=True) + "\n")
        self.broker.kill()
        self.broker = Broker(self.directory).start()
        deliveries, batch = delivered(consume(self.broker, ack_count=0))
        self.assertEqual(deliveries, [], "a durably acknowledged message is not redelivered")
        self.assertEqual(batch["end"], 1)
        entry = json.loads(journal.read_text().splitlines()[0])
        self.assertEqual(entry["message_id"], 100)

    def test_crash_windows_never_report_a_false_commit(self):
        for stage, redelivered in [
            ("before-append", True),
            ("after-append-before-sync", True),
            ("after-sync-before-reply", False),
        ]:
            with self.subTest(stage=stage):
                temporary = tempfile.TemporaryDirectory(prefix=f"rovqet-ack-{stage}-")
                directory = Path(temporary.name)
                try:
                    broker = Broker(directory).start()
                    result = broker.publish(0)
                    assert result.returncode == 0, (result.stdout, result.stderr)
                    broker.stop()
                    broker = Broker(directory, stage=stage).start()
                    consumer = subprocess.Popen(
                        [str(CONSUME), "durable", "1", "3", "0", "42", "9", "0",
                         "8", "65536", "0", "1"],
                        env=broker.environment(), stdout=subprocess.PIPE,
                        stderr=subprocess.PIPE, text=True)
                    broker.barrier(stage)
                    broker.kill()
                    consumer.communicate(timeout=20)
                    broker = Broker(directory).start()
                    result = consume(broker, ack_count=1)
                    deliveries, batch = delivered(result)
                    if redelivered:
                        self.assertEqual([d["offset"] for d in deliveries], [0], stage)
                        self.assertEqual(deliveries[0]["payload"], b"event-0")
                        self.assertEqual(ack_committed(result), [0])
                        _, end = delivered(consume(broker, generation=batch["generation"],
                                                   ack_count=0))
                        self.assertEqual(end["end"], 1)
                    else:
                        self.assertEqual(deliveries, [], stage)
                        self.assertEqual(batch["end"], 1)
                finally:
                    try:
                        broker.close()
                    except Exception:
                        pass
                    temporary.cleanup()

    def test_generation_fencing_and_authorization_are_typed(self):
        self.publish(0)
        deliveries, batch = delivered(consume(self.broker, ack_count=1))
        old_generation = batch["generation"]
        self.publish(1)
        deliveries, replaced = delivered(
            consume(self.broker, ack_count=0))  # explicit takeover
        self.assertEqual(replaced["generation"], old_generation + 1)
        stale = consume(self.broker, generation=old_generation, ack_count=0)
        self.assertNotEqual(stale.returncode, 0)
        self.assertIn("StaleGeneration", stale.stdout + stale.stderr)
        self.assertEqual(deliveries[0]["payload"], b"event-1")
        unauthorized = subprocess.run(
            [str(CONSUME), "durable", "2", "3", "0", "42", "9", "0", "8", "65536",
             "0", "0"], env=self.broker.environment(), capture_output=True,
            text=True, timeout=20)
        self.assertNotEqual(unauthorized.returncode, 0)
        self.assertIn("PermissionDenied",
                      unauthorized.stdout + unauthorized.stderr)
        stale_incarnation = subprocess.run(
            [str(CONSUME), "durable", "1", "3", "0", "41", "9", "0", "8", "65536",
             "0", "0"], env=self.broker.environment(), capture_output=True,
            text=True, timeout=20)
        self.assertNotEqual(stale_incarnation.returncode, 0)
        self.assertIn("ReplayNotAvailable",
                      stale_incarnation.stdout + stale_incarnation.stderr)

    def test_incomplete_fetch_response_never_completes_a_batch(self):
        self.publish(0, 1)
        fields = {9: b"durable", 12: b"\x01", 19: bytes(4), 20: (42).to_bytes(16, "big"),
                  24: (9).to_bytes(8, "big"), 21: bytes(8), 31: (8).to_bytes(4, "big"),
                  32: (65536).to_bytes(4, "big"), 33: bytes(4)}
        with connect(self.broker) as connection:
            connection.sendall(frame(17, 900, fields))
            kind, request, message = read_frame(connection)
            self.assertEqual((kind, request), (12, 900))
            self.assertEqual(int.from_bytes(message[21], "big"), 0)
            # Close before FETCH_END: the batch is abandoned, never a completed delivery.
        deliveries, batch = delivered(consume(self.broker, ack_count=0))
        self.assertEqual([d["offset"] for d in deliveries], [0, 2])
        self.assertEqual(batch["end"], 1)


if __name__ == "__main__":
    unittest.main(verbosity=2)
