#!/usr/bin/env python3
"""C-06 first increment: wrong durable class rejected over authenticated TCP.

Reuses the Q1/C-04 process harnesses, not a simulated route. Disposable loopback
development profile only; this focused check does not claim the C-06 fault matrix.
Build server and SDK examples explicitly before running.
"""
from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

import q1_oracle as oracle
from q1_smoke import Broker, committed, manifest_record_count
from test_q1_faults import connect, frame, publish_fields, read_frame
from test_q1_replay import sdk
from test_q2_publish import Campaign, INCARNATION, OWNER_EPOCH, TOPIC

EVIDENCE = (Path(__file__).resolve().parents[1] / "benchmarks" / "evidence"
            / "c06-q1-q2-isolation")


class WrongDurableClass(unittest.TestCase):
    def rejected(self, endpoint, *, topic, qos, owner):
        with connect(SimpleNamespace(endpoint=endpoint)) as connection:
            connection.sendall(frame(
                7, 100,
                publish_fields(1, qos=qos, topic=topic,
                               incarnation=INCARNATION, owner=owner),
                b"wrong-class-must-not-append",
            ))
            kind, request, fields = read_frame(connection)
            self.assertEqual((kind, request), (20, 100))
            # Frozen major-v1 registry: field 28 = ERROR_CODE,
            # code 14 = UNSUPPORTED_GUARANTEE.
            self.assertEqual(int.from_bytes(fields[28], "big"), 14)
            return {"frame_type": kind, "request_id": request, "error_code": 14,
                    "requested_qos": qos, "topic": topic}

    def test_wrong_class_rejected_without_durable_record_on_either_route(self):
        with tempfile.TemporaryDirectory(prefix="rovqet-c06-wire-") as temporary:
            directory = Path(temporary)
            q1_directory = directory / "q1"
            q1_directory.mkdir()
            EVIDENCE.mkdir(parents=True, exist_ok=True)
            journal = oracle.Journal(EVIDENCE / "wire.jsonl", "c06-wire")
            cluster_directory = directory / "q2"
            cluster_directory.mkdir()
            campaign = Campaign(journal, cluster_directory)
            broker = Broker(q1_directory)
            try:
                broker.start()
                committed(broker.publish(), 0)
                before = sdk(broker)
                self.assertEqual(before.returncode, 0, before.stderr)
                manifest = (broker.wal / "MANIFEST").read_bytes()
                count = manifest_record_count(broker.wal)
                rejection = self.rejected(
                    broker.endpoint, topic="durable", qos=2, owner=7,
                )
                journal.append("wire_rejected", **rejection)
                after = sdk(broker)
                self.assertEqual(after.returncode, 0, after.stderr)
                self.assertEqual(after.stdout, before.stdout)
                self.assertEqual((broker.wal / "MANIFEST").read_bytes(), manifest)
                self.assertEqual(manifest_record_count(broker.wal), count)
                journal.append(
                    "q1_history_unchanged", before=before.stdout,
                    after=after.stdout, record_count=count,
                    manifest_sha256=oracle.digest_hex(manifest),
                    incarnation=42, durable_class=1,
                )
                # Refusal did not consume the sequence or allocate an offset.
                committed(broker.publish(1), 2)

                campaign.start((1, 2, 3), bootstrap_only_node=1)
                observed = campaign.publish_until_certain(11, 0, 0, 100, "event-0")
                campaign.expect_committed(observed, 0)
                before_states = campaign.await_message(0, 100)
                self.assertEqual(set(before_states), {1, 2, 3})
                rejection = self.rejected(
                    f"127.0.0.1:{campaign.nodes[1].client_port}",
                    topic=TOPIC, qos=1, owner=OWNER_EPOCH,
                )
                journal.append("wire_rejected", **rejection)
                after_states = campaign.record_states()
                self.assertEqual(set(after_states), {1, 2, 3})
                for number in before_states:
                    self.assertIsNotNone(after_states[number])
                    for key in ("messages", "outcomes", "next_logical_offset"):
                        self.assertEqual(
                            after_states[number][key], before_states[number][key],
                            f"voter {number} changed {key} after wrong-class rejection",
                        )
                # Same producer token succeeds at the next logical offset only
                # when explicitly requested as Q2, not by implicit promotion.
                observed = campaign.publish_until_certain(11, 0, 1, 101, "event-1")
                campaign.expect_committed(observed, 1)
                states = campaign.await_message(1, 101)
                for state in states.values():
                    self.assertEqual(len(state["messages"]), 2)
                    self.assertEqual(state["next_logical_offset"], 2)
                journal.conclusion(oracle.PASS, "wire-level wrong-class checks only")
                print(json.dumps({
                    "scope": "C-06 wire-level wrong durable class",
                    "q1_qos2": "PASS", "q2_qos1": "PASS",
                    "no_durable_record": "PASS",
                    "full_c06": "NOT_RUN",
                }, sort_keys=True))
            except BaseException as error:
                journal.conclusion(oracle.FAIL, str(error))
                raise
            finally:
                broker.close()
                for node in campaign.nodes.values():
                    node.kill()
                    if node.process is not None and node.process.stderr is not None:
                        node.process.stderr.close()
                journal.close()


if __name__ == "__main__":
    unittest.main()
