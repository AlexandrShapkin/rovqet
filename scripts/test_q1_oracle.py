#!/usr/bin/env python3
"""Oracle reliability fixtures: deliberately corrupted evidence must never pass."""
from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

import q1_oracle as oracle


def payload(digest: str = "aa") -> dict:
    return {"message_id": 100, "digest": digest, "incarnation": 42, "offset": 0}


def attempt(**overrides) -> dict:
    record = {
        "event": "publication_attempt",
        "producer": 11,
        "epoch": 0,
        "sequence": 0,
        "message_id": 100,
        "digest": "aa",
        "incarnation": 42,
        "offset": 0,
    }
    record.update(overrides)
    return record


def committed(**overrides) -> dict:
    record = {
        "event": "publication_committed",
        "producer": 11,
        "epoch": 0,
        "sequence": 0,
        "message_id": 100,
        "digest": "aa",
        "incarnation": 42,
        "offset": 0,
    }
    record.update(overrides)
    return record


def recovered(**overrides) -> dict:
    record = {"incarnation": 42, "offset": 0, "message_id": 100, "digest": "aa"}
    record.update(overrides)
    return record


def concluded(records: list[dict]) -> list[dict]:
    return records + [{"event": "conclusion", "verdict": "PASS", "seq": len(records)}]


class PublicationOracle(unittest.TestCase):
    def reconcile(self, records, recovered_records):
        return oracle.reconcile_publications(concluded(records), recovered_records)

    def test_matching_acknowledgement_and_recovery_pass(self):
        result = self.reconcile([attempt(), committed()], [recovered()])
        self.assertEqual(result["verdict"], oracle.PASS, result)

    def test_missing_acknowledged_record_fails(self):
        result = self.reconcile([attempt(), committed()], [])
        self.assertEqual(result["verdict"], oracle.INVALID)
        result = self.reconcile(
            [attempt(), committed()], [recovered(offset=4)]
        )
        self.assertEqual(result["verdict"], oracle.FAIL, result)

    def test_changed_payload_digest_fails(self):
        result = self.reconcile(
            [attempt(), committed()], [recovered(digest="bb")]
        )
        self.assertEqual(result["verdict"], oracle.FAIL)

    def test_duplicate_committed_publication_fails(self):
        result = self.reconcile(
            [attempt(), committed()],
            [recovered(), recovered(offset=4)],
        )
        self.assertEqual(result["verdict"], oracle.FAIL)

    def test_wrong_offset_or_incarnation_fails(self):
        result = self.reconcile(
            [attempt(), committed()], [recovered(incarnation=41)]
        )
        self.assertEqual(result["verdict"], oracle.FAIL)
        result = self.reconcile(
            [attempt(), committed(offset=4)], [recovered()]
        )
        self.assertEqual(result["verdict"], oracle.FAIL)

    def test_uncertain_attempt_is_not_a_failure(self):
        result = self.reconcile(
            [attempt(sequence=1, message_id=101), committed()], [recovered()]
        )
        self.assertEqual(result["verdict"], oracle.PASS, result)
        self.assertEqual(result["counts"]["uncertain"], 1)
        self.assertEqual(result["counts"]["acknowledged"], 1)

    def test_unexplained_recovered_record_fails(self):
        result = self.reconcile(
            [attempt(), committed()],
            [recovered(), recovered(offset=6, message_id=999)],
        )
        self.assertEqual(result["verdict"], oracle.FAIL)

    def test_missing_conclusion_is_invalid(self):
        result = oracle.reconcile_publications(
            [attempt(), committed()], [recovered()]
        )
        self.assertEqual(result["verdict"], oracle.INVALID)

    def test_no_attempts_is_not_run(self):
        result = self.reconcile([], [])
        self.assertEqual(result["verdict"], oracle.NOT_RUN)


class ConsumerOracle(unittest.TestCase):
    def reconcile(self, records, observed, offsets):
        return oracle.reconcile_consumers(concluded(records), observed, offsets)

    def ack(self, offset, generation=1):
        return {
            "event": "ack_committed",
            "group": 9,
            "generation": generation,
            "offset": offset,
            "incarnation": 42,
        }

    def test_contiguous_acknowledgement_passes(self):
        records = [
            {"event": "generation_observed", "group": 9, "generation": 1},
            self.ack(0),
            self.ack(2),
        ]
        result = self.reconcile(
            records, {"delivered_offsets": [], "cursor": 4}, [0, 2]
        )
        self.assertEqual(result["verdict"], oracle.PASS, result)

    def test_acknowledged_redelivery_fails(self):
        records = [{"event": "generation_observed", "group": 9, "generation": 1}, self.ack(0)]
        result = self.reconcile(
            records, {"delivered_offsets": [0], "cursor": 0}, [0, 2]
        )
        self.assertEqual(result["verdict"], oracle.FAIL)

    def test_cursor_crossing_a_gap_fails(self):
        records = [{"event": "generation_observed", "group": 9, "generation": 1}, self.ack(2)]
        result = self.reconcile(
            records, {"delivered_offsets": [], "cursor": 4}, [0, 2]
        )
        self.assertEqual(result["verdict"], oracle.FAIL)

    def test_sparse_acknowledgement_beyond_cursor_is_valid(self):
        records = [{"event": "generation_observed", "group": 9, "generation": 1}, self.ack(2)]
        result = self.reconcile(
            records, {"delivered_offsets": [0], "cursor": 0}, [0, 2]
        )
        self.assertEqual(result["verdict"], oracle.PASS, result)

    def test_stale_generation_commit_fails(self):
        records = [
            {"event": "generation_observed", "group": 9, "generation": 2},
            self.ack(0, generation=1),
        ]
        result = self.reconcile(
            records, {"delivered_offsets": [], "cursor": 2}, [0]
        )
        self.assertEqual(result["verdict"], oracle.FAIL)

    def test_missing_cursor_is_invalid_for_the_check(self):
        records = [{"event": "generation_observed", "group": 9, "generation": 1}, self.ack(0)]
        result = self.reconcile(records, {"delivered_offsets": []}, [0])
        self.assertEqual(result["verdict"], oracle.FAIL)

    def test_lost_ack_reply_is_uncertain_progress_not_a_skip(self):
        records = [
            {"event": "generation_observed", "group": 9, "generation": 1},
            {"event": "delivery", "group": 9, "generation": 1, "offset": 0},
            {"event": "delivery", "group": 9, "generation": 1, "offset": 2},
        ]
        # The ACK reply for offset 0 was lost but its durable progress is visible as a
        # cursor at 2; the record was attempted, so this is uncertain, not a skip.
        result = self.reconcile(
            records, {"delivered_offsets": [], "cursor": 2}, [0, 2]
        )
        self.assertEqual(result["verdict"], oracle.PASS, result)
        self.assertEqual(result["counts"]["uncertain_committed"], 1)

    def test_no_consumer_observations_is_not_run(self):
        result = self.reconcile([], {"delivered_offsets": [], "cursor": 0}, [])
        self.assertEqual(result["verdict"], oracle.NOT_RUN)


class TerminalOracle(unittest.TestCase):
    def terminal(self, *, kind="dlq", **overrides):
        record = {
            "event": "terminal_committed",
            "group": 9,
            "kind": kind,
            "offset": 0,
            "disposition_id": 77,
            "original_message_id": 100,
            "source_incarnation": 42,
            "dlq_incarnation": 77,
            "dlq_offset": 0,
            "payload_digest": "aa",
        }
        record.update(overrides)
        return record

    def publication(self, **overrides):
        record = {
            "event": "dlq_publication",
            "disposition_id": 77,
            "incarnation": 77,
            "offset": 0,
        }
        record.update(overrides)
        return record

    def source(self, offset=0, message_id=100):
        return {"incarnation": 42, "offset": offset, "message_id": message_id,
                "digest": "aa"}

    def dlq(self, offset=0, message_id=77, digest="aa"):
        return {"incarnation": 77, "offset": offset, "message_id": message_id,
                "digest": digest}

    def reconcile(self, records, source, dlq, cursor):
        return oracle.reconcile_terminal(
            concluded(records), source, dlq, cursor
        )

    def test_ordered_dlq_terminal_passes(self):
        result = self.reconcile(
            [self.publication(), self.terminal()],
            [self.source()],
            [self.dlq()],
            8,
        )
        self.assertEqual(result["verdict"], oracle.PASS, result)

    def test_terminal_without_dlq_publication_fails(self):
        result = self.reconcile(
            [self.terminal()], [self.source()], [self.dlq()], 8
        )
        self.assertEqual(result["verdict"], oracle.FAIL)

    def test_missing_or_duplicate_dlq_destination_fails(self):
        result = self.reconcile(
            [self.publication(), self.terminal()], [self.source()], [], 8
        )
        self.assertEqual(result["verdict"], oracle.FAIL)
        result = self.reconcile(
            [self.publication(), self.terminal()],
            [self.source()],
            [self.dlq(), self.dlq(offset=4)],
            8,
        )
        self.assertEqual(result["verdict"], oracle.FAIL)

    def test_changed_dlq_payload_digest_fails(self):
        result = self.reconcile(
            [self.publication(), self.terminal(payload_digest="aa")],
            [self.source()],
            [self.dlq(digest="bb")],
            8,
        )
        self.assertEqual(result["verdict"], oracle.FAIL)

    def test_cursor_crossing_unexplained_source_fails(self):
        result = self.reconcile(
            [self.publication(), self.terminal(offset=2, disposition_id=78)],
            [self.source(0), self.source(2, 101)],
            [self.dlq(message_id=78)],
            8,
        )
        self.assertEqual(result["verdict"], oracle.FAIL)

    def test_terminal_for_uncommitted_source_fails(self):
        result = self.reconcile(
            [self.publication(), self.terminal(offset=9)],
            [self.source()],
            [self.dlq()],
            8,
        )
        self.assertEqual(result["verdict"], oracle.FAIL)

    def test_missing_conclusion_is_invalid(self):
        result = oracle.reconcile_terminal(
            [self.publication(), self.terminal()],
            [self.source()],
            [self.dlq()],
            8,
        )
        self.assertEqual(result["verdict"], oracle.INVALID)

    def test_no_terminal_observations_is_not_run(self):
        result = self.reconcile([], [], [], 0)
        self.assertEqual(result["verdict"], oracle.NOT_RUN)


class JournalIntegrity(unittest.TestCase):
    def test_truncated_or_invalid_journal_is_rejected(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "journal.jsonl"
            journal = oracle.Journal(path, "run")
            journal.append("publication_attempt", producer=11, sequence=0)
            journal.conclusion(oracle.PASS)
            journal.close()
            intact = oracle.load(path)
            self.assertEqual(intact[-1]["verdict"], oracle.PASS)
            text = path.read_text()
            path.write_text(text[:-12])
            with self.assertRaises(oracle.JournalError):
                oracle.load(path)

    def test_missing_conclusion_is_rejected_by_load(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "journal.jsonl"
            journal = oracle.Journal(path, "run")
            journal.append("publication_attempt", producer=11, sequence=0)
            journal.close()
            with self.assertRaises(oracle.JournalError):
                oracle.load(path)

    def test_sequence_gap_is_rejected(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "journal.jsonl"
            records = [
                {"schema": oracle.SCHEMA, "run": "r", "seq": 0, "event": "x"},
                {"schema": oracle.SCHEMA, "run": "r", "seq": 2, "event": "conclusion",
                 "verdict": "PASS"},
            ]
            path.write_text("\n".join(json.dumps(r) for r in records) + "\n")
            with self.assertRaises(oracle.JournalError):
                oracle.load(path)


if __name__ == "__main__":
    unittest.main(verbosity=2)
