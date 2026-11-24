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


def concluded(
    records: list[dict], verdict: str = "PASS", reason: str | None = None
) -> list[dict]:
    return records + [
        {"event": "conclusion", "verdict": verdict, "reason": reason,
         "seq": len(records)}
    ]


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


class SafetyOracle(unittest.TestCase):
    """The safety verifier must reject fabricated and contradictory evidence."""

    def base(self) -> list[dict]:
        """A complete, consistent journal of one crash scenario."""
        return [
            {"event": "scenario_started", "scenario": "C1", "barrier": "before_fence",
             "build": "abc123"},
            {"event": "barrier_reached", "barrier": "before_fence", "process": "source"},
            {"event": "process_killed", "process": "source"},
            {"event": "process_restarted", "process": "source"},
            {"event": "publication_attempt", "node": "source", "producer": 11,
             "epoch": 1, "sequence": 0},
            {"event": "publication_committed", "node": "source", "producer": 11,
             "epoch": 1, "sequence": 0, "offset": 0, "message_id": 100, "digest": "aa"},
            {"event": "transfer_verified", "member": "source", "verified": True,
             "inventory": "ff"},
            {"event": "migration_stage", "stage": "catch_up", "revision": 6},
            {"event": "migration_stage", "stage": "fence_old", "revision": 7},
            {"event": "fence_confirmed", "node": "source", "epoch": 7},
            {"event": "transfer_verified", "member": "source", "verified": True,
             "inventory": "ff"},
            {"event": "migration_stage", "stage": "activate_new", "revision": 8},
            {"event": "route_active", "node": "target"},
            {"event": "publication_attempt", "node": "target", "producer": 11,
             "epoch": 1, "sequence": 0},
            {"event": "publication_committed", "node": "target", "producer": 11,
             "epoch": 1, "sequence": 0, "offset": 0, "message_id": 100, "digest": "aa"},
            {"event": "history_observed", "node": "target",
             "records": [{"incarnation": 42, "offset": 0, "message_id": 100,
                          "digest": "aa", "producer": 11, "epoch": 1, "sequence": 0}]},
        ]

    def reconcile(self, records: list[dict]) -> dict:
        result = oracle.reconcile_safety(concluded(records))
        return result

    def test_a_consistent_journal_passes(self):
        result = self.reconcile(self.base())
        self.assertEqual(result["verdict"], oracle.PASS, result)

    def test_compact_broker_stage_spellings_are_accepted(self):
        # The brokers print their committed stage as
        # format!("{:?}", stage).to_lowercase(), so the journals record the
        # compact spelling; both spellings must reconcile to the same verdict.
        compact = {
            "catch_up": "catchup",
            "fence_old": "fenceold",
            "activate_new": "activatenew",
        }
        records = [
            {**record, "stage": compact[record["stage"]]}
            if record.get("event") == "migration_stage"
            else record
            for record in self.base()
        ]
        result = self.reconcile(records)
        self.assertEqual(result["verdict"], oracle.PASS, result)
        # A genuinely unknown stage is still rejected with its original value.
        records.append(
            {"event": "migration_stage", "stage": "teleport", "revision": 9}
        )
        result = self.reconcile(records)
        self.assertEqual(result["verdict"], oracle.FAIL, result)
        self.assertIn("unknown migration stage teleport", result["reasons"])

    def test_a_fabricated_activation_without_a_verified_transfer_fails(self):
        records = self.base()
        records = [r for r in records if r.get("member") != "source"]
        result = self.reconcile(records)
        self.assertEqual(result["verdict"], oracle.FAIL, result)
        self.assertTrue(
            any("verified transfer" in reason for reason in result["reasons"]),
            result["reasons"],
        )

    def test_a_missing_acknowledged_record_fails(self):
        records = self.base()
        for record in records:
            if record.get("event") == "history_observed":
                record["records"] = []
        result = self.reconcile(records)
        self.assertEqual(result["verdict"], oracle.FAIL, result)
        self.assertTrue(
            any("missing from the recovered history" in reason
                for reason in result["reasons"]),
            result["reasons"],
        )

    def test_a_changed_payload_digest_fails(self):
        records = self.base()
        for record in records:
            if record.get("event") == "history_observed":
                record["records"][0]["digest"] = "bb"
        result = self.reconcile(records)
        self.assertEqual(result["verdict"], oracle.FAIL, result)

    def test_overlapping_owner_commits_fail(self):
        records = self.base()
        # The old owner commits after its durable fence.
        records.append({"event": "publication_committed", "node": "source", "producer": 11,
                        "epoch": 1, "sequence": 1, "offset": 2, "message_id": 101,
                        "digest": "cc"})
        result = self.reconcile(records)
        self.assertEqual(result["verdict"], oracle.FAIL, result)
        self.assertTrue(
            any("after its fence" in reason for reason in result["reasons"]),
            result["reasons"],
        )

    def test_a_target_commit_before_the_activation_fails(self):
        records = self.base()
        activation = [r for r in records if r.get("stage") == "activate_new"][0]
        early = dict(records[0])
        early.update({"event": "publication_committed", "node": "target", "producer": 11,
                      "epoch": 1, "sequence": 0, "offset": 0, "message_id": 100,
                      "digest": "aa", "seq": 0})
        records.append(early)
        result = self.reconcile(records)
        self.assertEqual(result["verdict"], oracle.FAIL, result)

    def test_a_missing_dlq_proof_fails(self):
        records = self.base()
        records.append({"event": "dlq_disposition", "disposition_id": 5,
                        "verified": False})
        result = self.reconcile(records)
        self.assertEqual(result["verdict"], oracle.FAIL, result)

    def test_a_duplicated_disposition_fails(self):
        records = self.base()
        records.append({"event": "dlq_disposition", "disposition_id": 5, "verified": True})
        records.append({"event": "dlq_disposition", "disposition_id": 5, "verified": True})
        result = self.reconcile(records)
        self.assertEqual(result["verdict"], oracle.FAIL, result)

    def test_a_missed_barrier_is_never_a_pass(self):
        records = self.base()
        records = [r for r in records if r.get("event") != "barrier_reached"]
        result = self.reconcile(records)
        self.assertEqual(result["verdict"], oracle.FAIL, result)
        self.assertTrue(
            any("never reached its barrier" in reason for reason in result["reasons"]),
            result["reasons"],
        )

    def test_a_missing_conclusion_is_invalid(self):
        result = oracle.reconcile_safety(self.base())
        self.assertEqual(result["verdict"], oracle.INVALID, result)

    def test_an_empty_journal_is_not_run(self):
        result = oracle.reconcile_safety([])
        self.assertEqual(result["verdict"], oracle.INVALID, result)

    def test_a_cursor_that_moves_backwards_fails(self):
        records = self.base()
        records.append({"event": "cursor_observed", "node": "target", "cursor": 4})
        records.append({"event": "cursor_observed", "node": "target", "cursor": 2})
        result = self.reconcile(records)
        self.assertEqual(result["verdict"], oracle.FAIL, result)

    def test_a_chunk_receipt_that_changed_its_digest_fails(self):
        records = self.base()
        records.append({"event": "chunk_receipts", "expected_chunks": 1,
                        "chunks": [{"file_index": 0, "offset": 0, "digest": "aa"},
                                   {"file_index": 0, "offset": 0, "digest": "bb"}]})
        result = self.reconcile(records)
        self.assertEqual(result["verdict"], oracle.FAIL, result)

    def test_a_failed_scenario_conclusion_is_never_a_pass(self):
        # The journal itself is consistent, but the harness concluded FAIL: the safety
        # verdict may never claim success for a scenario that did not succeed.
        result = oracle.reconcile_safety(
            concluded(
                self.base(),
                verdict="FAIL",
                reason="the successor did not serve the bundle",
            )
        )
        self.assertEqual(result["verdict"], oracle.FAIL, result)
        self.assertTrue(
            any("the scenario concluded FAIL" in reason
                for reason in result["reasons"]),
            result["reasons"],
        )
        self.assertTrue(
            any("the successor did not serve the bundle" in reason
                for reason in result["reasons"]),
            result["reasons"],
        )

    def test_an_invalid_scenario_conclusion_is_never_a_pass(self):
        result = oracle.reconcile_safety(
            concluded(
                self.base(),
                verdict="INVALID",
                reason="the durable control never reached after-append-before-sync",
            )
        )
        self.assertNotEqual(result["verdict"], oracle.PASS, result)
        self.assertTrue(
            any("the scenario concluded INVALID" in reason
                for reason in result["reasons"]),
            result["reasons"],
        )

    def test_a_not_run_scenario_conclusion_is_never_a_pass(self):
        result = oracle.reconcile_safety(
            concluded(self.base(), verdict="NOT_RUN")
        )
        self.assertNotEqual(result["verdict"], oracle.PASS, result)
        self.assertTrue(
            any("the scenario concluded NOT_RUN" in reason
                for reason in result["reasons"]),
            result["reasons"],
        )



class Q2RaftOracle(unittest.TestCase):
    """The Q2 Raft oracle must reject every fabricated or contradictory commit."""

    PARTITION = "1:3:0"

    def state(self, node, *, index=2, count=1, chain="aa", request_id="1",
              event="applied_state"):
        return {
            "event": event,
            "node": node,
            "applied_index": index,
            "revision": 1,
            "partitions": [
                {
                    "partition": self.PARTITION,
                    "count": count,
                    "chain": chain,
                    "request_id": request_id,
                }
            ],
        }

    def result(self, node, request_id, *, ok=True, index=2):
        return {
            "event": "proposal_result",
            "node": node,
            "request_id": request_id,
            "ok": ok,
            "index": index,
            "applied_count": 1,
            "reason": None if ok else "no quorum",
        }

    def base(self) -> list[dict]:
        """A complete, consistent journal: one command committed by all three."""
        return [
            {"event": "scenario_started", "scenario": 2,
             "barrier": "committed_replication", "build": "test"},
            {"event": "cluster_started", "cluster_id": 31002, "shard_id": 7,
             "nodes": [1, 2, 3]},
            {"event": "quorum_observed", "available_nodes": 3, "quorum": 2},
            {"event": "proposal_attempt", "node": 1, "request_id": 1},
            self.result(1, 1),
            self.state(1),
            self.state(2),
            self.state(3),
        ]

    def reconcile(self, records: list[dict]) -> dict:
        return oracle.reconcile_q2_raft(concluded(records))

    def test_a_consistent_journal_passes(self):
        result = self.reconcile(self.base())
        self.assertEqual(result["verdict"], oracle.PASS, result)
        self.assertEqual(result["counts"]["scenarios"], 1)
        self.assertEqual(result["counts"]["observations"], 3)
        self.assertEqual(result["counts"]["majority_proofs"], 1)

    def test_a_commit_backed_by_one_node_fails(self):
        records = [
            record
            for record in self.base()
            if not (
                record.get("event") == "applied_state" and record.get("node") != 1
            )
        ]
        result = self.reconcile(records)
        self.assertEqual(result["verdict"], oracle.FAIL, result)
        self.assertTrue(
            any("supported by 1 voter" in reason for reason in result["reasons"]),
            result["reasons"],
        )

    def test_a_disappearing_command_after_a_restart_fails(self):
        records = [
            {"event": "scenario_started", "scenario": 4, "build": "test"},
            {"event": "cluster_started", "cluster_id": 31004, "shard_id": 7},
            {"event": "proposal_result", "node": 1, "request_id": 1, "ok": True},
            self.state(1, count=2, chain="bb", request_id="2"),
            self.state(2, count=2, chain="bb", request_id="2"),
            self.state(3, count=2, chain="bb", request_id="2"),
            {"event": "process_restarted", "node": 2},
            self.state(2, count=1, chain="aa", request_id="1"),
        ]
        result = self.reconcile(records)
        self.assertEqual(result["verdict"], oracle.FAIL, result)
        self.assertTrue(
            any("lost a committed command" in reason for reason in result["reasons"]),
            result["reasons"],
        )

    def test_divergent_committed_chains_fail(self):
        records = self.base()
        for record in records:
            if record.get("event") == "applied_state" and record["node"] == 3:
                record["partitions"][0]["chain"] = "ff"
        result = self.reconcile(records)
        self.assertEqual(result["verdict"], oracle.FAIL, result)
        self.assertTrue(
            any("divergent committed chains" in reason for reason in result["reasons"]),
            result["reasons"],
        )

    def test_a_lone_voter_acknowledgement_fails(self):
        records = self.base()
        records.append({"event": "proposal_attempt", "node": 1, "request_id": 2})
        records.append(self.result(1, 2, index=3))
        # Only the proposing node ever shows request 2 in its committed state; the
        # other two carry the earlier request while claiming the same count.
        records.append(self.state(1, index=3, count=2, chain="bb", request_id="2"))
        records.append(self.state(2, index=3, count=2, chain="bb", request_id="1"))
        records.append(self.state(3, index=3, count=2, chain="bb", request_id="1"))
        result = self.reconcile(records)
        self.assertEqual(result["verdict"], oracle.FAIL, result)
        self.assertTrue(
            any("reported by 1 node" in reason for reason in result["reasons"]),
            result["reasons"],
        )

    def test_an_uncommitted_tail_presented_as_committed_fails(self):
        records = self.base()
        records.append(
            {"event": "outcome_classified", "node": 3, "request_id": 2,
             "classification": "uncommitted"}
        )
        records.append(self.state(1, count=2, chain="bb", request_id="2"))
        result = self.reconcile(records)
        self.assertEqual(result["verdict"], oracle.FAIL, result)
        self.assertTrue(
            any("later reports it" in reason for reason in result["reasons"]),
            result["reasons"],
        )

    def test_a_missing_corruption_outcome_is_invalid(self):
        records = self.base()
        records.append(
            {"event": "corruption_injected", "node": 2, "partition": self.PARTITION}
        )
        result = self.reconcile(records)
        self.assertEqual(result["verdict"], oracle.INVALID, result)
        self.assertTrue(
            any("the corruption outcome was never observed" in reason
                for reason in result["reasons"]),
            result["reasons"],
        )

    def test_a_snapshot_that_lowers_committed_counts_fails(self):
        records = [
            {"event": "scenario_started", "scenario": 12, "build": "test"},
            {"event": "cluster_started", "cluster_id": 31012, "shard_id": 7},
            {"event": "proposal_result", "node": 1, "request_id": 1, "ok": True},
            self.state(1, count=2, chain="bb", request_id="2"),
            self.state(2, count=2, chain="bb", request_id="2"),
            self.state(3, count=2, chain="bb", request_id="2"),
            {"event": "snapshot_installed", "node": 1, "included_index": 5},
            self.state(1, count=1, chain="aa", request_id="1"),
        ]
        result = self.reconcile(records)
        self.assertEqual(result["verdict"], oracle.FAIL, result)
        self.assertTrue(
            any("after its snapshot" in reason for reason in result["reasons"]),
            result["reasons"],
        )

    def test_a_commit_after_a_lost_quorum_fails(self):
        records = self.base()
        records.append({"event": "quorum_observed", "available_nodes": 1, "quorum": 2})
        records.append(self.result(1, 2, index=3))
        records.append(self.state(1, index=3, count=2, chain="bb", request_id="2"))
        records.append(self.state(2, index=3, count=2, chain="bb", request_id="2"))
        result = self.reconcile(records)
        self.assertEqual(result["verdict"], oracle.FAIL, result)
        self.assertTrue(
            any("while only 1 voter" in reason for reason in result["reasons"]),
            result["reasons"],
        )

    def test_a_changed_cluster_or_shard_identity_fails(self):
        records = self.base()
        records.append({"event": "cluster_started", "cluster_id": 31099, "shard_id": 7})
        records.append({"event": "cluster_started", "cluster_id": 31002, "shard_id": 8})
        result = self.reconcile(records)
        self.assertEqual(result["verdict"], oracle.FAIL, result)
        self.assertTrue(
            any("cluster identity changed" in reason for reason in result["reasons"]),
            result["reasons"],
        )
        self.assertTrue(
            any("shard identity changed" in reason for reason in result["reasons"]),
            result["reasons"],
        )

    def test_a_failed_scenario_conclusion_is_never_a_pass(self):
        result = oracle.reconcile_q2_raft(
            concluded(
                self.base(),
                verdict="FAIL",
                reason="the majority never applied the command",
            )
        )
        self.assertEqual(result["verdict"], oracle.FAIL, result)
        self.assertTrue(
            any("the scenario concluded FAIL" in reason for reason in result["reasons"]),
            result["reasons"],
        )
        self.assertTrue(
            any("the majority never applied the command" in reason
                for reason in result["reasons"]),
            result["reasons"],
        )

    def test_state_observations_without_request_identity_are_invalid(self):
        records = self.base()
        for record in records:
            if record.get("event") == "applied_state":
                record["partitions"][0].pop("request_id")
        result = self.reconcile(records)
        self.assertEqual(result["verdict"], oracle.INVALID, result)
        self.assertTrue(
            any("carry no request identity" in reason for reason in result["reasons"]),
            result["reasons"],
        )

    def test_a_missing_conclusion_is_invalid(self):
        result = oracle.reconcile_q2_raft(self.base())
        self.assertEqual(result["verdict"], oracle.INVALID, result)

    def test_an_empty_journal_is_invalid_or_not_run(self):
        result = oracle.reconcile_q2_raft([])
        self.assertEqual(result["verdict"], oracle.INVALID, result)
        result = oracle.reconcile_q2_raft([], require_conclusion=False)
        self.assertEqual(result["verdict"], oracle.NOT_RUN, result)


# --- cross-class (Q1/Q2) isolation fixtures ------------------------------------

ISO_S1, ISO_S2, ISO_S3, ISO_S4 = oracle.ISOLATION_SCENARIOS


def isolation_record(event, scenario, **overrides) -> dict:
    record = {"event": event, "scenario": scenario, "schema": oracle.SCHEMA}
    record.update(overrides)
    return record


def isolation_voter_state(scenario, node, message_id, offset, digest, sequence=0, producer=0):
    applied = {
        "message_id": message_id, "offset": offset, "digest": digest,
        "producer": producer, "producer_epoch": 0, "sequence": sequence,
    }
    return isolation_record(
        "voter_state", scenario, node=node, applied_index=offset + 1,
        next_logical_offset=offset + 1, chain="c0ffee", count=1,
        messages=[applied], outcomes=[dict(applied)],
        partition={"partition_id": 0, "tenant_id": 1, "topic_id": 4},
    )


def isolation_journal() -> list[dict]:
    """A minimal well-formed journal: preserved restart, fail-closed loss, quorum loss."""
    partition = {"partition_id": 0, "tenant_id": 1, "topic_id": 3}
    records = [
        # Q1 restart preserves incarnation and offsets.
        isolation_record("broker_started", ISO_S1, durable_class=1, incarnation=42,
                         initialize_new_log=True, owner_epoch=7, partition=partition),
        isolation_record("publish_ack", ISO_S1, durable_class=1, incarnation=42, producer=11,
                         epoch=0, sequence=0, message_id=100, offset=0,
                         payload_digest="aa", partition=partition,
                         client_stdout="COMMITTED_LOCAL 42 0"),
        isolation_record("publish_ack", ISO_S1, durable_class=1, incarnation=42, producer=11,
                         epoch=0, sequence=1, message_id=101, offset=2,
                         payload_digest="bb", partition=partition,
                         client_stdout="COMMITTED_LOCAL 42 2"),
        isolation_record("offsets_journaled", ISO_S1, incarnation=42, offsets=[0, 2],
                         manifest_records=4),
        isolation_record("broker_killed", ISO_S1, incarnation=42, signal="SIGKILL",
                         offsets=[0, 2], manifest_records=4),
        isolation_record("broker_restarted", ISO_S1, incarnation=42,
                         initialize_new_log=False, same_data_dir=True, owner_epoch=7),
        isolation_record("history_recovered", ISO_S1, incarnations=[42], offsets=[0, 2],
                         record_count=2,
                         client_stdout=["READ_RECORD 42 0 100 aa", "READ_RECORD 42 2 101 bb"]),
        isolation_record("dedup_retry", ISO_S1, incarnation=42, producer=11, sequence=0,
                         offset=0, client_stdout="COMMITTED_LOCAL 42 0"),
        isolation_record("conclusion", ISO_S1, verdict="PASS", reason="restart preserved"),
        # Q1 unrecoverable history fails closed and the fresh incarnation is distinct.
        isolation_record("broker_started", ISO_S2, durable_class=1, incarnation=42,
                         initialize_new_log=True, owner_epoch=7, partition=partition),
        isolation_record("publish_ack", ISO_S2, durable_class=1, incarnation=42, producer=11,
                         epoch=0, sequence=0, message_id=100, offset=0,
                         payload_digest="aa", partition=partition,
                         client_stdout="COMMITTED_LOCAL 42 0"),
        isolation_record("broker_stopped", ISO_S2, incarnation=42, offsets=[0],
                         manifest_records=2, signal="SIGINT"),
        isolation_record("history_destroyed", ISO_S2, incarnation=42, offsets=[0],
                         manifest_records=2, removed_paths=["wal/MANIFEST"]),
        isolation_record("recovery_refused", ISO_S2, incarnation=42, exit_code=1,
                         manifest_recreated=False, offsets=[0],
                         error_text="REPLAY_NOT_AVAILABLE: existing Q1 history missing"),
        isolation_record("broker_started", ISO_S2, durable_class=1, incarnation=43,
                         supersedes_incarnation=42, initialize_new_log=True, owner_epoch=7,
                         partition=partition),
        isolation_record("old_offsets_not_replayable", ISO_S2, incarnation=42, offsets=[0],
                         manifest_records=0, old_cursor_returncode=1),
        isolation_record("fresh_incarnation_publish", ISO_S2, new_incarnation=43,
                         old_incarnation=42, old_offsets=[0], fresh_offset=0, producer=11,
                         sequence=0, message_id=100, client_stdout="COMMITTED_LOCAL 43 0"),
        isolation_record("conclusion", ISO_S2, verdict="PASS", reason="fail closed"),
        # Q2 quorum loss blocks the quorum ACK while Q1 still commits.
        isolation_record("broker_started", ISO_S3, node=2, bootstrap=False),
        isolation_record("broker_started", ISO_S3, node=3, bootstrap=False),
        isolation_record("shared_broker_started", ISO_S3, node=1, pid=1,
                         q1_topic="durable", q2_topic="durable-q2"),
        isolation_record("q2_publish_attempt", ISO_S3, producer=31, epoch=0, sequence=0,
                         message_id=300, payload_digest="cc"),
        isolation_record("q2_client_result", ISO_S3, producer=31, message_id=300,
                         returncode=0, timed_out=False,
                         client_stdout='{"outcome":"committed_quorum","sequence":0,'
                                       '"offset":0,"incarnation":42}'),
        isolation_voter_state(ISO_S3, 1, 300, 0, "cc", producer=31),
        isolation_voter_state(ISO_S3, 2, 300, 0, "cc", producer=31),
        isolation_voter_state(ISO_S3, 3, 300, 0, "cc", producer=31),
        isolation_record("healthy_baseline", ISO_S3, q1_ack="COMMITTED_LOCAL", q1_offset=0,
                         q2_offset=0, q2_payload_digest="cc",
                         q2_ack={"outcome": "committed_quorum", "sequence": 0, "offset": 0,
                                 "incarnation": 42}),
        isolation_record("broker_killed", ISO_S3, node=2, exit_code=-9),
        isolation_record("voter_stop_observed", ISO_S3, node=2, returncode=-9,
                         signal="SIGKILL"),
        isolation_record("broker_killed", ISO_S3, node=3, exit_code=-9),
        isolation_record("voter_stop_observed", ISO_S3, node=3, returncode=-9,
                         signal="SIGKILL"),
        isolation_record("authority_disturbed", ISO_S3, authority="Q2 data quorum",
                         configured_voters=[1, 2, 3], live_voters=[1], stopped_voters=[2, 3],
                         required_majority=2),
        isolation_record("q2_publish_attempt", ISO_S3, producer=32, epoch=0, sequence=0,
                         message_id=301, payload_digest="dd"),
        isolation_record("q2_client_result", ISO_S3, producer=32, message_id=301,
                         returncode=None, timed_out=True, timeout_seconds=15,
                         client_stdout="", client_stderr=""),
        isolation_record("quorum_loss_outcome", ISO_S3, delivery_claim="unknown",
                         observed={"detail": "client timeout", "outcome": "unknown"}),
        isolation_record("q1_commits_without_q2_quorum", ISO_S3, offset=2, previous_offset=0,
                         live_q2_voters=[1], returncode=0,
                         client_stdout="COMMITTED_LOCAL 42 2"),
        isolation_record("broker_started", ISO_S3, node=2, bootstrap=False),
        isolation_record("voter_restarted_on_original_storage", ISO_S3, node=2),
        isolation_record("broker_started", ISO_S3, node=3, bootstrap=False),
        isolation_record("voter_restarted_on_original_storage", ISO_S3, node=3),
        isolation_record("q2_publish_attempt", ISO_S3, producer=33, epoch=0, sequence=0,
                         message_id=302, payload_digest="ee"),
        isolation_record("q2_client_result", ISO_S3, producer=33, message_id=302,
                         returncode=0, timed_out=False,
                         client_stdout='{"outcome":"committed_quorum","sequence":0,'
                                       '"offset":2,"incarnation":42}'),
        isolation_voter_state(ISO_S3, 1, 302, 2, "ee", producer=33),
        isolation_voter_state(ISO_S3, 2, 302, 2, "ee", producer=33),
        isolation_voter_state(ISO_S3, 3, 302, 2, "ee", producer=33),
        isolation_record("conclusion", ISO_S3, verdict="PASS", reason="quorum loss blocked"),
        # Q2 history survives isolated Q1 storage loss.
        isolation_record("broker_started", ISO_S4, node=1, bootstrap=True,
                         q1_topic="durable", q2_topic="durable-q2"),
        isolation_record("broker_started", ISO_S4, node=2, bootstrap=False),
        isolation_record("broker_started", ISO_S4, node=3, bootstrap=False),
        isolation_record("publication_committed", ISO_S4, durable_class=2, producer=41,
                         epoch=0, sequence=0, message_id=400, offset=0,
                         payload_digest="dc77", outcome="committed_quorum",
                         partition={"partition_id": 0, "tenant_id": 1, "topic_id": 4}),
        isolation_voter_state(ISO_S4, 1, 400, 0, "dc77", producer=41),
        isolation_voter_state(ISO_S4, 2, 400, 0, "dc77", producer=41),
        isolation_voter_state(ISO_S4, 3, 400, 0, "dc77", producer=41),
        isolation_record("q1_storage_corrupted", ISO_S4,
                         path="throwaway-q1/wal/segment-00000000000000000000.wal",
                         operation="truncate to zero bytes", bytes_before=265, bytes_after=0),
        isolation_record("q1_storage_corrupted", ISO_S4,
                         path="throwaway-q1/wal/MANIFEST",
                         operation="overwrite with invalid manifest bytes",
                         bytes_before=118, bytes_after=20),
        isolation_record("q2_history_read_back", ISO_S4, node=1, incarnation=42, offset=0,
                         payload_digest="dc77"),
        isolation_record("q2_history_read_back", ISO_S4, node=2, incarnation=42, offset=0,
                         payload_digest="dc77"),
        isolation_record("q2_history_read_back", ISO_S4, node=3, incarnation=42, offset=0,
                         payload_digest="dc77"),
        isolation_record("conclusion", ISO_S4, verdict="PASS",
                         reason="Q2 history read back from all three persisted voter states"),
    ]
    return records


class IsolationOracle(unittest.TestCase):
    def reconcile(self, records=None):
        return oracle.reconcile_q1_q2_isolation(
            isolation_journal() if records is None else records
        )

    def reasons(self, records) -> str:
        return " | ".join(self.reconcile(records)["reasons"])

    def test_well_formed_journal_passes(self):
        result = self.reconcile()
        self.assertEqual(result["verdict"], oracle.PASS, result)
        self.assertEqual(result["counts"]["scenarios"], 4, result)
        self.assertEqual(result["counts"]["q2_blocked_timeouts"], 1, result)
        self.assertEqual(result["counts"]["majority_loss_windows"], 1, result)

    def test_wrong_class_acknowledgement_fails(self):
        records = isolation_journal()
        for record in records:
            if record["event"] == "q2_client_result" and record.get("producer") == 31:
                record["client_stdout"] = (
                    '{"outcome":"committed_local","sequence":0,"offset":0,"incarnation":42}'
                )
        self.assertEqual(self.reconcile(records)["verdict"], oracle.FAIL)
        self.assertIn("wrong-class acknowledgement", self.reasons(records))

    def test_quorum_ack_without_majority_evidence_fails(self):
        records = [r for r in isolation_journal() if r["event"] != "voter_state"]
        self.assertIn("backed only by Q1 evidence", self.reasons(records))
        self.assertEqual(self.reconcile(records)["verdict"], oracle.FAIL)

    def test_reused_incarnation_after_loss_fails(self):
        records = isolation_journal()
        index = next(i for i, r in enumerate(records) if r["event"] == "history_destroyed")
        records.insert(index + 2, isolation_record(
            "broker_started", ISO_S2, durable_class=1, incarnation=42,
            initialize_new_log=True,
        ))
        self.assertIn("reused Q1 incarnation", self.reasons(records))

    def test_merged_identity_across_classes_fails(self):
        records = isolation_journal()
        records.insert(1, isolation_record(
            "q2_client_result", ISO_S3, producer=11, epoch=0, sequence=0, incarnation=42,
            message_id=900, returncode=0, timed_out=False,
            client_stdout='{"outcome":"committed_quorum","sequence":0,"offset":0,'
                          '"incarnation":42}',
        ))
        self.assertIn("merged under one identity", self.reasons(records))

    def test_acknowledged_offset_missing_from_recovery_fails(self):
        records = isolation_journal()
        for record in records:
            if record["event"] == "history_recovered":
                record["offsets"] = [0]
                record["client_stdout"] = ["READ_RECORD 42 0 100 aa"]
        self.assertIn("missing from its recovery domain", self.reasons(records))
        self.assertEqual(self.reconcile(records)["verdict"], oracle.FAIL)

    def test_divergent_offsets_for_one_token_fail(self):
        records = isolation_journal()
        for record in records:
            if record["event"] == "dedup_retry":
                record["offset"] = 4
                record["client_stdout"] = "COMMITTED_LOCAL 42 4"
        self.assertIn("divergent logical offsets", self.reasons(records))

    def test_mutation_after_kill_without_recovery_fails(self):
        records = isolation_journal()
        index = next(i for i, r in enumerate(records)
                     if r["event"] == "broker_killed" and r.get("node") is None)
        records.insert(index + 1, isolation_record(
            "publish_ack", ISO_S1, durable_class=1, incarnation=42, producer=11,
            epoch=0, sequence=1, message_id=101, offset=2, payload_digest="bb",
            client_stdout="COMMITTED_LOCAL 42 2",
        ))
        self.assertIn("fenced/stale owner", self.reasons(records))

    def test_cursor_advance_across_incomplete_record_fails(self):
        records = isolation_journal()
        records.append(isolation_record(
            "group_state", ISO_S3,
            group={"cursor": 2, "generation": 1, "group_generation": 1, "outcomes": [0]},
        ))
        self.assertIn("cursor advanced across an incomplete record", self.reasons(records))

    def test_q2_success_during_majority_loss_fails(self):
        records = isolation_journal()
        index = next(i for i, r in enumerate(records) if r["event"] == "authority_disturbed")
        records.insert(index + 1, isolation_record(
            "q2_client_result", ISO_S3, producer=34, message_id=303, incarnation=42,
            epoch=0, sequence=0, returncode=0, timed_out=False,
            client_stdout='{"outcome":"committed_quorum","sequence":0,"offset":3,'
                          '"incarnation":42}',
        ))
        self.assertIn("Q2 success during proven majority loss", self.reasons(records))
        self.assertEqual(self.reconcile(records)["verdict"], oracle.FAIL)

    def test_incomplete_scenario_set_is_not_pass(self):
        records = [r for r in isolation_journal()
                   if not (r["event"] == "conclusion" and r["scenario"] == ISO_S2)]
        result = self.reconcile(records)
        self.assertNotEqual(result["verdict"], oracle.PASS)
        self.assertIn("incomplete scenario set", " | ".join(result["reasons"]))

    def test_storage_loss_scenario_is_required_for_pass(self):
        """Four scenarios minus the Q2-history-survives-Q1-storage-loss conclusion."""
        records = [r for r in isolation_journal()
                   if not (r["event"] == "conclusion" and r["scenario"] == ISO_S4)]
        result = self.reconcile(records)
        self.assertEqual(result["verdict"], oracle.INVALID, result)
        self.assertEqual(result["counts"]["scenarios"], 3, result)
        reasons = " | ".join(result["reasons"])
        self.assertIn("incomplete scenario set", reasons)
        self.assertIn(ISO_S4, reasons)


if __name__ == "__main__":
    from test_q2_consumer_oracle import Q2ConsumerFixtures

    unittest.main(verbosity=2)
