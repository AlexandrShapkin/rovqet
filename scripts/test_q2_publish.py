#!/usr/bin/env python3
"""C-04 Q2 publication campaign and its independent-oracle negative fixtures.

The campaign starts three real brokers in cluster mode, each with one embedded Q2
shard voter. A real SDK client binary (`target/debug/examples/q2_publish`) publishes
over TCP; every acknowledgment it prints is journaled verbatim. After each attempt the
campaign reads each voter's own committed `state.json` from that voter's data directory
and journals a copy. The oracle in `q1_oracle.reconcile_q2_publish` then establishes
the properties from those observations: a COMMITTED_QUORUM acknowledgment needs a
majority of distinct voters whose committed state carries the acknowledged message at
the acknowledged offset with the same exact payload bytes; an idempotent retry reports
the original offset and creates no second message; a definite rejection leaves the
committed state untouched; and an uncertain outcome is never counted as a commit. A
conflicting retry of an already-committed token is an identity conflict, not a
definite rejection of that token: the broker keeps the original commit and the
journal records the conflict as its own event. A voter state missing any quorum
witness field (node, partition, applied index, committed count, chain or message
identity) is INVALID evidence and never counts toward the majority. A digest without
the payload bytes cannot prove what a voter recovered, and a payload whose bytes
contradict their digest is a failure.

The required proof "leader crash strictly before a durable majority" is exercised by a
deterministic, explicitly-armed broker seam. A broker started with
`ROVQET_Q2_PREMAJORITY_BARRIER=<dir>` holds replication of exactly the `arm.json`
producer token before any follower socket is opened, and only after its own
`FileLogStorage` completed `sync_all` for that entry. It then writes `barrier.json`
(node, log index, durable index, token), so the parent can SIGKILL the named leader
before the entry reaches a quorum. Without that environment variable the same
production binary is inert, and the seam writes nothing. The scenario journals the
barrier, kills the leader, verifies no voter committed the entry, disarms the seam,
restarts the node and retries the same producer token: the retry must settle at the
next logical offset exactly once on every voter.

The distinct proof "leader crash after a durable majority but before the response" is
exercised by the mirrored `ROVQET_Q2_POSTMAJORITY_BARRIER=<dir>` seam. It engages in
the client-write completion of the leader - after openraft returned the applied result
of the entry, so the majority already committed it, and before the caller can build
the acknowledgment - writes `barrier.json` (node, committed index, logical offset,
token) and never returns. The parent waits for that barrier, waits until every running
voter's own `state.json` carries the message at the barrier's logical offset (the
majority proof that predates the kill), SIGKILLs the named node, and records that the
client call ended without any successful response. The group is then restarted on its
original storage: the retry of the identical token must resolve to the one committed
record at the barrier's offset, and a second retry must report the same offset without
creating a duplicate.

Two further scenarios close the remaining matrix rows: one stops exactly one follower
(identified from the last applied entry every voter persisted), publishes a real
COMMITTED_QUORUM publication through the SDK while that follower is unavailable and
then restarts it to check the catch-up; the other is the post-majority crash window
described above.

The seam is compiled only into a build with the `fault-injection` cargo feature, so
the live campaign needs the broker and the publishing example built with:

    cargo build -p rovqet-server --features fault-injection --locked
    cargo build -p rovqet-sdk --example q2_publish --locked

A broker built without that feature has no active pre-majority or post-majority
behavior at all; the preflight reports it as NOT_RUN instead of letting a barrier
scenario fail at a barrier that cannot exist.

Default invocation runs the negative fixtures and then the campaign. `--fixtures-only`
runs only the oracle fixture suite. Missing binaries are a real NOT_RUN observation,
not a silent pass; the campaign exits 2 in that case.
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
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import q1_oracle as oracle  # noqa: E402

ROOT = Path(__file__).resolve().parents[1]
SERVER = ROOT / "target" / "debug" / "rovqet"
EXAMPLE = ROOT / "target" / "debug" / "examples" / "q2_publish"
# The barrier environment variables exist in the broker binary only when it was
# built with the `fault-injection` feature, which is the only build that compiles
# the pre-majority and the post-majority seam.
SEAM_MARKER = b"ROVQET_Q2_PREMAJORITY_BARRIER"
POSTMAJORITY_MARKER = b"ROVQET_Q2_POSTMAJORITY_BARRIER"
EVIDENCE_DIR = ROOT / "benchmarks" / "evidence" / "c04-q2-publish"
JOURNAL_PATH = EVIDENCE_DIR / "q2-publish.jsonl"

PASS = "PASS"
FAIL = "FAIL"
INVALID = "INVALID"
NOT_RUN = "NOT_RUN"

CLUSTER = 4242
SHARD = 7
TOPIC = "durable-q2"
TENANT = 1
TOPIC_ID = 4
PARTITION = 0
INCARNATION = 42
OWNER_EPOCH = 5
PARTITION_KEY = f"{TENANT}:{TOPIC_ID}:{PARTITION}"

T_READY = 45.0
T_APPLIED = 30.0
T_PUBLISH = 60.0
T_POLL = 0.05


class CheckFailure(Exception):
    """A harness observation contradicted the C-04 contract."""


def digest_hex(payload: str) -> str:
    return oracle.digest_hex(payload.encode())


def payload_hex(payload: str) -> str:
    """The exact committed payload bytes as the JSON-safe hex the journal carries."""
    return payload.encode().hex()


def append_voter_state(journal: oracle.Journal, state: dict) -> dict:
    """Journal one voter-state observation without reusing the event keyword.

    `Node.read_state` returns a complete journal record, including its `event` field.
    `Journal.append(event, **fields)` owns that keyword, so the embedded copy is
    dropped here and the record is written with the single explicit event name.
    """
    fields = {key: value for key, value in state.items() if key != "event"}
    return journal.append("voter_state", **fields)


# --- Oracle fixtures ------------------------------------------------------------


def state(node: int, *, offset_count=1, chain="cc", **overrides) -> dict:
    record = {
        "event": "voter_state",
        "node": node,
        "applied_index": 2,
        "partition": {"tenant_id": TENANT, "topic_id": TOPIC_ID, "partition_id": PARTITION},
        "count": offset_count,
        "chain": chain,
        "next_logical_offset": offset_count,
        "messages": [
            {
                "offset": index,
                "message_id": 100 + index,
                "digest": digest_hex("alpha"),
                "payload": payload_hex("alpha"),
                "producer": 11,
                "producer_epoch": 0,
                "sequence": index,
            }
            for index in range(offset_count)
        ],
        "outcomes": [
            {
                "producer": 11,
                "producer_epoch": 0,
                "sequence": index,
                "offset": index,
                "message_id": 100 + index,
                "digest": digest_hex("alpha"),
            }
            for index in range(offset_count)
        ],
    }
    record.update(overrides)
    return record


def attempt(**overrides) -> dict:
    record = {
        "event": "publication_attempt",
        "producer": 11,
        "epoch": 0,
        "sequence": 0,
        "message_id": 100,
        "digest": digest_hex("alpha"),
        "incarnation": INCARNATION,
        "owner_epoch": OWNER_EPOCH,
    }
    record.update(overrides)
    return record


def committed(**overrides) -> dict:
    record = {
        "event": "publication_committed",
        "outcome": "committed_quorum",
        "producer": 11,
        "epoch": 0,
        "sequence": 0,
        "message_id": 100,
        "digest": digest_hex("alpha"),
        "incarnation": INCARNATION,
        "offset": 0,
        "partition": {
            "tenant_id": TENANT,
            "topic_id": TOPIC_ID,
            "partition_id": PARTITION,
        },
    }
    record.update(overrides)
    return record


def postmajority_barrier(**overrides) -> dict:
    record = {
        "event": "postmajority_barrier",
        "node": 1,
        "committed_index": 9,
        "logical_offset": 0,
        "producer_id": 11,
        "producer_epoch": 0,
        "sequence": 0,
        "message_id": 100,
    }
    record.update(overrides)
    return record


def killed(node: int = 1) -> dict:
    return {"event": "broker_killed", "node": node, "exit_code": -9}


def uncertain(**overrides) -> dict:
    record = {
        "event": "publication_unknown",
        "producer": 11,
        "epoch": 0,
        "sequence": 0,
        "message_id": 100,
        "digest": digest_hex("alpha"),
        "reason": "the client call ended without any successful response",
    }
    record.update(overrides)
    return record


def postmajority_base() -> list[dict]:
    """A complete, consistent post-majority barrier evidence set.

    The armed token is committed and applied, three pre-kill voter states witness it
    at the barrier offset, the barrier node is then killed, the interrupted call is
    recorded as uncertain and the retry commits at the same barrier offset.
    """
    return [
        {"event": "scenario_started", "scenario": 14},
        attempt(),
        postmajority_barrier(),
        state(1),
        state(2),
        state(3),
        killed(),
        uncertain(),
        committed(),
        state(1),
        state(2),
        state(3),
    ]


def base() -> list[dict]:
    return [
        {"event": "scenario_started", "scenario": 1},
        attempt(),
        committed(),
        state(1),
        state(2),
        state(3),
    ]


def concluded(records: list[dict], verdict: str = PASS, reason: str | None = None):
    return records + [{"event": "conclusion", "verdict": verdict, "reason": reason}]


class Q2PublishOracleFixtures(unittest.TestCase):
    """Deliberately corrupted Q2 publication evidence must never pass."""

    def reconcile(self, records):
        return oracle.reconcile_q2_publish(concluded(records))

    def test_consistent_journal_passes(self):
        result = self.reconcile(base())
        self.assertEqual(result["verdict"], oracle.PASS, result)
        self.assertEqual(result["counts"]["committed"], 1)
        self.assertEqual(result["counts"]["majority_proofs"], 1)

    def test_empty_journal_is_not_run(self):
        result = self.reconcile([{"event": "scenario_started", "scenario": 1}])
        self.assertEqual(result["verdict"], oracle.NOT_RUN, result)

    def test_missing_conclusion_is_invalid(self):
        result = oracle.reconcile_q2_publish(base())
        self.assertEqual(result["verdict"], oracle.INVALID, result)

    def test_a_scenario_that_concluded_fail_is_never_a_pass(self):
        result = oracle.reconcile_q2_publish(
            concluded(base(), FAIL, "witness missing")
        )
        self.assertEqual(result["verdict"], oracle.FAIL, result)

    def test_committed_ack_without_voter_state_is_invalid(self):
        result = self.reconcile([attempt(), committed()])
        self.assertEqual(result["verdict"], oracle.INVALID, result)

    def test_one_voter_is_not_a_quorum(self):
        result = self.reconcile([attempt(), committed(), state(1)])
        self.assertEqual(result["verdict"], oracle.FAIL, result)

    def test_voters_disagreeing_on_chain_fail(self):
        result = self.reconcile(
            [attempt(), committed(), state(1), state(2, chain="dd"), state(3)]
        )
        self.assertEqual(result["verdict"], oracle.FAIL, result)

    def test_fabricated_acknowledgment_without_the_message_fails(self):
        forged = committed(offset=5)
        result = self.reconcile([attempt(), forged, state(1), state(2), state(3)])
        self.assertEqual(result["verdict"], oracle.FAIL, result)

    def test_changed_retry_offset_fails(self):
        result = self.reconcile(
            [attempt(), committed(offset=0), committed(offset=1),
             state(1, offset_count=2), state(2, offset_count=2),
             state(3, offset_count=2)]
        )
        self.assertEqual(result["verdict"], oracle.FAIL, result)

    def test_retry_with_a_different_message_id_fails(self):
        result = self.reconcile(
            [attempt(), committed(message_id=100), committed(offset=0, message_id=101),
             state(1), state(2), state(3)]
        )
        self.assertEqual(result["verdict"], oracle.FAIL, result)

    def test_a_wrong_outcome_code_fails(self):
        result = self.reconcile(
            [attempt(), committed(outcome="committed_local"), state(1), state(2),
             state(3)]
        )
        self.assertEqual(result["verdict"], oracle.FAIL, result)

    def test_a_changed_payload_digest_fails(self):
        result = self.reconcile(
            [attempt(), committed(digest=digest_hex("other")), state(1), state(2),
             state(3)]
        )
        self.assertEqual(result["verdict"], oracle.FAIL, result)

    def test_a_digest_only_voter_state_without_payload_is_invalid(self):
        without = state(1)
        del without["messages"][0]["payload"]
        result = self.reconcile(
            [attempt(), committed(), without, state(2), state(3)]
        )
        self.assertEqual(result["verdict"], oracle.INVALID, result)

    def test_a_voter_state_with_a_null_payload_is_invalid(self):
        without = state(1)
        without["messages"][0]["payload"] = None
        result = self.reconcile(
            [attempt(), committed(), without, state(2), state(3)]
        )
        self.assertEqual(result["verdict"], oracle.INVALID, result)

    def test_a_payload_mutated_after_recovery_fails(self):
        mutated = state(1)
        mutated["messages"][0]["payload"] = payload_hex("tampered")
        result = self.reconcile(
            [attempt(), committed(), mutated, state(2), state(3)]
        )
        self.assertEqual(result["verdict"], oracle.FAIL, result)

    def test_a_payload_mutated_on_one_recovered_voter_fails(self):
        tampered = state(3)
        tampered["messages"][0]["payload"] = payload_hex("tampered")
        result = self.reconcile(
            [attempt(), committed(), state(1), state(2), tampered]
        )
        self.assertEqual(result["verdict"], oracle.FAIL, result)

    def test_a_payload_that_changes_between_observations_fails(self):
        before = state(1)
        after = state(1)
        after["messages"][0]["payload"] = payload_hex("tampered")
        result = self.reconcile(
            [attempt(), committed(), before, state(2), state(3), after]
        )
        self.assertEqual(result["verdict"], oracle.FAIL, result)

    def test_an_undecodable_payload_is_invalid(self):
        broken = state(1)
        broken["messages"][0]["payload"] = "not-hex"
        result = self.reconcile(
            [attempt(), committed(), broken, state(2), state(3)]
        )
        self.assertEqual(result["verdict"], oracle.INVALID, result)

    def test_missing_applied_indexes_cannot_form_a_quorum(self):
        # Without this check every state groups under (None, count, chain) and the
        # three missing values would manufacture a majority out of missing evidence.
        witnesses = [state(1), state(2), state(3)]
        for record in witnesses:
            record["applied_index"] = None
        result = self.reconcile([attempt(), committed(), *witnesses])
        self.assertEqual(result["verdict"], oracle.INVALID, result)
        self.assertEqual(result["counts"]["majority_proofs"], 0, result)

    def test_missing_chain_digests_cannot_form_a_quorum(self):
        witnesses = [state(1), state(2), state(3)]
        for record in witnesses:
            record["chain"] = None
        result = self.reconcile([attempt(), committed(), *witnesses])
        self.assertEqual(result["verdict"], oracle.INVALID, result)
        self.assertEqual(result["counts"]["majority_proofs"], 0, result)

    def test_one_witness_without_an_applied_index_is_invalid(self):
        incomplete = state(2)
        incomplete["applied_index"] = None
        result = self.reconcile(
            [attempt(), committed(), state(1), incomplete, state(3)]
        )
        self.assertEqual(result["verdict"], oracle.INVALID, result)

    def test_a_witness_without_a_node_identity_is_invalid(self):
        incomplete = state(2)
        incomplete["node"] = None
        result = self.reconcile(
            [attempt(), committed(), state(1), incomplete, state(3)]
        )
        self.assertEqual(result["verdict"], oracle.INVALID, result)

    def test_a_witness_without_a_message_id_is_invalid(self):
        incomplete = state(3)
        incomplete["messages"][0]["message_id"] = None
        result = self.reconcile(
            [attempt(), committed(), state(1), state(2), incomplete]
        )
        self.assertEqual(result["verdict"], oracle.INVALID, result)

    def test_a_witness_without_a_payload_digest_is_invalid(self):
        incomplete = state(1)
        incomplete["messages"][0]["digest"] = None
        result = self.reconcile(
            [attempt(), committed(), incomplete, state(2), state(3)]
        )
        self.assertEqual(result["verdict"], oracle.INVALID, result)

    def test_a_committed_count_that_regresses_fails(self):
        later = state(1, offset_count=0, messages=[], outcomes=[], next_logical_offset=0)
        result = self.reconcile(
            [attempt(), committed(), state(1), state(2), state(3), later]
        )
        self.assertEqual(result["verdict"], oracle.FAIL, result)

    def test_a_committed_message_does_not_require_a_retained_outcome(self):
        without = state(1, outcomes=[])
        result = self.reconcile(
            [attempt(), committed(), without, state(2), state(3)]
        )
        # The oracle only needs the message; the missing outcome is not a contradiction.
        self.assertEqual(result["verdict"], oracle.PASS, result)

    def test_a_definite_rejection_of_the_committed_identity_fails(self):
        rejection = {
            "event": "publication_rejected",
            "producer": 11,
            "epoch": 0,
            "sequence": 0,
            "message_id": 100,
            "digest": digest_hex("alpha"),
            "code": "SequenceConflict",
        }
        result = self.reconcile(
            [attempt(), committed(), rejection, state(1), state(2), state(3)]
        )
        self.assertEqual(result["verdict"], oracle.FAIL, result)
        self.assertTrue(
            any("same message identity" in reason for reason in result["reasons"]),
            result,
        )

    def test_a_rejection_of_a_committed_token_with_another_identity_fails(self):
        # The token is committed with message 100 and a definite rejection arrives
        # for the same token with message 101: the identity differs, the token still
        # may not be rejected.
        rejection = {
            "event": "publication_rejected",
            "producer": 11,
            "epoch": 0,
            "sequence": 0,
            "message_id": 101,
            "digest": digest_hex("other"),
            "code": "SequenceConflict",
        }
        result = self.reconcile(
            [attempt(), committed(), rejection, state(1), state(2), state(3)]
        )
        self.assertEqual(result["verdict"], oracle.FAIL, result)
        self.assertTrue(
            any("different message identity" in reason for reason in result["reasons"]),
            result,
        )

    def test_a_rejected_token_that_survives_in_recovered_state_fails(self):
        # No acknowledgment exists for sequence 1, but the recovered voter state
        # still carries its outcome and message while the token is definitively
        # rejected: the rejection may not survive in the recovered state.
        rejection = {
            "event": "publication_rejected",
            "producer": 11,
            "epoch": 0,
            "sequence": 1,
            "message_id": 101,
            "digest": digest_hex("beta"),
            "code": "SequenceConflict",
        }
        result = self.reconcile(
            [
                attempt(),
                committed(),
                rejection,
                state(1, offset_count=2),
                state(2, offset_count=2),
                state(3, offset_count=2),
            ]
        )
        self.assertEqual(result["verdict"], oracle.FAIL, result)
        self.assertTrue(
            any("definitively rejected" in reason for reason in result["reasons"]),
            result,
        )

    def test_a_conflicting_retry_of_a_committed_token_is_not_a_rejection(self):
        # Scenario 5 journals exactly this: the broker rejects the conflicting
        # identity of an already-committed token. That is an identity conflict, not
        # a definite rejection of the token, and it must not fail the oracle.
        conflict = {
            "event": "publication_identity_conflict",
            "producer": 11,
            "epoch": 0,
            "sequence": 0,
            "message_id": 101,
            "digest": digest_hex("other"),
            "code": "SequenceConflict",
        }
        result = self.reconcile(
            [attempt(), committed(), conflict, state(1), state(2), state(3)]
        )
        self.assertEqual(result["verdict"], oracle.PASS, result)

    def test_an_uncertain_attempt_is_not_a_commit_and_not_a_failure(self):
        unknown = {
            "event": "publication_unknown",
            "producer": 12,
            "epoch": 0,
            "sequence": 0,
            "message_id": 200,
            "digest": digest_hex("uncertain"),
            "reason": "OutcomeUnknown",
        }
        result = self.reconcile([attempt(), unknown, committed(), state(1), state(2),
                                 state(3)])
        self.assertEqual(result["verdict"], oracle.PASS, result)
        self.assertEqual(result["counts"]["unknown"], 1)

    def test_an_uncertain_record_that_claims_a_commit_fails(self):
        unknown = {
            "event": "publication_unknown",
            "producer": 12,
            "epoch": 0,
            "sequence": 0,
            "message_id": 200,
            "digest": digest_hex("uncertain"),
            "outcome": "committed_quorum",
            "offset": 1,
        }
        result = self.reconcile([attempt(), unknown, committed(), state(1), state(2),
                                 state(3)])
        self.assertEqual(result["verdict"], oracle.FAIL, result)

    def test_two_tokens_claiming_one_offset_fail(self):
        second = attempt(producer=12, message_id=200, digest=digest_hex("other"))
        second_ack = committed(producer=12, message_id=200,
                               digest=digest_hex("other"), offset=0)
        result = self.reconcile(
            [attempt(), committed(), second, second_ack, state(1), state(2), state(3)]
        )
        self.assertEqual(result["verdict"], oracle.FAIL, result)

    def test_an_acknowledgment_without_an_attempt_is_invalid(self):
        result = self.reconcile([committed(), state(1), state(2), state(3)])
        self.assertEqual(result["verdict"], oracle.INVALID, result)

    def test_an_improperly_written_journal_is_not_evidence(self):
        broken = [
            {"schema": oracle.SCHEMA, "run": "x", "seq": 0, "event": "conclusion",
             "verdict": PASS},
            {"schema": oracle.SCHEMA, "run": "x", "seq": 2, "event": "conclusion",
             "verdict": PASS},
        ]
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "journal.jsonl"
            path.write_text("\n".join(json.dumps(item) for item in broken) + "\n")
            with self.assertRaises(oracle.JournalError):
                oracle.load(path)

    def test_an_unrun_required_scenario_is_not_run(self):
        result = self.reconcile(
            base() + [{"event": "scenario_not_run", "scenario": 12,
                       "required": True, "reason": "no deterministic hook"}]
        )
        self.assertEqual(result["verdict"], oracle.NOT_RUN, result)

    def test_an_unrun_optional_scenario_still_passes(self):
        result = self.reconcile(
            base() + [{"event": "scenario_not_run", "scenario": 12,
                       "required": False, "reason": "not required"}]
        )
        self.assertEqual(result["verdict"], oracle.PASS, result)

    # --- Post-majority barrier evidence ------------------------------------

    def test_a_consistent_postmajority_barrier_passes(self):
        result = self.reconcile(postmajority_base())
        self.assertEqual(result["verdict"], oracle.PASS, result)
        self.assertEqual(result["counts"]["majority_proofs"], 1, result)

    def test_a_barrier_whose_node_is_never_killed_fails(self):
        records = [
            record
            for record in postmajority_base()
            if record.get("event") != "broker_killed"
        ]
        result = self.reconcile(records)
        self.assertEqual(result["verdict"], oracle.FAIL, result)
        self.assertTrue(
            any("never SIGKILLed" in reason for reason in result["reasons"]), result
        )

    def test_an_acknowledgment_before_the_barrier_kill_fails(self):
        records = postmajority_base()
        index = next(
            position
            for position, record in enumerate(records)
            if record.get("event") == "broker_killed"
        )
        records.insert(index, committed())
        result = self.reconcile(records)
        self.assertEqual(result["verdict"], oracle.FAIL, result)
        self.assertTrue(
            any("before the barrier node" in reason for reason in result["reasons"]),
            result,
        )

    def test_a_barrier_without_an_uncertainty_record_fails(self):
        records = [
            record
            for record in postmajority_base()
            if record.get("event") != "publication_unknown"
        ]
        result = self.reconcile(records)
        self.assertEqual(result["verdict"], oracle.FAIL, result)
        self.assertTrue(
            any(
                "no uncertainty record follows" in reason
                for reason in result["reasons"]
            ),
            result,
        )

    def test_a_barrier_without_a_prekill_majority_at_the_offset_fails(self):
        records = postmajority_base()
        kill = next(
            position
            for position, record in enumerate(records)
            if record.get("event") == "broker_killed"
        )
        records = [
            record
            for position, record in enumerate(records)
            if not (
                position < kill and record.get("event") == "voter_state"
            )
        ]
        result = self.reconcile(records)
        self.assertEqual(result["verdict"], oracle.FAIL, result)
        self.assertTrue(
            any(
                "pre-kill voter state" in reason for reason in result["reasons"]
            ),
            result,
        )

    def test_a_retry_at_another_offset_than_the_barrier_fails(self):
        records = [
            attempt(),
            postmajority_barrier(logical_offset=1, message_id=101),
            state(1, offset_count=2),
            state(2, offset_count=2),
            state(3, offset_count=2),
            killed(),
            uncertain(),
            committed(offset=0),
            state(1, offset_count=2),
            state(2, offset_count=2),
            state(3, offset_count=2),
        ]
        result = self.reconcile(records)
        self.assertEqual(result["verdict"], oracle.FAIL, result)
        self.assertTrue(
            any(
                "never committed at the barrier logical offset 1" in reason
                for reason in result["reasons"]
            ),
            result,
        )

    def test_a_duplicate_token_message_in_a_voter_state_fails(self):
        duplicated = state(1, offset_count=3)
        duplicated["messages"][2] = {**duplicated["messages"][0], "offset": 2}
        result = self.reconcile(
            [attempt(), committed(), duplicated, state(2), state(3)]
        )
        self.assertEqual(result["verdict"], oracle.FAIL, result)
        self.assertTrue(
            any(
                "2 messages for the single producer token" in reason
                for reason in result["reasons"]
            ),
            result,
        )

    def test_an_incomplete_required_scenario_set_never_passes(self):
        records = base()
        for record in records:
            record["run"] = "c04-q2-publish"
        result = oracle.reconcile_q2_publish(concluded(records))
        self.assertEqual(result["verdict"], oracle.FAIL, result)
        self.assertTrue(
            any(
                "never ran required scenario" in reason
                for reason in result["reasons"]
            ),
            result,
        )

    def test_a_complete_required_scenario_set_still_passes(self):
        records = base() + [
            {"event": "scenario_started", "scenario": number}
            for number in range(2, 15)
        ]
        for record in records:
            record["run"] = "c04-q2-publish"
        result = oracle.reconcile_q2_publish(concluded(records))
        self.assertEqual(result["verdict"], oracle.PASS, result)


class Q2JournalRecordingFixtures(unittest.TestCase):
    """The campaign's own journal path must accept a complete voter-state record."""

    def test_voter_state_recording_does_not_duplicate_the_event_keyword(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "journal.jsonl"
            journal = oracle.Journal(path, "recording-fixture")
            try:
                record = append_voter_state(journal, state(1))
                journal.append(
                    "publication_attempt",
                    producer=11,
                    epoch=0,
                    sequence=0,
                    message_id=100,
                    digest=digest_hex("alpha"),
                    incarnation=INCARNATION,
                )
                journal.append(
                    "publication_committed",
                    outcome="committed_quorum",
                    producer=11,
                    epoch=0,
                    sequence=0,
                    message_id=100,
                    digest=digest_hex("alpha"),
                    incarnation=INCARNATION,
                    offset=0,
                    partition={
                        "tenant_id": TENANT,
                        "topic_id": TOPIC_ID,
                        "partition_id": PARTITION,
                    },
                )
                append_voter_state(journal, state(2))
                append_voter_state(journal, state(3))
                journal.conclusion(PASS)
            finally:
                journal.close()
            records = oracle.load(path)
            voter_states = [item for item in records if item["event"] == "voter_state"]
            self.assertEqual(len(voter_states), 3)
            self.assertEqual(voter_states[0]["node"], 1)
            self.assertIn("messages", voter_states[0])
            self.assertEqual(record["event"], "voter_state")
            result = oracle.reconcile_q2_publish(records)
            self.assertEqual(result["verdict"], oracle.PASS, result)


# --- Live campaign --------------------------------------------------------------


def free_port(used: set[int]) -> int:
    for _ in range(1000):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.bind(("127.0.0.1", 0))
            port = sock.getsockname()[1]
        if port not in used:
            used.add(port)
            return port
    raise CheckFailure("no free loopback port available")


class Node:
    """One broker process with its own cluster identity and Q2 data directory."""

    def __init__(self, harness: "Campaign", number: int):
        self.harness = harness
        self.number = number
        self.directory = harness.directory / f"node{number}"
        self.directory.mkdir()
        # The campaign allocates every port once, before any node exists: the metadata
        # and every peer list must name the ports the processes really bind.
        self.client_port = harness.ports[number]["client"]
        self.cluster_port = harness.ports[number]["cluster"]
        self.q2_port = harness.ports[number]["q2"]
        self.metadata = harness.directory / f"metadata{number}.json"
        self.metadata.write_text(
            json.dumps(
                {
                    "cluster_id": CLUSTER,
                    "revision": 1,
                    "issued_at_ms": 1,
                    "revalidate_after_ms": 600_000,
                    "acl_revision": 1,
                    "policy_revision": 1,
                    "capability_revision": 1,
                    "nodes": [
                        {"node": member, "address": f"127.0.0.1:{harness.ports[member]['cluster']}"}
                        for member in (1, 2, 3)
                    ],
                    "topics": [
                        {"topic_id": TOPIC_ID, "durable_class": 2, "policy_revision": 1}
                    ],
                    "partitions": [
                        {
                            "tenant_id": TENANT,
                            "topic_id": TOPIC_ID,
                            "partition_id": PARTITION,
                            "owner": 1,
                            "ownership_epoch": OWNER_EPOCH,
                            "lease_valid_for_ms": 600_000,
                            "desired_replicas": 3,
                            "shard_id": SHARD,
                            "required_policy_revision": 1,
                        }
                    ],
                },
                indent=2,
            ),
            encoding="utf-8",
        )
        self.process: subprocess.Popen | None = None
        self.lines: list[str] = []

    def arguments(self, bootstrap: bool) -> list[str]:
        cluster_peers = [
            value
            for value in (
                ["--cluster-peer", f"{member}=127.0.0.1:{self.harness.ports[member]['cluster']}"]
                for member in (1, 2, 3)
                if member != self.number
            )
            for value in value
        ]
        q2_peers = [
            value
            for member in (1, 2, 3)
            for value in (
                "--q2-peer",
                f"{member}=127.0.0.1:{self.harness.ports[member]['q2']}",
            )
        ]
        return [
            str(SERVER),
            "--listen",
            f"127.0.0.1:{self.client_port}",
            "--cluster-id",
            str(CLUSTER),
            "--node-id",
            str(self.number),
            "--cluster-listen",
            f"127.0.0.1:{self.cluster_port}",
            *cluster_peers,
            "--cluster-metadata",
            str(self.metadata),
            "--q2-data-dir",
            str(self.directory),
            "--q2-topic",
            TOPIC,
            "--q2-tenant-id",
            str(TENANT),
            "--q2-topic-id",
            str(TOPIC_ID),
            "--q2-partition-id",
            str(PARTITION),
            "--q2-shard-id",
            str(SHARD),
            "--q2-incarnation",
            str(INCARNATION),
            "--q2-raft-listen",
            f"127.0.0.1:{self.q2_port}",
            *q2_peers,
            "--q2-bootstrap",
            "true" if bootstrap else "false",
        ]

    def start(self, *, bootstrap: bool) -> None:
        self.lines = []
        self.process = subprocess.Popen(
            self.arguments(bootstrap),
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            text=True,
            env=dict(os.environ, **self.harness.env),
        )
        threading.Thread(target=self._read, daemon=True).start()
        self.harness.journal.append(
            "broker_started",
            node=self.number,
            client_port=self.client_port,
            cluster_port=self.cluster_port,
            q2_port=self.q2_port,
            bootstrap=bootstrap,
        )

    def _read(self) -> None:
        assert self.process is not None and self.process.stderr is not None
        for line in self.process.stderr:
            self.lines.append(line.rstrip())

    def wait_ready(self, timeout: float = T_READY) -> None:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if any("listening on" in line for line in self.lines):
                return
            if self.process is not None and self.process.poll() is not None:
                raise CheckFailure(
                    f"node {self.number} exited before readiness: "
                    + " | ".join(self.lines[-5:])
                )
            time.sleep(T_POLL)
        raise CheckFailure(
            f"node {self.number} did not become ready: "
            + " | ".join(self.lines[-5:])
        )

    def wait_authority(self, timeout: float = T_READY) -> None:
        marker = f"Q2 authority: epoch={OWNER_EPOCH} owner=1"
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if any(marker in line for line in self.lines):
                return
            if self.process is not None and self.process.poll() is not None:
                raise CheckFailure(
                    f"node {self.number} exited before installing authority: "
                    + " | ".join(self.lines[-5:])
                )
            time.sleep(T_POLL)
        raise CheckFailure(f"node {self.number} did not install the Q2 authority")

    def stop(self, *, sig: int = signal.SIGINT, timeout: float = 20.0) -> int | None:
        if self.process is None or self.process.poll() is not None:
            return None if self.process is None else self.process.returncode
        self.process.send_signal(sig)
        try:
            self.process.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            self.process.kill()
            self.process.wait(timeout=10)
        self.harness.journal.append(
            "broker_stopped", node=self.number, exit_code=self.process.returncode
        )
        return self.process.returncode

    def kill(self) -> None:
        if self.process is not None and self.process.poll() is None:
            self.process.kill()
            self.process.wait(timeout=10)
            self.harness.journal.append(
                "broker_killed", node=self.number, exit_code=self.process.returncode
            )

    def read_state(self) -> dict | None:
        path = self.directory / "state" / "state.json"
        if not path.exists():
            return None
        document = json.loads(path.read_text(encoding="utf-8"))
        state = document["state"]
        partition = state.get("partitions", {}).get(PARTITION_KEY)
        last_applied = state.get("last_applied") or {}
        if partition is None:
            return {
                "event": "voter_state",
                "node": self.number,
                "applied_index": (last_applied.get("index")),
                "partition": {
                    "tenant_id": TENANT,
                    "topic_id": TOPIC_ID,
                    "partition_id": PARTITION,
                },
                "count": 0,
                "chain": "00" * 32,
                "next_logical_offset": 0,
                "messages": [],
                "outcomes": [],
            }
        outcomes = []
        for producer_key, producer in (partition.get("producers") or {}).items():
            principal, producer_id = producer_key.split(":", 1)
            for sequence, outcome in (producer.get("outcomes") or {}).items():
                outcomes.append(
                    {
                        "producer": int(producer_id),
                        "producer_epoch": producer.get("epoch"),
                        "sequence": int(sequence),
                        "offset": outcome.get("logical_offset"),
                        "message_id": outcome.get("message_id"),
                        "digest": bytes(outcome.get("payload_digest") or []).hex(),
                    }
                )
        return {
            "event": "voter_state",
            "node": self.number,
            "applied_index": last_applied.get("index"),
            "partition": {
                "tenant_id": TENANT,
                "topic_id": TOPIC_ID,
                "partition_id": PARTITION,
            },
            "count": partition.get("applied_count"),
            "chain": bytes(partition.get("chain_digest") or []).hex(),
            "next_logical_offset": partition.get("next_logical_offset"),
            "messages": [
                {
                    "offset": message.get("logical_offset"),
                    "message_id": message.get("message_id"),
                    "digest": bytes(message.get("payload_digest") or []).hex(),
                    "payload": (
                        bytes(message["payload"]).hex()
                        if isinstance(message.get("payload"), list)
                        else None
                    ),
                    "producer": message.get("producer_id"),
                    "producer_epoch": message.get("producer_epoch"),
                    "sequence": message.get("sequence"),
                }
                for message in (partition.get("messages") or [])
            ],
            "outcomes": outcomes,
        }

    def last_applied(self) -> dict | None:
        """The `last_applied` log id this voter persisted, read from its own storage.

        `leader_id.node_id` names the node that proposed that entry, so the entry every
        voter applied last identifies the leader that produced it. `state.json` is
        replaced atomically, so a running voter is never read half-written.
        """
        path = self.directory / "state" / "state.json"
        if not path.exists():
            return None
        try:
            document = json.loads(path.read_text(encoding="utf-8"))
            applied = document["state"].get("last_applied") or {}
            leader_id = applied.get("leader_id") or {}
            index = applied["index"]
            node_id = leader_id["node_id"]
        except (OSError, KeyError, TypeError, json.JSONDecodeError):
            return None
        if not isinstance(index, int) or not isinstance(node_id, int):
            return None
        return {"index": index, "term": leader_id.get("term"), "node_id": node_id}


class Campaign:
    def __init__(self, journal: oracle.Journal, directory: Path):
        self.journal = journal
        self.directory = directory
        # Extra environment for every broker this campaign starts. Empty by default:
        # a production broker is started without it, so the pre-majority seam stays
        # disabled until a scenario explicitly enables it.
        self.env: dict[str, str] = {}
        self._ports: set[int] = set()
        self.ports: dict[int, dict] = {}
        for number in (1, 2, 3):
            self.ports[number] = {
                "client": self.port(),
                "cluster": self.port(),
                "q2": self.port(),
            }
        self.nodes = {number: Node(self, number) for number in (1, 2, 3)}

    def port(self) -> int:
        return free_port(self._ports)

    def check(self, ok: bool, detail: str, **fields) -> None:
        self.journal.append("evidence", check=detail, ok=bool(ok), **fields)
        if not ok:
            raise CheckFailure(detail)

    def has_journal_event(self, event: str, **fields) -> bool:
        """Whether the journal on disk already holds such a record.

        Used to assert, at a precise point of the campaign, that an observation the
        oracle requires (or must not see yet) really is absent before the next fault
        is injected.
        """
        try:
            lines = self.journal.path.read_text(encoding="utf-8").splitlines()
        except OSError:
            return False
        for line in lines:
            if not line.strip():
                continue
            record = json.loads(line)
            if record.get("event") == event and all(
                record.get(key) == value for key, value in fields.items()
            ):
                return True
        return False

    def live(self) -> list[int]:
        return [
            number
            for number, node in self.nodes.items()
            if node.process is not None and node.process.poll() is None
        ]

    def start(self, numbers, *, bootstrap_only_node: int | None = None) -> None:
        for number in numbers:
            self.nodes[number].start(bootstrap=number == bootstrap_only_node)
            time.sleep(0.3)
        for number in numbers:
            self.nodes[number].wait_ready()
        self.nodes[1].wait_authority()

    def _arguments(
        self,
        producer: int,
        epoch: int,
        sequence: int,
        message_id: int,
        payload: str,
        incarnation: int,
        owner_epoch: int,
    ) -> list[str]:
        return [
            str(EXAMPLE),
            TOPIC,
            str(TENANT),
            str(TOPIC_ID),
            str(PARTITION),
            str(incarnation),
            str(owner_epoch),
            str(producer),
            str(epoch),
            str(sequence),
            str(message_id),
            payload,
            "1",
        ]

    def publish_start(
        self,
        producer: int,
        epoch: int,
        sequence: int,
        message_id: int,
        payload: str,
        *,
        incarnation: int = INCARNATION,
        owner_epoch: int = OWNER_EPOCH,
    ) -> subprocess.Popen:
        """Journal one publication attempt and start the real SDK example.

        The process is returned still running: a caller that must observe an external
        barrier before the publication can settle (scenario 12) collects it with
        :meth:`publish_finish`. Every other caller uses :meth:`publish`.
        """
        self.journal.append(
            "publication_attempt",
            producer=producer,
            epoch=epoch,
            sequence=sequence,
            message_id=message_id,
            digest=digest_hex(payload),
            payload=payload,
            incarnation=incarnation,
            owner_epoch=owner_epoch,
            partition={
                "tenant_id": TENANT,
                "topic_id": TOPIC_ID,
                "partition_id": PARTITION,
            },
        )
        return subprocess.Popen(
            self._arguments(
                producer, epoch, sequence, message_id, payload, incarnation, owner_epoch
            ),
            env=dict(os.environ, ROVQET_ADDR=f"127.0.0.1:{self.nodes[1].client_port}"),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )

    def publish_finish(
        self,
        process: subprocess.Popen,
        producer: int,
        epoch: int,
        sequence: int,
        message_id: int,
        payload: str,
        *,
        incarnation: int = INCARNATION,
        owner_epoch: int = OWNER_EPOCH,
        timeout: float = T_PUBLISH,
        rejection_event: str = "publication_rejected",
    ) -> dict:
        """Collect a started publication and journal the outcome it really observed."""
        try:
            stdout, stderr = process.communicate(timeout=timeout)
        except subprocess.TimeoutExpired:
            process.kill()
            process.communicate()
            raise CheckFailure(
                f"the SDK example for {producer}:{epoch}:{sequence} never finished "
                f"within {timeout}s"
            ) from None
        if process.returncode != 0:
            raise CheckFailure(
                f"the SDK example exited {process.returncode}: {stderr.strip()}"
            )
        observed = self._observed(stdout)
        self._journal_outcome(
            observed,
            producer,
            epoch,
            sequence,
            message_id,
            payload,
            incarnation=incarnation,
            rejection_event=rejection_event,
        )
        return observed

    def collect_attempt(
        self,
        process: subprocess.Popen,
        producer: int,
        epoch: int,
        sequence: int,
        message_id: int,
        payload: str,
        *,
        incarnation: int = INCARNATION,
        timeout: float = T_PUBLISH,
        exit_event: str | None = None,
    ) -> dict | None:
        """Collect an attempt whose broker may be killed while it is in flight.

        Unlike :meth:`publish_finish`, a non-zero exit is not itself a failure here:
        the SDK loses its socket when the harness kills the leader. The externally
        observed outcome is journaled only when the SDK actually printed one;
        otherwise the exit is recorded as its own evidence and no outcome is invented.

        `exit_event` names the record written for an attempt that printed nothing at
        all. When it is given the attempt is additionally collected as an uncertain
        `publication_unknown` outcome, so a killed call is an observed uncertainty in
        the journal instead of a silent gap: no acknowledgment is ever invented for
        it.
        """
        try:
            stdout, stderr = process.communicate(timeout=timeout)
        except subprocess.TimeoutExpired:
            process.kill()
            stdout, stderr = process.communicate()
        lines = [line for line in stdout.splitlines() if line.strip()]
        if not lines:
            self.journal.append(
                exit_event or "premajority_attempt_exit",
                producer=producer,
                producer_epoch=epoch,
                sequence=sequence,
                message_id=message_id,
                exit_code=process.returncode,
                stderr=stderr.strip()[-800:],
            )
            if exit_event is not None:
                self.journal.append(
                    "publication_unknown",
                    producer=producer,
                    epoch=epoch,
                    sequence=sequence,
                    message_id=message_id,
                    digest=digest_hex(payload),
                    incarnation=incarnation,
                    reason=(
                        f"{exit_event}: the client call ended without any successful "
                        "response"
                    ),
                )
            return None
        observed = self._observed(stdout)
        self._journal_outcome(
            observed,
            producer,
            epoch,
            sequence,
            message_id,
            payload,
            incarnation=incarnation,
        )
        return observed

    def await_premajority_barrier(
        self, path: Path, process: subprocess.Popen, *, timeout: float = T_APPLIED
    ) -> dict:
        """Wait until the armed broker wrote its pre-majority barrier document."""
        deadline = time.monotonic() + timeout
        last_error = "the barrier document never appeared"
        while time.monotonic() < deadline:
            if path.exists():
                try:
                    return json.loads(path.read_text(encoding="utf-8"))
                except (OSError, json.JSONDecodeError) as error:
                    last_error = f"unreadable barrier: {error}"
            if process.poll() is not None:
                raise CheckFailure(
                    "the SDK attempt finished before the armed broker emitted the "
                    f"pre-majority barrier (exit {process.returncode})"
                )
            time.sleep(T_POLL)
        raise CheckFailure(
            f"the armed broker did not emit the pre-majority barrier: {last_error}"
        )

    def await_postmajority_barrier(
        self, path: Path, process: subprocess.Popen, *, timeout: float = T_APPLIED
    ) -> dict:
        """Wait until the armed broker wrote its post-majority barrier document.

        A barrier that never appears is a failed observation, never a pass: the
        deadline is bounded and the caller reports the reason it missed.
        """
        deadline = time.monotonic() + timeout
        last_error = "the barrier document never appeared"
        while time.monotonic() < deadline:
            if path.exists():
                try:
                    return json.loads(path.read_text(encoding="utf-8"))
                except (OSError, json.JSONDecodeError) as error:
                    last_error = f"unreadable barrier: {error}"
            if process.poll() is not None:
                raise CheckFailure(
                    "the SDK attempt finished before the armed broker emitted the "
                    f"post-majority barrier (exit {process.returncode}): {last_error}"
                )
            time.sleep(T_POLL)
        raise CheckFailure(
            f"the armed broker did not emit the post-majority barrier: {last_error}"
        )

    @staticmethod
    def _observed(stdout: str) -> dict:
        lines = [line for line in stdout.splitlines() if line.strip()]
        if not lines:
            raise CheckFailure("the SDK example printed no outcome")
        try:
            return json.loads(lines[-1])
        except json.JSONDecodeError as error:
            raise CheckFailure(f"unparseable SDK output {lines[-1]!r}: {error}") from error

    def _journal_outcome(
        self,
        observed: dict,
        producer: int,
        epoch: int,
        sequence: int,
        message_id: int,
        payload: str,
        *,
        incarnation: int = INCARNATION,
        rejection_event: str = "publication_rejected",
    ) -> None:
        outcome = observed.get("outcome")
        if outcome == "committed_quorum":
            self.journal.append(
                "publication_committed",
                outcome="committed_quorum",
                producer=producer,
                epoch=epoch,
                sequence=sequence,
                message_id=message_id,
                digest=digest_hex(payload),
                incarnation=incarnation,
                offset=observed.get("offset"),
                partition={
                    "tenant_id": TENANT,
                    "topic_id": TOPIC_ID,
                    "partition_id": PARTITION,
                },
            )
        elif outcome == "rejected":
            # An identity conflict of an already-committed token is not a definite
            # rejection of that token (the token keeps its original commit), so the
            # caller may journal it under a distinct event; everything else is a
            # definite rejection of the producer token.
            self.journal.append(
                rejection_event,
                producer=producer,
                epoch=epoch,
                sequence=sequence,
                message_id=message_id,
                digest=digest_hex(payload),
                code=observed.get("detail"),
            )
        elif outcome == "unknown":
            self.journal.append(
                "publication_unknown",
                producer=producer,
                epoch=epoch,
                sequence=sequence,
                message_id=message_id,
                digest=digest_hex(payload),
                reason=observed.get("detail", "OutcomeUnknown"),
            )
        else:
            self.journal.append(
                "publication_unexpected",
                producer=producer,
                epoch=epoch,
                sequence=sequence,
                message_id=message_id,
                digest=digest_hex(payload),
                detail=observed,
            )
            raise CheckFailure(f"unexpected SDK outcome {observed!r}")

    def publish(
        self,
        producer: int,
        epoch: int,
        sequence: int,
        message_id: int,
        payload: str,
        *,
        incarnation: int = INCARNATION,
        owner_epoch: int = OWNER_EPOCH,
        timeout: float = T_PUBLISH,
        rejection_event: str = "publication_rejected",
    ) -> dict:
        process = self.publish_start(
            producer,
            epoch,
            sequence,
            message_id,
            payload,
            incarnation=incarnation,
            owner_epoch=owner_epoch,
        )
        return self.publish_finish(
            process,
            producer,
            epoch,
            sequence,
            message_id,
            payload,
            incarnation=incarnation,
            owner_epoch=owner_epoch,
            timeout=timeout,
            rejection_event=rejection_event,
        )

    def publish_until_certain(
        self,
        producer: int,
        epoch: int,
        sequence: int,
        message_id: int,
        payload: str,
        *,
        timeout: float = 60.0,
        **kwargs,
    ) -> dict:
        """Retry the identical token while the leader election is still settling.

        An uncertain result is not a commit; the idempotent token makes the retry
        safe, and every attempt stays in the journal so the oracle sees the uncertainty.
        """
        deadline = time.monotonic() + timeout
        while True:
            observed = self.publish(
                producer, epoch, sequence, message_id, payload, **kwargs
            )
            if observed.get("outcome") in ("committed_quorum", "rejected"):
                return observed
            if time.monotonic() >= deadline:
                raise CheckFailure(
                    f"publication {producer}:{epoch}:{sequence} never became certain: "
                    f"{observed!r}"
                )
            time.sleep(0.3)

    def observe_leader(self, *, timeout: float = T_READY) -> int:
        """Poll the three persisted `last_applied` documents until they name one leader.

        openraft serializes `last_applied.leader_id.node_id`, so the entry every voter
        applied last identifies the node that proposed it. All three voters must agree
        on the same node before the campaign stops one of the others; a group that
        never agrees fails instead of guessing.
        """
        deadline = time.monotonic() + timeout
        last: dict[int, object] = {}
        while time.monotonic() < deadline:
            last = {
                number: node.last_applied() for number, node in self.nodes.items()
            }
            applied = {
                number: value["node_id"]
                for number, value in last.items()
                if isinstance(value, dict)
            }
            if len(applied) == len(self.nodes) and len(set(applied.values())) == 1:
                leader = next(iter(applied.values()))
                self.journal.append(
                    "leader_observed",
                    leader=leader,
                    applied={str(key): value for key, value in applied.items()},
                )
                return leader
            time.sleep(T_POLL)
        raise CheckFailure(
            "the three voters never agreed on one leader: "
            + json.dumps(last, default=str)[:800]
        )

    def record_states(self, numbers=None) -> dict[int, dict | None]:
        numbers = self.live() if numbers is None else numbers
        states = {}
        for number in numbers:
            state = self.nodes[number].read_state()
            states[number] = state
            if state is not None:
                append_voter_state(self.journal, state)
        return states

    @staticmethod
    def message(state: dict | None, offset: int) -> dict | None:
        if state is None:
            return None
        for message in state["messages"]:
            if message.get("offset") == offset:
                return message
        return None

    def await_message(
        self, offset: int, message_id: int, *, timeout: float = T_APPLIED
    ) -> dict[int, dict]:
        deadline = time.monotonic() + timeout
        last = {}
        while time.monotonic() < deadline:
            last = self.record_states()
            if all(
                (message := self.message(state, offset)) is not None
                and message.get("message_id") == message_id
                for state in last.values()
                if state is not None
            ) and len([state for state in last.values() if state is not None]) == len(
                self.live()
            ):
                return {number: state for number, state in last.items() if state}
            time.sleep(T_POLL)
        raise CheckFailure(
            f"offset {offset} with message {message_id} never reached every live "
            f"voter: {json.dumps(last)[:1500]}"
        )

    def expect_committed(self, observed: dict, offset: int) -> None:
        self.check(
            observed.get("outcome") == "committed_quorum",
            f"the SDK observed COMMITTED_QUORUM for logical offset {offset}",
            observed=observed,
        )
        self.check(
            observed.get("offset") == offset,
            f"the acknowledged logical offset is {offset}",
            observed=observed,
        )
        self.check(
            observed.get("incarnation") == INCARNATION,
            "the acknowledgment carries the configured log incarnation",
            observed=observed,
        )

    def expect_rejected(self, observed: dict, code: str) -> None:
        self.check(
            observed.get("outcome") == "rejected",
            f"the broker definitively rejected the publication with {code}",
            observed=observed,
        )
        self.check(
            observed.get("detail") == code,
            f"the rejection names {code}",
            observed=observed,
        )

    def expect_unchanged(self, states: dict, count: int, next_offset: int) -> None:
        for number, state in states.items():
            if state is None:
                continue
            self.check(
                state["next_logical_offset"] == next_offset,
                f"node {number} kept its next logical offset at {next_offset}",
                node=number,
                state=state,
            )
            self.check(
                len(state["messages"]) == count,
                f"node {number} kept {count} retained message(s)",
                node=number,
                state=state,
            )


def run_campaign(campaign: Campaign) -> None:
    campaign.journal.append(
        "scenario_started", scenario=1, name="three-broker-q2-group-startup"
    )
    campaign.start([1, 2, 3], bootstrap_only_node=1)
    # Let the first election finish before the first publication; an early attempt is
    # still safe but would only add an uncertain observation.
    time.sleep(1.5)
    campaign.check(
        len(campaign.live()) == 3,
        "all three brokers are running with an embedded Q2 voter",
        live=campaign.live(),
    )
    campaign.journal.append("scenario_conclusion", scenario=1, verdict=PASS)

    campaign.journal.append(
        "scenario_started", scenario=2, name="committed-quorum-publish"
    )
    observed = campaign.publish_until_certain(11, 0, 0, 100, "alpha")
    campaign.expect_committed(observed, 0)
    states = campaign.await_message(0, 100)
    campaign.check(
        all(
            campaign.message(state, 0)["digest"] == digest_hex("alpha")
            and campaign.message(state, 0)["payload"] == payload_hex("alpha")
            for state in states.values()
        ),
        "every voter retained the committed payload bytes and digest at offset 0",
        states={str(k): v for k, v in states.items()},
    )
    campaign.check(
        all(state["next_logical_offset"] == 1 for state in states.values()),
        "every voter allocated exactly offset 0",
        states={str(k): v for k, v in states.items()},
    )
    campaign.journal.append("scenario_conclusion", scenario=2, verdict=PASS)

    campaign.journal.append(
        "scenario_started", scenario=3, name="idempotent-retry-same-offset"
    )
    observed = campaign.publish_until_certain(11, 0, 0, 100, "alpha")
    campaign.expect_committed(observed, 0)
    states = campaign.await_message(0, 100)
    campaign.check(
        all(
            state is not None
            and state["next_logical_offset"] == 1
            and len(state["messages"]) == 1
            for state in states.values()
        ),
        "the idempotent retry consumed no second offset and created no second message",
        states={str(k): v for k, v in states.items()},
    )
    campaign.journal.append("scenario_conclusion", scenario=3, verdict=PASS)

    campaign.journal.append(
        "scenario_started", scenario=4, name="sequence-and-offset-progress"
    )
    observed = campaign.publish_until_certain(11, 0, 1, 101, "beta")
    campaign.expect_committed(observed, 1)
    states = campaign.await_message(1, 101)
    campaign.check(
        all(
            state["next_logical_offset"] == 2
            and sorted(message["offset"] for message in state["messages"]) == [0, 1]
            for state in states.values()
        ),
        "sequence 1 was committed at offset 1 and the next offset advanced to 2",
        states={str(k): v for k, v in states.items()},
    )
    campaign.journal.append("scenario_conclusion", scenario=4, verdict=PASS)

    campaign.journal.append(
        "scenario_started", scenario=5, name="sequence-conflict-rejection"
    )
    observed = campaign.publish_until_certain(14, 0, 0, 202, "delta")
    campaign.expect_committed(observed, 2)
    campaign.await_message(2, 202)
    # The token (14, 0, 0) is already committed with message 202, so the broker's
    # SequenceConflict on the retry with message 203 rejects the conflicting identity,
    # not the producer token: journaling it as a definite token rejection would
    # contradict the committed outcome the same journal records.
    observed = campaign.publish(
        14, 0, 0, 203, "other", rejection_event="publication_identity_conflict"
    )
    campaign.expect_rejected(observed, "SequenceConflict")
    states = campaign.record_states()
    campaign.check(
        all(
            state is None
            or (
                state["next_logical_offset"] == 3
                and all(
                    message.get("message_id") != 203 for message in state["messages"]
                )
            )
            for state in states.values()
        ),
        "a conflicting retry created no message and consumed no offset",
        states={str(k): v for k, v in states.items()},
    )
    campaign.journal.append("scenario_conclusion", scenario=5, verdict=PASS)

    campaign.journal.append(
        "scenario_started", scenario=6, name="stale-ownership-epoch-rejection"
    )
    observed = campaign.publish(12, 0, 0, 200, "stale", owner_epoch=OWNER_EPOCH - 1)
    campaign.expect_rejected(observed, "StaleEpoch")
    states = campaign.record_states()
    campaign.expect_unchanged(states, 3, 3)
    campaign.journal.append("scenario_conclusion", scenario=6, verdict=PASS)

    campaign.journal.append(
        "scenario_started", scenario=7, name="wrong-incarnation-rejection"
    )
    observed = campaign.publish(13, 0, 0, 201, "inc", incarnation=INCARNATION - 1)
    campaign.expect_rejected(observed, "ReplayNotAvailable")
    states = campaign.record_states()
    campaign.expect_unchanged(states, 3, 3)
    campaign.journal.append("scenario_conclusion", scenario=7, verdict=PASS)

    campaign.journal.append(
        "scenario_started", scenario=8, name="lost-quorum-no-false-commit"
    )
    for number in (2, 3):
        campaign.nodes[number].kill()
    campaign.check(
        campaign.live() == [1],
        "two of three voters are unavailable",
        live=campaign.live(),
    )
    observed = campaign.publish(11, 0, 2, 102, "gamma", timeout=75)
    campaign.journal.append(
        "quorum_observed", available=[1], majority=int(len(campaign.live()) >= 2)
    )
    campaign.check(
        observed.get("outcome") == "unknown",
        "a publication without a quorum is uncertain, never a commit",
        observed=observed,
    )
    states = campaign.record_states([1])
    campaign.check(
        all(
            state is None
            or (
                campaign.message(state, 3) is None
                and state["next_logical_offset"] == 3
            )
            for state in states.values()
        ),
        "the lone voter applied no message for the uncertain publication",
        states={str(k): v for k, v in states.items()},
    )
    campaign.journal.append("scenario_conclusion", scenario=8, verdict=PASS)

    campaign.journal.append(
        "scenario_started", scenario=9, name="quorum-restored-retry-settles"
    )
    campaign.start([2, 3])
    deadline = time.monotonic() + 45
    observed = campaign.publish(11, 0, 2, 102, "gamma", timeout=75)
    while observed.get("outcome") == "unknown" and time.monotonic() < deadline:
        time.sleep(0.5)
        observed = campaign.publish(11, 0, 2, 102, "gamma", timeout=75)
    campaign.expect_committed(observed, 3)
    states = campaign.await_message(3, 102, timeout=45)
    campaign.check(
        all(
            state["next_logical_offset"] == 4
            and len([m for m in state["messages"] if m.get("message_id") == 102]) == 1
            for state in states.values()
        ),
        "the settled retry is present exactly once at offset 3 on every voter",
        states={str(k): v for k, v in states.items()},
    )
    campaign.journal.append("scenario_conclusion", scenario=9, verdict=PASS)

    campaign.journal.append(
        "scenario_started", scenario=10, name="owner-crash-after-quorum"
    )
    before = campaign.nodes[1].read_state()
    campaign.check(before is not None, "the owner state exists before the crash")
    campaign.nodes[1].kill()
    campaign.check(
        1 not in campaign.live(),
        "the owner broker was killed after a quorum-committed prefix",
        live=campaign.live(),
    )
    campaign.start([1])
    after = campaign.nodes[1].read_state()
    campaign.check(
        after is not None
        and before is not None
        and after["count"] >= before["count"]
        and after["chain"] == before["chain"]
        and all(
            campaign.message(after, offset) is not None
            and campaign.message(before, offset) is not None
            and campaign.message(after, offset)["payload"] is not None
            and campaign.message(after, offset)["payload"]
            == campaign.message(before, offset)["payload"]
            for offset in (0, 1, 2, 3)
        ),
        "the restarted owner retained the quorum-committed chain and payload bytes",
        before=before,
        after=after,
    )
    observed = campaign.publish_until_certain(11, 0, 3, 103, "omega")
    campaign.expect_committed(observed, 4)
    states = campaign.await_message(4, 103, timeout=45)
    campaign.check(
        all(state["next_logical_offset"] == 5 for state in states.values()),
        "the publication after the owner restart used the next offset",
        states={str(k): v for k, v in states.items()},
    )
    campaign.journal.append("scenario_conclusion", scenario=10, verdict=PASS)

    campaign.journal.append(
        "scenario_started", scenario=11, name="graceful-stop-keeps-committed-state"
    )
    for number in (1, 2, 3):
        campaign.nodes[number].stop()
    campaign.check(
        campaign.live() == [],
        "all brokers stopped gracefully",
        live=campaign.live(),
    )
    for number in (1, 2, 3):
        state = campaign.nodes[number].read_state()
        campaign.check(
            state is not None
            and state["next_logical_offset"] == 5
            and all(
                campaign.message(state, offset) is not None for offset in (0, 1, 2, 3, 4)
            ),
            f"node {number} kept its quorum-committed state on disk after shutdown",
            node=number,
            state=state,
        )
    campaign.journal.append("scenario_conclusion", scenario=11, verdict=PASS)

    campaign.journal.append(
        "scenario_started", scenario=12, name="leader-crash-before-majority"
    )
    # A fresh producer token that no earlier scenario attempted: the seam may only
    # hold this exact identity.
    producer, sequence, message_id, payload = 21, 0, 300, "premajority"
    barrier_directory = campaign.directory / "premajority"
    barrier_directory.mkdir(parents=True, exist_ok=True)
    arm_path = barrier_directory / "arm.json"
    barrier_path = barrier_directory / "barrier.json"
    barrier_path.unlink(missing_ok=True)
    arm_path.write_text(
        json.dumps(
            {
                "producer_id": producer,
                "producer_epoch": 0,
                "sequence": sequence,
                "message_id": message_id,
            }
        ),
        encoding="utf-8",
    )
    # Only brokers started from here on inherit the seam directory. Brokers started
    # earlier in this campaign ran without it, exactly like production.
    campaign.env["ROVQET_Q2_PREMAJORITY_BARRIER"] = str(barrier_directory)
    campaign.start([1, 2, 3])
    time.sleep(1.5)

    attempt = campaign.publish_start(producer, 0, sequence, message_id, payload)
    barrier = campaign.await_premajority_barrier(barrier_path, attempt)
    campaign.journal.append(
        "premajority_barrier",
        **{key: value for key, value in barrier.items() if key != "event"},
    )
    campaign.check(
        barrier.get("event") == "premajority_barrier"
        and barrier.get("producer_id") == producer
        and barrier.get("producer_epoch") == 0
        and barrier.get("sequence") == sequence
        and barrier.get("message_id") == message_id,
        "the armed broker held this exact publication at the pre-majority barrier",
        barrier=barrier,
    )
    campaign.check(
        isinstance(barrier.get("log_index"), int)
        and isinstance(barrier.get("durable_index"), int)
        and barrier["durable_index"] >= barrier["log_index"],
        "the barrier followed a completed local durable append of that entry",
        barrier=barrier,
    )
    leader = barrier.get("node")
    campaign.check(
        leader in campaign.live(),
        "the barrier names a live voter that was replicating the entry",
        barrier=barrier,
    )
    staged = campaign.record_states()
    campaign.check(
        all(
            state is None
            or (
                state["next_logical_offset"] == 5
                and campaign.message(state, 5) is None
            )
            for state in staged.values()
        ),
        "no voter had committed the held entry before the leader was killed",
        states={str(k): v for k, v in staged.items()},
    )
    campaign.nodes[leader].kill()
    campaign.check(
        leader not in campaign.live(),
        f"the leader named by the barrier (node {leader}) was SIGKILLed before quorum",
        node=leader,
    )
    interrupted = campaign.collect_attempt(
        attempt, producer, 0, sequence, message_id, payload
    )
    campaign.check(
        interrupted is None or interrupted.get("outcome") != "committed_quorum",
        "the interrupted attempt never observed COMMITTED_QUORUM",
        observed=interrupted,
    )
    # The crash point has been witnessed. Disarm before recovery so the restarted
    # voter behaves exactly like a production broker and the retry can settle.
    arm_path.unlink(missing_ok=True)
    campaign.env.pop("ROVQET_Q2_PREMAJORITY_BARRIER", None)
    campaign.start([leader])
    time.sleep(1.5)
    observed = campaign.publish_until_certain(
        producer, 0, sequence, message_id, payload, timeout=75
    )
    campaign.expect_committed(observed, 5)
    states = campaign.await_message(5, message_id, timeout=60)
    campaign.check(
        all(
            state["next_logical_offset"] == 6
            and len(
                [
                    message
                    for message in state["messages"]
                    if message.get("message_id") == message_id
                ]
            )
            == 1
            for state in states.values()
        ),
        "the recovered retry is committed exactly once at offset 5 on every voter",
        states={str(k): v for k, v in states.items()},
    )
    campaign.journal.append("scenario_conclusion", scenario=12, verdict=PASS)

    campaign.journal.append(
        "scenario_started",
        scenario=13,
        name="one-unavailable-follower-quorum",
    )
    # The leader is read from the entry every voter persisted, so exactly one
    # follower can be stopped while the leader and the client entry point stay up.
    leader = campaign.observe_leader()
    campaign.check(
        leader in campaign.nodes and leader in campaign.live(),
        "one leader is running among the three voters",
        leader=leader,
        live=campaign.live(),
    )
    stopped = next(number for number in (2, 3) if number != leader)
    campaign.nodes[stopped].stop()
    campaign.check(
        stopped not in campaign.live() and len(campaign.live()) == 2,
        f"exactly one follower (node {stopped}) is unavailable while the leader runs",
        stopped=stopped,
        leader=leader,
        live=campaign.live(),
    )
    campaign.journal.append(
        "follower_unavailable",
        stopped=stopped,
        leader=leader,
        live=campaign.live(),
    )
    producer, sequence, message_id, payload = 22, 0, 301, "one-down"
    offset = 6
    observed = campaign.publish_until_certain(
        producer, 0, sequence, message_id, payload, timeout=75
    )
    campaign.expect_committed(observed, offset)
    campaign.journal.append(
        "ack_with_follower_down",
        stopped=stopped,
        leader=leader,
        producer=producer,
        epoch=0,
        sequence=sequence,
        message_id=message_id,
        offset=offset,
        live=campaign.live(),
    )
    states = campaign.await_message(offset, message_id, timeout=45)
    campaign.check(
        all(
            state["next_logical_offset"] == offset + 1
            and campaign.message(state, offset)["payload"] == payload_hex(payload)
            and len(
                [
                    message
                    for message in state["messages"]
                    if message.get("message_id") == message_id
                ]
            )
            == 1
            for state in states.values()
        ),
        "the two available voters committed the publication exactly once at offset 6",
        states={str(key): value for key, value in states.items()},
    )
    campaign.start([stopped])
    states = campaign.await_message(offset, message_id, timeout=60)
    campaign.check(
        all(
            state["next_logical_offset"] == offset + 1
            and campaign.message(state, offset)["payload"] == payload_hex(payload)
            and campaign.message(state, offset)["digest"] == digest_hex(payload)
            for state in states.values()
        ),
        f"the restarted follower node {stopped} caught up to the same committed state",
        states={str(key): value for key, value in states.items()},
    )
    campaign.journal.append(
        "follower_recovered",
        node=stopped,
        leader=leader,
        offset=offset,
        message_id=message_id,
        next_logical_offset=states[stopped]["next_logical_offset"],
    )
    campaign.journal.append("scenario_conclusion", scenario=13, verdict=PASS)

    campaign.journal.append(
        "scenario_started",
        scenario=14,
        name="leader-crash-after-majority-before-response",
    )
    # A fresh producer token no earlier scenario attempted: the seam may only hold
    # this exact identity.
    producer, sequence, message_id, payload = 23, 0, 302, "postmajority"
    barrier_directory = campaign.directory / "postmajority"
    barrier_directory.mkdir(parents=True, exist_ok=True)
    arm_path = barrier_directory / "arm.json"
    barrier_path = barrier_directory / "barrier.json"
    barrier_path.unlink(missing_ok=True)
    arm_path.write_text(
        json.dumps(
            {
                "producer_id": producer,
                "producer_epoch": 0,
                "sequence": sequence,
                "message_id": message_id,
            }
        ),
        encoding="utf-8",
    )
    # Every broker is restarted so each process inherits the barrier directory: the
    # seam can only engage on whichever node ends up committing the entry.
    for number in (1, 2, 3):
        campaign.nodes[number].stop()
    campaign.check(
        campaign.live() == [],
        "all three brokers stopped before the post-majority seam was armed",
        live=campaign.live(),
    )
    campaign.env["ROVQET_Q2_POSTMAJORITY_BARRIER"] = str(barrier_directory)
    campaign.start([1, 2, 3])
    # A warm-up publication proves the restarted group elected a leader before the one
    # armed attempt; the armed token itself is attempted exactly once.
    warmup = campaign.publish_until_certain(24, 0, 0, 303, "warmup", timeout=75)
    campaign.check(
        warmup.get("outcome") == "committed_quorum"
        and isinstance(warmup.get("offset"), int),
        "the restarted group elected a leader that committed a warm-up publication",
        observed=warmup,
    )
    campaign.await_message(warmup["offset"], 303, timeout=45)
    leader = campaign.observe_leader()
    campaign.check(
        leader in campaign.live(),
        "a legitimate leader committed the warm-up publication",
        leader=leader,
        live=campaign.live(),
    )

    attempt = campaign.publish_start(producer, 0, sequence, message_id, payload)
    barrier = campaign.await_postmajority_barrier(barrier_path, attempt)
    campaign.journal.append(
        "postmajority_barrier",
        **{key: value for key, value in barrier.items() if key != "event"},
    )
    campaign.check(
        barrier.get("event") == "postmajority_barrier"
        and barrier.get("producer_id") == producer
        and barrier.get("producer_epoch") == 0
        and barrier.get("sequence") == sequence
        and barrier.get("message_id") == message_id,
        "the armed broker held this exact publication at the post-majority barrier",
        barrier=barrier,
    )
    campaign.check(
        isinstance(barrier.get("committed_index"), int)
        and isinstance(barrier.get("logical_offset"), int)
        and isinstance(barrier.get("node"), int),
        "the barrier names the committed index, the logical offset and the node",
        barrier=barrier,
    )
    barrier_node = barrier["node"]
    barrier_offset = barrier["logical_offset"]
    campaign.check(
        barrier_node in campaign.live(),
        "the barrier names a live voter that committed the entry",
        barrier=barrier,
    )
    # The majority proof has to predate the kill: every running voter must carry the
    # barrier message at the barrier's own logical offset first.
    states = campaign.await_message(barrier_offset, message_id, timeout=45)
    campaign.check(
        len(states) >= 2
        and all(
            campaign.message(state, barrier_offset)["digest"] == digest_hex(payload)
            and campaign.message(state, barrier_offset)["payload"] == payload_hex(payload)
            for state in states.values()
        ),
        "every running voter carries the barrier message at the barrier offset "
        "before the kill",
        barrier=barrier,
        states={str(key): value for key, value in states.items()},
    )
    campaign.check(
        not campaign.has_journal_event(
            "publication_committed", producer=producer, epoch=0, sequence=sequence
        ),
        "no acknowledgment for the armed token reached the client before the kill",
        barrier_node=barrier_node,
    )
    campaign.nodes[barrier_node].kill()
    campaign.check(
        barrier_node not in campaign.live(),
        f"the barrier node {barrier_node} was SIGKILLed after the barrier",
        barrier=barrier,
        live=campaign.live(),
    )
    interrupted = campaign.collect_attempt(
        attempt,
        producer,
        0,
        sequence,
        message_id,
        payload,
        exit_event="postmajority_attempt_exit",
    )
    campaign.check(
        interrupted is None or interrupted.get("outcome") != "committed_quorum",
        "the interrupted call never observed COMMITTED_QUORUM",
        observed=interrupted,
    )
    # The crash point has been witnessed. Disarm before recovery so the restarted
    # group behaves exactly like a production broker.
    arm_path.unlink(missing_ok=True)
    campaign.env.pop("ROVQET_Q2_POSTMAJORITY_BARRIER", None)
    for number in campaign.live():
        campaign.nodes[number].stop()
    campaign.check(
        campaign.live() == [],
        "the whole group stopped before recovery on its original storage",
        live=campaign.live(),
    )
    campaign.start([1, 2, 3])
    recovered_leader = campaign.observe_leader()
    campaign.check(
        recovered_leader in campaign.nodes and recovered_leader in campaign.live(),
        "the recovered group elected a legitimate leader",
        leader=recovered_leader,
        live=campaign.live(),
    )
    states = campaign.await_message(barrier_offset, message_id, timeout=60)
    expected = list(range(barrier_offset + 1))
    campaign.check(
        all(
            all(campaign.message(state, index) is not None for index in expected)
            for state in states.values()
        ),
        "every restarted voter retained the whole committed prefix, including the "
        "record the barrier acknowledged",
        barrier_offset=barrier_offset,
        states={str(key): value for key, value in states.items()},
    )
    observed = campaign.publish_until_certain(
        producer, 0, sequence, message_id, payload, timeout=75
    )
    campaign.expect_committed(observed, barrier_offset)
    states = campaign.await_message(barrier_offset, message_id, timeout=60)
    campaign.check(
        all(
            state["next_logical_offset"] == barrier_offset + 1
            and len(
                [
                    message
                    for message in state["messages"]
                    if message.get("message_id") == message_id
                ]
            )
            == 1
            for state in states.values()
        ),
        "the retry resolved to exactly one committed record at the barrier offset",
        barrier_offset=barrier_offset,
        states={str(key): value for key, value in states.items()},
    )
    observed_again = campaign.publish_until_certain(
        producer, 0, sequence, message_id, payload, timeout=75
    )
    campaign.expect_committed(observed_again, barrier_offset)
    states = campaign.await_message(barrier_offset, message_id, timeout=60)
    campaign.check(
        all(
            state["next_logical_offset"] == barrier_offset + 1
            and len(
                [
                    message
                    for message in state["messages"]
                    if message.get("message_id") == message_id
                ]
            )
            == 1
            for state in states.values()
        ),
        "the second retry reports the same offset and creates no duplicate",
        barrier_offset=barrier_offset,
        states={str(key): value for key, value in states.items()},
    )
    campaign.journal.append("scenario_conclusion", scenario=14, verdict=PASS)


def preflight_problem() -> str | None:
    missing = [
        str(path)
        for path in (SERVER, EXAMPLE)
        if not (path.is_file() and os.access(path, os.X_OK))
    ]
    if missing:
        return "missing executable(s): " + ", ".join(missing)
    if SEAM_MARKER not in SERVER.read_bytes():
        return (
            "broker built without the fault-injection feature, so the pre-majority "
            "seam does not exist: run "
            "cargo build -p rovqet-server --features fault-injection --locked"
        )
    if POSTMAJORITY_MARKER not in SERVER.read_bytes():
        return (
            "broker built without the fault-injection feature, so the post-majority "
            "seam does not exist: run "
            "cargo build -p rovqet-server --features fault-injection --locked"
        )
    return None


def main() -> int:
    if "--fixtures-only" not in sys.argv:
        EVIDENCE_DIR.mkdir(parents=True, exist_ok=True)
        problem = preflight_problem()
        if problem is not None:
            path = EVIDENCE_DIR / "q2-publish-not-run.jsonl"
            journal = oracle.Journal(path, "c04-q2-publish-not-run")
            try:
                journal.append("scenario_started", scenario=0, name="preflight")
                journal.append(
                    "scenario_not_run",
                    scenario=0,
                    name="preflight",
                    required=True,
                    reason=problem,
                )
                journal.conclusion(NOT_RUN, problem)
            finally:
                journal.close()
            print(f"NOT_RUN  {problem}")
            print(f"journal: {path}")
            return 2

    loader = unittest.defaultTestLoader
    suite = unittest.TestSuite(
        [
            loader.loadTestsFromTestCase(Q2PublishOracleFixtures),
            loader.loadTestsFromTestCase(Q2JournalRecordingFixtures),
        ]
    )
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    if not result.wasSuccessful():
        print("FAIL  the oracle fixture suite failed", file=sys.stderr)
        return 1
    print(f"PASS  {result.testsRun} oracle fixture tests")

    if "--fixtures-only" in sys.argv:
        return 0

    EVIDENCE_DIR.mkdir(parents=True, exist_ok=True)
    journal = oracle.Journal(JOURNAL_PATH, "c04-q2-publish")
    directory = Path(tempfile.mkdtemp(prefix="rovqet-c04-", dir="/tmp"))
    campaign = Campaign(journal, directory)
    failure: str | None = None
    try:
        run_campaign(campaign)
    except CheckFailure as error:
        failure = str(error)
    except Exception:  # noqa: BLE001 - every unexpected failure is a failed campaign
        failure = traceback.format_exc()
    finally:
        for node in campaign.nodes.values():
            try:
                node.stop()
            except Exception:  # noqa: BLE001 - best effort cleanup
                pass
        records = oracle.load(JOURNAL_PATH, require_conclusion=False)
        verdict = oracle.reconcile_q2_publish(records, require_conclusion=False)
        if failure is not None:
            journal.conclusion(FAIL, failure)
        elif verdict["verdict"] == PASS:
            journal.conclusion(PASS)
        elif verdict["verdict"] == INVALID:
            journal.conclusion(INVALID, "; ".join(verdict["reasons"]))
        elif verdict["verdict"] == NOT_RUN:
            journal.conclusion(NOT_RUN, "; ".join(verdict["reasons"]))
        else:
            journal.conclusion(FAIL, "; ".join(verdict["reasons"]))
        # The verdict journaled as evidence must reflect the conclusion just written:
        # otherwise a crashed campaign could leave a PASS oracle record beside a FAIL
        # conclusion, which is exactly the misleading path this harness must not emit.
        final_records = oracle.load(JOURNAL_PATH, require_conclusion=False)
        verdict = oracle.reconcile_q2_publish(final_records, require_conclusion=False)
        journal.append(
            "oracle_verdict",
            verdict=verdict["verdict"],
            reasons=verdict["reasons"],
            counts=verdict["counts"],
        )
        journal.close()
        shutil.rmtree(directory, ignore_errors=True)

    print(
        f"campaign: {failure if failure is not None else 'all executed checks passed'}"
    )
    print(f"oracle: {verdict['verdict']} {verdict['reasons']}")
    print(f"counts: {json.dumps(verdict['counts'], sort_keys=True)}")
    print(f"journal: {JOURNAL_PATH}")
    if failure is not None or verdict["verdict"] == FAIL:
        return 1
    if verdict["verdict"] == INVALID:
        return 1
    if verdict["verdict"] == NOT_RUN:
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
