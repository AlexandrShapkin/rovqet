#!/usr/bin/env python3
"""Independent Q1 durability oracle.

The parent harness writes append-only JSONL records with an fsync boundary outside the
broker data directory. Reconciliation uses only externally observed TCP results plus a
post-recovery read performed by an external client. Broker WAL files are supplementary.

Verdicts are deliberately distinct: PASS, FAIL, INVALID, NOT_RUN. A missing conclusion,
truncated line or missing required observation is INVALID; it is never a pass.
"""
from __future__ import annotations

import json
import os
import time
from pathlib import Path

PASS = "PASS"
FAIL = "FAIL"
INVALID = "INVALID"
NOT_RUN = "NOT_RUN"
SCHEMA = "rovqet-q1-oracle-v1"


class JournalError(Exception):
    pass


class Journal:
    """Append-only, fsynced observation journal owned by the parent process."""

    def __init__(self, path: Path, run_id: str):
        self.path = Path(path)
        self.run_id = run_id
        self.sequence = 0
        self._handle = self.path.open("w", encoding="utf-8")

    def append(self, event: str, **fields) -> dict:
        record = {
            "schema": SCHEMA,
            "run": self.run_id,
            "seq": self.sequence,
            "event": event,
            "time_mono_ns": time.monotonic_ns(),
            **fields,
        }
        self.sequence += 1
        self._handle.write(json.dumps(record, sort_keys=True) + "\n")
        self._handle.flush()
        os.fsync(self._handle.fileno())
        return record

    def conclusion(self, verdict: str, reason: str | None = None, **fields) -> None:
        if verdict not in (PASS, FAIL, INVALID, NOT_RUN):
            raise JournalError(f"invalid verdict {verdict}")
        self.append("conclusion", verdict=verdict, reason=reason, **fields)

    def close(self) -> None:
        if not self._handle.closed:
            self._handle.close()


def load(path: Path, *, require_conclusion: bool = True) -> list[dict]:
    records = []
    try:
        lines = Path(path).read_text(encoding="utf-8").splitlines()
    except OSError as error:
        raise JournalError(f"unreadable journal: {error}") from error
    for index, line in enumerate(lines):
        if not line.strip():
            raise JournalError(f"blank journal line {index}")
        try:
            record = json.loads(line)
        except json.JSONDecodeError as error:
            raise JournalError(f"truncated or invalid journal line {index}") from error
        if record.get("schema") != SCHEMA:
            raise JournalError(f"unexpected schema at line {index}")
        if record.get("seq") != index:
            raise JournalError(f"non-contiguous journal sequence at line {index}")
        records.append(record)
    if require_conclusion:
        if not records or records[-1].get("event") != "conclusion":
            raise JournalError("journal has no concluding verdict")
    return records


def _result(verdict: str, reasons: list[str], counts: dict) -> dict:
    return {"verdict": verdict, "reasons": reasons, "counts": counts}


def publication_ledger(records: list[dict]) -> dict:
    """Indexes publication attempts and externally received COMMITTED_LOCAL results."""
    acknowledged: dict[tuple, dict] = {}
    uncertain: dict[tuple, dict] = {}
    failed: dict[tuple, dict] = {}
    for record in records:
        if record["event"] == "publication_attempt":
            key = (record["producer"], record["epoch"], record["sequence"])
            uncertain.setdefault(key, record)
        elif record["event"] == "publication_committed":
            key = (record["producer"], record["epoch"], record["sequence"])
            acknowledged[key] = record
            uncertain.pop(key, None)
        elif record["event"] == "publication_failed":
            key = (record["producer"], record["epoch"], record["sequence"])
            failed.setdefault(key, record)
            uncertain.pop(key, None)
    return {"acknowledged": acknowledged, "uncertain": uncertain, "failed": failed}


def reconcile_publications(
    records: list[dict], recovered: list[dict], *, require_conclusion: bool = True
) -> dict:
    """Compares externally acknowledged publications with recovered committed history.

    `recovered` items are {incarnation, offset, message_id, digest} observed through an
    external read client after restart.
    """
    reasons: list[str] = []
    try:
        if require_conclusion:
            if not records or records[-1]["event"] != "conclusion":
                raise JournalError("missing conclusion")
        ledger = publication_ledger(records)
    except JournalError as error:
        return _result(INVALID, [str(error)], {})
    acknowledged = ledger["acknowledged"]
    if not acknowledged and not ledger["uncertain"]:
        return _result(NOT_RUN, ["no publication attempts recorded"], {})
    if not recovered:
        return _result(INVALID, ["no post-recovery observations"], {})

    by_offset = {}
    by_identity = {}
    for item in recovered:
        key = (item["incarnation"], item["offset"])
        if key in by_offset and by_offset[key]["message_id"] != item["message_id"]:
            reasons.append(f"conflicting identities at offset {key}")
        by_offset[key] = item
        identity = (item["message_id"], item["digest"])
        if identity in by_identity and by_identity[identity] != key:
            reasons.append(
                f"duplicate committed publication {item['message_id']} at "
                f"{by_identity[identity]} and {key}"
            )
        by_identity.setdefault(identity, key)

    for key, ack in acknowledged.items():
        reference = (ack["incarnation"], ack["offset"])
        found = by_offset.get(reference)
        if found is None:
            reasons.append(f"acknowledged publication {key} missing after recovery")
            continue
        if found["message_id"] != ack["message_id"]:
            reasons.append(f"message identity changed for {key}")
        if found["digest"] != ack["digest"]:
            reasons.append(f"payload digest changed for {key}")
        if found.get("incarnation") != ack["incarnation"]:
            reasons.append(f"incarnation changed for {key}")

    # Every recovered record should be traceable to an attempt or be an allowed
    # uncertain publication; a completely unknown record is a reconciliation failure.
    for item in recovered:
        match = [
            ack
            for ack in acknowledged.values()
            if ack["message_id"] == item["message_id"]
            and ack["incarnation"] == item["incarnation"]
        ]
        if not match and not any(
            attempt.get("message_id") == item["message_id"]
            for attempt in ledger["uncertain"].values()
        ):
            reasons.append(
                f"recovered record {item['message_id']} has no observed attempt"
            )

    counts = {
        "acknowledged": len(acknowledged),
        "uncertain": len(ledger["uncertain"]),
        "failed": len(ledger["failed"]),
        "recovered": len(recovered),
    }
    return _result(FAIL if reasons else PASS, reasons, counts)


def expected_cursor(application_offsets: list[int], acknowledged: set[int]) -> int | None:
    offsets = sorted(set(application_offsets))
    for offset in offsets:
        if offset not in acknowledged:
            return offset
    return offsets[-1] + 1 if offsets else 0


def reconcile_consumers(
    records: list[dict],
    observed: dict,
    application_offsets: list[int],
    *,
    require_conclusion: bool = True,
) -> dict:
    """Checks acknowledged consumer progress against redelivery and recovered cursor.

    `observed` carries `delivered_offsets` (post-recovery redeliveries) and `cursor`
    (the durable continuation reported by the broker).
    """
    reasons: list[str] = []
    if require_conclusion and (
        not records or records[-1]["event"] != "conclusion"
    ):
        return _result(INVALID, ["missing conclusion"], {})
    acknowledged_by_group: dict[int, dict[int, dict]] = {}
    attempted_offsets = set()
    current_generation: dict[int, int] = {}
    for record in records:
        if record["event"] == "delivery":
            attempted_offsets.add(record["offset"])
        elif record["event"] == "generation_observed":
            # Generation observations are ordered: an ACK is valid while its generation
            # is current, and stale only when issued after a later fence was observed.
            current_generation[record["group"]] = record["generation"]
        elif record["event"] == "ack_committed":
            group = record["group"]
            offset = record["offset"]
            current = current_generation.get(group, record["generation"])
            if record["generation"] != current:
                reasons.append(
                    f"generation {record['generation']} committed ACK while {current} current"
                )
            acknowledged_by_group.setdefault(group, {})[offset] = record
        elif record["event"] == "ack_lookup_committed":
            continue
    acknowledged = set()
    for offsets in acknowledged_by_group.values():
        acknowledged.update(offsets)
    if not acknowledged and not observed.get("delivered_offsets") and not attempted_offsets:
        return _result(NOT_RUN, ["no consumer observations recorded"], {})

    delivered = set(observed.get("delivered_offsets", []))
    redelivered_acked = acknowledged & delivered
    for offset in sorted(redelivered_acked):
        reasons.append(f"acknowledged offset {offset} was redelivered")

    cursor = observed.get("cursor")
    uncertain: list[int] = []
    if cursor is None:
        reasons.append("missing recovered cursor")
    else:
        # Acknowledged offsets beyond the cursor are legitimate sparse state; the cursor
        # must simply never cross an offset that was not attempted or acknowledged.
        for offset in application_offsets:
            if offset < cursor and offset not in acknowledged:
                if offset in attempted_offsets:
                    # A delivered record whose ACK reply was lost may legitimately have
                    # committed. That is uncertain progress, not a cursor skip.
                    uncertain.append(offset)
                else:
                    reasons.append(
                        f"cursor {cursor} crosses unacknowledged offset {offset}"
                    )
        expected = expected_cursor(application_offsets, acknowledged)
        if expected is not None and cursor < expected:
            reasons.append(f"cursor {cursor} did not reach expected {expected}")
    counts = {
        "acknowledged": len(acknowledged),
        "uncertain_committed": len(uncertain),
        "redelivered": len(delivered),
        "cursor": cursor,
        "application_offsets": len(application_offsets),
    }
    return _result(FAIL if reasons else PASS, reasons, counts)


def check_duplicate_retry(
    first: dict, retry: dict, *, token_unchanged: bool
) -> dict:
    """A committed token retried after restart must keep its original offset."""
    reasons = []
    if not token_unchanged:
        reasons.append("retry used a different producer token")
    if first.get("offset") != retry.get("offset"):
        reasons.append("retry changed the durable offset")
    if first.get("message_id") != retry.get("message_id"):
        reasons.append("retry changed the message identity")
    return _result(FAIL if reasons else PASS, reasons, {})


def reconcile_terminal(
    records: list[dict],
    source_history: list[dict],
    dlq_history: list[dict],
    cursor: int | None,
    *,
    require_conclusion: bool = True,
) -> dict:
    """Validates NACK/terminal/DLQ ordering and durable cursor closure.

    `source_history` and `dlq_history` items are
    {incarnation, offset, message_id, digest}; `digest` for DLQ is the digest of the
    original application payload carried by the DLQ message.
    """
    if require_conclusion and (not records or records[-1]["event"] != "conclusion"):
        return _result(INVALID, ["missing conclusion"], {})
    reasons: list[str] = []
    terminal: list[dict] = []
    acked: set[int] = set()
    attempted: set[int] = set()
    dlq_events: dict[object, dict] = {}
    for index, record in enumerate(records):
        event = record["event"]
        if event == "delivery":
            attempted.add(record["offset"])
        elif event == "ack_committed":
            acked.add(record["offset"])
        elif event == "terminal_committed":
            terminal.append(record)
            acked.add(record["offset"])
            if record.get("kind") == "dlq":
                prior = [
                    item
                    for item in records[:index]
                    if item["event"] == "dlq_publication"
                    and item.get("disposition_id") == record["disposition_id"]
                ]
                if not prior:
                    reasons.append(
                        f"DLQ terminal {record['disposition_id']} has no prior DLQ publication"
                    )
                else:
                    destination = prior[-1]
                    if (
                        destination.get("incarnation") != record.get("dlq_incarnation")
                        or destination.get("offset") != record.get("dlq_offset")
                    ):
                        reasons.append(
                            f"terminal proof does not match DLQ publication {record['disposition_id']}"
                        )
        elif event == "dlq_publication":
            key = record["disposition_id"]
            if key in dlq_events:
                reasons.append(f"duplicate DLQ publication {key}")
            dlq_events[key] = record
        elif event == "ack_lookup_committed":
            continue
    if not terminal and not dlq_events:
        return _result(NOT_RUN, ["no terminal observations recorded"], {})

    source_offsets = {item["offset"]: item for item in source_history}
    for item in terminal:
        if item["offset"] not in source_offsets:
            reasons.append(f"terminal source offset {item['offset']} is not committed history")
        if item.get("kind") == "dlq":
            matches = [
                entry
                for entry in dlq_history
                if entry["message_id"] == item["disposition_id"]
            ]
            if len(matches) != 1:
                reasons.append(
                    f"DLQ disposition {item['disposition_id']} has {len(matches)} destination records"
                )
            elif (
                matches[0]["incarnation"] != item.get("dlq_incarnation")
                or matches[0]["offset"] != item.get("dlq_offset")
            ):
                reasons.append(
                    f"DLQ identity mismatch for {item['disposition_id']}"
                )
            elif matches[0].get("digest") and item.get("payload_digest") and (
                matches[0]["digest"] != item["payload_digest"]
            ):
                reasons.append(f"DLQ payload digest mismatch for {item['disposition_id']}")

    source_by_id = {item["message_id"]: item for item in source_history}
    for item in terminal:
        original = source_by_id.get(item.get("original_message_id"))
        if original is None:
            reasons.append(f"terminal lost original source identity {item.get('original_message_id')}")
        elif item.get("source_incarnation") != original.get("incarnation"):
            reasons.append("terminal source incarnation mismatch")

    if cursor is None:
        reasons.append("missing recovered cursor")
    else:
        for offset, item in source_offsets.items():
            if offset < cursor and offset not in acked and offset not in attempted:
                reasons.append(f"cursor {cursor} crosses unexplained source offset {offset}")
    counts = {
        "terminal": len(terminal),
        "dlq_publications": len(dlq_events),
        "dlq_records": len(dlq_history),
        "cursor": cursor,
    }
    return _result(FAIL if reasons else PASS, reasons, counts)


MIGRATION_STAGES = ("prepare", "catch_up", "fence_old", "activate_new", "cleanup")


def reconcile_migration(records: list[dict], *, require_conclusion: bool = True) -> dict:
    """Checks an ownership migration journal for safety violations.

    The journal is written by the parent harness and the oracle uses only the observed
    events. A contradiction in the evidence is INVALID, a broken safety invariant is FAIL
    and a journal without migration evidence is NOT_RUN. Nothing here is inferred from a
    clock: an old owner that was not fenced counts as a violation, not as an expiry.
    """
    reasons: list[str] = []
    begins = [r for r in records if r["event"] == "migration_begin"]
    stages = [r for r in records if r["event"] == "migration_stage"]
    fences = [r for r in records if r["event"] == "fence_confirmed"]
    activations = [r for r in records if r["event"] == "activation"]
    commits = [r for r in records if r["event"] == "publication_committed"]

    if not begins and not stages and not activations:
        return _result(NOT_RUN, ["no migration evidence"], {})

    identifiers = {
        r.get("migration_id")
        for r in begins + stages + fences + activations
        if r.get("migration_id") is not None
    }
    if len(identifiers) > 1:
        reasons.append(
            f"conflicting migration ids: {sorted(str(value) for value in identifiers)}"
        )
    if not identifiers:
        reasons.append("migration evidence without a migration id")
    migration_id = next(iter(identifiers), None)

    # The committed stages may not skip and may not move backwards.
    order: list[tuple[int, str]] = []
    for record in stages:
        stage = record.get("stage")
        if stage not in MIGRATION_STAGES:
            reasons.append(f"unknown migration stage {stage}")
            continue
        order.append((MIGRATION_STAGES.index(stage), stage))
    for previous, current in zip(order, order[1:]):
        if current[0] < previous[0]:
            reasons.append(f"migration stage moved back from {previous[1]} to {current[1]}")
        elif current[0] > previous[0] + 1:
            reasons.append(f"migration stage jumped from {previous[1]} to {current[1]}")

    # The activation needs a durable fence confirmation and the transfer evidence.
    if activations and not fences:
        reasons.append("the activation has no durable fence confirmation")
    if activations and fences:
        if fences[0]["seq"] > activations[0]["seq"]:
            reasons.append("the activation precedes the fence confirmation")
    if activations:
        activation = activations[0]
        for field in ("fence_offset", "fence_digest", "manifest_generation"):
            if activation.get(field) in (None, ""):
                reasons.append(f"the activation has no {field}")
        if activation.get("target_ready") is False:
            reasons.append("the activation has no target readiness evidence")
        if activation.get("transfer_watermark") is None:
            reasons.append("the activation has no verified transfer watermark")

    # No dual owner: the source may not commit after its fence and the target may not
    # commit before the activation.
    fence_seq = fences[0]["seq"] if fences else None
    activation_seq = activations[0]["seq"] if activations else None
    source = begins[0].get("source") if begins else None
    target = begins[0].get("target") if begins else None
    for record in commits:
        node = record.get("node")
        if fence_seq is not None and node == source and record["seq"] > fence_seq:
            reasons.append(
                f"the old owner committed at seq {record['seq']} after its fence at {fence_seq}"
            )
        if activation_seq is not None and node == target and record["seq"] < activation_seq:
            reasons.append(
                f"the new owner committed at seq {record['seq']} before the activation at {activation_seq}"
            )

    # The producer outcomes survive: one token keeps one offset and one message identity.
    outcomes: dict[tuple, dict] = {}
    for record in commits:
        key = (record.get("producer"), record.get("epoch"), record.get("sequence"))
        previous = outcomes.get(key)
        if previous is None:
            outcomes[key] = record
            continue
        if previous.get("offset") != record.get("offset"):
            reasons.append(f"the producer token {key} changed its offset after the migration")
        if previous.get("message_id") != record.get("message_id"):
            reasons.append(f"the producer token {key} changed its message identity")

    # The incarnation may not be replaced silently.
    incarnations = {record.get("incarnation") for record in commits if record.get("incarnation")}
    if len(incarnations) > 1:
        reasons.append(f"the log incarnation changed during the migration: {sorted(map(str, incarnations))}")

    # The committed watermark of the source and the target must agree.
    if activations and fences:
        final = [r for r in records if r["event"] == "transfer_finalize"]
        if final:
            target_watermark = final[-1].get("last_committed_offset")
            activation_watermark = activations[0].get("transfer_watermark")
            if target_watermark is not None and activation_watermark is not None:
                if int(target_watermark) != int(activation_watermark):
                    reasons.append(
                        f"the target watermark {target_watermark} differs from the committed "
                        f"activation watermark {activation_watermark}"
                    )

    # A dependent DLQ partition must have a recorded proof: a terminal disposition that
    # references a DLQ record needs a recoverable destination, and a missing or fabricated
    # proof must never pass.
    dependencies = [
        record for record in records if record["event"] == "migration_dependency"
    ]
    proofs = [record for record in records if record["event"] == "dlq_proof"]
    for dependency in dependencies:
        if not any(
            proof.get("migration_id") == dependency.get("migration_id")
            and proof.get("partition") == dependency.get("partition")
            for proof in proofs
        ):
            reasons.append(
                "the dependent partition "
                f"{dependency.get('partition')} has no recorded DLQ proof"
            )
    for proof in proofs:
        if proof.get("verified") is not True:
            reasons.append(
                f"the DLQ proof of {proof.get('partition')} is not verified"
            )

    # A source or target commit outside the committed ownership window is a dual owner.
    for record in commits:
        node = record.get("node")
        if node == source and fence_seq is not None and record["seq"] <= fence_seq:
            continue
        if node == target and activation_seq is not None and record["seq"] > activation_seq:
            continue
        if node in (source, target) and fence_seq is not None and activation_seq is not None:
            if fence_seq < record["seq"] < activation_seq:
                reasons.append(
                    f"node {node} committed at seq {record['seq']} between the fence and "
                    "the activation"
                )

    counts = {
        "migration_id": str(migration_id),
        "stages": len(stages),
        "fences": len(fences),
        "activations": len(activations),
        "commits": len(commits),
        "dependencies": len(dependencies),
        "dlq_proofs": len(proofs),
    }
    verdict = FAIL if reasons else PASS
    return _result(verdict, reasons, counts)
