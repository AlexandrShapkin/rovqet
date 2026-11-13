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
