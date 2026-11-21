#!/usr/bin/env python3
"""Independent Q1 durability oracle.

The parent harness writes append-only JSONL records with an fsync boundary outside the
broker data directory. Reconciliation uses only externally observed TCP results plus a
post-recovery read performed by an external client. Broker WAL files are supplementary.

Verdicts are deliberately distinct: PASS, FAIL, INVALID, NOT_RUN. A missing conclusion,
truncated line or missing required observation is INVALID; it is never a pass.
"""
from __future__ import annotations

import hashlib
import json
import os
import time
from pathlib import Path

PASS = "PASS"
FAIL = "FAIL"
INVALID = "INVALID"
NOT_RUN = "NOT_RUN"
SCHEMA = "rovqet-q1-oracle-v1"


def digest_hex(value: bytes) -> str:
    """The payload digest used by every observation of this oracle."""
    return hashlib.sha256(value).hexdigest()


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

# The brokers print their committed stage as
# ``format!("{:?}", record.stage).to_lowercase()`` (crates/server/src/migration.rs
# ``report()``), which yields the compact spellings below.  The journals record
# exactly what the broker printed, so both spellings name the same stage.
STAGE_ALIASES = {
    "catchup": "catch_up",
    "fenceold": "fence_old",
    "activatenew": "activate_new",
}


def canonical_stage(value):
    """Map a broker-printed stage spelling onto the canonical stage name.

    ``prepare`` and ``cleanup`` are already canonical and pass through; an
    unknown value is returned unchanged so the caller can reject it with
    ``unknown migration stage <value>``.
    """
    if isinstance(value, str):
        return STAGE_ALIASES.get(value, value)
    return value


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
        stage = canonical_stage(record.get("stage"))
        if stage not in MIGRATION_STAGES:
            reasons.append(f"unknown migration stage {record.get('stage')}")
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


def reconcile_bundle(records: list[dict], *, require_conclusion: bool = True) -> dict:
    """Reconciles a coordinated source-plus-DLQ handover journal.

    The journal is written by the parent harness from externally received results only: the
    publisher's committed offsets, the consumer's terminal outcomes, the externally read
    source and DLQ records before and after the handover, and the committed transitions the
    brokers reported. A contradiction is INVALID, a broken safety invariant is FAIL and a
    journal without bundle evidence is NOT_RUN. Nothing is inferred from a clock or from a
    broker's own claim that a stage completed.
    """
    reasons: list[str] = []
    if require_conclusion and (not records or records[-1]["event"] != "conclusion"):
        return _result(INVALID, ["missing conclusion"], {})

    begins = [r for r in records if r["event"] == "migration_begin"]
    stages = [r for r in records if r["event"] == "migration_stage"]
    activations = [r for r in records if r["event"] == "activation"]
    fences = [r for r in records if r["event"] == "fence_confirmed"]
    inventories = [r for r in records if r["event"] == "bundle_inventory"]
    promotions = [r for r in records if r["event"] == "bundle_promotion"]
    quiesced = [r for r in records if r["event"] == "bundle_quiesced"]
    proofs = [r for r in records if r["event"] == "dlq_proof"]
    publications = [r for r in records if r["event"] == "dlq_publication"]
    terminals = [r for r in records if r["event"] == "terminal_committed"]
    recovered = [r for r in records if r["event"] == "recovered_record"]
    commits = [r for r in records if r["event"] == "publication_committed"]
    successor = [r for r in records if r["event"] == "successor_active"]
    restarts = [r for r in records if r["event"] == "successor_restarted"]

    if not begins and not activations:
        return _result(NOT_RUN, ["no bundle migration evidence"], {})

    identifiers = {
        str(r.get("migration_id"))
        for r in begins + stages + fences + activations + inventories
        + promotions + proofs + quiesced
        if r.get("migration_id") is not None
    }
    if not identifiers:
        reasons.append("bundle evidence without a migration id")
    if len(identifiers) > 1:
        reasons.append(f"conflicting migration ids: {sorted(identifiers)}")
    migration_id = next(iter(identifiers), None)

    # 1. The parent identifier must be bound to both partitions.
    if begins:
        begin = begins[0]
        if not begin.get("dependent_partition"):
            reasons.append("the bundle has no dependent partition identity")
        if begin.get("dependent_incarnation") in (None, 0):
            reasons.append("the bundle has no dependent incarnation")
        if begin.get("incarnation") in (None, 0):
            reasons.append("the bundle has no source incarnation")
        if begin.get("dependent_partition") == begin.get("partition"):
            reasons.append("the dependency is the source partition itself")

    # 2. The committed stages may neither skip nor move backwards.
    order: list[tuple[int, str]] = []
    for record in stages:
        stage = canonical_stage(record.get("stage"))
        if stage not in MIGRATION_STAGES:
            reasons.append(f"unknown migration stage {record.get('stage')}")
            continue
        order.append((MIGRATION_STAGES.index(stage), stage))
    for previous, current in zip(order, order[1:]):
        if current[0] < previous[0]:
            reasons.append(f"migration stage moved back from {previous[1]} to {current[1]}")
        elif current[0] > previous[0] + 1:
            reasons.append(f"migration stage jumped from {previous[1]} to {current[1]}")
    if order and order[-1][1] != "activate_new":
        reasons.append(f"the migration never activated: last stage {order[-1][1]}")

    # 3. Both members must be fenced durably before the activation.
    fence_members = {fence.get("member") for fence in fences}
    if activations and fence_members != {"source", "dependent"}:
        reasons.append(
            f"the activation needs the fence of both members, observed {sorted(fence_members)}"
        )
    for fence in fences:
        if fence.get("node") != (begins[0]["source"] if begins else fence.get("node")):
            reasons.append(f"the {fence.get('member')} fence was not produced by the source")
        if fence.get("fence_digest") in (None, ""):
            reasons.append(f"the {fence.get('member')} fence has no durable digest")
        if fence.get("fence_offset") is None:
            reasons.append(f"the {fence.get('member')} fence has no durable offset")
    if activations and fences:
        activation_seq = activations[0]["seq"]
        for fence in fences:
            if fence["seq"] > activation_seq:
                reasons.append(
                    f"the {fence.get('member')} fence follows the activation"
                )

    # 4. Two independently identified inventories, both finally verified.
    by_member: dict[str, list[dict]] = {}
    for inventory in inventories:
        by_member.setdefault(inventory.get("member", "?"), []).append(inventory)
        if inventory.get("manifest_id") in (None, ""):
            reasons.append(f"the {inventory.get('member')} inventory has no identity")
        if inventory.get("watermark") is None:
            reasons.append(f"the {inventory.get('member')} inventory has no watermark")
    if inventories and set(by_member) != {"source", "dependent"}:
        reasons.append(f"both inventories must be transferred: {sorted(by_member)}")
    source_final = [i for i in by_member.get("source", []) if i.get("phase") == "final"]
    dependent_final = [
        i for i in by_member.get("dependent", []) if i.get("phase") == "final"
    ]
    if inventories and (not source_final or not dependent_final):
        reasons.append("both members need a final verified inventory")
    if source_final and dependent_final:
        if source_final[-1]["manifest_id"] == dependent_final[-1]["manifest_id"]:
            reasons.append("the two bundle inventories share one identity")

    # 5. The activation carries both final identities, both fences and readiness.
    if activations:
        activation = activations[0]
        if activation.get("target_ready") is not True:
            reasons.append("the activation has no target readiness evidence")
        if activation.get("transfer_watermark") is None:
            reasons.append("the activation has no source transfer watermark")
        if activation.get("dependent_transfer_watermark") is None:
            reasons.append("the activation has no dependent transfer watermark")
        for field in ("fence_offset", "fence_digest"):
            if activation.get(field) in (None, ""):
                reasons.append(f"the activation has no source {field}")
        for field in ("dependent_fence_offset", "dependent_fence_digest"):
            if activation.get(field) in (None, ""):
                reasons.append(f"the activation has no dependent {field}")
        if source_final and activation.get("manifest_id") != source_final[-1]["manifest_id"]:
            reasons.append("the activation names another source inventory")
        if dependent_final and (
            activation.get("dependent_manifest_id") != dependent_final[-1]["manifest_id"]
        ):
            reasons.append("the activation names another dependent inventory")
        if dependent_final and (
            activation.get("dependent_transfer_watermark")
            != dependent_final[-1]["watermark"]
        ):
            reasons.append("the dependent activation watermark is not the verified one")

    # 6. The quiescence barrier precedes both fences and reports its accounting.
    if quiesced:
        if fences and quiesced[0]["seq"] > min(fence["seq"] for fence in fences):
            reasons.append("a fence precedes the bundle quiescence barrier")
        if quiesced[0].get("source_watermark") is None:
            reasons.append("the quiescence barrier reports no watermark")
    elif fences:
        reasons.append("the bundle was fenced without a quiescence barrier")

    # 7. The terminal disposition: one publication, one committed proof, one destination.
    ids = [record["disposition_id"] for record in publications]
    if len(ids) != len(set(ids)):
        reasons.append(f"duplicate DLQ publications: {ids}")
    dlq_by_identity: dict[tuple, dict] = {}
    for record in recovered:
        if record.get("partition") != "dependent":
            continue
        key = (record.get("incarnation"), record.get("offset"))
        if key in dlq_by_identity and (
            dlq_by_identity[key]["message_id"] != record.get("message_id")
            or dlq_by_identity[key]["digest"] != record.get("digest")
        ):
            reasons.append(f"the recovered DLQ record {key} changed")
        dlq_by_identity[key] = record
    for terminal in terminals:
        if terminal.get("kind") != "dlq":
            continue
        prior = [
            item for item in publications
            if item["disposition_id"] == terminal["disposition_id"]
            and item["seq"] < terminal["seq"]
        ]
        if not prior:
            reasons.append(
                f"DLQ terminal {terminal['disposition_id']} has no prior publication"
            )
        else:
            destination = prior[-1]
            if (
                destination.get("incarnation") != terminal.get("dlq_incarnation")
                or destination.get("offset") != terminal.get("dlq_offset")
            ):
                reasons.append(
                    f"the terminal proof of {terminal['disposition_id']} does not match "
                    "its publication"
                )
            if destination.get("digest") != terminal.get("payload_digest"):
                reasons.append(
                    f"the terminal proof of {terminal['disposition_id']} carries another digest"
                )
    for proof in proofs:
        if proof.get("verified") is not True:
            reasons.append(
                f"the DLQ proof of {proof.get('partition')} is not verified"
            )
        if terminals and proof.get("disposition_id") != terminals[0].get("disposition_id"):
            reasons.append("the dependent proof names another disposition")
        if terminals and proof.get("dlq_offset") != terminals[0].get("dlq_offset"):
            reasons.append("the dependent proof names another destination offset")
    if terminals and not proofs:
        reasons.append("the committed disposition has no dependent proof")

    # 8. The successor may only become active after both promotions completed.
    for active in successor:
        prior = [p for p in promotions if p["seq"] < active["seq"]]
        if not prior:
            reasons.append("the successor became active before any promotion evidence")
        elif not (prior[-1].get("dependent_promoted") and prior[-1].get("source_promoted")):
            reasons.append("the successor became active before both promotions completed")
        if not restarts:
            pass
    if promotions and not successor:
        reasons.append("the bundle promoted the storage but the successor never activated")

    # 9. The successor serves exactly the observed source records and DLQ record.
    expected_dlq = {record["disposition_id"]: record for record in publications}
    served_dlq = [
        record for record in recovered
        if record.get("partition") == "dependent" and not record.get("restart")
    ]
    if expected_dlq:
        if not served_dlq:
            reasons.append("the successor never served the dependent record")
        else:
            served = served_dlq[0]
            published = expected_dlq.get(served["message_id"])
            if published is None:
                reasons.append(
                    f"the successor served an unknown disposition {served['message_id']}"
                )
            elif (
                published.get("incarnation") != served.get("incarnation")
                or published.get("offset") != served.get("offset")
                or published.get("digest") != served.get("digest")
            ):
                reasons.append("the recovered dependent record changed identity")
    for record in recovered:
        if record.get("partition") != "source":
            continue
        acknowledged = [
            commit for commit in commits
            if commit.get("offset") == record.get("offset")
            and commit.get("message_id") == record.get("message_id")
        ]
        if not acknowledged:
            reasons.append(
                f"the recovered source record {record.get('offset')} has no observed commit"
            )
        elif acknowledged[0].get("digest") != record.get("digest"):
            reasons.append(
                f"the payload digest of source offset {record.get('offset')} changed"
            )

    # 10. No dual owner: the old owner never commits after its fence.
    source_fence_seq = None
    if fences:
        source_fences = [fence for fence in fences if fence.get("member") == "source"]
        if source_fences:
            source_fence_seq = min(fence["seq"] for fence in source_fences)
    if source_fence_seq is not None and begins:
        source_node = begins[0]["source"]
        for commit in commits:
            if commit.get("node") == source_node and commit["seq"] > source_fence_seq:
                reasons.append(
                    f"the old owner committed at seq {commit['seq']} after its fence"
                )

    # 11. One producer token keeps one offset and one message identity.
    outcomes: dict[tuple, dict] = {}
    for commit in commits:
        key = (commit.get("producer"), commit.get("epoch"), commit.get("sequence"))
        previous = outcomes.get(key)
        if previous is None:
            outcomes[key] = commit
            continue
        if previous.get("offset") != commit.get("offset"):
            reasons.append(f"the producer token {key} changed its offset")
        if previous.get("message_id") != commit.get("message_id"):
            reasons.append(f"the producer token {key} changed its message identity")

    # 12. A terminal lookup never publishes a second dependent message.
    for lookup in (r for r in records if r["event"] == "terminal_lookup"):
        if lookup.get("dlq_records") not in (1, None):
            reasons.append(
                f"a terminal lookup left {lookup.get('dlq_records')} dependent records"
            )

    counts = {
        "migration_id": migration_id,
        "stages": len(stages),
        "fences": len(fences),
        "inventories": len(inventories),
        "promotions": len(promotions),
        "dlq_publications": len(publications),
        "dlq_proofs": len(proofs),
        "terminals": len(terminals),
        "recovered_records": len(recovered),
        "restarts": len(restarts),
    }
    return _result(FAIL if reasons else PASS, reasons, counts)


def reconcile_safety(records: list[dict], *, require_conclusion: bool = True) -> dict:
    """Reconciles a crash, isolation or uncertainty journal.

    The journal is written by the parent harness from externally observed results only:
    the process lifecycle, the committed transition reports of the brokers, the results a
    client received and the histories an external reader saw after a restart. The verifier
    establishes the safety properties from that data and never from a broker's own claim
    that it did the right thing.

    A contradiction is INVALID, a broken safety invariant is FAIL and a journal without
    evidence is NOT_RUN. A missed barrier, a missing observation or an absent journal is
    never a pass. A journal whose own conclusion is not PASS is never reported as a
    successful safety verdict: the harness that observed the scenario has the final say
    about whether the scenario succeeded.
    """
    reasons: list[str] = []
    if require_conclusion and (not records or records[-1]["event"] != "conclusion"):
        return _result(INVALID, ["missing conclusion"], {})
    # The journal order is the observation order: a fixture without an explicit sequence
    # number keeps the position it was written at.
    records = [
        record if record.get("seq") is not None else {**record, "seq": index}
        for index, record in enumerate(records)
    ]

    # 0. The scenario's own conclusion must be a success. A scenario that concluded
    # FAIL, INVALID or NOT_RUN can never be reported as a successful safety verdict,
    # even when the journal alone does not contradict it.
    conclusions = [r for r in records if r.get("event") == "conclusion"]
    if conclusions:
        conclusion = conclusions[-1]
        own_verdict = conclusion.get("verdict")
        if own_verdict != PASS:
            detail = conclusion.get("reason")
            if detail:
                reasons.append(f"the scenario concluded {own_verdict}: {detail}")
            else:
                reasons.append(f"the scenario concluded {own_verdict}")

    scenarios = [r for r in records if r["event"] == "scenario_started"]
    barriers = [r for r in records if r["event"] == "barrier_reached"]
    kills = [r for r in records if r["event"] == "process_killed"]
    restarts = [r for r in records if r["event"] == "process_restarted"]
    attempts = [r for r in records if r["event"] == "publication_attempt"]
    commits = [r for r in records if r["event"] == "publication_committed"]
    failures = [r for r in records if r["event"] == "publication_failed"]
    histories = [r for r in records if r["event"] == "history_observed"]
    routes = [r for r in records if r["event"] == "route_active"]
    stages = [r for r in records if r["event"] == "migration_stage"]
    fences = [r for r in records if r["event"] == "fence_confirmed"]
    transfers = [r for r in records if r["event"] == "transfer_verified"]
    dispositions = [r for r in records if r["event"] == "dlq_disposition"]
    cursors = [r for r in records if r["event"] == "cursor_observed"]
    receipts = [r for r in records if r["event"] == "chunk_receipts"]

    if not scenarios and not reasons:
        # A journal with no scenario evidence is NOT_RUN, but a journal whose own
        # conclusion already failed must keep that failure instead.
        return _result(NOT_RUN, ["no scenario evidence"], {})

    # 1. A scenario that never reached its barrier is never a pass.
    for scenario in scenarios:
        if scenario.get("barrier"):
            reached = [b for b in barriers if b.get("barrier") == scenario["barrier"]]
            if not reached:
                reasons.append(
                    f"the scenario {scenario.get('scenario')} never reached its barrier "
                    f"{scenario['barrier']}"
                )
        if not scenario.get("build"):
            reasons.append(f"the scenario {scenario.get('scenario')} has no build identity")

    # 2. A killed process must be restarted on its original storage.
    killed = {r.get("process") for r in kills}
    restarted = {r.get("process") for r in restarts}
    for process in killed - restarted:
        reasons.append(f"the killed process {process} was never restarted")

    # 3. No acknowledged publication may disappear.
    acknowledged = {}
    for commit in commits:
        key = (commit.get("node"), commit.get("producer"), commit.get("epoch"),
               commit.get("sequence"))
        previous = acknowledged.get(key)
        if previous is None:
            acknowledged[key] = commit
            continue
        if previous.get("offset") != commit.get("offset"):
            reasons.append(f"the producer token {key} changed its offset")
        if previous.get("message_id") != commit.get("message_id"):
            reasons.append(f"the producer token {key} changed its message identity")
    for history in histories:
        node = history.get("node")
        for record in history.get("records", []):
            key = (node, record.get("producer"), record.get("epoch"),
                   record.get("sequence"))
            observed = acknowledged.get(key)
            if observed is None:
                continue
            if observed.get("offset") != record.get("offset"):
                reasons.append(
                    f"the recovered record of {key} changed its offset"
                )
            if observed.get("message_id") != record.get("message_id"):
                reasons.append(
                    f"the recovered record of {key} changed its message identity"
                )
            if observed.get("digest") and record.get("digest") \
                    and observed["digest"] != record["digest"]:
                reasons.append(
                    f"the recovered payload of {key} changed its digest"
                )
    # Every acknowledged publication must survive in a history that was observed after the
    # acknowledgement. A node whose history was never observed after the acknowledgement
    # carries no evidence either way.
    for key, commit in acknowledged.items():
        node = key[0]
        later = [
            history for history in histories
            if history.get("node") == node and history["seq"] > commit["seq"]
        ]
        if not later:
            continue
        recovered_here = [
            record
            for history in later
            for record in history.get("records", [])
            if record.get("offset") == commit.get("offset")
            and record.get("message_id") == commit.get("message_id")
        ]
        if not recovered_here:
            reasons.append(
                f"the acknowledged publication {key} is missing from the recovered history"
            )

    # 4. An uncertain outcome is never a definite noncommit.
    for failure in failures:
        if failure.get("outcome") == "timeout" and failure.get("committed") is True:
            reasons.append("a timeout was reported as a committed outcome")
    for attempt in attempts:
        key = (attempt.get("node"), attempt.get("producer"), attempt.get("epoch"),
               attempt.get("sequence"))
        if key in acknowledged and attempt.get("outcome") == "timeout":
            reasons.append(
                f"the attempt {key} is both uncertain and acknowledged without an outcome"
            )

    # 5. The incarnation of an intact history never changes.
    incarnations = {
        record.get("incarnation")
        for history in histories
        for record in history.get("records", [])
        if record.get("incarnation")
    }
    if len(incarnations) > 1:
        reasons.append(f"the log incarnation changed: {sorted(map(str, incarnations))}")

    # 6. No target commit may precede the verified activation.
    activations = [
        s for s in stages if canonical_stage(s.get("stage")) == "activate_new"
    ]
    for commit in commits:
        if commit.get("node") != "target":
            continue
        if not activations:
            reasons.append("the target committed without a committed activation")
            break
        if commit["seq"] < activations[0]["seq"]:
            reasons.append(
                f"the target committed at seq {commit['seq']} before the activation"
            )

    # 7. No old owner commit may follow its effective durable fence.
    for commit in commits:
        if commit.get("node") != "source":
            continue
        later = [f for f in fences if f["seq"] < commit["seq"]]
        if later:
            reasons.append(
                f"the old owner committed at seq {commit['seq']} after its fence at "
                f"{later[0]['seq']}"
            )

    # 8. No activation may happen without a verified transfer.
    for activation in activations:
        prior = [t for t in transfers if t["seq"] < activation["seq"]]
        if not prior:
            reasons.append("the activation has no verified transfer")
        if not any(t.get("member") == "source" for t in prior):
            reasons.append("the activation has no verified source inventory")
        if any(t.get("member") == "dependent" for t in prior) and not any(
            t.get("member") == "dependent" and t.get("verified") for t in prior
        ):
            reasons.append("the activation has no verified dependent inventory")

    # 9. The committed stage never moves backwards and never skips.
    order: list[tuple[int, str]] = []
    for record in stages:
        stage = canonical_stage(record.get("stage"))
        if stage not in MIGRATION_STAGES:
            reasons.append(f"unknown migration stage {record.get('stage')}")
            continue
        order.append((MIGRATION_STAGES.index(stage), stage))
    for previous, current in zip(order, order[1:]):
        if current[0] < previous[0]:
            reasons.append(f"the migration stage moved back from {previous[1]} to {current[1]}")
        elif current[0] > previous[0] + 1:
            reasons.append(f"the migration stage jumped from {previous[1]} to {current[1]}")

    # 10. A repeated committed transition never creates a second revision.
    revisions: dict[str, set[int]] = {}
    for record in stages:
        revisions.setdefault(
            str(canonical_stage(record.get("stage", ""))), set()
        ).add(record.get("revision"))
    for stage, values in revisions.items():
        if len(values) > 1:
            reasons.append(f"the stage {stage} was committed with several revisions: {values}")

    # 11. No committed disposition is duplicated and every disposition is proved.
    ids = [d.get("disposition_id") for d in dispositions]
    if len(ids) != len(set(ids)):
        reasons.append(f"a disposition was published twice: {ids}")
    for disposition in dispositions:
        if disposition.get("verified") is not True:
            reasons.append(
                f"the disposition {disposition.get('disposition_id')} is not verified"
            )

    # 12. The cursor never moves backwards and never skips an unacknowledged record.
    by_node: dict[object, list[int]] = {}
    for cursor in cursors:
        by_node.setdefault(cursor.get("node"), []).append(cursor.get("cursor"))
    for node, values in by_node.items():
        for previous, current in zip(values, values[1:]):
            if current < previous:
                reasons.append(f"the cursor of {node} moved back from {previous} to {current}")

    # 13. The staged chunk receipts are unique per chunk identity.
    for observation in receipts:
        seen: dict[tuple, str] = {}
        for entry in observation.get("chunks", []):
            key = (entry.get("file_index"), entry.get("offset"))
            digest = entry.get("digest")
            if key in seen and seen[key] != digest:
                reasons.append(f"the chunk receipt {key} changed its digest")
            seen[key] = digest
        expected = observation.get("expected_chunks")
        if expected is not None and len(seen) != expected:
            reasons.append(
                f"the staging area holds {len(seen)} chunk receipts, expected {expected}"
            )

    counts = {
        "scenarios": len(scenarios),
        "barriers": len(barriers),
        "kills": len(kills),
        "restarts": len(restarts),
        "commits": len(commits),
        "histories": len(histories),
        "stages": len(stages),
        "fences": len(fences),
        "transfers": len(transfers),
        "dispositions": len(dispositions),
    }
    return _result(FAIL if reasons else PASS, reasons, counts)


# -- Q2 Raft acceptance ------------------------------------------------------

# The three-voter Q2 cluster commits with two of three voters.
MAJORITY = 2

# Both event names describe the committed state of one node: `applied_state` is the
# live applied view and `recovered_state` is the same view taken after a restart or
# catch-up. A majority proof may draw on either, but never on a broker's own claim.
Q2_STATE_EVENTS = ("applied_state", "recovered_state")


def _q2_int(value) -> int | None:
    """A count or index that may serialise as a JSON number or a decimal string."""
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _q2_request_ids(value) -> set[str]:
    """Normalise one or many request identities into decimal strings."""
    if value is None or isinstance(value, bool):
        return set()
    if isinstance(value, (list, tuple, set)):
        ids: set[str] = set()
        for item in value:
            ids |= _q2_request_ids(item)
        return ids
    number = _q2_int(value)
    if number is not None:
        return {str(number)}
    text = str(value).strip()
    return {text} if text else set()


def _q2_partition_key(value) -> str | None:
    if isinstance(value, dict):
        return "{" + ",".join(f"{key}={value[key]}" for key in sorted(value)) + "}"
    return None if value is None else str(value)


def _q2_state(record: dict) -> dict:
    """One committed-state observation with normalised partitions."""
    partitions = []
    for entry in record.get("partitions") or []:
        if not isinstance(entry, dict):
            continue
        partitions.append(
            {
                "key": _q2_partition_key(entry.get("partition")),
                "count": _q2_int(entry.get("count")),
                "chain": entry.get("chain"),
                "request_ids": _q2_request_ids(entry.get("request_id"))
                | _q2_request_ids(entry.get("request_ids")),
            }
        )
    return {
        "seq": record.get("seq"),
        "event": record.get("event"),
        "node": record.get("node"),
        "applied_index": _q2_int(record.get("applied_index")),
        "request_ids": _q2_request_ids(record.get("request_id"))
        | _q2_request_ids(record.get("request_ids")),
        "partitions": partitions,
    }


def _q2_state_request_ids(state: dict) -> set[str]:
    ids = set(state["request_ids"])
    for partition in state["partitions"]:
        ids |= partition["request_ids"]
    return ids


def _q2_classification(record: dict) -> str:
    for field in ("classification", "class", "outcome", "kind"):
        value = record.get(field)
        if value not in (None, ""):
            return str(value)
    return ""


def _q2_committed_classification(record: dict) -> bool:
    """Whether a classified outcome claims a durable commit."""
    explicit = record.get("committed")
    if isinstance(explicit, bool):
        return explicit
    text = _q2_classification(record).strip().lower()
    if not text:
        return False
    if text.startswith(
        ("un", "not", "non", "never", "reject", "discard", "drop", "truncat",
         "partial", "timeout", "fail", "lose", "lost", "abort", "orphan")
    ):
        return False
    return "commit" in text or text in {"applied", "durable", "replicated"}


def _q2_state_regression(
    label: str,
    marker: str,
    node,
    marker_seq,
    previous: dict,
    later: dict,
    reasons: list[str],
) -> None:
    """Compare the last state before a marker with one state after it.

    Counts are compared strictly in every case. Chains are compared only when the
    counts are equal, because a journal records one latest chain per partition and
    the chain of an extended count legitimately differs from the shorter prefix.
    """
    for partition in previous["partitions"]:
        if partition["key"] is None or partition["count"] is None:
            continue
        match = next(
            (entry for entry in later["partitions"] if entry["key"] == partition["key"]),
            None,
        )
        if match is None or match["count"] is None:
            reasons.append(
                f"{label}: node {node} no longer reports partition {partition['key']} "
                f"after {marker} at seq {marker_seq}; its committed count "
                f"{partition['count']} disappeared"
            )
            continue
        if match["count"] < partition["count"]:
            reasons.append(
                f"{label}: node {node} lost a committed command after {marker} at seq "
                f"{marker_seq}: partition {partition['key']} count {partition['count']} "
                f"-> {match['count']}"
            )
        elif match["count"] == partition["count"] and match["chain"] != partition["chain"]:
            reasons.append(
                f"{label}: node {node} reports another chain for partition "
                f"{partition['key']} at the unchanged count {partition['count']} after "
                f"{marker} at seq {marker_seq}"
            )


def reconcile_q2_raft(records: list[dict], *, require_conclusion: bool = True) -> dict:
    """Independent correctness oracle for the Q2 Raft acceptance journals.

    The journal is written by the parent harness from externally observed node output
    only: the process lifecycle, the applied/recovered state each node printed, the
    results a client received, the quorum it observed, the corruption it injected and
    its own classification of an outcome. The oracle never trusts a broker's claim that
    a command committed: a committed result needs a majority of distinct voters that
    report the same committed command from their own state.

    A broken safety invariant is FAIL, missing or contradictory evidence is INVALID and
    a journal without scenario or cluster evidence is NOT_RUN. A journal whose own
    conclusion is not PASS is never reported as PASS. Nothing is inferred from a clock.

    The journals record one latest chain digest per partition, so a chain can only be
    compared when two observations report the same count. Across a restart or a
    snapshot the count must never decrease, and at an unchanged count the chain must be
    identical. An extended count yields a different chain by construction and is not a
    divergence. The same limitation means a state observation that carries no request
    identity cannot prove which proposal it contains; such a journal is INVALID rather
    than a pass, because the majority acknowledgement is then unverifiable.
    """
    reasons: list[str] = []
    if require_conclusion and (not records or records[-1].get("event") != "conclusion"):
        return _result(INVALID, ["missing conclusion"], {})
    # The journal order is the observation order: a fixture without an explicit
    # sequence number keeps the position it was written at.
    records = [
        record if record.get("seq") is not None else {**record, "seq": index}
        for index, record in enumerate(records)
    ]

    invalid: list[str] = []

    # 0. The scenario's own conclusion must be a success. A scenario that concluded
    # FAIL, INVALID or NOT_RUN can never be reported as a successful safety verdict.
    conclusions = [r for r in records if r.get("event") == "conclusion"]
    if conclusions:
        conclusion = conclusions[-1]
        own_verdict = conclusion.get("verdict")
        if own_verdict != PASS:
            detail = conclusion.get("reason")
            if detail:
                reasons.append(f"the scenario concluded {own_verdict}: {detail}")
            else:
                reasons.append(f"the scenario concluded {own_verdict}")

    started = [r for r in records if r.get("event") == "scenario_started"]
    clusters = [r for r in records if r.get("event") == "cluster_started"]
    if not started and not clusters and not reasons:
        return _result(NOT_RUN, ["no Q2 Raft scenario evidence"], {})

    # The journal is grouped by scenario; a record before the first scenario_started
    # belongs to the journal itself so that no evidence is silently dropped.
    groups: list[tuple[object, list[dict]]] = []
    current: list[dict] | None = None
    for record in records:
        if record.get("event") == "scenario_started":
            current = [record]
            groups.append((record.get("scenario", len(groups)), current))
        elif current is None:
            current = [record]
            groups.append((None, current))
        else:
            current.append(record)

    counts = {
        "scenarios": 0,
        "observations": 0,
        "kills": 0,
        "restarts": 0,
        "majority_proofs": 0,
        "snapshots": 0,
    }
    state_total = 0
    proposal_total = 0

    for scenario_key, group in groups:
        label = (
            f"the scenario {scenario_key}" if scenario_key is not None else "the journal"
        )
        counts["scenarios"] += 1
        states = [_q2_state(r) for r in group if r.get("event") in Q2_STATE_EVENTS]
        proposals = [r for r in group if r.get("event") == "proposal_result"]
        kills = [r for r in group if r.get("event") == "process_killed"]
        restarts = [r for r in group if r.get("event") == "process_restarted"]
        quorums = [r for r in group if r.get("event") == "quorum_observed"]
        outcomes = [r for r in group if r.get("event") == "outcome_classified"]
        corruptions = [r for r in group if r.get("event") == "corruption_injected"]
        snapshots = [
            r for r in group
            if r.get("event") in ("snapshot_installed", "snapshot_observed")
        ]
        group_conclusions = [r for r in group if r.get("event") == "conclusion"]
        counts["observations"] += len(states)
        counts["kills"] += len(kills)
        counts["restarts"] += len(restarts)
        counts["snapshots"] += len(snapshots)
        state_total += len(states)
        proposal_total += len(proposals)

        # 1. A committed applied state needs a majority of distinct voters that report
        # the same (applied_index, chain) for the same partition. One node's committed
        # index is a claim, never a majority proof.
        committed_states = [
            state
            for state in states
            if state["event"] == "applied_state"
            and state["applied_index"] is not None
            and any(
                entry["key"] is not None
                and entry["count"] not in (None, 0)
                for entry in state["partitions"]
            )
        ]
        proofs: set[tuple] = set()
        for state in committed_states:
            for partition in state["partitions"]:
                if partition["key"] is None or partition["count"] in (None, 0):
                    continue
                witnesses = {
                    other["node"]
                    for other in committed_states
                    if other["applied_index"] == state["applied_index"]
                    and any(
                        entry["key"] == partition["key"]
                        and entry["chain"] == partition["chain"]
                        for entry in other["partitions"]
                    )
                }
                if len(witnesses) < MAJORITY:
                    reasons.append(
                        f"{label}: node {state['node']} records a committed applied "
                        f"state at index {state['applied_index']} for partition "
                        f"{partition['key']} supported by {len(witnesses)} voter(s) "
                        f"{sorted(map(str, witnesses))}; {MAJORITY} voters must report "
                        "the same index and chain"
                    )
                else:
                    proofs.add(
                        (
                            state["applied_index"],
                            partition["key"],
                            repr(partition["chain"]),
                        )
                    )
        counts["majority_proofs"] += len(proofs)

        # 2. A committed command never disappears across a restart of the same node.
        by_node: dict[object, list[dict]] = {}
        for state in states:
            by_node.setdefault(state["node"], []).append(state)
        for node, observations in by_node.items():
            for restart in restarts:
                if restart.get("node") != node:
                    continue
                before = [s for s in observations if s["seq"] < restart["seq"]]
                after = [s for s in observations if s["seq"] > restart["seq"]]
                if not before or not after:
                    continue
                for later in after:
                    _q2_state_regression(
                        label, "its restart", node, restart["seq"], before[-1],
                        later, reasons,
                    )

        # 3a. Two nodes that report the same count must report the same committed
        # chain; a divergent committed prefix is a safety violation.
        chains: dict[tuple, set] = {}
        for state in states:
            for partition in state["partitions"]:
                if partition["key"] is None or partition["count"] in (None, 0):
                    continue
                chains.setdefault(
                    (partition["key"], partition["count"]), set()
                ).add((state["node"], repr(partition["chain"])))
        for (key, count), observed in chains.items():
            if len({chain for _, chain in observed}) > 1:
                reasons.append(
                    f"{label}: divergent committed chains for partition {key} at count "
                    f"{count}: "
                    f"{sorted((str(node), chain) for node, chain in observed)}"
                )

        # 3b/4. A committed result and an `ok` acknowledgement both need a majority of
        # distinct voters that later report the request id in their committed state.
        carrying = any(_q2_state_request_ids(state) for state in states)
        unverifiable_reported = False
        for proposal in proposals:
            if proposal.get("ok") is not True:
                continue
            ids = _q2_request_ids(proposal.get("request_id"))
            if not ids:
                invalid.append(
                    f"{label}: a committed proposal at seq {proposal['seq']} carries "
                    "no request identity"
                )
                continue
            if not carrying:
                if not unverifiable_reported:
                    invalid.append(
                        f"{label}: the state observations carry no request identity, "
                        "so the committed acknowledgement cannot be verified"
                    )
                    unverifiable_reported = True
                continue
            holders = {
                state["node"]
                for state in states
                if state["seq"] > proposal["seq"]
                and ids & _q2_state_request_ids(state)
            }
            if len(holders) < MAJORITY:
                reasons.append(
                    f"{label}: the committed result of request {sorted(ids)[0]} is "
                    f"reported by {len(holders)} node(s) {sorted(map(str, holders))}; "
                    f"{MAJORITY} voters must carry the committed command"
                )

        # 5. An uncommitted tail is never presented as committed: a request the harness
        # classified as not committed may not appear in any later committed state.
        for outcome in outcomes:
            if _q2_committed_classification(outcome):
                continue
            ids = _q2_request_ids(outcome.get("request_id")) | _q2_request_ids(
                outcome.get("request_ids")
            )
            if not ids:
                continue
            for state in states:
                if state["seq"] <= outcome["seq"]:
                    continue
                hits = ids & _q2_state_request_ids(state)
                if hits:
                    reasons.append(
                        f"{label}: request {sorted(hits)[0]} was classified "
                        f"{_q2_classification(outcome) or 'not committed'} but node "
                        f"{state['node']} later reports it in its committed state"
                    )

        # 6. Injected corruption is never silently ignored: its outcome must have been
        # observed. A PASS conclusion additionally needs applied state evidence.
        for corruption in corruptions:
            later = [o for o in outcomes if o["seq"] > corruption["seq"]]
            if not later:
                invalid.append(
                    f"{label}: the corruption outcome was never observed (node "
                    f"{corruption.get('node')}, seq {corruption['seq']})"
                )
        if group_conclusions and group_conclusions[-1].get("verdict") == PASS:
            if not any(state["event"] == "applied_state" for state in states):
                invalid.append(
                    f"{label}: concluded PASS without any applied state observation"
                )

        # 7. A snapshot never changes a previously committed identity: the count must
        # not decrease and an unchanged count keeps its chain.
        for snap in snapshots:
            node = snap.get("node")
            before = [s for s in states if s["node"] == node and s["seq"] < snap["seq"]]
            after = [s for s in states if s["node"] == node and s["seq"] > snap["seq"]]
            if not before or not after:
                continue
            for later in after:
                _q2_state_regression(
                    label, "its snapshot", node, snap["seq"], before[-1], later, reasons
                )

        # 8. A commit never happens while fewer than a majority of voters are
        # available. Availability is an observation of the current cluster, so the
        # latest quorum record before the commit is the one that governs it: a later
        # restart that restored the majority clears the earlier outage.
        for proposal in proposals:
            if proposal.get("ok") is not True:
                continue
            observed_quorums = [q for q in quorums if q["seq"] < proposal["seq"]]
            if not observed_quorums:
                continue
            quorum = observed_quorums[-1]
            available = _q2_int(quorum.get("available_nodes"))
            if available is None or available >= MAJORITY:
                continue
            reasons.append(
                f"{label}: request {proposal.get('request_id')} committed at seq "
                f"{proposal['seq']} while only {available} voter(s) were available "
                f"at seq {quorum['seq']}"
            )

        # 9. The cluster and the shard identity never change. An observation that
        # carries no identity cannot contradict one; a recorded change is a failure.
        identities = []
        for record in group:
            if record.get("event") == "cluster_started" or (
                record.get("event") in Q2_STATE_EVENTS
                and (
                    record.get("cluster_id") is not None
                    or record.get("shard_id") is not None
                )
            ):
                identities.append(
                    (record.get("cluster_id"), record.get("shard_id"), record["seq"])
                )
        baseline = next(
            ((c, s) for c, s, _ in identities if c is not None or s is not None),
            None,
        )
        if identities and baseline is None:
            invalid.append(f"{label}: no cluster or shard identity was recorded")
        elif baseline is not None:
            for cluster_id, shard_id, seq in identities:
                if (
                    cluster_id is not None
                    and baseline[0] is not None
                    and _q2_int(cluster_id) != _q2_int(baseline[0])
                ):
                    reasons.append(
                        f"{label}: the cluster identity changed from {baseline[0]} to "
                        f"{cluster_id} at seq {seq}"
                    )
                if (
                    shard_id is not None
                    and baseline[1] is not None
                    and _q2_int(shard_id) != _q2_int(baseline[1])
                ):
                    reasons.append(
                        f"{label}: the shard identity changed from {baseline[1]} to "
                        f"{shard_id} at seq {seq}"
                    )

    reasons = list(dict.fromkeys(reasons))
    invalid = list(dict.fromkeys(invalid))
    if reasons:
        return _result(FAIL, reasons, counts)
    if invalid:
        return _result(INVALID, invalid, counts)
    if state_total == 0 and proposal_total == 0:
        return _result(NOT_RUN, ["no Q2 Raft state evidence"], counts)
    return _result(PASS, [], counts)
