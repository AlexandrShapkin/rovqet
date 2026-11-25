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


Q2_CONSUMER_REQUIRED = {
    "normal", "sparse", "fencing", "idempotent", "quorum",
    "follower_restart", "full_restart", "crash_before_proposal",
    "crash_before_majority", "crash_after_majority", "negative", "snapshot_install",
}


def _consumer_token(token: dict) -> tuple:
    """Canonical full delivery identity, independently normalized from voter state."""
    token = token.get("GroupAck", token)
    partition = token.get("partition", token)
    fields = [partition.get(k) for k in ("tenant_id", "topic_id", "partition_id")]
    fields += [token.get(k) for k in (
        "incarnation", "group_id", "generation", "lease_id", "metadata_epoch",
        "offset", "attempt", "principal", "message_id",
    )]
    if not all(isinstance(v, int) and not isinstance(v, bool) and v >= 0 for v in fields):
        raise ValueError("incomplete Q2 consumer identity")
    return tuple(fields)


def reconcile_q2_consumer(records: list[dict], *, require_conclusion: bool = True) -> dict:
    """Reconcile external SDK observations against distinct persisted Raft voters.

    No application assertion or scenario PASS is itself durability evidence.
    Exact saved tokens in two independent applied-state files establish a majority;
    final recovered files must retain every successful ACK. Missing coverage is
    never accepted, even when the campaign claims PASS.
    """
    failures, invalid = [], []
    conclusions = [r for r in records if r.get("event") == "conclusion"]
    own = conclusions[-1].get("verdict") if conclusions else None
    if require_conclusion and (own is None or not records or records[-1].get("event") != "conclusion"):
        invalid.append("missing campaign conclusion")
    if own == INVALID or own not in (None, PASS, FAIL, INVALID, NOT_RUN):
        invalid.append("campaign concluded INVALID or with an unknown verdict")
    scenarios = {r.get("name"): r.get("verdict") for r in records
                 if r.get("event") == "consumer_scenario"}
    missing = Q2_CONSUMER_REQUIRED - scenarios.keys()
    incomplete = missing | {n for n, v in scenarios.items() if v != PASS}
    if own == PASS and incomplete:
        failures.append("incomplete consumer campaign reported as PASS")
    if any(r.get("verdict") == FAIL for r in records if r.get("event") == "consumer_scenario") or own == FAIL:
        failures.append("consumer scenario failed")
    configurations = [r for r in records if r.get("event") == "consumer_configuration"]
    if not configurations or any(r.get("q1_configured") is not False for r in configurations):
        invalid.append("missing proof that Q2 consumers run without Q1")
    attempts, delivered, successes = set(), set(), []
    witnesses, recovered = {}, {}
    previous = {}
    majority_proofs = 0
    for record in records:
        event = record.get("event")
        try:
            if event in ("consumer_delivery", "consumer_attempt", "consumer_result"):
                key = _consumer_token(record["token"])
                if event == "consumer_delivery":
                    delivered.add(key)
                elif event == "consumer_attempt":
                    attempts.add(key)
                elif record.get("result", {}).get("outcome") == "ack_committed":
                    result = record["result"]
                    if key not in attempts or key not in delivered:
                        failures.append("fabricated ACK success without an attempted delivered token")
                    if (result.get("offset"), result.get("generation")) != (key[8], key[5]):
                        failures.append("ACK result contradicts its delivery identity")
                    successes.append((key, record))
            elif event == "consumer_state":
                node, index = record["node"], record["applied_index"]
                if node not in (1, 2, 3) or not isinstance(index, int):
                    raise ValueError("missing voter identity/index")
                partition = record["partition"]
                messages = {m["logical_offset"]: m["message_id"] for m in partition["messages"]}
                old_groups = {scope[1] for scope in previous if scope[0] == node}
                if not old_groups.issubset(partition.get("groups", {}).keys()):
                    failures.append("recovery erased a previously observed group")
                for group_id, group in partition.get("groups", {}).items():
                    cursor, generation = group["cursor"], group["generation"]
                    outcomes = group["outcomes"]
                    offsets = {int(o) for o in outcomes}
                    if cursor < 0 or cursor > partition["next_logical_offset"] or generation != partition["group_generation"]:
                        failures.append("invalid group fence or cursor")
                    if len([o for o in offsets if o < cursor]) != cursor:
                        failures.append("cursor skipped an incomplete offset")
                    if set(group["sparse_acks"]) != {o for o in offsets if o >= cursor} or cursor in group["sparse_acks"]:
                        failures.append("sparse ACK set contradicts contiguous cursor")
                    scope = (node, group_id)
                    old = previous.get(scope)
                    if old and (index < old[0] or generation < old[1] or cursor < old[2]
                                or not old[3].issubset(offsets)):
                        failures.append("generation/cursor/ACK state regressed across recovery")
                    previous[scope] = (index, generation, cursor, offsets)
                    for offset, saved in outcomes.items():
                        key = _consumer_token(saved["command"])
                        outcome = saved["outcome"]["GroupAcked"]
                        if (key[0:3] != (1, 4, 0) or key[3] != partition["incarnation"]
                            or key[4] != int(group_id) or key[8] != int(offset)
                            or messages.get(int(offset)) != key[11]
                            or key[5] > generation or outcome["generation"] != key[5]
                            or outcome["offset"] != key[8] or outcome["group_id"] != key[4]):
                            failures.append("saved ACK contradicts committed record/token")
                        witnesses.setdefault(key, set()).add(node)
                        if record.get("recovered"):
                            recovered.setdefault(key, set()).add(node)
            elif event == "consumer_rejection_check":
                if record.get("before") != record.get("after"):
                    failures.append("rejected stale/invalid ACK mutated group progress")
            elif event == "consumer_duplicate_check":
                if record.get("before") != record.get("after"):
                    failures.append("identical saved ACK retry caused another mutation")
            elif event == "consumer_redelivery":
                if record.get("offset") not in record.get("delivered_offsets", []):
                    failures.append("unacknowledged record was not redelivered")
            elif event == "consumer_snapshot_install":
                if not (record["leader_purged_index"] > record["before_index"]
                        and record["after_index"] >= record["leader_purged_index"]
                        and record["before_groups"] == record["after_groups"]):
                    failures.append("snapshot installation erased consumer progress or lacks a log-gap proof")
        except (KeyError, TypeError, ValueError) as error:
            invalid.append(f"invalid {event} evidence: {error}")
    for key, record in successes:
        if len(witnesses.get(key, set())) < 2:
            failures.append("ACK success has no distinct durable majority evidence")
        else:
            majority_proofs += 1
        if recovered.get(key, set()) != {1, 2, 3}:
            failures.append("ACK success is missing recovered ACK state")
        if len(record.get("live", [])) < 2 and not any(
            other_key == key and len(other.get("live", [])) >= 2
            and other.get("seq", 0) < record.get("seq", 0)
            for other_key, other in successes
        ):
            failures.append("new ACK success without an available majority")
    for stage in ("before_proposal", "before_majority", "after_majority"):
        if scenarios.get("crash_" + stage) != PASS:
            continue
        barriers = [r for r in records if r.get("event") == "consumer_barrier" and r.get("stage") == stage]
        kills = [r for r in records if r.get("event") == "consumer_barrier_kill" and r.get("stage") == stage]
        recoveries = [r for r in records if r.get("event") == "consumer_crash_recovered" and r.get("stage") == stage]
        if len(barriers) != 1 or len(kills) != 1 or len(recoveries) != 1:
            invalid.append(f"missing deterministic {stage} barrier/kill/recovery evidence")
            continue
        barrier, kill, recovery = barriers[0], kills[0], recoveries[0]
        try:
            key = _consumer_token(barrier["token"])
            document = barrier["barrier"]
            if (_consumer_token(document["command"]) != key
                or document["stage"] != stage or document["node"] != kill["node"]
                or kill["signal"] != "SIGKILL"
                or not barrier["seq"] < kill["seq"] < recovery["seq"]):
                failures.append("crash barrier identity/order does not match the killed leader")
            before_kill = set()
            for state in records:
                if state.get("event") != "consumer_state" or not barrier["seq"] < state["seq"] < kill["seq"]:
                    continue
                for group in state["partition"].get("groups", {}).values():
                    for saved in group["outcomes"].values():
                        if _consumer_token(saved["command"]) == key:
                            if stage == "after_majority" and state["applied_index"] >= document["committed_index"]:
                                before_kill.add(state["node"])
                            elif stage != "after_majority":
                                failures.append("pre-commit barrier already carried a committed ACK")
            if stage == "after_majority" and len(before_kill) < 2:
                failures.append("post-majority kill lacks a real persisted majority before the kill")
            if stage == "before_majority" and document["durable_index"] < document["log_index"]:
                failures.append("pre-majority barrier preceded local fsync")
            interrupted = [r for r in records if r.get("event") == "consumer_result"
                and kill["seq"] < r["seq"] < recovery["seq"]
                and _consumer_token(r["token"]) == key
                and r["result"].get("outcome") == "error"
                and "OutcomeUnknown" in r["result"].get("detail", "")]
            if not interrupted:
                invalid.append("held ACK response was not externally observed as uncertain")
        except (KeyError, ValueError, TypeError) as error:
            invalid.append(f"invalid crash evidence: {error}")
    counts = {"successful_acks": len(successes), "majority_proofs": majority_proofs,
              "scenario_passes": sum(v == PASS for v in scenarios.values())}
    if failures:
        return _result(FAIL, sorted(set(failures)), counts)
    if invalid:
        return _result(INVALID, sorted(set(invalid)), counts)
    if incomplete or own == NOT_RUN:
        return _result(NOT_RUN, ["required scenarios not executed: " + ", ".join(sorted(incomplete))], counts)
    if not successes:
        return _result(INVALID, ["no external successful ACK observations"], counts)
    return _result(PASS, [], counts)


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


# --- Q2 publication (C-04) -------------------------------------------------------
#
# A Q2 publication journal is written by the parent campaign from two observations it
# owns: the JSON line the SDK example printed for each attempt, and a copy of each
# voter's committed `state.json` read from that voter's own data directory after the
# attempt. The oracle never derives a commit from the broker's own startup line or from
# a proposal being sent: a COMMITTED_QUORUM outcome needs a majority of distinct voters
# whose committed state contains the acknowledged message.

Q2_PUBLISH_MAJORITY = 2
# The C-04 campaign's own journal identity and the scenarios it must have executed
# before its conclusion may be accepted as PASS. An incomplete campaign is never a
# PASS, whatever the rest of the evidence says.
Q2_PUBLISH_RUN = "c04-q2-publish"
Q2_PUBLISH_REQUIRED_SCENARIOS = frozenset(range(1, 15))


def _q2_hex(value) -> str | None:
    """A digest as lowercase hex, from a hex string or a list of bytes."""
    if value is None:
        return None
    if isinstance(value, str):
        return value.lower()
    if isinstance(value, (bytes, bytearray, list, tuple)):
        try:
            return bytes(value).hex()
        except (TypeError, ValueError):
            return None
    return None


def _q2_payload_hex(value) -> str | None:
    """A committed payload as lowercase hex, or None when it is absent entirely.

    None (no payload observation) is deliberately distinct from an empty payload,
    which serialises as the empty string. The campaign journals the payload bytes
    explicitly so a digest alone can never stand in for the message it describes.
    """
    if value is None:
        return None
    if isinstance(value, str):
        return value.strip().lower()
    if isinstance(value, (bytes, bytearray, list, tuple)):
        try:
            return bytes(value).hex()
        except (TypeError, ValueError):
            return None
    return None


def _q2_payload_bytes(value: str | None) -> bytes | None:
    """Decode a hex payload observation back into the exact committed bytes."""
    if not isinstance(value, str) or len(value) % 2 != 0:
        return None
    try:
        return bytes.fromhex(value)
    except ValueError:
        return None


def _q2_publish_key(record: dict):
    producer = _q2_int(record.get("producer", record.get("producer_id")))
    epoch = _q2_int(record.get("epoch", record.get("producer_epoch")))
    sequence = _q2_int(record.get("sequence"))
    if producer is None or epoch is None or sequence is None:
        return None
    return (producer, epoch, sequence)


def _q2_publish_messages(record: dict) -> dict[int, dict]:
    """The retained messages of one voter state keyed by logical offset."""
    messages: dict[int, dict] = {}
    for entry in record.get("messages") or []:
        if not isinstance(entry, dict):
            continue
        offset = _q2_int(entry.get("offset", entry.get("logical_offset")))
        if offset is None:
            continue
        messages[offset] = {
            "offset": offset,
            "message_id": _q2_int(entry.get("message_id")),
            "digest": _q2_hex(entry.get("digest", entry.get("payload_digest"))),
            "payload": _q2_payload_hex(entry.get("payload", entry.get("payload_hex"))),
            "producer": _q2_int(entry.get("producer", entry.get("producer_id"))),
            "epoch": _q2_int(entry.get("epoch", entry.get("producer_epoch"))),
            "sequence": _q2_int(entry.get("sequence")),
        }
    return messages


def _q2_publish_outcomes(record: dict) -> dict:
    """The retained producer outcomes of one voter state keyed by producer token."""
    outcomes: dict[tuple, dict] = {}
    for entry in record.get("outcomes") or []:
        if not isinstance(entry, dict):
            continue
        key = _q2_publish_key(entry)
        if key is None:
            continue
        offset = _q2_int(entry.get("offset", entry.get("logical_offset")))
        if offset is None:
            continue
        outcomes[key] = {
            "offset": offset,
            "message_id": _q2_int(entry.get("message_id")),
            "digest": _q2_hex(entry.get("digest", entry.get("payload_digest"))),
        }
    return outcomes


def _q2_publish_states(records: list[dict]) -> list[dict]:
    states = []
    for record in records:
        if record.get("event") != "voter_state":
            continue
        states.append(
            {
                "seq": record.get("seq"),
                "node": record.get("node"),
                "applied_index": _q2_int(record.get("applied_index")),
                "partition": _q2_partition_key(record.get("partition")),
                "count": _q2_int(record.get("count")),
                "chain": _q2_hex(record.get("chain", record.get("chain_digest"))),
                "next_logical_offset": _q2_int(record.get("next_logical_offset")),
                "messages": _q2_publish_messages(record),
                "outcomes": _q2_publish_outcomes(record),
            }
        )
    return states


def _q2_publish_state_consistency(
    states: list[dict], reasons: list[str], invalid: list[str]
) -> None:
    """Per-node monotonicity and cross-node agreement of committed state."""
    by_node: dict[object, list[dict]] = {}
    for state in states:
        if state["node"] is None or state["partition"] is None or state["count"] is None:
            invalid.append(
                f"voter state at seq {state['seq']} has no node, partition or committed "
                "count"
            )
            continue
        by_node.setdefault(state["node"], []).append(state)
        if state["next_logical_offset"] is not None:
            for offset in state["messages"]:
                if offset >= state["next_logical_offset"]:
                    reasons.append(
                        f"node {state['node']} reports message offset {offset} beyond "
                        f"its next logical offset {state['next_logical_offset']}"
                    )
    for node, observations in by_node.items():
        previous_by_partition: dict[str, dict] = {}
        for state in sorted(observations, key=lambda item: item["seq"] or 0):
            previous = previous_by_partition.get(state["partition"])
            if previous is not None:
                if state["count"] < previous["count"]:
                    reasons.append(
                        f"node {node} lost committed publications on partition "
                        f"{state['partition']}: count {previous['count']} -> "
                        f"{state['count']}"
                    )
                elif (
                    state["count"] == previous["count"]
                    and state["chain"] != previous["chain"]
                ):
                    reasons.append(
                        f"node {node} reports another committed chain for partition "
                        f"{state['partition']} at the unchanged count {state['count']}"
                    )
                if (
                    state["next_logical_offset"] is not None
                    and previous["next_logical_offset"] is not None
                    and state["next_logical_offset"] < previous["next_logical_offset"]
                ):
                    reasons.append(
                        f"node {node} moved its next logical offset backwards on "
                        f"partition {state['partition']}: "
                        f"{previous['next_logical_offset']} -> "
                        f"{state['next_logical_offset']}"
                    )
            previous_by_partition[state["partition"]] = state
    # A retained payload is immutable: a later observation (for example after a
    # restart or catch-up) must reproduce the exact bytes an earlier observation
    # already reported, and may not silently drop them.
    payloads: dict[tuple, str | None] = {}
    for state in states:
        if state["node"] is None or state["partition"] is None:
            continue
        for offset, message in state["messages"].items():
            identity = (
                state["node"],
                state["partition"],
                offset,
                message["message_id"],
            )
            if identity not in payloads:
                payloads[identity] = message["payload"]
                continue
            previous_payload = payloads[identity]
            if previous_payload is not None and message["payload"] is None:
                reasons.append(
                    f"node {state['node']} no longer reports the payload of offset "
                    f"{offset} on partition {state['partition']} that it reported before"
                )
            elif (
                previous_payload is not None
                and message["payload"] is not None
                and previous_payload != message["payload"]
            ):
                reasons.append(
                    f"node {state['node']} reports different payload bytes for offset "
                    f"{offset} on partition {state['partition']} than it reported before"
                )
    chains: dict[tuple, set] = {}
    for state in states:
        if state["partition"] is None or state["count"] is None or state["chain"] is None:
            continue
        chains.setdefault((state["partition"], state["count"]), set()).add(state["chain"])
    for (partition, count), observed in chains.items():
        if len(observed) > 1:
            reasons.append(
                f"voters disagree about the committed chain of partition {partition} "
                f"at count {count}: {sorted(observed)}"
            )


def reconcile_q2_publish(
    records: list[dict], *, require_conclusion: bool = True
) -> dict:
    """Independent oracle for the C-04 Q2 publication campaign.

    A COMMITTED_QUORUM outcome is accepted only when at least two distinct voters'
    committed state (read from their own data directories by the parent) contains the
    acknowledged message at the acknowledged logical offset with the same message
    identity, the same exact payload bytes (whose digest must equal the acknowledged
    digest), and the same applied index and partition chain. Only a voter state whose
    node, partition, applied index, committed count, chain and message identity are
    all present may enter the quorum grouping: a missing witness field is INVALID and
    never forms a majority by comparing missing values against each other. A vote that
    carries the digest but no payload can never prove what was recovered: it is
    INVALID, while a payload whose bytes contradict the digest or the attempted
    payload is FAIL. A retry of the same producer token must report the original
    offset and must not create a second message. A definite rejection must leave the
    committed state untouched: a rejected producer token may not be acknowledged, may
    not retain an outcome and may not retain a committed message, whatever the
    message identity says, and the same and the different identity are reported
    distinctly. An uncertain outcome is recorded as uncertain, never as a commit or
    as a failure.

    A safety violation is FAIL, missing or contradictory evidence is INVALID, and a
    journal with no committed publication at all is NOT_RUN. A journal whose own
    conclusion is not PASS is never reported as PASS.
    """
    reasons: list[str] = []
    invalid: list[str] = []
    if require_conclusion and (not records or records[-1].get("event") != "conclusion"):
        return _result(INVALID, ["missing conclusion"], {})
    records = [
        record if record.get("seq") is not None else {**record, "seq": index}
        for index, record in enumerate(records)
    ]
    conclusions = [record for record in records if record.get("event") == "conclusion"]
    own_verdict = conclusions[-1].get("verdict") if conclusions else None
    if conclusions and own_verdict != PASS:
        detail = conclusions[-1].get("reason")
        reasons.append(
            f"the scenario concluded {own_verdict}"
            + (f": {detail}" if detail else "")
        )

    started = [record for record in records if record.get("event") == "scenario_started"]
    not_run_required = [
        record
        for record in records
        if record.get("event") == "scenario_not_run"
        and record.get("required", True) is not False
    ]
    # One producer token may be attempted several times with the same identity (the
    # idempotent retry) and, deliberately, once with another identity (the sequence
    # conflict fixture). Every attempt is kept: a commitment has to match one of them.
    attempts: dict[tuple, list[dict]] = {}
    for record in records:
        if record.get("event") != "publication_attempt":
            continue
        key = _q2_publish_key(record)
        if key is None:
            invalid.append(f"attempt at seq {record.get('seq')} has no producer token")
            continue
        if record.get("digest") is None or _q2_hex(record.get("digest")) is None:
            invalid.append(f"attempt {key} has no payload digest")
            continue
        if _q2_int(record.get("message_id")) is None:
            invalid.append(f"attempt {key} has no message id")
            continue
        attempts.setdefault(key, []).append(record)

    def matching_attempt(key, record):
        for attempt in attempts.get(key, []):
            if (
                _q2_int(attempt.get("message_id")) == _q2_int(record.get("message_id"))
                and _q2_hex(attempt.get("digest")) == _q2_hex(record.get("digest"))
                and _q2_int(attempt.get("incarnation"))
                == _q2_int(record.get("incarnation"))
            ):
                return attempt
        return None

    states = _q2_publish_states(records)
    _q2_publish_state_consistency(states, reasons, invalid)
    committed: list[dict] = []
    rejected: list[dict] = []
    unknown: list[dict] = []
    for record in records:
        event = record.get("event")
        if event not in (
            "publication_committed",
            "publication_rejected",
            "publication_unknown",
        ):
            continue
        key = _q2_publish_key(record)
        offset = _q2_int(record.get("offset"))
        outcome = record.get("outcome")
        if event == "publication_committed":
            if outcome != "committed_quorum":
                reasons.append(
                    f"a committed publication at seq {record.get('seq')} reports "
                    f"outcome {outcome!r}; a Q2 commit must be committed_quorum"
                )
            if key is None:
                invalid.append(
                    f"committed publication at seq {record.get('seq')} has no producer token"
                )
                continue
            if key not in attempts:
                invalid.append(
                    f"committed publication {key} has no matching attempt observation"
                )
                continue
            attempt = matching_attempt(key, record)
            if attempt is None:
                reasons.append(
                    f"committed publication {key} acknowledges a message identity "
                    "that was never attempted"
                )
                continue
            if offset is None:
                invalid.append(f"committed publication {key} has no logical offset")
                continue
            committed.append(record)
        elif event == "publication_rejected":
            if key is None:
                invalid.append(
                    f"rejection at seq {record.get('seq')} has no producer token"
                )
            else:
                if not record.get("code"):
                    invalid.append(f"rejection {key} has no error code")
                rejected.append(record)
        else:
            if outcome == "committed_quorum" or offset is not None:
                reasons.append(
                    f"an uncertain outcome at seq {record.get('seq')} claims a commit"
                )
            if key is None:
                invalid.append(
                    f"uncertain outcome at seq {record.get('seq')} has no producer token"
                )
            else:
                unknown.append(record)

    # Idempotency and offset allocation.
    ack_by_key: dict[tuple, list[dict]] = {}
    ack_by_offset: dict[int, dict] = {}
    ack_by_message: dict[int, dict] = {}
    for ack in committed:
        key = _q2_publish_key(ack)
        offset = _q2_int(ack["offset"])
        ack_by_key.setdefault(key, []).append(ack)
        previous = ack_by_offset.get(offset)
        if previous is not None and _q2_publish_key(previous) != key:
            reasons.append(
                f"logical offset {offset} was acknowledged for two different producer "
                f"tokens {_q2_publish_key(previous)} and {key}"
            )
        else:
            ack_by_offset[offset] = ack
        message_id = _q2_int(ack.get("message_id"))
        previous_message = ack_by_message.get(message_id)
        if previous_message is not None and _q2_int(previous_message["offset"]) != offset:
            reasons.append(
                f"message id {message_id} was committed at two logical offsets "
                f"{_q2_int(previous_message['offset'])} and {offset}"
            )
        else:
            ack_by_message[message_id] = ack
    for key, acks in ack_by_key.items():
        offsets = {_q2_int(ack["offset"]) for ack in acks}
        if len(offsets) > 1:
            reasons.append(
                f"retries of producer token {key} reported different logical offsets "
                f"{sorted(offsets)}"
            )
        message_ids = {_q2_int(ack.get("message_id")) for ack in acks}
        if len(message_ids) > 1:
            reasons.append(
                f"retries of producer token {key} changed the message id: "
                f"{sorted(message_ids)}"
            )
    sequences: dict[tuple, list[int]] = {}
    for key in ack_by_key:
        sequences.setdefault((key[0], key[1]), []).append(key[2])
    for token, accepted in sequences.items():
        ordered = sorted(set(accepted))
        if ordered != list(range(ordered[0], ordered[-1] + 1)):
            reasons.append(
                f"producer {token} committed a sequence gap: accepted {ordered}"
            )
        offsets = [
            _q2_int(ack_by_key[(token[0], token[1], sequence)][0]["offset"])
            for sequence in ordered
        ]
        if offsets != sorted(offsets) or len(set(offsets)) != len(offsets):
            reasons.append(
                f"producer {token} does not map its committed sequences to strictly "
                f"increasing logical offsets: {offsets}"
            )
    for rejection in rejected:
        key = _q2_publish_key(rejection)
        rejection_identity = (
            _q2_int(rejection.get("message_id")),
            _q2_hex(rejection.get("digest")),
        )
        for ack in ack_by_key.get(key, []):
            ack_identity = (
                _q2_int(ack.get("message_id")),
                _q2_hex(ack.get("digest")),
            )
            if ack_identity == rejection_identity:
                reasons.append(
                    f"producer token {key} was both committed and definitively "
                    "rejected with the same message identity"
                )
            else:
                reasons.append(
                    f"producer token {key} was both committed and definitively "
                    f"rejected with a different message identity: committed "
                    f"{ack_identity}, rejected {rejection_identity}"
                )

    # A commit needs a majority of distinct voters whose own state carries the message.
    majority_proofs = 0
    for ack in committed:
        key = _q2_publish_key(ack)
        offset = _q2_int(ack["offset"])
        message_id = _q2_int(ack.get("message_id"))
        digest = _q2_hex(ack.get("digest"))
        # The attempt the acknowledgment is bound to, used to compare the recovered
        # bytes with the bytes that were actually published.
        ack_attempt = matching_attempt(key, ack) or {}
        partition_ack = ack.get("partition")
        partition_ack = _q2_partition_key(partition_ack)
        witnesses: dict[tuple, set] = {}
        contradicted = False
        incomplete = False
        for state in states:
            if partition_ack is not None and state["partition"] != partition_ack:
                continue
            message = state["messages"].get(offset)
            outcome = state["outcomes"].get(key)
            if outcome is not None and outcome["offset"] != offset:
                reasons.append(
                    f"node {state['node']} retains outcome {key} at offset "
                    f"{outcome['offset']}, not the acknowledged {offset}"
                )
                contradicted = True
            if message is None:
                continue
            # A witness may only be counted when every field the grouping compares is
            # actually present. A missing field is missing evidence (INVALID), never a
            # witness identity of its own: `None` must not be grouped with other `None`
            # values to manufacture a majority.
            absent = ", ".join(
                f"no {label}"
                for label, value in (
                    ("node", state["node"]),
                    ("partition", state["partition"]),
                    ("applied index", state["applied_index"]),
                    ("committed count", state["count"]),
                    ("chain", state["chain"]),
                    ("message id", message["message_id"]),
                    ("payload digest", message["digest"]),
                )
                if value is None
            )
            if absent:
                invalid.append(
                    f"voter state at seq {state['seq']} cannot witness offset "
                    f"{offset} for {key}: {absent}"
                )
                incomplete = True
                continue
            if message["message_id"] != message_id or message["digest"] != digest:
                reasons.append(
                    f"node {state['node']} has a different message at acknowledged "
                    f"offset {offset} for {key}"
                )
                contradicted = True
                continue
            retained_payload = message["payload"]
            retained_bytes = _q2_payload_bytes(retained_payload)
            if retained_payload is None:
                invalid.append(
                    f"node {state['node']} retains offset {offset} for {key} without "
                    "the committed payload; the recovered bytes cannot be verified"
                )
                contradicted = True
                continue
            if retained_bytes is None:
                invalid.append(
                    f"node {state['node']} retains an undecodable payload for offset "
                    f"{offset} of {key}"
                )
                contradicted = True
                continue
            if digest_hex(retained_bytes) != digest:
                reasons.append(
                    f"node {state['node']} retains payload bytes at offset {offset} "
                    f"for {key} whose digest does not match the acknowledged {digest}"
                )
                contradicted = True
                continue
            attempted_payload = ack_attempt.get("payload")
            if (
                isinstance(attempted_payload, str)
                and retained_bytes != attempted_payload.encode()
            ):
                reasons.append(
                    f"node {state['node']} retains payload bytes at offset {offset} "
                    f"for {key} that differ from the attempted payload"
                )
                contradicted = True
                continue
            if (
                message["producer"] not in (None, key[0])
                or message["epoch"] not in (None, key[1])
                or message["sequence"] not in (None, key[2])
            ):
                reasons.append(
                    f"node {state['node']} attributes offset {offset} to another "
                    f"producer token than {key}"
                )
                contradicted = True
                continue
            witnesses.setdefault(
                (state["applied_index"], state["count"], state["chain"]), set()
            ).add(state["node"])
        best = max((len(nodes) for nodes in witnesses.values()), default=0)
        if contradicted or incomplete:
            # Missing or contradictory evidence: the quorum cannot be established
            # either way, and the recorded `invalid`/`reason` decides the verdict.
            continue
        if not states:
            invalid.append(
                f"committed publication {key} at offset {offset} has no voter state "
                "observation; the majority cannot be established"
            )
            continue
        if best < Q2_PUBLISH_MAJORITY:
            reasons.append(
                f"committed publication {key} at offset {offset} is present in the "
                f"committed state of {best} distinct voter(s); "
                f"{Q2_PUBLISH_MAJORITY} are required"
            )
        else:
            majority_proofs += 1
    # A definite rejection of a producer token may not survive anywhere in the
    # recovered state: neither a retained outcome nor a retained committed message of
    # that token may exist, whatever the message identity reports. The same and the
    # different identity are reported distinctly so the contradiction stays readable.
    for state in states:
        for key, outcome in state["outcomes"].items():
            for rejection in rejected:
                if _q2_publish_key(rejection) != key:
                    continue
                rejected_identity = (
                    _q2_int(rejection.get("message_id")),
                    _q2_hex(rejection.get("digest")),
                )
                retained_identity = (outcome["message_id"], outcome["digest"])
                if rejected_identity == retained_identity:
                    reasons.append(
                        f"node {state['node']} committed token {key} that another "
                        "observation reports as definitively rejected with the same "
                        "message identity"
                    )
                else:
                    reasons.append(
                        f"node {state['node']} committed token {key} that another "
                        "observation reports as definitively rejected with a "
                        f"different message identity: retained {retained_identity}, "
                        f"rejected {rejected_identity}"
                    )
        for rejection in rejected:
            key = _q2_publish_key(rejection)
            rejected_identity = (
                _q2_int(rejection.get("message_id")),
                _q2_hex(rejection.get("digest")),
            )
            for message in state["messages"].values():
                if (message["producer"], message["epoch"], message["sequence"]) != key:
                    continue
                retained_identity = (message["message_id"], message["digest"])
                if rejected_identity == retained_identity:
                    reasons.append(
                        f"node {state['node']} retains the committed message of "
                        f"token {key} that another observation reports as "
                        "definitively rejected with the same message identity"
                    )
                else:
                    reasons.append(
                        f"node {state['node']} retains the committed message of "
                        f"token {key} that another observation reports as "
                        "definitively rejected with a different message identity: "
                        f"retained {retained_identity}, rejected {rejected_identity}"
                    )

    # The post-majority barrier is an externally observed commit point: the armed
    # publication is already committed and applied by a majority, its response never
    # left the named node, and that node was killed afterwards. Every one of those
    # observations has to be present; a barrier without them is not evidence of the
    # "leader crash after majority before response" window.
    for barrier in (r for r in records if r.get("event") == "postmajority_barrier"):
        barrier_seq = _q2_int(barrier.get("seq")) or 0
        token = _q2_publish_key(barrier)
        offset = _q2_int(barrier.get("logical_offset"))
        committed_index = _q2_int(barrier.get("committed_index"))
        node = _q2_int(barrier.get("node"))
        if committed_index is None:
            invalid.append(
                f"the post-majority barrier at seq {barrier.get('seq')} carries no "
                "committed index"
            )
        if token is None or offset is None or node is None:
            invalid.append(
                f"the post-majority barrier at seq {barrier.get('seq')} has no "
                "producer token, logical offset or node"
            )
            continue
        kills = [
            record
            for record in records
            if record.get("event") == "broker_killed"
            and _q2_int(record.get("node")) == node
            and (_q2_int(record.get("seq")) or 0) > barrier_seq
        ]
        kill_seq = _q2_int(kills[0].get("seq")) if kills else None
        if kill_seq is None:
            reasons.append(
                f"the barrier node {node} was never SIGKILLed after the post-majority "
                f"barrier at seq {barrier.get('seq')}"
            )
        acknowledged_before_kill = [
            record
            for record in committed
            if _q2_publish_key(record) == token
            and kill_seq is not None
            and (_q2_int(record.get("seq")) or 0) < kill_seq
        ]
        if acknowledged_before_kill:
            reasons.append(
                f"producer token {token} was acknowledged at seq "
                f"{acknowledged_before_kill[0].get('seq')} before the barrier node "
                f"{node} was killed"
            )
        uncertainties = [
            record
            for record in records
            if record.get("event") == "publication_unknown"
            and _q2_publish_key(record) == token
            and (_q2_int(record.get("seq")) or 0) > barrier_seq
        ]
        if not uncertainties:
            reasons.append(
                f"no uncertainty record follows the post-majority barrier for "
                f"producer token {token}"
            )
        # The majority that predates the kill: only voter states observed after the
        # barrier and before the kill may witness the barrier's own commit point.
        pre_kill = (
            [
                state
                for state in states
                if state["seq"] is not None
                and barrier_seq < state["seq"] < kill_seq
            ]
            if kill_seq is not None
            else []
        )
        witness_nodes = {
            state["node"]
            for state in pre_kill
            if state["messages"].get(offset) is not None
            and state["messages"][offset]["message_id"]
            == _q2_int(barrier.get("message_id"))
        }
        if len(witness_nodes) < Q2_PUBLISH_MAJORITY:
            reasons.append(
                f"only {len(witness_nodes)} pre-kill voter state(s) carry the barrier "
                f"message of {token} at offset {offset}; {Q2_PUBLISH_MAJORITY} are "
                "required before the kill"
            )
        retries = [
            record
            for record in committed
            if _q2_publish_key(record) == token
            and _q2_int(record.get("offset")) == offset
            and kill_seq is not None
            and (_q2_int(record.get("seq")) or 0) > kill_seq
        ]
        if not retries:
            reasons.append(
                f"the retry of producer token {token} never committed at the barrier "
                f"logical offset {offset}"
            )

    # One producer token may own at most one retained message in a voter's committed
    # state: a second message for the same token is a duplicated publication and the
    # whole journal is rejected, not merely noted.
    for state in states:
        per_token: dict[tuple, int] = {}
        for message in state["messages"].values():
            key = (message["producer"], message["epoch"], message["sequence"])
            if key[0] is None or key[1] is None or key[2] is None:
                invalid.append(
                    f"node {state['node']} retains a message without a complete "
                    "producer token; duplication cannot be checked"
                )
                continue
            per_token[key] = per_token.get(key, 0) + 1
        for key, count in sorted(per_token.items()):
            if count > 1:
                reasons.append(
                    f"node {state['node']} retains {count} messages for the single "
                    f"producer token {key}"
                )

    # The C-04 campaign must have executed every required scenario: a journal of
    # this run that stops early can never be reported as PASS.
    if any(record.get("run") == Q2_PUBLISH_RUN for record in records):
        missing = sorted(
            Q2_PUBLISH_REQUIRED_SCENARIOS
            - {_q2_int(record.get("scenario")) for record in started}
        )
        if missing:
            reasons.append(
                "the C-04 Q2 publication campaign never ran required scenario(s) "
                + ", ".join(str(number) for number in missing)
            )

    counts = {
        "scenarios": len(started),
        "attempts": len(attempts),
        "committed": len(committed),
        "retries": sum(len(acks) - 1 for acks in ack_by_key.values()),
        "rejected": len(rejected),
        "unknown": len(unknown),
        "voter_states": len(states),
        "majority_proofs": majority_proofs,
    }
    reasons = list(dict.fromkeys(reasons))
    invalid = list(dict.fromkeys(invalid))
    if reasons:
        return _result(FAIL, reasons, counts)
    if invalid:
        return _result(INVALID, invalid, counts)
    if own_verdict == FAIL:
        return _result(FAIL, ["the scenario concluded FAIL"], counts)
    if own_verdict == INVALID:
        return _result(INVALID, ["the scenario concluded INVALID"], counts)
    if own_verdict == NOT_RUN:
        return _result(NOT_RUN, ["the scenario concluded NOT_RUN"], counts)
    if not committed:
        return _result(NOT_RUN, ["no committed Q2 publication evidence"], counts)
    if not_run_required:
        return _result(
            NOT_RUN,
            [
                "a required C-04 scenario was not run: "
                + "; ".join(
                    f"{record.get('scenario')}: {record.get('reason', 'no reason')}"
                    for record in not_run_required
                )
            ],
            counts,
        )
    return _result(PASS, [], counts)


# --- cross-class (Q1/Q2) isolation oracle -------------------------------------

ISOLATION_SCENARIOS = (
    "q1-restart-preserves-incarnation-and-offsets",
    "q1-unrecoverable-history-fails-closed-and-fresh-incarnation-is-distinct",
    "q2-quorum-loss-blocks-quorum-ack-while-q1-commits",
    "q2-history-survives-isolated-q1-storage-loss",
)

ISOLATION_LOCAL_ACK = "COMMITTED_LOCAL"
ISOLATION_QUORUM_ACK = "COMMITTED_QUORUM"
ISOLATION_ACK_VOCABULARY = (ISOLATION_LOCAL_ACK, ISOLATION_QUORUM_ACK)
ISOLATION_Q1_ACK_EVENTS = frozenset({
    "publish_ack", "dedup_retry", "fresh_incarnation_publish",
    "q1_commits_without_q2_quorum",
})
ISOLATION_Q2_ACK_EVENTS = frozenset({"q2_client_result", "q2_availability_restored"})
ISOLATION_Q1_MUTATIONS = frozenset({"publish_ack", "fresh_incarnation_publish"})


def _isolation_lines(record: dict) -> list[str]:
    value = record.get("client_stdout")
    if isinstance(value, str):
        return [line.strip() for line in value.splitlines() if line.strip()]
    if isinstance(value, (list, tuple)):
        return [line.strip() for line in value if isinstance(line, str) and line.strip()]
    return []


def _isolation_vocabulary(line: str) -> str | None:
    """The acknowledgement vocabulary carried by one client output line."""
    parts = line.split()
    if parts and parts[0] in ISOLATION_ACK_VOCABULARY:
        return parts[0]
    if line.startswith("{"):
        try:
            outcome = str(json.loads(line).get("outcome", "")).upper()
        except json.JSONDecodeError:
            return None
        if outcome in ISOLATION_ACK_VOCABULARY:
            return outcome
    return None


def _isolation_first_int(*values) -> int | None:
    for value in values:
        if isinstance(value, int) and not isinstance(value, bool):
            return value
    return None


def _isolation_offset(record: dict, line: str) -> int | None:
    parts = line.split()
    if len(parts) > 2 and parts[2].lstrip("-").isdigit():
        return int(parts[2])
    if line.startswith("{"):
        try:
            return _isolation_first_int(json.loads(line).get("offset"))
        except json.JSONDecodeError:
            return _isolation_first_int(record.get("offset"))
    return _isolation_first_int(record.get("offset"))


def _isolation_class(record: dict) -> int | None:
    explicit = record.get("durable_class")
    if explicit in (1, 2):
        return int(explicit)
    event = record.get("event") or ""
    if event in ISOLATION_Q1_ACK_EVENTS or event in (
        "offsets_journaled", "history_recovered", "history_destroyed",
        "recovery_refused", "old_offsets_not_replayable",
    ):
        return 1
    if event in ISOLATION_Q2_ACK_EVENTS or event in (
        "voter_state", "authority_disturbed", "voter_stop_observed",
        "voter_restarted_on_original_storage", "quorum_loss_outcome",
    ):
        return 2
    return None


def _isolation_acknowledgements(records: list[dict]) -> list[dict]:
    """Normalize every class acknowledgement the journal actually records."""
    acks = []
    for record in records:
        event = record.get("event")
        if event in ISOLATION_Q1_ACK_EVENTS or event in ISOLATION_Q2_ACK_EVENTS:
            cls = _isolation_class(record)
            for line in _isolation_lines(record):
                vocabulary = _isolation_vocabulary(line)
                if vocabulary is None:
                    continue
                parsed = {}
                if line.startswith("{"):
                    try:
                        parsed = json.loads(line)
                    except json.JSONDecodeError:
                        parsed = {}
                acks.append({
                    "class": cls,
                    "vocabulary": vocabulary,
                    "offset": _isolation_offset(record, line),
                    "incarnation": _isolation_first_int(
                        record.get("incarnation"), parsed.get("incarnation"),
                        record.get("new_incarnation"),
                    ),
                    "producer": _isolation_first_int(record.get("producer"), parsed.get("producer")),
                    "epoch": _isolation_first_int(record.get("epoch"), parsed.get("producer_epoch")),
                    "sequence": _isolation_first_int(record.get("sequence"), parsed.get("sequence")),
                    "message_id": _isolation_first_int(record.get("message_id")),
                    "digest": record.get("payload_digest") if isinstance(record.get("payload_digest"), str) else None,
                    "record": record,
                })
        elif event == "healthy_baseline":
            q1_ack = record.get("q1_ack")
            if isinstance(q1_ack, str):
                vocabulary = _isolation_vocabulary(q1_ack)
                if vocabulary:
                    acks.append({
                        "class": 1, "vocabulary": vocabulary,
                        "offset": _isolation_first_int(record.get("q1_offset")),
                        "incarnation": _isolation_first_int(record.get("incarnation")),
                        "producer": None, "epoch": None, "sequence": None,
                        "message_id": None,
                        "digest": record.get("q1_payload_digest") if isinstance(record.get("q1_payload_digest"), str) else None,
                        "record": record,
                    })
            q2_ack = record.get("q2_ack")
            if isinstance(q2_ack, dict):
                outcome = str(q2_ack.get("outcome", "")).upper()
                if outcome in ISOLATION_ACK_VOCABULARY:
                    acks.append({
                        "class": 2, "vocabulary": outcome,
                        "offset": _isolation_first_int(q2_ack.get("offset")),
                        "incarnation": _isolation_first_int(q2_ack.get("incarnation")),
                        "producer": None, "epoch": None, "sequence": None,
                        "message_id": None,
                        "digest": record.get("q2_payload_digest") if isinstance(record.get("q2_payload_digest"), str) else None,
                        "record": record,
                    })
        elif event in ISOLATION_Q2_ACK_EVENTS and isinstance(record.get("observed"), dict):
            observed = record["observed"]
            outcome = str(observed.get("outcome", "")).upper()
            if outcome in ISOLATION_ACK_VOCABULARY:
                acks.append({
                    "class": 2, "vocabulary": outcome,
                    "offset": _isolation_first_int(observed.get("offset")),
                    "incarnation": _isolation_first_int(observed.get("incarnation")),
                    "producer": None, "epoch": None, "sequence": None,
                    "message_id": _isolation_first_int(record.get("message_id")),
                    "digest": None,
                    "record": record,
                })
    return acks


def _isolation_attempts(records: list[dict]) -> dict:
    attempts = {}
    for record in records:
        if record.get("event") == "q2_publish_attempt":
            message_id = _isolation_first_int(record.get("message_id"))
            if message_id is not None:
                attempts[message_id] = record
    return attempts


def _isolation_majority_witnesses(records: list[dict]) -> tuple[dict, dict, int]:
    """Message/digest -> distinct voter nodes that applied it, plus the majority size."""
    by_message, by_digest = {}, {}
    required = 2
    for record in records:
        states = {}
        if record.get("event") == "voter_state":
            states = {record.get("node"): record}
        elif isinstance(record.get("voter_states"), dict):
            states = record["voter_states"]
        for node, state in states.items():
            if not isinstance(state, dict):
                continue
            node_id = int(node) if isinstance(node, str) and node.isdigit() else node
            applied = state.get("outcomes") or state.get("messages") or []
            for item in applied:
                if not isinstance(item, dict):
                    continue
                message_id = _isolation_first_int(item.get("message_id"))
                if message_id is not None:
                    by_message.setdefault(message_id, set()).add(node_id)
                digest = item.get("digest")
                if isinstance(digest, str):
                    by_digest.setdefault(digest, set()).add(node_id)
        if record.get("event") == "authority_disturbed":
            majority = _isolation_first_int(record.get("required_majority"))
            configured = record.get("configured_voters")
            if majority is not None and majority > 0:
                required = majority
            elif isinstance(configured, list) and configured:
                required = len(configured) // 2 + 1
    return by_message, by_digest, required


def reconcile_q1_q2_isolation(records: list[dict], *, require_conclusion: bool = True) -> dict:
    """Reconcile a campaign journal that mixes class-1 and class-2 observations.

    The journal is the only evidence. A client timeout during proven majority loss is
    the correct outcome: it counts as evidence that the quorum ACK was blocked and is
    never read as a commit and never itself a failure. A missing required field or
    scenario is INVALID, never PASS.
    """
    failures: list[str] = []
    invalid: list[str] = []
    counts = {
        "records": len(records),
        "scenarios": 0,
        "q1_acknowledged": 0,
        "q2_quorum_acknowledged": 0,
        "q2_blocked_timeouts": 0,
        "majority_loss_windows": 0,
        "violations": 0,
    }
    if not records:
        return {"verdict": NOT_RUN, "reasons": ["no isolation journal records"], "counts": counts}

    acks = _isolation_acknowledgements(records)
    attempts = _isolation_attempts(records)
    counts["q1_acknowledged"] = sum(1 for ack in acks if ack["class"] == 1)
    counts["q2_quorum_acknowledged"] = sum(
        1 for ack in acks
        if ack["class"] == 2 and ack["vocabulary"] == ISOLATION_QUORUM_ACK
    )

    # 1. A wrong-class publication acknowledged for the identity it names.
    for ack in acks:
        if ack["class"] == 2 and ack["vocabulary"] == ISOLATION_LOCAL_ACK:
            failures.append(
                "wrong-class acknowledgement: COMMITTED_LOCAL recorded for a class-2 identity"
            )
        elif ack["class"] == 1 and ack["vocabulary"] == ISOLATION_QUORUM_ACK:
            failures.append(
                "wrong-class acknowledgement: COMMITTED_QUORUM recorded for a class-1 identity"
            )

    # 2. A Q2 ACK backed only by Q1 evidence.
    by_message, by_digest, required_majority = _isolation_majority_witnesses(records)
    for ack in acks:
        if ack["vocabulary"] != ISOLATION_QUORUM_ACK or ack["class"] != 2:
            continue
        if ack["record"].get("event") == "q2_availability_restored":
            continue  # a restoration observation carries its own voter_states
        message_id = ack["message_id"]
        digest = ack["digest"]
        if digest is None and message_id is not None:
            digest = attempts.get(message_id, {}).get("payload_digest")
        if message_id is None and digest is None:
            states = ack["record"].get("voter_states")
            if not isinstance(states, dict) or not states:
                invalid.append(
                    "Q2 quorum acknowledgement without a resolvable message identity"
                )
                continue
            if len(states) < required_majority:
                failures.append(
                    "Q2 acknowledgment backed only by Q1 evidence: COMMITTED_QUORUM with "
                    "%d of %d voters in its own state summary"
                    % (len(states), required_majority)
                )
            continue
        witnesses = set(by_message.get(message_id, set())) if message_id is not None else set()
        if len(witnesses) < required_majority and isinstance(digest, str):
            witnesses |= by_digest.get(digest, set())
        if len(witnesses) < required_majority:
            failures.append(
                "Q2 acknowledgment backed only by Q1 evidence: COMMITTED_QUORUM without "
                "majority/raft witness (message %r, %d of %d voters)"
                % (message_id, len(witnesses), required_majority)
            )

    # 3. A reused Q1 incarnation after unrecoverable loss.
    destroyed = {}
    for index, record in enumerate(records):
        if record.get("event") == "history_destroyed":
            incarnation = _isolation_first_int(record.get("incarnation"))
            if incarnation is not None:
                destroyed.setdefault(incarnation, index)
    for index, record in enumerate(records):
        incarnation = _isolation_first_int(record.get("incarnation"))
        supersedes = _isolation_first_int(record.get("supersedes_incarnation"))
        new_incarnation = _isolation_first_int(record.get("new_incarnation"))
        old_incarnation = _isolation_first_int(record.get("old_incarnation"))
        if new_incarnation is not None and new_incarnation == old_incarnation:
            failures.append(
                "reused Q1 incarnation after unrecoverable loss: fresh history claims the "
                "destroyed incarnation %d" % new_incarnation
            )
        if incarnation is not None and supersedes is not None and incarnation == supersedes:
            failures.append(
                "reused Q1 incarnation after unrecoverable loss: new log supersedes itself (%d)"
                % incarnation
            )
        if incarnation is not None and incarnation in destroyed and index > destroyed[incarnation]:
            if record.get("event") in (
                "broker_started", "broker_restarted", "history_recovered",
                "fresh_incarnation_publish",
            ):
                failures.append(
                    "reused Q1 incarnation after unrecoverable loss: incarnation %d reused "
                    "after its history was destroyed" % incarnation
                )

    # 4. Q1 and Q2 histories merged under one identity.
    token_classes: dict[tuple, set] = {}
    digest_classes: dict[str, set] = {}
    for ack in acks:
        token = (ack["incarnation"], ack["producer"], ack["sequence"])
        if ack["class"] in (1, 2) and all(value is not None for value in token):
            token_classes.setdefault(token, set()).add(ack["class"])
        digest = ack["digest"]
        if digest is None and ack["message_id"] is not None:
            digest = attempts.get(ack["message_id"], {}).get("payload_digest")
        if ack["class"] in (1, 2) and isinstance(digest, str):
            digest_classes.setdefault(digest, set()).add(ack["class"])
    for token, classes in token_classes.items():
        if len(classes) > 1:
            failures.append(
                "Q1 and Q2 histories merged under one identity: producer token %r "
                "acknowledged by both classes" % (token,)
            )
    for digest, classes in digest_classes.items():
        if len(classes) > 1:
            failures.append(
                "Q1 and Q2 histories merged under one identity: payload %s... acknowledged "
                "by both classes" % digest[:12]
            )

    # 5. A previously acknowledged record missing from its recovery domain.
    acknowledged: dict = {}
    for ack in acks:
        if ack["class"] != 1 or ack["offset"] is None:
            continue
        scenario = ack["record"].get("scenario")
        acknowledged.setdefault(scenario, set()).add(ack["offset"])
    preserving = set()
    recovered: dict = {}
    unrecoverable = set()
    loss_documented = set()
    for record in records:
        scenario = record.get("scenario")
        event = record.get("event")
        if event == "broker_restarted" and record.get("same_data_dir") is True:
            preserving.add(scenario)
        elif event == "broker_started" and record.get("node") is None:
            if record.get("initialize_new_log") is False:
                preserving.add(scenario)
        elif event == "history_recovered":
            offsets = {o for o in record.get("offsets") or [] if isinstance(o, int)}
            for line in _isolation_lines(record):
                parts = line.split()
                if parts and parts[0] == "READ_RECORD" and len(parts) > 2 and parts[2].isdigit():
                    offsets.add(int(parts[2]))
            recovered[scenario] = offsets
        elif event == "history_destroyed":
            unrecoverable.add(scenario)
        elif event in ("recovery_refused", "old_offsets_not_replayable"):
            loss_documented.add(scenario)
    for scenario, offsets in acknowledged.items():
        if scenario in unrecoverable:
            if scenario not in loss_documented:
                invalid.append(
                    "unrecoverable loss in %r is not documented by a fail-closed refusal"
                    % (scenario,)
                )
            continue
        if scenario not in preserving:
            continue
        if scenario not in recovered:
            invalid.append("missing post-restart read for %r" % (scenario,))
            continue
        missing = offsets - recovered[scenario]
        if missing:
            failures.append(
                "previously acknowledged record missing from its recovery domain: "
                "offsets %s absent from the post-restart read" % sorted(missing)
            )

    # 6. Divergent logical offsets for one committed producer token.
    token_offsets: dict[tuple, set] = {}
    for ack in acks:
        token = (ack["incarnation"], ack["producer"], ack["sequence"])
        if all(value is not None for value in token) and ack["offset"] is not None:
            token_offsets.setdefault(token, set()).add(ack["offset"])
    for token, offsets in token_offsets.items():
        if len(offsets) > 1:
            failures.append(
                "divergent logical offsets for one committed producer token: %r "
                "acknowledged at offsets %s" % (token, sorted(offsets))
            )

    # 7. A fenced or stale owner successfully mutating.
    closed: dict = {}
    for record in records:
        event = record.get("event")
        node = record.get("node")
        if event == "voter_stop_observed":
            closed[("q2", node)] = True
        elif event == "broker_killed":
            closed[("q2", node) if node is not None else ("q1", None)] = True
        elif event == "broker_stopped":
            closed[("q1", None)] = True
        elif event == "broker_started":
            if node is None:
                closed[("q1", None)] = False
            elif record.get("q1_topic") or record.get("q2_topic"):
                closed[("q1", None)] = False
                closed[("q2", node)] = False
            else:
                closed[("q2", node)] = False
        elif event == "broker_restarted":
            closed[("q1", None)] = False
        elif event == "voter_restarted_on_original_storage":
            closed[("q2", node)] = False
        elif event in ISOLATION_Q1_MUTATIONS and closed.get(("q1", None)):
            failures.append(
                "fenced/stale owner mutated: %s recorded after a kill with no recovery "
                "event in between" % event
            )

    # 8. A cursor advancing across an incomplete record (only when the journal has cursors).
    for record in records:
        group = record.get("group") if isinstance(record.get("group"), dict) else None
        if group is None and _isolation_first_int(record.get("cursor")) is not None \
                and isinstance(record.get("outcomes"), list):
            group = record
        if not isinstance(group, dict):
            continue
        cursor = _isolation_first_int(group.get("cursor"))
        if cursor is None:
            continue
        offsets = {o for o in group.get("outcomes") or [] if isinstance(o, int)}
        if len([o for o in offsets if o < cursor]) != cursor:
            failures.append(
                "cursor advanced across an incomplete record: cursor %d over offsets %s"
                % (cursor, sorted(offsets))
            )

    # 9. Q2 success during proven majority loss.
    alive: set = set()
    majority = required_majority
    for record in records:
        event = record.get("event")
        node = record.get("node")
        if event == "broker_started" and node is not None:
            alive.add(node)
        elif event == "voter_restarted_on_original_storage" and node is not None:
            alive.add(node)
        elif event in ("broker_killed", "voter_stop_observed") and node is not None:
            alive.discard(node)
        elif event == "authority_disturbed":
            counts["majority_loss_windows"] += 1
            configured = set(record.get("configured_voters") or [])
            live = set(record.get("live_voters") or [])
            stopped = set(record.get("stopped_voters") or [])
            if configured:
                alive = (alive | live | configured) - stopped
            observed_majority = _isolation_first_int(record.get("required_majority"))
            if observed_majority is not None and observed_majority > 0:
                majority = observed_majority
        if event in ISOLATION_Q2_ACK_EVENTS:
            if record.get("timed_out") is True:
                counts["q2_blocked_timeouts"] += 1
            observed = record.get("observed")
            if isinstance(observed, dict) and str(observed.get("outcome", "")).lower() == "unknown":
                if record.get("timed_out") is not True:
                    counts["q2_blocked_timeouts"] += 1
            committed_here = any(
                ack["record"] is record and ack["vocabulary"] == ISOLATION_QUORUM_ACK
                for ack in acks
            )
            if committed_here and len(alive) < majority:
                failures.append(
                    "Q2 success during proven majority loss: COMMITTED_QUORUM with %d of "
                    "%d required voters alive" % (len(alive), majority)
                )

    # 10. An incomplete scenario set reported PASS.
    conclusions: dict = {}
    for record in records:
        if record.get("event") == "conclusion":
            conclusions[record.get("scenario")] = record
    counts["scenarios"] = len({
        record.get("scenario") for record in records
        if record.get("event") in ("scenario_started", "conclusion")
        and isinstance(record.get("scenario"), str)
    })
    if require_conclusion:
        for name in ISOLATION_SCENARIOS:
            record = conclusions.get(name)
            if record is None:
                invalid.append("incomplete scenario set: missing conclusion for %s" % name)
                continue
            verdict = record.get("verdict")
            if verdict == FAIL:
                failures.append("scenario %s concluded FAIL" % name)
            elif verdict != PASS:
                invalid.append(
                    "incomplete scenario set: %s concluded %s instead of PASS"
                    % (name, verdict)
                )

    unique_failures = list(dict.fromkeys(failures))
    unique_invalid = list(dict.fromkeys(invalid))
    counts["violations"] = len(unique_failures)
    verdict = FAIL if unique_failures else INVALID if unique_invalid else PASS
    return {"verdict": verdict, "reasons": unique_failures + unique_invalid, "counts": counts}


# ---------------------------------------------------------------------------
# C-07 cluster fault gate
#
# The gate's journal is written by the parent harness from observed client results,
# kill/restart observations and post-scenario voter state.  It is reconciled here
# from those records alone: no scenario verdict is itself durability evidence.
# An uncertain outcome (OutcomeUnknown) during majority loss and a typed refusal
# after an observed lease expiry are the *correct* results and count as evidence
# for the quorum rules; neither is a commit.
# ---------------------------------------------------------------------------

FAULT_GATE_SCENARIOS = (
    "healthy-q0-q1-q2-baseline",
    "consumer-ack-refused-without-q2-majority",
    "metadata-election-preserves-ownership-epoch",
    "expired-authority-fails-closed",
)
REPOSITORY_ROOT = Path(__file__).resolve().parent.parent
EVIDENCE_ROOT = REPOSITORY_ROOT / "benchmarks" / "evidence"


def _gate_partition(value) -> tuple | None:
    """A hashable partition identity from a route, placement or publication record."""
    if isinstance(value, dict):
        value = value.get("partition", value)
        fields = [value.get(k) for k in ("tenant_id", "topic_id", "partition_id")]
        if all(isinstance(v, int) and not isinstance(v, bool) for v in fields):
            return tuple(fields)
    return None


def _gate_placements(record: dict) -> list[dict]:
    """Every metadata placement carried by one observation."""
    found: list[dict] = []
    for key in ("placement", "committed_placement"):
        value = record.get(key)
        if isinstance(value, dict):
            found.extend(p for p in value.get("placements") or [] if isinstance(p, dict))
    evidence = record.get("evidence")
    if isinstance(evidence, dict):
        found.extend(p for p in evidence.get("placements") or [] if isinstance(p, dict))
    placements = record.get("placements")
    if isinstance(placements, list):
        found.extend(p for p in placements if isinstance(p, dict))
    return found


def _gate_consumer_outcome(record: dict) -> dict:
    """The parsed consumer result, falling back to the raw client stdout."""
    parsed = record.get("parsed")
    if isinstance(parsed, dict) and parsed:
        return parsed
    stdout = record.get("stdout")
    if isinstance(stdout, str) and stdout.lstrip().startswith("{"):
        try:
            value = json.loads(stdout)
        except json.JSONDecodeError:
            return {}
        if isinstance(value, dict):
            return value
    return {}


def _gate_group_progress(record: dict) -> tuple[set[int], dict[int, int]]:
    """Saved group outcomes of a consumer_state observation.

    Returns the saved logical offsets and, per offset, the generation they were
    saved under, so a later observation can be checked for a stale mutation.
    """
    offsets: set[int] = set()
    generations: dict[int, int] = {}
    progress = record.get("progress")
    if isinstance(progress, dict):
        for node in progress.values():
            outcomes = node.get("outcomes") if isinstance(node, dict) else None
            if not isinstance(outcomes, dict):
                continue
            for offset, saved in outcomes.items():
                try:
                    number = int(offset)
                except (TypeError, ValueError):
                    continue
                offsets.add(number)
                if not isinstance(saved, dict):
                    continue
                acked = (saved.get("outcome") or {}).get("GroupAcked")
                generation = acked.get("generation") if isinstance(acked, dict) else None
                if isinstance(generation, int):
                    generations[number] = generation
    return offsets, generations


def _gate_cited_verdicts(path: Path) -> list:
    """Verdicts recorded by a cited evidence file, JSONL or a single document."""
    text = path.read_text(encoding="utf-8")
    try:
        document = json.loads(text)
    except json.JSONDecodeError:
        document = None
    if isinstance(document, dict):
        return [document["verdict"]] if "verdict" in document else []
    verdicts = []
    for line in text.splitlines():
        if not line.strip():
            continue
        try:
            record = json.loads(line)
        except json.JSONDecodeError:
            return [INVALID]
        if isinstance(record, dict) and "verdict" in record:
            verdicts.append(record["verdict"])
    return verdicts


def reconcile_cluster_fault_gate(
    records: list[dict], *, require_conclusion: bool = True
) -> dict:
    """Reconcile the C-07 cluster fault gate from the journal it produced.

    The journal is the only evidence: externally received client results, the
    parent's kill/restart observations and broker-printed voter state.  A commit
    needs distinct voter witnesses; an ACK or cursor may not outrun them.  An
    uncertain outcome during majority loss is the expected result and a typed
    refusal after an observed ``lease_ms`` expiry is the correct fail-closed
    result.  Missing evidence is INVALID, never PASS.
    """
    failures: list[str] = []
    invalid: list[str] = []
    counts = {
        "records": len(records),
        "scenarios": 0,
        "conclusions": 0,
        "scenario_passes": 0,
        "q1_committed": 0,
        "q2_committed": 0,
        "majority_proofs": 0,
        "ack_committed": 0,
        "refusals": 0,
        "uncertain": 0,
        "reused_evidence": 0,
        "violations": 0,
    }
    if not records:
        return _result(NOT_RUN, ["no cluster fault gate evidence"], counts)

    conclusions: dict = {}
    for record in records:
        if record.get("event") == "conclusion":
            conclusions.setdefault(record.get("scenario"), record)
    journals: dict[str, list[dict]] = {}
    for record in records:
        scenario = record.get("scenario")
        if isinstance(scenario, str):
            journals.setdefault(scenario, []).append(record)
    counts["scenarios"] = len(journals)
    counts["conclusions"] = len(conclusions)
    counts["scenario_passes"] = sum(
        1 for conclusion in conclusions.values() if conclusion.get("verdict") == PASS
    )

    # 11. The scenario set and its conclusions.
    if require_conclusion and not conclusions:
        invalid.append("missing campaign conclusion")
    if require_conclusion:
        for name in FAULT_GATE_SCENARIOS:
            conclusion = conclusions.get(name)
            if conclusion is None:
                failures.append(
                    f"incomplete scenario set: missing conclusion for {name}"
                )
            elif conclusion.get("verdict") != PASS:
                failures.append(
                    f"incomplete scenario set: {name} concluded "
                    f"{conclusion.get('verdict')} instead of PASS"
                )

    witness_fields = 0
    for scenario, journal in journals.items():
        # -- the routes, voters and metadata epochs this scenario advertises --
        q1_partition = q2_partition = None
        voters: set[int] = set()
        declared_majority = None
        declared_epoch = None
        epochs: set[int] = set()
        epochs_by_partition: dict[tuple, set[int]] = {}
        for record in journal:
            event = record.get("event")
            if event == "scenario_started":
                route = record.get("q1_route")
                if isinstance(route, dict):
                    q1_partition = _gate_partition(route.get("partition"))
                    if isinstance(route.get("owner_epoch"), int):
                        declared_epoch = route["owner_epoch"]
                        epochs_by_partition.setdefault(q1_partition, set()).add(
                            route["owner_epoch"]
                        )
                route = record.get("q2_route")
                if isinstance(route, dict):
                    q2_partition = _gate_partition(route.get("partition"))
                    # The Q2 data domain owns its own epoch; it is not the Q1 epoch.
                    if isinstance(route.get("owner_epoch"), int):
                        epochs_by_partition.setdefault(q2_partition, set()).add(
                            route["owner_epoch"]
                        )
                observed = record.get("voters")
                if isinstance(observed, dict):
                    voters |= {int(node) for node in observed}
            elif event == "healthy_baseline":
                identities = record.get("identities")
                if isinstance(identities, dict):
                    q1_identity = identities.get("q1")
                    q2_identity = identities.get("q2")
                    if isinstance(q1_identity, dict) and q1_partition is None:
                        q1_partition = _gate_partition(q1_identity)
                    if isinstance(q2_identity, dict) and q2_partition is None:
                        q2_partition = _gate_partition(q2_identity)
            quorum = record.get("quorum_scope")
            if isinstance(quorum, dict):
                if isinstance(quorum.get("required_majority"), int):
                    declared_majority = quorum["required_majority"]
                configured = quorum.get("configured_voters")
                if isinstance(configured, list):
                    voters |= {v for v in configured if isinstance(v, int)}
            for placement in _gate_placements(record):
                epoch = placement.get("ownership_epoch")
                if isinstance(epoch, int):
                    epochs.add(epoch)
                    epochs_by_partition.setdefault(
                        _gate_partition(placement), set()
                    ).add(epoch)
        required = declared_majority or (len(voters) // 2 + 1 if voters else 2)
        expected_epoch = max(epochs) if epochs else declared_epoch
        for partition, observed in epochs_by_partition.items():
            epochs_by_partition[partition] = {max(observed)}

        # -- observed voter witnesses: offset -> nodes, identity -> nodes --
        carriers: dict[int, set[int]] = {}
        identity_carriers: dict[tuple, set[int]] = {}
        for record in journal:
            if record.get("event") != "voter_state":
                continue
            node = record.get("node")
            for collection in ("messages", "outcomes"):
                entries = record.get(collection)
                if not isinstance(entries, list):
                    continue
                for entry in entries:
                    if not isinstance(entry, dict):
                        continue
                    offset = entry.get("offset")
                    if not isinstance(offset, int):
                        continue
                    if not isinstance(node, int):
                        # A witness without a voter identity is not a majority.
                        invalid.append(
                            "voter state without a witness node field for "
                            f"offset {offset}"
                        )
                        continue
                    witness_fields += 1
                    carriers.setdefault(offset, set()).add(node)
                    message_id = entry.get("message_id")
                    if isinstance(message_id, int):
                        identity_carriers.setdefault(
                            (offset, message_id), set()
                        ).add(node)

        # -- the publications this scenario acknowledged --
        q1_offsets: set[int] = set()
        quorum_publications: list[dict] = []
        for record in journal:
            if record.get("event") != "publication_committed":
                continue
            acknowledgment = str(record.get("acknowledgment", "")).upper()
            outcome = str(record.get("outcome", "")).lower()
            if acknowledgment == "COMMITTED_LOCAL" or record.get("durable_class") == "q1-local":
                if isinstance(record.get("offset"), int):
                    q1_offsets.add(record["offset"])
                continue
            if outcome == "committed_quorum":
                quorum_publications.append(record)
                if isinstance(record.get("offset"), int):
                    counts["q2_committed"] += 1
        counts["q1_committed"] += len(q1_offsets)

        # -- 1. every acknowledged offset survives in the evidence that retains it --
        for record in quorum_publications:
            offset = record.get("offset")
            if isinstance(offset, int) and not carriers.get(offset):
                failures.append(
                    f"acknowledged Q2 offset {offset} is missing from the retaining evidence"
                )
        retained = None
        for record in journal:
            if record.get("event") == "healthy_baseline":
                identities = record.get("identities")
                if isinstance(identities, dict):
                    identity = identities.get("q1")
                    if isinstance(identity, dict) and isinstance(identity.get("offsets"), list):
                        retained = {o for o in identity["offsets"] if isinstance(o, int)}
            elif record.get("event") == "q1_retained_offsets":
                offsets = record.get("offsets")
                if isinstance(offsets, list):
                    retained = {o for o in offsets if isinstance(o, int)}
        if retained is not None:
            for offset in sorted(q1_offsets - retained):
                failures.append(
                    f"acknowledged Q1 offset {offset} is missing from the retaining evidence"
                )

        # -- 2. a committed quorum needs two distinct voter witnesses --
        for record in quorum_publications:
            offset, message_id = record.get("offset"), record.get("message_id")
            witnesses = identity_carriers.get((offset, message_id), set())
            if len(witnesses) < max(required, 2):
                failures.append(
                    "committed quorum without two distinct voter witnesses for "
                    f"offset {offset} message {message_id}: {sorted(witnesses)}"
                )
            else:
                counts["majority_proofs"] += 1

        # -- 3. an ACK is never committed without a live majority --
        dead: set[int] = set()
        for record in journal:
            event = record.get("event")
            node = record.get("node")
            if str(record.get("role", "")).startswith("metadata"):
                continue
            if event in ("broker_killed", "voter_stop_observed"):
                if isinstance(node, int) and node in voters:
                    dead.add(node)
            elif event in ("broker_started", "voter_restarted_on_original_storage"):
                if isinstance(node, int) and node in voters:
                    dead.discard(node)
            elif event in ("quorum_loss_outcome", "q1_commits_without_q2_quorum"):
                live = record.get("live_voters", record.get("live_q2_voters"))
                if isinstance(live, list) and voters:
                    dead = set(voters) - {v for v in live if isinstance(v, int)}
            elif event == "consumer_result":
                outcome = _gate_consumer_outcome(record)
                if outcome.get("outcome") == "ack_committed":
                    counts["ack_committed"] += 1
                    live = set(voters) - dead
                    if voters and len(live) < required:
                        failures.append(
                            "ack_committed recorded with %d live Q2 voters while %d are required"
                            % (len(live), required)
                        )
                elif outcome.get("outcome") == "error":
                    detail = str(outcome.get("detail", ""))
                    if "OutcomeUnknown" in detail:
                        counts["uncertain"] += 1
                    elif detail:
                        counts["refusals"] += 1
        for record in journal:
            if record.get("event") == "publication_refused":
                counts["refusals"] += 1

        # -- 4. one producer token never produces two committed records or offsets --
        committed_by_token: dict[tuple, list[dict]] = {}
        committed_by_identity: dict[tuple, set[int]] = {}
        for record in journal:
            if record.get("event") != "publication_committed":
                continue
            key = (
                _gate_partition(record.get("partition")),
                record.get("producer"),
                record.get("epoch"),
                record.get("sequence"),
            )
            committed_by_token.setdefault(key, []).append(record)
            if isinstance(record.get("offset"), int) and isinstance(record.get("message_id"), int):
                identity = (_gate_partition(record.get("partition")), record["message_id"])
                committed_by_identity.setdefault(identity, set()).add(record["offset"])
        for key, committed in committed_by_token.items():
            pairs = {(r.get("offset"), r.get("message_id")) for r in committed}
            offsets = {r.get("offset") for r in committed}
            if len(pairs) > 1:
                failures.append(f"producer token {key} produced two committed records")
            if len(offsets) > 1:
                failures.append(f"producer token {key} produced two offsets")

        # -- 5. a retry keeps the original logical offset --
        for (_, message_id), offsets in committed_by_identity.items():
            if len(offsets) > 1:
                failures.append(
                    f"retry changed the logical offset of message {message_id}: {sorted(offsets)}"
                )
        for record in journal:
            if record.get("event") != "dedup_check":
                continue
            original, retry = record.get("original_offset"), record.get("retry_offset")
            if isinstance(original, int) and isinstance(retry, int) and original != retry:
                failures.append(
                    f"retry changed the logical offset {original} -> {retry}"
                )

        # -- 6. a cursor never crosses the contiguous acknowledged prefix --
        acknowledged: dict[object, set[int]] = {}
        for record in journal:
            event = record.get("event")
            group = record.get("group")
            if event == "consumer_result":
                outcome = _gate_consumer_outcome(record)
                if outcome.get("outcome") == "ack_committed" and isinstance(
                    outcome.get("offset"), int
                ):
                    acknowledged.setdefault(group, set()).add(outcome["offset"])
            elif event == "consumer_state":
                offsets, _ = _gate_group_progress(record)
                if offsets:
                    acknowledged.setdefault(group, set()).update(offsets)
        for record in journal:
            if record.get("event") != "consumer_state" or record.get("kind") != "group_cursor":
                continue
            cursors = record.get("cursors")
            if not isinstance(cursors, dict):
                continue
            group = record.get("group")
            acked = acknowledged.get(group, set())
            prefix = 0
            while prefix in acked:
                prefix += 1
            for voter, cursor in cursors.items():
                if not isinstance(cursor, int):
                    continue
                if cursor > prefix:
                    failures.append(
                        f"cursor {cursor} of voter {voter} is beyond the contiguous "
                        f"acknowledged prefix {prefix}"
                    )
                # -- 8. the uncommitted tail is never reported committed --
                for offset in range(max(cursor, 0)):
                    if len(carriers.get(offset, set())) < max(required, 2):
                        failures.append(
                            f"uncommitted tail at offset {offset} reported committed by "
                            f"cursor {cursor}"
                        )

        # -- 7. a stale epoch or generation never mutates state it does not own --
        generation: dict[object, int] = {}
        for record in journal:
            event = record.get("event")
            if event == "consumer_delivery":
                token = record.get("token")
                if isinstance(token, dict):
                    group = token.get("group_id")
                    value = token.get("generation")
                    if isinstance(group, int) and isinstance(value, int):
                        generation[group] = max(generation.get(group, 0), value)
            elif event == "consumer_result":
                outcome = _gate_consumer_outcome(record)
                group, value = record.get("group"), outcome.get("generation")
                if (
                    outcome.get("outcome") == "ack_committed"
                    and isinstance(group, int)
                    and isinstance(value, int)
                ):
                    current = generation.get(group, value)
                    if value < current:
                        failures.append(
                            f"stale generation {value} mutated group {group} while "
                            f"{current} is current"
                        )
                    generation[group] = max(current, value)
            elif event == "consumer_state":
                _, generations = _gate_group_progress(record)
                group = record.get("group")
                for offset, value in generations.items():
                    current = generation.get(group, value)
                    if value < current:
                        failures.append(
                            f"stale generation {value} saved offset {offset} while "
                            f"{current} is current"
                        )
                    generation[group] = max(current, value)
        if isinstance(expected_epoch, int):
            for record in journal:
                if record.get("event") not in ("publication_attempt", "publication_committed"):
                    continue
                epoch = record.get("owner_epoch")
                if not isinstance(epoch, int):
                    continue
                partition = _gate_partition(record.get("partition"))
                observed = epochs_by_partition.get(partition)
                expected = max(observed) if observed else (
                    expected_epoch if partition is None else None
                )
                if expected is not None and epoch < expected:
                    failures.append(
                        f"stale ownership epoch {epoch} mutated state while "
                        f"{expected} is current"
                    )

        # -- 9. the Q1 and Q2 domains justify only their own acknowledgment --
        if q1_partition is not None and q1_partition == q2_partition:
            failures.append(
                "Q1/Q2 domain substitution: both routes name the same partition"
            )
        for record in journal:
            acknowledgment = str(record.get("acknowledgment", "")).upper()
            outcome = str(record.get("outcome", "")).lower()
            partition = _gate_partition(record.get("partition"))
            if outcome == "committed_quorum":
                if q1_partition is not None and partition == q1_partition:
                    failures.append(
                        "COMMITTED_QUORUM justified only by Q1 evidence"
                    )
                elif q2_partition is not None and partition is not None and partition != q2_partition:
                    failures.append(
                        "COMMITTED_QUORUM outside the declared Q2 partition"
                    )
            if acknowledgment == "COMMITTED_LOCAL":
                if outcome in ("committed_quorum", "ack_committed"):
                    failures.append(
                        "COMMITTED_LOCAL presented as a quorum result"
                    )
                elif q2_partition is not None and partition == q2_partition:
                    failures.append(
                        "COMMITTED_LOCAL presented as a quorum result on the Q2 partition"
                    )
            if (
                record.get("event") == "q1_commits_without_q2_quorum"
                and acknowledgment == "COMMITTED_QUORUM"
            ):
                failures.append("COMMITTED_QUORUM justified only by Q1 evidence")

        # -- 10. a corrupted replica stops continuation or is detected and recovered --
        for index, record in enumerate(journal):
            if record.get("event") not in (
                "replica_corrupted",
                "replica_corruption",
                "corrupted_replica",
            ):
                continue
            later = journal[index + 1:]
            detected = record.get("detected") is True or bool(record.get("detection"))
            detected = detected or any(
                item.get("event") in (
                    "replica_corruption_detected",
                    "replica_checksum_mismatch",
                    "publication_refused",
                )
                for item in later
            )
            recovered = any(
                item.get("event") in (
                    "replica_recovered",
                    "replica_restored",
                    "voter_restarted_on_original_storage",
                )
                for item in later
            )
            continued = any(
                item.get("event") in ("publication_committed", "consumer_result")
                and (
                    str(item.get("acknowledgment", "")).upper() in ("COMMITTED_LOCAL", "COMMITTED_QUORUM")
                    or _gate_consumer_outcome(item).get("outcome") == "ack_committed"
                )
                for item in later
            ) or any(
                item.get("event") == "conclusion" and item.get("verdict") == PASS
                for item in later
            )
            if continued and not detected and not recovered:
                failures.append(
                    "continuation claimed after a corrupted replica without detection "
                    "or recovery"
                )

    # -- the reuse check: a cited evidence file exists and records PASS --
    reused = [record for record in records if record.get("event") == "reused_evidence"]
    counts["reused_evidence"] = len(reused)
    for record in reused:
        cited = record.get("path") or record.get("file")
        if not isinstance(cited, str) or not cited:
            invalid.append("reused evidence without a cited path")
            continue
        resolved = Path(cited)
        if not resolved.is_absolute():
            resolved = (REPOSITORY_ROOT / resolved).resolve()
        try:
            resolved.relative_to(EVIDENCE_ROOT)
        except ValueError:
            continue
        if not resolved.is_file():
            invalid.append(f"cited evidence {cited} does not exist")
            continue
        try:
            verdicts = _gate_cited_verdicts(resolved)
        except OSError as error:
            invalid.append(f"cited evidence {cited} is unreadable: {error}")
            continue
        for verdict in verdicts:
            if verdict != PASS:
                invalid.append(f"cited evidence {cited} records verdict {verdict}")

    unique_failures = list(dict.fromkeys(failures))
    unique_invalid = list(dict.fromkeys(invalid))
    counts["witness_fields"] = witness_fields
    counts["violations"] = len(unique_failures)
    if unique_failures:
        return _result(FAIL, unique_failures + unique_invalid, counts)
    if unique_invalid:
        return _result(INVALID, unique_invalid, counts)
    if counts["scenarios"] == 0:
        return _result(NOT_RUN, ["no cluster fault gate evidence"], counts)
    return _result(PASS, [], counts)
