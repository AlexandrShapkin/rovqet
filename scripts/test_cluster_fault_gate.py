#!/usr/bin/env python3
"""C-07 fault gate: Q0/Q1/Q2 baseline, ACK quorum loss, and live metadata authority.

F7a observes a three-voter metadata-Raft leader replacement with unchanged
committed placement and continued Q1 commits under a 60-second lease. F7b
commits a 200-ms placement but reports INVALID if the non-DLQ route cannot
expose installation/expiry; elapsed time alone never establishes expiry.

F1 (healthy-q0-q1-q2-baseline): one campaign starts three real Q2 voters plus
a single broker that hosts the Q1 route, the Q2 route and the Q0 admission
path. Q0 mirrors scripts/q0_smoke.py (two external subscribers and one
external publisher over loopback TCP), Q1 publishes through the real SDK
example and observes COMMITTED_LOCAL, and Q2 publishes and ACKs through the
real q2_publish/q2_consumer examples and observes COMMITTED_QUORUM with a
durable group cursor that advances to include the acknowledged message.

F3b (consumer-ack-refused-without-q2-majority): a healthy Q2 publication is
fetched so a real delivery token exists, two of three Q2 voters are SIGKILLed,
and the identical ACK is attempted against the surviving voter. The ACK must
never produce ACK_COMMITTED and the durable cursor must not advance; Q1 (and
Q0) keep working on the shared broker; after both voters are restored on their
original storage the identical ACK identity resolves consistently — committed
exactly once or still refused/uncertain — with no duplicate record.

The parent owns the fsynced journal at
benchmarks/evidence/c07-cluster-fault-gate/fault-gate.jsonl.

Every subprocess has a bounded deadline and its stdout/stderr reaches the
journal before any interpretation. Durability boundaries are observed outputs
(the Q1 COMMITTED_LOCAL line, the Q2 committed_quorum acknowledgment, the ACK
cursor advancement), never sleeps. Each process owns an independent persistent
directory under a disposable temporary root; all listeners are loopback. If Q0
cannot be exercised the way q0_smoke.py exercises it against this broker, the
scenario is journaled NOT_RUN with the exact reason — never PASS. If a
delivery lease expires before a quorum-loss ACK can be attempted, the
scenario is INVALID, never PASS. No oracle reconciler runs in this
increment.
"""
from __future__ import annotations

import json
import subprocess
import sys
import tempfile
import time
import traceback
from pathlib import Path

import q1_oracle as oracle
from q0_smoke import line_within
from q1_smoke import Q0, Q1, SERVER, Broker, q1_command
from test_metadata_raft import Node as MetadataVoter, NODE as METADATA_NODE, free_port
from test_q2_publish import (
    Campaign,
    EXAMPLE,
    INCARNATION,
    OWNER_EPOCH as Q2_OWNER,
    PARTITION as Q2_PARTITION,
    PARTITION_KEY,
    TENANT as Q2_TENANT,
    T_POLL,
    TOPIC as Q2_TOPIC,
    TOPIC_ID as Q2_TOPIC_ID,
)

EVIDENCE = (Path(__file__).resolve().parents[1] / "benchmarks" / "evidence"
            / "c07-cluster-fault-gate")
Q2_CONSUMER = (Path(__file__).resolve().parents[1] / "target" / "debug"
               / "examples" / "q2_consumer")

F1 = "healthy-q0-q1-q2-baseline"
F3B = "consumer-ack-refused-without-q2-majority"
F7A = "metadata-election-preserves-ownership-epoch"
F7B = "expired-authority-fails-closed"

Q1_TOPIC = "durable"
Q1_TENANT, Q1_TOPIC_ID, Q1_PARTITION = 1, 3, 0
Q1_INCARNATION, Q1_OWNER_EPOCH = 42, 7
Q1_PRODUCER, Q1_PRODUCER_EPOCH = 11, 0

Q0_TOPIC = "demo"
Q0_PAYLOAD = "c07-f1-q0-baseline"

Q2_PRODUCER, Q2_PRODUCER_EPOCH = 11, 0
Q2_GROUP = 70
Q2_PAYLOAD = "c07-f1-q2-baseline"
Q2_PAYLOAD_TWO = "c07-f1-q2-domain"
Q2_MESSAGE, Q2_MESSAGE_TWO = 400, 401

F3B_GROUP = 71
F3B_PAYLOAD = "c07-f3b-ack-refusal"
F3B_MESSAGE = 500
F3B_Q0_PAYLOAD = "c07-f3b-q0-outage"

TIMEOUT_CLIENT = 15
TIMEOUT_CONSUMER = 20
TIMEOUT_PUBLISH_CERTAIN = 60
TIMEOUT_CURSOR = 30
# The SDK example's own request deadline is 20s; this parent bound sits above
# it so the client's own refusal text is observed before any parent timeout.
TIMEOUT_ACK = 25


class Q0Unavailable(RuntimeError):
    """Q0 could not be exercised the way q0_smoke.py does against this broker."""


class ScenarioInvalid(RuntimeError):
    """The scenario could not establish its safety observation honestly."""


class ScenarioJournal:
    """Include the scenario even on observations emitted by reused launchers."""

    def __init__(self, journal: oracle.Journal, scenario: str):
        self._journal = journal
        self._scenario = scenario

    def append(self, event, **fields):
        return self._journal.append(event, scenario=self._scenario, **fields)


def partition_identity() -> dict:
    return {"tenant_id": Q1_TENANT, "topic_id": Q1_TOPIC_ID,
            "partition_id": Q1_PARTITION}


def run_bounded(command, env, timeout) -> dict:
    """Run an SDK example with a bounded deadline; never raise on timeout."""
    try:
        result = subprocess.run(command, env=env, capture_output=True,
                                text=True, timeout=timeout)
        return {"stdout": result.stdout, "stderr": result.stderr,
                "returncode": result.returncode, "timed_out": False,
                "timeout_seconds": timeout}
    except subprocess.TimeoutExpired as error:
        def text(value):
            return value.decode() if isinstance(value, bytes) else (value or "")
        return {"stdout": text(error.stdout), "stderr": text(error.stderr),
                "returncode": None, "timed_out": True,
                "timeout_seconds": timeout}


def parse_json_line(observed: dict, what: str) -> dict:
    assert not observed["timed_out"], (
        f"{what} timed out after {observed['timeout_seconds']}s: {observed}")
    assert observed["returncode"] == 0, f"{what}: {observed}"
    lines = [line for line in observed["stdout"].splitlines() if line.strip()]
    assert len(lines) == 1, f"{what} printed {len(lines)} lines: {observed}"
    return json.loads(lines[0])


def exercise_q0(journal, broker, payload: str = Q0_PAYLOAD) -> dict:
    """Mirror q0_smoke.py: two external subscribers, then one external publisher."""
    env = broker.environment()
    command = [str(Q0)]
    outcome = {"topic": Q0_TOPIC, "payload": payload, "deliveries": [],
               "subscription_lines": []}
    subscribers = []
    try:
        for index in range(2):
            subscriber = subprocess.Popen(
                command + ["sub", Q0_TOPIC], env=env, stdout=subprocess.PIPE,
                stderr=subprocess.PIPE, text=True)
            subscribers.append(subscriber)
            try:
                line = line_within(subscriber.stdout, TIMEOUT_CLIENT)
            except BaseException as error:
                if subscriber.poll() is None:
                    subscriber.kill()
                out, err = subscriber.communicate(timeout=TIMEOUT_CLIENT)
                journal.append("q0_not_run", phase="subscribe", subscriber=index,
                               pid=subscriber.pid,
                               reason=f"{type(error).__name__}: {error}",
                               subscriber_stdout=out, subscriber_stderr=err)
                raise Q0Unavailable(
                    f"Q0 subscriber {index} never became ready: "
                    f"{type(error).__name__}: {error}; stderr={err.strip()[:400]!r}"
                ) from error
            if not line.startswith("SUBSCRIBED "):
                journal.append("q0_not_run", phase="subscribe", subscriber=index,
                               pid=subscriber.pid,
                               reason=f"unexpected subscription line {line!r}",
                               subscription_line=line)
                raise Q0Unavailable(
                    f"Q0 subscriber {index} printed {line!r} instead of a "
                    "SUBSCRIBED line against this broker")
            outcome["subscription_lines"].append(line)
            journal.append("consumer_state", consumer="q0", subscriber=index,
                           pid=subscriber.pid, subscription_line=line)

        publisher_command = command + ["pub", Q0_TOPIC, payload]
        journal.append("publication_attempt", durable_class="q0",
                       mode="pub", topic=Q0_TOPIC, payload=payload,
                       payload_digest=oracle.digest_hex(payload.encode()),
                       command=publisher_command)
        observed = run_bounded(publisher_command, env, TIMEOUT_CLIENT)
        if (observed["timed_out"] or observed["returncode"] != 0
                or observed["stdout"].strip() != "Admitted"):
            journal.append("q0_not_run", phase="publish", **observed)
            raise Q0Unavailable(
                "Q0 publisher did not reach Admitted against this broker: "
                f"{observed}")
        outcome["publish_stdout"] = observed["stdout"].strip()
        journal.append("publication_committed", durable_class="q0",
                       mode="pub", topic=Q0_TOPIC, payload=payload,
                       payload_digest=oracle.digest_hex(payload.encode()),
                       acknowledgment="ADMITTED",
                       client_stdout=observed["stdout"].strip(),
                       client_stderr=observed["stderr"].strip() or None)

        for index, subscriber in enumerate(subscribers):
            try:
                out, err = subscriber.communicate(timeout=TIMEOUT_CLIENT)
            except subprocess.TimeoutExpired:
                subscriber.kill()
                out, err = subscriber.communicate(timeout=TIMEOUT_CLIENT)
                journal.append("q0_not_run", phase="receive", subscriber=index,
                               timed_out=True, timeout_seconds=TIMEOUT_CLIENT,
                               subscriber_stdout=out, subscriber_stderr=err)
                raise Q0Unavailable(
                    f"Q0 subscriber {index} did not receive within "
                    f"{TIMEOUT_CLIENT}s: stdout={out!r} stderr={err!r}") from None
            expected = f"{Q0_TOPIC}: {payload}"
            journal.append("consumer_result", consumer="q0", subscriber=index,
                           returncode=subscriber.returncode,
                           expected_delivery=expected,
                           subscriber_stdout=out.strip(),
                           subscriber_stderr=err.strip() or None)
            if subscriber.returncode != 0 or out.strip() != expected:
                journal.append("q0_not_run", phase="receive", subscriber=index,
                               returncode=subscriber.returncode,
                               expected=expected, subscriber_stdout=out,
                               subscriber_stderr=err)
                raise Q0Unavailable(
                    f"Q0 subscriber {index} observed {out.strip()!r} with exit "
                    f"{subscriber.returncode}, expected {expected!r}")
            outcome["deliveries"].append(out.strip())
        return outcome
    finally:
        for subscriber in subscribers:
            if subscriber.poll() is None:
                subscriber.kill()
                subscriber.wait(timeout=TIMEOUT_CLIENT)


def publish_q1(journal, broker, sequence: int) -> dict:
    command = q1_command(sequence, incarnation=Q1_INCARNATION,
                         owner=Q1_OWNER_EPOCH)
    payload = f"event-{sequence}"
    journal.append("publication_attempt", durable_class="q1-local",
                   topic=Q1_TOPIC, producer=Q1_PRODUCER,
                   epoch=Q1_PRODUCER_EPOCH, sequence=sequence,
                   message_id=100 + sequence, incarnation=Q1_INCARNATION,
                   owner_epoch=Q1_OWNER_EPOCH, partition=partition_identity(),
                   payload=payload,
                   payload_digest=oracle.digest_hex(payload.encode()),
                   command=command)
    observed = run_bounded(command, broker.environment(), TIMEOUT_CLIENT)
    parts = observed["stdout"].split()
    valid = False
    if (not observed["timed_out"] and observed["returncode"] == 0
            and len(parts) == 3 and parts[0] == "COMMITTED_LOCAL"):
        try:
            valid = int(parts[1]) == Q1_INCARNATION
        except ValueError:
            valid = False
    if not valid:
        journal.append("client_failure", durable_class="q1-local",
                       sequence=sequence, **observed)
        raise AssertionError(
            f"Q1 client did not observe COMMITTED_LOCAL: {observed}")
    offset = int(parts[2])
    journal.append("publication_committed", durable_class="q1-local",
                   topic=Q1_TOPIC, acknowledgment="COMMITTED_LOCAL",
                   client_stdout=observed["stdout"].strip(),
                   client_stderr=observed["stderr"].strip() or None,
                   incarnation=Q1_INCARNATION, offset=offset,
                   sequence=sequence, message_id=100 + sequence,
                   partition=partition_identity(), payload=payload,
                   payload_digest=oracle.digest_hex(payload.encode()))
    return {"line": observed["stdout"].strip(), "incarnation": Q1_INCARNATION,
            "offset": offset}


def q2_publish_certain(campaign, sequence: int, message_id: int,
                       payload: str) -> dict:
    """Publish one Q2 token until a definite quorum acknowledgment is observed."""
    deadline = time.monotonic() + TIMEOUT_PUBLISH_CERTAIN
    while True:
        observed = campaign.publish(Q2_PRODUCER, Q2_PRODUCER_EPOCH, sequence,
                                    message_id, payload)
        if observed.get("outcome") == "committed_quorum":
            return observed
        if time.monotonic() >= deadline:
            raise AssertionError(
                f"Q2 publication {Q2_PRODUCER}:{Q2_PRODUCER_EPOCH}:{sequence} "
                f"never became certain: {observed!r}")
        time.sleep(0.3)  # Poll the observable acknowledgment, never a crash window.


def q2_messages(campaign) -> dict[int, list[dict]]:
    messages = {}
    for number in campaign.live():
        state = campaign.nodes[number].read_state()
        assert state is not None, f"voter {number} has no persisted state"
        messages[number] = list(state["messages"])
    return messages


def group_cursor(campaign, group: int) -> dict[int, object]:
    cursors = {}
    for number in campaign.live():
        path = campaign.nodes[number].directory / "state" / "state.json"
        state = json.loads(path.read_text(encoding="utf-8"))["state"]
        groups = state["partitions"][PARTITION_KEY].get("groups", {})
        cursors[number] = groups.get(str(group), {}).get("cursor")
    return cursors


def await_cursor(journal, campaign, group: int, expected) -> dict[int, object]:
    deadline = time.monotonic() + TIMEOUT_CURSOR
    while True:
        cursors = group_cursor(campaign, group)
        if cursors and all(value == expected for value in cursors.values()):
            journal.append("consumer_state", consumer="q2", kind="group_cursor",
                           group=group, expected=expected,
                           cursors={str(k): v for k, v in cursors.items()})
            return cursors
        if time.monotonic() >= deadline:
            journal.append("consumer_state", consumer="q2", kind="group_cursor",
                           group=group, expected=expected,
                           cursors={str(k): v for k, v in cursors.items()},
                           timed_out=True)
            raise AssertionError(
                f"group {group} cursor never reached {expected}: {cursors}")
        time.sleep(T_POLL)


def ack_arguments(token: dict) -> list[str]:
    """The exact q2_consumer ack argv for a delivery token (plus its label)."""
    return ["ack", *[str(token[key]) for key in (
        "group_id", "generation", "lease_id", "metadata_epoch",
        "incarnation", "offset", "attempt", "message_id")], "parent"]


def classify_ack(observed: dict) -> tuple[str, dict | None]:
    """Classify one ACK client result as committed, refused or uncertain.

    A parent-imposed timeout, a client-side timeout/deadline text and an
    OutcomeUnknown detail are all uncertain: none of them proves refusal, and
    none of them may ever be recorded as a commit.
    """
    if observed["timed_out"]:
        return "uncertain", None
    lines = [line for line in observed["stdout"].splitlines() if line.strip()]
    if observed["returncode"] != 0 or len(lines) != 1:
        return "uncertain", None
    try:
        parsed = json.loads(lines[0])
    except ValueError:
        return "uncertain", None
    if parsed.get("outcome") == "ack_committed":
        return "committed", parsed
    if parsed.get("outcome") == "error":
        detail = str(parsed.get("detail", "")).lower()
        if any(marker in detail for marker in (
                "timeout", "timed out", "deadline", "elapsed", "unknown")):
            return "uncertain", parsed
        return "refused", parsed
    return "uncertain", parsed


def lease_expired(parsed: dict | None) -> bool:
    detail = str((parsed or {}).get("detail", "")).lower()
    return "lease" in detail and "expir" in detail


def attempt_ack(journal, broker, token, *, phase: str, timeout: float,
                attempt_index: int = 1) -> dict:
    """Run one bounded ACK attempt and journal its exact client result first."""
    command = [str(Q2_CONSUMER), *ack_arguments(token)]
    observed = run_bounded(command, broker.environment(), timeout)
    classification, parsed = classify_ack(observed)
    journal.append("consumer_result", consumer="q2", phase=phase,
                   durable_class="q2-quorum", attempt_index=attempt_index,
                   classification=classification, parsed=parsed,
                   command=ack_arguments(token), **observed)
    return {"classification": classification, "parsed": parsed,
            "observed": observed}


def group_progress(campaign, group: int) -> dict[int, dict]:
    """Per-live-voter durable cursor and ACK outcomes for one group."""
    progress = {}
    for number in campaign.live():
        path = campaign.nodes[number].directory / "state" / "state.json"
        state = json.loads(path.read_text(encoding="utf-8"))["state"]
        entry = state["partitions"][PARTITION_KEY].get("groups", {}).get(
            str(group), {})
        progress[number] = {"cursor": entry.get("cursor"),
                            "outcomes": dict(entry.get("outcomes", {}))}
    return progress


def scenario_f1(journal) -> str:
    """F1: one broker serving Q0, Q1 and Q2 with three real Q2 voters healthy."""
    with tempfile.TemporaryDirectory(prefix="rovqet-c07-f1-") as temporary:
        directory = Path(temporary)
        cluster = directory / "q2"
        cluster.mkdir()
        local = directory / "q1"
        local.mkdir()
        campaign = Campaign(journal, cluster)
        primary = campaign.nodes[1]
        # Cluster-mode routing needs an explicit placement for the local Q1
        # partition as well as the Q2 placement supplied by Campaign.
        for node in campaign.nodes.values():
            metadata = json.loads(node.metadata.read_text())
            metadata["topics"].append(
                {"topic_id": Q1_TOPIC_ID, "durable_class": 1,
                 "policy_revision": 1})
            metadata["partitions"].append({
                **partition_identity(), "owner": 1,
                "ownership_epoch": Q1_OWNER_EPOCH, "lease_valid_for_ms": 600_000,
                "desired_replicas": 1, "shard_id": 8,
                "required_policy_revision": 1,
            })
            node.metadata.write_text(json.dumps(metadata), encoding="utf-8")
        broker = None
        try:
            # Reuse the real voter configuration, but let Broker supply its Q1
            # route, the Q0 default route and an ephemeral loopback listener.
            broker = Broker(local, incarnation=Q1_INCARNATION,
                            extra=primary.arguments(True)[3:])
            primary.process = broker.process
            primary.lines = broker.lines
            for number in (2, 3):
                campaign.nodes[number].start(bootstrap=False)
            broker.start()
            primary.client_port = int(broker.endpoint.rsplit(":", 1)[1])
            for node in campaign.nodes.values():
                node.wait_ready()
            primary.wait_authority()
            journal.append(
                "scenario_started", broker_endpoint=broker.endpoint,
                broker_pid=broker.process.pid,
                q1_route={"topic": Q1_TOPIC, "partition": partition_identity(),
                          "incarnation": Q1_INCARNATION,
                          "owner_epoch": Q1_OWNER_EPOCH},
                q2_route={"topic": Q2_TOPIC,
                          "partition": {"tenant_id": Q2_TENANT,
                                        "topic_id": Q2_TOPIC_ID,
                                        "partition_id": Q2_PARTITION},
                          "incarnation": INCARNATION,
                          "owner_epoch": Q2_OWNER},
                q0_route={"topic": Q0_TOPIC,
                          "class": "admission-only, no durable offset"},
                voters={str(number): str(campaign.nodes[number].directory)
                        for number in campaign.nodes},
                metadata="static --cluster-metadata JSON fixtures")

            # Q0: the q0_smoke.py round trip against this shared broker.
            q0 = exercise_q0(journal, broker)

            # Q1: two real SDK publishes; local offsets step by two.
            q1_first = publish_q1(journal, broker, 0)
            q1_second = publish_q1(journal, broker, 1)
            assert q1_second["offset"] == q1_first["offset"] + 2, (
                q1_first, q1_second)
            q1_offsets = [q1_first["offset"], q1_second["offset"]]

            # Q2: quorum publish through the real SDK example.
            digest = oracle.digest_hex(Q2_PAYLOAD.encode())
            observed = q2_publish_certain(campaign, 0, Q2_MESSAGE, Q2_PAYLOAD)
            q2_offset = observed["offset"]
            assert observed.get("incarnation") == INCARNATION, observed
            assert observed["outcome"] == "committed_quorum", observed
            q2_line = json.dumps(observed, sort_keys=True)
            states = campaign.await_message(q2_offset, Q2_MESSAGE)
            assert set(states) == {1, 2, 3}, states

            # Fetch the published payload through the real Q2 group path.
            fetch_observed = run_bounded(
                [str(Q2_CONSUMER), "fetch", str(Q2_GROUP), "0"],
                broker.environment(), TIMEOUT_CONSUMER)
            journal.append("consumer_fetch", consumer="q2", group=Q2_GROUP,
                           durable_class="q2-quorum", **fetch_observed)
            batch = parse_json_line(fetch_observed, "q2 fetch")
            assert batch.get("outcome") == "batch", batch
            assert len(batch["messages"]) == 1, batch
            token = batch["messages"][0]
            assert token["offset"] == q2_offset, (token, q2_offset)
            assert token["incarnation"] == INCARNATION, token
            fetched_digest = oracle.digest_hex(bytes.fromhex(token["payload"]))
            assert fetched_digest == digest, (fetched_digest, digest)
            journal.append("consumer_delivery", consumer="q2", token=token,
                           payload_digest=fetched_digest,
                           expected_digest=digest, logical_offset=q2_offset)

            # ACK the token through the Q2 group path.
            ack_command = ["ack", *[str(token[key]) for key in (
                "group_id", "generation", "lease_id", "metadata_epoch",
                "incarnation", "offset", "attempt", "message_id")], "parent"]
            ack_observed = run_bounded(
                [str(Q2_CONSUMER), *ack_command], broker.environment(),
                TIMEOUT_CONSUMER)
            journal.append("consumer_result", consumer="q2", group=Q2_GROUP,
                           durable_class="q2-quorum",
                           command=["ack", *ack_command[1:]], **ack_observed)
            ack = parse_json_line(ack_observed, "q2 ack")
            assert ack.get("outcome") == "ack_committed", ack
            assert ack.get("offset") == q2_offset, (ack, q2_offset)
            cursors = await_cursor(journal, campaign, Q2_GROUP, q2_offset + 1)

            # Producer-token dedup: the identical token returns the same offset
            # and creates no second record on any voter.
            before = q2_messages(campaign)
            assert all(len(messages) == 1 for messages in before.values()), before
            retry = q2_publish_certain(campaign, 0, Q2_MESSAGE, Q2_PAYLOAD)
            assert retry.get("offset") == q2_offset, (retry, q2_offset)
            after = q2_messages(campaign)
            assert {n: len(m) for n, m in after.items()} == \
                {n: len(m) for n, m in before.items()}, (before, after)
            assert all(all(message.get("offset") != q2_offset + 1
                           for message in messages)
                       for messages in after.values()), after
            refetch = run_bounded(
                [str(Q2_CONSUMER), "fetch", str(Q2_GROUP), "0"],
                broker.environment(), TIMEOUT_CONSUMER)
            journal.append("consumer_fetch", consumer="q2", group=Q2_GROUP,
                           purpose="dedup_verification", **refetch)
            refetch_batch = parse_json_line(refetch, "q2 dedup fetch")
            assert refetch_batch.get("outcome") == "batch", refetch_batch
            assert refetch_batch["messages"] == [], refetch_batch
            journal.append(
                "dedup_check", producer=Q2_PRODUCER, epoch=Q2_PRODUCER_EPOCH,
                sequence=0, message_id=Q2_MESSAGE, payload=Q2_PAYLOAD,
                original_offset=q2_offset, retry_offset=retry["offset"],
                retry_line=retry,
                message_counts_before={str(n): len(m)
                                       for n, m in before.items()},
                message_counts_after={str(n): len(m)
                                      for n, m in after.items()},
                record_at_next_offset=False, refetch_messages=[])

            # A second distinct Q2 token proves the dense quorum offset domain.
            second = q2_publish_certain(campaign, 1, Q2_MESSAGE_TWO,
                                        Q2_PAYLOAD_TWO)
            assert second["offset"] == q2_offset + 1, (second, q2_offset)
            second_states = campaign.await_message(second["offset"],
                                                   Q2_MESSAGE_TWO)
            assert set(second_states) == {1, 2, 3}, second_states
            assert all(len(messages) == 2
                       for messages in q2_messages(campaign).values())

            # Distinct identities and verbatim acknowledgement vocabularies.
            q1_identity = {"tenant_id": Q1_TENANT, "topic_id": Q1_TOPIC_ID,
                           "partition_id": Q1_PARTITION,
                           "incarnation": Q1_INCARNATION,
                           "offsets": q1_offsets, "offset_step": 2}
            q2_identity = {"tenant_id": Q2_TENANT, "topic_id": Q2_TOPIC_ID,
                           "partition_id": Q2_PARTITION,
                           "incarnation": INCARNATION,
                           "offsets": [q2_offset, second["offset"]],
                           "offset_step": 1}
            assert q1_identity["topic_id"] != q2_identity["topic_id"], (
                q1_identity, q2_identity)
            assert sorted(q1_offsets) != sorted(q2_identity["offsets"]), (
                q1_offsets, q2_identity["offsets"])
            assert q1_identity != q2_identity
            assert "COMMITTED_LOCAL" in q1_first["line"], q1_first["line"]
            q2_vocab = observed["outcome"].upper()
            assert q2_vocab == "COMMITTED_QUORUM", observed
            q0_text = q0["publish_stdout"] + " " + " ".join(q0["deliveries"])
            assert "COMMITTED_LOCAL" not in q0_text, q0_text
            assert "COMMITTED_QUORUM" not in q0_text, q0_text
            assert "committed_quorum" not in q0_text, q0_text
            vocabularies = {"q0": "ADMITTED", "q1": "COMMITTED_LOCAL",
                            "q2": "COMMITTED_QUORUM"}
            assert len(set(vocabularies.values())) == 3, vocabularies

            journal.append(
                "healthy_baseline",
                q0={"payload": Q0_PAYLOAD, "topic": Q0_TOPIC,
                    "publish_stdout": q0["publish_stdout"],
                    "deliveries": q0["deliveries"],
                    "subscription_lines": q0["subscription_lines"],
                    "acknowledgment": vocabularies["q0"],
                    "durable_class": "none (admission receipt, no offset)"},
                q1={"acknowledgment": vocabularies["q1"],
                    "client_lines": [q1_first["line"], q1_second["line"]],
                    "incarnation": Q1_INCARNATION, "offsets": q1_offsets,
                    "partition": partition_identity(), "durable_class": 1},
                q2={"acknowledgment": vocabularies["q2"],
                    "wire_outcome": observed["outcome"],
                    "client_line": q2_line, "incarnation": INCARNATION,
                    "logical_offset": q2_offset,
                    "second_offset": second["offset"],
                    "payload": Q2_PAYLOAD, "payload_digest": digest,
                    "fetched_digest": fetched_digest,
                    "fetch_token_offset": token["offset"],
                    "ack": ack, "ack_cursor": {str(n): v
                                               for n, v in cursors.items()},
                    "group": Q2_GROUP, "dedup_retry_offset": retry["offset"],
                    "partition": {"tenant_id": Q2_TENANT,
                                  "topic_id": Q2_TOPIC_ID,
                                  "partition_id": Q2_PARTITION},
                    "durable_class": 2},
                identities={"q1": q1_identity, "q2": q2_identity,
                            "distinct": True,
                            "note": "log incarnations are configured equal "
                                    "(42) in this MVP profile; separation is "
                                    "by partition (topic 3 vs 4) and offset "
                                    "domain (step-2 local vs dense quorum)"},
                vocabularies=vocabularies,
                voter_states={str(n): state.get("last_applied")
                              for n, state in second_states.items()},
                metadata_mode="static --cluster-metadata JSON fixtures")
        finally:
            if broker is not None:
                broker.close()
            for number in (2, 3):
                node = campaign.nodes[number]
                if node.process is None:
                    continue
                if node.process.poll() is None:
                    node.kill()
                if node.process.stderr is not None:
                    node.process.stderr.close()
    return (f"Q0 {q0['publish_stdout']} with {len(q0['deliveries'])} "
            f"subscriber deliveries; Q1 COMMITTED_LOCAL incarnation "
            f"{Q1_INCARNATION} offsets {q1_offsets}; Q2 COMMITTED_QUORUM "
            f"offset {q2_offset} digest {digest}, ACK cursor {cursors}, dedup "
            f"retry offset {retry['offset']} created no second record; "
            f"vocabularies {vocabularies}")


def scenario_f3b(journal) -> str:
    """F3b: a valid consumer ACK never commits while the Q2 quorum is lost."""
    with tempfile.TemporaryDirectory(prefix="rovqet-c07-f3b-") as temporary:
        directory = Path(temporary)
        cluster = directory / "q2"
        cluster.mkdir()
        local = directory / "q1"
        local.mkdir()
        campaign = Campaign(journal, cluster)
        primary = campaign.nodes[1]
        # Cluster-mode routing needs an explicit placement for the local Q1
        # partition as well as the Q2 placement supplied by Campaign.
        for node in campaign.nodes.values():
            metadata = json.loads(node.metadata.read_text())
            metadata["topics"].append(
                {"topic_id": Q1_TOPIC_ID, "durable_class": 1,
                 "policy_revision": 1})
            metadata["partitions"].append({
                **partition_identity(), "owner": 1,
                "ownership_epoch": Q1_OWNER_EPOCH, "lease_valid_for_ms": 600_000,
                "desired_replicas": 1, "shard_id": 8,
                "required_policy_revision": 1,
            })
            node.metadata.write_text(json.dumps(metadata), encoding="utf-8")
        broker = None
        try:
            broker = Broker(local, incarnation=Q1_INCARNATION,
                            extra=primary.arguments(True)[3:])
            primary.process = broker.process
            primary.lines = broker.lines
            for number in (2, 3):
                campaign.nodes[number].start(bootstrap=False)
            broker.start()
            primary.client_port = int(broker.endpoint.rsplit(":", 1)[1])
            for node in campaign.nodes.values():
                node.wait_ready()
            primary.wait_authority()
            journal.append(
                "scenario_started", broker_endpoint=broker.endpoint,
                broker_pid=broker.process.pid,
                objective="a valid consumer ACK must not produce ACK_COMMITTED "
                          "or cursor advancement while two of three Q2 voters "
                          "are dead",
                q1_route={"topic": Q1_TOPIC, "partition": partition_identity(),
                          "incarnation": Q1_INCARNATION,
                          "owner_epoch": Q1_OWNER_EPOCH},
                q2_route={"topic": Q2_TOPIC,
                          "partition": {"tenant_id": Q2_TENANT,
                                        "topic_id": Q2_TOPIC_ID,
                                        "partition_id": Q2_PARTITION},
                          "incarnation": INCARNATION,
                          "owner_epoch": Q2_OWNER},
                q0_route={"topic": Q0_TOPIC,
                          "class": "admission-only, no durable offset"},
                voters={str(number): str(campaign.nodes[number].directory)
                        for number in campaign.nodes},
                metadata="static --cluster-metadata JSON fixtures")

            # Step 1: healthy quorum publication, then a real delivery token
            # whose lease is held but not yet acknowledged.
            payload = F3B_PAYLOAD
            digest = oracle.digest_hex(payload.encode())
            published = q2_publish_certain(campaign, 0, F3B_MESSAGE, payload)
            offset = published["offset"]
            assert published.get("incarnation") == INCARNATION, published
            states = campaign.await_message(offset, F3B_MESSAGE)
            assert set(states) == {1, 2, 3}, states

            fetch_observed = run_bounded(
                [str(Q2_CONSUMER), "fetch", str(F3B_GROUP), "0"],
                broker.environment(), TIMEOUT_CONSUMER)
            journal.append("consumer_fetch", consumer="q2", group=F3B_GROUP,
                           durable_class="q2-quorum", **fetch_observed)
            batch = parse_json_line(fetch_observed, "f3b fetch")
            assert batch.get("outcome") == "batch", batch
            assert len(batch["messages"]) == 1, batch
            token = batch["messages"][0]
            assert token["offset"] == offset, (token, offset)
            assert token["incarnation"] == INCARNATION, token
            fetched_digest = oracle.digest_hex(bytes.fromhex(token["payload"]))
            assert fetched_digest == digest, (fetched_digest, digest)
            journal.append("consumer_delivery", consumer="q2", token=token,
                           payload_digest=fetched_digest,
                           expected_digest=digest, logical_offset=offset,
                           phase="pre_quorum_loss")
            cursor_before = group_progress(campaign, F3B_GROUP)
            journal.append("consumer_state", consumer="q2",
                           kind="group_progress", phase="pre_quorum_loss",
                           group=F3B_GROUP,
                           progress={str(number): entry for number, entry
                                     in cursor_before.items()})
            assert all(entry["cursor"] in (None, 0)
                       for entry in cursor_before.values()), cursor_before

            # Step 2: two of three Q2 voters die; only node 1 (the broker)
            # keeps running, so no Q2 data majority remains.
            stopped = []
            for number in (2, 3):
                node = campaign.nodes[number]
                pid = node.process.pid
                node.kill()
                code = node.process.poll()
                journal.append("voter_stop_observed", node=number, pid=pid,
                               signal="SIGKILL", returncode=code,
                               observation="Popen.wait completed; poll returned "
                                           "exit status",
                               storage=str(node.directory))
                assert code == -9, code
                stopped.append(number)
            assert campaign.live() == [1], campaign.live()
            journal.append(
                "quorum_loss_outcome",
                quorum_scope={
                    "authority": "Q2 data quorum",
                    "metadata": "static --cluster-metadata JSON fixtures; "
                                "metadata quorum not tested",
                    "configured_voters": [1, 2, 3], "required_majority": 2},
                stopped_voters=stopped,
                stopped_exit_codes={str(number): -9 for number in stopped},
                live_voters=campaign.live(),
                delivery_token_held=True)

            # Step 3: attempt the ACK with the held token; a parent timeout is
            # uncertain, never a commit and never proof of refusal.
            outage = attempt_ack(journal, broker, token,
                                 phase="quorum_loss_attempt",
                                 timeout=TIMEOUT_ACK)
            if outage["classification"] == "committed":
                raise AssertionError(
                    "consumer ACK committed without the Q2 data quorum: "
                    f"{outage}")
            if (outage["classification"] == "refused"
                    and lease_expired(outage["parsed"])):
                raise ScenarioInvalid(
                    "the delivery lease expired before the quorum-loss ACK "
                    f"attempt could be answered: {outage['parsed']}")

            # Step 4: nothing became acknowledged and the durable cursor did
            # not advance across the message while the quorum is gone.
            requeue = run_bounded(
                [str(Q2_CONSUMER), "fetch", str(F3B_GROUP), "0"],
                broker.environment(), TIMEOUT_ACK)
            journal.append("consumer_fetch", consumer="q2", group=F3B_GROUP,
                           purpose="post_quorum_loss_refetch", **requeue)
            progress_after = group_progress(campaign, F3B_GROUP)
            journal.append("consumer_state", consumer="q2",
                           kind="group_progress",
                           phase="post_quorum_loss_attempt", group=F3B_GROUP,
                           progress={str(number): entry for number, entry
                                     in progress_after.items()},
                           cursor_before={str(number): entry["cursor"]
                                          for number, entry
                                          in cursor_before.items()},
                           ack_classification=outage["classification"])
            for number, entry in progress_after.items():
                assert entry == cursor_before[number], (
                    cursor_before, progress_after)
                assert str(offset) not in entry["outcomes"], entry
            live_messages = q2_messages(campaign)
            assert all(any(message.get("offset") == offset
                           for message in messages)
                       for messages in live_messages.values()), live_messages

            # Step 5: Q1 still commits locally during the Q2 outage; Q0 runs
            # in the same process because it is cheap.
            q1 = publish_q1(journal, broker, 0)
            journal.append("q1_commits_without_q2_quorum",
                           offset=q1["offset"], client_stdout=q1["line"],
                           acknowledgment="COMMITTED_LOCAL",
                           incarnation=Q1_INCARNATION,
                           broker_pid=broker.process.pid,
                           live_q2_voters=campaign.live())
            try:
                exercise_q0(journal, broker, payload=F3B_Q0_PAYLOAD)
            except Q0Unavailable as error:
                # Q0 is opportunistic here; its own q0_not_run records already
                # carry the exact reason.
                journal.append("q0_outage_not_run", reason=str(error))

            # Step 6: restore both voters on their original storage, then
            # resolve the uncertain ACK identity with the identical token.
            for number in (2, 3):
                node = campaign.nodes[number]
                if node.process.stderr is not None:
                    node.process.stderr.close()
                node.start(bootstrap=False)
                node.wait_ready()
                journal.append("voter_restarted_on_original_storage",
                               node=number, storage=str(node.directory),
                               bootstrap=False)
            assert set(campaign.live()) == {1, 2, 3}, campaign.live()
            leader = campaign.observe_leader()

            # Step 6: retry the identical ACK now that the voters are back.
            # The durable state, not only the client echo, decides the
            # resolution: an OutcomeUnknown proposal may commit once quorum
            # returns even though no client ever saw ACK_COMMITTED.
            resolution = attempt_ack(journal, broker, token,
                                     phase="post_restore_resolution",
                                     timeout=TIMEOUT_CONSUMER,
                                     attempt_index=1)
            restored_progress = group_progress(campaign, F3B_GROUP)
            journal.append("consumer_state", consumer="q2",
                           kind="post_restore_state", group=F3B_GROUP,
                           client_classification=resolution["classification"],
                           progress={str(number): entry for number, entry
                                     in restored_progress.items()})

            # Step 7: the second retry of the identical identity is idempotent.
            idempotent = attempt_ack(journal, broker, token,
                                     phase="post_restore_idempotency",
                                     timeout=TIMEOUT_CONSUMER,
                                     attempt_index=2)
            claimed = [attempt for attempt in (resolution, idempotent)
                       if attempt["classification"] == "committed"]
            for attempt in claimed:
                assert attempt["parsed"].get("offset") == offset, attempt
            state_progress = group_progress(campaign, F3B_GROUP)
            advanced = any(entry["cursor"] not in (None, 0)
                           or str(offset) in entry["outcomes"]
                           for entry in state_progress.values())
            if claimed or advanced:
                # The identity committed: every live voter must show exactly
                # one commit for it, with the cursor including the offset.
                cursors = await_cursor(journal, campaign, F3B_GROUP,
                                       offset + 1)
                final_progress = group_progress(campaign, F3B_GROUP)
                assert bool(final_progress) and all(
                    entry["cursor"] == offset + 1
                    and set(entry["outcomes"]) == {str(offset)}
                    for entry in final_progress.values()), final_progress
                resolution_kind = "committed"
                resolution_detail = (
                    f"committed exactly once at offset {offset} with durable "
                    f"cursor {cursors} (client outcomes "
                    f"{resolution['classification']} then "
                    f"{idempotent['classification']})")
            else:
                assert resolution["classification"] in ("refused",
                                                        "uncertain"), \
                    resolution
                assert idempotent["classification"] in ("refused",
                                                        "uncertain"), \
                    idempotent
                final_progress = state_progress
                assert all(entry["cursor"] in (None, 0)
                           and str(offset) not in entry["outcomes"]
                           for entry in final_progress.values()), final_progress
                resolution_kind = resolution["classification"]
                resolution_detail = (
                    f"{resolution_kind} on both retries "
                    f"({(resolution['parsed'] or {}).get('detail') or 'client timeout'} / "
                    f"{(idempotent['parsed'] or {}).get('detail') or 'client timeout'}); "
                    "durable cursor did not advance")

            records_after = q2_messages(campaign)
            assert all(len(messages) == 1
                       for messages in records_after.values()), records_after
            assert all(all(message.get("offset") != offset + 1
                           for message in messages)
                       for messages in records_after.values()), records_after
            journal.append("consumer_state", consumer="q2",
                           kind="idempotency_summary", group=F3B_GROUP,
                           first_resolution=resolution["classification"],
                           second_retry=idempotent["classification"],
                           resolution_kind=resolution_kind,
                           cursor={str(number): entry["cursor"]
                                   for number, entry in final_progress.items()},
                           record_counts={str(number): len(messages)
                                          for number, messages
                                          in records_after.items()})

            before_cursor = {str(number): entry["cursor"] for number, entry
                             in cursor_before.items()}
            after_cursor = {str(number): entry["cursor"] for number, entry
                            in progress_after.items()}
            outage_detail = (outage["parsed"] or {}).get("detail") or (
                outage["observed"]["stdout"].strip() or "client timeout")
        finally:
            if broker is not None:
                broker.close()
            for number in (2, 3):
                node = campaign.nodes[number]
                if node.process is None:
                    continue
                if node.process.poll() is None:
                    node.kill()
                if node.process.stderr is not None:
                    node.process.stderr.close()
    return (f"delivery token at offset {offset} ACKed with 1/3 Q2 voters "
            f"alive was {outage['classification']} ({outage_detail}); cursor "
            f"{before_cursor} -> {after_cursor}; Q1 {q1['line']} during the "
            f"outage; voters {stopped} restored with leader {leader}; "
            f"identical ACK resolved {resolution_detail}; second retry "
            f"{idempotent['classification']} kept "
            f"{len(next(iter(records_after.values())))} record per voter")


class MetadataJournal:
    """Scope every reused observation to metadata, never the Q2 data quorum."""

    def __init__(self, journal):
        self.journal = journal

    def append(self, event, **fields):
        return self.journal.append(
            event, metadata_quorum={
                "domain": "metadata-Raft", "configured_voters": [1, 2, 3],
                "required_majority": 2, "q2_data_quorum": "not exercised"},
            **fields)


class LiveMetadata:
    """Two command-capable voters plus the broker as the third metadata voter."""

    def __init__(self, directory, journal):
        self.directory = directory
        self.journal = MetadataJournal(journal)
        self.ports = {number: free_port() for number in (1, 2, 3)}
        self.nodes = [
            MetadataVoter(number, self.ports[number], self.ports,
                          directory / f"metadata-{number}")
            for number in (1, 2)]
        self.broker = None
        self.captured = {1: 0, 2: 0}

    def observations(self):
        events = []
        for node in self.nodes:
            history = list(node.history)
            for event in history[self.captured[node.node_id]:]:
                self.journal.append("metadata_revision" if
                                    event.get("event") == "applied" else
                                    "metadata_observation", evidence=event)
            self.captured[node.node_id] = len(history)
            if node.process is not None:
                events.extend(history)
        return events

    def wait(self, predicate, description, timeout=30):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            result = predicate(self.observations())
            if result:
                return result
            time.sleep(0.02)  # Poll observable events; elapsed time is not proof.
        raise AssertionError(f"metadata deadline: {description}")

    def leader(self, after_term=0):
        return self.wait(
            lambda events: next(
                (event for event in reversed(events)
                 if event.get("event") in ("leader", "follower")
                 and event["term"] > after_term), None),
            "leader transition")

    def propose(self, leader_id, command):
        node = next(node for node in self.nodes if node.node_id == leader_id)
        response = node.propose(command, timeout=35)
        self.journal.append("metadata_proposal", command=command,
                            response=response, node=leader_id)
        assert response is not None and response.get("ok"), response
        return response["response"]["Applied"]["revision"]

    def placement(self, revision):
        return self.wait(
            lambda events: next(
                (event for event in reversed(events)
                 if event.get("event") == "applied"
                 and event["revision"] >= revision
                 and event.get("placements")), None),
            f"committed placement at revision {revision}")

    def start(self):
        for node in self.nodes:
            node.start()
        initial = self.leader()
        leader_id = initial["leader"]
        # Bootstrap has two live voters; start the Q1 owner only after the
        # command-capable voters have elected a leader.
        local = self.directory / "broker"
        local.mkdir()
        args = [
            "--cluster-id", "990011", "--node-id", "3",
            "--cluster-listen", f"127.0.0.1:{free_port()}",
            "--metadata-raft", "true",
            "--metadata-raft-dir", str(local / "metadata"),
            "--metadata-raft-listen", f"127.0.0.1:{self.ports[3]}",
            "--metadata-raft-bootstrap", "false",
        ]
        for number, port in self.ports.items():
            args += ["--metadata-raft-peer", f"{number}=127.0.0.1:{port}"]
        self.broker = Broker(local, extra=args)
        self.broker.start()
        self.propose(leader_id, {"SetNode": {
            "node": 3, "address": self.broker.endpoint}})
        self.propose(leader_id, {"SetTopicPolicy": {
            "topic_id": 3, "durable_class": 1, "policy_revision": 1}})
        revision = self.propose(leader_id, {"SetPlacement": {
            **partition_identity(), "owner": 3, "ownership_epoch": 7,
            "lease_valid_for_ms": 60000}})
        placement = self.placement(revision)
        with self.broker.condition:
            ready = self.broker.condition.wait_for(
                lambda: any("Q1 route active:" in line
                            for line in self.broker.lines), timeout=30)
        assert ready, self.broker.lines
        self.journal.append(
            "scenario_started", broker_endpoint=self.broker.endpoint,
            placement=placement, leader=initial,
            profile="loopback plaintext disposable research prototype",
            prior_evidence={
                "stale_epoch": "C-04 scenario 6",
                "stale_generation": "C-05 fencing",
                "handover_epoch_7_to_8": "C-02 D2 journal"})
        self.journal.append("authority_observed",
                            evidence=list(self.broker.lines),
                            committed_placement=placement)
        return initial, placement

    def close(self):
        if self.broker is not None:
            self.broker.close()
            self.journal.append("broker_output",
                                stderr=list(self.broker.lines))
        for node in self.nodes:
            node.kill()
        self.observations()


def scenario_f7a(journal):
    """A real metadata election does not mint a new ownership epoch."""
    with tempfile.TemporaryDirectory(prefix="rovqet-c07-f7a-") as temporary:
        live = LiveMetadata(Path(temporary), journal)
        try:
            initial, before = live.start()
            first = publish_q1(live.journal, live.broker, 0)
            old_id = initial["leader"]
            old = next(node for node in live.nodes if node.node_id == old_id)
            pid = old.process.pid
            old.kill()
            live.journal.append("broker_killed", role="metadata-voter",
                                node=old_id, pid=pid, signal="SIGKILL",
                                returncode=-9, storage=str(old.directory))
            replacement = live.leader(after_term=initial["term"])
            new_id = replacement["leader"]
            assert new_id != old_id, replacement
            live.journal.append("leader_observed", old_leader=old_id,
                                new_leader=new_id, evidence=replacement)
            # No metadata command is issued after the kill: only an election.
            # The survivor's committed state is observed after the transition.
            survivor = next(node for node in live.nodes
                            if node.process is not None)
            after = next(event for event in reversed(survivor.history)
                         if event.get("event") == "applied"
                         and event["revision"] >= before["revision"])
            assert after["placements"] == before["placements"], (before, after)
            epoch_before = before["placements"][0]["ownership_epoch"]
            epoch_after = after["placements"][0]["ownership_epoch"]
            assert epoch_before == epoch_after == Q1_OWNER_EPOCH, (before, after)
            second = publish_q1(live.journal, live.broker, 1)
            assert second["offset"] != first["offset"], (first, second)
            live.journal.append("metadata_revision", phase="after_election",
                                evidence=after, epoch_before=epoch_before,
                                epoch_after=epoch_after,
                                placement_unchanged=True,
                                q1_before=first, q1_after=second)
            return (f"metadata leader {old_id}->{new_id}, term "
                    f"{initial['term']}->{replacement['term']}; committed "
                    f"placement unchanged, epoch 7->7; {first['line']}; "
                    f"{second['line']} within 60000 ms cached authority")
        finally:
            live.close()


def scenario_f7b(journal):
    """Never infer non-DLQ Q1 authority expiry from elapsed wall time."""
    with tempfile.TemporaryDirectory(prefix="rovqet-c07-f7b-") as temporary:
        live = LiveMetadata(Path(temporary), journal)
        try:
            initial, _ = live.start()
            revision = live.propose(initial["leader"], {"SetPlacement": {
                **partition_identity(), "owner": 3, "ownership_epoch": 7,
                "lease_valid_for_ms": 200}})
            committed = live.placement(revision)
            live.journal.append(
                "authority_observed", state="short_lease_committed",
                evidence=committed, lease_ms=200,
                expiry_observed=False, installation_observed=False)
            raise ScenarioInvalid(
                "200 ms placement committed in live metadata Raft, but the "
                "non-DLQ Q1 route exposes neither an authority-install update "
                "diagnostic nor an expired-state observation. main.rs logs "
                "'Q1 DLQ authority' only for a configured DLQ; the ordinary "
                "'Q1 authority' line is startup-only. No elapsed-time inference, "
                "publication refusal, or no-record proof is claimed; unsupported "
                "Q1 lease-expiry migration fallback not used.")
        finally:
            live.close()


SCENARIOS = ((F1, scenario_f1), (F3B, scenario_f3b),
             (F7A, scenario_f7a), (F7B, scenario_f7b))


def main() -> int:
    missing = [str(path) for path in
               (SERVER, Q1, Q0, EXAMPLE, Q2_CONSUMER) if not path.is_file()]
    EVIDENCE.mkdir(parents=True, exist_ok=True)
    journal = oracle.Journal(EVIDENCE / "fault-gate.jsonl",
                             "c07-cluster-fault-gate")
    verdicts: dict[str, str] = {}
    try:
        for name, scenario in SCENARIOS:
            scenario_missing = missing + (
                [str(METADATA_NODE)] if name in (F7A, F7B)
                and not METADATA_NODE.is_file() else [])
            if scenario_missing:
                verdict, reason = oracle.NOT_RUN, (
                    "binaries not built: " + ", ".join(scenario_missing))
            else:
                scoped = ScenarioJournal(journal, name)
                try:
                    reason = scenario(scoped)
                    verdict = oracle.PASS
                except Q0Unavailable as error:
                    verdict = oracle.NOT_RUN
                    reason = str(error)
                except ScenarioInvalid as error:
                    verdict = oracle.INVALID
                    reason = str(error)
                except BaseException as error:
                    verdict = oracle.FAIL
                    reason = f"{type(error).__name__}: {error}"
                    journal.append("scenario_failure", scenario=name,
                                   detail=traceback.format_exc())
            if name in (F7A, F7B):
                journal.conclusion(
                    verdict, reason, scenario=name,
                    metadata_quorum={"domain": "metadata-Raft",
                                     "configured_voters": [1, 2, 3],
                                     "required_majority": 2,
                                     "q2_data_quorum": "not exercised"})
            else:
                journal.conclusion(verdict, reason, scenario=name)
            verdicts[name] = verdict
            print(f"{verdict} {name}: {reason}", flush=True)
    finally:
        journal.close()
    return 0 if all(verdict == oracle.PASS
                    for verdict in verdicts.values()) else 1


if __name__ == "__main__":
    sys.exit(main())
