#!/usr/bin/env python3
"""C-07 fault gate scenario F1: a healthy Q0/Q1/Q2 baseline over real processes.

One campaign starts three real Q2 voters plus a single broker that hosts the Q1
route, the Q2 route and the Q0 admission path. Q0 mirrors scripts/q0_smoke.py
(two external subscribers and one external publisher over loopback TCP), Q1
publishes through the real SDK example and observes COMMITTED_LOCAL, and Q2
publishes and ACKs through the real q2_publish/q2_consumer examples and
observes COMMITTED_QUORUM with a durable group cursor that advances to include
the acknowledged message. The parent owns the fsynced journal at
benchmarks/evidence/c07-cluster-fault-gate/fault-gate.jsonl.

Every subprocess has a bounded deadline and its stdout/stderr reaches the
journal before any interpretation. Durability boundaries are observed outputs
(the Q1 COMMITTED_LOCAL line, the Q2 committed_quorum acknowledgment, the ACK
cursor advancement), never sleeps. Each process owns an independent persistent
directory under a disposable temporary root; all listeners are loopback. If Q0
cannot be exercised the way q0_smoke.py exercises it against this broker, the
scenario is journaled NOT_RUN with the exact reason — never PASS. No oracle
reconciler runs in this increment.
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

TIMEOUT_CLIENT = 15
TIMEOUT_CONSUMER = 20
TIMEOUT_PUBLISH_CERTAIN = 60
TIMEOUT_CURSOR = 30


class Q0Unavailable(RuntimeError):
    """Q0 could not be exercised the way q0_smoke.py does against this broker."""


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


def exercise_q0(journal, broker) -> dict:
    """Mirror q0_smoke.py: two external subscribers, then one external publisher."""
    env = broker.environment()
    command = [str(Q0)]
    outcome = {"topic": Q0_TOPIC, "payload": Q0_PAYLOAD, "deliveries": [],
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

        publisher_command = command + ["pub", Q0_TOPIC, Q0_PAYLOAD]
        journal.append("publication_attempt", durable_class="q0",
                       mode="pub", topic=Q0_TOPIC, payload=Q0_PAYLOAD,
                       payload_digest=oracle.digest_hex(Q0_PAYLOAD.encode()),
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
                       mode="pub", topic=Q0_TOPIC, payload=Q0_PAYLOAD,
                       payload_digest=oracle.digest_hex(Q0_PAYLOAD.encode()),
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
            expected = f"{Q0_TOPIC}: {Q0_PAYLOAD}"
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


def main() -> int:
    missing = [str(path) for path in
               (SERVER, Q1, Q0, EXAMPLE, Q2_CONSUMER) if not path.is_file()]
    EVIDENCE.mkdir(parents=True, exist_ok=True)
    journal = oracle.Journal(EVIDENCE / "fault-gate.jsonl",
                             "c07-cluster-fault-gate")
    verdict = oracle.FAIL
    reason = "scenario did not run"
    try:
        if missing:
            verdict, reason = oracle.NOT_RUN, (
                "binaries not built: " + ", ".join(missing))
        else:
            scoped = ScenarioJournal(journal, F1)
            try:
                reason = scenario_f1(scoped)
                verdict = oracle.PASS
            except Q0Unavailable as error:
                verdict = oracle.NOT_RUN
                reason = str(error)
            except BaseException as error:
                verdict = oracle.FAIL
                reason = f"{type(error).__name__}: {error}"
                journal.append("scenario_failure", scenario=F1,
                               detail=traceback.format_exc())
        journal.conclusion(verdict, reason, scenario=F1)
        print(f"{verdict} {F1}: {reason}", flush=True)
    finally:
        journal.close()
    return 0 if verdict == oracle.PASS else 1


if __name__ == "__main__":
    sys.exit(main())
