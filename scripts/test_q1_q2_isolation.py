#!/usr/bin/env python3
"""C-06 isolation: Q1 history and same-process Q2 quorum-loss isolation.

Three scenarios over real TCP with external SDK clients, reusing the Q1 process
harness from q1_smoke. A SIGKILL restart must preserve the incarnation, the
logical offsets and producer dedup, while a destroyed history must refuse
recovery explicitly; only an explicit new-log initialization may start a
distinct fresh incarnation that does not honour the old token or cursor.
The quorum-loss scenario shares one broker between Q1 and Q2 and restores the
two killed Q2 voters on their original storage. Metadata uses static fixtures.

Destructive work is confined to disposable temporary directories; the parent
owns the fsynced journal at
benchmarks/evidence/c06-q1-q2-isolation/isolation.jsonl. SIGKILL is process
crash recovery, not physical power-loss certification.
"""
from __future__ import annotations

import subprocess
import sys
import tempfile
import json
import time
from pathlib import Path

import q1_oracle as oracle
from q1_smoke import Q1, READ, SERVER, Broker, manifest_record_count
from test_q2_publish import Campaign, EXAMPLE, INCARNATION, OWNER_EPOCH as Q2_OWNER

EVIDENCE = (Path(__file__).resolve().parents[1] / "benchmarks" / "evidence"
            / "c06-q1-q2-isolation")

S1 = "q1-restart-preserves-incarnation-and-offsets"
S2 = "q1-unrecoverable-history-fails-closed-and-fresh-incarnation-is-distinct"
S3 = "q2-quorum-loss-blocks-quorum-ack-while-q1-commits"

TOPIC = "durable"
TENANT_ID, TOPIC_ID, PARTITION_ID = 1, 3, 0
DURABLE_CLASS = 1
PRODUCER, PRODUCER_EPOCH = 11, 0
OLD_INCARNATION, NEW_INCARNATION, OWNER_EPOCH = 42, 43, 7


def partition_identity() -> dict:
    return {"tenant_id": TENANT_ID, "topic_id": TOPIC_ID, "partition_id": PARTITION_ID}


def committed_local(result: subprocess.CompletedProcess, incarnation: int,
                    offset: int) -> int:
    assert result.returncode == 0, (result.stdout, result.stderr)
    parts = result.stdout.split()
    assert len(parts) == 3 and parts[0] == "COMMITTED_LOCAL", result.stdout
    observed = (int(parts[1]), int(parts[2]))
    assert observed == (incarnation, offset), (result.stdout, incarnation, offset)
    return observed[1]


def read_records(broker: Broker, incarnation: int, offset: int = 0,
                 count: int = 64) -> tuple[subprocess.CompletedProcess, list[dict], list[str] | None]:
    result = subprocess.run(
        [str(READ), TOPIC, str(TENANT_ID), str(TOPIC_ID), str(PARTITION_ID),
         str(incarnation), str(offset), str(count), "65536", "0"],
        env=broker.environment(), capture_output=True, text=True, timeout=30)
    records, end = [], None
    for line in result.stdout.splitlines():
        parts = line.split()
        if not parts:
            continue
        if parts[0] == "READ_RECORD":
            records.append({
                "incarnation": int(parts[1]),
                "offset": int(parts[2]),
                "message_id": int(parts[3]),
                "payload_digest": oracle.digest_hex(bytes.fromhex(parts[4])),
            })
        elif parts[0] == "READ_END":
            end = parts
    return result, records, end


def publish(broker: Broker, sequence: int, incarnation: int) -> subprocess.CompletedProcess:
    return broker.publish(sequence, incarnation=incarnation, owner=OWNER_EPOCH)


def scenario_restart(journal: oracle.Journal) -> str:
    """S1: a SIGKILL restart keeps incarnation, offsets and producer dedup."""
    name = S1
    with tempfile.TemporaryDirectory(prefix="rovqet-c06-restart-") as temporary:
        directory = Path(temporary)
        broker = Broker(directory, incarnation=OLD_INCARNATION)
        try:
            broker.start()
            journal.append(
                "broker_started", scenario=name, endpoint=broker.endpoint,
                topic=TOPIC, durable_class=DURABLE_CLASS,
                partition=partition_identity(), incarnation=OLD_INCARNATION,
                owner_epoch=OWNER_EPOCH, initialize_new_log=True,
            )
            offsets = []
            for sequence in range(3):
                result = publish(broker, sequence, OLD_INCARNATION)
                offset = committed_local(result, OLD_INCARNATION, sequence * 2)
                offsets.append(offset)
                journal.append(
                    "publish_ack", scenario=name, sequence=sequence,
                    producer=PRODUCER, epoch=PRODUCER_EPOCH,
                    message_id=100 + sequence, incarnation=OLD_INCARNATION,
                    offset=offset, client_stdout=result.stdout.strip(),
                    payload_digest=oracle.digest_hex(f"event-{sequence}".encode()),
                    durable_class=DURABLE_CLASS, partition=partition_identity(),
                )
            assert offsets == [0, 2, 4], offsets
            count_before = manifest_record_count(broker.wal)
            journal.append(
                "offsets_journaled", scenario=name, offsets=offsets,
                incarnation=OLD_INCARNATION, manifest_records=count_before,
            )
            broker.kill()
            journal.append(
                "broker_killed", scenario=name, signal="SIGKILL",
                incarnation=OLD_INCARNATION, offsets=offsets,
                manifest_records=count_before,
            )
        finally:
            broker.close()

        # Same data dir, same incarnation and owner epoch, no new-log flag.
        broker = Broker(directory, incarnation=OLD_INCARNATION)
        try:
            broker.start()
            journal.append(
                "broker_restarted", scenario=name, endpoint=broker.endpoint,
                incarnation=OLD_INCARNATION, owner_epoch=OWNER_EPOCH,
                same_data_dir=True, initialize_new_log=False,
            )
            result, records, end = read_records(broker, OLD_INCARNATION)
            assert result.returncode == 0, (result.stdout, result.stderr)
            assert end is not None and end[1] == str(OLD_INCARNATION), end
            assert [record["offset"] for record in records] == offsets, records
            assert [record["message_id"] for record in records] == [100, 101, 102], records
            expected = [oracle.digest_hex(f"event-{sequence}".encode())
                        for sequence in range(3)]
            assert [record["payload_digest"] for record in records] == expected, records
            assert all(record["incarnation"] == OLD_INCARNATION
                       for record in records), records
            # The recovered history is non-empty and starts where it did: the
            # restarted owner did not begin an empty replacement history.
            assert records and records[0]["offset"] == 0, records
            count_recovered = manifest_record_count(broker.wal)
            assert count_recovered == count_before, (count_before, count_recovered)
            journal.append(
                "history_recovered", scenario=name,
                offsets=[record["offset"] for record in records],
                incarnations=sorted({record["incarnation"] for record in records}),
                record_count=len(records), manifest_records=count_recovered,
                client_stdout=result.stdout.splitlines(), read_end=end,
            )
            # Dedup: the same producer token returns the same committed offset
            # and appends no new record.
            retry = publish(broker, 0, OLD_INCARNATION)
            retry_offset = committed_local(retry, OLD_INCARNATION, offsets[0])
            assert retry_offset == offsets[0], retry.stdout
            count_after_retry = manifest_record_count(broker.wal)
            assert count_after_retry == count_before, (count_before, count_after_retry)
            _, after_records, after_end = read_records(broker, OLD_INCARNATION)
            assert [record["offset"] for record in after_records] == offsets, after_records
            assert after_end is not None and after_end[1] == str(OLD_INCARNATION), after_end
            journal.append(
                "dedup_retry", scenario=name, sequence=0, producer=PRODUCER,
                epoch=PRODUCER_EPOCH, incarnation=OLD_INCARNATION,
                offset=retry_offset, client_stdout=retry.stdout.strip(),
                manifest_records=count_after_retry,
                record_count=len(after_records),
            )
        finally:
            broker.close()
    return (f"3 records retained at offsets {offsets} under incarnation "
            f"{OLD_INCARNATION} after SIGKILL (manifest records {count_before}); "
            f"retry of producer {PRODUCER}/seq 0 returned offset {retry_offset} "
            f"with the manifest unchanged")


def scenario_fail_closed(journal: oracle.Journal) -> str:
    """S2: destroyed history refuses recovery; a fresh incarnation is distinct."""
    name = S2
    with tempfile.TemporaryDirectory(prefix="rovqet-c06-failclosed-") as temporary:
        directory = Path(temporary)
        broker = Broker(directory, incarnation=OLD_INCARNATION)
        try:
            broker.start()
            journal.append(
                "broker_started", scenario=name, endpoint=broker.endpoint,
                topic=TOPIC, durable_class=DURABLE_CLASS,
                partition=partition_identity(), incarnation=OLD_INCARNATION,
                owner_epoch=OWNER_EPOCH, initialize_new_log=True,
            )
            old_offsets = []
            for sequence in range(2):
                result = publish(broker, sequence, OLD_INCARNATION)
                offset = committed_local(result, OLD_INCARNATION, sequence * 2)
                old_offsets.append(offset)
                journal.append(
                    "publish_ack", scenario=name, sequence=sequence,
                    producer=PRODUCER, epoch=PRODUCER_EPOCH,
                    message_id=100 + sequence, incarnation=OLD_INCARNATION,
                    offset=offset, client_stdout=result.stdout.strip(),
                    payload_digest=oracle.digest_hex(f"event-{sequence}".encode()),
                    durable_class=DURABLE_CLASS, partition=partition_identity(),
                )
            assert old_offsets == [0, 2], old_offsets
            count_before = manifest_record_count(broker.wal)
            broker.stop()
            journal.append(
                "broker_stopped", scenario=name, signal="SIGINT",
                returncode=broker.process.returncode,
                incarnation=OLD_INCARNATION, offsets=old_offsets,
                manifest_records=count_before,
            )
        finally:
            broker.close()

        # Destroy every durable history file under the q1 data dir: the WAL
        # history is the MANIFEST plus its segment files (SERVER.lock is not
        # history but must go so explicit re-initialization sees an empty dir).
        wal = directory / "wal"
        removed = sorted(path for path in wal.iterdir() if path.is_file())
        names = [path.name for path in removed]
        assert "MANIFEST" in names, names
        assert any(name.startswith("segment-") for name in names), names
        for path in removed:
            path.unlink()
        assert list(wal.iterdir()) == [], sorted(
            path.name for path in wal.iterdir())
        journal.append(
            "history_destroyed", scenario=name, wal_dir=str(wal),
            removed_paths=[str(path) for path in removed],
            incarnation=OLD_INCARNATION, offsets=old_offsets,
            manifest_records=count_before,
        )

        # Recovery without initialization must fail closed and must not
        # silently create a replacement log that could advertise old offsets.
        broker = Broker(directory, incarnation=OLD_INCARNATION)
        try:
            diagnostics = broker.reject_startup()
        finally:
            broker.close()
        exit_code = broker.process.returncode
        assert exit_code not in (0, None), diagnostics
        lines = [line for line in diagnostics.splitlines() if line.strip()]
        assert lines, diagnostics
        replay_error = next(
            (line for line in lines if "REPLAY_NOT_AVAILABLE" in line), None)
        assert replay_error is not None, diagnostics
        assert not any("listening on" in line for line in lines), diagnostics
        assert not (wal / "MANIFEST").exists(), \
            "recovery silently created a replacement log"
        journal.append(
            "recovery_refused", scenario=name, exit_code=exit_code,
            error_text=diagnostics.strip()[:500],
            replay_not_available_line=replay_error,
            incarnation=OLD_INCARNATION, offsets=old_offsets,
            manifest_recreated=False,
        )

        # Explicit new-log initialization with a fresh incarnation.
        broker = Broker(directory, incarnation=NEW_INCARNATION,
                        extra=("--q1-initialize-new-log", "true"))
        try:
            broker.start()
            journal.append(
                "broker_started", scenario=name, endpoint=broker.endpoint,
                topic=TOPIC, durable_class=DURABLE_CLASS,
                partition=partition_identity(), incarnation=NEW_INCARNATION,
                owner_epoch=OWNER_EPOCH, initialize_new_log=True,
                supersedes_incarnation=OLD_INCARNATION,
            )
            fresh_records = manifest_record_count(broker.wal)
            assert fresh_records == 0, fresh_records
            for offset in old_offsets:
                _, records, _ = read_records(broker, NEW_INCARNATION, offset=offset)
                assert records == [], (offset, records)
            old_read, old_cursor_records, old_cursor_end = read_records(
                broker, OLD_INCARNATION)
            assert old_cursor_records == [], (
                old_cursor_records, old_read.stdout, old_read.stderr)
            journal.append(
                "old_offsets_not_replayable", scenario=name,
                incarnation=OLD_INCARNATION, offsets=old_offsets,
                manifest_records=fresh_records,
                old_cursor_returncode=old_read.returncode,
                old_cursor_records=len(old_cursor_records),
                old_cursor_stderr=old_read.stderr.strip()[:400] or None,
                old_cursor_read_end=old_cursor_end,
            )
            # The old producer token must append a fresh record under the new
            # incarnation: a surviving dedup outcome would acknowledge the old
            # incarnation's offset and append nothing.
            result = publish(broker, 0, NEW_INCARNATION)
            fresh_offset = committed_local(result, NEW_INCARNATION, 0)
            count_after = manifest_record_count(broker.wal)
            assert count_after == 2, count_after
            new_read, new_records, new_end = read_records(broker, NEW_INCARNATION)
            assert new_read.returncode == 0, (new_read.stdout, new_read.stderr)
            assert [record["offset"] for record in new_records] == [fresh_offset], \
                new_records
            assert [record["message_id"] for record in new_records] == [100], \
                new_records
            assert [record["payload_digest"] for record in new_records] == \
                [oracle.digest_hex(b"event-0")], new_records
            assert all(record["incarnation"] == NEW_INCARNATION
                       for record in new_records), new_records
            assert new_end is not None and new_end[1] == str(NEW_INCARNATION), new_end
            _, old_after, _ = read_records(broker, OLD_INCARNATION)
            assert all(record["incarnation"] != OLD_INCARNATION
                       for record in new_records + old_after), \
                (new_records, old_after)
            journal.append(
                "fresh_incarnation_publish", scenario=name,
                old_incarnation=OLD_INCARNATION, new_incarnation=NEW_INCARNATION,
                old_offsets=old_offsets, fresh_offset=fresh_offset,
                producer=PRODUCER, epoch=PRODUCER_EPOCH, sequence=0,
                client_stdout=result.stdout.strip(),
                manifest_records_before=fresh_records,
                manifest_records_after=count_after,
                recovered=[{**record, "payload_digest": record["payload_digest"]}
                           for record in new_records],
                read_end=new_end,
            )
        finally:
            broker.close()
    return (f"recovery without initialization refused with "
            f"{replay_error!r} (exit {exit_code}); fresh incarnation "
            f"{NEW_INCARNATION} started with {fresh_records} records, reports "
            f"nothing at old offsets {old_offsets}, and accepted the old "
            f"producer token as a fresh record at offset {fresh_offset} "
            f"(manifest records {count_after})")


def scenario_quorum_loss(journal: oracle.Journal) -> str:
    """Stop two data voters; the surviving broker must still commit its Q1 route."""
    class ScenarioJournal:
        # Include the scenario even on observations emitted by the reused launcher.
        def append(self, event, **fields):
            return journal.append(event, scenario=S3, **fields)

    scoped = ScenarioJournal()
    with tempfile.TemporaryDirectory(prefix="rovqet-c06-quorum-") as temporary:
        directory = Path(temporary)
        cluster = directory / "q2"
        cluster.mkdir()
        local = directory / "q1"
        local.mkdir()
        campaign = Campaign(scoped, cluster)
        primary = campaign.nodes[1]
        # Cluster-mode routing needs an explicit placement for the local Q1
        # partition as well as the Q2 placement supplied by Campaign.
        for node in campaign.nodes.values():
            metadata = json.loads(node.metadata.read_text())
            metadata["topics"].append(
                {"topic_id": TOPIC_ID, "durable_class": 1, "policy_revision": 1})
            metadata["partitions"].append({
                **partition_identity(), "owner": 1,
                "ownership_epoch": OWNER_EPOCH, "lease_valid_for_ms": 600_000,
                "desired_replicas": 1, "shard_id": 8,
                "required_policy_revision": 1,
            })
            node.metadata.write_text(json.dumps(metadata), encoding="utf-8")
        broker = None
        try:
            # Reuse the real voter configuration, but let Broker supply its Q1
            # route and ephemeral TCP listener. No extra proxy or simulated voter.
            broker = Broker(local, extra=primary.arguments(True)[3:])
            primary.process = broker.process
            primary.lines = broker.lines
            for number in (2, 3):
                campaign.nodes[number].start(bootstrap=False)
            broker.start()
            primary.client_port = int(broker.endpoint.rsplit(":", 1)[1])
            for node in campaign.nodes.values():
                node.wait_ready()
            primary.wait_authority()
            scoped.append("shared_broker_started", node=1, pid=broker.process.pid,
                          endpoint=broker.endpoint, q1_topic=TOPIC,
                          q2_topic="durable-q2")

            def q2_call(producer, message, payload, timeout=15):
                command = campaign._arguments(
                    producer, 0, 0, message, payload, INCARNATION, Q2_OWNER)
                scoped.append("q2_publish_attempt", producer=producer, epoch=0,
                              sequence=0, message_id=message,
                              payload_digest=oracle.digest_hex(payload.encode()))
                timed_out = False
                try:
                    result = subprocess.run(
                        command, env=broker.environment(), capture_output=True,
                        text=True, timeout=timeout)
                    stdout, stderr, code = result.stdout, result.stderr, result.returncode
                except subprocess.TimeoutExpired as error:
                    timed_out = True
                    def text(value):
                        return value.decode() if isinstance(value, bytes) else value or ""
                    stdout, stderr, code = text(error.stdout), text(error.stderr), None
                # Preserve exact output before interpreting or asserting anything.
                scoped.append("q2_client_result", producer=producer,
                              message_id=message, client_stdout=stdout,
                              client_stderr=stderr, returncode=code,
                              timed_out=timed_out, timeout_seconds=timeout)
                # A success printed even by a subsequently hung client is still
                # a success claim, never hidden behind the timeout classification.
                observed = [json.loads(line) for line in stdout.splitlines() if line.strip()]
                if timed_out:
                    assert not any(item.get("outcome") == "committed_quorum"
                                   for item in observed), stdout
                    return {"outcome": "unknown", "detail": "client timeout"}
                assert code == 0 and len(observed) == 1, (code, stdout, stderr)
                return observed[0]

            def certain(producer, message, payload):
                deadline = time.monotonic() + 60
                while True:
                    observed = q2_call(producer, message, payload)
                    if observed.get("outcome") != "unknown":
                        assert observed.get("outcome") == "committed_quorum", observed
                        return observed
                    assert time.monotonic() < deadline, observed
                    time.sleep(0.3)

            baseline = certain(31, 300, "q2-baseline")
            q2_offset = baseline["offset"]
            states = campaign.await_message(q2_offset, 300)
            assert set(states) == {1, 2, 3}, states
            local_offset = committed_local(publish(broker, 0, OLD_INCARNATION),
                                           OLD_INCARNATION, 0)
            scoped.append("healthy_baseline", q2_ack=baseline,
                          q2_offset=q2_offset,
                          q2_payload_digest=oracle.digest_hex(b"q2-baseline"),
                          q1_offset=local_offset, q1_ack="COMMITTED_LOCAL",
                          voter_states=states)
            for number in (2, 3):
                node = campaign.nodes[number]
                pid = node.process.pid
                node.kill()
                code = node.process.poll()
                scoped.append("voter_stop_observed", node=number, pid=pid,
                              signal="SIGKILL", returncode=code,
                              observation="Popen.wait completed; poll returned exit status",
                              storage=str(node.directory))
                assert code == -9, code
            assert campaign.live() == [1], campaign.live()
            scoped.append(
                "authority_disturbed", authority="Q2 data quorum",
                live_voters=campaign.live(), stopped_voters=[2, 3],
                configured_voters=[1, 2, 3], required_majority=2,
                metadata_mode="static --cluster-metadata JSON fixtures",
                metadata_quorum_tested=False,
                evidence="Q2 voter processes 2 and 3 reaped; node 1 remains alive",
            )
            lost = q2_call(32, 301, "q2-without-quorum")
            scoped.append("quorum_loss_outcome", observed=lost,
                          delivery_claim="unknown; no claim of non-delivery")
            assert lost.get("outcome") == "unknown" or (
                lost.get("outcome") == "rejected"
                and "quorum" in str(lost.get("detail", "")).lower()), lost
            result = publish(broker, 1, OLD_INCARNATION)
            new_offset = committed_local(result, OLD_INCARNATION, 2)
            assert campaign.live() == [1], campaign.live()
            scoped.append("q1_commits_without_q2_quorum", offset=new_offset,
                          previous_offset=local_offset,
                          client_stdout=result.stdout, client_stderr=result.stderr,
                          returncode=result.returncode, broker_pid=broker.process.pid,
                          live_q2_voters=campaign.live())
            for number in (2, 3):
                node = campaign.nodes[number]
                if node.process.stderr is not None:
                    node.process.stderr.close()
                node.start(bootstrap=False)
                node.wait_ready()
                scoped.append("voter_restarted_on_original_storage", node=number,
                              storage=str(node.directory), bootstrap=False)
            restored = certain(33, 302, "q2-restored")
            assert restored["offset"] > q2_offset, restored
            states = campaign.await_message(restored["offset"], 302)
            assert set(states) == {1, 2, 3}, states
            scoped.append("q2_availability_restored", observed=restored,
                          voter_states=states)
        finally:
            if broker is not None:
                broker.close()
            for number in (2, 3):
                node = campaign.nodes[number]
                node.kill()
                if node.process is not None and node.process.stderr is not None:
                    node.process.stderr.close()
    return (f"baseline Q2 offset {q2_offset}, Q1 offset {local_offset}; "
            f"two voters killed: Q2 {lost}; same broker Q1 offset {new_offset}; "
            f"original-storage restart restored Q2 {restored}")


def main() -> int:
    missing = [str(path) for path in (SERVER, Q1, READ, EXAMPLE) if not path.is_file()]
    EVIDENCE.mkdir(parents=True, exist_ok=True)
    journal = oracle.Journal(EVIDENCE / "isolation.jsonl", "c06-q1-q2-isolation")
    verdicts: dict[str, str] = {}
    try:
        for name, scenario in ((S1, scenario_restart), (S2, scenario_fail_closed),
                               (S3, scenario_quorum_loss)):
            if missing:
                verdict, reason = oracle.NOT_RUN, (
                    "binaries not built: " + ", ".join(missing))
            else:
                try:
                    reason = scenario(journal)
                    verdict = oracle.PASS
                except BaseException as error:
                    verdict = oracle.FAIL
                    reason = f"{type(error).__name__}: {error}"
            journal.conclusion(verdict, reason, scenario=name)
            verdicts[name] = verdict
            print(f"{verdict} {name}: {reason}", flush=True)
    finally:
        journal.close()
    return 0 if all(verdict == oracle.PASS
                    for verdict in verdicts.values()) else 1


if __name__ == "__main__":
    sys.exit(main())
