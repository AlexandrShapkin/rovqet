#!/usr/bin/env python3
"""C-05 real-process consumer campaign; incomplete coverage is never PASS.

Reuse the C-04 launchers, producer, fsynced journal and barrier observations.
Every invocation builds and copies fresh normal binaries into its private directory.
The SDK consumer is always an external process. No Q1 route is configured.
"""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import time
import traceback

import q1_oracle as oracle
import test_q2_publish as harness

ROOT = harness.ROOT
EVIDENCE = ROOT / "benchmarks/evidence/c05-q2-consumer"


def build(arguments):
    subprocess.run(["cargo", *arguments], cwd=ROOT, check=True)


class ConsumerCampaign:
    def __init__(self, journal, directory, consumer):
        self.journal = journal
        self.c = harness.Campaign(journal, directory)
        self.consumer = consumer
        self.sequence = 0
        self.leader = 1
        self.results = {}

    def verdict(self, name, verdict, reason=None):
        self.results[name] = verdict
        self.journal.append("consumer_scenario", name=name, verdict=verdict, reason=reason)
        print(f"{verdict} {name}: {reason or 'observed required checks'}", flush=True)

    def state(self, node):
        path = self.c.nodes[node].directory / "state/state.json"
        return json.loads(path.read_text())["state"]

    def partition(self, node):
        return self.state(node)["partitions"][harness.PARTITION_KEY]

    def observe(self, *, recovered=False):
        for node in self.c.live():
            state = self.state(node)
            self.journal.append("consumer_state", node=node,
                applied_index=state["last_applied"]["index"], recovered=recovered,
                partition=state["partitions"][harness.PARTITION_KEY])

    def await_condition(self, predicate, detail, timeout=45):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if predicate():
                return
            time.sleep(harness.T_POLL)  # condition polling, never selecting a crash window
        raise harness.CheckFailure(detail)

    def publish(self, payload="consumer"):
        result = self.c.publish_until_certain(11, 0, self.sequence, 100 + self.sequence, payload)
        self.c.expect_committed(result, self.sequence)
        self.c.await_message(self.sequence, 100 + self.sequence)
        self.sequence += 1

    def elect(self):
        self.publish("election-witness")
        self.leader = self.c.observe_leader()

    def start_call(self, arguments):
        self.journal.append("consumer_process", arguments=arguments, node=self.leader)
        return subprocess.Popen([str(self.consumer), *map(str, arguments)],
            env=dict(os.environ, ROVQET_ADDR=f"127.0.0.1:{self.c.nodes[self.leader].client_port}"),
            text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    def finish(self, process):
        try:
            stdout, stderr = process.communicate(timeout=35)
        except subprocess.TimeoutExpired:
            process.kill()
            process.communicate()
            raise harness.CheckFailure("external consumer exceeded its bounded deadline") from None
        if process.returncode or not stdout.strip():
            raise harness.CheckFailure(f"consumer exit={process.returncode}: {stderr[-1200:]}")
        return json.loads(stdout.splitlines()[-1])

    def fetch(self, group, generation=0):
        result = self.finish(self.start_call(["fetch", group, generation]))
        self.journal.append("consumer_fetch", group=group, result=result)
        assert result["outcome"] == "batch", result
        for token in result["messages"]:
            self.journal.append("consumer_delivery", token=token)
        return result

    @staticmethod
    def ack_arguments(token):
        return ["ack", *[token[k] for k in ("group_id", "generation", "lease_id",
            "metadata_epoch", "incarnation", "offset", "attempt", "message_id")], "parent"]

    def start_ack(self, token):
        self.journal.append("consumer_attempt", token=token)
        return self.start_call(self.ack_arguments(token))

    def finish_ack(self, token, process):
        result = self.finish(process)
        self.journal.append("consumer_result", token=token, result=result, live=self.c.live())
        return result

    def ack(self, token, expected="ack_committed"):
        before = self.partition(self.leader).get("groups", {})
        result = self.finish_ack(token, self.start_ack(token))
        if expected == "ack_committed":
            assert result["outcome"] == expected, result
            self.await_ack(token)
        else:
            assert result["outcome"] == "error" and expected in result["detail"], result
            after = self.partition(self.leader).get("groups", {})
            self.journal.append("consumer_rejection_check", token=token, before=before, after=after)
            assert before == after, "rejection mutated consumer state"
        self.observe()
        return result

    def await_ack(self, token):
        def present():
            return all(
                str(token["offset"]) in self.partition(n).get("groups", {}).get(str(token["group_id"]), {}).get("outcomes", {})
                for n in self.c.live())
        self.await_condition(present, "ACK did not reach every live voter's applied state")
        self.observe()

    def cursor(self, group, expected):
        self.await_condition(lambda: all(
            self.partition(n)["groups"][str(group)]["cursor"] == expected for n in self.c.live()),
            f"group {group} cursor is not {expected}")
        self.observe()

    def restart_all(self):
        for node in self.c.nodes.values():
            node.stop()
        self.c.start([1, 2, 3])
        self.elect()

    def run_core(self):
        self.c.start([1, 2, 3], bootstrap_only_node=1)
        for _ in range(3):
            self.publish()
        self.leader = self.c.observe_leader()
        normal = self.fetch(10)
        assert len(normal["messages"]) == 3
        self.normal = normal["messages"][0]
        self.ack(self.normal)
        self.cursor(10, 1)
        self.verdict("normal", oracle.PASS)

        sparse = self.fetch(20)["messages"]
        self.ack(sparse[2])
        self.cursor(20, 0)
        self.ack(sparse[1])
        self.cursor(20, 0)
        self.ack(sparse[0])
        self.cursor(20, 3)
        self.verdict("sparse", oracle.PASS)

        old = self.fetch(30)["messages"][0]
        current = self.fetch(31)["messages"][0]
        self.ack(old, "StaleGeneration")
        self.ack(dict(current, metadata_epoch=4), "StaleEpoch")
        saved = self.ack(current)
        self.batch = self.fetch(32)
        assert self.ack(current) == saved
        self.verdict("fencing", oracle.PASS)

        tokens = self.batch["messages"]
        saved = self.ack(tokens[2])
        before = self.partition(self.leader)
        assert self.ack(tokens[2]) == saved
        assert self.partition(self.leader) == before, "duplicate caused a second mutation"
        self.journal.append("consumer_duplicate_check", token=tokens[2], before=before, after=self.partition(self.leader))
        self.journal.append("consumer_duplicate", token=tokens[2], result=saved)
        self.verdict("idempotent", oracle.NOT_RUN, "duplicate PASS; held/lost response pending crash scenario")

        follower = next(n for n in (2, 3) if n != self.leader)
        self.c.nodes[follower].stop()
        self.ack(tokens[1])
        other = next(n for n in self.c.live() if n != self.leader)
        self.c.nodes[other].stop()
        self.ack(tokens[0], "OutcomeUnknown")
        self.c.start([follower, other])
        self.elect()
        self.ack(tokens[0])
        self.cursor(32, 3)
        self.verdict("quorum", oracle.PASS)
        self.verdict("follower_restart", oracle.PASS)

        # Lag one follower beyond the leader's purged prefix: catch-up now
        # requires snapshot installation, not merely replay of retained entries.
        follower = next(n for n in (2, 3) if n != self.leader)
        before_index = self.state(follower)["last_applied"]["index"]
        self.c.nodes[follower].stop()
        batch = self.fetch(40)
        self.ack(batch["messages"][0])
        for _ in range(18):
            self.publish("snapshot-pressure")
        leader = self.leader
        purged = self.c.nodes[leader].directory / "raft/purged.json"
        self.await_condition(lambda: purged.exists() and json.loads(purged.read_text())["index"] > before_index,
            "leader did not purge beyond the stopped follower's applied prefix")
        expected = self.partition(leader)["groups"]
        purged_index = json.loads(purged.read_text())["index"]
        self.c.start([follower])
        self.await_condition(lambda: self.partition(follower).get("groups") == expected
            and self.state(follower)["last_applied"]["index"] >= purged_index,
            "snapshot catch-up lost group progress")
        self.journal.append("consumer_snapshot_install", node=follower, before_index=before_index,
            leader_purged_index=purged_index, after_index=self.state(follower)["last_applied"]["index"],
            before_groups=expected, after_groups=self.partition(follower)["groups"])
        self.observe()
        self.verdict("snapshot_install", oracle.PASS)

        self.restart_all()
        self.cursor(40, 1)
        redelivery = self.fetch(40, batch["generation"])
        offsets = [t["offset"] for t in redelivery["messages"]]
        assert 0 not in offsets and 1 in offsets
        self.journal.append("consumer_redelivery", offset=1, delivered_offsets=offsets)
        self.ack(self.normal)
        self.verdict("full_restart", oracle.PASS)

        token = redelivery["messages"][0]
        self.ack(dict(token, lease_id=999), "LeaseExpired")
        self.ack(dict(token, incarnation=43), "ReplayNotAvailable")
        self.ack(dict(token, offset=99999), "SequenceConflict")
        self.ack(dict(token, message_id=99999), "SequenceConflict")
        self.verdict("negative", oracle.PASS)

    @staticmethod
    def command(token):
        command = {k: token[k] for k in ("incarnation", "group_id", "generation",
            "lease_id", "metadata_epoch", "offset", "attempt", "principal", "message_id")}
        command["partition"] = {k: token[k] for k in ("tenant_id", "topic_id", "partition_id")}
        return {"GroupAck": command}

    def run_crashes(self, fault_binary):
        harness.SERVER = fault_binary
        for number, stage in enumerate(("before_proposal", "before_majority", "after_majority"), 50):
            for node in self.c.nodes.values():
                node.stop()
            barrier_dir = self.c.directory / stage
            barrier_dir.mkdir()
            env = ("ROVQET_Q2_PREMAJORITY_BARRIER" if stage == "before_majority"
                   else "ROVQET_Q2_POSTMAJORITY_BARRIER")
            self.c.env[env] = str(barrier_dir)
            self.c.start([1, 2, 3])
            self.elect()
            batch = self.fetch(number)
            token = batch["messages"][0]
            arm = {"stage": stage, "command": self.command(token)}
            arm_path = barrier_dir / "arm.json"
            with arm_path.open("w") as handle:
                json.dump(arm, handle)
                handle.flush()
                os.fsync(handle.fileno())
            attempt = self.start_ack(token)
            barrier = self.c.await_postmajority_barrier(barrier_dir / "barrier.json", attempt)
            assert barrier["event"] == "consumer_ack_barrier" and barrier["stage"] == stage, barrier
            assert barrier["command"] == arm["command"] and barrier["node"] == self.leader, barrier
            self.journal.append("consumer_barrier", stage=stage, barrier=barrier, token=token)
            if stage == "after_majority":
                self.await_ack(token)
                assert all(self.state(n)["last_applied"]["index"] >= barrier["committed_index"]
                           for n in self.c.live())
            else:
                assert all(not self.partition(n).get("groups", {}).get(str(number), {}).get("outcomes")
                           for n in self.c.live()), "pre-commit barrier already closed an offset"
                if stage == "before_majority":
                    assert barrier["durable_index"] >= barrier["log_index"], barrier
                self.observe()
            self.c.nodes[barrier["node"]].kill()
            self.journal.append("consumer_barrier_kill", stage=stage, node=barrier["node"],
                token=token, signal="SIGKILL")
            result = self.finish_ack(token, attempt)
            assert result["outcome"] == "error" and "OutcomeUnknown" in result["detail"], result
            arm_path.unlink()
            self.c.env.pop(env)
            self.restart_all()
            if stage == "after_majority":
                original = self.ack(token)
                before = self.partition(self.leader)
                assert self.ack(token) == original
                assert self.partition(self.leader) == before, "saved retry mutated committed state"
                self.journal.append("consumer_duplicate_check", token=token, before=before, after=self.partition(self.leader))
                self.cursor(number, 1)
                self.verdict("idempotent", oracle.PASS, "duplicate and actually lost held response resolved identically")
            elif stage == "before_majority" and str(token["offset"]) in self.partition(self.leader).get("groups", {}).get(str(number), {}).get("outcomes", {}):
                # A locally durable, previously uncommitted tail may legitimately
                # commit after quorum recovery. It was uncertain, not rejected.
                # Resolve the original saved identity; never demand its absence.
                self.ack(token)
                self.journal.append("consumer_premajority_resolution", token=token, outcome="committed_after_recovery")
            else:
                self.ack(token, "LeaseExpired")
                redelivery = self.fetch(number, batch["generation"])
                offsets = [t["offset"] for t in redelivery["messages"]]
                assert token["offset"] in offsets
                self.journal.append("consumer_redelivery", offset=token["offset"], delivered_offsets=offsets)
                self.ack(redelivery["messages"][0])
            self.journal.append("consumer_crash_recovered", stage=stage, token=token)
            self.verdict("crash_" + stage, oracle.PASS)


def main():
    EVIDENCE.mkdir(parents=True, exist_ok=True)
    directory = Path(tempfile.mkdtemp(prefix="c05-consumer-", dir="/tmp/opencode"))
    journal = oracle.Journal(EVIDENCE / "consumer.jsonl", "c05-q2-consumer")
    campaign = None
    failure = None
    try:
        build(["build", "-p", "rovqet-server", "--locked"])
        build(["build", "-p", "rovqet-sdk", "--example", "q2_publish", "--example", "q2_consumer", "--locked"])
        normal = directory / "normal-broker"
        shutil.copy2(harness.SERVER, normal)
        consumer = directory / "q2_consumer"
        producer = directory / "q2_publish"
        shutil.copy2(ROOT / "target/debug/examples/q2_consumer", consumer)
        shutil.copy2(harness.EXAMPLE, producer)
        assert harness.POSTMAJORITY_MARKER not in normal.read_bytes(), "normal binary contains fault hooks"
        harness.SERVER, harness.EXAMPLE = normal, producer
        journal.append("consumer_configuration", q1_configured=False,
            binary_sha256=hashlib.sha256(normal.read_bytes()).hexdigest(),
            commands=["cargo build -p rovqet-server --locked",
                      "cargo build -p rovqet-sdk --example q2_publish --example q2_consumer --locked"])
        campaign = ConsumerCampaign(journal, directory, consumer)
        journal.append("consumer_launch_configuration", nodes={
            str(n): {"arguments": node.arguments(n == 1),
                     "metadata": json.loads(node.metadata.read_text())}
            for n, node in campaign.c.nodes.items()})
        campaign.run_core()
        build(["build", "-p", "rovqet-server", "--features", "fault-injection", "--locked"])
        fault_binary = directory / "fault-broker"
        shutil.copy2(ROOT / "target/debug/rovqet", fault_binary)
        assert harness.SEAM_MARKER in fault_binary.read_bytes()
        assert harness.POSTMAJORITY_MARKER in fault_binary.read_bytes()
        journal.append("consumer_fault_build", binary_sha256=hashlib.sha256(fault_binary.read_bytes()).hexdigest(),
            command="cargo build -p rovqet-server --features fault-injection --locked")
        campaign.run_crashes(fault_binary)
    except Exception:
        failure = traceback.format_exc()
        journal.append("consumer_failure", detail=failure)
        if campaign:
            campaign.verdict("execution", oracle.FAIL, failure)
    finally:
        if campaign:
            for name in sorted(oracle.Q2_CONSUMER_REQUIRED - campaign.results.keys()):
                campaign.verdict(name, oracle.NOT_RUN, "not implemented/executed in this increment")
            for node in campaign.c.nodes.values():
                node.stop()
            # Preserve complete raw recovery evidence before removing scratch storage.
            for number, node in campaign.c.nodes.items():
                target_dir = EVIDENCE / f"node{number}"
                target_dir.mkdir(parents=True, exist_ok=True)
                shutil.copy2(node.metadata, target_dir / "metadata.json")
                (target_dir / "stderr.txt").write_text("\n".join(node.lines) + "\n")
                for name in ("state/state.json", "snapshot.json", "state/snapshot.json", "raft/purged.json"):
                    source = node.directory / name
                    if source.exists():
                        target = EVIDENCE / f"node{number}" / name
                        target.parent.mkdir(parents=True, exist_ok=True)
                        shutil.copy2(source, target)
                path = node.directory / "state/state.json"
                if path.exists():
                    state = campaign.state(number)
                    journal.append("consumer_state", node=number, recovered=True,
                        applied_index=state["last_applied"]["index"],
                        partition=state["partitions"][harness.PARTITION_KEY])
        records = oracle.load(journal.path, require_conclusion=False)
        verdict = oracle.reconcile_q2_consumer(records, require_conclusion=False)
        journal.conclusion(oracle.FAIL if failure else verdict["verdict"],
            failure or "; ".join(verdict["reasons"]))
        journal.close()
        verdict = oracle.reconcile_q2_consumer(oracle.load(journal.path))
        (EVIDENCE / "oracle.json").write_text(json.dumps(verdict, indent=2) + "\n")
        shutil.rmtree(directory)
    print(json.dumps(verdict, indent=2))
    return 1 if failure or verdict["verdict"] in (oracle.FAIL, oracle.INVALID) else (0 if verdict["verdict"] == oracle.PASS else 2)


if __name__ == "__main__":
    sys.exit(main())
