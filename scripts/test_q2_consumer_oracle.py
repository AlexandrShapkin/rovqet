"""Independent consumer-oracle corruption fixtures."""
import copy
import unittest

import q1_oracle as oracle


class Q2ConsumerFixtures(unittest.TestCase):
    def base(self):
        token = dict(tenant_id=1, topic_id=4, partition_id=0, incarnation=42,
                     group_id=7, generation=1, lease_id=123, metadata_epoch=5,
                     offset=0, attempt=1, principal=1, message_id=100)
        command = {k: v for k, v in token.items() if k not in ("tenant_id", "topic_id", "partition_id")}
        command["partition"] = {k: token[k] for k in ("tenant_id", "topic_id", "partition_id")}
        group = dict(generation=1, cursor=1, sparse_acks=[], outcomes={"0": {
            "command": {"GroupAck": command},
            "outcome": {"GroupAcked": dict(generation=1, group_id=7, offset=0, cursor=1)},
        }})
        records = [
            dict(event="consumer_configuration", q1_configured=False),
            dict(event="consumer_delivery", token=token),
            dict(event="consumer_attempt", token=token),
            dict(event="consumer_result", token=token, live=[1, 2, 3],
                 result=dict(outcome="ack_committed", offset=0, generation=1)),
        ]
        for node in (1, 2, 3):
            records.append(dict(event="consumer_state", node=node, applied_index=5,
                recovered=True, partition=dict(incarnation=42, group_generation=1,
                    next_logical_offset=1, messages=[dict(logical_offset=0, message_id=100)],
                    groups={"7": copy.deepcopy(group)})))
        for index, stage in enumerate(("before_proposal", "before_majority", "after_majority")):
            armed = dict(token, group_id=8 + index) if stage != "after_majority" else token
            armed_command = copy.deepcopy(command)
            armed_command["group_id"] = armed["group_id"]
            barrier = dict(stage=stage, node=1, command={"GroupAck": armed_command},
                           committed_index=5, log_index=6, durable_index=6)
            records.append(dict(event="consumer_barrier", stage=stage, token=armed, barrier=barrier))
            if stage == "after_majority":
                records.extend(copy.deepcopy(r) for r in list(records)
                               if r["event"] == "consumer_state")
            records.append(dict(event="consumer_barrier_kill", stage=stage, node=1, signal="SIGKILL"))
            records.append(dict(event="consumer_result", token=armed, live=[2, 3],
                                result=dict(outcome="error", detail="OutcomeUnknown")))
            records.append(dict(event="consumer_crash_recovered", stage=stage))
        records.extend(dict(event="consumer_scenario", name=name, verdict=oracle.PASS)
                       for name in oracle.Q2_CONSUMER_REQUIRED)
        records.append(dict(event="conclusion", verdict=oracle.PASS))
        for seq, record in enumerate(records):
            record["seq"] = seq
        return records

    def test_complete_evidence_is_accepted(self):
        self.assertEqual(oracle.reconcile_q2_consumer(self.base())["verdict"], oracle.PASS)

    def test_fabricated_ack_success_is_rejected(self):
        records = self.base()
        records[3] = copy.deepcopy(records[3])
        records[3]["token"]["message_id"] = 999
        self.assertEqual(oracle.reconcile_q2_consumer(records)["verdict"], oracle.FAIL)

    def test_cursor_skip_is_rejected(self):
        records = self.base()
        records[4]["partition"]["groups"]["7"]["cursor"] = 2
        self.assertEqual(oracle.reconcile_q2_consumer(records)["verdict"], oracle.FAIL)

    def test_generation_regression_is_rejected(self):
        records = self.base()
        stale = copy.deepcopy(records[4])
        stale["applied_index"] = 6
        stale["partition"]["groups"]["7"]["generation"] = 0
        stale["partition"]["group_generation"] = 0
        records.insert(-1, stale)
        self.assertEqual(oracle.reconcile_q2_consumer(records)["verdict"], oracle.FAIL)

    def test_missing_majority_evidence_is_rejected(self):
        records = [r for r in self.base()
                   if r["event"] != "consumer_state" or r["node"] == 1]
        self.assertEqual(oracle.reconcile_q2_consumer(records)["verdict"], oracle.FAIL)

    def test_missing_recovered_ack_state_is_rejected(self):
        records = self.base()
        for record in records:
            if record["event"] == "consumer_state":
                record["recovered"] = False
        self.assertEqual(oracle.reconcile_q2_consumer(records)["verdict"], oracle.FAIL)

    def test_incomplete_campaign_cannot_report_pass(self):
        records = [r for r in self.base()
                   if r.get("name") != "crash_after_majority"]
        self.assertEqual(oracle.reconcile_q2_consumer(records)["verdict"], oracle.FAIL)
        records[-1]["verdict"] = oracle.NOT_RUN
        self.assertEqual(oracle.reconcile_q2_consumer(records)["verdict"], oracle.NOT_RUN)


if __name__ == "__main__":
    unittest.main(verbosity=2)
