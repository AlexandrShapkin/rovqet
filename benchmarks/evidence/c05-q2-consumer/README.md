# C-05 Q2 consumer evidence

Run: `python3 scripts/test_q2_consumer.py`. Every run freshly builds normal broker
and external SDK producer/consumer executables, copies them into private scratch
storage and starts three actual brokers/voters with separate persistent directories.
No Q1 route is configured. The parent alone writes and fsyncs `consumer.jsonl`.

- `consumer.jsonl`: SDK process arguments and actual responses, full delivered
  tokens, scenario verdicts, quorum changes, kills/restarts, and applied voter states.
- `oracle.json`: independently reconciled verdict, not a campaign self-assertion.
- `node*/state/state.json`: recovered raw state including saved ACK identities/results.
- `node*/state/snapshot.json`, `node*/raft/purged.json`: snapshot/purge recovery points.

Normal, sparse, fencing, duplicate ACK, quorum, follower catch-up, full restart,
snapshot installation and invalid-token checks have executed. Snapshot installation
is forced by keeping one follower offline until the leader purges beyond its old
applied index; its catch-up must preserve all group outcomes.

The initial oracle verdict is **NOT_RUN**, not PASS: lost-response and deterministic
crash-window scenarios are not yet covered. C-05 remains IN_PROGRESS. Failed exploratory
runs are not retained as passing evidence; the indexed journal is the latest complete
invocation and records every unexecuted scenario explicitly.
