# C-05 Q2 consumer evidence

Run: `python3 scripts/test_q2_consumer.py`. Every run freshly builds normal broker
and external SDK producer/consumer executables, copies them into private scratch
storage and starts three actual brokers/voters with separate persistent directories.
No Q1 route is configured. The parent alone writes and fsyncs `consumer.jsonl`.

- `consumer.jsonl`: SDK process arguments and actual responses, full delivered
  tokens, scenario verdicts, quorum changes, kills/restarts, and applied voter states.
- `oracle.json`: independently reconciled verdict, not a campaign self-assertion.
- `verification.json`: executed commands and exit codes.
- `release.json`: exact verified implementation SHA and green CI run.
- `node*/state/state.json`: recovered raw state including saved ACK identities/results.
- `node*/state/snapshot.json`, `node*/raft/purged.json`: snapshot/purge recovery points.

Normal, sparse, fencing, duplicate ACK, quorum, follower catch-up, full restart,
snapshot installation and invalid-token checks have executed. Snapshot installation
is forced by keeping one follower offline until the leader purges beyond its old
applied index; its catch-up must preserve all group outcomes.

All **12 required scenarios PASS**, including the actually lost response:
normal, sparse, fencing, idempotent, quorum, follower restart, full restart,
snapshot installation, negative tokens, crash before proposal, crash after local
fsync before majority, and crash after majority before response. The independent
oracle verdict is **PASS**, with 16 externally successful ACK observations backed
by distinct durable majorities and recovered saved state on all three voters.

The crash scenarios freshly build `cargo build -p rovqet-server --features
fault-injection --locked` and reuse the existing pre-/post-majority seam directories
and parent barrier helpers, armed with the **full ACK command**. The parent observes
the barrier, verifies applied state before killing, SIGKILLs the named leader and
restarts the original three storage directories. A missed barrier is FAIL. There
are no timing sleeps to choose a crash point. The normal scenarios use a separately
built/copied normal binary with no fault marker. A pre-majority uncertain tail may
legitimately commit after recovery: the campaign resolves the exact saved ACK or
requires redelivery; it never equates uncertainty with rejection.

Failed exploratory runs are not relabeled as passing evidence. The indexed journal
is the latest full invocation. All runs are three processes on **one host**, not
evidence of independent physical failure domains or production qualification.
