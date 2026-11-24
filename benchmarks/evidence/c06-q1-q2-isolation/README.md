# C-06 — partial evidence, not an acceptance claim

C-06 remains **IN_PROGRESS**. This directory contains the wire-level increment
and three focused isolation scenarios. The full fault/migration campaign and its
independent reconciler remain **NOT_RUN**, not PASS.

## Focused isolation journal

Run `timeout 600 python3 scripts/test_q1_q2_isolation.py` after the build below.
`isolation.jsonl` records Q1 restart/dedup, fail-closed history loss with an
explicit fresh incarnation, and
`q2-quorum-loss-blocks-quorum-ack-while-q1-commits`.
The last scenario shares one TCP broker between Q1 and Q2, with three real
Q2 voters. It observes healthy Q2/Q1 offsets 0/0, reaps voters 2 and 3 after
SIGKILL, and bounds a fresh Q2 request at 15 seconds. In the recorded run the
client timed out with empty stdout/stderr: the outcome is uncertain, **not**
proof of non-delivery. That same broker acknowledges Q1 at offset 2 while only
one Q2 voter is alive. Restarting the original voter storage restores a Q2
quorum commit at offset 2 (the uncertain request may commit after restoration).
Every scenario has its own conclusion; these PASS results do not complete C-06.

This disturbs the Q2 **data** quorum. Metadata is supplied by static JSON
fixtures, not a live metadata Raft quorum, so no metadata-quorum isolation
claim is made. All processes and storage are disposable loopback prototypes;
no physical host-failure or production-security certification is implied.

## Reproduce the focused check

```sh
cargo build -p rovqet-server -p rovqet-sdk --examples --bins --locked
python3 scripts/test_q1_q2_wire.py
```

`wire-result.txt` records the successful focused check; `wire.jsonl` is the
parent-owned fsynced observation journal. Its PASS conclusion applies **only to
the wire check**, not to the full C-06 work package. The temporary broker storage
is removed after the check. All listeners are plaintext loopback development
listeners with static test identity; no production security claim is made.

## Observed behavior

PUBLISH contains an explicit QOS field (major-v1 field 12). The SDK Q1 and Q2
publication APIs set it to 1 and 2 respectively. Topic routing does not implicitly
upgrade or downgrade the requested durable class:

| Domain | Identity | Wrong request | Observation |
| --- | --- | --- | --- |
| Q1 | tenant 1, topic 3 (`durable`), partition 0, incarnation 42, owner epoch 7 | QOS 2 | ERROR 14 (`UNSUPPORTED_GUARANTEE`), unchanged replay, manifest and record count |
| Q2 | tenant 1, topic 4 (`durable-q2`), partition 0, incarnation 42, owner epoch 5; shard 7, cluster 4242 | QOS 1 | ERROR 14, unchanged messages, producer outcomes and next offset on all three voters |

Q1 initially acknowledges local offset 0. After the rejected sequence-1 request,
explicit Q1 sequence 1 succeeds at offset 2 (Q1 physical history also contains
producer outcomes). Q2 initially acknowledges logical offset 0; after the
rejected sequence-1 request, explicit Q2 sequence 1 succeeds at logical offset
1, with exactly two publications on every voter. Equal numeric incarnations
across these distinct topic/class identities do not merge their namespaces.

No production class-mismatch defect was found: `q2::Route::publish` already
checks QOS before proposing, and dispatch sends non-Q2 durable publications to
the unsupported-guarantee path rather than upgrading them.

The wire test uses a separate Q1 broker and three Q2 brokers. It does **not**
prove same-process isolation during data-quorum loss, class-conflict validation,
storage-loss recovery, migration, independent consumer state, cross-partition
ACK rejection, or any fault scenario. No general Q1-to-Q2 migration is provided.

## Existing voter stability evidence

The C-06 ledger records the fix in `7d7d801`: bounded waits on Raft-published
metrics replaced premature one-shot reads. Recorded outcomes are 20/20 isolated
repetitions, 12/12 with 10 CPU hogs on 8 cores, and 18/18 with 6 concurrent test
processes (50/50 total). These are inherited ledger results, not new repetitions
performed by this wire check.

The separate `q1-q2-wire` CI job builds the normal broker and SDK examples and
runs this regression with an eight-minute timeout. It is not the missing full
C-06 isolation job and leaves the existing jobs unchanged.
