# C-06 — partial evidence, not an acceptance claim

C-06 remains **IN_PROGRESS**. This directory currently contains only the first,
wire-level increment. The full isolation campaign and its independent reconciler
are not implemented. Their verdict is **NOT_RUN**, not PASS.

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

This focused test uses a separate Q1 broker and three Q2 brokers. It does **not**
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
