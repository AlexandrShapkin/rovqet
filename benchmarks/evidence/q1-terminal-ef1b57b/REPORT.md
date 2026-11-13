# B-06 NACK, retry, quarantine and real Q1 DLQ report

Source commit: `ef1b57b52036ae7bd4e94f79f5930bbcba073688`  
Campaign time (UTC): 2026-11-13T15:48:46Z  
Verdicts: **11/11 PASS**.

## Method

Real external SDK consumer processes (`nack_q1`, `consume_q1`, `read_q1`) drive a real broker
over loopback TCP. The parent process writes fsynced JSONL journals outside broker storage.
The journal records deliveries, NACK requests, retry scheduling, terminal commits, DLQ
publications, recovered cursors and faults. Reconciliation checks exact disposition identity,
DLQ payload and source identity, publication-before-terminal ordering, one destination record
per disposition, retained source publications and contiguous cursor progress.

Disabled/faulty evidence is never a pass: journals are append-only and
`q1_oracle.reconcile_terminal` rejects missing conclusions, a terminal with no prior DLQ
publication, missing or duplicated DLQ records, digest mismatches, uncommitted source
offsets, unexplained cursor movement and incomplete observations.

## Executed campaigns

- NACK backoff and redelivery over TCP (bounded initial delay, no early redelivery).
- Maximum attempts with no terminal policy: exhaustion persists across restart, cursor stays
  0, source publication remains.
- Quarantine terminal: PENDING+COMMITTED barrier, source retained, cursor stable after restart.
- Real DLQ: committed Q1 message readable through READ_FETCH with original payload and
  message identity; duplicate terminal token is a read-only ALREADY_CLOSED outcome and does
  not insert a second DLQ message.
- Six deliberate broker kills: PENDING after append/before sync (quarantine incomplete),
  PENDING after sync, DLQ committed before source terminal, terminal committed append before
  sync, terminal committed after sync before reply, and quarantine pending-before-fsync.
  Each restarted on the same storage and incarnation.
- Stale-generation NACK rejected without creating a disposition.

## Results

- All DLQ crash windows recovered with **exactly one** DLQ record, source cursor advanced
  only after the committed terminal record, and the original source publication retained.
- Crash after PENDING append before sync left the source incomplete and redeliverable.
- Crash after PENDING sync, after DLQ commit, during committed append and after committed
  sync all converged to the same durable terminal state without duplicate DLQ messages.
- Quarantine terminal committed exactly the intended source offset; restart did not
  redeliver it.
- Exhaustion without a policy never closed the message or moved the cursor.

## Defects found and fixed

1. Standalone READ_FETCH skipped consumer ACK/checkpoint records but not the new RETRY and
   TERMINAL_DISPOSITION metadata; a post-terminal replay poisoned the source actor and
   returned STORAGE_UNAVAILABLE. The skip set now covers all internal metadata and the TCP
   test asserts source replay after terminal disposition.
2. NACK request dispatch and response encoding required the full frozen token/result fields;
   integration tests pin request correlation, no ACK_COMMITTED reuse and typed outcomes.

## Limitations

- Process-crash recovery on intact ext4, not physical power loss.
- One local Q1 source and one explicitly configured local Q1 DLQ partition; no distributed
  atomic transaction, Q2, rebalance or admin redrive/retention policy.
- Retry eligibility is not wall-clock preserved across restart: pending retries become
  immediately eligible (conservative) while attempt counts from durable retry/terminal
  records are preserved.
- Delivery attempts for crashes before any retry record are not durable; more retries, never
  fewer, can occur.
- DLQ idempotence uses one deterministic producer identity per disposition; producer
  cardinality is bounded by destination configuration.
- EXTEND_LEASE remains unsupported. At-least-once delivery is verified; exactly-once
  application execution is not claimed.
