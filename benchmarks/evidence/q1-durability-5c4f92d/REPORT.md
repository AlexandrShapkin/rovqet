# B-07 independent Q1 durability and crash-recovery report

Source commit: `5c4f92d57f06ca07d3f2cadbc98736a4546521ea`  
Campaign time (UTC): 2026-11-13T08:41:51Z  
Host: Linux-7.2.15-zen1-1-zen-x86_64-with-glibc2.44, kernel 7.2.15-zen1-1-zen, 8 CPUs,
filesystem `ext4`  
Rust: rustc 1.99.1 (58cf8f9c3 2026-10-13) (Arch Linux rust 1:1.99.1-2)  
Verdicts: **26/26 PASS** (PASS, FAIL, INVALID and NOT_RUN are
distinct; see `scripts/q1_oracle.py`).

## Method

Real external Rust SDK processes (`q1`, `consume_q1`, `read_q1`, `q1_measure`) talk to a real
broker over loopback TCP. The parent process writes an append-only JSONL journal with an
fsync boundary outside the broker directory before terminating the broker. Journals record
publication attempts and received `COMMITTED_LOCAL`, delivery tokens, ACK attempts and
received `ACK_COMMITTED`, generation changes, faults, restarts and recovery observations.
Reconciliation uses exact message IDs, payload SHA-256, incarnation and offset sets; broker
WAL files are supplementary only. Deterministic fault-injection barriers target append,
sync, reply and checkpoint windows; seven WAL fault points are injected across a forced
segment rotation.

Reproduce:

```sh
cargo build -p rovqet-server --features fault-injection --locked
cargo build -p rovqet-sdk --examples --locked
cargo build -p rovqet-wal --examples --locked
python3 scripts/test_q1_oracle.py
python3 scripts/test_q1_durability.py
```

## Executed campaigns

- 6 publication crash campaigns (before append, after append/before sync,
  after sync/before reply; seeds 42/43).
- 8 consumer campaign runs (same three barriers plus cursor-checkpoint
  append/sync; seeds 44/45).
- 7 WAL fault runs (`append`, `partial-append`, `segment-sync`,
  `manifest-write`, `manifest-sync`, `manifest-rename`, `directory-sync`) with 512-byte
  segments and a 256-byte record ceiling, forcing rotation.
- Sparse out-of-order ACK and generation-fencing run; unsynced generation-claim crash;
  corrupt and missing storage startup rejection; Q0 isolation fixture; measurement run.
- 22 direct process terminations/fault injections occurred across the matrix (6 publication,
  8 consumer, 1 generation claim, 7 WAL faults). Each crashed broker was restarted on its
  original storage and incarnation.

## Reconciliation results

- Every externally received `COMMITTED_LOCAL` was present after recovery with the same
  message ID, payload digest, incarnation and offset. Before-append and before-sync crash
  publications were recovered 3/3 acknowledged records plus one uncertain attempt (absent);
  after-sync crash recovered 4/4 records with one explicitly uncertain, unacknowledged
  publication.
- Repeated producer tokens returned their original offsets after restart; a changed payload
  under a committed token was rejected (`SequenceConflict`); an old epoch was fenced
  (`ProducerFenced`) while a new epoch started at sequence 0.
- Consumer ACK before append/before sync: the ACK was not durable, the record was redelivered,
  and the cursor stayed at the incomplete offset. ACK after sync and ACK after a checkpoint
  append/before sync: the cursor recovered at 4, the acknowledged record was not redelivered,
  and one offset was classified as uncertain committed progress. No cursor crossed an
  unattempted or unacknowledged application record.
- Sparse ACK of the second record left the cursor at 0 after restart and redelivered the
  first and third records; closing the gap advanced the cursor to the end.
- A generation claim killed before its checkpoint sync was discarded: after restart the first
  durable claim was generation 1, with no fabricated lease or ACK.
- Duplicate committed ACK lookup succeeded after restart/fencing without a new mutation;
  the previous generation could not acknowledge a different record (`StaleGeneration`).
- Corrupt committed segment bytes and an empty replacement directory failed startup without
  creating or resetting storage.
- Repeated restarts preserved message identities and offsets exactly.

## Defect found and fixed

B-05 introduced sparse application offsets, but major-1 codec validation still required even
`READ_RECORD`/`READ_END` offsets. Once ACK/checkpoint records shifted the continuation to an
odd position, standalone replay returned `RESOURCE_EXHAUSTED`. The codec now accepts sparse
positions; existing golden vectors are unchanged and
`sparse_application_offsets_allow_odd_record_and_terminal_positions` plus the B-07 consumer
matrix pin the regression. Failing pre-fix journals are preserved under
`benchmarks/evidence/q1-durability-prefix-3031258/`.

## Measurements (exploratory, single host, n=200 where applicable)

| Metric | p50 | p95 | p99 | p99.9 | n |
|---|---:|---:|---:|---:|---:|
| End-to-end Q1 publish (TCP `COMMITTED_LOCAL`) | 883 us | 1300 us | 1450 us | 1491 us | 200 |
| End-to-end consumer ACK (`ACK_COMMITTED`) | 794 us | 1153 us | 1235 us | 1387 us | 200 |
| Direct WAL append (message+outcome) | 69 us | 172 us | 214 us | 242 us | 200 |
| Direct WAL fsync | 56 us | 144 us | 163 us | 202 us | 200 |
| Append + fsync | 124 us | 326 us | 363 us | 375 us | 200 |

- Broker peak RSS: 5840 KiB; CPU after the publish/ack run:
  15 jiffies.
- Recovery readiness (launch to listening): median 4.17 ms,
  min 3.49 ms, max 5.35 ms over 14 restarts.
- Recovered records per publication campaign: 3 or 4 (including one uncertain publication
  when it became durable). Recovered read of 200 records took 44.3 ms;
  consume fetched/acked 200 records.
- Q1 queue utilization, active producer/outcome counts, active group/lease counts and sparse
  ACK entry counts are **UNAVAILABLE**: no Q1 metrics endpoint. Q0 broker metrics are in
  `measurements.json` but are not Q1 evidence.
- p95/p99 use nearest-rank on one run; p99.9 with n=200 is not statistically stable. The
  append/sync profile uses a separate disposable WAL directory, not broker-internal phase
  timing. These are prototype measurements, not an SLO or power-loss certification.

## Limitations

- SIGKILL is process-crash recovery on an intact ext4 filesystem; it is not physical
  power-loss or torn-write hardware certification.
- One host, one configured Q1 partition, debug builds, no soak, no cross-machine clock.
- Single-consumer generation model only; no competing-worker rebalance, NACK/retry schedule,
  DLQ/quarantine or SEEK (B-06 remains TODO).
- At-least-once delivery is verified; exactly-once application execution is not claimed.
- All destructive work used disposable directories; journals and measurements are preserved
  in this directory and its `journals/` subtree.
