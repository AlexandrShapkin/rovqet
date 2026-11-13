# 14 — Verification, reproducible evidence and test oracle

## Two acceptance tracks

**Research MVP:** deterministic smoke + relevant integrity tests + measured density/latency against a baseline. Fast enough for each component task. Results state their coverage and do not claim full product certification.

**Product v1:** TEST-001…TEST-017, REL-01…REL-07, PERF-D1/D2/D3/R1/P1/M1/C1, VAL-01, normalized H1 rig, multi-run statistical evidence, security/soak/production packaging from normative requirements.

## Independent oracle design

Test harness on a separate process/host records append-only JSONL, independent of broker durable state.

~~~json
{"event":"publish_sent","run":"r1","message_id":"...","producer":"...","epoch":1,"sequence":0,"qos":"Q1","partition":"...","digest":"...","time_mono_ns":123}
{"event":"publish_ack","run":"r1","message_id":"...","outcome":"COMMITTED_LOCAL","incarnation":"...","offset":0,"time_mono_ns":456}
{"event":"delivery","run":"r1","message_id":"...","group":"g","generation":3,"lease":"...","offset":0}
{"event":"consumer_ack","run":"r1","group":"g","offset":0,"outcome":"ACK_COMMITTED"}
{"event":"fault","run":"r1","kind":"process_kill","node":"n1","at":"after_fsync"}
{"event":"recovered_record","run":"r1","node":"n1","message_id":"...","digest":"..."}
~~~

Use real monotonic timing for same-host and synchronized-clock methodology for E2E across hosts. Never average percentiles. The oracle computes set-difference of committed IDs vs recovered records, divergent digest, unintended duplicate committed record, offset reincarnation, Q2 no-quorum ACK, cursor skips and replay gaps. An uncertain client outcome is not automatically classified as lost.

## Test fixture naming

\`unit::<crate>::<contract>\`, \`integration::q0::<scenario>\`, \`recovery::q1::<barrier>\`, \`cluster::q2::<fault>\`, \`group::<scenario>\`, \`bench::<profile>\`. Each unit of implementation includes at least one negative test: not only the happy path.

## Mandatory research gates

| Gate | Goal | Proving fixture | Failure action |
|---|---|---|---|
| D0 | architecture complete enough for coding | doc checker, index and trace review | repair contract once, not recreate architecture per crate |
| A | Q0 end-to-end | 1→N, slow subscriber, overload, 10k session smoke | repair routing/queue |
| B | Q1 durable local | crash before/after fsync + independent oracle | no Q1 claim until fixed |
| C | Q2 quorum | 3 nodes, leader loss, quorum loss, committed prefix | no Q2 claim until fixed |
| D | concept comparison | preregistered equivalent NATS profile, raw latency/resources | report negative result honestly |

Gate success does not require approval/review process; it means the engineer/agent has actually executed the behavioral fixture and kept the artifacts.

## Requirement crosswalk for MVP

| Requirement | Architecture chapter | Initial proof |
|---|---|---|
| FR-CON-001/003 | 03, 10 | 10k sessions, bounded memory |
| FR-TOP-002/003/008 | 05, 10 | 1:N, 1:1000 fan-out, slow client |
| FR-QOS-001/003/006/007 | 04, 06, 07 | wrong class rejected, ACK outcomes |
| FR-QOS-004 | 06, 07 | same sequence same digest/conflict/gap |
| FR-STO-001/003/005 | 06 | crash matrix + WAL recovered oracle |
| FR-GRP-003/006 | 08 | stale ACK, sparse closure, cursor |
| FR-CLU-002/004/005/011 | 07, 09 | fenced owner, Q2 quorum loss |
| NFR-RES-001/006 | 10 | offered/admitted vs memory sweep |
| INV-01/03/07/08/13/16 | 06–09, 11 | failure matrix |

Production evidence expands this mapping to every normative requirement, not just MVP.

## Measurements and baseline validity

Measure offered → admitted → committed → delivered rates and bytes separately. Count physical fan-out; reject/drop counts accompany p99. For Q0 compare to NATS Core at matching TLS/profile (or explicitly note local plaintext mismatch and prevent VAL-01 claim). For Q1/Q2 compare appropriately configured JetStream modes, equal replication/durability boundary where possible; if a mode is not semantically equivalent, report a limited observation, not an 'advantage'.

Record CPU, memory RSS, kernel socket memory, NIC, disk writes/fsync, pending queues, recovery time, p95/p99/p99.9 and total concurrent authenticated sessions. Keep raw histograms, seeds, exact command, hardware and source SHA. Early MVP results are exploratory; H1/VAL-01 acceptance occurs after product rig exists.

## CI philosophy

PR-free by default. On docs change: run doc checker and optionally diagram syntax check. On Rust change: formatting, cargo check and tests touching changed crate; on WAL/ACK/Raft: targeted crash/model tests. Do not run a 72-hour soak or generate release artifacts on every commit. Trigger large benchmark or compatibility suite only at corresponding milestone, manual dispatch or explicit performance-impacting change.

## B-07 Q1 durability and crash-recovery campaign

`scripts/test_q1_durability.py` executes the B gate with real external SDK processes. Each
campaign has its own `scripts/q1_oracle.py` journal: append-only JSONL written and fsynced
by the parent process outside the broker data directory before any deliberate termination.
The journal records publication attempts and received `COMMITTED_LOCAL` results, delivery
tokens, ACK attempts and received `ACK_COMMITTED` results, generation changes, faults and
restart observations. Broker WAL files are only supplementary; the independent journal is
authoritative for externally observed outcomes.

Reconciliation compares exact identities, not counts: each externally acknowledged
publication must be recovered with the same message ID, payload SHA-256, incarnation and
offset; repeated producer tokens must not create a second committed record; a conflicting
payload for a committed token must be rejected; sparse ACKs may not move the cursor past an
unattempted or unacknowledged application record; acknowledged offsets must not be
redelivered. A delivered record whose ACK reply was lost is classified as uncertain progress,
never as a skip or a durability failure.

Verdicts are `PASS`, `FAIL`, `INVALID`, `NOT_RUN`. Missing conclusions, truncated lines,
non-contiguous journal sequence numbers or missing post-recovery observations are `INVALID`;
an interrupted fixture can never yield a pass. `scripts/test_q1_oracle.py` feeds deliberately
corrupted evidence (missing records, changed digests, duplicate publications, wrong
offsets/incarnations, missing ACK state, cursor gaps, stale-generation commits, truncated
journals) and requires rejection.

The executed matrix covers publication crashes before append, after append/before sync and
after sync/before reply; consumer ACK crashes at the same barriers plus cursor-checkpoint
append/sync; generation-claim crash before the fence is durable; sparse out-of-order ACKs;
generation replacement; all seven WAL fault points across segment rotation; corrupt and
missing committed storage; Q0 isolation under a blocked Q1 storage barrier; and repeated
restart identity stability. Measurements include end-to-end publication/ACK latency, direct
WAL append/fsync latency, broker peak RSS and CPU jiffies, recovery readiness time and
recovered record counts. Q1 internal queue, lease and sparse-set occupancy have no public
metric endpoint and are reported `UNAVAILABLE`. Small samples are exploratory: p99.9 is
reported with its sample count and is not a production SLO claim.

A defect found by this campaign has been fixed: B-05 made application offsets sparse, but
the major-1 codec still rejected odd `READ_RECORD`/`READ_END` offsets, so replay after
interleaved consumer metadata returned `RESOURCE_EXHAUSTED`. The codec now accepts sparse
positions; the golden vectors are unchanged and a protocol regression pinned the behavior.

## B-06 retry and terminal-disposition campaign

`scripts/test_q1_terminal.py` drives real external SDK consumers through NACK, bounded
backoff, exhaustion, quarantine and a real DLQ partition. The parent journal records NACK
requests, retry scheduling, deliveries, terminal commits, DLQ publications and recovered
cursors. `q1_oracle.reconcile_terminal` rejects a DLQ terminal without a prior destination
publication, a missing or duplicated DLQ record, a payload-digest mismatch, a terminal for
an uncommitted source offset, cursor movement across an unexplained source record, and any
incomplete journal.

Deterministic barriers cover crash after the PENDING record append/before sync, after the
PENDING sync, after the DLQ commit/before the source terminal, and the committed terminal
append/sync windows. Recovery must produce exactly one DLQ record, complete the source
terminal only after its own barrier, and retain the original publication in every case.
Actor tests additionally cover bounded retry/terminal state, corrupt terminal metadata,
unavailable DLQ, sparse terminal closure and real destination readability and idempotence.
