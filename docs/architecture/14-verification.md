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
