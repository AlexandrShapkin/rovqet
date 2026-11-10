# 15 — Decision inventory: fixed behavior vs replaceable technology

This register is a **technical choice table**, not an approval or change-control workflow. An agent may make a local implementation decision where the reference default is given. Only a violation of normative semantics or a measured disadvantage should trigger architecture rework.

| ID | Question | MVP reference choice | Evidence that can change it | Must remain invariant |
|---|---|---|---|---|
| DEC-01 | Transport | TCP loopback/plaintext in lab; production-oriented TCP/TLS interface | M0-A QUIC vs TCP CPU/connection, p99, loss/reconnect | same broker ACK and wire semantics |
| DEC-02 | Session concurrency | per-session Tokio task, shared timer tick | session memory slope, wakeups, p99 | no OS thread/client or unbounded sessions |
| DEC-03 | Subscription index | per-topic sharded hashmap for exact filters; trie for wildcard | M0-C subscriptions/wildcard scale, lock profile | no scan-all-clients per publish |
| DEC-04 | Q0 scheduler | bounded weighted queues with CONTROL reserve | PERF-P1 mixed-load and starvation | fair bounded service, authorized priority |
| DEC-05 | Durable state | append-only segmented WAL with WAL-derived indexes; redb possible metadata candidate | M0-B fsync/compaction/recovery | Q1 fsync; Q2 durable majority |
| DEC-06 | Dedup | producer high-water + bounded outcome history integrated with committed log | producer-cardinality and crash tests | same-seq identical outcome / conflict |
| DEC-07 | Raft core | pinned external OpenRaft adapter | API stability, storage correctness, load | never in-house Raft/false quorum ACK |
| DEC-08 | Q2 grouping | several logical partitions in a fixed shared Raft group | M0-C hot partition interference + recovery | logical offsets isolation |
| DEC-09 | Metadata topology | 3 static seed voters in MVP | join/rebalance experiments | epoch/fence and CP outage contract |
| DEC-10 | Metrics | structured log, bounded counters, raw benchmark histograms | profiling overhead and product need | no exporter hot-path block |
| DEC-11 | Secondary SDK | Rust first, Go or Python after MVP | use-case demand after proof | exact protocol semantics |
| DEC-12 | Protocol numbers | major-1 central registry from 04 and existing A-01 golden vectors, frozen by R0-05 | golden-vector compatibility | no conflicting independent IDs |
| DEC-13 | Q1 lost-history | new unique log incarnation + explicit SDK reset | none without changing documented failure class | no silent replay continuity |
| DEC-14 | Q1/Q2 per partition | immutable durable_class | none in v1 | no mixed committed prefix |
| DEC-15 | DLQ inter-shard | idempotent durable two-step disposition | crash tests may require refinement | no source cursor skip |
| DEC-16 | Prototype security | static test identities in isolated environment | entering untrusted/public deployment | no false claim of SEC/production compliance |

## OpenRaft integration note

As checked from docs.rs, OpenRaft pre-1.0 API evolves; pin the exact crate version and lockfile at implementation, and write one storage + network adapter with an explicit durable-majority fixture. No component should use raw library internals outside \`rovqet-consensus\`. Reference docs: https://docs.rs/openraft/latest/openraft/.

## Decision protocol for an autonomous coding agent

1. Find the fixed semantic contract in requirements and this architecture.
2. Use the reference choice if it compiles and passes affected fixtures.
3. If impossible or measurably worse, change only the local seam, state the measurement/constraint and update the decision row + tests.
4. Do not generate an ADR for every ordinary refactor or ask for human consent merely to choose a crate-internal data structure.
5. Never call a hypothetical implementation 'verified' without executing the relevant test.

## Remaining architecture details to freeze at coding gates

- M1: numeric wire frame/TLV/error registry, exact encoder golden vectors, list of supported optional features. \`log_incarnation\` must be in all durable wire references.
- M2: actual WAL binary version and manifest ordering, disk-sync contract on target FS, producer history retention size, ACK checkpoint consistency.
- M3: membership network message types and metadata snapshot/lease TTL unit.
- M4: pinned Raft library/storage adapter version and deterministic Q2 state machine snapshot schema.
- M5: experimentally selected scheduler, transport default and grouping thresholds.

These are bounded **interface freezes**, not whole-system architecture discovery; roadmap work packages supply defaults and acceptance tests.

## R0-04 bounded reference decisions (2026-11-10)

Retain Tokio and the existing framed TCP transport. The operational Q0 implementation already supplies the runtime/session seam, bounded codec and real-socket tests. No concrete transport defect requires QUIC; QUIC remains deferred by the MVP profile. This is the explicitly permitted reference-default rationale, not a TCP-versus-QUIC performance finding. No historical broker benchmark data is collected or reinterpreted for this decision.

For B-01 retain append-only segmented files, explicit file synchronization, atomic manifest replacement followed by directory synchronization, and indexes reconstructed from the committed WAL. No separate KV engine is needed to establish this ordering; a KV candidate would introduce another durable publication boundary without a demonstrated requirement. Use a single exclusive writer per WAL with bounded submissions and batches. Storage calls run outside the Tokio executor's asynchronous worker threads when integrated by B-02. Q0 has no storage dependency.

OpenRaft remains the external library assumption for Q2, isolated in the future consensus adapter. Pin its exact version when C-03 implements that adapter; B-01 introduces neither Raft dependencies nor simulated quorum success. Q1 and Q2 must have distinct histories.

Evidence commands: `CARGO_HOME=/tmp/rovqet-cargo cargo test -p rovqet-protocol --locked --offline` passed 13 unit and 3 golden-vector tests on 2026-11-10. The B-01 filesystem/fault tests validate the selected storage seam; no extended performance study or power-loss claim is part of R0-04. The existing reference defaults are selected on implementation fit and correctness, without a claimed fsync latency distribution.

R0-05 fixes the existing major-1 protocol registry and the B-01 concrete storage contract in [06](06-q1-storage.md). Future producer dedup retention and consumer ACK checkpoint policy remain B-03/B-05 actor concerns; the typed WAL records preserve their recoverable ordering seam.
