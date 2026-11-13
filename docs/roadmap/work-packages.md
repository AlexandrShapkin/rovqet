# Dependency-ordered executable work packages

**Do not invent new architectural goals when implementing these tasks.** Each task identifies the contract chapter and a minimum proof. Work packages may be subdivided into smaller commits without changing interfaces. Status of a task is tracked in \`tasks.json\`, not by issue/PR bureaucracy. A task is DONE only after its test actually ran.

## D0 — Architecture and repository preparation

**D0-01 — Normative sources.** Commit exact English requirements and historical change register to \`docs/requirements\`; link them from README; confirm source precedence. Done when both files are readable in main and no content is summarized away.

**D0-02 — Formal reference architecture.** Write \`docs/architecture/00–16\`, C4 context/deployment, PlantUML publish/ACK/ownership flows, contracts for Q0/Q1/Q2 and failure matrices. Done when every MVP subsystem has state owner, inputs, output, durability boundary, failure behavior and test.

**D0-03 — Autonomous agent workflow.** Add AGENTS.md, docs checker, concise task protocol and focused CI; no mandatory PRs/approvals/releases. Done when docs check runs successfully and main is writable.

**D0-04 — Consistency audit.** Check source requirements against design for FR-QOS, FR-STO, FR-GRP, FR-CLU, INV-01…16 and prototype deviations; resolve contradictory claims; keep open only documented M0 experiment choices. Done when cross-reference checker and manual contract checklist pass. Do not mark complete simply because docs exist.

## R0 — Minimal M0 experimental groundwork

**R0-01 — Rust workspace bootstrap** (depends D0). Create Cargo workspace rooted at \`Cargo.toml\`; library crates/modules as architecture 02; single \`rovqet\` runnable binary with \`--version\` and \`--config\`; Rust SDK crate. Avoid empty crates if modules are sufficient. Proof: \`cargo fmt --check && cargo check --workspace\`.

**R0-02 — Independent load generator and oracle** (depends R0-01). Implement open-loop publisher/consumer driver with real independent TCP sessions, deterministic PRNG seed, 1:N/N:N, configurable 256B/1KiB payload, JSONL oracle of send/ACK/delivery/recovery and HDR percentile summarizer. Use external process and independent storage. Proof: generator self-test and known artificial transport fixture; no claims about broker performance yet.

**R0-03 — Baseline runner** (depends R0-02). Supply reproducible NATS Core Q0 and JetStream reliable reference configs, scripts to bring up isolated baseline and run equal-rate workload; record versions/semantics and raw results. No invented baseline measurements. Proof: one successful repeatable baseline smoke with committed fixtures, or explicit NOT_RUN if unavailable.

**R0-04 — Transport and storage spikes** (depends R0-01). Use the existing Tokio/TCP and segmented WAL reference defaults unless a concrete defect requires a bounded spike. A documented reference-default rationale is sufficient; do not build QUIC solely for a checkbox. If a filesystem experiment is needed, record exact commands, bounded workload and observed sync/rotation behavior without an extended performance study. Proof: executable evidence or explicit default rationale in architecture chapter 15.

**R0-05 — Reference decision freeze** (depends R0-03,R0-04). Freeze the existing major-1 wire registry, basic session and concrete local WAL API, exact MVP profile and OpenRaft integration assumption. Proof: architecture updated with decision and a runnable codec fixture plan; no approval ceremony.

## A — Single-node Q0 and Rust SDK

**A-01 — Core types and binary framing** (depends R0-01). Implement IDs/QoS/priority/request errors, canonical varint, bounds-first framing, TLV parsing, version/cap negotiation, central registry/golden vectors. Files: \`crates/protocol\` or equivalent. Proof: malformed length/varint/duplicate critical field tests and binary golden vectors.

**A-02 — Session and transport** (depends A-01). Implement loopback TCP listener, HELLO/WELCOME/AUTH/AUTH_OK test identity, session state, heartbeat and bounded inflight/outbound writer. Files: transport/server. Proof: 1,000 sessions smoke, no per-connection thread, rejects data pre-auth, no unbounded read buffer.

**A-03 — Topic routing and subscriptions** (depends A-01). Fixed topic config, partition hash, exact-topic indexed subscribe/unsubscribe, later bounded wildcard trie; server policy selected priority. Files: router. Proof: stable hash fixture, concurrent subscribe/unsubscribe, wrong topic not delivered.

**A-04 — Budgeted scheduler/admission** (depends A-02,A-03). Hierarchical reservation of byte/message/session/egress capacity, basic weighted priority queues, resource reserve, overflow states. Files: scheduler. Proof: hitting each limit rejects rather than growing memory; Q0 drops have explicit reason.

**A-05 — Q0 fan-out vertical slice** (depends A-02,A-03,A-04). Implement PUBLISH→ADMITTED, shared \`Bytes\` payload, subscription matching, per-subscriber queue, send credits, TTL and configured Q0 overflow policies. Proof: publisher→subscriber, 1:1000, one slow subscriber isolated, Q0 no WAL dependency.

**A-06 — Rust SDK and smoke CLI** (depends A-05). SDK connect, publish/subscribe, typed outcomes, reconnect/backoff; test-only CLI can wrap SDK. Proof: two independent SDK processes exchange and disconnect cleanly.

**A-07 — Q0 evidence gate** (depends A-06,R0-02). Run real sessions (10k smoke if environment permits), 1:N and N:N, oversubscription, p95/p99 and process+kernel memory. Proof: raw JSONL/histograms and profile including actual hardware, explicit unfinished PERF targets. A gate failure is fixed as code, not waived by opinion.

## B — Q1 local durability

**B-01 — Segment WAL writer/reader** (depends A-01,R0-05). Implement versioned manifest, record lengths/checksums, append, rotate/seal, fsync/group commit, bounded writer queues and record index. Proof: append/reopen, torn tail, checksum corruption before/after committed boundary.

**B-02 — Q1 partition actor and local PUB_ACK** (depends B-01,A-05). Immutable Q1 partition policy, local owner epoch, offset allocation, append+fsync, COMMITTED_LOCAL only after durable barrier; independent of Q0 queues. Proof: kill between write/fsync/ACK; every acknowledged record recovers on intact disk.

**B-03 — Idempotent producer and dedup** (depends B-02). Scope identity/epoch/sequence by principal+partition+lane; high-water + bounded outcomes, conflict/gap/fence, recovery from WAL. Proof: same/changed/gapped/outdated token; lost ACK and restart return same offset.

**B-04 — Replay and log incarnation** (depends B-02). \`FETCH\` by incarnation+offset/limits and bounded segments; REPLAY_NOT_AVAILABLE for expired or lost Q1 history; explicit reset after permanent owner loss. Proof: stable replay after crash, no same-incarnation continuity with unrecoverable history.

B-04 uses additive standalone READ_FETCH/READ_RECORD/READ_END frames with exact encoded
batch limits and mandatory completion. Existing group FETCH/MESSAGE remain unchanged.
Real TCP/SDK, restart/journal, fault and cancellation evidence is recorded in tasks.json.
Standalone inspection does not implement B-05 delivery, leases or cursor persistence.

**B-05 — Durable group lease/ACK/cursor** (depends B-03,B-04). Lease tokens, generation fencing, bounded sparse ACK set, contiguous cursor durable barrier and idempotent duplicate ACK read. Proof: out-of-order ACK gap, stale generation, crash before/after fsync result, redelivery.

B-05 uses FETCH/MESSAGE/CONSUMER_ACK/CONSUMER_ACK_RESULT with an additive terminal
FETCH_END (26). One generation per group is durably fenced when FETCH omits/zeros the
generation; leases are volatile and redelivery follows expiry or restart; CONSUMER_ACK and
CURSOR_CHECKPOINT records extend the existing v1 WAL without changing its binary layout.
Real external consumer processes, a parent-owned ACK/cursor journal and three ACK crash
windows are exercised by `scripts/test_q1_consumer.py`. B-06 owns NACK, quarantine/DLQ and
terminal disposition; B-07 is the crash/oracle gate.

**B-06 — Q1 terminal disposition** (depends B-05). Retry/NACK, quarantine and DLQ idempotent two-step protocol using Q1; keep source incomplete if DLQ fails. Proof: crash between DLQ commit and terminal record, no cursor skip. Can follow first Q1 PoC if strict time pressure; cannot claim terminal support until tested.

**B-07 — Q1 crash/oracle gate** (depends B-05,R0-02). External acknowledged set versus recovered WAL after repeated SIGKILL at controlled barriers; memory and fsync p99. Proof: 0 acknowledged Q1 losses with intact disk and honest uncertain outcomes. SIGKILL != physical power cut.

B-07 is executed by `scripts/test_q1_durability.py` with the append-only journal oracle in
`scripts/q1_oracle.py` and its self-corruption fixtures. The matrix covers publication and
ACK crash barriers, checkpoint append/sync, generation-claim fencing, sparse ACKs, all seven
WAL fault points across segment rotation, corrupt/missing storage and Q0 isolation; raw
journals, exact reconciliation and measurements live under `benchmarks/evidence/q1-durability-*`.
The campaign found and fixed a sparse-offset codec defect in the B-05 replay path.

## C — Cluster ownership and Q2 quorum

**C-01 — Inter-node RPC and snapshots** (depends A-01,A-02). Typed inter-node framing, bounded proxy hop, metadata snapshot version, node ID, cluster ID and owner epoch checks. Proof: stale map redirect, no uncontrolled forwarding loop.

**C-02 — Metadata group and ownership** (depends C-01,B-04). Integrate external Raft for metadata; static 3 voters, topic/policy and placement snapshot, epoch+lease, staged Q1 transfer. Proof: stale owner fenced, no split brain under asymmetric partition.

**C-03 — Q2 Raft storage adapter** (depends B-01,C-02). Integrate pinned OpenRaft, durable log/term/vote and deterministic snapshot/apply. Keep library confined to consensus crate. Proof: 3-node library smoke, snapshot install, committed prefix invariant.

**C-04 — Q2 publish and dedup** (depends C-03,B-03). Immutable Q2 partition class; deterministic producer outcomes/offsets from committed state machine; quorum-only COMMITTED_QUORUM. Proof: leader crash before and after majority, retry same offset, NO_QUORUM when 1/3.

**C-05 — Q2 durable consumers** (depends C-04,B-05). Group ACK/cursor and optional DLQ state backed by Raft quorum, no local-only ACK. Proof: fail leader after ACK, recover cursor, no skip; stale generation forbidden.

**C-06 — Q1/Q2 class and migration gates** (depends C-04,B-04). TEST-017 incompatible publish rejects before append; Q1 owner migration with complete WAL+dedup; Q1 lost owner creates new incarnation; Q2 unaffected. Proof: external oracle across 3 nodes.

**C-07 — Cluster fault gate** (depends C-05,C-06). Kill 1 node, lose majority, stale metadata lease, log corruption; verify Q2 acknowledged records and Q0/Q1 isolation. Proof: raw failure matrix with oracle, explicit limitations on physical failure domains.

## D — Evaluate the concept

**D-01 — Resource/scheduling measurement** (depends A-07,B-07,C-07). Measure process+socket RAM per connection, CPU, Q0/Q1/Q2 p99, physical fan-out, fsync/quorum overhead and CONTROL isolation under mixed load. Proof: exact reproducible profile, raw histograms.

**D-02 — Semantically constrained comparisons** (depends D-01,R0-03). NATS Core and JetStream versus Rovqet with matched ACK/durability/RF/TLS/recipients where possible. If no equivalent baseline, state semantic delta. Proof: honesty checker covers offered/admitted/committed/delivered and rejects/drops.

**D-03 — MVP verdict** (depends D-02). Summarize whether design demonstrates differentiated value, observed limitations, likely bottlenecks and next constraints. Explicit 'concept failed/inconclusive' is acceptable scientific outcome. Do not declare production SLO certification.

## P — Post-MVP production work

**P-01 — Security and operations.** TLS 1.3/mTLS, JWT/JWKS, ACL, admin HTTPS and audit, production config validation, parser security, secret handling.
**P-02 — Scalability and QoS hardening.** Full M0-C/M5 tuning, dynamic grouping, QUIC adapter, 50k/150k density scenarios, H1 percentile targets, multi-tenant isolation.
**P-03 — Release and independent acceptance.** Rolling upgrades, backup/restore, long soak, exact-head test evidence, production TEST-001…017, REL/PERF/VAL-01 and M7 deliverables.

## Task dependencies and parallelism

A-01/A-03 are parallel after bootstrap; R0-02 can run alongside A-01 once SDK stub exists. Q1 WAL tests can be developed independently of Q0 fan-out after protocol types settle. C-01 metadata RPC can be drafted alongside B-05, but C-02 must not claim Q1 migration without B-04. Parallel agents may own separate crates, not edit the same contract file concurrently.

## Per-task record

Each DONE transition records \`commit\`, \`test_command\`, \`observed_result\`, \`limitations\`. Evidence may be a short comment in tasks.json or linked fixture; no PR approval required.
