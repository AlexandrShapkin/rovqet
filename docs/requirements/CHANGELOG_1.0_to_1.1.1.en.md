# Requirements Specification Change Register: 1.0 → 1.1.1

**Product:** Specialized message broker for high client connection density  
**Date:** 2026-11-05  
**Language:** English  
**Latest specification:** `BROKER_REQUIREMENTS_v1.1.1.en.md`  
**Status:** Historical change register. The specification, not this register, is normative.

Version 1.1 retains the original sections 1–17, Q0/Q1/Q2 requirements, Rust/Tokio, bounded resources, Raft-shard aggregation, metadata/data-plane separation, experimental ADRs, and target PERF hypotheses. New provisions do not replace the source text: the consolidated complete specification is delivered separately.

## 1. Targeted changes: 1.0 → 1.1

| ID | Section / requirement | What changed | Basis | Why this solution was chosen | Verification |
|---|---|---|---|---|---|
| CH-01 | Cover, §1 | Updated version, date, and sources | Completeness of revision | Explicitly identifies version 1.1 status | Document inspection |
| CH-02 | §3.2 | Defined ACK accepted/committed and durable cursor | Grok M-01; Kimi E-01 | One consistent conceptual boundary without a new subsystem | TEST-011 |
| CH-03 | FR-GRP-006, FR-QOS-002, §5.4/6.5, INV-13 | Consumer ACK validity/durability, fencing, idempotency, lost response, contiguous cursor | All three reviews | Normative contract; same-log remains recommended; no publish+ACK transaction required | TEST-011, REL-05 |
| CH-04 | FR-TOP-008, §6.3/6.4, OBS-001 | Multi-level fan-out quotas, per-subscriber buffers, egress metrics, slow-client policies | Grok M-03; Qwen #3 | Limits without arbitrary fixed numbers or unnecessary restrictions on mass broadcast | TEST-014 |
| CH-05 | FR-CLU-004, FR-STO-007/010, §6.5, INV-14 | Isolation of mixed durable-shards; compaction, retention, quotas and logical offsets | Kimi E-02, Qwen #1 | Preserves shard aggregation while preventing cross-partition violations | TEST-012 |
| CH-06 | FR-CLU-011, §4.8, INV-15 | Normative degraded-mode matrix: control-plane outage, security TTL, ownership, quorum, disk | Kimi E-03 | A control-plane outage need not stop safe data-plane operations, but mutations fail closed | TEST-013 |
| CH-07 | FR-QOS-004, ADR-002 | Identity/epoch/sequence, conflicts, gaps, dedup window and compact-state bounds | Qwen #5; Kimi meta-analysis | Strict contract without mandating a two-tier or per-message store | TEST-015, M0-B |
| CH-08 | NFR-RES-006, §6.4, §8.1, §14 | Capacity model for RAM/CPU/kernel/NIC/disk, admitted load, hysteresis, reserved buffers | Kimi E-04 | Model already needed for the M1 gate; auto-tuning unnecessary | TEST-014, PERF-M1 |
| CH-09 | §5.1, §5.1.1–5.1.2 | Error catalog, CONSUMER_ACK_RESULT, versions and golden vectors | Grok M-05; Kimi E-08 | Completes existing wire-protocol deliverable before M1 and closes ACK semantics | TEST-001/003/011 |
| CH-10 | §5.4.1 | DLQ/QUARANTINED/replay, idempotent terminal state, failed DLQ | Kimi E-08 | Clarifies an existing state machine without an external distributed transaction | TEST-011 |
| CH-11 | SEC-004/007/009 | Pre-auth/handshake budgets, admin authority, TTL, fail-closed behavior | Kimi E-07 | Minimum specification of access control and quotas | TEST-009/013/014 |
| CH-12 | PERF-C1 | Ten seconds per individual attempt among 10k attempts, from start to AUTH_OK; 60 s arrivals + 10 s observation | Ambiguity in original text | Testable interpretation without changing 95%/10s/10k | TEST-007 |
| CH-13 | VAL-01, §9.2, TEST-016 | Pre-registered semantic equivalence rubric and delta report | Kimi E-05 | Prevents spurious wins from weaker guarantees or hidden drops | TEST-016 |
| CH-14 | M0-A/B/C, ADR-001/003/005 | Advance criteria for transport, KV, scheduler and shard selection, metadata load | Grok M-06; Qwen #1/4/6 | Choose using reproducible profiles, not fixed percentage rules | M0 reports, M5 |
| CH-15 | §9.3, §9.6 | TEST-011…016 and linkage to degraded-mode matrix | Tests must demonstrate P0 contracts | Minimal expansion of the existing test suite | Fixture/command/oracle |
| CH-16 | §10.1/10.4, §12, §13 | Requirement→test→fixture→command→evidence→status; updated gates and deliverables | Kimi E-06 | A single authoritative source of requirements | DoD audit |
| CH-17 | §11, §15, §17.3 | Risks R-29…33, invariants 13…15, independent-review status | Closed-loop verification of changes | Only causally justified additions | Cross-section audit |

## 2. Decisions on expert comments

**Accepted:** precise ACK/cursor durability contract; shard isolation; explicit degraded modes; multi-level fan-out budgeting; capacity/admission model; pre-registered semantic baseline; required error registry and traceability. Existing requirements were strengthened only where a verifiable condition was missing.

**Partially accepted:** strict same-log storage (retained as a recommended implementation, while equivalently recoverable durability is allowed); separate two-tier dedup store (bounded state and M0-B experiment retained, without a mandatory technology); quantitative QUIC selection (predefined metrics yes, arbitrary decision algorithm no); numerical shard-sizing/subscriber budgets (only values measured for a workload).

**Rejected:** mandatory “1,000 subscribers,” “10–50 partitions per shard,” “10k messages/s per shard,” “choose QUIC when p99 delta is below 20%,” “hybrid dedup hot 1 h/cold 23 h,” a mandatory independent cursor store, and mandatory adaptive sharding. Their benefits are unproven, and they conflict with minimum sufficiency or hardware-independent requirements.

## 3. Preserved without architectural changes

Q0/Q1/Q2 classes; independent volatile/durable lanes and no cross-lane order; no silent downgrade; metadata Raft outside the hot path; coordinated ownership and fencing; Raft aggregation; Rust/Tokio; segmented WAL and pluggable storage; TLS/mTLS; ACL/tenant isolation; Q0/Q1/Q2 crash boundaries; at-least-once/dedup; P0/P1/P2; PERF-D1/D2/D3/R1/P1/M1/C1 and REL-01…07 (target hypotheses unchanged); VAL-01 threshold; M0…M7; TEST-001…010; existing risks R-01…28 and invariants INV-01…12.

## 4. Questions remaining open to experiments

M0-A: TCP/TLS versus QUIC in LAN, loss, NAT and reconnect conditions; M0-B: WAL/KV and dedup state; M0-C: shard grouping, hot-partition impact, metadata load and subscription-index scaling; M1: connection resource slope and full capacity/admission calibration; M5: scheduling fairness, sustainable capacity, numerical PERF hypotheses, and VAL-01 under comparable semantics. No winning implementation or optimal limits are selected in advance. Added risks: ACK write amplification, ACL/ownership expiry, conservative admission (R-29…33). Operational SLOs are not yet demonstrated.

## 5. Changes in version 1.1 → 1.1.1

The 1.0 → 1.1 section (CH-01…CH-17) is preserved with no alteration to its entries. The following changes were accepted after independent examination of three consistency scenarios. All new rules apply to existing subsystems; no new delivery-guarantee model is introduced.

| ID | Section / requirement | What changed | Basis | Why this solution was chosen | Verification |
|---|---|---|---|---|---|
| CH-18 | §3.2, FR-STO-001, FR-CLU-004, §6.5, M2–M4 | Separated Q1 local WAL from Q2 replicated Raft-shards at the commit boundary; preserved shared StorageEngine | Original wording requiring every durable partition to reside in a Raft-shard conflicted with Q1 standalone/M2 | Shared WAL API and physical aggregation are allowed; Q1 does not require a quorum, Q2 does not relinquish one | TEST-017, TEST-004, M2/M4 gates |
| CH-19 | FR-QOS-001/006/007, FR-STO-003, FR-CLU-005, §5.3/5.5 | Prohibited mixing Q1/Q2 within one logical durable partition; fixed durable_class; Q1 loss establishes a log-incarnation boundary | A shared offset stream with lost Q1 history could violate Q2 prefix/replay | Preserved the two Q0/durable lanes, shared namespace and storage; incompatible classes are explicitly rejected | TEST-017, REL-01/04, INV-16 |
| CH-20 | §3.2, FR-GRP-003/006, §5.4/5.4.1, INV-13 | Cursor now includes contiguous committed ACK and durable terminal disposition; SEEK is a separate audited action | INV-13’s “ACK only” rule conflicted with DLQ/QUARANTINED without a user ACK | Idempotent DLQ transition needs no cross-shard XA; unfinished offsets are never skipped | TEST-011, oracle, DLQ fault injection |
| CH-21 | §6.1/6.5, §9–13, §15–16, R-34, INV-16 | Aligned M2–M4 phases, fault matrix, TEST-017, mandatory-invariant status and version 1.1.1 | Related requirements must have consistent checks and gates | Minimal cross-section alignment without new QoS classes or numerical SLOs | ID uniqueness audit, render, text diff |

## 6. Rationale and alternatives evaluated

### CH-18. Q1 and Raft-shards

**Confirmed:** if every durable placement necessarily has a Raft group, M2 and M3 cannot implement Q1 without unannounced consensus overhead. Alternatives considered: (1) Raft for every durable record; (2) separate storage backend implementations; (3) a shared StorageEngine with local Q1 groups and replicated Q2 physical groups. **Selected: (3).** The WAL and interfaces remain shared; both commit barriers already existed, and no new product layers are introduced. **Risk:** loss of a Q1 node may destroy local history and must not be disguised as reconstruction from Q2.

### CH-19. A logical partition and differing guarantees

**Confirmed:** a Q1, Q2, Q1 sequence with shared offsets cannot simultaneously promise Q1 local-only durability, Q2 Raft committed-prefix, and continuous replay after irrecoverable loss of Q1 data without an additional protocol. Alternatives considered: (1) a shared physical Raft log with noncommitted Q1 prefixes and extra recovery rules; (2) separate per-class offset streams/lanes; (3) immutable `durable_class` on a logical partition. **Selected: (3)** as the minimum v1 solution. Q0 remains independent; Q1 and Q2 can operate in different topics/partitions through the shared StorageEngine. An incompatible publish is rejected before admission. **Constraint:** choosing Q1 versus Q2 within the same key/partition requires an explicit topic/partition change, not arbitrary per-message override.

### CH-20. ACK, terminal disposition and cursor

**Confirmed:** the previous INV-13 prohibited any cursor advance without an ACK, while §5.4.1 allowed DLQ/QUARANTINED. Alternatives considered: (1) mandatory artificial ACK on terminal transition; (2) indefinitely retaining the cursor in place; (3) generalizing committed closure to a valid ACK or policy-authorized durable terminal disposition, with a separate privileged SEEK. **Selected: (3)** with a contiguous-prefix invariant, equivalent-or-stronger durability, durable DLQ before terminal marker, idempotent recovery, and no skip on failed DLQ. Cross-shard XA is not required; TEST-011 checks crash boundaries.

### CH-21. Normative status and acceptance changes

**Confirmed:** INV-01…INV-15 were already mandatory under the existing DoD, so the former “recommended invariants” heading was inaccurate; it was renamed **“Mandatory Engineering Invariants.”** Added INV-16 and TEST-017; clarified TEST-011 and M2–M4. No changes to the architecture, PERF targets, or P0/P1/P2.

## 7. Accepted and rejected comments

**Accepted:** normative distinction between local Q1 and quorum Q2; prohibition of a shared Q1/Q2 offset namespace within one durable partition; durable closure of a consumer offset by a policy-authorized terminal disposition; new log incarnation after irrecoverable Q1 loss; mandatory status of engineering invariants.

**Not mandated:** separate process/StorageEngine for Q1; additional physical Q1/Q2 lanes on top of the existing Q0/durable separation; replication of all Q1 records solely for a common Raft prefix; distributed XA transactions for DLQ; fixed values for shard sizing, latency or buffers. These options add complexity or change guarantees without necessity. Comments were examined against the original contracts, rather than accepted solely because a reviewer suggested them.

**Unchanged:** CH-01…CH-17, P0/P1/P2, PERF-D1/D2/D3/R1/P1/M1/C1, REL-01…07, VAL-01, M0–M7 phase order, Rust/Tokio, TLS/mTLS, bounded resources, actor/worker architecture, Raft-shard aggregation for Q2, control/data-plane separation, and all requirements not listed above. Experimental ADR-001…006 remain open; ADR-005 was only clarified for local Q1 versus Raft Q2.

**New and remaining risks:** loss of a local Q1 owner requires an explicit log-incarnation boundary for replay; fixing `durable_class` constrains per-message QoS choices within one topic/partition; excessive physical coupling among neighboring Q1/Q2 storage groups can degrade p99 and must be evaluated in M0-C. No additional numerical SLOs were introduced.