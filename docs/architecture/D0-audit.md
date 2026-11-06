# D0 — Architecture preparation audit

**Scope:** design readiness, not a claim that broker code, Raft quorum or production metrics have been verified.

## Source integrity

- The v1.1.1 requirements and 1.0→1.1.1 change register were imported in full and their GitHub UTF-8 contents compared exactly with both supplied source files on 2026-11-06: **exact match**.
- Original normative terminology and requirement identifiers were not rewritten. The architecture is a subordinate reference design.
- The prototype-only deviations (disabled real auth/TLS, reduced operations) are explicitly isolated to dev/local and controlled lab, never asserted to meet production SEC requirements.

## Design completeness inspection

| Design dependency | Contract | Status |
|---|---|---|
| Overall system topology/deployment | 01 | documented |
| Rust crates and typed API ownership | 02 | documented |
| Runtime, cancellation, queue bounds | 03 | documented |
| Wire framing, numeric registry, retry and SDK | 04 + protocol-registry | documented, binary vectors deferred to M1 |
| Q0 topic/fan-out/TTL/slow subscriber | 05 | documented |
| Q1 WAL format, commit barrier/recovery/dedup | 06 | documented |
| Q2 Raft durable quorum, snapshot/leader change | 07 | documented, library adapter unimplemented |
| Durable group cursor, lease, ACK, DLQ | 08 | documented |
| Ownership, control-plane outage and inter-node RPC | 09 + inter-node-rpc | documented |
| Capacity/admission/fair scheduling | 10 | documented, numeric calibration deferred |
| Failure matrices and crash boundary oracle | 11, 14 | documented |
| Explicit MVP security/operational exceptions | 12, 16 | documented |
| Internal transition tables and source diagrams | 13 + diagrams | documented |
| Decision seams and chosen defaults | 15 | documented |
| Dependency-ordered agent tasks and checks | roadmap, AGENTS | documented |

## Mandatory semantic consistency review

- [x] Q0 ADMITTED never advertises durability/delivery; volatile queues do not depend on Q1/Q2 write barriers.
- [x] Q1 is locally committed after fsync; loss of intact process vs permanent disk loss are distinct.
- [x] Q2 only returns durable majority commit and never uses Q1 local-only records in the Raft committed prefix.
- [x] Each durable partition has immutable Q1 or Q2 class; mixed publishing rejects before append.
- [x] Durable offsets include log incarnation; the wire schema now carries it in every durable reference.
- [x] Producer retries use original identity/epoch/sequence and have recorded uncertainty semantics.
- [x] Consumer cursor only crosses committed ACK or committed terminal disposition without gaps.
- [x] Old owner/generation is fenced for mutation; cached metadata cannot create authority after expiry.
- [x] All active queues/inflight and per-subscriber buffers have enforced budget contract.
- [x] The roadmap explicitly distinguishes local Q0/Q1 demo, Q2 proof, honest baseline and productionization.

## Evidence and limitations

The dependency-free documentation checker has run successfully in GitHub Actions for earlier architecture commits; the final updated main must also pass its latest run. The checker validates existing local links, chapter presence, task DAG, source contract identifiers and balanced PlantUML blocks. It is not a Structurizr or PlantUML parser, and neither renderer has been executed here. That syntax should be checked with an installed renderer when available, without blocking ordinary Rust work.

**Not yet verified:** Rust compile/tests (no workspace code yet), actual codec golden vectors, physical fsync/power-cut, Raft library integration, Q2 fault tolerance, H1/PERF/VAL-01 performance, production security. Those are work-package acceptance tests, not D0 claims.

## Exit decision

Architecture and task contracts are sufficiently explicit for the R0 implementation bootstrap without another whole-project architecture brainstorming cycle. A localized missing detail should be resolved in its owning document and accompanying fixture. All scientific/technology choices remain evidence-based per ADR-001…006. No PR approval or human sign-off is required to proceed.

**Next dependency:** R0-01 (Rust workspace), then R0-02 (oracle) and A-01 protocol vertical slice. Architecture documentation is not a deployed broker.
