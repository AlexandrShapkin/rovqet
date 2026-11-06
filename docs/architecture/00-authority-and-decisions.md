# 00 — Architectural authority and invariants

**Target:** fast concept-validation prototype followed by full broker implementation.

## Contract hierarchy and interpretation

1. Normative v1.1.1 requirements define what can be claimed about the *product*.
2. This design fixes a reference implementation and MVP plan, **not** a new normative QoS class.
3. Prototype exceptions are explicit, reversible and confined to loopback/private controlled test benches.
4. Historical change register explains why requirements were selected; it never overrides them.

Priority labels P0/P1/P2 describe impact, **not** implementation order. The roadmap's MVP slices may defer production P0 work provided the prototype does not advertise compliance with the deferred contract.

## System-level invariants

- Q0: admission into bounded volatile state is not delivery and may be lost on crash or configured overflow.
- Q1: \`COMMITTED_LOCAL\` only after local fsync/group-commit barrier on a functioning current owner. Process restart with intact disk retains acknowledged records; permanent disk loss may lose history.
- Q2: \`COMMITTED_QUORUM\` only after durable majority commit in RF=3 Q2 Raft group. No local-only Q1 entry forms part of Q2's committed prefix.
- Each durable logical partition is immutable Q1 *or* Q2; Q0 uses a separate volatile lane. No total order across lanes or partitions.
- Offsets are scoped to partition, lane and log incarnation; never reuse an acknowledged offset within its incarnation. A Q1 lost-history restart uses a **new incarnation**, never invents continuity.
- Durable consumer cursor is the greatest contiguous prefix closed by durable valid ACK or durable authorized terminal disposition; stale-generation mutation is forbidden.
- Producer retries within the dedup window preserve committed outcome for identical scoped identity/epoch/sequence; conflicting payloads fail.
- At most one active ownership authority per epoch; stale owner or client cannot produce new durable ACK.
- Every active queue, buffer, session, in-flight request, subscriber and timer has an enforced bound. Shared processes do not imply shared unbounded budgets.
- No silent downgrade of QoS and no false success when resources/quorum/authority are missing.

## Chosen reference approach

Single Rust workspace and single broker process per node. Tokio for network/task orchestration; bounded mpsc for intercomponent messages; owning partition actors for mutable partition state. Shared immutable payload bytes (\`bytes::Bytes\`); explicit request IDs. Q1 uses append-only local WAL; Q2 uses a proven Raft library behind a narrow group interface (not a new Raft implementation). Metadata/ownership control is separate from Q2 data groups. Q0 and durable delivery have independent queues and budget pools. A small dedicated blocking IO pool serializes fsync/segment operations.

## Open experimental seams (do not redesign without evidence)

- TCP/plaintext in **local-only prototype**, TCP/TLS product baseline; QUIC alternative.
- Reference WAL and in-memory metadata indexes first; transactional KV choice after M0-B.
- Weighted deficit round-robin candidate, initially a simpler bounded priority scheduler if measurable.
- Fixed conservative Raft shard grouping in MVP; tune after M0-C.
- Per-connection Tokio task default; registry/reactor alternative when resource slope demands it.

An undecided value must have a bounded default and replaceable interface; it must not prevent implementing a component already specified.

## Atomicity and uncertainty policy

Never synthesize a publish or ACK success from enqueue completion. Persist outcome/dedup metadata in the same recoverable durability domain as its record, or make recovery provably equivalent. Treat a lost response as \`OUTCOME_UNKNOWN\` until a deduplicated query/retry settles it. A completed physical write whose reply is lost is not a definite failure.

## Change policy

Architecture changes touching externally visible behavior require updating the corresponding chapter, state table and focused fixture in one change. Other internal refactors require no ADR paperwork. Tests that falsify an invariant take precedence over target SLO measurements.
