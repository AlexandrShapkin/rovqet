# 16 — Fast research MVP contract

The project builds a *research PoC*, not a public service. Product v1.1.1 remains the eventual normative destination.

## MVP acceptance ladder

| Slice | Capability | Minimum proof | Intentionally not claimed |
|---|---|---|---|
| A: Q0 | One process, real socket sessions, partitioned topics, indexed subscriptions, live fan-out, TTL, bounded queues, simple priorities, Rust SDK | Real 1:N and N:N exchange; steady load; slow consumer isolation | Crash durability, global order, Internet security |
| B: Q1 | Same process + append-only WAL, fsync barrier, replay, scoped producer dedup, basic durable group, ACK/cursor | Kill and restart with intact disk: no acknowledged record lost; retry does not duplicate committed write within chosen dedup window | Survives permanent disk loss, multi-node failover |
| C: Q2 | 3-node cluster, minimum metadata ownership, Raft-backed Q2 partitions, quorum ACK, consumer group progress | Fail one node after acknowledged Q2 write: committed data survives; fail quorum: new Q2 ACK denied | Multi-region high availability, production operational excellence |
| D: evaluate | Deterministic load generator, NATS equivalence rubric, p95/p99/p99.9, connections/CPU/RAM/egress, fault oracle | Evidence of whether selective reliability improves resource/latency envelope | VAL-01 or production SLO PASS until required experiment conditions actually met |

An *early demo* is A+B. A *concept-validation MVP* is A+B+C+D. A passing smoke test is not proof that the full PERF-* hypotheses hold.

## Explicitly deferred in the MVP

Production multi-tenant identity, JWT/JWKS, ACL hierarchy, certificates/rotation, mTLS, internet-facing admin API, security penetration testing, SBOM, long-term backup, rolling upgrade, Kubernetes, full production dashboards, continuous fuzz farm, 72h soak, QUIC, bridge protocols, high-cardinality audit trails, cross-version migrations and full formal evidence bureaucracy.

Use a **development profile** with localhost by default, disposable data, test principals, and plaintext if faster. This exception must be visible in config/README and prevented from accidental public exposure. Production claims are unavailable until the deferred checks are implemented.

## Not deferrable even in PoC

Honest Q0 \`ADMITTED\` outcome; honest Q1 local fsync and Q2 durable quorum ACKs; immutable per-partition durable class; bounded active memory/queues; basic fencing; isolation of volatile and durable paths; correct consumer cursor; recoverable WAL semantics; re-runable tests with independent correctness oracle. Failure to preserve any of these makes the concept experiment scientifically invalid.

## Prototype simplifications

- One binary + Rust SDK; optional CLI is a thin wrapper around SDK.
- No generic pluggable deployment framework; environment config file and environment variables.
- Fixed test topic policies, test identity (never production), static three-node cluster membership initially.
- Single disk per node for Q1 and Q2; lightweight persistent metadata. Durable majority still uses separate nodes for Q2.
- Start with exact topic subscriptions, then indexed wildcard subset. No bridge, advanced schema registry or multi-language SDK.
- One consumer group per partition in first vertical slice; bounded inflight ACK with explicit out-of-order storage, later rebalance.
- Use a library Raft core; never substitute 'replicated TCP send' for quorum commitment.
- Prefer deterministic instrumentation over production monitoring systems.
- Fast local checks on changed components; long benchmarks only at focused milestone gates.

## MVP exit report

Record build SHA, machine/profile, exact command, actual connection count, offered/admitted/committed/delivered rates, p95/p99, CPU and process+socket memory, test cases, failures and comparisons. Explicitly name unsupported product requirements. An unrun test is NOT_RUN rather than PASS.


## Implemented B-02 boundary

The loopback development server optionally exposes one explicitly configured Q1 publisher route with real local fsync, scoped recoverable outcomes and SDK retry identity. B-03 adds a configurable 24-hour-default conservative logical-time window and independent hard producer/outcome quotas, advertised at startup; shorter configured research windows are explicitly reduced contracts. Q0 remains independent. This is not the full B slice: B-04 provides actor-owned committed-history replay and fail-closed log resumption; additive READ_FETCH/READ_RECORD/READ_END frames expose it through the Rust SDK and TCP with exact encoded bounds and explicit completion. This is read-only inspection, not reliable group processing. Durable consumer ACK/cursor, retention and Q2 remain separate work. The `fault-injection` build is for disposable isolated tests only; the default build has no active crash barriers and rejects test-control environment settings.
