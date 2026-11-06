# Rovqet implementation architecture

**Audience:** coding agents implementing a Rust message broker, and engineers verifying contracts. **Status:** reference architecture, not implemented software.

## Normative boundary

The [v1.1.1 specification](../requirements/BROKER_REQUIREMENTS_v1.1.1.en.md) is authoritative. These documents turn requirements into executable design choices. The [MVP profile](16-mvp-scope.md) deliberately defers selected production work without downgrading claimed broker data semantics.

## Architecture chapters

| File | Contract |
|---|---|
| [00 Authority and assumptions](00-authority-and-decisions.md) | precedence, frozen invariants, undecided options |
| [01 System and deployment](01-system-and-deployment.md) | C4 model, nodes, external actors, network paths |
| [02 Module API](02-module-contracts.md) | crates, types, traits, dependency constraints |
| [03 Execution and concurrency](03-runtime-and-concurrency.md) | Tokio, tasks, channels, ownership, backpressure |
| [04 Protocol and SDK](04-protocol-and-sdk.md) | frames, session behavior, errors, compatibility |
| [05 Q0](05-q0-data-plane.md) | volatile dispatch, subscriptions, fan-out |
| [06 Q1](06-q1-storage.md) | WAL layout, fsync, offsets, recovery and dedup |
| [07 Q2](07-q2-consensus.md) | Raft groups, durable prefix, elections and quorum |
| [08 Consumers](08-consumer-groups.md) | lease, ACK/cursor, DLQ, terminal states |
| [09 Cluster](09-cluster-control.md) | metadata, placement, ownership and fencing |
| [10 Resources](10-resources-and-priorities.md) | queue budgets, scheduler, failure of slow clients |
| [11 Failure scenarios](11-failure-matrix.md) | crash/partition/disk/retry boundaries |
| [12 Security & operations](12-security-and-ops.md) | production requirements versus isolated MVP |
| [13 State models](13-state-machines.md) | explicit transition invariants and diagrams |
| [14 Verification](14-verification.md) | reference oracle, fixture contracts and evidence |
| [15 ADR inventory](15-open-decisions.md) | chosen defaults and evidence-gated alternatives |
| [16 Research MVP](16-mvp-scope.md) | deliverables and deliberate omissions |
| [Protocol registry](protocol-registry.md) | fixed reference wire fields, codes and frame structures |
| [Inter-node RPC](inter-node-rpc.md) | routing/ownership/transfer messages and retry semantics |
| [Diagrams](diagrams/) | Structurizr DSL and PlantUML sources |

The [roadmap](../roadmap/README.md) identifies the exact implementation order. Refer to work-package IDs in commits and handoffs.

## Contract maturity

- **FROZEN:** QoS ACK semantics; Q1/Q2 partition separation; no false durable ACK; bounded state; consumer cursor; log incarnation; ownership fencing.
- **REFERENCE CHOICE:** crate layout, internal Rust traits and batching algorithm. Change only with a localized documented interface revision and matching tests.
- **EXPERIMENTAL:** transport default, KV engine, scheduler policy, Raft group sizing, connection-registry design. Document the selected MVP default and the evidence needed to change it.
- **DEFERRED:** full multi-tenant security, production TLS/mTLS rollout, hardened admin/control plane, 72h soak, packaging and production-level SLO certification.

An architecture document must identify scope, state owner, inputs/outputs, invariants, failure behavior, test, and migration/compatibility boundaries. C4 drawings are navigation aids, not a substitute for these contracts.
