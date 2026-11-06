# Architecture-to-requirements coverage and prototype deviations

Normative source: [v1.1.1 requirements](../requirements/BROKER_REQUIREMENTS_v1.1.1.en.md). **Architecture coverage is descriptive; it is not proof of implemented compliance.** Chapter numbers refer to [architecture index](../architecture/README.md).

| Contract group | Architecture | Research MVP | Full product follow-up |
|---|---|---|---|
| FR-CON connections/heartbeat | 01–04, 10, P | TCP sessions, bounded inflight and idle/heartbeat | full TLS/QUIC and production connection limits |
| FR-TOP namespaces/subscriptions | 02, 05, 09, 10 | fixed topics, exact filter then bounded wildcard | full normalization/tenant ACL/admin mutations |
| FR-QOS Q0/Q1/Q2 | 00, 04–08, 11, 13, P | all three core ACK semantics by slice C | full contractual dedup window, migration and complete policy |
| FR-TIM TTL/queues | 03, 05, 10 | Q0 TTL and bounded queues | all cross-host deadline/retention scenarios |
| FR-PRI priorities/overload | 03, 05, 10 | simple weighted queues + reserves | full WDRR/EDF, calibrated resource model |
| FR-STO WAL/replay/retention | 06, 07, 11 | Q1 WAL, Q2 replicated WAL, bounded replay | backup, repair, robust retention/compaction |
| FR-GRP consumer groups | 08, 13 | one group, generation, durable ACK/cursor, retry | rebalancing/DLQ at production scale |
| FR-CLU topology/consensus | 07, 09, R | fixed three-node metadata and Q2 quorum | joint config, dynamic placement, full degraded matrix |
| FR-API SDK/admin | 04, 12, P | Rust SDK, test CLI | OpenAPI/admin HTTPS, second SDK |
| Wire §5 | 04, P, R | central binary v1, log-incarnation fields, test vectors | compatibility and protocol hardening |
| §6 runtime/architecture | 01–10, diagrams | Tokio actors and bounded seams | tuning and modular replacements |
| SEC-001…009 | 12 | isolated fake identities and loopback | **deferred** production TLS/mTLS, ACL and threat testing |
| PERF-* | 10, 14, roadmap D | exploratory p99/client density/CPU/RAM | H1/five-run numerical qualification |
| REL-* | 06–08, 11, 14 | focused Q1/Q2 oracle and one-node failover | all REL-01…07 and physical fault assumptions |
| OBS-* | 10, 12, 14 | bounded counters/raw histograms | Prometheus/OTLP, dashboard/runbooks |
| TEST-001…017 | 11, 14, roadmap | focused slice tests | complete normative suite |
| INV-01…16 | 00, 07–11, 13 | durability, fencing, cursor and bounded memory core | all invariant proofs and reports |
| ADR-001…006 | 15 | default seams, limited experiments | full M0 evidence and product ADRs |

## Explicitly material gaps until implemented

1. Rust workspace and v1 frame codec now exist and compile; there is still **no live broker listener**, no Q0 dispatch, Q1 WAL or Q2 Raft implementation.
2. Canonical wire IDs in appendix P have not yet been encoded/decoded by a real fixture; M1 must verify and freeze.
3. Structurizr/PlantUML diagrams are text sources; CI checks document structure but does not run diagram renderers.
4. Raft adapter must be compiled against a pinned library version and tested with durable storage; the design does not itself prove a quorum.
5. Physical power-loss tests, live H1 hardware, actual NATS baselines, and product security qualification have not been performed.
6. No public listener or real users are authorized under the prototype profile.

## Non-negotiable before claiming complete concept-validation MVP

Correct Q0 ADMITTED semantics, Q1 local fsync and restart, Q2 independent durable quorum, no silent QoS downgrades or Q1/Q2 mixed durable partition, consumer cursor no-gap invariant, producer retry dedup, ownership fencing, bounded resources, independent oracle and actual comparative benchmark.
