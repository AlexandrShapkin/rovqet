# Development roadmap — architecture first, fast concept-validation MVP

This is the **execution plan** for autonomous coding agents. The [requirements](../requirements/BROKER_REQUIREMENTS_v1.1.1.en.md) define the eventual product; the [architecture](../architecture/README.md) defines implementation contracts; the [MVP envelope](../architecture/16-mvp-scope.md) records deferred production features. Tasks are in [work-packages.md](work-packages.md) and machine-readable [tasks.json](tasks.json); contract coverage is tracked in [architecture-coverage.md](architecture-coverage.md).

## Delivery order

~~~text
D0: complete formal architecture, diagrams, backlog and agent rules
      |
      v
R0: minimal test harness + M0 spikes and baselines, decisions at replaceable seams
      |
      v
A: wire + SDK + single-node Q0 pub/sub + fan-out + budgets  (first visible demo)
      |
      v
B: Q1 local WAL + recovery + replay + dedup + group ACK  (first honest durable demo)
      |
      v
C: metadata ownership + three-node cluster + Q2 quorum  (distributed PoC)
      |
      v
D: focused quantitative comparison + independent oracle (concept validated or falsified)
      |
      v
P: productionization, security hardening, full normative M5–M7 qualification
~~~

**D0 is performed before feature code.** R0 may be short: it is for making high-cost technology choices before implementing an incompatible backend, not a reason to delay the Q0 demo for weeks. Run narrow spikes, default to working TCP and append-only WAL, revisit later only on evidence.

## Relationship to normative M0–M7

| Research plan | Normative mapping | What is purposely different |
|---|---|---|
| D0 | added architecture preparation | no change to normative guarantees |
| R0 | M0 | focused spikes and baseline fixture ahead of feature code |
| A | M1 subset | loopback prototype skips production TLS/auth; retains bounded Q0 behavior |
| B | M2 subset | true local durable guarantees, recovery, dedup; limited groups/UI |
| C | M3+M4 subset | fixed three-node topology, true Q2 quorum, basic fencing |
| D | exploratory M5 subset | honest PoC comparison, not full H1/VAL-01 certification |
| P | remaining M1–M7 obligations | production security, full stress/fault suite and acceptance |

Prototype gates are not production gate substitutions. For final M7 require original full traceability, security and performance evidence.

## Primary success criteria

1. Q0 can serve many simultaneous socket connections and perform 1:N fan-out without unbounded RAM or one slow subscriber blocking everyone.
2. Q1 retains all acknowledged publications across process kill/restart with intact disk and retries don't create duplicate committed writes within its declared dedup window.
3. Q2 retains acknowledged publications after one independent node failure and refuses new quorum ACK without quorum.
4. The system measurably exhibits the selective-reliability separation (Q0 path does not await Q1 fsync/Q2 Raft).
5. Independent oracle and NATS comparison disclose actual admitted/committed/delivered rate, p99, CPU/RAM and semantic differences.
6. No claim of production SLO, security or operational readiness is made until full normative gate passes.

## Autonomy and merging

No mandatory issues, PRs, external approval or fixed review chain. Agents work dependency-ordered \`READY\` tasks, may commit directly to main after focused tests, and update task state with concrete evidence. For parallel work, use disjoint task-owned paths and short-lived feature branches only when they reduce conflicts. Never use force push to overwrite another agent. No build/release artifacts on routine documentation commits or routine main merges.

## Current status

D0 documentation is being established in-repository. Broker implementation and measured M0/MVP benchmarks are **NOT STARTED**. Tasks remain TODO until actual commands succeed. A generated plan is not a test result.

## Work package ownership rule

A work package describes input contracts, implementation steps, files, tests, and done conditions. If a narrow unforeseen design gap blocks it, update the precise architecture chapter and test and then implement; do not reopen the project-wide design from scratch.

Read [work-packages](work-packages.md), [MVP smoke tests](mvp-smoke-and-benchmarks.md), and [agent execution protocol](agent-task-protocol.md).
