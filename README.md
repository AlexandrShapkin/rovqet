# rovqet

Rovqet is an experimental, Rust/Tokio message broker focused on dense client connectivity, predictable tail latency, priority isolation and selective durability.

**Status: architecture-first, pre-implementation.** The repository is initialized with the normative v1.1.1 requirements, a deliberately reduced research-MVP delivery profile, detailed component contracts, implementation roadmap and instructions for autonomous AI coding agents. Do not interpret architectural designs or proposed interfaces as implemented features.

## Read in this order

1. [Normative requirements](docs/requirements/BROKER_REQUIREMENTS_v1.1.1.en.md): authoritative product and failure semantics.
2. [Architecture index](docs/architecture/README.md): concrete implementation blueprint, runtime models and behavior.
3. [MVP envelope](docs/architecture/16-mvp-scope.md): what the research prototype includes, defers and must never falsely claim.
4. [Delivery roadmap](docs/roadmap/README.md): dependency-ordered work packages, milestones and acceptance proofs.
5. [AGENTS.md](AGENTS.md): operating instructions for Codex and other coding agents.
6. [Change register](docs/requirements/CHANGELOG_1.0_to_1.1.1.en.md): history, rationale and rejected options.

## Operating model

Architecture is written before feature implementation. Engineering tasks are autonomous and small; agent may commit directly to main (or use short-lived branches when needed) without requiring PRs, approvals, governance meetings or release bureaucracy. Focused checks and behavioral contracts replace status theater. Extensive security hardening, production operations and full SLO qualification are deferred from the **isolated research MVP only**, not removed from product requirements.

**Do not expose the insecure prototype to public or untrusted networks.** A correct durability ACK, no silent Q2 downgrade, a bounded resource model and honest results are required even for a prototype: without them the PoC cannot test the stated concept.

## MVP definition

- **Slice A (Q0):** wire protocol, single-node broker, Rust SDK, topic/partition routing, live pub/sub, bounded fan-out, simple priority and measurable connection overhead.
- **Slice B (Q1):** local WAL + fsync, replay, dedup, durable consumer ACK/cursor and restart recovery; no Raft dependency.
- **Slice C (Q2):** minimal three-node metadata/ownership, replicated Raft durable shards, quorum ACK, leader takeover, basic failover oracle.
- **Slice D (validation):** repeatable baseline against suitable NATS modes; latency, client density, fan-out and Q0/Q1/Q2 failure semantics.

A+B gives a *local research prototype*; A+B+C+D gives a *concept-validation MVP*. Neither means production-ready. The full specification retains M0–M7; the reduced slices are a development sequence, not a rewrite of normative P0/P1/P2.

## Local commands

- \`python3 scripts/check_docs.py\`: validate repository documentation references and task identifiers.
- \`bash scripts/preflight.sh\`: run only available fast, relevant local checks.
- After implementation starts: \`cargo fmt --check\`, \`cargo check --workspace\`, and focused \`cargo test\` when applicable.

License, crate APIs and build scripts will be added in a dedicated implementation task; no empty pretense of compilable broker code.
