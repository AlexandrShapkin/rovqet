# rovqet

Rovqet is an experimental, Rust/Tokio message broker focused on dense client connectivity, predictable tail latency, priority isolation and selective durability.

**Status: operational development-only Q0 TCP pub/sub and Rust SDK; durability and quantitative validation remain unfinished.** The repository is initialized with the normative v1.1.1 requirements, a deliberately reduced research-MVP delivery profile, detailed component contracts, implementation roadmap and instructions for autonomous AI coding agents. Do not interpret architectural designs or proposed interfaces as implemented features.

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

Run the broker and SDK examples in separate terminals:

```sh
cargo run -p rovqet-server -- --listen 127.0.0.1:7443
cargo run -p rovqet-sdk --example q0 -- sub demo
cargo run -p rovqet-sdk --example q0 -- pub demo hello
```

The fixed development topics are `demo` and `telemetry` (four partitions each).
The listener refuses non-loopback addresses; AUTH method 0 uses the static `dev`
identity. Q1/Q2 and durable consumer operations are rejected. `Admitted` means
bounded volatile admission, with no promise of durability or subscriber receipt.

Reproduce the independent-process smoke (one publisher, two subscribers):

```sh
cargo build -p rovqet-server -p rovqet-sdk --examples --bins --locked
python3 scripts/q0_smoke.py
cargo fmt --all -- --check
cargo check --workspace --locked
cargo test --workspace --locked
```

Current bounds: 1,024 connections including incomplete handshakes; one inbound
frame per session (1 MiB frame, 256 KiB payload); 64 outbound data slots and
256 KiB per session including the active writer; eight protected control slots
and 4 KiB per session. Q0 ingress has 1,024 slots (one reserved for CONTROL),
16 MiB global estimated cost, 8 MiB per topic and 128 slots/1 MiB per publishing
session. Admission cost includes estimated physical recipients and metadata.
These are configurable library prototype bounds, **not measured capacity or
production SLOs**. Kernel socket and allocator memory are additional.

Library topic policies implement all four Q0 choices: `DROP_NEWEST`,
`DROP_OLDEST`, `COALESCE_LATEST` (same topic/key/subscription), and `REJECT_PUBLISH`
(pre-admission reservation for every current recipient). Zero-credit queued data
remains bounded and evictable; TTL expiry and unsubscribe release reservations.
Shared encoded payloads are reused across recipients with per-subscription header
IDs written separately.

`ResourceConfig` enforces outstanding outbound message/byte budgets globally,
per tenant and per topic (default 16,384 messages; 64/32/16 MiB respectively).
Ingress also charges tenant and partition budgets. Q0 data egress byte/delivery
quotas apply to global/node, tenant and topic domains at admission and again at
socket delivery. Defaults are 256 MiB/s and 1,000,000 deliveries/s per domain,
with a one-second token-bucket burst; per-topic overrides are supported.
Protected control traffic has its separate bounded reserve. These prototype
settings await calibration in R0-02/A-07 and are not measured throughput claims.

The overload state is observable through `Broker::overload_state()`: occupancy
enters PRESSURE at 70% and SHED at 90%; recovery uses 65%/50% thresholds and a
100 ms minimum dwell. DRAIN rejects with `ServerDraining`; UNAVAILABLE rejects
with `ResourceExhausted`; explicit recovery passes through SHED and PRESSURE.
The state uses ingress bytes/messages in this volatile MVP; durable-resource and
production CPU/p99 signals are future work. No WAL is called by Q0.
Exact names only; wildcards and production authorization remain deferred.

The SDK serializes operations per client, bounds pending deliveries, sends
heartbeats while receiving, and explicitly reconnects with bounded exponential
backoff/jitter and subscription recreation. An unused client has no background
keepalive and may need reconnect after idle expiry. SUBSCRIBE negotiates finite
message/encoded-byte delivery credits (default 64 messages/256 KiB, maximum
128 messages/1 MiB). `subscribe_with_credit` permits an initial zero window;
`add_credit` replenishes it explicitly. The SDK normally returns each consumed
message's credit before the next receive; `auto_replenish=false` enables manual
control. Invalid, foreign, stale or excessive credit operations return typed
errors. Reconnect recreates each subscription once with its original window.
A lost publication response
is `OutcomeUnknown`; uncertain Q0 publications are never automatically replayed.
Transport counters distinguish successful MESSAGE writes/bytes, expiry, write
failure, newest/oldest drops, coalescing, closed subscriptions and cancellation.
Broker counters distinguish admission, enqueue, rejection, egress rejection,
overload transitions and ingress TTL/unsubscribe/shutdown drops. Outstanding
usage and active session/subscription counts are exposed for resource tests.
Successful socket writes do not prove SDK receipt. The independent load/oracle
and quantitative resource profile remain the next tasks (R0-02/A-07).
