# rovqet

Rovqet is an experimental, Rust/Tokio message broker focused on dense client connectivity, predictable tail latency, priority isolation and selective durability.

**Status: operational development-only Q0 pub/sub and opt-in Q1 local-durable TCP publishing with restart recovery. Replay, durable consumers, full dedup-window qualification and quantitative validation remain unfinished.** The repository is initialized with the normative v1.1.1 requirements, a deliberately reduced research-MVP delivery profile, detailed component contracts, implementation roadmap and instructions for autonomous AI coding agents. Do not interpret architectural designs or proposed interfaces as implemented features.

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
identity (principal 1, tenant 1). Q1 requires the explicit configuration below; Q2 and durable consumer operations are rejected. `Admitted` means
bounded volatile admission, with no promise of durability or subscriber receipt.

Reproduce the independent-process smoke (one publisher, two subscribers):

```sh
cargo build -p rovqet-server -p rovqet-sdk --examples --bins --locked
python3 scripts/q0_smoke.py
cargo fmt --all -- --check
cargo check --workspace --locked
cargo test --workspace --locked
```

## Experimental Q1 publisher

Create a disposable directory once and retain its path when restarting the broker:

```sh
Q1_DATA=$(mktemp -d /tmp/rovqet-q1.XXXXXX)
cargo run -p rovqet-server -- --listen 127.0.0.1:7443 \
  --q1-data-dir "$Q1_DATA" --q1-topic durable \
  --q1-tenant-id 1 --q1-topic-id 3 --q1-partition-id 0 \
  --q1-incarnation 42 --q1-owner-epoch 7 --q1-initialize-new-log true
```

In another terminal, publish with an explicit stable producer token:

```sh
cargo run -p rovqet-sdk --example q1 -- durable 11 0 0 100 0 42 7 hello 30000
# COMMITTED_LOCAL 42 0
```

Arguments are `topic producer_id producer_epoch sequence message_id partition_id incarnation owner_epoch payload [ttl_ms]`. `ROVQET_ADDR` overrides the default `127.0.0.1:7443`. The Rust API is `Client::publish_q1(topic, payload, Q1PublishOptions { ... })`; Q0's existing `publish` API is unchanged.

Stop with Ctrl-C, restart using the **same directory and identity**, omitting `--q1-initialize-new-log true`, then repeat the identical command: it returns the original offset. The next publication uses sequence 1 and a new message ID, for example `durable 11 0 1 101 0 42 7 next 30000`. Message offsets are 0, 2, 4, ... because each message has a paired producer-outcome record. The startup diagnostic advertises the dedup window (default 24 hours, configurable with `--q1-dedup-window-ms`). Downtime and uncertain clock intervals extend retention; expired historical tokens return OUTCOME_UNKNOWN and cannot reinsert records because sequence watermarks remain. Keep the original token, message ID and payload when retrying `OutcomeUnknown`; reconnecting never changes them automatically. No Q1-to-Q0 fallback occurs.

`COMMITTED_LOCAL` follows synchronization of the message, scoped producer outcome, manifest and directory. It survives a process crash with the same intact functioning storage; permanent disk loss and physical power-loss certification are outside this evidence. Never reuse an old incarnation after lost history. Startup resumes existing history by default and rejects a missing manifest without recreating it. `--q1-initialize-new-log true` explicitly initializes only an empty new directory with a fresh caller-allocated incarnation; it never resets existing storage. The server requires an existing directory, holds an exclusive advisory lock, and refuses corrupted or incompatible committed storage without resetting it. The configured route is Q1-only; unsupported partitions/classes are rejected. The development identity can publish only in tenant 1; Q1 topic IDs must exceed the fixed Q0 IDs 1 and 2. TTL must be 1–30000 ms. Q1 admission allows 64 queued/active commands and 4 MiB of retained payload capacity plus record/outcome overhead; producer watermarks and unexpired saved outcomes each have a configurable hard 100000-entry default limit, with no eviction of unexpired outcomes.

Storage startup and shutdown run on blocking workers; per-request results use asynchronous replies. Q0 never calls WAL or waits for its barriers. Existing `BROKER_METRICS` counters cover Q0; Q1 results are verified through the SDK and independent ACK journal. Shutdown stops ingress and drains accepted Q1 work; unresolved storage after the five-second final drain deadline causes a non-successful process exit and requires recovery. A lost reply during shutdown is an uncertain outcome.

Run the independent process smoke and deterministic fault suite:

```sh
cargo build -p rovqet-server -p rovqet-sdk --examples --bins --locked
python3 scripts/q1_smoke.py --check-hooks-disabled
cargo test -p rovqet-durable --features fault-injection --locked
cargo build -p rovqet-server --features fault-injection --locked
python3 scripts/test_q1_faults.py
```

Fault controls exist only in an explicit `fault-injection` build and use disposable marker directories. Default builds reject their environment variables. Tests pause before append, after append/before sync and after sync/before reply; the parent records received ACKs in a separate append-only fsynced journal. SIGKILL tests do not simulate physical power loss.

## Q0 resource bounds

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
Successful socket writes do not prove SDK receipt. The independent load/oracle (R0-02) is implemented. Build and run its reusable
profiles using [benchmark instructions](benchmarks/README.md). The
[exploratory Q0 evidence](benchmarks/evidence/q0-f9221b3/REPORT.md) records three
repetitions, real sessions through4,096, all raw histogram distributions and
explicit limits. A-07 remains IN_PROGRESS:10k sessions exceeded the safety
budget, and overload runs exposed generator pressure rather than establishing
maximum sustainable broker throughput.

Controlled density experiments may set `--max-connections` explicitly. The default
remains1,024; limits above it require `--connection-budget-bytes` covering the
conservative per-session envelope. This validates a configured bound rather than
reserving RAM or certifying capacity. The runner also checks available memory
headroom; loopback enforcement remains mandatory. Optional
`--metrics-interval-ms 250` emits bounded-frequency `BROKER_METRICS` JSON to stderr.

Standalone Q1 history inspection is available through the Rust SDK and external example:

```sh
cargo run -p rovqet-sdk --example read_q1 -- durable 1 3 0 42 0 64 65536 0
# READ_RECORD 42 0 100 68656c6c6f
# READ_END 42 2 0 2 173
```

Arguments are `topic tenant_id topic_id partition_id incarnation offset max_messages
max_encoded_bytes wait_ms`. `ROVQET_ADDR` selects the loopback endpoint. The typed Rust API
is `Client::read_q1(topic, ReadOptions { partition, from, max_messages, max_bytes, wait })`.
It returns `ReadBatch` with original IDs/payloads, sparse incarnation-bearing offsets,
continuation, `has_more`, `end_of_current_history`, encoded byte count and stop status.
Stop statuses are EMPTY=1, END=2, COUNT_LIMIT=3, BYTE_LIMIT=4, WAIT_EXPIRED=5. End is the
current committed snapshot, so later publications may extend it. Maximum encoded bytes
includes every record frame and the terminal READ_END (78 bytes), not only payloads.
Hard ceilings are 1024 records, 4 MiB and 30 seconds wait; aggregate partition budgets
may reject a request below these ceilings. An oversized first record fails explicitly.

The SDK returns success only after validating READ_END. EOF, timeout, malformed frames or
cancellation cannot turn a partial multi-frame response into a completed batch; the
uncertain socket is closed. `ReadError` exposes typed authority, incarnation/history,
resource, storage, unsupported-peer and framing failures. The wire extension uses frames
23–25 and fields 49–50; existing FETCH/MESSAGE and golden vectors remain unchanged.
Read-only inspection creates no consumer leases, ACKs, cursors or at-least-once processing
promise.

Durable Q1 consumer groups are available through the Rust SDK and an external example:

```sh
cargo run -p rovqet-sdk --example consume_q1 -- durable 1 3 0 42 9 0 8 65536 0 8
# DELIVERY 42 0 100 1 1 1 30000 6576656e742d30
# GROUP_BATCH 9 1 0 0 1
# ACK_COMMITTED 42 0 1
```

Arguments are `topic tenant_id topic_id partition_id incarnation group_id generation
max_messages max_bytes wait_ms ack_count`. Generation 0 claims (or replaces) the group and
durably fences the previous consumer; the broker returns the new generation in every
delivery. Each `DELIVERY` line carries the complete token fields
`incarnation offset message_id generation lease_id attempt ack_deadline_ms payload`;
`Client::ack_q1_group` returns ACK_COMMITTED only after the broker synced the
`CONSUMER_ACK` record. Add an eleventh argument to acknowledge only that offset.

Out-of-order ACKs are retained as a bounded sparse set and the cursor advances only over the
contiguous acknowledged prefix; restart redelivers unacknowledged records instead of
skipping them. Duplicate ACKs return the saved committed result without another durable
mutation. Expired leases and stale generations are rejected explicitly. FETCH batches end
with the additive FETCH_END (frame 26) frame, so a connection lost mid-batch is never a
completed SDK result. See [the consumer contract](docs/architecture/08-consumer-groups.md).

```sh
python3 scripts/test_q1_replay.py
python3 scripts/test_q1_consumer.py   # needs the fault-injection broker build
python3 scripts/test_q1_durability.py # B-07 oracle campaign, fault-injection build
python3 scripts/test_q1_oracle.py     # oracle self-corruption fixtures
```

The durability campaign journals externally acknowledged publications and consumer ACKs in a
parent-owned, fsynced file, kills the broker at deterministic barriers, restarts on the same
storage and reconciles exact identities, sparse cursor state, generation fencing and WAL
rotation. Raw journals, per-run verdicts and measurements are preserved under
`benchmarks/evidence/q1-durability-*/`; verdicts are PASS, FAIL, INVALID or NOT_RUN, and a
truncated or incomplete journal can never pass.

The Q1 restart smoke also reads all independently journaled ACKs through an external SDK
process and verifies IDs, offsets and payload hashes after clean restart and SIGKILL.
See [the replay contract](docs/architecture/06-q1-storage.md).
