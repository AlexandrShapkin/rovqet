# 03 — Tokio runtime, tasks, ownership and cancellation

## Execution model

One async server process per node. The Tokio runtime handles network readiness, lightweight sessions, timers, and bounded actor commands. Disk fsync, checksum scan, compression and recovery work run in dedicated blocking workers. No per-connection OS thread. Reference MVP uses one task per connection plus sharded subscriber registries; test memory slope before replacing the task model.

### Task tree

\`Supervisor\` starts (1) listener accept loops, (2) session tasks, (3) router and subscription shards, (4) Q0 dispatch workers, (5) Q1 partition actors + WAL workers, (6) Q2 group drivers when slice C starts, (7) metadata owner/reconciler, (8) metrics/health. All child tasks get a shutdown token and are registered for bounded drain. Unexpected failure of durable writer marks affected partition unavailable, **not** success; supervisor may restart only after recovery.

## Communication and channel sizes

Default channels are \`tokio::sync::mpsc\` with both message-count and byte budgets (a channel slot does not bound payload size). \`oneshot\` returns outcome to request owner. Use \`Bytes\`/Arc for immutable shared payload; avoid copying per subscriber unless IO framing needs it. Distinguish backpressure \`Full\` from \`Closed\`.

Reference first-cut limits (NOT final SLO): per-session outstanding 32 frames, per-subscriber egress 64 messages / 256 KiB, per-topic Q0 queue 4096 messages / 8 MiB, per-partition WAL batching 1024 records / 4 MiB / 2 ms. Tune by capacity model. On a configured limit breach, reject or apply the exact Q0 overflow policy; never allocate over the reserve.

## Actor invariants

- A partition actor serializes all state transitions that assign offsets or change dedup/cursor. No shared Mutex on the global publication hot path.
- Shared metadata reads use immutable \`Arc<Snapshot>\` (e.g. ArcSwap). Control-plane updates swap the whole snapshot at a version boundary.
- Each session has one outbound writer, preventing frame interleaving. Prohibit concurrent raw socket writes from publishers/subscribers.
- A \`Reservation\` is released on cancellation/drop/commit transfer, not leaked when a future is aborted.
- An ACK waiter's cancellation never rolls back a possibly committed write. The durable operation must continue to a known barrier or be recovered and exposed as OUTCOME_UNKNOWN.
- A timeout is a client observation, not automatic cancellation of a submitted durable commit.

## Publish path (Q0)

1. TCP reader parses one bounded frame, validates schema/test identity, acquires session inflight credit.
2. Server policy resolves tenant/topic/partition/priority; caller-requested priority cannot elevate policy.
3. Admission reserves topic+tenant+node budget, including estimated fan-out.
4. Router dispatches \`PublishQ0\` to partition worker.
5. Worker chooses recipients from snapshot and enqueues \`Bytes\` references in separately bounded subscriber channels.
6. Producer receives \`ADMITTED\` after step 4; delivery/drop accounting happens independently after step 5.
7. Each session writer drains only its own outbound queue, with a bounded send deadline.

If a destination is slow, its queue uses configured DROP_NEWEST/DROP_OLDEST/COALESCE_LATEST/REJECT; no shared mutex or send await may block unrelated subscribers.

## Publish path (Q1/Q2)

1–3 as above, but durable class must match partition.
4. Route to current owner/leader; attach epoch and idempotency token.
5. Durability actor verifies producer epoch/sequence; obtains offset only when append is accepted.
6. Q1 append and durable fsync barrier; Q2 Raft propose and library durable-majority commit barrier.
7. Outcome and dedup high-water are recoverable with record. Only now send \`COMMITTED_LOCAL/QUORUM\`.
8. Durable consumer delivery scheduler sees only committed records.

Use a pinned request ID to correlate result; on network disconnect after step 5, the persistent operation may finish. Retry safely with same scoped producer token.

## Scheduling and fairness

Two resource planes: Q0 volatile and durable; four priority classes within each. Reference MVP can use independent bounded queues and service reserves, then upgrade to weighted deficit round robin (WDRR). Periodically service all admitted lanes without starvation; do not let low-priority disk flushes monopolize worker. Metrics: queue wait p99, scheduler starvation count, per-class admitted/dropped, block durations, CPU budget.

High-priority CONTROL has reserved queue and CPU tokens; it cannot bypass durable barrier or tenant policy. Background compaction/yield has its own budget below foreground commit.

## Connection density

Each connection has compact session state: transport handle, authenticated test principal, decoder buffer, heartbeat, subscriptions, inflight map, outbound queue references. No unbounded per-session read accumulation or one timer task per heartbeat; use a shared tick/timer wheel where measured useful. Track process heap, allocator-retained memory, kernel socket buffers and FDs separately. Admission stops before configured resource reserve is consumed.

## Cancellation, shutdown and draining

- \`NEW→HANDSHAKE→ACTIVE→DRAINING→CLOSED\`; a session closes on parse failure, idle deadline or explicit drain.
- On DRAINING stop new ingress; complete known in-flight durable commits within bounded grace; for unresolved operations return OUTCOME_UNKNOWN (if response possible).
- On SIGTERM: stop accept, advertise draining, quiesce Q0, flush active Q1/Q2 according to durability barrier, persist snapshots only if useful, close sockets; process kill correctness must not depend on graceful path.
- A panicking worker must fail its attached requests and trigger affected component health loss; no speculative ACK from actor restart.

## Testing

Deterministic scheduler tests: concurrent subscribe/unsubscribe, Q0 slow consumer, 10k sessions smoke, overload rejection, cancellation before/after append, out-of-order ACK and fencing. Collect allocation-per-connection and queue histogram. Failure tests deliberately interrupt tasks at all outcome boundaries.
