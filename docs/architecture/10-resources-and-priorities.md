# 10 — Global budgets, admission, priorities and backpressure

## Principle

A bound is meaningful only when it aggregates across every connection and destination. Avoid a per-session buffer that multiplies by 50,000 clients without counting against global RAM. Prefer refusing new work over OOM, false ACK or indefinite queue growth.

## Hierarchical budgets

\`Global → node → tenant → topic → partition/lane → session/subscriber\`. Account separate ingress and physical egress budgets. Resource dimensions: messages, bytes, inflight operations, connections, subscriptions, CPU dispatch tokens, per-priority credits, disk WAL backlog, durable storage, network bytes/s and Raft/control reserves.

Admission \`reserve(ResourceCost)\` atomically subtracts all relevant budgets or fails without partially allocating. Use a reference-counted/RAII reservation so cancellation frees resources once; handoff to the next bounded queue transfers cost ownership. Never hold unbounded queue elements behind a short mutex.

## Capacity equations and enforcement

~~~text
RAM >= N_connections * (process_conn + kernel_socket + TLS_state)
      + subscription_index + sum(subscriber_outbound_reserved)
      + sum(inflight_payload_and_metadata) + wal_buffers + raft_buffers
      + runtime_allocator_margin + operating_reserve

NIC_egress >= sum(publish_rate(topic) * actual_recipients(topic) *
                  on_wire_bytes_per_delivery) + cluster_replication + control

Disk_bytes/s >= Q1_append + Q2_replica_append + durable_ACK_updates +
                snapshot/compaction/headroom
~~~

Keep observed process RSS/PSS and kernel socket memory separate. Coefficients come from M0/M1 experiments; initial numeric budgets are prototype configuration, never universal product guarantees.

## Admission decision

1. Validate request policy/security/owner/durable class before expensive work.
2. Verify TTL, frame size and per-principal/tenant rates.
3. Compute bounded estimated downstream cost (for Q0 include recipients; for durable include WAL/dedup overhead, not speculative infinite replay).
4. Reserve message + byte + physical egress budget; if failure return RESOURCE_EXHAUSTED/EGRESS_QUOTA/RETRY_AFTER before durable admission.
5. Transfer reservation to bounded worker. When commit finishes, release ingress reservation; persistent log occupancy is separately charged to retention budget.
6. For already-committed Q1/Q2, never shed data merely to free subscriber outbound buffers. Instead pause delivery, reduce credit or disconnect slow consumer while preserving replay.

## Four priority classes

CONTROL, INTERACTIVE, TELEMETRY, BULK are resolved by server policy. Within each lane use weighted deficit round robin after reference validation. MVP simpler weighted round-robin is allowed but must reserve CONTROL service and avoid starvation under supported capacity. Durability barriers are the same regardless of priority. CPU dispatcher, disk writer and socket egress need **separate** quotas: prioritizing only one will not isolate p99 on shared hardware.

Example weights for early experiments (not requirements): CONTROL 8, INTERACTIVE 4, TELEMETRY 2, BULK 1. Record actual serviced work and queue wait histogram; adjust based on measured fairness and p99, not subjective importance.

## Overload state machine

\`NORMAL → PRESSURE → SHED → DRAIN → UNAVAILABLE\`, with recovery arrows using lower hysteresis thresholds and minimum dwell. State inputs: free RAM/reserve, queue age, p99 wait, disk fsync p99, CPU saturation, NIC egress, missing Q2 quorum. State machine per affected resource domain; disk-failure of one Q1 partition should not shut down unrelated Q0 if the node is healthy.

- NORMAL: all admitted classes within capacity.
- PRESSURE: slow admissions, preferentially shed low-class Q0 as policy permits, restrict new sessions.
- SHED: reject new requests before queues explode; emit reason/counters.
- DRAIN: planned shutdown or migration, stop new work and settle in-flight results.
- UNAVAILABLE: no safe processing for affected domain, never false ACK.

## Slow consumer and fan-out budgets

Each subscriber has max outbound bytes and messages, plus writer socket deadline. Remote destination node receives one payload copy then fan-outs locally, but traffic billed by actual local recipients. Q0 can drop per topic/subscriber policy; durable group fetch respects credits and may pause without modifying cursor. Counters: offered/admitted/delivered/rejected/dropped, bytes in/out, fan-out multiplier and queue occupancy per bounded label set.

## Reserve budgets

Separate protected credits for ACK/PUB_ACK, control heartbeat, Raft election/replication and CONTROL traffic. A client that fills BULK buffers cannot prevent a consumer ACK from being returned or a Q2 majority from functioning, provided underlying hardware has remaining capacity.

## Calibration and tests

Sweep 30/50/70/80/90/100/110% sustainable capacity, and add 10k client handshake burst; measure process+kernel RAM, CPU, network, queue growth, successful delivered rate and p99. Include 1→N and N→N, slow consumers, stalled fsync, exporter outage, Raft follower lag and reconnect storms. If model underestimates or admissions cause OOM, fix bound/model rather than introducing a procedural acceptance waiver.

Only formal product tests prove PERF-D1/D2/etc; a demo cannot.

## Implemented Q0 prototype profile

Reservations lock all affected domains in stable order before mutation; RAII
releases each charge once on failure or cancellation. Ingress accounts global,
tenant, topic, partition and publishing session scopes. Delivery queues retain
independent global/tenant/topic outstanding-byte/message reservations through
queued and in-progress writes, in addition to per-session queue limits. The
single-node global domain also represents the node. Protected control queues
have separate capacity and are not blocked by delivery credits.

Q0 physical egress uses global/tenant/topic two-dimensional token buckets
(bytes and actual recipient copies, one second of burst). Separate admission
and writer buckets prevent a delayed zero-credit queue from bypassing the rate
at delivery. Failed/partial socket write attempts consume their reserved rate
cost conservatively; canceled unsent reservations roll back. An impossible or
exhausted admission estimate returns EGRESS_QUOTA, not ADMITTED. The defaults
in ResourceConfig are experimental bounds awaiting R0-02/A-07 calibration.

The volatile overload signal is the maximum of ingress byte and message
occupancy: NORMAL enters PRESSURE at 70%, any active state enters SHED at 90%.
SHED recovers to PRESSURE at <=65%, PRESSURE to NORMAL at <=50%; each recovery
requires the configured minimum dwell (default 100 ms). DRAIN/UNAVAILABLE are
explicit terminal processing states until recovery is requested; recovery
starts at SHED. State and transition counts are exposed. Storage, CPU and p99
signals are not claimed for this Q0-only implementation. Saturation does not
alter the Q0 acknowledgment boundary or introduce a durable dependency.
