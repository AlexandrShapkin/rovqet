# 05 — Q0 volatile routing, dispatch and fan-out

**QoS contract:** a successful Q0 \`ADMITTED\` means the message entered bounded broker work, not that any subscriber received it. Crash, TTL expiry and configured overflow may lose it. No Q0 WAL, durable cursor or implicit Q1 upgrade.

## Topic, partition, subscription representation

\`TopicConfig\`: tenant/topic ID, partition count, allowed QoS, default priority, TTL bounds, Q0 overflow policy, max subscriptions and egress quota. \`PartitionKey\` hashes deterministically with pinned seed/version. MVP supports exact topic matching, then wildcard \`*\` for one path segment and \`**\` for trailing subtree, with explicit wildcard expansion limit. Unicode production canonicalization is kept behind a name-parser interface; MVP test names are ASCII, never an unbounded regex.

\`Subscription\`: opaque ID, session ID, topic filter, effective priority, bounded outgoing queue handle and credit count. Register/unregister atomically in topic-sharded \`SubscriptionIndex\`. Reader snapshots avoid global write locks. Disconnect removes ephemeral subscriptions and queued Q0 deliveries; reconnect recreates them through SDK.

## Data path

1. Session decoded PUBLISH validated against topic policy and computed partition.
2. Budget manager checks message bytes + recipient estimate; Q0 ingress rejected if no bounded slot (RESOURCE_EXHAUSTED) or subject to explicit configured drop strategy.
3. Current partition worker places \`Q0Envelope\` with server receive timestamp and monotonic expiry into bounded ingress queue.
4. Return \`PUB_ACK(ADMITTED)\` if requested; do not wait for fan-out.
5. Dispatcher fetches bounded recipient batch from index (without per-message scan of all sessions).
6. For each local subscriber: enqueue shared immutable payload reference into per-subscriber bytes/messages bounded queue. Remote cluster Q0 path sends one logical copy to each interested destination node.
7. Session writer applies credits and writes frame, records actual delivered count only after successful socket write boundary (SDK receipt measured by separate generator).
8. Expired/overloaded per-subscriber copy is counted and dropped according to the chosen policy; other subscribers proceed.

## Overflow choices

| Policy | Behavior | Allowed |
|---|---|---|
| DROP_NEWEST | new queued item discarded, increment dropped counter | Q0 only |
| DROP_OLDEST | evict oldest unconsumed item, enqueue latest | Q0 only |
| COALESCE_LATEST | replace older queued event for the same key; retain latest timestamp | Q0 only, enabled topic |
| REJECT_PUBLISH | reject before Q0 admission when capacity cannot be reserved | Q0 and before-commit durable overload |

Never apply Q0 policies to Q1/Q2 already committed records. A subscribed consumer that cannot drain is disconnected or has its own queue shed; it does not block a topic worker.

## Priority behavior

The server resolves CONTROL/INTERACTIVE/TELEMETRY/BULK from policy rather than trusting client priority. Each Q0 topic shard has separate priority queues and token/CPU budget; reserve capacity for CONTROL and control frames. Reference first-cut scheduler: weighted round-robin with minimum service slots for admitted lower priorities; upgrade to WDRR in M5 if tail measurements demonstrate need. TTL is checked before enqueue and immediately before socket delivery.

## Capacity and complexity

Matching exact-topic subscribers is O(1) lookup plus O(recipients) physical fan-out; neither copying nor NIC egress can be sublinear in actual deliveries. Wildcards use trie or indexed materialized matches, bounded by count and expansion quotas. Shared \`Bytes\` reference reduces user-space copy but does not remove physical egress bytes. Count both offered and delivered messages, subscriber queue depth and bytes.

## Q0 tests

- Single publisher/subscriber; 1→1000 fan-out; many publishers to one subscriber; wildcard boundaries; unsubscribe races.
- Disconnect while message queued; stale subscription IDs not reused as active.
- Slow subscriber cannot delay another; max outbound bytes and queue messages enforced.
- TTL expired before admission vs before delivery; coalesce correct last-value semantics.
- Under overloaded offered rate, report rejects/drops and actual deliveries; no fabricated improved p99.
- Mixed Q0 + Q1/Q2 load: fsync/quorum slowdown cannot block the Q0 worker except for measured shared hardware resource contention.

**Out of MVP:** guaranteed delivery to all subscribers, wildcard regex language, per-client persistent Q0 cursors, bridging to MQTT/Kafka, global ordering.
