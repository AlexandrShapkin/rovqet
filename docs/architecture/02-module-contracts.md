# 02 — Rust workspace, type system and component contracts

**Status:** architecture API blueprint for implementation. Types and method names are reference choices; externally observable semantics are binding. Keep first cut minimal and compile incrementally.

## Crate and responsibility map

| Crate | Owns | Allowed dependencies | Forbidden coupling |
|---|---|---|---|
| \`rovqet-protocol\` | wire frame, bounded decoder, errors, identifiers, registry | bytes, small codec utilities | Tokio sockets, disk, business routing |
| \`rovqet-core\` | shared config types, QoS, policies, messages and outcomes | protocol | disk/network effects |
| \`rovqet-transport\` | TCP session lifecycle; QUIC adapter seam | protocol, core, Tokio | durable commit decisions |
| \`rovqet-router\` | topics/partitions, subscription index, owner cache | core | concrete WAL/consensus implementation |
| \`rovqet-scheduler\` | bounded budgets, admission, queues, fair dispatch | core | socket or file ownership |
| \`rovqet-q0\` | volatile publish/fan-out, TTL, overflow | core, router, scheduler | WAL and quorum dependency |
| \`rovqet-wal\` | segment writer, checksums, fsync/rotation and recovery | core, std/fs | consumer group or TLS details |
| \`rovqet-durable\` | Q1/Q2 partition state, producer dedup, durable consumer cursor | core, wal, scheduler | HTTP admin or UI |
| \`rovqet-consensus\` | proven Raft library integration, storage adapter, Q2 log groups | core, wal, durable model | client wire parser |
| \`rovqet-control\` | metadata snapshots, ownership epochs, placement/reconciler | core, consensus | per-message metadata RPC |
| \`rovqet-server\` | composition root, task supervision, config, lifecycle | all | hidden business logic bypassing component APIs |
| \`rovqet-sdk\` | client connection, producer retry, subscriber and group SDK | protocol, transport client | server state or disk |
| \`rovqet-testkit\` | external oracle, test peers, fault injection, generator | sdk, test tools | production hot path |

Start by implementing \`protocol/core/transport/router/scheduler/q0/server/sdk\`, then WAL/durable, then consensus/control. Do not create empty crates solely to match this table if they slow vertical integration; preserve logical module boundaries and split later without changing contracts.

## Canonical identifiers and values

Use newtypes; avoid passing tenant, topic, node, group and shard as untyped strings interchangeably. Intern or compactly map names after handshake. Suggested shape:

~~~rust
struct TenantId(u64); struct TopicId(u64); struct PartitionId(u32);
struct ShardId(u64); struct NodeId(u64); struct GroupId(u64);
struct ProducerId(u128); struct MessageId(u128);
struct OwnershipEpoch(u64); struct ProducerEpoch(u64);
struct LogIncarnation(u128);
struct PartitionRef { tenant: TenantId, topic: TopicId, partition: PartitionId }
struct DurableOffset { incarnation: LogIncarnation, sequence: u64 }
enum QoS { Q0, Q1, Q2 }
enum Priority { Control, Interactive, Telemetry, Bulk }
enum CommitOutcome { Admitted, CommittedLocal(DurableOffset), CommittedQuorum(DurableOffset) }
struct PublishRequest {
  partition: PartitionRef, qos: QoS, priority: Priority,
  payload: bytes::Bytes, message_id: MessageId,
  producer: Option<ProducerToken>, ttl_ms: u32, request_id: u64
}
struct ProducerToken { producer_id: ProducerId, epoch: ProducerEpoch, sequence: u64 }
~~~

The \`ProducerToken\` is scoped by principal+partition+lane; never deduplicate on \`message_id\` alone. \`DurableOffset\` without incarnation is unsafe. \`CommitOutcome::Admitted\` is not durability.

## Service boundaries

~~~rust
// Semantic pseudocode; use Result<T, BrokerError> and explicit async cancellation.
trait PartitionRouter {
  fn locate(&self, partition: PartitionRef) -> Result<Route, BrokerError>;
  fn current_policy(&self, partition: PartitionRef) -> Result<PolicySnapshot, BrokerError>;
}
trait Admission {
  fn reserve(&self, req: &PublishRequest, recipients: usize) -> Result<Reservation, BrokerError>;
}
trait SubscriptionIndex {
  fn subscribe(&mut self, who: SubscriberId, filter: Filter) -> Result<SubscriptionId, BrokerError>;
  fn unsubscribe(&mut self, id: SubscriptionId) -> Result<(), BrokerError>;
  fn matches(&self, topic: TopicId) -> BoundedRecipientBatch;
}
trait Q0Dispatch {
  async fn publish(&self, request: PublishRequest, reservation: Reservation) -> Result<Admitted, BrokerError>;
}
trait DurablePartition {
  async fn publish(&self, request: PublishRequest, reservation: Reservation) -> Result<CommitOutcome, BrokerError>;
  async fn fetch(&self, from: DurableOffset, limit: FetchLimits) -> Result<FetchBatch, BrokerError>;
}
trait ConsumerGroup {
  async fn lease(&self, group: GroupId, max: u32) -> Result<DeliveryBatch, BrokerError>;
  async fn ack(&self, token: DeliveryToken) -> Result<AckOutcome, BrokerError>;
  async fn nack(&self, token: DeliveryToken, reason: NackReason) -> Result<(), BrokerError>;
}
trait OwnershipAuthority {
  fn validate(&self, partition: PartitionRef, epoch: OwnershipEpoch, node: NodeId) -> Result<(), BrokerError>;
}
~~~

The code fragments define semantic interfaces; concrete Rust trait async dispatch (generics, boxed futures or actor handles) may be selected by implementation. Avoid \`dyn\`-heavy calls on hot path unless profiling justifies them.

## Broker error taxonomy

\`BrokerError\` is a typed enum mapping exactly to wire error catalog (§5.1.1). Include \`retryable\`, \`retry_after\`, \`current_epoch\`, \`request_id\` and \`commit_outcome_unknown\` where applicable. Reject before admission for QoS mismatch, expired TTL, missing authority, missing Q2 quorum or resource exhaustion. Never turn a queue-disconnect into definite not-committed when durable IO may have succeeded.

## Ownership and mutability matrix

| State | Sole mutating owner | Persistence |
|---|---|---|
| Client session, requests-in-flight | session task | transient |
| Topic/ACL snapshot | control-plane publisher; data-plane immutable view | consensus in cluster, static file in early MVP |
| Subscription index | topic-shard index actor, reader snapshots | Q0 transient |
| Admission budgets | quota manager/shard with RAII reservations | runtime; config persisted |
| Q0 dispatch queue | destination/topic worker | transient |
| Q1 log offset and producer high-water | Q1 partition actor | local WAL + consistent checkpoint |
| Q2 log offset and producer high-water | Q2 shard leader state machine | Raft committed prefix + snapshot |
| Group cursor, delivered leases and terminal states | durable group owner | Q1 local / Q2 quorum by class |
| Owner epoch | metadata consensus/lease authority | coordinated metadata |
| Raft term/index | Q2 Raft library | persistent Q2 group state |

No two tasks may mutate a partition's offset allocator or cursor directly. A consumer's \`ACK\` request must enter the current group owner and pass generation fencing before any persistent mutation.

## Cross-module operation contracts

- \`Session\` parses bounded frame, assigns authenticated principal, validates request ID and forwards a typed command through a **bounded** channel.
- \`Router\` resolves class and current owner snapshot; stale owner returns redirect/stale epoch; Q0 and durable never share a mutable queue.
- \`Admission\` atomically reserves global+tenant+topic+session budget and releases on failure; reservation remains held until downstream state has bounded ownership.
- \`Q0\` returns \`ADMITTED\` once bounded work is accepted, not upon arrival at a subscriber socket.
- \`Q1\` persistently appends record/dedup outcome, issues a local barrier and only then returns COMMITTED_LOCAL.
- \`Q2\` proposes to legitimate Raft leader, waits until durable majority commit is established by library, then returns COMMITTED_QUORUM.
- \`Consumer\` ACK verifies the token and stores closure in same-or-stronger domain before returning ACK_COMMITTED.

## Compatibility

Rust internal structs may evolve without wire changes. Wire registry identifiers freeze by M1. WAL versions freeze for Q1 at M2 and Q2 at M4; migrations must declare a method. Changes to identifier scope, Q1/Q2 partition class, ACK meaning or cursor closure require a new explicit architecture+fixture revision.

## R0-05 / B-01 concrete local storage seam

`rovqet-wal::Wal` is a concrete synchronous, exclusive owner of one Q1 partition/incarnation directory. It is not a generic storage-engine framework. `Config` fixes cluster/node identity, `PartitionRef`, incarnation, segment/record byte limits, pending record/byte limits, and segment/index-entry limits. B-02 must place this blocking owner on a dedicated worker or equivalent blocking execution context and preserve bounded admission. The Q0 crate does not depend on it.

The stable operations are `open(path, config)`, `enqueue(record)`, `cancel_pending()`, `flush()`, `append(record)`, `sync()`, `committed_position()`, `read(from, max_records, max_bytes)`, `pending_bytes()` and `generation()`. `Record` carries a typed kind, logical offset, owner epoch, producer ID/epoch/sequence, message ID, server timestamp and opaque payload. `sync()` returns the established optional `DurableOffset`; `read` accepts an incarnation-bearing offset and exposes only the committed prefix. See [06](06-q1-storage.md) for physical encoding and failure ordering.

`enqueue` establishes bounded in-memory ownership, charging retained payload capacity plus record overhead; `cancel_pending` removes only that memory queue. `append` and `flush` write into the operating-system page cache and never imply durable commitment. `sync` establishes a durable boundary only after segment and manifest publication barriers succeed. A failed write/sync makes the writer unavailable until recovery; caller cancellation cannot roll back already issued writes or prove noncommitment. Successful reopening validates the persisted boundary and reconstructs the committed index; it supplies restart evidence distinct from a write or sync result. Storage does not emit `COMMITTED_LOCAL`: the Q1 actor must include both message and recoverable scoped producer outcome in the successful barrier before issuing that broker result.

Typed errors distinguish write, synchronization/publication uncertainty, capacity/queue exhaustion, incompatible version/identity/incarnation, invalid or missing offset and committed corruption. `Error::Io` preserves `IoFailureKind::{Read, Write, Sync}`, the operation name and underlying I/O source. Unsupported versions and identity mismatches fail closed. Retention, consumer state policy and dedup interpretation remain outside this crate.

## B-02 Q1 publication path

`rovqet-durable::Partition` owns a dedicated blocking thread and a bounded command channel. `PartitionConfig` fixes partition, ownership epoch, queue message/byte limits and a hard saved-outcome count. `open`, `PendingPublish::wait`, `shutdown` and handle destruction are blocking operations and must run outside async network workers. `try_publish` accepts an already authorized `PublishRequest`, checks partition/Q1 class/epoch and reserves payload allocation capacity plus both record/outcome overheads through the active commit. A cancelled waiter does not cancel an accepted storage operation. Shutdown drains accepted work and joins the owner.

The opt-in server route validates the authenticated development principal, configured topic/partition, Q1 class, incarnation, ownership epoch and TTL before bounded submission. `try_publish_before` carries a monotonic deadline; expired fresh queued work does not append, while an already committed retry can return its established result. `PendingPublish::wait_async` awaits a Tokio oneshot without occupying a blocking worker. `SessionHandler::request` defaults to an immediate result for Q0 and permits a deferred future for Q1. Accepted durable operations survive dropped response receivers.

The actor persists paired message/outcome records and reconstructs scoped results on restart. It has no eviction or advertised timed dedup window: exhausted outcome capacity rejects new tokens. The server owns an exclusive advisory `SERVER.lock` through recovery and drain. Startup and final actor shutdown run through `spawn_blocking`; the Q0 crate has no filesystem dependency. Durable consumer delivery, replay/FETCH and Q2 remain unsupported.

### B-04 concrete read-only actor API

`rovqet_durable::Partition::try_replay(ReplayRequest)` returns `PendingReplay` with blocking
and asynchronous waits. Requests specify partition, `DurableOffset`, maximum messages,
maximum response charge and bounded wait. `ReplayBatch` contains only `ReplayMessage`
values, an incarnation-bearing next offset, charged bytes and explicit available-history
end. Typed errors distinguish invalid limits/ranges, stale incarnation, capacity, too-small
response, unavailable storage and corruption. The actor serializes reads with commits;
returned batches retain their budget reservation. `open_existing` fails on missing history
rather than creating it. Detailed offset, memory, cancellation and wire boundaries are in
[06](06-q1-storage.md). No consumer ACK or group mutation is implied by this interface.
