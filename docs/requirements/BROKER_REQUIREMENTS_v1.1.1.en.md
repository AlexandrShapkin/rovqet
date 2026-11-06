# Technical Requirements Specification for a Specialized Message Broker

> High client connection density · Predictable latency · Selective reliability

**Version:** 1.1.1 (complete normative edition; supersedes version 1.1)  
**Date:** 2026-11-05  
**Status:** Specification for implementation and technical acceptance  
**Origin:** Engineering specification based on two research studies [S1], [S2], external references [E1]–[E9], and independently evaluated reviews [R1].  
**Companion:** [Change register, versions 1.0 → 1.1.1](CHANGELOG_1.0_to_1.1.1.en.md)

**Priority of objectives:** Maintain predictable service quality at high client connection density. Technologies are means to an end, not ends in themselves. This specification includes requirements, architecture decisions, limitations, verification, risks, and an implementation plan. Version 1.1.1 incorporates independently checked findings from Grok, Qwen, Kimi, and comparative meta-analyses [R1]; those reviews are not normative sources.

**Normative language.** `MUST`, `MUST NOT`, and `REQUIRED` are binding. `SHOULD` identifies a preferred implementation, with any exception documented. Numerical values explicitly identified as *target hypotheses* are not demonstrated achievements: they may be adopted or revised only following reproducible M0 testing and a new specification revision. `P0`, `P1`, and `P2` denote requirement criticality, not implementation phases. If provisions appear ambiguous, normative P0 invariants, durable-commit boundaries and security restrictions take precedence; performance is not a justification for weakening them.

## Contents

1. Purpose and engineering rationale
2. Reassessment of the original architecture
3. Workloads, entities, and terminology
4. Normative functional requirements
5. Wire protocol and state semantics
6. Recommended architecture and components
7. Security and failure safety
8. Non-functional targets and SLOs
9. Testing and verification
10. Acceptance, required artifacts, and Definition of Done
11. Architectural risks, bottlenecks, and mitigations
12. Implementation plan, milestones, and gates
13. Research-to-implementation traceability
14. Example minimal v1 configuration (invariant schema)
15. Mandatory engineering invariants
16. Open engineering decisions with acceptable alternatives
17. Sources and normative references

## 1. Purpose and engineering rationale

### 1.1. Development objective

Develop a new message broker for robotics, cyber-physical, IoT, agent-based and streaming neuroinformation systems that provides:

- Many simultaneously connected clients with controlled and measurable overhead per connection.
- Low, predictable latency for short messages, especially p95/p99/p99.9 tail latency and bounded jitter.
- Isolation of critical channels from high-volume noncritical telemetry and slow consumers.
- Different durability and delivery guarantees based on message/topic policy without forcing all traffic to incur synchronous persistence and quorum costs.
- Horizontal scalability without a single central router on every message path.
- Explicit, safe, diagnosable degradation under resource exhaustion, topology changes and failures.

The primary success metric is **not** peak messages per second in isolation but predictable latency and continued operation as connection density, subscription complexity, failures and bursts increase. Results MUST be compared with relevant operating modes of existing brokers at comparable guarantees.

### 1.2. Findings supported by the source material

The first research study [S1, pp. 12–14, 18–22, 23–33] identifies latency, throughput, concurrent connections, CPU/RAM/network/disk consumption and recovery metrics, and describes one-to-many, many-to-one and many-to-many experiments, bursts and network/node failures. Its core finding concerns connection-management overhead and the latency–persistence trade-off.

The second research study [S2, pp. 6–11, 12–17, 18–30] proposes partitioned topics, consumer groups, hybrid storage, consistent hashing, asynchronous processing, priorities, backpressure, selective replication and Raft. Its initial stack [S2, pp. 31–39] includes Rust, Tokio, QUIC/Quinn, FlatBuffers, a custom segmented log and sled. The study itself characterizes this architecture as a theoretical hypothesis requiring implementation and comparative measurement.

**Evidence boundary.** Available materials do not contain a complete machine-readable raw measurement dataset, an exact reproducible hardware description, all broker versions, absolute message sizes, the network topology or the load-testing source code. Charts [S1, pp. 24, 26, 28] and qualitative conclusions MUST NOT be turned into established numerical SLOs without reconstructing the methodology. This specification defines a new validation procedure rather than presenting desired results as experimental facts.

### 1.3. Product scope and exclusions

**Included:** native data plane and protocol, SDK, standalone server, cluster, routing and partition ownership, best-effort delivery, local and replicated reliable delivery, replay, observability, policy management, load and fault testing, and operational tools.

**Excluded from the first production release:** hard real-time guarantees over nondeterministic IP networks; distributed transactions with an external DBMS; end-to-end exactly-once execution of user business operations; general streaming analytics; SQL over messages; geographically distributed active-active with automatic conflict resolution; full Kafka/AMQP/MQTT compatibility; custom cryptography or an in-house consensus algorithm; and unconditional delivery in the absence of a quorum.

Protocol connectors/bridges may be added later; native data-plane performance MUST be measured separately from bridge performance.

### 1.4. Priorities

| Priority | Scope | Acceptable trade-off |
|---|---|---|
| P0 | Protocol safety; correctness of guarantees; bounded memory; recovery; critical-traffic p99; measurability | Reject new load rather than accumulate it without bounds |
| P1 | Connection density; horizontal scaling; replay; group model; efficient storage | Higher reliable-traffic latency; no cross-partition ordering |
| P2 | QUIC optimizations, more SDKs, bridges, advanced filtering, zero-copy, auto-tuning | Implement only with demonstrated value and preservation of P0/P1 |

## 2. Reassessment of the original architecture

Retain the objectives and useful principles from [S2], but apply these engineering corrections.

| Original assumption | Adopted implementation decision | Rationale and verification |
|---|---|---|
| QUIC is the mandatory base transport | Compatible TCP + TLS 1.3 baseline; QUIC/Quinn is a mandatory experimental adapter and a candidate default after A/B tests | QUIC reduces inter-stream blocking, but each independent device still uses a connection; CPU, RAM, UDP/NAT behavior and shared congestion windows must be measured [E1] |
| 0-RTT can accelerate arbitrary requests | Disable 0-RTT for publishing, acknowledgments, administration and state-changing requests | Prevent early-data replay [E2] |
| FlatBuffers for every payload | Opaque payload and compact versioned envelope; FlatBuffers/Protobuf chosen by SDK/application | The broker does not inspect payloads; a mandatory format does not speed routing and harms interoperability |
| sled is the mandatory metadata database | Storage API, with redb as initial candidate for local ACID state, replaceable after testing | Avoid tying consistency/durability to an unproven KV choice; redb documents ACID and crash safety [E5] |
| Consistent hashing determines ownership | Hash/rendezvous computes only desired placement; actual ownership comes from a coordinated epoch/fencing map | Otherwise network partitions may yield two active owners |
| Asynchronous disk write alongside delivery provides reliability | Q1/Q2 publication acknowledgment and reliable delivery only after the corresponding durable commit | Data may be lost before fsync/quorum even if delivered speculatively |
| Three QoS modes up to exactly-once | Q0/Q1/Q2, at-least-once for reliable consumers, and bounded deduplication | Exactly-once execution outside the broker needs application coordination [E3] |
| Raft for each logical partition | Aggregate logical partitions into physical durable-shards with Raft groups for Q2; Q1 uses local WAL without mandatory consensus | Avoid excessive leaders, timers, logs, quorums and fsync operations |
| Sublinear RAM versus connection count | Bounded linear cost per connection, minimized RAM/CPU slope and stable p99 | Every connection needs nonzero state; globally sublinear memory is physically inappropriate |
| One queue model for all recipients | Separate volatile fan-out and durable consumer groups under a shared namespace/API | Avoid persistent state for every one of tens of thousands of ephemeral subscribers |

**Architecture invariant:** fast and reliable traffic share security, metadata and routing, but use independently bounded queues and different acknowledgment barriers. Overload MUST NOT silently downgrade a message's guarantee.

## 3. Workloads, entities, and terminology

### 3.1. Representative use cases

**UC-01 — Dense telemetry.** Tens of thousands of sensors retain long-lived connections; most periodically send small measurements. Under overload, fresh data are more valuable than old delayed values: Q0, TTL, coalescing by source key.

**UC-02 — Critical commands.** A controller sends high-priority commands with explicit deadlines to actuators. Broker acceptance and execution by the device require separate acknowledgments. Non-repeatable commands use Q2 plus an idempotent `command_id` at the actuator. Broker ACK does not mean that the action was executed.

**UC-03 — Event collection for analytics.** Many publishers write to a replayable stream with bounded retention and groups of scalable workers. Q1 or Q2.

**UC-04 — Multicast state propagation.** One publisher sends state to thousands of subscribers. Physical fan-out must be accounted for and per-subscriber backpressure must prevent a slow recipient from blocking others.

**UC-05 — Network migration and reconnection.** Devices survive timeouts, reconnects and interface changes. The client restores ephemeral subscriptions; the server restores durable cursors.

**UC-06 — Cluster reconfiguration.** Node admission, drain, leader change and partial network failures must not cause split-brain or falsely acknowledged durable commits.

### 3.2. Core entities

- **Tenant:** administrative and quota boundary.
- **Principal:** authenticated identity of a client, node or operator.
- **Topic:** named publication namespace, allowed guarantees, ACL and retention.
- **Partition:** logical ordering and sequence key within a topic.
- **Lane:** independent volatile or durable processing path; no ordering across lanes.
- **Durable shard:** physical group of logical durable partitions using a shared StorageEngine/WAL. Q1 may use a local aggregate log without Raft. A Q2 replicated durable-shard MUST use a coordinated journal and Raft group (or an equivalent protocol with verifiable committed-prefix semantics). Code/interface sharing does not imply a shared acknowledgment boundary.
- **Group:** consumer group in which one message from a partition is offered to one active member; replay/ACK exist only with durable policy.
- **Session:** transport connection, identity and transient state, subject to buffer and timer limits.
- **Epoch/term:** monotonically increasing coordinated ownership/consensus term used to fence older owners.
- **Committed:** Q1 = locally durable commit on the owner's current functioning disk; Q2 = durable committed prefix of a replicated log on a quorum. Q1 does not survive irrecoverable owner/disk loss by contract. Q2 cannot depend on locally acknowledged but non-replicated Q1 records.
- **Consumer ACK accepted:** validated acknowledgment within the current generation/lease. **ACK committed:** acknowledgment durably recorded in the applicable failure domain.
- **Durable cursor:** greatest contiguous prefix of completed offsets, each closed by a committed consumer ACK or a policy-authorized durable terminal disposition (`DEAD_LETTER`/`QUARANTINED`). An unfinished offset blocks advancement. Authorized and audited `SEEK` is a separate administrative operation, not an ACK or normal terminal disposition.
- **Log incarnation:** immutable identity of a physically recoverable Q1-log generation. After a proven irrecoverable loss of a Q1 owner, the old offset sequence MUST NOT continue silently under the same incarnation.

### 3.3. Guarantee boundaries

Distinguish (a) sent by client, (b) received by server network stack, (c) admitted to a bounded internal queue, (d) durably committed, (e) delivered to consumer, (f) acknowledged by consumer and (g) executed by application. None implies the next automatically. Protocol/API status MUST specify its actual boundary.

## 4. Normative functional requirements

### 4.1. Connections and sessions

**FR-CON-001 (P0).** The server MUST serve long-lived connections using nonblocking I/O, without an OS thread per client. Client, inter-node and administrative listeners MUST be independent.

**FR-CON-002 (P0).** Handshake includes protocol version, capabilities, authentication, maximum frame size, limits and heartbeat negotiation. An unknown mandatory capability produces an explicit incompatibility error.

**FR-CON-003 (P0).** Limits MUST cover open sessions, incomplete handshakes, channels/subscriptions, inbound/outbound bytes, inflight messages, CPU budget and reconnections per period. Exhaustion MUST NOT cause unbounded heap growth. Numerical limits and reserves MUST derive from the reproducible capacity model (NFR-RES-006), not unrelated defaults.

**FR-CON-004 (P0).** Heartbeat/idle timeouts are configurable by profile (normal/mobile/critical), adaptive within documented bounds without violating the negotiated failure-detection SLA. TCP keepalive alone is insufficient.

**FR-CON-005 (P0).** Implement reconnect exponential backoff with jitter and a reconnect-rate limit. Clients MUST obey `RETRY_AFTER` or `SERVER_DRAINING` returned by servers.

**FR-CON-006 (P1).** In `DRAINING`, stop accepting new publications, finish or redirect outstanding acknowledged operations, return correct final outcomes and close sessions within bounded time.

**FR-CON-007 (P1).** The QUIC adapter MUST have controlled limits for streams, datagrams, flow control and connection migration. 0-RTT application writes are prohibited; only separately demonstrated safe idempotent reads may be considered.

### 4.2. Namespaces, routing and subscriptions

**FR-TOP-001 (P0).** Namespace form: `tenant/topic[/subtopic]`. The protocol defines UTF-8 normalization, name length, allowed characters, reserved prefixes and wildcard rules. Ambiguous normalization, overlapping masks or aliases MUST NOT permit ACL bypass.

**FR-TOP-002 (P0).** Support publish, subscribe, unsubscribe and addressed communication through explicit routing within the authorized namespace. Pub/sub uses independent subscribers; a queue group has one active receiver per partition within the group.

**FR-TOP-003 (P0).** Pattern subscriptions have quotas on match count and result size. Subscription lookup MUST use an index rather than scanning every client on each publish. Wildcard matching quotas MUST be assessed together with total subscribers, aggregate fan-out and egress buffers (FR-TOP-008).

**FR-TOP-004 (P0).** Determine a partition using a stable hash of `(tenant, topic, partition_key)` and a fixed `partition_count`. Changing partition count requires controlled migration and a warning about possible key-order changes.

**FR-TOP-005 (P1).** Inter-node routing uses a cached, coordinated map. The normal route is at most one proxy hop to the owner; stale mappings yield `REDIRECT` with epoch rather than an unbounded forwarding loop. Never issue a new unsafe map without the relevant quorum.

**FR-TOP-006 (P1).** Support idempotent topic create/update/delete, configuration versions, audit, dry-run validation and migration requirements for incompatible changes.

**FR-TOP-007 (P1).** Bounded coalescing (retain latest value per `coalesce_key`) is allowed only on explicitly enabled Q0 topics. Never replace already accepted Q1/Q2 records without a separate formal compaction policy.

**FR-TOP-008 (P0).** The server MUST provide configurable hard/soft limits for active subscriptions per topic/tenant; bytes/messages per subscriber outbound buffer; outbound bytes/s and deliveries/s per topic/tenant/node/globally; and fan-out admission rates and physical recipients. At the hard limit, reject new subscriptions with `SUBSCRIBER_LIMIT` or `QUOTA_EXCEEDED`; egress quota overruns trigger targeted backpressure/`RETRY_AFTER`. Q0 losses are permitted only under the configured per-subscriber/topic policy, with dropped counts; slow subscribers do not block others. For Q1/Q2, committed data MUST NOT be deleted to free outbound buffers: pause delivery/apply credits, preserve replay/cursor, and optionally disconnect overloaded consumers without modifying committed data. Quantities follow the capacity model and tests, not fixed values in this specification. `PUB_ACK` never means delivery to all subscribers; `delivered_count` is separate retrospective telemetry.

### 4.3. Guarantee classes and acknowledgment contract

| Class | Operation | Meaning of PUB_ACK | Duplicates | Failure properties |
|---|---|---|---|---|
| Q0 / volatile | Publish into bounded memory for live subscribers | `ADMITTED`: accepted for processing, not delivered or stored; response may be disabled | May result from client republishing; broker makes no deduplication promise | Loss allowed on crash, disconnect, TTL or overload as per policy |
| Q1 / local durable | Append, integrity verification, local durable commit | `COMMITTED_LOCAL` after contractual fsync/group commit | Retries may produce duplicates; idempotent producer and bounded dedup supported | Survives process crash/restart on the same functioning disk; node/disk loss may cause unavailability or loss |
| Q2 / quorum durable | Append/replicate and commit across a quorum of independent nodes | `COMMITTED_QUORUM` after verified durable quorum | Retries may produce duplicates; bounded dedup and idempotent consumer APIs | With RF=3, up to one node failure preserves acknowledged records under stated assumptions; no new commit ACK without quorum |

**FR-QOS-001 (P0).** Verify that requested class is allowed by topic policy and infrastructure is ready; for Q1/Q2 the logical partition's durable mode MUST match the requested class. On a mismatch or missing prerequisites, return `UNSUPPORTED_GUARANTEE`, `NO_QUORUM`, `STORAGE_UNAVAILABLE`, or `RESOURCE_EXHAUSTED` **before admission**. Silent Q2→Q1→Q0 downgrade and implicit Q1→Q2 upgrade are prohibited.

**FR-QOS-002 (P0).** For Q1/Q2 durable subscriptions, support at-least-once delivery with ACK, NACK, `EXTEND_LEASE`, maximum delivery attempts, retry backoff, explicit `DEAD_LETTER`/`QUARANTINED` and manual replay. Event records and durable cursor/ACK state MUST survive the applicable faults. ACK and cursor MUST follow FR-GRP-006; consumer ACK and `PUB_ACK` are distinct.

**FR-QOS-003 (P0).** Transport acknowledgments and broker `PUB_ACK` MUST be distinct. Lost responses/timeouts yield `OUTCOME_UNKNOWN`, not a claim that the publish failed definitively. Client retries can use the same producer identity/sequence/message ID.

**FR-QOS-004 (P0).** Maintain persistent producer identity scoped to `(tenant_id, principal_id, producer_id)`, monotonically increasing `producer_epoch`, fencing of old epochs, and monotonic `sequence_number` scoped to `(tenant_id, principal_id, producer_id, topic, partition, durable lane)`. Producer-epoch fencing is enforced within this scope without cross-shard global RPC. SDK MUST maintain independent sequences per partition. Provide a bounded Q1/Q2 dedup window (default 24 hours, configurable). Within the window, retrying identical `(identity, epoch, sequence)` with identical digest/attributes MUST return the original committed outcome/offset without a second committed record. Conflicting content MUST return `SEQUENCE_CONFLICT`. Clients assign increasing sequences; gaps/out-of-order are rejected with `SEQUENCE_GAP` in v1 until the expected sequence is restored, with no hidden buffering or acknowledgment of invalid ordering. Older epochs return `PRODUCER_FENCED`. On reconnect, owner change or crash, dedup/fencing state MUST recover consistently with the relevant durable commit; overload MUST NOT silently shorten the window. After expiration, preventing reinsertion is no longer guaranteed: an old sequence may be rejected if a high watermark remains, or accepted again if window state was removed. SDK MUST disclose this limitation. Changing `partition_count` requires a new scoped stream/epoch, not an illusion of global ordering. State MUST be bounded by active-producer/window quotas and MAY use compact high-watermark plus bounded outcome history instead of per-message records. If the promised window cannot be retained, reject before admission with `RESOURCE_EXHAUSTED`, rather than silently lose dedup guarantees. Memory/KV/hybrid strategy remains open for M0-B, including power-cut/recovery tests.

**FR-QOS-005 (P0).** MUST NOT claim exactly-once application execution. Single log insertion within the dedup window and application-side idempotence are possible. External effects (actuator command, database write) require an application idempotency key or application-managed transactional coordination.

**FR-QOS-006 (P1).** QoS policy is configured per topic; per-publish overrides are allowed only within the authorized range and MUST NOT change an existing logical partition's durability domain. In v1, every `(tenant, topic, partition, durable lane)` MUST have one immutable `durable_class`: Q1 **or** Q2. Publishing different Q1/Q2 classes to the same partition is prohibited (`UNSUPPORTED_GUARANTEE`). Different topics or explicitly allocated partitions may use different classes; sharing a StorageEngine does not change guarantees. Q0, when allowed, uses its separate volatile lane. Overrides cannot alter encryption, quotas, ACL or permitted priority range. Changing durable class requires a new log incarnation/partition identity or a controlled migration with provably transferred committed records and fencing; editing a configuration value in place is prohibited.

**FR-QOS-007 (P0).** For a mixed topic, Q0 and the permitted durable class (Q1 or Q2) use separate volatile/durable lanes. No ordering or shared offsets exist between lanes. Within `(tenant, topic, partition, durable lane, log incarnation)`, committed records are monotonically ordered; mixing Q1 and Q2 within one durable offset namespace is **PROHIBITED** in v1. End-to-end ordering requires a durable-only topic with one durable class. Separate physical Q1 and Q2 durability domains ensure Q2's committed prefix never depends on Q1 local-only append.

**FR-QOS-008 (P0).** Reliable messages MUST NOT be presented as durable to consumers before commit. Speculative delivery MAY exist only as a separately marked unsafe API without ACK guarantees and is disabled by default.

### 4.4. Time, lifetime, and queues

**FR-TIM-001 (P0).** Envelope includes untrusted client timestamp, broker receive timestamp and monotonic-derived queue/processing durations. Within-node deadlines use monotonic time. Cross-host timestamp comparison requires known synchronization error; E2E tests account for clock skew.

**FR-TIM-002 (P0).** `ttl_ms` starts at broker admission unless a valid absolute deadline is given; support `max_ttl` per topic. A passed deadline yields `EXPIRED` before admission or `EXPIRED_BEFORE_DELIVERY` after admission, never a false successful-delivery status.

**FR-TIM-003 (P0).** Q0 overload policies are `DROP_NEWEST`, `DROP_OLDEST`, `COALESCE_LATEST`, `REJECT_PUBLISH`, selected by topic/priority with explicit telemetry. Silent post-commit drops of Q1/Q2 are prohibited; expiration/retention/poison messages follow a formal audited policy.

**FR-TIM-004 (P0).** Queue limits cover both bytes and message counts. High/low watermarks trigger credit-based limits and consumer-to-ingress backpressure. Per-client, topic, tenant and global limits are required. Subscriber/topic outbound buffers, physical deliveries and credits MUST be included per FR-TOP-008 for separate Q0 and durable paths.

**FR-TIM-005 (P1).** Q0 may limit maximum queued-message age: explicit expiry is preferable to delivering obsolete state. Q1/Q2 SLA-expiry handling MUST NOT invalidate durability guarantees for previously acknowledged records.

### 4.5. Priority and overload management

**FR-PRI-001 (P0).** Define four classes: `CONTROL`, `INTERACTIVE`, `TELEMETRY`, `BULK`. Priority comes from server ACL/policy; clients cannot elevate themselves to CONTROL.

**FR-PRI-002 (P0).** Apply resource-budget isolation and fair scheduling to CPU dispatch, outbound queues and disk requests. Baseline scheduler: weighted deficit round-robin by class, bounded maximum wait and aging. EDF inside a class is permitted if fairness is preserved.

**FR-PRI-003 (P0).** Higher-priority messages MUST NOT be delayed indefinitely by older low-priority tasks. Lower priorities MUST NOT starve below advertised capacity. Under known over-capacity, some QoS budget may be reserved exclusively for critical classes.

**FR-PRI-004 (P0).** Overload states `NORMAL`, `PRESSURE`, `SHED`, `DRAIN`, `UNAVAILABLE` are defined by machine-enforced rules with hysteresis, anti-oscillation and exported metrics. Inputs include queue length, p99 queue wait, network inflight, durable-log backlog, disk latency and CPU saturation. Thresholds and hysteresis MUST be documented with derivation from the capacity model and tested before reserves run out. Auto-tuning is not required in v1.

**FR-PRI-005 (P0).** Each rejection/drop provides a diagnosable reason when a response is physically possible. Global implicit loss policies or reliability downgrades are prohibited.

**FR-PRI-006 (P1).** Tenant isolation limits noisy neighbors by connections, publish rate, outbound fan-out, durable storage, topic/partition count, metadata operations and control requests. Ingress and egress quotas are independent.

### 4.6. Storage, replay, and retention

**FR-STO-001 (P0).** Volatile uses bounded ring buffers/queues with explicit eviction policies. Durable uses segmented append-only WAL with per-record checksums, versioned format, seal/footer and safe rotation. Q1 MUST support local durability without Raft dependency; Q2 MUST use a replicated WAL/commit protocol. Implementations may share StorageEngine, WAL code and physical resources while retaining guarantee isolation and separate commit barriers.

**FR-STO-002 (P0).** Logs MUST recover from torn writes, corrupted tails, crashes during fsync, rotation, index creation and leader change. A checksum mismatch MUST NOT silently skip acknowledged data.

**FR-STO-003 (P0).** Durable offsets MUST be monotonic in `(tenant, topic, partition, durable lane, log incarnation)`, never reused after the applicable Q1/Q2 commit, and retain identity through snapshots, compaction, migration and recovery from an available copy. A Q1 offset/`COMMITTED_LOCAL` is valid only within the local failure model; it never becomes Q2. If a Q1 owner/disk is lost with no recoverable copy and the final acknowledged boundary is unknown, continuing the old log incarnation or advertising old Q1 offsets as replayable is **PROHIBITED**. Only an explicit diagnosable failure or authorized new log incarnation with notification of historical loss/replay discontinuity is allowed. After Q2 failover, only the Raft committed prefix is visible; an uncommitted local tail is not committed. Tombstones and compaction never reuse offsets.

**FR-STO-004 (P0).** Retention supports age, byte and maximum disk-budget limits, with explicit per-topic handling on reaching them. If contracted retained data would otherwise be lost, stop new publications instead of deleting confirmed records contrary to policy.

**FR-STO-005 (P0).** fsync/flush and group commit require measurable boundaries. OS page-cache insertion is not Q1/Q2 durability. With RF=3, `COMMITTED_QUORUM` requires durable majority commit under the protocol; uncertain storage acknowledgment fails closed.

**FR-STO-006 (P1).** Bounded replay by offset/time with `FETCH(max_bytes, max_messages, wait_ms, credits)`, without loading the full log into RAM.

**FR-STO-007 (P1).** WAL indexes may be rebuilt; snapshot publication MUST be atomic, versioned, checksummed and checked against metadata epoch. Background compaction/retention cannot contend with the commit path without control. Snapshot/compaction MUST preserve logical-offset→record/tombstone mapping, acknowledged cursor and fencing. A snapshot cannot become active before integrity verification and coordination with commit index. Old records are removed only by explicit retention policy, never merely because they occupy a segment beside another partition.

**FR-STO-008 (P1).** On disk-full, read-only filesystem, EIO, ENOSPC, fsync timeout or degraded RAID, move affected partitions out of write-ready state; never issue false ACKs. Operator involvement in recovery is needed only for genuinely ambiguous state.

**FR-STO-009 (P1).** Implement minimal backups: consistent metadata snapshot plus replicated durable-state snapshot, checksum, version manifest and isolated-cluster restore verification. Backup does not replace Raft; Raft does not protect against accidental administrative deletion as a backup does.

**FR-STO-010 (P0).** When logical partitions share a local Q1 shard or replicated Q2 durable-shard, the system MUST independently enforce each partition's retention, ACL, tenant quotas, committed-prefix visibility, logical offsets, consumer cursor and restore/compaction. Deleting shared physical segments is **PROHIBITED** while it would destroy live records in any partition or violate declared retention. Legitimate expiration may make old offsets unavailable but MUST NOT reuse them, advertise uncommitted tails as committed, or silently advance consumer cursors. Physical WAL/Raft index rearrangements during snapshot/migration cannot change logical offsets. Tenant isolation is checked before shared-storage access. Per-partition quotas limit hot partitions; sharing CPU/disk allows measurable but not unbounded neighbor degradation, with criteria fixed in M0-C. Resource-incompatible critical and BULK partitions SHOULD be assigned separate shards unless budgets and tests demonstrate adequate isolation.

### 4.7. Consumer groups and task processing

**FR-GRP-001 (P0).** Volatile subscriptions support live push without a retained server cursor after disconnect. Durable groups have `group_id`, independent durable cursor, lease/inflight tracking, replay and ownership. A durable cursor MUST be bound to log incarnation/partition identity. A new Q1 incarnation after irrecoverable history loss requires explicit reset/new generation with audit and SDK notification; the old cursor is not silently carried over or claimed continuous.

**FR-GRP-002 (P0).** For each `(group_id, topic, partition)`, at most one consumer can have a valid current generation/lease ownership. Reassignment fences the former owner; late ACKs from it cannot advance the cursor.

**FR-GRP-003 (P0).** Support ACK, NACK, `ACK_CUMULATIVE` only for a contiguous completed prefix, `EXTEND_LEASE`, authorized administrative `SEEK`, `PAUSE` and `RESUME`; strictly validate generation and offset. `ACK_CUMULATIVE` MUST NOT leapfrog incomplete offsets. Out-of-order ACKs and committed terminal dispositions may be recorded in a bounded sparse set, but the cursor moves only over a contiguous completed prefix per FR-GRP-006. Timeout, NACK and `MAX_RETRY` alone do **not** complete an offset. Authorized SEEK requires separate audit, reason and generation/fencing change; it is not presented as an ordinary ACK result.

**FR-GRP-004 (P1).** Define and implement `max_inflight`, `ack_deadline`, `max_retries`, `retry_backoff`, DLQ policy and per-tenant limits on durable group count/state. Deleting a group does not remove messages except through retention.

**FR-GRP-005 (P1).** Rebalances SHOULD be incremental where possible, bounded in concurrent moves, measurable in duration, and tested against slow consumers/disconnect storms.

**FR-GRP-006 (P0).** Consumer ACK is valid only for the current `(tenant, group, topic, partition, ownership epoch, generation, lease)` from an authorized principal and a previously delivered committed offset. Stale, expired or unauthorized ACKs MUST NOT change cursor state. Successful `ACK_RESULT=ACK_COMMITTED` MUST be emitted only after durable ACK-state/cursor persistence: local barrier for Q1, coordinated durable quorum barrier for Q2. The durable cursor MUST advance only over a contiguous prefix whose offsets are closed by (a) committed valid consumer ACK or (b) a committed terminal disposition permitted by policy and checked per §5.4.1. Terminal state has durability no weaker than the original message. Timeout/NACK/retry exhaustion without committed disposition do not advance the cursor. Durable selective-ACK set, disposition set and generation fencing must recover consistently with the original committed prefix, including snapshots/replay. Coalesced records in the same coordinated durability domain are recommended; alternative KV/side-log implementations require equivalent crash/recovery and fencing evidence. An atomic transaction between the original publication and subsequent consumer ACK is **NOT REQUIRED**. A duplicate of an already committed ACK may return its saved success as a read-only outcome even after fencing; a new mutation from a stale generation remains prohibited. Losing `ACK_RESULT` before the barrier means `ACK_PENDING`/`OUTCOME_UNKNOWN`; after a committed ACK, retry is safe. Incomplete offsets are never skipped on crash/rebalance; at-least-once redelivery is permitted. Authorized SEEK is audited separately, not disguised as ACK.

### 4.8. Cluster membership, consensus, and routing

**FR-CLU-001 (P0).** Cluster control-plane metadata, membership, schema/topic policies, ACL revisions, desired placement and ownership epochs are coordinated through a consensus library (Raft or formally equivalent), outside the per-message path. Production metadata quorum has 3 or 5 voters. Metadata Raft unavailability follows FR-CLU-011; valid cached policies may be used only within their authorization lifetime.

**FR-CLU-002 (P0).** Placement transitions use `PREPARE → CATCH_UP → FENCE_OLD → ACTIVATE_NEW → CLEANUP` with monotonically increasing epochs. Incomplete moves restart idempotently; an old epoch cannot acknowledge a new durable write.

**FR-CLU-003 (P0).** Q2 is replicated across independent physical nodes, normally RF=3, majority=2. Loss of one node must not lose previously acknowledged records under correct storage and no correlated corruption. Without quorum, the partition becomes `READ_ONLY`/`UNAVAILABLE` and stops acknowledging new writes.

**FR-CLU-004 (P0).** Q2 logical durable partitions MUST be aggregated into physical replicated Raft-shards (or equivalent consensus groups) to amortize consensus costs. Q1 logical partitions MUST use local durable WAL/shards without mandatory Raft. Q1 partitions may share local logs and StorageEngine implementation with Q2, but cannot be required to pay a quorum-commit cost. Group/partition counts are configured using M0-C; a separate consensus group per ephemeral subscription, tiny topic or consumer is prohibited. Q1 and Q2 have no shared Raft committed prefix: v1 cannot mix Q1/Q2 within one logical durable partition (FR-QOS-007). Each shard retains partition-independent logical offsets, retention, ACL, quotas, recovery/snapshot and policies (FR-STO-010/INV-14). Hot partitions MUST be tested for cross-partition p99, fsync queuing and migration; CONTROL/BULK may share a shard only within measured budgets. Following loss of a local Q1 owner, fail/recover from an available copy or establish an explicit new log incarnation (FR-STO-003), never silently continue the old log.

**FR-CLU-005 (P0).** Q2 consumers read only the committed prefix of the current legitimate Raft leader (or provably safe committed snapshots under approved policy); lagging replicas may serve only explicitly marked bounded-staleness reads. An uncommitted old-WAL tail MUST NOT become visible as committed after leader change. Q1 replay is available only from the current owner's functioning local WAL or a provably restored snapshot. If history is unrecoverable, return explicit `REPLAY_NOT_AVAILABLE`/`STORAGE_UNAVAILABLE` with a log-incarnation boundary; do not substitute Q2 records or unrelated Q1 offsets.

**FR-CLU-006 (P0).** Split-brain, stale leaders, duplicate heartbeats, reordered events, delayed joining nodes and clock skew are expected fault scenarios. Heartbeats may raise suspicion, but only coordinated ownership authorizes writes.

**FR-CLU-007 (P1).** Use stable desired placement (e.g., rendezvous hashing with rack/node availability constraints). A coordinated decision chooses the actual owner. Prefer distributing replicas across failure domains.

**FR-CLU-008 (P1).** Bound simultaneous migrations, catch-up bandwidth, snapshot-transfer quotas and NVMe load during mass reconfiguration; critical messages take precedence over background migration.

**FR-CLU-009 (P1).** Membership changes use safe joint configuration or equivalent quorum-safe procedure, not untrusted gossip alone.

**FR-CLU-010 (P1).** Support join, leave/drain, replace, reconcile, snapshot, inspect, repair plan, version compatibility check and health. A repair operation cannot silently delete committed state without administrator approval.

**FR-CLU-011 (P0).** On metadata/control-plane failure or quorum loss, servers MUST enter explicitly diagnosable degraded modes following the normative matrix below. Each operation separately checks (a) a still-valid authorization snapshot with revision/revalidation TTL; (b) a coordinated ownership epoch/lease; and (c) Q1 local storage or Q2 durable-shard quorum as appropriate. Existing data-plane operations MAY continue without synchronous metadata Raft access when all prerequisites remain valid. When ACL/JWKS/credential TTL or ownership lease expires, operations requiring that authority MUST fail closed; a frozen epoch cannot be renewed locally. New topic/ACL/ownership changes and all consensus-dependent administrative mutations are **PROHIBITED** without metadata quorum. A node restart neither renews TTL nor legitimizes stale snapshots; the more restrictive failure state always wins. Provide machine-readable degraded reason and recovery/resynchronization sequence after control-plane recovery.

**FR-CLU-011 operation matrix.** The “CP offline” column assumes valid cached security snapshot, ownership lease and ready class-specific data plane; otherwise use the stricter column. “Yes” means allowed within ACL/quotas; “No” means fail closed; “Conditional” means only if the additional requirements hold.

| Operation | CP offline; snapshot valid | ACL / ownership expired | Q2 shard without quorum | Local storage EIO |
|---|---|---|---|---|
| New connection / reconnect | **Yes:** locally verifiable identity/JWKS/ACL, limits, versions | **No:** deny auth or close per revalidation policy | **Conditional:** Q0 possible; Q2 operations denied | **Conditional:** sessions may exist without affected durable writes |
| Q0 publish/subscribe | **Yes:** existing topic, valid owner/ACL, bounded quotas | **No:** if ACL/epoch invalid | **Yes:** if Q0 owner/resources valid | **Yes:** if Q0 resources/owner healthy |
| Q1 publish/replay | **Yes:** authorized Q1 owner and functioning local WAL | **No:** stale owner/ACL | **Conditional:** Q1 only on its functioning local state | **No** on affected Q1 WAL; unaffected partitions yes |
| Q2 publish/committed replay | **Yes:** shard quorum and legitimate leader/epoch | **No:** stale owner/ACL | **No** publish; committed replay only if provably safe read | **No** on affected node; may use functioning quorum |
| Consumer ACK | **Yes:** valid generation/lease and durability barrier | **No:** stale generation/owner/ACL | **No** Q2 ACK without quorum; Q1 per own storage | **No** on affected durable store; other groups yes |
| Create/modify topics | **No:** metadata quorum needed | **No** | **No** | **No** when metadata quorum required |
| Admin mutations / ACL / security | **No**, including security-policy changes | **No** | **No** without metadata quorum | **No** for impossible mutations; read-only diagnostics conditional |
| Ownership/epoch change | **No:** no new consensus epoch | **No** | **No** if metadata quorum lost | **No** without metadata quorum; local shard election does not extend ownership |
| Read-only status/metrics | **Yes:** stale/revision marking and ACL | Only explicitly permitted public anonymized health; protected diagnostics denied | **Yes:** health plus `NO_QUORUM` | **Yes:** health plus `STORAGE_UNAVAILABLE` |

### 4.9. Administrative operations and SDK

**FR-API-001 (P0).** Native binary data protocol v1; administration via separate secured HTTPS API with versioned v1 OpenAPI schema. Never serve administration on a public data listener.

**FR-API-002 (P0).** Minimum administrative API: health/readiness, cluster nodes, partitions/shards/owners, tenants, principals/roles, topics/policies, quotas, retention, message statistics, group lag, drain, config validation and audit events. All mutations use RBAC, dry-run and idempotency keys.

**FR-API-003 (P0).** Official Rust SDK supports publish, subscribe, group consume, ACK/NACK, retry, async streams, metrics, tracing context, idempotent publisher, reconnect, backpressure callbacks and strongly typed errors. It cannot treat `OUTCOME_UNKNOWN` as definite failure. Consumer ACK outcome is typed separately from publish outcome; missing ACK response is uncertain, not proof of successful persistence.

**FR-API-004 (P1).** A second SDK in Python or Go (chosen at M0), with cross-version client/server contract tests. Low-power device examples may use C ABI or a simplified reference client after the initial release.

**FR-API-005 (P1).** CLI covers administration, topic/partition/connection diagnostics, publish/subscribe/replay, benchmark smoke tests and config validation. Production includes a read-only CLI profile.

**FR-API-006 (P1).** Server major vN supports client major vN; support/deprecation period for older major versions is documented separately. Negotiate minor capabilities explicitly. Unsupported frame types are rejected safely or ignored only when marked optional.

## 5. Wire protocol and state semantics

### 5.1. Proposed wire contract

The first release MUST publish the complete wire v1 specification and test vectors (hex plus JSON descriptions) for all major frames. Before the M1 exit gate, freeze numeric `frame_type`/TLV/error IDs, required/optional flags, endianness, canonical varint encoding, maximum nesting/lengths and feature negotiation modes.

Frame layout:

```text
magic(2) | protocol_major(1) | flags(1) | header_len(varint) |
payload_len(varint) | frame_type(varint) | request_id(64) |
TLV headers | payload | optional checksum
```

Use network byte order; validate size before allocating memory. Standard routing need not decode all TLVs. Unknown optional TLVs are safely skipped; unknown critical TLVs are rejected.

Required envelope fields: `tenant_id` (derived from authorization, never trusting an asserted client field), `topic_id/topic_name`, `partition_key`, 128-bit `message_id`, `producer_id`, `producer_epoch`, `sequence_no` (Q1/Q2 idempotence), `qos`, `priority`, `ttl_ms`, `payload_len`; optional `schema_hint`, `trace_context`, `content_type`, `client_timestamp_ns`. `server_receive_ns` and `offset` are server-assigned.

Preliminary v1 limits: maximum frame 1 MiB; default `max_payload` 256 KiB (CONTROL 16 KiB); maximum topic name 255 bytes; TLV metadata 4 KiB; session/subscription limits per tenant policy. Large payloads SHOULD be stored as external objects with published references, not unbounded frames. Validate these limits by fuzzing and profiling.

Primary frame types: `HELLO`, `WELCOME`, `AUTH`, `AUTH_OK`, `PING`, `PONG`, `PUBLISH`, `PUB_ACK`, `SUBSCRIBE`, `SUB_ACK`, `UNSUBSCRIBE`, `MESSAGE`, `CONSUMER_ACK`, `CONSUMER_ACK_RESULT`, `CONSUMER_NACK`, `EXTEND_LEASE`, `FETCH`, `FLOW_CREDIT`, `REDIRECT`, `ERROR`, `DRAIN`, `CLOSE`. `CONSUMER_ACK_RESULT` correlates to the original ACK's `request_id`; its outcome is `ACK_COMMITTED`, `ACK_PENDING`, or a rejection from §5.1.1. The SDK MUST NOT treat a missing result as successful persistence.

### 5.1.1. Mandatory v1 error catalog

An `ERROR` or frame rejection MUST carry machine-readable code, stable class (`CLIENT`, `AUTH`, `RESOURCE`, `RETRYABLE`, `DURABILITY`, `TOPOLOGY`), request ID, `retryable` boolean, optional `retry_after_ms`, safely disclosable current epoch/revision and a secret-free diagnostic. The following codes and classes are mandatory; freeze numerical wire IDs in a published registry before M1 and never reuse them. `RETRY_AFTER` is a minimum retry delay, **not** a promise to admit the retry. `OUTCOME_UNKNOWN` means the operation *may* have committed: only idempotent retry or status lookup is safe. `NO_QUORUM` forbids new Q2 commits but does not imply loss of already committed records.

| Stable code | Class | Required meaning |
|---|---|---|
| `BAD_FRAME` | CLIENT | Noncanonical frame/varint, invalid lengths or mandatory fields; connection may close |
| `UNSUPPORTED_VERSION` | CLIENT | Unsupported major protocol version; safe handshake failure |
| `UNSUPPORTED_CAPABILITY` | CLIENT | Required negotiated capability missing |
| `UNAUTHENTICATED` | AUTH | Identity not established; data operations prohibited |
| `AUTH_EXPIRED` | AUTH | Token/JWKS/security snapshot expired; stop operations |
| `PERMISSION_DENIED` | AUTH | ACL/tenant/policy rejects operation |
| `QUOTA_EXCEEDED` | RESOURCE | Tenant/topic budget exceeded; may include `RETRY_AFTER` |
| `SUBSCRIBER_LIMIT` | RESOURCE | Subscriber hard limit; reject before creating subscription state |
| `EGRESS_QUOTA` | RESOURCE | Egress budget exhausted; flow-control/drop only according to allowed QoS policy |
| `RESOURCE_EXHAUSTED` | RESOURCE | Queue/inflight/memory shortage; never false ACK |
| `RETRY_AFTER` | RETRYABLE | Temporary rejection, `retry_after_ms`, backoff + jitter |
| `SERVER_DRAINING` | RETRYABLE | Server draining; refuses new operations |
| `OUTCOME_UNKNOWN` | RETRYABLE | Lost response or unobserved commit; do not classify as definite failure |
| `UNSUPPORTED_GUARANTEE` | CLIENT | Topic disallows requested Q0/Q1/Q2 |
| `NO_QUORUM` | DURABILITY | No quorum for Q2/applicable ACK state; no new durable ACK |
| `STORAGE_UNAVAILABLE` | DURABILITY | WAL EIO, ENOSPC, fsync failure or corruption; fail closed |
| `STALE_EPOCH` | TOPOLOGY | Outdated ownership epoch; redirect only with valid authority |
| `STALE_GENERATION` | TOPOLOGY | ACK/NACK from fenced consumer generation; cursor unchanged |
| `LEASE_EXPIRED` | TOPOLOGY | Consumer lease expired; assignment must be renewed |
| `SEQUENCE_GAP` | CLIENT | Sequence discontinuity; expected value safely provided |
| `SEQUENCE_CONFLICT` | CLIENT | Same sequence with different digest/fields; reject without new commit |
| `PRODUCER_FENCED` | TOPOLOGY | Stale producer epoch; writes from that epoch prohibited |
| `EXPIRED` | CLIENT | TTL/deadline elapsed before admission |
| `EXPIRED_BEFORE_DELIVERY` | CLIENT | Admitted, but expired before Q0 delivery or lawful durable outcome |
| `ACK_NOT_COMMITTED` | DURABILITY | ACK processed/accepted but durability barrier not reached; consumer retry is safe |
| `REPLAY_NOT_AVAILABLE` | CLIENT | Requested offset/old log incarnation missing due to legitimate retention or unrecoverable local Q1 history; return safely disclosable current incarnation and explicit reset/replay indicator, not an invented committed gap |
| `DLQ_UNAVAILABLE` | DURABILITY | DLQ transition not committed; original message cannot be forgotten |

This catalog is shared by SDK, administrative audit and observability. An unknown or nonexistent error code MUST NOT be treated as success. Unknown future codes map to `UNKNOWN_REMOTE_ERROR`, preserving the retryable flag but avoiding automatic unsafe retries.

### 5.1.2. Protocol test vectors and compatibility

Before M1 completion, publish byte-accurate golden vectors (hex, expected decoded JSON, CRC/length and expected response) for HELLO/WELCOME negotiation, Q0/Q1/Q2 publishing, PUB_ACK stages, CONSUMER_ACK/RESULT, flow control, REDIRECT, errors from each class, optional/critical TLV, invalid UTF-8, oversized frame/varint, corrupt checksum, truncation and unknown future frames. Record major/minor/capabilities, endianness and fixture ID for every vector. Server and SDK MUST interoperate within the negotiated major and supported minors. Incompatible required fields return `UNSUPPORTED_CAPABILITY`/`UNSUPPORTED_VERSION`; unknown optional fields are skipped safely without changing guarantees. New wire `frame_type` or `error_code` values require registry versioning/change control.

### 5.2. Connection states

`NEW → HANDSHAKE → AUTHENTICATED → ACTIVE → DRAINING → CLOSED`; exceptional states include `FAILED` and `BANNED/RATE_LIMITED`. Data frames are forbidden before `AUTHENTICATED`. When credentials or ACL revision expire, revoke access no later than the documented revalidation TTL.

During transition to DRAINING, the server MUST NOT accept a publication for which it cannot produce a meaningful outcome. Every request has a `request_id`; re-sending a response never changes the committed outcome.

### 5.3. Q1/Q2 publication states

`RECEIVED → VALIDATED → ADMITTED → APPENDED → (Q2: REPLICATED) → DURABLY_COMMITTED → PUB_ACK_SENT`; alternatives: `REJECTED`, `EXPIRED`, and client-observed `OUTCOME_UNKNOWN`. For Q1, durable commit is the owner's local fsync/barrier; for Q2, it is the Raft group's durable majority committed prefix. Check immutable partition `durable_class` before ADMITTED; the opposite class is rejected with `UNSUPPORTED_GUARANTEE` without append or offset allocation. Q2 commit never relies on locally acknowledged Q1 entries: they are absent from its Raft log. After ownership change, Q2 exposes only the proven committed prefix. Dedup state and outcome are persisted in the applicable durability domain. Following reconnect, a producer queries the result or safely retries with the same identity/epoch/sequence.

Q0 may follow `RECEIVED → ADMITTED → DISPATCHED/SHED/EXPIRED`; ADMITTED is not delivered. No live subscribers is not an error unless policy requires a subscriber.

### 5.4. Delivery states for reliable consumers

`READY → LEASED(generation, deadline, ownership_epoch) → DELIVERED → ACK_RECEIVED → ACK_VALIDATED → ACK_DURABLY_COMMITTED → ACK_COMMITTED`. Alternative paths: `LEASE_EXPIRED/NACK → RETRY_QUEUED`, `MAX_RETRY → DEAD_LETTER_PENDING or QUARANTINE_PENDING`; after durable terminal disposition: `DEAD_LETTER/QUARANTINED → CLOSED_FOR_GROUP`. Only a committed ACK or committed terminal disposition closes an offset for the contiguous consumer cursor; PENDING does not. `CONSUMER_ACK_RESULT=ACK_COMMITTED` is possible only after FR-GRP-006's barrier. Losing the ACK response permits an idempotent read-only lookup of a previously committed outcome; a fenced generation cannot create a new ACK. Broker makes no claim of exactly-once application handler execution.

### 5.4.1. Retry, DLQ, quarantine and replay

On NACK/LEASE_EXPIRED, the message remains in its original committed log. Attempts and retry backoff are bounded. Timeout, NACK and exhaustion of `max_retries` do **not** close the offset.

When terminalization policy permits:

1. `QUARANTINE_PENDING → QUARANTINED` only after durable persistence of `(group, source log incarnation, offset, reason, attempts, generation/fence)` in a durability domain no weaker than the source Q1/Q2.
2. `DEAD_LETTER_PENDING → DEAD_LETTER` only after a complete replayable DLQ record has been durably published with guarantee no weaker than the source **and** the original terminal disposition is durably recorded.

The remote DLQ may belong to another shard. A distributed atomic transaction is **NOT REQUIRED** if an idempotent `disposition_id` and recoverable order `DLQ_COMMITTED before TERMINAL_COMMITTED` prevent loss and permit safe recovery/retry. If DLQ is unavailable, publication outcome is unknown, or terminal record is not committed, the source offset remains incomplete and the cursor does not advance. Retrying may create a DLQ duplicate only within an explicitly documented/allowed model and MUST use bounded dedup/idempotence. After durable terminal commit, the offset can enter the contiguous completed prefix; `MANUAL_REPLAY`/administrative `SEEK` are separately authorized and audited operations with a new replay attempt/generation; they do not rewrite earlier ACK history. Retention applies by contract and is not an indefinite preservation guarantee. The protocol, metrics and oracle MUST distinguish committed terminal disposition from consumer ACK.

### 5.5. Ordering and corruption semantics

In v1, each logical durable partition contains Q1 **or** Q2, never both. Committed offsets are ordered within `(tenant, topic, partition, durable lane, log incarnation)`. Q2 order is defined by the coordinated Raft append/commit prefix and is independent of Q1 local-only logs; Q1 order is the current incarnation's local durable append/commit sequence. Concurrent producers are ordered by actual commit, not client timestamps. Following Q2 takeover, consumers see only the committed prefix. After irrecoverable Q1 owner loss, replay may have an explicit discontinuity/new log incarnation, but the broker MUST NOT represent it as a continuous Q1/Q2 log or claim unrecoverable records as committed. There is no Q1/Q2 ordering within one partition in v1 because such mixed publishing is rejected.

No global order exists across partitions, tenants or volatile/durable lanes. Per-key order holds while partition count, placement policy, durable class and log incarnation are stable. On controlled migration/incarnation change, the client receives an explicit boundary; continuity cannot be assumed without proven state transfer.

For concurrent publishers, the actual order is committed append order, not sender timestamp.

Payload compression is allowed at topic/SDK level with decompression-bomb protection. The server need not decompress opaque payloads; bridges MUST enforce the maximum expanded size.

CRC detects accidental corruption and TLS protects transit; CRC is not authentication, so both protections are needed.

## 6. Recommended architecture and components

### 6.1. Logical separation

**Data plane:** transport reactors → authentication/admission → topic router → partition owner → volatile dispatch or durable shard → per-subscriber/consumer flow control → outbound transport. Work queues are bounded; changes to tenant policy apply through an atomic locally versioned snapshot.

**Control plane:** metadata Raft → immutable configuration snapshots → topic/ACL/placement reconciler → ownership epochs and leader leases → node agents. Metadata changes do not require a synchronous RPC per publication.

**Storage plane:** volatile memory pools plus shared StorageEngine/segmented-WAL format; local Q1 log groups and replicated Q2 Raft-shards have independent commit barriers, local metadata/index/snapshots and background compaction. Q0 is independent of disk apart from separate system logs/metrics; Q1 append does not require consensus.

**Observability/operations:** metrics, sampled traces, structured logs, health checks, auditing, config validation. Telemetry export never blocks the critical message path.

### 6.2. Processes and execution threads

The first implementation runs one principal Rust broker process with logically separate components, bounded channels, and dedicated runtime/worker budgets for network reactors, dispatchers, durable I/O and background tasks. Blocking fsync, compaction and cryptographically expensive token processing MUST NOT run on Tokio core reactor threads.

Each session may have a small state machine or lightweight task; implementation MUST compare “per-connection task” against “sharded connection registry + reactors” on memory, context switches, wakeups and p99. Buffer pools must not retain large allocations after bursts. Profile allocator fragmentation, NUMA locality, per-core queues, socket-buffer autotuning and thread contention.

### 6.3. Routing and subscription index

Hierarchical topics are indexed using a prefix tree or equivalent with copy-on-write/RCU snapshots optimized for frequent reads. Subscription updates are amortized/bounded and must not block publication through a global mutex. Hot-topic fan-out is batched with credit accounting and bounded CPU time per dispatch cycle.

For distributed broadcast, forward one logical copy broker-to-broker per receiving node, then perform local fan-out. Batching and shared immutable payload ownership are recommended; neither removes the physical `payload × recipients` egress requirement. Local fan-out remains subject to per-subscriber credits, per-topic/tenant/global egress budgets and physical-delivery accounting. Saving inter-node traffic does not exempt traffic from FR-TOP-008 egress quotas.

### 6.4. Independent queues and resource budgeting

For each `(tenant, topic, partition, lane)` configure ingress publication rate, queue messages/bytes, inflight ACKs, subscription count, outbound queues/credits, priority and fairness. Use ingress/egress token buckets and bounded active queues. Never allocate an unbounded subscriber queue. Aggregate budgets against process and kernel resources; each subscriber's outbound reservation counts against both its quota and the global pool so large subscriber counts cannot multiply buffering without limit.

Provide independent emergency reserves for service ACKs, FLOW frames, Raft heartbeats/elections and high-priority commands. BULK logging/compression cannot exhaust those reserves. Critical/durable traffic cannot depend on the writable socket buffer of a slow Q0 client.

**Capacity/admission contract (NFR-RES-006).** The basic capacity model MUST estimate at least:

```text
RAM_required >= N_conn * (user_conn + kernel_socket + TLS_state)
                + sum(subscription_state)
                + sum(outbound_reserved)
                + sum(inflight)
                + sum(shard_buffers)
                + allocator/page_cache_margin

NIC_egress >= sum(admitted_publications_per_second
                  * expected_actual_recipients
                  * wire_bytes_per_delivery)
```

CPU includes TLS, routing, fan-out and heartbeat. Disk/IOPS includes durable writes, fsync, cursor/ACK updates, replication, snapshots and replay. Account separately for offered/admitted/committed/delivered message and byte rates, queue age and durable backlog. Combined configured limits must not allow resource exhaustion before `PRESSURE`/`SHED` activates. M0 derives coefficients/safety margins and M1 validates them; they are not physical constants.

### 6.5. Durable log and Raft-group placement

Physical storage uses a shared StorageEngine/WAL abstraction but distinct contractual groups. **Q1 local durable-shards** contain one or more `(topic, partition)` and require local durability without Raft. **Q2 replicated durable-shards** aggregate several `(topic, partition)` in one Raft-committed log and acknowledge only durable quorum commit. A single logical partition cannot be served simultaneously as Q1 and Q2 or switched in place without an explicit migration. A Q2 WAL contains `record_version`, `shard_id`, `raft_term/index`, `topic_partition`, `logical_offset`, producer sequence, checksum, flags and commit/snapshot boundaries; local Q1 WAL shares the basic format but does not invent Raft terms/indices. On-disk versions/migration rules are fixed before M2/M4 recovery tests.

Group commit SHOULD be applied independently within each durability domain: sequential append+fsync/group commit for Q1, and append/replication+fsync+Raft majority commit for Q2. Local-only Q1 records MUST NOT be inserted as a non-quorum prefix in the Q2 Raft log. If the StorageEngine uses a shared physical file anyway, Q2's published committed prefix MUST remain self-contained and Q1-independent. Batching has time/resource limits, respects deadline-sensitive Q2 traffic and issues ACK only after the applicable barrier.

Raft safety metadata (term, vote, commit indices, membership) is persisted according to the protocol only in Q2 consensus domains. Q1 uses its local durable barrier and coordinated cluster ownership epochs without claiming Q2 guarantees. ACK/cursor/generation/terminal disposition durability MUST be no weaker than the corresponding partition (FR-GRP-006) and cannot be applied as committed before their own barriers. Coalesced records in the same durability domain are preferred. Alternative KV/side log requires evidence of correct recovered ordering/fencing across crash+snapshot+replay. No atomic transaction between original publish and later consumer ACK is required. Inter-shard DLQ uses a durable idempotent disposition protocol (§5.4.1), not a distributed transaction. Q1 owner loss cannot be hidden by continuing the prior log incarnation. Shared Q1/Q2 groups preserve FR-STO-010 isolation; test WAL+KV crashes as a single composition.

### 6.6. Transport separation

Mandatory P1 implementations: native TCP/TLS transport and a Transport interface for QUIC. Both expose identical message semantics; frame packaging adapts to streams/datagrams. TCP/TLS supplies a straightforward widely supported baseline. QUIC may improve mobility/reconnect and independent stream loss recovery, at additional CPU/packet/memory cost [E1].

QUIC DATAGRAM, if provided, may carry **only Q0** traffic where lack of transport retransmission is acceptable. DATAGRAM still follows congestion control and needs application memory limits [E4]. Q1/Q2 use reliable streams and explicit broker commit ACKs. Production default is selected through paired LAN, packet-loss, NAT/mobile and reconnect-storm tests.

### 6.7. Implementation evolution

Internal boundaries (`Transport`, `StorageEngine`, `Placement`, `Consensus`, `SubscriptionIndex`, `Scheduler`) MUST allow replacement without changing wire semantics or Q0/Q1/Q2 contracts. On-disk format changes are an exception, requiring controlled offline/online migration with schema version, checksum and rollback plan.

## 7. Security and failure safety

### 7.1. Authentication, authorization, identity

**SEC-001 (P0).** Production client and cluster connections MUST use TLS 1.3, including QUIC. Inter-node connections use mTLS, validating trusted CA/issuer and node identity. Plaintext is allowed only for `127.0.0.1` in an explicit development profile.

**SEC-002 (P0).** Support short-lived tokens/JWT with locally cached keys/JWKS and optional client certificates. Validate signature, `aud`, `iss`, `exp`, `nbf`, key rotation and tenant binding. Anonymous production publish/subscribe is prohibited.

**SEC-003 (P0).** Apply ACL to topic creation, publishing, subscription, replay, consumer-group management, priority changes, administration and metadata reads. Validate before expensive routing/durable append; never trust client-supplied tenant identity.

**SEC-004 (P0).** Protect against handshake floods, unauthenticated half-open sessions, reconnect storms, wildcard explosion, large numbers of tiny frames, oversized varints, duplicate IDs, slowloris, compressed-payload bombs in bridges and metric-cardinality attacks. CPU cost per unauthenticated connection, pending-handshake limits, TLS crypto queues and pre-auth memory MUST be included in the capacity model. Enforced limits MUST be observable and tested with handshake floods.

**SEC-005 (P0).** Parser/validation errors must not cause panic/undefined behavior, credential leakage, cross-tenant delivery or quota bypass. Secrets never enter logs, metrics or traces; payload logging is disabled by default.

**SEC-006 (P0).** Audit administrative changes, ACL changes, join/leave, forced repair, data deletion, certificate updates and durable-retention actions. Ordinary publishers must not be able to tamper with the audit log.

**SEC-007 (P1).** Rate limit by IP pre-authentication and by principal/tenant post-authentication; account for NAT so tens of thousands of legitimate clients behind one IP are not blocked without a distinct budget. Support allow/deny CIDR on control listener. The administrative listener requires separate authentication and authorization using mTLS or equivalent OIDC/TLS/RBAC. Reauthentication and credential/ACL revocation require a configurable maximum revalidation TTL, not indefinite trust in an old snapshot.

**SEC-008 (P1).** Reproducible builds, pinned dependencies, SBOM, vulnerability/secret scanning and an update policy are required. Unsafe native plugin loading in the data plane is prohibited without an explicit sandbox model.

**SEC-009 (P0).** Production administrative mutations MUST require fresh authority, confirmed identity and a functioning coordinated metadata plane. If an authoritative ACL revision is inaccessible or credentials/revalidation TTL expired, reject mutations regardless of data-plane health. Read-only diagnostics remain limited by valid authority. TTL values, JWKS update procedure and revocation policy MUST be configurable, documented and tested during issuer/metadata-quorum outages.

### 7.2. Threat model and correctness boundaries

Assume malicious/misbehaving clients, process crashes, sudden node power loss, packet loss, asymmetric partitions, full disks and stale node state. Standard Raft is **not** assumed to withstand Byzantine nodes, a simultaneously compromised majority or storage hardware that falsely acknowledges fsync. Those require separate safeguards and threat models.

### 7.3. Secrets and identifier policies

Configuration supports secrets in protected files or secret managers, hot certificate rotation without full restart where library capabilities permit, protection against dumping keys into core files and separation of metrics/admin endpoints. Retention for identifying client IDs/IPs and audit records is configurable; investigation-critical records reside in protected audit logs.

## 8. Non-functional performance targets and SLOs

### 8.1. Applicability conditions

The following values are **engineering target hypotheses** for development and acceptance, **not** measurements established in [S1]/[S2]. Before freezing version 1.1.1 acceptance criteria, perform M0 baselines on NATS and at least one MQTT-oriented broker under equal profiles. Physically unattainable or irrelevant thresholds must be documented before optimization and approved through a specification change that preserves raw results. Thresholds cannot be changed after a failed final test without a formal decision. All PERF-* numerical targets are carried forward unchanged from v1.0.

**H1 reference rig:** Linux x86-64; broker node with 16 dedicated vCPUs, 64 GiB RAM, NVMe SSD, dedicated 10GbE, pinned kernel version and CPU frequency/governor, no noisy colocated workload. Q2 uses three equivalent nodes on separate physical hosts, with inter-node RTT p50 ≤1 ms and p99 ≤3 ms absent injected faults. Generators run on separate machines and must stay at or below 60% aggregate CPU and NIC use during valid experiments. Measure server resources at both process and kernel/socket level; exclude exporter traffic. Attach hardware specifications, kernel tunables, NUMA/topology, dependency versions, certificates and generator seeds.

**Measurement:** at least 10 min warm-up, 30 min steady-state measurement, five independent repetitions, separate per-run results, equal payloads/auth/connections. Compute percentiles from raw distributions/histograms, never mean-of-percentiles. Fault scenarios use 30 repeats or a justified statistical precision.

M0/M1 MUST deliver a versioned capacity-model report covering connection overhead factors, fan-out, CPU/cryptography and durable-commit costs; RAM upper bounds including kernel sockets, NIC bytes, disk/IOPS, inflight/queues, reserves and low/high watermarks. Compare actual observations with predictions at independent load levels. If the model cannot explain observed saturation or admission permits OOM, false ACK or unbounded queues, M1 is FAIL. This does not authorize arbitrary changes to PERF targets.

### 8.2. Load profiles and preliminary acceptance criteria

| ID | Workload | Target result on H1 | Mandatory check |
|---|---|---|---|
| PERF-D1 | 50,000 TLS sessions/node; 90% idle; 10% publish 1 msg/s; 256 B; fan-out ≈1; Q0 | Delivery E2E p99 ≤15 ms; p99.9 ≤50 ms; stable for 30 min | No runaway RAM, long pauses, overheating or unprovoked repeated handshakes |
| PERF-D2 | 50,000 sessions, 10,000 active publishing 10 msg/s, 256 B, fan-out ≈1, Q0 | 100,000 publications/s; p99 ≤25 ms; bounded queues | Determine saturation point; test invalid if generator/NIC is the limiting resource |
| PERF-D3 | Three nodes × 50,000 sessions, 90/10% mix, 256 B, Q0, appropriately distributed topics | ≥150,000 simultaneous cluster connections; unaffected partitions remain available | Independently reconcile handshakes/connected clients, not merely open FDs |
| PERF-R1 | RF=3 Q2, 10,000 publications/s, 1 KiB, stable NVMe, fan-out ≈1 | Committed PUB_ACK p99 ≤75 ms; p99.9 ≤200 ms | ACK only after durable quorum, including fsync and replication costs |
| PERF-P1 | 80% previously measured sustainable capacity, mixed Q0+Q2, 5% CONTROL | CONTROL p99 ≤2× its own p99 at 30% load; no unbounded growth | Fairness, bounded BULK, no hidden priority starvation |
| PERF-M1 | 5,000 → 50,000 idle TLS sessions, measure resource slope | Incremental process RAM plus kernel socket state ≤96 KiB/idle client; idle CPU ≤2 vCPUs at 50k | Include per-connection buffers and allocator fragmentation |
| PERF-C1 | Open-loop start of 10,000 independent TLS/auth handshakes over 60 s at uniform arrival rate, with 50,000 established sessions; observe through 70 s | ≥95% of all 10,000 attempts finish handshake+`AUTH_OK/WELCOME` within 10 s of their individual `start_attempt`; failures/timeouts count in denominator; no OOM | Arrival schedule, separate success/failure fractions, p95 handshake duration, rate limiting and impact on critical traffic |

**E2E definition for PERF-D1/D2:** from completion of logical message send by the producer SDK until receipt of the complete payload by subscriber SDK; excludes application processing. Use synchronized clocks (chrony/PTP with recorded max skew) or one-way measurement with an agreed monotonic reference. Uncontrolled clock skew invalidates E2E results. Also measure ingress→dispatch, queue wait, write wait and egress send separately. PUB_ACK latency is SDK send to receipt of broker acknowledgment.

**PERF-C1 clarification:** the 10-second condition applies separately to each attempt, from scheduled `start_attempt` through successful TLS and protocol handshake/authentication. It does **not** refer to completion of the entire 10,000-client batch. Last attempts start no later than t=60 s and outcomes are observed through t=70 s. Generator-internal queuing must not delay the defined start. Failed/rejected/timed-out attempts remain in the denominator of 10,000, of which at least 9,500 must meet the 10-second bound.

**Sustainable capacity:** maximum offered rate under which queues do not grow without bounds for ≥30 min, rejection ratio follows explicit policy and target p99 is met. Instantaneous peak messages/s without steady state is not sustainable throughput.

**Load and saturation sweep:** test 30/50/70/80/90/100/110% of measured sustainable capacity. Beyond saturation the broker may reject new Q0/durable publishes per contract; it cannot hang, grow memory unboundedly, send false ACK or suffer cascading crashes. Report p99 for successfully admitted messages **and** reject/drop rates to prevent fake p99 improvement from hidden shedding.

**VAL-01 — comparative value, mandatory architecture gate.** In addition to absolute PERF targets, formulate at M0 and verify at M5 at least one measurable advantage over the most relevant baseline with equivalent guarantees. Initial hypotheses: either ≥25% more sustainable connections at the same p99 bound, **or** ≥20% lower CONTROL p99 at equal mixed traffic and quotas. These are not claimed achievements. A negative result triggers architecture/product-specialization reconsideration; it cannot be called success by using incomparable profiles. Publish CPU, RAM, throughput, recovery and operability regressions alongside improvements. A result is not convincing when obtained through weaker guarantees, authentication or hidden drops. Before final comparative tests, preregister an immutable protocol including per-baseline semantic-equivalence rubric: Q0/Q1/Q2 class and ACK meaning; durability boundary/flush; RF/quorum/failure assumptions; consumer ACK/replay/retention/TTL; auth/TLS; fan-out recipients/topic distribution; offered/admitted/committed/delivered msg/s and bytes/s; rejections/drops; hardware/OS/network/generator; and load profile. Record meaningful differences in a semantic-delta report. VAL-01 PASS is forbidden if an advantage results from weaker semantics, fewer actual deliveries or hidden throttling.

### 8.3. Reliability and recovery

| ID | Scenario/metric | Target condition |
|---|---|---|
| REL-01 | Q2 RF=3, forced crash of one node after ACK | Zero acknowledged records lost, subject to durable quorum assumptions, verified by oracle |
| REL-02 | Q2 leader change with two healthy remaining nodes | p95 time to resume acknowledged writes ≤15 s, p99 ≤30 s (30 runs) |
| REL-03 | Q2 quorum loss | No new `COMMITTED_QUORUM` before quorum returns; explicit `NO_QUORUM`/timeout |
| REL-04 | Q1 process crash with intact local disk | Zero lost Q1 records for which `COMMITTED_LOCAL` was sent |
| REL-05 | One million Q1/Q2 messages, reconnect/retry, lost ACKs | No acknowledged losses or forbidden duplicate durable writes within dedup window; consumer redelivery follows at-least-once |
| REL-06 | Cluster topology change/drain | No two active owners with same epoch on completion; log and cursor invariants preserved |
| REL-07 | Disk full, checksum corruption, fsync EIO | Fail-closed durable ACK, diagnosable error and safe recovery procedure |

Do not set “absolute availability 99.99%” as a provable short laboratory test: 30-day availability depends on network, operations, nodes and error budget. A post-pilot operational SLO of 99.9% over 30 days for Q0 ingress is proposed, but is not part of initial laboratory acceptance and does not imply Q2 operation without quorum.

### 8.4. Resource use and limits

**NFR-RES-001 (P0).** All mutable buffers are bounded. Bounds aggregate per-connection, tenant, priority, partition and cluster-wide budgets; RAM budget must reject work before OOM. Memory metrics cover RSS/PSS, live heap, allocator-retained memory, page cache, kernel socket memory and FDs. The model MUST additionally account for per-subscriber egress reservations, estimated fan-out, retention backlog, dedup state, WAL group commit and capacity reserve for one failed cluster node.

**NFR-RES-002 (P0).** Sustainable traffic MUST NOT lead to uncontrolled growth in FDs, tasks, timers, queues, subscriptions, shard count or metrics series. Measure `ΔRAM/ΔN`, `ΔCPU/ΔN` with fixed traffic profile.

**NFR-RES-003 (P0).** Below stated capacity, p99 must not show recurring pathological drops caused by retention, compaction, snapshot or rebalance. Export separate latency histograms for each background activity.

**NFR-RES-004 (P1).** For high-fan-out traffic, publish ingress/egress bytes, messages and number of physical recipients. Comparing only publish rates while actual deliveries differ is invalid.

**NFR-RES-005 (P1).** Priorities extend beyond application queues: Linux socket send buffers, Tokio scheduling, storage I/O QoS, NIC saturation and TLS crypto tasks must be budgeted or explicitly documented as non-isolatable limits.

**NFR-RES-006 (P0).** Before M1 completes, publish a reproducible capacity model linking connection/publisher/subscriber counts, fan-out and message sizes to CPU, process+kernel RAM, NIC ingress/egress, disk/IOPS/fsync, durable backlog, inflight operations, queues/buffers and overload thresholds. It MUST derive admission-rate limits, quotas/reserves, and explain measured versus predicted capacity/latency across several profiles; revise incorrect predictions with documentation. Admission MUST activate before OOM, uncontrolled queue growth or P0 invariant violations. Auto-tuning is not mandatory.

### 8.5. Observability and diagnostics

**OBS-001 (P0).** Export via Prometheus or OTLP: active/idle connections, attempted/successful handshakes, reconnects, publications admitted/rejected/committed, delivery attempts, dropped/expired/retried messages, per-class E2E/queue/commit latency histograms, outbound bytes, queue depth/age, lag, fsync p99, Raft commit/index/term/leader changes, snapshot/compaction duration, RAM, CPU, NIC and error codes. Required additional data: physical fan-out multiplier, per-subscriber egress-queue occupancy, per-topic/tenant ingress/egress rates and causes of fan-out rejections/drops **without per-client metric-label cardinality**.

**OBS-002 (P0).** Default metric labels are bounded: node, transport, lane, QoS, priority, status and bounded topic group. Raw `client_id`, `message_id`, arbitrary topic, `trace_id` and IP are prohibited as labels because they can overwhelm metrics for 100k clients.

**OBS-003 (P0).** Structured logging; trace context propagates through envelope. Full tracing of every message is disabled under load. Use sampling and exemplars/correlation. Measure monitoring overhead enabled/disabled so it cannot hide critical-path interference.

**OBS-004 (P1).** Provide dashboards: Connections & Admission, Latency by Class, Quorum & Durability, Queue & Backpressure, Fan-out & Egress, Partition Map/Rebalance, Kernel & NIC. Every alert has a diagnostic runbook.

**OBS-005 (P1).** Maintain stable internal metric naming, mapping to evolving OpenTelemetry messaging conventions [E6] as useful without tying compatibility to an unstable standard.

## 9. Testing and verification

### 9.1. Initial P0 experiment: required before selecting transport and storage

Build a standalone Rust benchmark client simulating 1:1, 1:N, N:1, N:N, bursts, 90–99% idle, fast/slow consumers, Q0/Q1/Q2, reconnects, packet loss, churn, topology changes, IPv4/IPv6, TLS authentication and wildcard depth. Use actual independent network sessions/identities with reproducible rate control, not merely virtual users inside one process.

**M0-A — Transport.** Compare native secure TCP/TLS and QUIC with identical profiles. For QUIC additionally vary stream count, buffer limits, Q0 datagrams, mobility, RTT, packet loss 0/0.1/1/3%, and bandwidth. Report CPU/1k connections, per-connection memory including kernel, p99/p99.9, handshake success and bytes/message. Select default transport; retain the alternative as a tested adapter. Publish ADR-001 criteria *before* testing: choose a feasible transport meeting security/correctness/resource budgets with the best p99/p99.9 on a preregistered weighted target workload. If uncertainty intervals overlap, SHOULD prefer lower complexity and per-connection cost. Record physical connection count, kernel UDP/TCP buffers and NAT/mobile assumptions. Do not apply arbitrary fixed percentage decision rules.

**M0-B — Storage.** Compare local KV engines and WAL implementations on fsync latency, page-cache pressure, crash recovery, write amplification, compaction, stable disk format and corruption behavior. Select ACID KV only after load and failure experiments, not insertion-throughput benchmarks alone. Include dedup memory/recovery at high producer cardinality (incremental high watermark + bounded outcomes vs per-record cache, KV/hybrid) and combined WAL/KV crash. A two-tier store is **not** mandatory.

**M0-C — Cardinality and placement.** Analyze topics, logical partitions, durable shards, groups, subscribers per topic and tenant. Measure per-Raft-group cost (CPU/RAM/timers/fsync), neighbor-partition p99 under hot partitions, shards/node, partitions/shard, recovery/catch-up/snapshot time and cost of metadata Raft changes. Compare at least uniform and skewed/hot-key loads, several shard groupings and a controlled sequence of topic/ACL/placement mutations (including 1/10/100 ops/s as **experimental points, not product limits**). Publish a Pareto frontier for fewer groups versus cross-partition interference and recovery; choose shard-count range and isolation rules from data rather than predetermined partitions/shard.

### 9.2. Baseline implementations

Reference configurations: NATS Core for Q0; NATS JetStream for comparable Q1/Q2 modes; EMQX or Mosquitto for dense MQTT pub/sub; RabbitMQ quorum queues/streams for durable groups when comparable. Kafka is a separate throughput/replay baseline and MUST NOT be presented as equivalent for 100k individual IoT clients. Reports MUST describe semantic differences, especially publish ACK, replication factor, consumer ACK, flush, TTL, authentication and compression.

Every comparison uses equal hardware class, broker node count, payload, TLS, publisher rate, fan-out topology, client count, accepted semantics, warm-up, duration and generator resources. Report latency-versus-connections, throughput-versus-connections, resource slope, failover-versus-load and an honest performance envelope showing both wins and losses. Preregister a semantic equivalence rubric and semantic delta report; differing guarantees cannot be disguised by labeling both systems Q0/Q1/Q2. If equivalent semantics cannot be established, report only a limited observation, not VAL-01 PASS.

### 9.3. Test levels

**TEST-001 (P0), unit.** Wire parser, limits/varints, error codes, hash stability, ACL matching, quotas, TTL, priority scheduler, dedup, retention, reconfiguration state machine. Property-based tests for arbitrary operation sequences.

**TEST-002 (P0), concurrency.** Loom or equivalent deterministic models for critical shared structures, concurrent subscribe/unsubscribe, ACK generations and ownership fencing. Cover all invariants with assertions.

**TEST-003 (P0), integration.** End-to-end Q0/Q1/Q2, producer retries, durable cursors, join/leave, snapshot reset, rolling upgrade, downgrade prevention, security policies, special characters and interoperability across minor versions. Q1 and Q2 fault profiles use separate immutable `durable_class` partitions; mixed Q1/Q2 publish to one partition is rejected before admission (TEST-017).

**TEST-004 (P0), crash consistency.** Repeated SIGKILL/power-cut simulation at every append/fsync/commit/ACK/snapshot/compact/rotate boundary, reconciled against an external oracle. `kill -9` is not power-loss simulation and does not replace testing flush semantics.

**TEST-005 (P0), fault injection.** Toxiproxy/tc/netem plus host constraints: 0–10% packet loss, jitter, reordering, asymmetric partition, blackhole, stale DNS, one-way failure, 100–500 ms RTT, throttled CPU/disk, ENOSPC, EIO, clock skew, simultaneous node failure and reconnect storms.

**TEST-006 (P0), fuzz.** Continuous coverage-guided fuzzing of wire parser, TLS/QUIC application framing, compression bridge, topic parser, WAL recovery, snapshot decoder and metadata operations. Sanitizers/Miri wherever compatible with Rust code.

**TEST-007 (P0), load.** PERF-D1…PERF-C1; raw histograms, CSV/JSON events, resource snapshots, dashboard exports and deterministic generator configuration.

**TEST-008 (P1), soak.** At least 72 hours of dense mixed traffic with periodic certificate rotation, retention/compaction, rolling restarts and single-threaded long-tail spikes; RAM stabilization without monotonic leakage, acknowledged data preserved.

**TEST-009 (P1), security.** Penetration tests against parser/ACL, credential expiry, DoS, 0-RTT replay, unauthorized priority, cross-tenant leakage, administrative isolation, secret exposure and trust-root updates.

**TEST-010 (P1), deployment.** systemd service, read-only configuration, container health probes, graceful shutdown, upgrade by one minor version, rollback and backup restoration.

**TEST-011 (P0), durable consumer ACK and terminal cursor.** Inject crashes between ACK_RECEIVED, ACK_VALIDATED, fsync/quorum and result; lose ACK_RESULT, retry idempotent ACKs and use stale generations. NACK/timeout/MAX_RETRY cannot advance cursor. Check out-of-order ACK, committed QUARANTINED, DLQ_PENDING, Q1/Q2 DLQ durability no weaker than source, crashes before/after DLQ commit and terminal disposition, DLQ outage, MANUAL_REPLAY and administrative SEEK. The oracle verifies contiguous completed prefix. Terminal offsets close only on durable disposition; a missed barrier permits redelivery but never skipping.

**TEST-012 (P0), mixed durable-shard.** Independent retention/ACL/quota/offsets in shared WAL; hot partitions beside CONTROL; snapshot/compaction/seal/corruption/migration, lagging groups and EIO. Verify committed prefix remains unchanged and no cross-tenant data leakage occurs.

**TEST-013 (P0), control-plane degradation.** Metadata voter/quorum loss, stale-but-valid snapshots, expired credentials/ACL/JWKS, frozen ownership epoch, live/dead shard quorum, Q1 storage failure, reconnect and security mutation. For every FR-CLU-011 matrix cell, record allow/deny, error code and absence of unsafe mutations.

**TEST-014 (P0), capacity/admission/fan-out.** Independent CPU/RAM/kernel-NIC/disk budgets, 1:N growth, per-subscriber slow consumer, exhausted topic/tenant/global egress, bursts above sustainable capacity, hysteresis thresholds, unavailable exporter, no-OOM/no-unbounded-queue and predicted-versus-measured resource slopes.

**TEST-015 (P0), producer dedup.** Q1/Q2 retries with identical/conflicting payload, out-of-order sequences, gaps, producer-epoch fencing, beyond-window replay, clock skew, crashes/leader changes, producer-cardinality memory ceiling and durable recovery without second committed record within the window.

**TEST-016 (P0), baseline semantic parity.** Automated audit of preregistered comparison rubric and fixture; offered/admitted/committed/delivered accounting, RF, fsync, authentication, fan-out, consumer ACK and reject/drop policies. Semantic differences invalidate claimed VAL-01 advantage or require a limited finding.

**TEST-017 (P0), Q1/Q2 partition durability.** Interleaved Q1→Q2→Q1 requests in one logical partition, including concurrent producers, MUST reject incompatible `durable_class` before append/offset without silent upgrade/downgrade. Q1 standalone/clustered must recover after process crash with intact local WAL; irrecoverable owner loss must not reuse old log incarnation. Q1 migration/drain tests transfer/fencing. Q2 follower election, snapshot and replay preserve only the committed Raft prefix, independent of Q1 local records. In-place Q1↔Q2 policy change without proven migration is rejected. External oracle compares offsets, storage headers and PUB_ACKs. Multi-partition Q1/Q2 tests maintain distinct durable streams.

### 9.4. Independent reliability oracle

An independent generator MUST maintain on a separate healthy host a journal of `(producer, seq, message_id, checksum, send_time, received_pub_ack, qos, expected_partition)`. The consumer maintains `(message_id, offset, group_generation, deliveries, ACK_result)`. After each test reconcile acknowledged publications, actual committed records, observed delivery/ACK, lost/extra/duplicate records and uncertain outcomes. Equal sent/received counters alone do **not** prove zero loss: use set comparison and content checks. The oracle also records consumer ACK requests/`ACK_COMMITTED` responses, recovered durable generation, terminal DLQ state, immutable dedup outcomes and times when reserved resource budgets were breached.

### 9.5. Statistical methodology

Retain histogram buckets or HDR Histograms accurate enough for p99.9. Publish min/mean/median/p95/p99/p99.9/max, repeat-distribution confidence intervals, rejected/dropped throughput and per-stage latency budgets. Control alternative explanations across brokers: socket-buffer settings, kernel backlog, Nagle/delayed ACK, TLS ciphers, CPU turbo/governor, NIC IRQ affinity, NUMA, container NAT, disk write caching and actual RF.

### 9.6. Minimum failure matrix

This matrix covers data-plane failures and is supplemented by normative control-plane matrix FR-CLU-011 (§4.8). For combined faults, apply the most restrictive condition. After Q2 quorum loss, reading may continue only from provably safe previously committed snapshots, never as a new commit ACK. Q1 owner loss invokes the log-incarnation boundary (FR-STO-003); Q2 committed prefixes cannot include non-quorum Q1 entries (TEST-017).

| Scenario | Expected Q0 | Expected Q1 | Expected Q2 |
|---|---|---|---|
| Broker process crash | Inflight messages lost; flow resumes after reconnect | All `COMMITTED_LOCAL` records recover on intact disk | All `COMMITTED_QUORUM` records available after leader election |
| Complete loss of one node | Clients migrate to other owners; node's volatile messages may disappear | `COMMITTED_LOCAL` may be unavailable/lost with local disk; without recoverable copy, old log incarnation is not continued and replay explicitly reports break/new incarnation | Remaining 2/3 quorum operates, prior commits preserved |
| Quorum loss | Remaining local Q0 subscription may continue under established mode | Q1 with valid ownership and healthy WAL can continue without Q2 data quorum; loss of metadata authority limits continuation to valid epoch/ACL lifetime | No new durable commits; fail closed |
| Disk full | Q0 can continue while resources permit; independent of WAL | Reject durable publishing without false ACK | Affected shard degrades; quorum accounts for healthy replicas |
| Network reorder/duplicate | Best-effort disruption and reconnect permissible | Idempotent publisher detects duplicate within window | Replication/epochs prevent double commit and split-brain |
| Slow subscriber | Drop/disconnect only that subscriber, isolate others | Backpressure/group lag or separate group quota | Likewise; one consumer cannot block all publishers |
| Cluster membership change | Temporary local delays possible by policy | Q1 staged transfer requires preserved local WAL/snapshot and fencing; missing history cannot silently become a continuous new offset stream | Transfer, leader fencing and committed prefix preservation |

## 10. Acceptance, mandatory artifacts, and Definition of Done

### 10.1. Functional acceptance

Before M1 completion, maintain a versioned machine-readable traceability matrix for every `FR-*`, `SEC-*`, `NFR-*`, `OBS-*`, `REL-*`, `INV-*` and applicable `PERF-*`:

```text
requirement_id -> test_id -> fixture_id -> exact_command ->
evidence_URI/hash -> status(NOT_RUN|PASS|FAIL|WAIVED) ->
commit_SHA/environment_fingerprint
```

Every change request updates matrix and checks. An orphan P0 requirement blocks acceptance. Final report gives PASS/FAIL/WAIVED with rationale, commit/version, configuration, exact hardware and raw artifacts. P0 requirements **cannot** be WAIVED for production acceptance. Documented P1/P2 waivers do not substitute for required functional evidence.

### 10.2. Data and fault-tolerance acceptance

Production-ready status requires: (1) no test finds a false Q1/Q2 PUB_ACK; (2) no uncommitted record is suddenly presented as committed after leader change; (3) no concurrent active owners for one generation/epoch; (4) REL-01…07 pass; and (5) protocol parser/ACL security review passes. Performance PASS without correctness PASS is invalid. Additionally TEST-011/017 MUST demonstrate that terminal disposition never skips unfinished offsets, Q1 is not represented as Q2, and Q2 does not rely on Q1 local-only state.

### 10.3. Performance acceptance

For every PERF profile, publish all raw metrics, five repetition results and baseline. Compare equivalent guarantees. If latency is met only through undeclared drop/reject or lower QoS, the test FAILS. PERF-D* requires actual connected clients and subscription activity, not just an FD count. Average latency cannot replace required percentiles.

### 10.4. Delivery package

- Server and SDK source code with reproducible builds; license and dependency policy.
- Native protocol v1 specification, error model, wire fixtures, compatibility matrix, topic and guarantee contracts; versioned error registry, `CONSUMER_ACK_RESULT`, protocol golden fixtures, DLQ/QUARANTINED/replay state definitions.
- Architecture documentation and ADRs explaining choices/rejections of QUIC, KV, scheduler, hashing and Raft shards.
- On-disk format v1, migration, retention, backup, recovery and power-loss guarantees.
- CLI/admin API, OpenAPI, secure example single-node and three-node configurations.
- Test suite, chaos harness, benchmark generator, baseline comparisons, raw data and engineering report, including tested FR-CLU-011 matrix, capacity-model report, VAL-01 semantic-equivalence rubric/delta report, TEST-017 evidence of Q1/Q2 class policy and DLQ/terminal cursor oracle.
- Dashboards, alert rules, runbooks and SLO, upgrade/downgrade/drain/disaster-recovery operations guide.
- SBOM, dependency audit, threat model, security-test report and independent review results.

## 11. Architectural risks, bottlenecks, and mitigations

| ID | Bottleneck / risk | Symptom | Mitigation / target test |
|---|---|---|---|
| R-01 | State and buffers for 100k connections | RAM/FD/kernel socket exhaustion | RAM slope per connection, bounded idle state, kernel memory profiling, PERF-M1 |
| R-02 | TLS/QUIC CPU on small messages | CPU-bound before network saturation | TCP/QUIC A/B, TLS cipher comparison, batching without p99 regression |
| R-03 | Wildcard index and fan-out explosion | O(subscriptions) work per publish, egress saturation | Topic trie/RCU, wildcard quotas, node-local fan-out, outbound byte accounting |
| R-04 | Slow consumer | Unbounded queues and delays for everyone | Credits, per-subscriber buffer, drop/disconnect or durable lag, backpressure |
| R-05 | Global mutex/central dispatcher | p99 spikes, poor NUMA locality | Key/core sharding, lock profiling, no global write lock on hot path |
| R-06 | Timer/heartbeat storm | Excessive wakeups/context switches | Timer wheel, coalescing, activity profiles, PERF-C1 |
| R-07 | Incorrect ACK semantics | False durable success, data loss | Precise commit barriers, external oracle, crash injection |
| R-08 | Excessive Raft groups | Memory, elections, fsync storms | Durable-shard aggregation, bounded groups, M0-C |
| R-09 | Split-brain with stale routing map | Dual leaders, duplicate commit | Epoch/fencing, quorum ownership, partition tests |
| R-10 | Fsync spikes/ENOSPC | Q2 p99/failed commits | NVMe qualification, group-commit budget, fail-closed, disk quotas |
| R-11 | WAL/snapshot corruption | Inconsistent offsets, replay loss | Checksums, transactional snapshots, oracle-based recovery, fuzzing |
| R-12 | KV store compaction | Unpredictable tail latency | Storage off hot path, redb/RocksDB A/B, compaction limits |
| R-13 | Rebalance storms | Mass redirects and replays | Rate-limited staged migration, ownership epochs, loaded tests |
| R-14 | Burst/drop distorts latency | Fake p99 gains | Publish rejects/drops alongside latency; offered vs admitted accounting |
| R-15 | Unsynchronized clocks | Invalid E2E latency, false TTL outcomes | Monotonic broker TTL, sync error tracking, separate timing domains |
| R-16 | Priority spoofing/starvation | CONTROL DoS or starvation | Server-authorized classes, weighted fairness, reserved class budgets |
| R-17 | Mass reconnect behind NAT | Acceptance bottleneck, unfair rate limiting | Principal quotas, pre-auth safeguards, jitter/backoff |
| R-18 | QUIC 0-RTT replay | Duplicate commands/ACKs | 0-RTT writes prohibited [E2] |
| R-19 | Metrics-cardinality explosion | Monitoring/broker OOM | No per-client labels, sampling, OBS-002 |
| R-20 | Meaningless broker benchmarks | Product value cannot be proven | Equal semantics/RF/TLS, independent harness, all raw data |
| R-21 | Excessive generality | Delayed product readiness | Clear MVP exclusions, ADR against premature bridges/SQL |
| R-22 | Coupling Q0 to durable path | Unexpected fsync stalls for critical telemetry | Independent queues/workers; soak during compaction |
| R-23 | Guarantees without quorum | False quorum ACK | Read-only state and split-brain invariant tests |
| R-24 | Retention vs durable group lag | Unread data loss, full disks | Explicit retention modes, lag alerts, hard-limit behavior |
| R-25 | Mixed-QoS global ordering complexity | Unpredictable event order | Independent lanes/sequences; explicitly no cross-lane ordering |
| R-26 | Huge durable groups / high ACK rate | Control metadata write amplification | Group quotas, coalesced acknowledged progress, same shard log where suitable |
| R-27 | Commodity/virtualized I/O | External delays prevent SLO compliance | Qualify hosts, disk sync tests, dedicated NIC/CPU, invalidate flawed measurements |
| R-28 | Correlated host/rack failures | RF=3 not sufficient | Fault-domain replica placement, disaster-recovery documentation |
| R-29 | ACK commit/cursor write amplification | High ACK rate delays Q2 and overloads WAL | Coalesce only contiguous completed prefix; fsync/p99 measurements; TEST-011 |
| R-30 | Hot partition in shared durable-shard | Noisy-neighbor/retention cross-impact | FR-STO-010, M0-C hot-key experiments, isolate CONTROL as necessary |
| R-31 | Control-plane outage plus expired security snapshot | Unsafe trust in stale ACL/owner | FR-CLU-011, SEC-009, expiry/reconnect chaos TEST-013 |
| R-32 | Unbounded dedup/producer state | OOM or incomplete dedup after crash | Quotas/high-water state, bounded window, TEST-015, M0-B |
| R-33 | Incorrect capacity model or limits | OOM/egress collapse before admission rejection | NFR-RES-006, early thresholds, TEST-014 |
| R-34 | Shared Q1/Q2 offset namespace and false continuity after Q1 loss | Unrecoverable Q1 gap, reused offset or Q2 depending on local-only prefix | FR-QOS-007, FR-STO-003, FR-CLU-004/005, INV-16, TEST-017 |

## 12. Implementation plan, milestones, and transition gates

The order first establishes correctness and architectural efficiency, then adds distributed complexity. A milestone advances only after its exit criteria pass. No calendar schedule is assumed without staffing/hardware estimates.

| Stage | Scope | Deliverable | Exit gate |
|---|---|---|---|
| **M0 — research baseline** | Reproducible test rig and generator; NATS/MQTT baselines; TCP vs QUIC spike, KV/WAL spike, capacity model | ADR-001…005, raw benchmarks, preregistered semantic rubric, preliminary capacity model including kernel memory/egress, M0-A/B/C conclusions | Reproducible rig, parity rubric; transport/KV/shard selection methodology frozen before evaluation |
| **M1 — protocol & single-node Q0** | Wire v1, TLS, auth, topic index, pub/sub, Q0, bounded buffers, TTL, priority, Rust SDK | Working Q0 broker, published wire/error registry/golden vectors, requirement→test matrix, calibrated capacity model | Wire fuzz, SEC P0, no-OOM, 10k connections smoke, Q0 E2E, TEST-014; fixtures for all M1 P0 requirements |
| **M2 — Q1 durability** | Local segmented Q1 WAL, fsync, recovery, replay, idempotent producers, durable groups/ACK; local WAL aggregation without Raft; immutable partition class | Reliable standalone broker | Crash/power cut, ACK/terminal cursor TEST-011, dedup TEST-015, Q1 local/no-Raft and class policy TEST-017, no false local ACK, oracle PASS |
| **M3 — cluster routing** | Metadata consensus, placement, epochs, join/leave, forwarding, incremental migrations | Three-node Q0/Q1 cluster; Q1 remains exclusively locally durable with ownership fencing and explicit lost-history/incarnation semantics | No stale-map loops/dual ownership; TEST-013 CP degradation, TEST-017 Q1 owner loss/migration, rebalance PASS |
| **M4 — Q2 replication** | Only Q2 replicated Raft-shards, durable-majority committed prefix, leader failover; Q1 remains local | Replicated delivery | REL-01…07 PASS, TEST-011 terminal states, TEST-012 isolation, TEST-017 Q2 prefix and mixed-class rejection, no-quorum fail-closed |
| **M5 — latency & density** | Profiling, scheduler isolation, QUIC decision, batching, memory/FD/NUMA, fan-out | Performance release candidate | PERF-D1…PERF-C1 on H1, TEST-014/016, fair comparable baseline + semantic delta and VAL-01 |
| **M6 — productionization** | Admin API/CLI, backup, rolling upgrades, security audit, diagnostics, 72h soak, dashboards | Production release candidate | No critical security/correctness findings, TEST-008…010 PASS |
| **M7 — independent acceptance** | Cross-version testing, raw-artifact audit, repeat fault matrix, final ADR/report | Evidence-backed release v1.0 | Traceability PASS with fixture/command/evidence, independently reproducible raw evidence, documented limitations and sign-off |

A reduced research MVP is M0+M1+M2: an honest experimental prototype without claims of distributed durability. M4 is required before claiming “cluster-guaranteed delivery”; M5/M6 before claiming production readiness. M2 proves local Q1 only. M3 does not magically replicate Q1 or provide Q2 fault tolerance.

### 12.1. Initial project structure

Suggested repository layout; actual names may differ as long as module boundaries are retained:

```text
broker/
  crates/
    protocol/          # wire types, parser, errors, vectors
    transport/         # TCP/TLS; QUIC adapter
    auth/              # identity, ACL, token validation
    router/            # topics, subscriptions, placement cache
    scheduler/         # admission, quotas, priorities
    volatile/          # Q0 dispatch and fan-out
    wal/               # segmented journal and recovery
    metadata/          # local KV abstraction, snapshots
    consensus/         # metadata and durable-shard Raft integration
    durable/           # Q1/Q2 state, dedup, cursors
    broker/            # composition and service lifecycle
    admin/             # HTTPS API, CLI
    observability/     # bounded metrics, traces, logs
  sdk/rust/
  sdk/python-or-go/
  tests/{contract,chaos,soak,security}/
  benchmarks/{generator,profiles,baselines,analysis}/
  docs/{protocol,architecture,adr,operations,runbooks}/
  deploy/{systemd,container,kubernetes}/
```

Crate boundaries must not introduce extra network RPC within a node or unnecessary serialization. Do not insert a plugin runtime or general workflow engine in the critical path before requirements justify it.

### 12.2. Development and CI rules

Any wire, WAL, Raft, ACL, routing or QoS change requires unit/contract tests, relevant property/fuzz/chaos subsets, independent invariant review and versioned ADR. Every PR need not run multi-day suites: fast correctness checks run on PRs, extended regression/performance for hot-path changes, periodic full soak. Build release artifacts only on release branches/tags, not every merge. Benchmarks attach exact commit SHA and environment fingerprint. Protocol registry, degraded-mode matrix and traceability matrix are version controlled. P0 changes suspend affected release gates until reverified.

### 12.3. Requirements change management

M0 evidence may justify choosing QUIC rather than TCP, replacing redb, changing subscription indexing, scheduler or shard-group count. P0 invariants remain inviolable: no false durable ACK, bounded memory, tenant isolation, no silent QoS downgrade, no concurrent shard owners and verifiable outcomes.

Changes require an ADR plus change request: original requirement → measured problem → alternatives → measured costs → decision → updated tests. This prevents technology-driven optimization and allows evidence-based deviation from [S2]. Every numeric budget/target change includes M0/M1 measured rationale, old/new values and compatibility consequences. Reviewers' arbitrary proposed limits do not become normative automatically.

## 13. Traceability from research to implementation

| Evidence | Requirement / refinement | Verification |
|---|---|---|
| [S1, pp. 12–14, 23–25] — p95/p99 and throughput | §8.1–8.2, E2E and admitted semantics | PERF-D*, PERF-R1, raw histograms |
| [S1, pp. 18–22] — 1:N, N:1, N:N and failure models | UC-01…06, FR-TOP, FR-CLU | TEST-005/007, fan-out tests |
| [S1, pp. 25–33] — CPU/RAM/NIC/disk, degradation | NFR-RES, FR-PRI, R-01…28 | PERF-M1, saturation sweep, soak |
| [S2, pp. 6–11] — flexible guarantees, deadlines, jitter | FR-QOS, FR-TIM, class scheduler | Protocol invariants, PERF-P1 |
| [S2, pp. 12–17] — hybrid storage/routing | Q0 volatile lane and Q1/Q2 as distinct per-partition guarantees; no Q1-only data in Q2 committed prefix | TEST-003/017, REL-01/04, durable-offset/incarnation oracle |
| [S2, pp. 18–30] — node components, backpressure, clustering | §6, FR-STO, FR-CLU | Cluster chaos, WAL crash, no-OOM |
| [S2, pp. 31–39] — Rust/Tokio/QUIC/FlatBuffers/sled/Raft | §2, §6, M0 ADR | Paired benchmarks, explicit changes |
| [E1]/[E2]/[E4] — QUIC, 0-RTT and DATAGRAM | FR-CON-007, SEC, adapter | QUIC loss/replay/flood tests |
| [E3] — bounded dedup and consumer ACK | FR-QOS-002…005 | Retry/ACK-outcome oracle |
| [E5] — crash-safe KV | Storage API/metadata trial | M0-B crash/snapshot tests |
| [E6]/[E7] — monitoring/security | OBS, SEC | Cardinality and cross-tenant security tests |
| [R1] — consumer ACK/cursor races | FR-GRP-006, FR-GRP-003, INV-13, §5.4.1: ACK or committed terminal disposition | TEST-011 ACK/DLQ/quarantine/SEEK, crash/lost-ACK oracle |
| [R1] — shard isolation/retention | FR-STO-010, FR-CLU-004, INV-14 | TEST-012, M0-C hot-partition/compaction |
| [R1] — degraded control plane | FR-CLU-011, SEC-009, §4.8 matrix | TEST-013, expired ACL/lease/quorum faults |
| [R1] — fan-out and admission capacity | FR-TOP-008, NFR-RES-006, §6.4 | TEST-014, PERF-M1, egress accounting |
| [R1] — bounded dedup identity | FR-QOS-004 | TEST-015, M0-B, dedup failover |
| [R1] — semantic baseline | VAL-01, §9.2, §10.1 | TEST-016, preregistered rubric/delta |

## 14. Example minimal v1 configuration (invariant schema)

This is a **normative illustration of configuration structure**, not a promise that the not-yet-built CLI already supports these exact field names. Publish and validate an independent YAML schema before startup. All size values need explicit units. `max_connections`, per-session buffers and other figures below are illustrative, not independent SLOs or defaults until confirmed by the capacity model. Mandatory environment variables may provide measured hard budgets.

```yaml
broker:
  node_id: "node-1"
  data_dir: "/var/lib/broker"
  public:
    tcp_tls: "0.0.0.0:7443"
    quic: null                 # enable after transport A/B testing
    tls_min_version: "1.3"
    max_connections: 50000
    max_frame_bytes: 1048576
    max_payload_bytes: 262144
  control:
    https: "127.0.0.1:8443"
    audit_enabled: true
  runtime:
    memory_budget_bytes: 17179869184
    max_pending_handshakes: 1000
    max_outbound_bytes_per_session: 262144
    max_subscribers_per_topic: ${MAX_TOPIC_SUBSCRIBERS} # hard limit from capacity model
    max_outbound_bytes_per_sec: ${MAX_GLOBAL_EGRESS_BPS} # all egress, including fan-out
  cluster:
    metadata_voters: ["node-1", "node-2", "node-3"]
    replication_factor: 3
    desired_placement: "rendezvous"
    reject_q2_without_quorum: true
    stale_metadata_policy: "fail_closed_on_expiry"
  durability:
    wal_segment_bytes: 134217728
    fsync_mode: "batched_durable" # ACK only after actual durability barrier
    max_commit_batch_delay_ms: 5
  policies:
    defaults:
      qos: "Q0"
      priority: "TELEMETRY"
      ttl_ms: 2000
      max_subscriptions_per_session: 16
      max_inflight_messages: 128
    topics:
      "telemetry/*":
        allowed_qos: ["Q0"]
        overflow: "COALESCE_LATEST"
        max_ttl_ms: 5000
      "commands/*":
        allowed_qos: ["Q2"]
        priority: "CONTROL"
        overflow: "REJECT_PUBLISH"
        max_ttl_ms: 30000
      "events/*":
        allowed_qos: ["Q1", "Q2"]
        priority: "INTERACTIVE"
        retention_hours: 72
```

Configuration validation MUST refuse startup/updates with clear diagnostics for wildcard ACL violations, invalid integer types, memory-budget overruns, RF impossible for node count, too few failure domains, conflicting QoS/overflow policies and administrative endpoints bound to public interfaces. Missing required measured hard egress/subscriber limits, malformed placeholders/environment variables, expired security TTL or inconsistent degraded-mode settings also prevent startup with a clear error.

## 15. Mandatory engineering invariants

| Code | Invariant | Evidence / proof method |
|---|---|---|
| INV-01 | Never report `COMMITTED_QUORUM` without durable majority commit | Model/property tests, crash at every commit stage |
| INV-02 | At most one active owner/leader per `(shard, epoch)`; old owner is fenced | Network split, stale clients, delayed messages |
| INV-03 | Recovery after crash contains all acknowledged Q1/Q2 records under their respective failure models | External oracle, power-cut testing |
| INV-04 | No unbounded queues, subscriptions or timers | Static bounds audit, overload tests, memory probes |
| INV-05 | Client-provided fields cannot expand ACL, tenant or priority authority | Adversarial protocol/ACL fuzzing |
| INV-06 | Q0 overflow cannot alter promised Q1/Q2 guarantees | Mixed-load fault tests and outcome ledger |
| INV-07 | Durable offsets are not reused or rolled back beyond committed prefix | Re-election, snapshot recovery |
| INV-08 | Stale consumer ACK/fencing tokens cannot advance cursor | Generation race tests |
| INV-09 | Lost PUB_ACK leaves idempotent retry safe within dedup window | Drop ACK, resend, failover |
| INV-10 | Above measured capacity, report reject/drop/overload accurately | Offered-versus-admitted throughput oracle |
| INV-11 | No fast-path request requires a metadata-consensus round trip | Hot-path tracing and faults under metadata load |
| INV-12 | Monitoring export is not a mandatory blocking dependency | Stop exporter/collector under load |
| INV-13 | Consumer cursor advances only over a contiguous prefix closed by committed valid ACK or policy-permitted committed terminal disposition (DLQ/QUARANTINED). NACK/timeout/MAX_RETRY cannot close offsets; SEEK is separately authorized/audited. | TEST-011: out-of-order ACK, unavailable/crashing DLQ, QUARANTINED, audited SEEK, stale generation, replay oracle |
| INV-14 | Shared durable-shards cannot violate logical offsets, retention, ACL/tenant quotas, replay or neighboring partitions' committed prefix | TEST-012 mixed-shard compaction, migration and hot traffic |
| INV-15 | Metadata quorum loss cannot locally extend cached authorization/ownership. Expired authority fails closed; valid data-plane continues only until lease/TTL expiry. | TEST-013 degraded-mode matrix |
| INV-16 | Q1 does not require Raft or promise survival of lost local disk. Q2 committed prefix cannot depend on Q1 local-only records. Each v1 logical durable partition has exactly one durable class; Q1 owner loss cannot reuse the old log incarnation or falsely claim continuous replay. | TEST-017, REL-01/04, Q1 owner-loss/epoch and Q2 leader-election oracle |

## 16. Open engineering decisions with acceptable alternatives

This section does not waive requirements. It identifies implementation choices that require M0 evidence.

**ADR-001 — Transport.** Choose TCP/TLS or QUIC as production default after M0-A using preregistered LAN/loss/NAT/mobile/reconnect profiles, weighted SLOs, p99/p99.9, kernel+user memory per connection, CPU per 1,000 connections, handshake reliability, network overhead and operational cost. Both transports must satisfy wire/Q0/Q1/Q2/security contracts. Select QUIC only when its measured advantage on priority workloads outweighs resource/operational regressions without violating P0. When the difference is inconclusive, retain the simpler proven TCP/TLS baseline. No fixed automatic percentage rule.

**ADR-002 — KV/dedup state.** Evaluate redb, RocksDB (if native dependencies acceptable), compact WAL-derived state and optionally hybrid memory/disk. Decide using crash safety, durable ordering with WAL, dedup-lookup tail, memory upper bound, fsync amplification, compaction/restore p99 and migration simplicity. Candidate must pass TEST-004/011/015. Neither per-message dedup KV records nor two-tier storage are mandatory.

**ADR-003 — Scheduler.** Compare weighted deficit round-robin with fair queuing/EDF, reserves and starvation limits under identical mixed loads at 30–110% sustainable capacity. Retain the version meeting PERF-P1, fair service for admitted BULK, minimal CONTROL p99 and bounded CPU/queue overhead. An alternative must show reproducible advantage without changing admission semantics.

**ADR-004 — Topology placement.** Rendezvous/consistent hashing propose desired placement only. Coordinated membership and fencing epochs are authoritative.

**ADR-005 — Durable-shard sizing.** M0-C evaluates Q1 local WAL grouping separately from Q2 replicated Raft-shards. For Q2 measure Raft-shards/node, logical partitions/shard, group cost, neighboring CONTROL p99 under hot-neighbor load, fsync/write amplification, recovery/snapshot/migration and retention/tenant isolation. For Q1 measure local WAL groups and recovery with no Raft overhead. Keep Q2 aggregation while it lowers overhead and satisfies FR-STO-010/REL; split groups more finely if a hot partition violates isolation budgets. Do not mandate unproven partition-count or fixed group sizes.

**ADR-006 — Performance-target revision.** Revise PERF norms only through approved change control, preserving old/new values and reasons.

**Final deliverable objective:** an implemented and verified message broker that demonstrably supports dense client connections, supplies correct requested Q0/Q1/Q2 guarantees, manages overload without hidden reliability downgrades, recovers from defined faults and reports measurable characteristics on a reproducible test rig. Departures from [S2] are allowed only when product invariants are maintained and the change is supported by experiments.

## 17. Sources and normative references

### 17.1. User-provided research

**[S1]** *Исследование масштабируемости брокеров сообщений при высокой плотности клиентов* [Research on the Scalability of Message Brokers at High Client Density], research report, 2025, 36 pages. Relevant §§1.3, 2.2–2.3, 3.1–3.4 and conclusions; Figures 3.2–3.5 (pp. 24, 26, 28) constitute qualitative support and are insufficient alone for numerical acceptance.

**[S2]** *Проектирование собственного решения брокера сообщений для распределённых систем с высокой плотностью клиентов* [Designing a Custom Message Broker for Distributed Systems with High Client Density], research report, 2026, 44 pages in the supplied file. Relevant §§1.2–1.5, 2.1–2.5, 3.1–3.8, 4.1–4.6, architecture Figures 1–8 (pp. 20–30). Risks discussed on pp. 37–39 form the initial basis for R-01…28.

### 17.2. External technical references

**[E1]** IETF RFC 9000, *QUIC: A UDP-Based Multiplexed and Secure Transport* — https://www.rfc-editor.org/rfc/rfc9000.html (multiplexing, flow control, stream/connection limits).

**[E2]** IETF RFC 9001, *Using TLS to Secure QUIC* — https://www.rfc-editor.org/rfc/rfc9001.html (0-RTT replay risk and mitigation).

**[E3]** NATS, *JetStream Documentation* — https://docs.nats.io/concepts/jetstream (at-least-once, ACK, replay, dedup and limits of exactly-once claims).

**[E4]** IETF RFC 9221, *An Unreliable Datagram Extension to QUIC* — https://www.rfc-editor.org/rfc/rfc9221.html (unreliable datagrams, datagram flow control limitations, congestion-control restrictions).

**[E5]** `redb` crate documentation — https://docs.rs/redb/latest/redb/ (ACID, MVCC, crash-safe API; selection remains subject to M0-A/B).

**[E6]** OpenTelemetry Messaging Semantic Conventions — https://opentelemetry.io/docs/specs/semconv/messaging/ (metrics/traces; exact convention stability to be assessed during implementation).

**[E7]** NIST SP 800-204B, *Attribute-based Access Control for Microservices-Based Applications Using a Service Mesh* — https://csrc.nist.gov/pubs/sp/800/204/b/final (identity/mTLS/authorization principles; the broker is not required to use a service mesh).

**[E8]** Ongaro, D. and Ousterhout, J. (2014), *In Search of an Understandable Consensus Algorithm (Raft)* — https://raft.github.io/raft.pdf (coordinated replication, elections and safety).

**[E9]** Linux kernel documentation — https://docs.kernel.org/ (sockets, networking memory, epoll, performance; concrete kernel tunables recorded in test-rig report).

### 17.3. Independent reviews and status of expert suggestions

**[R1]** Grok, Qwen and Kimi reviews provided by the user (first round) and subsequent comparative meta-analyses (second round), file *Pasted text (2).txt*, 2026. These reports were treated as hypotheses about deficiencies, checked against the original v1.0 specification. Model opinions alone do not establish experimental effectiveness or implementation correctness. Detailed accepted/rejected decisions are in the separate change register for 1.0 → 1.1.1.