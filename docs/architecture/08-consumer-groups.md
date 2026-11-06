# 08 — Durable consumers, leases, ACKs, cursor and terminal disposition

## Durable delivery semantics

For Q1/Q2 a consumer group has one active receiver per logical partition at a time. The broker promises at-least-once delivery subject to the class' retention and failure model; the application must make external side effects idempotent. The broker's ACK reports **broker progress persisted**, never application execution.

### Stable identifiers

\`DeliveryToken = {tenant, group, partition_ref, log_incarnation, offset, ownership_epoch, generation, lease_id, delivery_attempt}\`. These fields must be bound to a delivered committed record and authorized principal. \`group_id\` namespace is tenant-scoped. A new group generation invalidates mutation authority of previous lease holders.

## Group state

Per group/partition: \`generation, active_consumer, next_delivery_offset, lease_deadline, max_inflight, pending_leases, completed_sparse_set, durable_cursor, retry_schedule, terminal_dispositions\`. Only the owning group actor mutates. Durable state includes cursor closure and any out-of-order ACKs that would otherwise be lost across restart. Delivered but unACKed messages are eligible for redelivery after lease loss/rebalance; do not incorrectly advance the cursor.

## Lease and delivery state machine

\`READY→LEASED→DELIVERED→ACK_RECEIVED→ACK_VALIDATED→ACK_DURABLY_COMMITTED→ACK_COMMITTED\`.

- Before lease: require committed Q1/Q2 record, available credits and valid group ownership.
- At lease: allocate bounded inflight slot, attach current generation/epoch and deadline.
- On ACK: validate current delivered offset, principal, epoch, generation and live lease; stale ACK cannot mutate.
- Persist ACK/closed-offset with Q1 local barrier or Q2 quorum barrier; only then return ACK_COMMITTED.
- If ACK response lost after commit, retry/lookup of same token can return saved committed outcome without a new mutation, even when old generation is fenced.
- If ACK not committed: OUTCOME_UNKNOWN or ACK_PENDING (not success). Restart may redeliver, so handler must be idempotent.

## Cursor rule

Let \`closed(i)\` be true iff a previously delivered offset i has a committed valid ACK **or** a policy-authorized, durably committed terminal disposition. A group cursor is the largest \`k\` such that all relevant offsets from its previous durable boundary through k are closed. ACK_CUMULATIVE cannot leap over an incomplete offset. An out-of-order ACK is recorded in a **bounded sparse set**; subsequent closure fills the gap and permits advancing cursor. No timeout, NACK or max-retry event by itself closes an offset.

Offsets are coupled to \`LogIncarnation\`. On irrecoverable Q1 history loss, the old cursor is not carried to the new incarnation without an explicit authorized reset + generation bump; SDK sees REPLAY_NOT_AVAILABLE/current incarnation.

## Retry and poison message paths

\`NACK / LEASE_EXPIRED → RETRY_QUEUED\`; schedule exponential backoff with limit and bounded timer storage. On MAX_RETRY the policy chooses:
- \`QUARANTINE_PENDING\`: durably commit terminal marker in domain no weaker than the source, then QUARANTINED and close offset.
- \`DEAD_LETTER_PENDING\`: durably publish a full replayable DLQ record with no weaker class, then persist source terminal marker with stable \`disposition_id\`; only after both barriers close source offset.
- If disabled/no capacity/failed DLQ, keep source incomplete and retry/backpressure; **never drop just because attempts exhausted**.

Cross-shard DLQ: no XA requirement. Recoverable order is \`DLQ_COMMITTED→TERMINAL_COMMITTED\`. Before marker commit, retry an idempotent DLQ publish with same disposition ID; duplicate DLQ delivery is only allowed within documented retry/dedup limits, never silent loss of source. When outcome is uncertain, resolve by token lookup before acting as committed.

\`SEEK\` is a separate privileged administrative action with audit, reason, new generation/fence and explicit discontinuity. MVP omits general admin API but may implement a test-only reset command; it is **never** an implicit consumer ACK.

## Group ownership and rebalance

Group assignment depends on valid metadata ownership epoch; only one active consumer lease generation per group/partition. On disconnect, wait for bounded lease expiry or explicitly fence and reassign. Persist generation fence in the same-or-stronger durability domain before a new owner can mutate the cursor. Scale group count/inflight state under tenant/global quotas.

Reference MVP sequencing: single group with one consumer, then reconnect + ACK, then competing workers/rebalance, then DLQ/quarantine after durable contract is proven.

## API behavior table

| Event | Write allowed? | Cursor effect | Client result |
|---|---|---|---|
| valid ACK before barrier | pending | none yet | ACK_PENDING / outcome unknown |
| valid ACK after barrier | committed | advance only contiguous prefix | ACK_COMMITTED |
| stale-generation new ACK | no | none | STALE_GENERATION |
| duplicate previously committed ACK | read-only lookup | no new mutation | saved ACK_COMMITTED |
| NACK/timeout | retry schedule | none | accepted retry status |
| max retries | terminal pending | none | pending / DLQ_UNAVAILABLE if failed |
| durable DLQ + disposition | committed | advance if prior offsets complete | CLOSED_FOR_GROUP |
| authorized SEEK | admin-only | explicit reset/boundary | audited outcome |

## Tests and oracle

Crash at ACK receive, validation, append, fsync/quorum, response; crash between DLQ commit and disposition marker; lose replies, replay retry; two consumers race at rebalance; terminate after noncontiguous ACK; expire lease; change Q1 incarnation; ensure no offset is silently skipped. The oracle records \`(group,incarnation,offset,delivery token,ACK result,terminal state,cursor)\` independently.
