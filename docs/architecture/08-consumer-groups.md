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

## Standalone read-only inspection boundary

B-04's READ_FETCH/READ_RECORD/READ_END and Rust `read_q1` API inspect committed history.
They do not allocate leases, acknowledge deliveries, advance a group cursor, fence group
generations or promise at-least-once consumer-group processing. An inspection continuation
is a log reference, never consumer progress.

## B-05 implemented local Q1 subset

One explicitly configured Q1 partition supports multiple tenant-scoped group IDs, but only
one active mutation generation per group. Implementation choices:

- `FETCH` with no `group_generation` field or generation 0 **claims** the group: the actor
  appends a generation fence (`CURSOR_CHECKPOINT`) and syncs before delivering. A later
  claim durably increments the generation and cancels the previous generation's leases;
  `FETCH`/`CONSUMER_ACK` carrying the old generation returns STALE_GENERATION.
- A new group starts at the earliest committed application message. Leases are volatile
  grants bounded by `max_inflight`, `max_lease_bytes` and a lease deadline; after expiry or
  restart the same committed message is redelivered with a higher attempt or a new lease.
  Attempts are delivery-scoped, not durable consumer progress.
- `CONSUMER_ACK` validates principal, tenant/topic/partition, incarnation, ownership epoch,
  generation, lease, offset, attempt and message identity. The actor appends a `CONSUMER_ACK`
  WAL record, syncs, recomputes the contiguous cursor and only then returns ACK_COMMITTED.
  A duplicate ACK for an already committed offset is a read-only saved-outcome lookup, valid
  even after fencing. Any durability failure returns ACK_NOT_COMMITTED/OUTCOME_UNKNOWN and
  poisons the actor; it is never success.
- Out-of-order ACKs are retained in a bounded sparse set. The cursor advances only over the
  contiguous acknowledged prefix; it never skips an unacknowledged application record.
  Periodic `CURSOR_CHECKPOINT` records embed the cursor and the sparse set, so compaction
  cannot erase acknowledged gaps.
- `FETCH` batches terminate with the additive `FETCH_END` frame after all `MESSAGE` frames.
  Losing the connection before `FETCH_END` yields no completed SDK batch. No timeout, NACK or
  disconnect closes an offset by itself.

## B-06 implemented retry and terminal disposition

`CONSUMER_NACK` validates the same full token as ACK and never acknowledges processing. It
either schedules a bounded retry (initial delay, exponential doubling, maximum delay,
maximum attempts) or starts the configured terminal policy. Repeated NACKs of the same
attempt are idempotent. `CONSUMER_NACK_RESULT` reports RETRY_SCHEDULED, RETRY_EXHAUSTED,
TERMINAL_COMMITTED, TERMINAL_PENDING or ALREADY_CLOSED; ACK_COMMITTED is never reused.

Retry exhaustion alone leaves the source incomplete. With policy QUARANTINE the broker
durably appends a PENDING terminal record and then a COMMITTED quarantine record before the
cursor may close the offset. With policy DLQ the broker durably appends PENDING, publishes
the complete original payload to a separate explicitly configured Q1 DLQ partition with a
stable disposition ID, verifies the destination commit, and only then appends the source
COMMITTED record. Crash recovery reconciles a durable PENDING record: it re-verifies or
idempotently republishes the DLQ message and completes the source transition only after its
own barrier. A missing DLQ proof fails closed; DLQ unavailability reports DLQ_UNAVAILABLE and
leaves the cursor unchanged.

Terminal closure participates in the same sparse completed set as ACKs, so later closures
never advance the cursor across an earlier incomplete record. Quarantine and DLQ never delete
the original Q1 publication. `EXTEND_LEASE`, SEEK, rebalancing, Q2 and distributed terminal
transactions remain out of scope.

Limitations: a lease token is valid only for its process lifetime; redelivery attempts reset
after restart. Group state is not replicated (Q2/Raft remains C-03/C-05), and an
irrecoverably lost Q1 history still requires an explicit authorized reset with a fresh
incarnation — recovery never resets progress silently.
