# 07 — Q2 replicated Raft-shards and durable quorum semantics

## Scope and library decision

Q2 uses a **proven Raft implementation**, never bespoke quorum-by-ACK replication. Reference Rust integration candidate: \`openraft\` (API 0.9.x; check/pin exact version at implementation). OpenRaft exposes storage, state-machine and network adapters and warns about pre-1.0 API incompatibility; keep those details behind \`ConsensusGroup\` and freeze Cargo.lock. See https://docs.rs/openraft/latest/openraft/.

Metadata consensus and each Q2 data shard are separate Raft groups, though they may share the library, networking and executor. RF=3 on three independent hosts for MVP. Q1 local WAL is **not** part of a Q2 Raft log.

## Durable-shard state

One Raft group \`shard_id\` hosts multiple Q2 logical partitions. Committed entries contain \`shard_id, raft_term, raft_index, partition_ref, logical_offset, producer_identity/epoch/sequence, payload_digest, record, metadata_mutation\`. The Raft state machine maintains per-partition \`next_logical_offset, committed_index, producer_outcomes, group_cursors\` and retention metadata. A physical Raft index never becomes the public logical offset.

Reference single shard append sequence: leader validates auth/class/epoch and producer sequence; submits a deterministic \`PublishCommand\` to Raft; log storage persists; replication reaches a durable majority; the leader applies committed entries in index order; the state machine returns the assigned logical offset and durable PUB_ACK outcome. Use the chosen library's exact commit/apply contract, not an assumed completion callback. Ensure storage adapter flushes required entries before reporting durable.

## Majority durability barrier

\`COMMITTED_QUORUM\` requires: (1) legitimate leader and ownership authority, (2) entry durably appended according to Raft storage contract, (3) durable acknowledgment from a majority of voters, (4) committed index advanced and deterministic state machine result known. The usual RF3 majority is two independent physical nodes. A TCP send, remote page-cache write or non-durable replica response is insufficient. Term/vote and membership persistence must follow the Raft library protocol.

When quorum disappears, pending Q2 requests return NO_QUORUM or OUTCOME_UNKNOWN according to whether the result is definitively uncommitted; never substitute Q1 or report a local fallback. Reads without quorum may serve only a specifically justified committed snapshot with staleness semantics; regular write+ACK is disabled.

## Deterministic state machine

For each \`PublishCommand\`:
- Validate embedded immutable partition class and producer token relative to replicated producer state.
- If identical committed sequence exists, return existing \`CommitOutcome\` without applying a second record; conflict/gap produces replicated deterministic rejection or a preproposal rejection consistent with replicas.
- Allocate logical offset **at committed state-machine apply**, not from a leader-local provisional counter.
- Record payload, outcome digest and sequence in the same deterministic state transition; expose result only after library reports applied commit at proper barrier.
- Update relevant indexes atomically within the state-machine checkpoint.
- Consumer ACK/terminal dispositions are separate Raft commands for the same durability domain; they must not overtake their message's committed prefix.

A leader may propose an entry that later fails; uncommitted old-leader tail must never be replayed as committed. A retry from an application producer uses its original token, not fresh sequence as a workaround for uncertainty.

## Raft storage adapter boundary

~~~text
trait ConsensusGroup {
  propose(shard_id, command, ownership_epoch) -> Future<Result<AppliedResult>>
  committed_snapshot(shard_id) -> Result<SnapshotRef>
  observe_leadership(shard_id) -> LeaderEpoch
}

RaftStorage:
  save_vote/term -> durable storage, ordering per library
  append_entries -> WAL with complete checksums
  flush -> durable per-library promise (no false majority)
  apply_to_state_machine -> deterministic indexed changes
  build_snapshot -> atomically publish manifest, state, dedup, cursors
  install_snapshot -> verify version/checksum/index/membership before activation
~~~

Do not implement this with two unsynchronized stores whose crash consistency is assumed. Either keep committed state WAL-derived or write an independently checked recovery protocol that reconstructs dedup/cursor state from committed entries.

## Ownership and elections

Metadata ownership epoch fences a shard's authority at the cluster placement layer; Raft term elects a leader **within** the authorized replica set. A local election cannot create a new global placement epoch. On leader change, reconcile lease/epoch with metadata snapshot, reject stale producer/consumer generations and only resume writes after legitimate leadership and majority. Two nodes must never both acknowledge writes for one shard/epoch, even during asymmetric partition.

## Snapshot, compaction and recovery

Capture (a) last included Raft term/index, (b) per-partition logical offset mapping, (c) producer dedup/fencing state, (d) consumer ACK sparse/cursor/terminal states, (e) membership and generation, and (f) checksums/version. An atomic snapshot publication and its manifest must be durable before compacting covered log. Install requires version/checksum and authority checks. Logical offsets must survive physical log compaction; tombstones never reset counters. Reader is constrained by committed prefix, not local append tail.

A corrupted or missing member can rejoin from surviving quorum snapshot plus committed log. Recovery must never silently turn noncommitted local entries into committed records.

## Group sizing and interference

Aggregation reduces per-partition timers/elections/fsync; a hot partition may worsen p99 for a neighboring CONTROL partition. Reference MVP uses a small fixed mapping of Q2 partitions to groups, enough to test 1 shared group versus separate groups. M0-C selects future sizing via measured scheduler interference, recovery time, snapshot cost, CPU/RAM and Raft leader overhead; no arbitrary universal 10–50 partitions/shard limit.

## Fault tests

1. Kill leader after send but before majority: no false COMMITTED_QUORUM; retry outcome safe.
2. Kill leader after majority but before reply: returned/persisted outcome is discoverable and stable.
3. Isolate one follower (2/3 healthy): writes still commit, under durable-majority assumptions.
4. Partition 1/2 split: lone node cannot commit; old leader cannot acknowledge.
5. Reorder/duplicate RPCs, delayed callbacks, stale term, stale metadata owner epoch: fencing correct.
6. Snapshot installation concurrent with consumer ACK and producer dedup: no cursor skip or repeated committed write.
7. Q1 log loss has no influence on any Q2 committed prefix.

Tests use an external oracle and independently persisted test node data directories. A three-process single-host smoke test is useful development feedback but not a claim about independent physical failure domains.

## C-02 metadata consensus: library decision and implemented contract

C-02 implements **metadata** consensus only; Q2 data shards stay C-03 work. The architecture
references OpenRaft 0.9.x, so the selected line is **openraft 0.9.26** (the newest 0.9
release), pinned exactly (`openraft = "=0.9.26"`, features `serde` and `storage-v2`). The
newer 0.10 line changes the storage and network contracts and is therefore not used. The
library types stay inside the dedicated `crates/metadata-raft` adapter; the public
control-plane model (`MetadataSnapshot`, placements, epochs) does not depend on OpenRaft
internals, and Raft log indexes remain unrelated to Q1 application offsets: the metadata log
and the Q1 WAL are separate durability domains.

The verified contract of that release, as implemented:

- **Log indexes are zero based and there is no sentinel entry.** The first entry of a fresh
  group has index 0, and `prev_log_id = None` means "replicate from the very beginning"
  (`None.next_index() == 0`). A default `LogId` must never be fabricated.
- **The persisted log is keyed by `entry.log_id.index`.** A second numbering scheme (a
  storage-internal sequence counter) silently shifts entries and trips the library's own
  follower assertion (`first entry index == prev_log_id.next_index()`).
- **Readers must observe later appends.** `RaftLogReader` instances are held across appends
  and must return a non-empty result for a non-empty range, so the reader shares the live
  index map instead of a copy taken when the reader was created.
- **`truncate(log_id)` drops `log_id` itself and everything after it; `purge(log_id)` drops
  everything up to and including it.** Neither may leave a hole, and the last purged log id
  is persisted, because `get_log_state` must report that id when no entry remains.
- **The state machine returns exactly one result per applied entry**, including blank and
  membership entries; the library asserts the count.
- **The durable completion callback fires only after the data is durable.** A failed flush is
  an uncertain write: the storage is poisoned and fails closed instead of serving a log it
  cannot vouch for. A corrupted, truncated or discontinuous log fails closed on open and is
  never reset.
- **Snapshots are durable and validated.** A snapshot is published as an image plus its
  metadata, atomically replaced; an interrupted publication leaves the previous snapshot
  intact, and a snapshot that does not validate is never served or installed. The received
  image is read from the start of the cursor.
- **Inter-node RPCs** are length-prefixed, CRC32C-checked JSON frames over loopback TCP,
  carrying only the library's own vote, append-entries and snapshot messages. No second
  consensus or election algorithm exists in the adapter.

## C-02 ownership migration: committed contract

The Q1 ownership handover is a committed state machine plus a bounded transfer plus a
durable revocation. The stages are `PREPARE -> CATCH_UP -> FENCE_OLD -> ACTIVATE_NEW ->
CLEANUP`, and every transition is committed by the metadata group.

- **The committed record** names the migration id, the source and the target, the log
  incarnation that must survive, the original and the next ownership epoch, the verified
  transfer watermark, the manifest identity, the fence evidence, the target readiness and
  the dependent partitions. The state machine rejects a stale expected epoch, a concurrent
  migration of the same partition, a conflicting migration id, a skipped stage, a fence
  whose evidence does not match the migration, an activation without fence evidence, an
  activation without a verified transfer and an activation without target readiness. Only
  the committed activation moves the placement to the new owner and the new epoch; until
  then the old owner stays authoritative.
- **The durable fence is a record of the Q1 WAL.** It is written by the actor that
  serializes the durable mutations, it is synced before the proof is returned, and recovery
  replays it. A restarted owner therefore cannot regain its mutation authority because its
  in-memory authority is empty again. A fence is bound to its migration and to the revoked
  epoch, so it can never be replayed as an authorization for another handover. The consumer
  fetch path checks the revocation before it claims a generation, because claiming a
  generation writes a durable checkpoint.
- **The transfer is bounded.** The prepare request declares the inventory, a chunk carries
  a file index and never a path, and the chunk, the file count and the staged bytes are all
  bounded. A repeated chunk with the same identity and content is idempotent, the same
  identity with different bytes is refused, a missing chunk keeps the finalize from
  succeeding and a file that does not match its declared digest is refused. The transfer is
  keyed by an inventory identity: the history that the fence made final is a second,
  separately verified inventory and not a mutation of the first one.
- **The lease-expiry fallback stays unsupported.** A handover needs a durable fence of the
  actual old owner. When the old owner cannot be contacted or durably fenced, the
  migration is refused or deferred; a local timer of the successor never proves that a
  remote owner stopped writing.

## C-02 storage promotion and successor activation

The storage lifecycle of a successor is explicit:
`STAGING -> VERIFIED -> FENCE_CONFIRMED -> ACTIVATION_COMMITTED -> PROMOTED -> ACTIVE`.

- **Only the active, validated route may serve committed Q1 operations.** A node without a
  committed placement stays inactive. A node that the committed metadata names as the owner
  opens its route; a successor of a committed migration never initializes an empty log as a
  substitute for the transferred history.
- **The route is replaceable at runtime.** The dispatch, the peer handler and the shutdown
  read the current route from a handle, so a broker can go from inactive target to active
  owner without a fabricated fresh log and without a stale session keeping mutation
  authority.
- **The promotion is crash-safe and recoverable.** The intent is journalled before the first
  file moves, every file is published with a digest check, an already published file is
  verified and skipped, the directory is synced and a durable record states that the
  promotion completed. A partition directory that holds unrelated or already active storage
  is refused, and the source storage is never touched. A crash before the promotion leaves
  the successor unavailable; it never boots an empty partition.
- **The transferred history keeps its identity.** The successor opens the promoted storage
  with the existing Q1 recovery logic, so the original incarnation, the committed offsets,
  the producer outcomes, the consumer groups, the sparse ACK sets, the retry metadata and
  the terminal dispositions are recovered instead of being invented again.
- **The dependent history is checked before the promotion.** A migration whose DLQ dependency
  cannot be shown locally keeps the successor inactive: the existence of the dependency field
  is never proof of durability, and a terminal disposition whose destination cannot be
  recovered is never dropped to make the source open.
- **A follower forwards its proposal.** A node that does not lead the group forwards a
  proposal to the committed leader instead of pretending that it can commit on its own.

The remaining C-02 work is the multi-process migration campaign against real broker
processes with the independent oracle, the crash matrix around the promotion and the reopen,
and the asymmetric-partition scenario.
