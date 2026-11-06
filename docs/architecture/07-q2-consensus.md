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
