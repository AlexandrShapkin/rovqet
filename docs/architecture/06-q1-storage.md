# 06 — Q1 local WAL, replay, dedup and recovery

## Failure contract

Q1 publication returns \`COMMITTED_LOCAL\` only after the current owner has persisted the complete record and the associated recoverable producer outcome to a functioning local disk. A process crash with that disk intact must recover every acknowledged record. Permanent owner/disk loss may lose acknowledged history; do not represent a new log incarnation as continuous replay. **No Raft** is required for Q1.

## Partition state

Owned by one Q1 partition actor: \`PartitionRef, durable_class=Q1, ownership_epoch, log_incarnation, next_offset, last_committed_offset, WAL handle, retention boundary, producer_dedup, group_cursors\`. Mutable access is serialized by the actor. Data records are immutable after commit. Multiple Q1 partitions may share a WAL writer but must retain independent logical offsets, retention and ACL.

## Reference on-disk format v1

~~~text
directory: data/q1/<tenant>/<topic>/<partition>/<incarnation>/
MANIFEST: magic, format_version, cluster_id, node_id, partition_ref,
          durable_class=Q1, log_incarnation, segment_list, recovery_watermark,
          manifest_generation, checksum
segment-N.wal header:
  magic "RQWL" | major:u16 | minor:u16 | segment_id:u64 |
  partition_or_shard_id:u64 | incarnation:u128 | created_at:u64 | header_crc32c
record:
  len:u32 BE | version:u8 | kind:u8 | flags:u16 |
  logical_offset:u64 | owner_epoch:u64 | producer_id:u128 |
  producer_epoch:u64 | sequence:u64 | message_id:u128 |
  server_timestamp:u64 | payload_len:u32 | opaque_payload |
  record_crc32c:u32
segment footer (sealed):
  last_complete_offset:u64 | record_count:u64 | segment_crc32c
~~~

This is an MVP **reference** physical layout; actual \`producer\` identity also contains the authenticated principal/partition scope in an associated metadata record. Record kinds: MESSAGE, PRODUCER_OUTCOME, CONSUMER_ACK, TERMINAL_DISPOSITION, CURSOR_CHECKPOINT, TOMBSTONE. Keep fields named and versioned; never cast structs directly to disk. Prefer checksum over all record fields including length/version and a separate manifest generation.

WAL format is frozen before M2 recovery tests. Metadata and dedup must be in a recoverable ordering relation to log records. A \`manifest.tmp→fsync→atomic rename→directory fsync\` publication pattern is required where the filesystem contract supports it. Preallocate segments only if benchmarks show value; preallocation is not a durability barrier.

## Append and commit algorithm

1. Validate current owner epoch, topic class Q1, quota, TTL and producer epoch/sequence.
2. Dedup lookup: identical committed token returns prior outcome/offset; changed digest returns SEQUENCE_CONFLICT; gap returns SEQUENCE_GAP; old epoch returns PRODUCER_FENCED.
3. Reserve bounded WAL batch bytes; assign \`next_offset\` once and place \`MESSAGE + recoverable outcome/dedup metadata\` in an append batch.
4. Write append records serially. Complete writes to OS page cache are **not** COMMITTED_LOCAL.
5. Perform an explicit durable \`sync_data\`/equivalent fsync barrier for all records needed for that batch and metadata ordering, including prior segment rotations.
6. Advance last_committed_offset and send COMMITTED_LOCAL with (incarnation, offset) to waiting request(s).
7. After commit, enqueue record for eligible durable consumer group; never deliver it earlier as safely durable.

If fsync returns error or has unknown result, do not send success. Mark partition unhealthy, reject new writes and recover/inspect state. A timed-out caller may still have committed; retry with same scoped token settles using dedup. Batches do not delay deadline-sensitive traffic beyond measured configurable maximum.

## Dedup and producer state

Key: \`(tenant,principal,producer_id,producer_epoch,topic,partition,durable_lane)\`, with sequence as the monotone per-partition counter. Persist producer epoch fencing and a compact \`last_seq\` watermark plus enough \`seq→{digest,outcome,offset}\` entries to satisfy the configured dedup window. If a sequence is missing, reject until next expected; never implicitly reorder. A retry of a committed same-sequence message returns the same physical outcome. If the result was never committed and sequence is still expected, a retry may become the first durable append.

Reference MVP window can be shorter for small experiments **only if explicitly advertised as a reduced prototype contract**; full product default is 24 hours. Do not allow memory exhaustion to evict entries while still claiming the full window. Quotas cap active producers and outcome history. On disk loss, the old dedup history is also lost: no automatic reuse of old incarnation with a guessed watermark.

## Recovery state machine

\`STARTING → MANIFEST_VALIDATION → SEGMENT_SCAN → TRUNCATE_UNCOMMITTED_TAIL → REBUILD_INDEX/DEDUP/CURSORS → READY\`, or \`UNAVAILABLE\`.

- Verify manifest generation/checksum, segment headers and sealed footers.
- Sequentially scan until last complete checksum-valid record. A torn or partially written tail **after** last known durable barrier is truncated.
- A corrupted or missing record at/before last durably acknowledged offset is fatal without an independently verified recovery source; never silently skip.
- Rebuild logical offset index, producer outcome ledger, ACK sparse sets, cursor checkpoint, retention metadata and leases/fencing. Resolve incomplete terminal disposition protocol idempotently.
- Recovery watermark must not move backwards below acknowledged boundary with intact disk. Reopen latest segment only after validation.
- If the owner/disk is irrecoverable and no trustworthy copy exists, refuse replay old incarnation. A new incarnation requires explicit authorized reset, SDK notification and a fresh scoped producer stream.

## Replay and retention

\`FETCH(incarnation, offset, max_bytes, max_messages, wait_ms)\` uses a sparse index into sealed/open segments and returns only committed records. Read across segments with bounded buffer; cursor/ACK state is separate from physical retention deletion. An expired segment may be removed only per configured retention and not while containing live records of another logical partition. Compaction/tombstones do not reuse existing offsets.

Q1 migration: PREPARE/CATCH_UP (copy and verify WAL/snapshot, replay delta) → FENCE_OLD → ACTIVATE_NEW → CLEANUP. Never have two writable Q1 owners for same epoch. If transfer fails, retain old owner/deny new ownership rather than fabricate durable state.

## Crash boundaries / oracle

Inject stop after write, before fsync, during fsync, after fsync before PUB_ACK, during segment rotation, after checkpoint before manifest rename and after rename before directory sync. Assert: no COMMITTED_LOCAL without proven barrier, all acknowledged records recover with intact disk, no duplicate insertion from a same-sequence retry in the dedup window, no cursor skips, no old-incarnation replay after irrecoverable loss.

The **independent oracle** journal lives outside Q1's storage and records received ACKs and original message checksums. WAL metrics: append/sync p50/p99, pending batch bytes, segment count, recovery scan time, dedup count and Q1 pending backlog.
