# 06 — Q1 local WAL, replay, dedup and recovery

## Failure contract

Q1 publication returns \`COMMITTED_LOCAL\` only after the current owner has persisted the complete record and the associated recoverable producer outcome to a functioning local disk. A process crash with that disk intact must recover every acknowledged record. Permanent owner/disk loss may lose acknowledged history; do not represent a new log incarnation as continuous replay. **No Raft** is required for Q1.

## Partition state

Owned by one Q1 partition actor: \`PartitionRef, durable_class=Q1, ownership_epoch, log_incarnation, next_offset, last_committed_offset, WAL handle, retention boundary, producer_dedup, group_cursors\`. Mutable access is serialized by the actor. Data records are immutable after commit. Multiple Q1 partitions may share a WAL writer but must retain independent logical offsets, retention and ACL.

## Frozen B-01 on-disk format v1

~~~text
directory: data/q1/<tenant>/<topic>/<partition>/<incarnation>/
MANIFEST:
  magic "RQWM" | major:u16=1 | minor:u16=0 |
  cluster_id:u64 | node_id:u64 | tenant_id:u64 | topic_id:u64 |
  partition_id:u32 | durable_class:u8=1 | incarnation:u128 |
  generation:u64 | committed_record_count:u64 | segment_count:u32 |
  segment[segment_count]: {id:u64, committed_len:u64, record_count:u64,
                           last_offset:u64, sealed:u8} | manifest_crc32c:u32
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

All fixed-width integers use big endian. Header size is 52 bytes; record length includes its own four bytes and trailing CRC (88 bytes plus payload); footer size is 20 bytes. Record version is 1, flags are zero, and kind codes are MESSAGE=1, PRODUCER_OUTCOME=2, CONSUMER_ACK=3, TERMINAL_DISPOSITION=4, CURSOR_CHECKPOINT=5, TOMBSTONE=6. Empty segments use `u64::MAX` as last offset. Published segment IDs start at zero and increase by one, with no reuse of committed inventory. Segment identity is scoped by partition and log incarnation. `created_at` is zero in v1 and is not used for correctness. The record CRC covers every preceding record byte including length; header and manifest CRCs cover their complete preceding bytes. The footer CRC covers the complete segment header/records plus its last-offset/count fields. Unsupported versions fail closed; no automatic cross-version migration is provided.

This is the frozen MVP physical layout; actual \`producer\` identity also contains the authenticated principal/partition scope in an associated metadata record. Record kinds: MESSAGE, PRODUCER_OUTCOME, CONSUMER_ACK, TERMINAL_DISPOSITION, CURSOR_CHECKPOINT, TOMBSTONE. Keep fields named and versioned; never cast structs directly to disk. Prefer checksum over all record fields including length/version and a separate manifest generation.

The manifest inventory lengths and record count establish the exact recoverable boundary; uncommitted tails are excluded even when their checksums validate. The manifest generation starts at one and increases on each successful barrier. The decoder caps manifest bytes at 16 MiB and validates all counts/lengths before allocating or scanning. Each typed record has a strictly sequential offset beginning at zero. WAL format is frozen by R0-05 before B-01 recovery tests. Metadata and dedup must be in a recoverable ordering relation to log records. A \`manifest.tmp→fsync→atomic rename→directory fsync\` publication pattern is required where the filesystem contract supports it. Preallocate segments only if benchmarks show value; preallocation is not a durability barrier.

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

## B-01 storage infrastructure boundary

The concrete `rovqet-wal::Wal` API is frozen in [02](02-module-contracts.md). B-01 owns storage, not publication authorization or the Q1 actor. Each WAL directory contains one configured Q1 partition and incarnation. The caller supplies a preexisting directory, ensures its creation is durable in its parent, and guarantees a single exclusive owner; this crate does not supply multiprocess file locking. Cluster, node, tenant, topic, partition and incarnation are checked on open; no automatic incarnation reset, retention or Q1/Q2 conversion is supported. Authenticated principal scope and the saved producer result belong in a typed `PRODUCER_OUTCOME` payload supplied by the partition actor; fixed producer ID fields alone are not a dedup key.

The durability states are distinct:

| State | Evidence | Readable as committed? |
|---|---|---|
| queued | bounded in-memory ownership | no |
| written | complete append returned from filesystem write | no |
| synchronized | segment barriers plus manifest publication and directory barrier succeeded | yes, through the returned boundary |
| recovered | reopening validated persisted identity, inventory and checksummed committed records | yes, through the recovered boundary |

A batch can include a message and its associated typed metadata. Each typed record has a sequential WAL offset; a producer-outcome payload binds its original message offset. The durable boundary records a physical complete-record boundary as well as logical position; a checksum-valid tail alone does not establish commitment. An unsynchronized message or producer outcome must never become broker success simply because scanning finds its bytes. B-02 returns the message reference only after its associated outcome is included in the same successful barrier.

The writer serializes append, rotation and barriers. A barrier synchronizes all segments needed by its record prefix, writes the next manifest generation to a temporary file, synchronizes that file, atomically renames it to `MANIFEST`, then synchronizes the WAL directory. Rename alone is insufficient. A synchronization or publication failure poisons the writer: no successful durable result follows until recovery. Such failure can leave a newer manifest visible, so a caller must treat the outcome as unknown rather than definite rollback. Cancellation removes only still-pending memory work; issued writes remain uncertain until an explicit successful barrier or reopening resolves them.

Recovery trusts only a valid published manifest, validates its exact committed inventory, and fails on missing/corrupt committed data. An interrupted temporary manifest never replaces the authority of a valid published generation. Only bytes and unreferenced files beyond the last proven committed boundary may be discarded. Storage tests simulate process termination, short/torn writes and injected I/O failures in temporary directories. They do not demonstrate physical power-loss durability. Deployment requires a local filesystem supporting the documented file synchronization, same-directory atomic replacement and directory synchronization semantics; unsupported operations return errors. Hardware/controller flush honesty and power-cut validation remain separate qualification work.

## B-02 publication-pair payload v1

The initial Q1 actor writes each message at WAL offset `n` and its producer outcome at `n+1`, then synchronizes both before returning `PublishOutcome::CommittedLocal(n)`. The outcome payload is exactly `"RQPO" | version:u16BE=1 | principal:u128BE | message_offset:u64BE` (30 bytes). Its fixed record metadata repeats the message's owner epoch, producer ID/epoch/sequence, message ID and server timestamp. Partition/incarnation/Q1 scope comes from the validated manifest. The saved token is `(principal, producer_id, producer_epoch, sequence)` within that scope. The original message bytes and identity settle identical retries; changed content conflicts, gaps and fenced epochs fail explicitly.

Recovery accepts only complete, ordered MESSAGE/PRODUCER_OUTCOME pairs with matching metadata and exact offset bindings. It rebuilds the bounded outcome/producer state, refuses a configured ownership epoch older than recovered history, and fails closed on malformed committed pairs. Storage errors make the actor unavailable; missing replies are unknown outcomes. The server exposes this publication path over TCP; B-03's production dedup-window qualification remains separate.


## B-02 development TCP route

An explicit server configuration selects one Q1-only topic and `PartitionRef`, incarnation, owner epoch and existing data directory. The default remains Q0-only. The server checks AUTH-derived principal 1 / tenant 1, rejects conflicting principal/tenant/topic claims, and requires the configured partition/incarnation/epoch on every Q1 publish. It rejects Q2 and unsupported Q1 routes before durable admission. Message IDs and nonzero producer IDs are supplied explicitly by the publisher. The opaque partition key does not select another partition in this single explicitly addressed route; client priority cannot elevate the fixed route's storage service.

TTL is 1–30000 ms and starts at bounded admission. Fresh work expiring before append returns EXPIRED_BEFORE_DELIVERY; an invalid/zero/over-limit TTL is rejected as EXPIRED before admission. Once writes start, caller timeouts or TTL expiry cannot roll back a durable result. Saved committed retry results remain resolvable. Queue limits cover 64 queued/active operations and 4 MiB retained payload allocation plus message/outcome overhead; the actor retains at most 100000 saved outcomes and never evicts to admit new ones.

Only an actor `CommittedLocal` result produces PUB_ACK `(qos=1, outcome=2, tenant_id, topic_id, partition_id, incarnation, message_offset)` with the original request ID. Storage write/sync uncertainty returns OUTCOME_UNKNOWN; subsequent unhealthy requests return STORAGE_UNAVAILABLE. A disconnected or timed-out SDK closes its uncertain stream and requires an explicit retry with identical producer token/message/payload. No automatic sequence change or Q0 downgrade occurs.

The server locks the existing directory before opening WAL, synchronizes its parent/directory, and completes recovery before processing sessions. Corrupt or mismatched history fails startup; no automatic reset or deletion of committed history is performed. Network workers do not open files, synchronize storage or perform blocking actor waits. Shutdown stops admission/sessions and drains the bounded accepted queue on a blocking worker. If the final drain exceeds five seconds, the server exits unsuccessfully with unresolved outcomes; restart recovery is required. Fault-test controls are compiled only with the explicit `fault-injection` feature; normal builds reject their environment variables.
