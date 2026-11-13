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

The server locks the existing directory before opening WAL, synchronizes its parent/directory, and completes recovery before processing sessions. Corrupt or mismatched history fails startup; no automatic reset or deletion of committed history is performed. Network workers do not open files, synchronize storage or perform blocking actor waits. Shutdown stops admission/sessions and drains the bounded accepted queue on a blocking worker. Listener failures also drain and join sessions before the composition root regains exclusive storage ownership. If the final drain exceeds five seconds, the server exits unsuccessfully with unresolved outcomes; restart recovery is required. Fault-test controls are compiled only with the explicit `fault-injection` feature; normal builds reject their environment variables.

## B-03 canonical retry and timed outcome retention

The authenticated principal and configured manifest scope `(tenant, topic, partition, Q1,
incarnation)` are authoritative. Within that scope the token is `(principal, producer ID,
epoch, sequence)`; request IDs only correlate replies. Canonical content is the exact
message ID and payload bytes (no collision-prone hash). Optional wire SHA-256 must match
the actual payload independently on every request, including retries. A conflicting
principal/tenant/topic claim is denied before admission. Wrong partition, class,
incarnation or ownership epoch is rejected before append. This single direct route has
fixed effective storage priority; partition-key and priority hints do not change its
routing or durable attributes. TTL remains B-02's admission deadline, not a persisted
message expiration attribute. Group/lease/disposition attributes are unsupported rather
than accepted as silently discarded durable attributes. These prototype restrictions
must be revisited before implementing general persisted message TTL/priority policies.

`--q1-dedup-window-ms` defaults to **86400000 (24 hours)**; a smaller value explicitly
selects a reduced research contract. The effective window and both quotas are advertised
in the server's startup diagnostic and `--help`; the wire registry is unchanged. The
Rust SDK documents this operator-advertised contract. There is no wire discovery of
route policy in major 1. `--q1-max-producers` and `--q1-max-outcomes` each default to
100000. Actor configuration exposes the same separate limits. Fixed-size map entries,
the existing bounded WAL index and queued payload budgets bound memory; no unexpired
outcome is evicted to admit work. Exhaustion returns RESOURCE_EXHAUSTED before append,
while known identical retries remain resolvable.

The existing paired-record `server_timestamp` is a conservative **logical millisecond
clock**, not an authoritative UTC creation time. The binary record/outcome layout and
version are unchanged. It starts at 1 in an empty log; recovery resumes the greatest
persisted timestamp. Running monotonic elapsed time advances it; durability IO, process
downtime, unavailable clock samples and backward intervals do not count. The timestamp
assigned to the pair marks the window's entry; excluding barrier IO ensures at least the
full configured duration following commit. An outcome expires when logical age is
**greater than or equal to** the configured window. Expiration is checked on actor
operations and during streaming recovery. Consequently real retention may be longer,
including after restart; no wall-clock adjustment can shorten the promise. Zero legacy
timestamps represent unavailable evidence and never expire. Nonzero decreasing durable
timestamps fail recovery conservatively. Legacy v1 Unix-millisecond timestamps remain
readable as the initial logical epoch. No client timestamp is trusted.

Recovery scans every committed pair and rebuilds producer epoch/next-sequence watermarks
independently of retained outcomes. Later durable timestamps prove which prior outcomes
can expire without loading an unbounded history. Time elapsed since the last persisted
sample is intentionally forgotten on restart; an expired outcome can conservatively
become available again. A smaller quota or larger window may make recovery fail closed
rather than violate retention. Expired same-epoch sequences return actor
`OutcomeUnavailable` (wire OUTCOME_UNKNOWN); they can never reinsert a message because
high-water marks are not removed. Older epochs remain PRODUCER_FENCED. A newer epoch
starts at sequence zero and fences only after its first successful durable commit.

`timed_dedup` tests inject time without sleeping, verify exact expiration, conservative
restart/rollback/unavailable evidence, separate quotas, recovered high-water/fencing,
and 512 concurrent retries with one durable pair. Existing model, actor fault and
independent TCP ACK-journal/SIGKILL fixtures continue to verify stable original offsets.

## B-04 actor-owned committed replay and standalone TCP inspection

`Partition::try_replay(ReplayRequest)` submits to the same storage owner as publication.
The request carries the exact `PartitionRef`, incarnation-bearing starting offset,
message/response-byte limits and optional wait. Results are typed `ReplayBatch` /
`ReplayError`; there are no deliveries, leases, group generations or consumer ACKs.
Each `ReplayMessage.offset` and `ReplayBatch.next` retains the incarnation.

The B-02 mapping is now explicit: MESSAGE offsets are `0,2,4,...`; each associated
PRODUCER_OUTCOME occupies the next odd offset. Continuation advances by two per message.
An odd offset is not an application cursor. Internal outcomes are validated but never
returned or counted against the application message limit. Reads use the existing
WAL-derived committed record index, one complete verified pair at a time, across
segments. Both recovery and runtime reads share the exact v1 pair-binding validator.
A checksum/binding failure returns no partial batch, poisons the partition, and leaves
storage unchanged. The actor advances its visible end only after the durability barrier.

Hard request ceilings are 1024 messages, 4 MiB response charge and 30 seconds wait.
Zero messages, less than 128 response bytes and over-limit requests are InvalidLimits.
Response charge is 128 batch bytes plus 128 bytes per message plus payload length;
these conservative application/memory charges are **not wire frame sizes**. Message
count and byte limits both apply. The first message exceeding the limit returns
ResponseTooSmall with the required charge; no false empty/end response is fabricated.
Admission additionally reserves bounded WAL decoding scratch and the preallocated
message vector. Pending requests, waits, channel-held responses and returned batches
retain reservations against the shared partition message/byte budget until dropped.
Oversized aggregate requests return Capacity before allocation/IO. A caller can exhaust
its own partition budget by retaining batches, but cannot silently allocate broker
responses without reservations. Queue limits may be stricter than the hard API ceiling.

`from == committed_end` returns an empty batch with `end_of_available_history=true` and
the same incarnation-bearing continuation, or waits until publication/deadline/shutdown.
`from > end` or an odd offset returns ReplayNotAvailable; a stale incarnation returns
IncarnationMismatch. End refers to a serialized committed snapshot, not permanent EOF.
There is currently no retention deletion, so every earlier even committed offset is
available. Waits live in bounded actor-owned pending state and do not block publications.
Idle waits poll at most every 10 ms for cancellation/deadline; shutdown resolves waiting
reads immediately against the final drained snapshot. Dropping a waiter cancels read-only
work before IO when possible, or discards an already executing result. Filesystem IO
itself is blocking and cannot promise a hard OS-level cancellation/latency bound.

`Partition::open_existing` refuses missing MANIFEST/history with HistoryUnavailable and
never initializes a replacement log. The TCP server likewise resumes by default; an
empty replacement directory is REPLAY_NOT_AVAILABLE even if all old files disappeared.
`--q1-initialize-new-log true` is deliberate initialization of **new empty storage**,
requiring the caller to allocate a fresh incarnation; it refuses nonempty directories.
This is not a reset/erase operation or an incarnation allocator. No reset API, automatic
data deletion or offset continuity across independent incarnations exists. Incarnation
allocation remains configuration authority's responsibility; the broker cannot prove
global uniqueness after loss of every copy of configuration/storage. An intact ordinary
restart reuses its configured identity and committed index.

Standalone TCP uses the additive READ_FETCH/READ_RECORD/READ_END contract in
[the registry](protocol-registry.md#b-04-standalone-inspection-extension-major-1-additive).
FETCH/MESSAGE keep their original group requirements. The prototype explicitly rejects
FETCH/CONSUMER_ACK/CONSUMER_NACK/EXTEND_LEASE; no group state is fabricated.

The authenticated principal and configured Q1 route authorize inspection. Topic-name/ID,
tenant/principal assertions, partition and incarnation are checked before actor admission.
`try_replay_encoded` refines the existing read seam with trusted terminal/record sizing;
legacy actor memory-charge reads are unchanged. It returns the exact stop reason and
reserves additional space for encoding, vector capacity and retained response bytes.
Client wire max_bytes includes all READ_RECORD frames plus the 78-byte READ_END. The
partition's aggregate queue budget can reject requests below the hard 4 MiB wire ceiling;
no reservation or dedup guarantee is silently relaxed to accommodate reads.

Networking awaits the actor without blocking Tokio. A separate one-slot lossless reply
path writes one bounded frame at a time and awaits write completion; every queued write
holds the actor batch reservation. Q0's volatile queues, credits and control reserve stay
independent. Dropping a read-only session cancels its actor waiter; a peek detects EOF
without consuming a partial pipelined frame. This serial profile does not pipeline batches;
unread pipelined input can defer EOF recognition until the bounded wait ends. Blocking
filesystem IO remains non-preemptible. The SDK owns the stream for the whole operation,
so cancellation or interruption between frames closes it. Only a validated READ_END
returns a successful typed batch; partial records never escape as a completed result.
Standalone reads never persist group cursors or mutate producer state.

Actor proofs cover clean/SIGKILL recovery against the independent ACK journal, segment
boundaries, message/byte limits, metadata exclusion, empty/end/invalid ranges, stale
incarnation, committed message/outcome corruption, incomplete uncommitted tail, concurrent
publication, cancellation/retained-response quotas, asynchronous wait and bounded wait
expiry/shutdown. Fault-enabled tests hold an unsynced append and prove queued replay cannot
expose it; cancellation releases the retained read budget. Disposable loss fixtures never
invoke broker deletion or reset operations. TCP tests prove unsupported group operations
fail-closed lost-history startup and explicit group-operation rejection. Real TCP/SDK tests add exact encoded bounds, multi-frame validation, continuation, long polling, negotiated frame limits, cancellation and read-only producer-state checks. The independent process ACK journal verifies message IDs, offsets and payload hashes through the SDK after clean restart and SIGKILL.

## B-05 durable consumer metadata in the Q1 WAL

The v1 record, segment and manifest formats are unchanged. B-05 uses the already reserved
record kinds **CONSUMER_ACK (3)** and **CURSOR_CHECKPOINT (5)** with self-describing
payloads; `MESSAGE` (1) and `PRODUCER_OUTCOME` (2) keep their exact existing encoding.

- `CONSUMER_ACK` payload `RQCA\\0\\x01` (66 bytes): group_id u64, generation u64,
  ownership_epoch u64, lease_id u128, application offset u64, attempt u32, principal u128,
  message_id u128. The record's `message_id`/`sequence` mirror the delivery identity; the
  payload is authoritative for group state.
- `CURSOR_CHECKPOINT` payload `RQCP\\0\\x01`: group_id u64, generation u64, cursor u64,
  acknowledged sparse-offset count u32, then that many u64 offsets in strictly ascending
  order. The count is bounded by the configured sparse limit. A checkpoint embeds the full
  sparse set, so later checkpoints never erase an acknowledged gap.
- Leases, deadlines and delivery attempts are deliberately **volatile**. After a restart the
  same committed record is redelivered with a fresh lease; a lease token from a previous
  process is never accepted.

Recovery is kind-aware and sequential. A committed `MESSAGE` must be immediately followed by
a matching `PRODUCER_OUTCOME`; an orphan outcome, a missing pair, a malformed ACK/checkpoint
payload, an ACK for an unknown application message, a checkpoint ahead of its own append
position, a decreasing generation, or an unsupported terminal/tombstone record fails closed.
Every ACK and checkpoint is replayed in WAL order; checkpoints replace the cursor state and
ACKs then apply incrementally. Existing storage containing only publication pairs recovers
without migration and without changing any previously acknowledged offset.

Application message offsets remain the stable WAL logical offsets of `MESSAGE` records. Once
consumer metadata is interleaved they become sparse: a MESSAGE is no longer guaranteed to sit
at an even offset and consecutive messages are not an arithmetic sequence. Standalone replay
therefore scans WAL positions, skips `PRODUCER_OUTCOME`/`CONSUMER_ACK`/`CURSOR_CHECKPOINT`
records, and returns the next unread position as continuation. That continuation may be odd:
`READ_RECORD` and `READ_END` accept every in-range application position, and refusing odd
positions would break replay as soon as consumer metadata is interleaved. Cursor advancement likewise
walks the ordered list of application messages; it never derives the next message by
incrementing a physical or logical offset.

Default group bounds are 8 groups, 256 in-flight leases, 4096 sparse ACK entries, 4 MiB leased
bytes, a 30 s lease and a checkpoint every 64 ACKs. Group state, WAL reads and retained batch
reservations are charged to the existing partition budget before IO; exhaustion is an explicit
CAPACITY error. The single partition actor remains the only mutator, and all group durability
barriers are ordinary Q1 segment+manifest syncs.

## B-06 retry and terminal metadata; separate Q1 DLQ

The v1 record/segment/manifest envelope is unchanged. B-06 adds the previously unassigned
record kind **RETRY (7)** with an `RQRT` v1 payload (group, incarnation, source offset,
generation, ownership epoch, attempt, reason, delay, exhausted flag), and now uses the
reserved **TERMINAL_DISPOSITION (4)** kind with an `RQTD` v1 payload. The terminal payload
carries state (PENDING/COMMITTED), kind (QUARANTINE/DLQ), group, generation, ownership epoch,
source incarnation/offset, deterministic disposition ID, reason, attempts, original message
ID, and for committed DLQ the destination incarnation/offset. Disposition IDs are derived
from `(group, source incarnation, source offset)` and must match on recovery.

Recovery applies retry records as durable attempt state (eligible immediately after restart,
because monotonic wall time is unavailable) and terminal records as pending or closed state.
Pending records are reconciled before the actor reports ready: quarantine appends its
committed marker; DLQ republishes idempotently to the destination, verifies the destination
record and only then appends the source COMMITTED marker. A committed DLQ terminal record is
re-verified against the DLQ partition during recovery; a missing destination proof fails
closed. Checkpoints and compaction never erase an unresolved PENDING record.

The DLQ is a second explicitly configured local Q1 partition with its own directory,
incarnation, WAL and manifest. Its messages are ordinary v1 application messages whose
payload is an `RQDL` v1 frame preserving the original bytes and identity. A DLQ publication
uses the deterministic disposition ID as producer/message identity, so a repeated
disposition returns the original destination offset instead of inserting a duplicate. No
Q0 fallback exists; unavailable DLQ storage yields DLQ_UNAVAILABLE and leaves the source
cursor blocked.
