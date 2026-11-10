# 11 — Failure handling, outcomes and recovery matrix

A failure test is valid only when it can distinguish an operation **not sent**, **admitted**, **possibly written**, **durably committed**, **delivered**, and **consumer-ACK committed**. A timeout is not evidence of rollback.

## Fault taxonomy

\`F0\` client disconnect; \`F1\` broker process crash; \`F2\` local disk EIO/ENOSPC/power cut; \`F3\` permanent Q1 owner/disk loss; \`F4\` Q2 leader crash; \`F5\` Q2 quorum loss; \`F6\` metadata quorum loss; \`F7\` stale ownership epoch; \`F8\` stale consumer generation/lease; \`F9\` bad WAL/snapshot checksum; \`F10\` slow subscriber; \`F11\` resource saturation; \`F12\` dedup/ACK reply lost; \`F13\` incomplete DLQ transfer.

## Expected observable behavior

| Event | New Q0 | Q1 publish / replay | Q2 publish / replay | ACK and other effects |
|---|---|---|---|---|
| client disconnect | ephemeral subscription removed, queued Q0 may drop | in-flight publish may have committed; reconnect retry identical token | likewise; check majority outcome | unACKed durable delivery may redeliver |
| broker process crash; disk intact | volatile work lost | recover all COMMITTED_LOCAL; no false ACK | remaining quorum elects or waits; only committed prefix | recover durable ACK/cursor/generation from relevant domain |
| disk full / fsync EIO | may continue if independent resource healthy | affected partition refuses commit, STORAGE_UNAVAILABLE | quorum may use healthy replicas; affected node not eligible to attest durability | ACK cannot be successful before its barrier |
| permanent Q1 owner loss | Q0 may move with new subscriptions | old incarnation unavailable; explicit new incarnation/reset only | no change to separate Q2 prefix | old cursor/producers cannot silently reuse offsets |
| Q2 leader crash before majority | unaffected | unaffected | outcome unknown or not committed; no fabricated ACK | consumers read committed prefix only |
| Q2 leader crash after majority, reply lost | unaffected | unaffected | retry/lookup returns same committed result/offset | dedup state reconstructed from committed entries |
| Q2 quorum loss | unaffected if authority/resources valid | may continue on local WAL + valid owner | no new COMMITTED_QUORUM; safe read only if justified | no new quorum-backed ACK commits |
| metadata quorum loss, valid cached authority | existing Q0 allowed | current owner+healthy Q1 allowed | current leader+data quorum allowed | no ownership/topic mutation |
| metadata authority/lease expired | refuse owner-dependent operations | refuse Q1 write/replay without valid owner | refuse Q2 write/ACK | fail closed; no local mint of epoch |
| stale owner map | REDIRECT/STALE_EPOCH | no old-owner new commit | no old-owner new commit | redirect at most bounded once |
| stale consumer generation | N/A | record remains deliverable | record remains deliverable | mutation denied; saved prior ACK outcome lookup is read-only |
| corrupted WAL before committed boundary | Q0 unaffected | partition unavailable until verified restoration | affected replica resyncs from healthy quorum | no silent checksum skip |
| overloaded subscriber | drop only its Q0 copies per policy | pause credits/group delivery; records retained per retention | same | cursor unchanged without ACK/disposition |
| total memory pressure | reject/bounded Q0 shed | reject before append; never lose committed | reject before proposal; never fake quorum | leave protected control/ACK reserves |
| DLQ commit succeeds, disposition fails | Q0 unaffected | source remains incomplete until terminal marker | same, stronger durability | retry idempotent DLQ and marker |
| DLQ commit outcome unknown | Q0 unaffected | source incomplete until resolved | same | no skipped source offset |

## Q1 local barrier checkpoints

~~~text
A request parsed
B admitted to bounded queue
C assigned temporary offset + WAL append attempted
D write() returned (page cache only)
E fsync barrier completed successfully (record + outcome + manifest as needed)
F PUB_ACK(COMMITTED_LOCAL) returned to client
~~~

Crash before E: record may be absent or present but not yet acknowledgeable; recovery scans checksum-valid prefix and cannot invent a success. Crash after E before F: publication committed but client sees OUTCOME_UNKNOWN; retry with identical sequence returns original result. After F with intact disk: record and dedup outcome must recover. After permanent loss: invalidate old incarnation explicitly.

## Q2 majority barrier checkpoints

~~~text
A leader validates policy/epoch
B proposal appended locally
C replicas receive and durably append
D majority durable commit established
E deterministic state machine applied
F PUB_ACK(COMMITTED_QUORUM) sent
~~~

A–C are **not** success. D/E vs F lost response is an uncertain client outcome but must resolve to one committed value. Raft leadership changes cannot reveal B/C tail as committed. Q2 consumer cursor may not advance before its own quorum-backed ACK command commit.

## Consumer ACK checkpoints

~~~text
A token delivered with current generation/lease
B ACK received and token validated
C ACK/terminal marker appended to local Q1 or replicated Q2 domain
D durability barrier reached
E cursor recomputed over contiguous completed prefix
F ACK_COMMITTED result sent
~~~

Crash before D permits redelivery and cannot move recovered cursor; crash after D before F permits saved success lookup, never a second cursor advancement. Stale A/B rejected. If gaps remain, E cannot cross them.

## DLQ and terminal protocol

\`SOURCE_COMMITTED → DLQ_PENDING → DLQ_COMMITTED → TERMINAL_COMMITTED → CLOSED_FOR_GROUP\`. An interrupted DLQ operation leaves source open; recover by \`disposition_id\` lookup and idempotent resend. Quarantine replaces DLQ_COMMITTED with durable QUARANTINED marker. Never turn max_retry, NACK, timeout or retention timer alone into terminal closure.

## Operational incident response

Each failure emits \`component, partition/shard, epoch, log_incarnation (where relevant), phase, error_code, retryable, last_known_commit, metadata_revision\` in bounded structured diagnostics. The SDK distinguishes RETRY_AFTER, NO_QUORUM, STORAGE_UNAVAILABLE, STALE_EPOCH, OUTCOME_UNKNOWN, REPLAY_NOT_AVAILABLE, STALE_GENERATION and DLQ_UNAVAILABLE.

## Recovery invariants

1. No acknowledged Q1 record lost after **process** crash with disk intact.
2. No acknowledged Q2 record lost after one of three independent nodes fails.
3. No new Q2 ACK with no quorum.
4. No two active owners for same epoch, including during transfer.
5. No consumer cursor skip over unclosed source offset.
6. No dedup conflict stored as successful second committed record.
7. No silent Q1 replay continuity after irrecoverable owner loss.
8. No unbounded memory growth during failure storms.

TEST-004/005/011/013/015/017 each map to these scenarios. Reduced MVP runs the subset touching current slice; full production matrix later.


## Executable B-02 process boundaries

`scripts/test_q1_faults.py` uses an explicitly fault-enabled loopback broker and external SDK processes. Marker/release events pause the storage owner before append, after complete append/before sync, and after durable sync/before the actor reply. A killed caller receives no fabricated result: SDK transport loss is OUTCOME_UNKNOWN. Recovery discards the first two uncommitted tails and preserves the third boundary. Lost committed replies settle through the same scoped token without another insertion. Received successful ACKs are journaled and synchronized by the parent outside the WAL directory before SIGKILL.

Injected append/partial-write, segment-sync, manifest-write/sync/rename and directory-sync errors never yield COMMITTED_LOCAL and poison the actor. Corrupted committed bytes or configuration identity disagreement fail startup without resetting history. A pending Q1 storage barrier does not stop Q0 publishes even with one Tokio worker. Shutdown drains accepted writes; a disappearing response remains unknown. These tests cover process termination on intact storage, not a physical power-cut or production qualification.
