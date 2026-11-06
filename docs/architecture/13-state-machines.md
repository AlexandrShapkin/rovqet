# 13 — Formal state machines and transitions

This chapter is authoritative for **internal state progression** in the reference design, subject to the normative requirements. For diagrams see \`diagrams/states.puml\`. Tests must assert forbidden transitions.

## Session

States: NEW, HANDSHAKE, AUTHENTICATED, ACTIVE, DRAINING, CLOSED, FAILED.

| Current | Event/guard | Next | Output/effect |
|---|---|---|---|
| NEW | TCP accepted within connection budget | HANDSHAKE | create bounded decoder |
| HANDSHAKE | HELLO/versions valid | HANDSHAKE | WELCOME/negotiated caps |
| HANDSHAKE | AUTH successful | ACTIVE | AUTH_OK; principal bound |
| HANDSHAKE | timeout/unsupported | FAILED | typed error, close |
| ACTIVE | PUBLISH/SUBSCRIBE valid | ACTIVE | typed request dispatch |
| ACTIVE | operator drain | DRAINING | stop new work, settle outcomes |
| ACTIVE | idle timeout/disconnect | CLOSED | remove Q0 subscriptions |
| DRAINING | all settled or grace expired | CLOSED | response or unknown outcome |
| ANY nonclosed | malformed frame/transport fatal | FAILED | cancel session only |

No data frames before ACTIVE; no indefinite DRAINING and no fallback to unbound principal.

## Q0 publish

RECEIVED→VALIDATED→ADMITTED→QUEUED→DISPATCHED, with optional SHED/EXPIRED. Q0 \`ADMITTED\` acknowledges a bounded queue placement, *not* DISPATCHED or DELIVERED. A drop is only according to configured Q0 policy and must increment reason counters.

## Q1/Q2 publish

~~~text
RECEIVED -> VALIDATED -> ADMITTED -> APPENDED
    Q1: APPENDED -> FSYNC_DURABLE -> COMMITTED_LOCAL -> PUB_ACK_SENT
    Q2: APPENDED -> REPLICATING -> DURABLE_MAJORITY
                   -> APPLIED_COMMITTED -> COMMITTED_QUORUM -> PUB_ACK_SENT
    failures: REJECTED / STORAGE_UNAVAILABLE / NO_QUORUM / OUTCOME_UNKNOWN
~~~

Guard to VALIDATED: authorized class, current owner, TTL, quota and sequence acceptable. APPENDED is not success. COMMITTED_LOCAL/QUORUM is terminal for durable outcome; retries of same sequence return same outcome without new append. Q2 leader may not publish COMMITTED_QUORUM from local-only uncommitted tail.

## Q1 log incarnation

ACTIVE_INCAR(n) → OWNER_DISK_LOST → HISTORY_UNAVAILABLE. Only after explicit reset/authorized new owner: NEW_INCAR(n+1) → ACTIVE_INCAR(n+1). The \`n+1\` is conceptual: wire ID is a fresh unique u128, not necessarily monotone arithmetic. A request referring to n fails REPLAY_NOT_AVAILABLE; no offset carryover and no automatic resumption of producer dedup stream.

A completed Q1 migration with verified full transfer may retain the same incarnation, but only under ownership fencing and provable record history.

## Q2 shard

FOLLOWER → CANDIDATE → LEADER on library majority election; leader may propose only if metadata ownership epoch current and voter membership valid. On quorum loss LEADER→NO_QUORUM (no new committed writes) or FOLLOWER after step down. On recovery, snapshot install validated before committed reads/writes. The metadata ownership epoch is **not** a Raft term and is not renewed by Raft election.

## Consumer delivery/ACK

READY→LEASED→DELIVERED→ACK_RECEIVED→ACK_VALIDATED→ACK_PERSISTING→ACK_COMMITTED. Pending failure/timeout re-enters READY/RETRY_QUEUED without cursor advance. A stale-generation ACK goes to REJECTED with no mutation. Duplicate prior ACK returns a read-only saved outcome.

For every candidate offset i:

\`closed(i) = durable_valid_ack(i) OR durable_authorized_terminal_disposition(i)\`

\`cursor' = max k : all i in (cursor,k] have closed(i)\`.

Track out-of-order completed offsets in bounded sparse storage; no gap jumps. Cursor references a particular log incarnation.

## DLQ/quarantine

~~~text
READY -> RETRY_QUEUED -> MAX_RETRY -> DEAD_LETTER_PENDING
         -> DLQ_COMMITTED -> TERMINAL_COMMITTED -> CLOSED_FOR_GROUP
                             [cursor advances iff contiguous prefix]

MAX_RETRY -> QUARANTINE_PENDING -> QUARANTINED_DURABLE -> CLOSED_FOR_GROUP
~~~

If DLQ commit is unknown, remain DEAD_LETTER_PENDING, retry idempotently. If disposition marker fails after DLQ commit, source remains incomplete and retry records terminal marker. NACK, timeout, retry-count exhaustion or unavailable DLQ alone NEVER closes source.

## Metadata ownership

PREPARE→CATCH_UP→FENCE_OLD→ACTIVATE_NEW→CLEANUP. At every stage \`owner(epoch)\` is a single coordinated authority. Crash during stage is resumed idempotently from durable stage and snapshot revision, not guessed from live socket availability.

## Overload

NORMAL→PRESSURE→SHED→DRAIN/UNAVAILABLE. Recovery uses guarded low-watermarks, not immediate oscillation; a loss of Q2 quorum may force Q2 domain unavailable while Q0 remains active. No overloaded state downgrades Q2→Q1 or Q1→Q0.

## Model/property assertions

For any generated sequence of transitions:
- Once COMMITTED_LOCAL with intact disk, recovered log contains its record.
- Once COMMITTED_QUORUM, no allowed ≤1-node crash erases it.
- \`cursor_after >= cursor_before\` within a fixed incarnation, excluding explicit audited SEEK.
- A stale generation/epoch never changes durable state.
- Every admitted in-memory message remains accounted for in bounded resources until release/dispatch/drop.
- Terminal closure requires its proper durable barrier and exact source reference.
- Q1 permanent loss never produces silent same-incarnation continuity.

Use state-machine properties in unit tests; run adversarial schedules when implementing each actor.
