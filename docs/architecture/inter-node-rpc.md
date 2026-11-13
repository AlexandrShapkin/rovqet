# Appendix R — Intra-cluster protocol and ownership messages

**Purpose:** enough concrete types for agents implementing the three-node slice without inventing separate incompatible RPCs. Transport choice (raw TCP/TLS vs QUIC) is behind a channel interface; in lab use bounded TCP on a private network, in production mTLS.

## RPC envelope

~~~text
ClusterEnvelope {
  cluster_id:u128, source_node:u64, target_node:u64,
  request_id:u128, rpc_kind:u16, api_version:u16,
  metadata_revision:u64, ownership_epoch:u64,
  timeout_budget_ms:u32, body_len:u32, body:bytes
}
~~~

The monotonic deadline is only meaningful within one host; over the network send a bounded relative time-to-live plus observed local processing time, **not** a remote monotonic absolute timestamp. The \`deadline_mono_ms\` above is local sender state; transport serializes \`timeout_budget_ms:u32\` instead. Explicit correction: do **not** compare source monotonic ticks across nodes.

All operations must be bounded in request bytes, concurrency and retries; per-shard Raft AppendEntries and snapshots obey library's protocol and separate bandwidth budgets. Do not wrap custom business RPC around internal Raft election messages if the Raft adapter already defines their format.

## Application RPC kinds

| ID | RPC | Request fields | Reply / failure |
|---:|---|---|---|
| 1 | ROUTE_PUBLISH | partition_ref, QoS, producer token, payload digest/body, epoch | PUB_ACK or STALE_EPOCH/NO_QUORUM/unknown |
| 2 | ROUTE_FETCH | group, incarnation, offset, bounds, epoch | committed bounded batch or replay error |
| 3 | ROUTE_CONSUMER_ACK | delivery token, generation, epoch | ACK_COMMITTED/pending/fenced |
| 4 | ROUTE_Q0_FANOUT | topic, payload, recipient-node ID, expiry/budget | admission into bounded remote dispatch, not subscriber delivery |
| 5 | METADATA_SNAPSHOT_GET | known revision, node ID | full/delta immutable snapshot |
| 6 | OWNERSHIP_INSPECT | partition, expected epoch | current owner/epoch/lease |
| 7 | Q1_TRANSFER_PREPARE | migration ID, source/target, incarnation, manifest hash | accepted/rejected |
| 8 | Q1_TRANSFER_CHUNK | migration ID, chunk sequence, bytes, checksum | durable verified chunk receipt |
| 9 | Q1_TRANSFER_FINALIZE | migration ID, final checksum, last committed offset, dedup/cursor watermark | READY_FOR_FENCE or mismatch |
| 10 | HEALTH | node, storage and quorum state | bounded status, not an authority grant |

MVP can implement only ROUTE_PUBLISH/FETCH/ACK, METADATA_SNAPSHOT_GET and HEALTH first, then transfer RPCs when Q1 migration is actually enabled. Any unimplemented command returns UNSUPPORTED_CAPABILITY; never silently falls back to local speculative handling.

## RPC idempotency

Stable \`request_id\` links one proxy attempt across one hop; the producer token or ACK token is the *business* idempotency key. A proxy may return TIMEOUT while the owner committed; no definitive 'not written' claim. Q0 remote FANOUT is best effort and can duplicate under retry, as allowed by Q0; Q1/Q2 use scoped dedup. Transfer chunks use \`(migration_id, chunk_index, checksum)\` and are repeatable.

## Routing guards

Ingress may forward only to owner from a valid metadata revision. The owner validates its current epoch again immediately before mutation. A stale owner returns STALE_EPOCH; client or ingress may refresh metadata once and retry within bounded hop count. A redirected request must never be endlessly reforwarded between two stale nodes.

## Q1 transfer safety

- PREPARE: metadata records migration ID and freezes target identity; old owner remains sole writer.
- CATCH_UP: copy sealed segments, replay open tail, dedup/ACK state, manifest and current log incarnation. Validate checksums and last durable offset.
- FENCE_OLD: durably revoke old ownership epoch; reject fresh writes and stale ACKs at old owner.
- ACTIVATE_NEW: target gets new ownership epoch with verified entire history and same incarnation; or explicit fresh incarnation if history is irrecoverable.
- CLEANUP: delete old copy only under explicit retention/repair policy, never speculative during transfer.

A transfer cannot preserve continuity from an incomplete WAL copy. Treat a restarted transfer as idempotent by migration ID, not by timing or machine identity.

## Q2 Raft RPC delegation

The consensus adapter handles vote, append-entries, snapshot and membership requests. It owns term/index and rejects old-term responses; business RPC only forwards legitimate proposals to the chosen leader. Metadata placement epoch must be validated at the outer boundary and never derived from a local Raft election. The Q2 applied result includes logical partition and offset, not merely Raft index.

## CP outages and leases

An already-authorized owner may continue allowed work while its persisted lease/security snapshot remains valid, subject to Q1 disk or Q2 data quorum. On expiry, fail closed; do not grant a new lease locally. Restarts preserve the original expiration deadline. A current follower becoming leader is not automatically a new global owner and cannot extend CP authority.

## Fault fixtures

Stale metadata version, client proxy timeout after commit, duplicate transfer chunk, missing final transfer chunk, checksum mismatch, old owner sends after fencing, Q2 leader changed before proxy reply, CP lost during Q1 transfer, Q2 data quorum lost while CP works, asymmetric partition where old leader sees stale map.

## C-01 implementation note

C-01 implements the envelope above as a separate inter-node namespace with an explicit
portable binary layout: magic `RQCL`, `api_version`, `rpc_kind`, flags, `cluster_id`,
`source_node`, `target_node`, `request_id`, `metadata_revision`, `ownership_epoch`,
`timeout_budget_ms`, `body_len`, the bounded body and a trailing CRC32C. The cluster
namespace does not reuse or extend the client-facing frame registry; client frames and
golden vectors are unchanged.

The normative RPC kind numbering of the table above is used verbatim: 1 ROUTE_PUBLISH,
2 ROUTE_FETCH, 3 ROUTE_CONSUMER_ACK, 4 ROUTE_Q0_FANOUT, 5 METADATA_SNAPSHOT_GET,
6 OWNERSHIP_INSPECT, 7/8/9 Q1_TRANSFER_*, 10 HEALTH. Kinds 4 and 7-9 are answered with an
explicit unsupported result; unknown kinds and unsupported envelope versions are rejected.
Bodies use explicit big-endian fields with length-prefixed byte strings; no native Rust
structure is serialized.

Bounds chosen for the prototype (the frozen registry numbers remain authoritative for the
client protocol): 1 MiB maximum body, 64 outstanding peer requests, 64 concurrent peer
connections, 2 s connection establishment, 5 s request timeout and 60 s maximum advertised
timeout budget. A timeout after a request was written is reported as an uncertain outcome
(`OUTCOME_UNKNOWN` for client publications), never as a definite rollback.
