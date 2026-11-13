# 09 — Cluster metadata, ownership, placement, routing and degradation

Reference inter-node message schemas and migration replies are in [inter-node-rpc.md](inter-node-rpc.md).

## Two consensus domains

**Metadata group:** cluster ID, membership, node addresses, topics, durable classes, ACL/role revision, desired partition placement, ownership epochs/leases and policy snapshot version. One 3-voter metadata Raft group for full three-node MVP; it is not consulted synchronously per data-plane message.

**Data groups:** Q2 durable-shard Raft groups, independent from metadata group and each other. Q1 persists locally and uses coordinated ownership epochs, not Q2 Raft replication.

## Metadata snapshot

~~~text
MetadataSnapshot {
  cluster_id, revision, issued_at, revalidate_deadline,
  nodes: Map<NodeId, Endpoint>,
  topics: Map<TopicId, TopicPolicy>,
  placements: Map<PartitionRef, {owner, ownership_epoch, lease_deadline,
                               desired_replicas, shard_id}>,
  acl_revision, capabilities
}
~~~

Only metadata leader issues committed topology/epoch changes. Data-plane caches immutable snapshot, swaps atomically at revision boundary, exposes status revision and expiration. Clock skew or node restart must never extend authority silently.

Reference MVP static test identities and immutable topic config still use the same \`Snapshot\` data type; real ACL revocation arrives later. Never make a new ownership epoch from gossip or TCP reconnect.

## Routing algorithm

For request R:
1. Hash \`(tenant,topic,partition_key)\` with versioned seed and fixed partition count to select logical partition.
2. Read one local snapshot; if absent, invalid or expired, fail with typed topology/authority error.
3. Validate authorized requested class, quotas and current owner epoch.
4. If local owner: submit to the owner actor and recheck epoch before commit.
5. If remote owner: send one bounded proxy request with request ID, epoch and original producer token. No unbounded redirect chain.
6. On stale epoch: return REDIRECT with current committed snapshot revision, then SDK refreshes once within bounded retry budget.
7. If Q2 leader differs from placement owner, reconcile/forward only under a legitimate authority; a local Raft election is not a permission to bypass global fencing.

MVP proxy forwarding can be implemented with the same length-delimited frame codec over authenticated test inter-node sockets on private network; production mTLS later.

## Ownership transition (Q1/Q2)

\`PREPARE→CATCH_UP→FENCE_OLD→ACTIVATE_NEW→CLEANUP\`. Persist migration ID + stage and make every stage idempotent. No two owners accept same epoch writes. During transition a request is redirected, retried idempotently or rejected; no false ACK.

Q1 CATCH_UP requires complete local WAL/snapshot transfer (all acknowledged records, dedup/cursor state, manifest, incarnation) plus delta synchronization. The new owner cannot advertise continuity unless it proves intact transfer. If disk gone, explicit new log incarnation and data-loss diagnostic; old producer/consumer streams must reset explicitly.

Q2 CATCH_UP requires Raft learner/snapshot catch-up and membership protocol; only the legitimate quorum/leader serves committed writes. Metadata placement epoch and Raft term are different numbers; both conditions need verification. Use library-supported joint configuration rather than uncoordinated group rewrites.

## Failure and partial control availability

When metadata quorum is absent, only already authorized operations may continue until cached security/ownership TTL expires, assuming class-specific storage/quorum is healthy. Frozen epoch cannot be self-renewed. On expiration fail closed for operations needing that authority. New topic/ACL/ownership mutations are impossible without metadata quorum.

| Condition | Q0 | Q1 | Q2 | Mutation |
|---|---|---|---|---|
| CP quorum lost, valid policy+lease | existing traffic allowed | local WAL allowed if current owner | data quorum allowed if current leader | no new metadata changes |
| CP lost and ownership lease expired | reject owner-dependent work | reject new commits/replay requiring owner | reject new commits | none |
| Q2 quorum lost, CP healthy | unaffected Q0 allowed | unaffected Q1 allowed | no new Q2 ACK; safe committed reads only | metadata may mutate separately |
| local Q1 disk EIO | Q0 unaffected if resources permit | affected partition unavailable | Q2 other healthy quorum may continue | policy changes only if CP healthy |
| stale/revoked identity | fail closed | fail closed | fail closed | fail closed |

This is a design summary; normative source is the full FR-CLU-011 operation matrix. Never broaden availability beyond it for MVP convenience.

## Security hardening boundary

In research MVP, use fixed credentials and a sandbox network to test routing logic. Control-plane authority still must be coherently versioned; testing a cluster without fencing invalidates the topology PoC. Production uses mTLS, JWT/JWKS, independently bounded auth and admin traffic and revocation TTL policies.

## Tests

Admit three nodes with unique IDs; kill metadata leader; kill Q2 data leader independently; stale map redirect; asymmetric partition; delayed reconnect; Q1 migration with intact WAL; lost Q1 owner with explicit incarnation reset; stale owner attempts publish and ACK; no metadata quorum plus expired lease; idempotent resumed migration after crash.

## C-01 implementation status

C-01 provides the network and routing foundation only. It implements the typed identities
(cluster, node, peer endpoint, metadata revision, partition reference, partition owner,
ownership epoch, lease validity, RPC request id, RPC kind and timeout budget), the inter-node
envelope and codec, a bounded peer transport, immutable versioned metadata snapshots and the
owner-aware routing decision.

The metadata source in C-01 is an explicitly configured static fixture, installed atomically
and validated (cluster identity, revision monotonicity, node references, duplicate entries,
policy compatibility and lease validity). It is a development authority fixture and not a
Raft-committed snapshot. A process restart starts without any installed authority, so a
restart can never extend ownership authorization. Authority is time-bounded from local
installation and expires fail-closed.

In Raft mode the source is the committed metadata of the metadata group instead: the broker
joins the group, installs every committed and applied snapshot into the metadata store and
re-installs the ownership authority whenever the committed revision changes. The store stays
empty until the first snapshot is committed, so the data plane fails closed while the group
has no committed metadata. A static fixture and Raft mode are mutually exclusive: a fixture
must never override committed Raft metadata, and a broker that is configured with both
refuses to start. The authority is always taken verbatim from the committed snapshot: no
epoch is minted, no lease is extended and no replacement owner is activated by an election.

Routing reads exactly one snapshot, validates the partition and epoch and then either serves
locally, forwards once to the configured owner or returns a typed stale/expired/no-metadata
error. A routed request is never routed again, which bounds redirect loops. A proxy never
substitutes the producer token, never mints an epoch and never reports COMMITTED_LOCAL
without the owner's own durable barrier.

Metadata consensus and membership management are implemented in C-02; ownership handover, WAL
migration, Q2 replication and majority acknowledgment remain C-02/C-03 work.

## C-02 ownership fencing protocol (implemented part)

The durable Q1 partition owns the fencing state, not only the routing layer. A partition in
standalone mode (no installed authority) keeps the previous single-node behaviour. A
partition with an installed authority is a fenced owner:

- `Authority { owner, epoch, lease }` is installed from committed control-plane metadata by
  `Partition::set_authority`. The command is ordered with durable mutations in the actor.
- Every durable mutation checks the authority before it starts: publication, deduplicated
  retry, consumer FETCH, consumer ACK, consumer NACK and terminal disposition (quarantine and
  DLQ) all fail closed with a stale-epoch error when this node is not the committed owner.
- The ownership lease is bounded and counted from local installation. An expired lease fails
  closed; a restart never extends it.
- An older epoch can never replace a newer authority, so a stale snapshot cannot un-fence a
  fenced owner.
- A mutation whose lease expires while its durability barrier runs is reported as an
  uncertain outcome, never as a definite failure or a false `COMMITTED_LOCAL`.

Safety model: a replacement owner may only activate after the previous owner has been
observed to stop accepting mutations (confirmed revocation) or after its lease has expired.
Until one of those conditions holds, the target stays inactive; availability is never
traded for single-owner safety. The metadata Raft group (implemented in C-02) provides the
committed transition that starts this protocol; the C-01 static fixture is only a
development authority source. A Raft election alone still proves no revocation: the old
owner keeps its authority until its lease validity interval ends, so a committed placement
revision does not by itself activate a replacement owner.
