# 01 — System context, topologies and deployment contract

## Context

**External actors:** (1) publishers/sensors, (2) live subscribers, (3) durable workers, (4) administrator/experiment operator, (5) independent benchmark generator and correctness oracle, (6) optional external identity provider (production only). **External systems:** storage devices; cluster network; Prometheus/OTLP collectors (optional); NATS/MQTT baseline systems (not part of Rovqet).

Rovqet exposes one versioned native protocol and one Rust SDK. It is not a Kafka/MQTT compatibility layer. The broker owns transport sessions, topic routing, volatile Q0 fan-out, local Q1 logs, quorum Q2 logs, cursor state and control metadata. The client owns business effects and end-to-end idempotency beyond the broker.

## Single-node topology

Client SDK → TCP listener → session state machine → bounded admission → partition router → [Q0 fan-out | Q1 WAL/consumer group]. All state is in one process except durable files and independent test oracle. No Raft is needed for Q1. Q0 stays available while Q1 WAL is unavailable, subject to shared resource budgets. Each component exchanges typed messages; module boundaries imply **no network RPC within a node**.

MVP local-only listener defaults to \`127.0.0.1:7443\` with plain TCP and test credentials. Public \`0.0.0.0\` and production TLS are separately gated; never silently bind the prototype publicly.

## Three-node topology

Three broker processes N1/N2/N3. Each runs: the same data-plane; replicated metadata consensus group; Q2 Raft groups for assigned shards; optionally local Q1 WAL partitions. Clients may connect to any node.

- A client-connected ingress node resolves topic/partition from cached metadata snapshot, then forwards at most once to current owner.
- Owner checks current epoch, guarantees, budget and class, appends and returns typed outcome. On stale map, ingress returns or follows a bounded REDIRECT with authoritative epoch.
- Q0 fan-out: owner sends at most one logical delivery to a remote destination node with interested subscribers; destination performs local fan-out. No durable state per Q0 subscriber.
- Q1: only current authorized owner writes its local WAL; migration copies/verifies durable state and fences old owner. Unrecoverable owner loss creates an explicit new log incarnation; there is no Q2-style replica guarantee.
- Q2: owner is the legitimate Raft group leader for a replicated durable-shard. COMMITTED_QUORUM requires majority durability; leader loss triggers election and replay of only the committed prefix.
- Metadata control group (membership, ACL revision, placement and ownership) is a **distinct** consensus domain; it is not involved in each individual publication.

MVP static seed peers and three nodes with fixed ports and no external orchestrator. Each node has a distinct data directory and physical failure domain for quorum experiments. Do not run all Q2 members on one physical host and claim single-host-failure tolerance.

## Logical namespace

\`tenant/topic[/subtopic]\` → \`partition_key\` deterministic stable hash → \`partition_id\` → \`lane\`.

- Q0 uses the volatile lane; message sequence and offset are not durable.
- A durable partition has immutable \`durable_class=Q1|Q2\`; all offsets are scoped to \`(tenant,topic,partition,lane,log_incarnation)\`.
- Partitions of different classes can share a Rust StorageEngine implementation but not one Raft committed prefix.
- No global ordering across partitions, across lanes, or across log incarnations.
- ACL and priority are server policy; the client request cannot elevate either.

## Deployment: MVP vs production

| Aspect | MVP | Eventual normative production |
|---|---|---|
| Node processes | 1 for Q0/Q1; 3 for Q2 | 3+ with placement and maintenance |
| Protocol | TCP/plaintext only loopback/trusted isolated lab | TCP/TLS 1.3; QUIC tested alternate |
| Identity | static test principals and single test tenant | JWT/JWKS, mTLS inter-node, ACL/RBAC |
| Storage | local WAL, fsync, static RF3 files | versioned WAL+snapshots, backup/restore |
| Metadata | local config first; simple fixed membership Raft in slice C | versioned coordinated metadata and reconciliation |
| Operations | stdout structured log; \`/health\` loopback | HTTPS admin API, monitoring, audit, runbooks |
| Correctness | oracle and crash/quorum scripts | full TEST-001…017 and REL/PERF qualification |

## Trust and failure boundaries

Network connections are not authority. Session identity is established by handshake, topic policy by versioned snapshot, owner by epoch, Q2 commit by majority and consumer ACK by generation/lease + durability barrier. A successful TCP write is neither a broker \`PUB_ACK\` nor a consumer \`ACK_COMMITTED\`.

Separate failure domains: client session, partition actor, Q1 disk, Q2 shard quorum, metadata quorum and external measurement/oracle. A Q2 quorum outage must not automatically destroy Q0 sessions; a metadata outage cannot locally mint new ownership leases after expiry.

## Operational identifiers

\`node_id\` stable across restart; \`cluster_id\` prevents joining wrong cluster; \`tenant_id\`, \`topic_id\`, \`partition_id\` identify logical placement; \`shard_id\` identifies physical log; \`ownership_epoch\` fences routing; \`raft_term/index\` orders Q2 physical entries; \`log_incarnation\` scopes Q1 lost-history boundaries; \`group_id\`, \`generation\`, \`lease_id\` fence consumer ACK.

Expose these identifiers in debug traces and errors (subject to authorization), never conflate epoch, term and log incarnation.

## Data-path diagrams

See \`diagrams/workspace.dsl\` for C4 context/container/component structure and \`diagrams/flows.puml\` for message sequences.
