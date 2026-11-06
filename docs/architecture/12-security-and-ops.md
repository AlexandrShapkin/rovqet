# 12 — Security boundary, operations, configuration and production migration

Security is *not abolished*. The isolated MVP explicitly defers production controls so the team can validate the message-routing/durability hypothesis quickly. The eventual normative standard remains §7, §10 and M6 of requirements 1.1.1.

## Environments

| Mode | Exposure | Auth | TLS | Admin | Data |
|---|---|---|---|---|---|
| \`dev-local\` (initial) | loopback by default | static fake principal | optional off | local health/inspect only | disposable |
| \`lab-cluster\` (Q2 PoC) | physically isolated trusted lab/VPN | fixed per-node test token | optional off | bound to operator subnet | synthetic only |
| \`production\` (future) | controlled public/private | JWT/JWKS, ACL, mTLS inter-node | TLS 1.3 mandatory | separate HTTPS/RBAC/audit | actual user data |

Refuse \`dev-local\` startup on non-loopback listener unless an explicit \`--allow-insecure-lab-network\` override is set with a warning. This is a low-cost safeguard against accidental exposure; no formal approval mechanism. Lab configuration prints a prominent 'INSECURE RESEARCH PROFILE' banner.

## Config schema reference

~~~yaml
profile: dev-local
node:
  id: n1
  cluster_id: local-test
  data_dir: ./target/rovqet-data/n1
listener:
  bind: 127.0.0.1:7443
  transport: tcp
  tls: false
auth:
  mode: static-dev
  test_principal: "dev"
limits:
  max_frame_bytes: 1048576
  max_connections: 10000
  global_ram_budget_bytes: 1073741824
  subscriber_queue_bytes: 262144
  subscriber_queue_messages: 64
  max_inflight_per_session: 32
  max_topic_queue_messages: 4096
durability:
  q1:
    segment_bytes: 134217728
    batch_max_delay_ms: 2
    fsync: required-before-ack
  q2:
    enabled: false
cluster:
  enabled: false
topics:
  telemetry:
    partitions: 4
    allowed_qos: [Q0]
    priority: TELEMETRY
  events:
    partitions: 2
    allowed_qos: [Q1]
    priority: INTERACTIVE
~~~

All sample limits are provisional benchmark settings, **not** normative capacity guarantees. Every parser validates nonnegative sizes, configured memory bound, Q1 durability and class compatibility at startup. Production profile must refuse disabled TLS, missing auth and other normative violations.

## Local operations

Reference CLI subcommands: \`rovqet server --config <path>\`, \`rovqet ping\`, \`rovqet publish\`, \`rovqet subscribe\`, \`rovqet inspect partitions\`, \`rovqet inspect stats\`, \`rovqet bench smoke\`. Only \`server\` and SDK-driven smoke are required initially; later commands are thin facades, not a generic admin platform.

Data directory contains node/cluster manifest, Q1 local WAL trees, Q2 Raft trees, metadata snapshots, temporary recovery work and logs. Never open a directory with mismatched cluster_id/node_id and write over its contents. Recovery from corruption fails closed with a useful diagnostic. Never delete data as an automatic repair action.

## Minimal observability

Structured stderr logs with request ID, shard/partition, epoch, QoS, reason; counters for connections, publish outcomes, fan-out, queue and durable fsync/quorum; HDR/histogram latency as test instrumentation. Cardinality must be bounded: no per-message ID metrics labels. Collector outage cannot block data plane.

Production upgrade adds Prometheus/OTLP, audit events, dashboards and runbooks. Instrumentation records exact test profile and commit SHA.

## Safety of test and fault tooling

Chaos scripts operate only on paths under a disposable test root; no global \`rm -rf\` or network-wide iptables modifications. Test harness prints target processes/PIDs, retains oracle, and limits power-loss approximations. SIGKILL tests do **not** establish physical power-cut durability; record the limitation.

## Future security work

M6 adds TLS 1.3 public listener, inter-node mTLS, signed JWT/JWKS and expiry handling, ACL/tenant quotas, admin HTTPS, secret handling, parser fuzz, security tests, rolling upgrades, SBOM/dependency scan, backup/restore and 72h soak. Do not silently merge the MVP security exception into production config.

## Security work that cannot be ignored even in MVP

Frame length checked before allocation; invalid varint cannot allocate unbounded memory; do not store credentials in source code beyond clearly fake dev values; no remote public binding by default; no hidden code path that bypasses partition ownership/fencing; no false QoS ACK. These are correctness and basic experiment isolation, not expensive compliance bureaucracy.
