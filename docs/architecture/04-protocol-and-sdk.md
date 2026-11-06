# 04 — Wire protocol and Rust SDK implementation contract

The normative specification defines a versioned binary envelope and explicit error registry (§5). This chapter fixes **reference MVP encodings** to prevent per-component re-design. Freeze exact numeric IDs and golden vectors at the M1 boundary; until then the registry below is a design decision, not yet an implemented standard.

## Frame layout and parser

~~~text
magic:2 ("RQ") | major:u8 (1) | flags:u8 |
header_len: canonical unsigned LEB128 | payload_len: canonical unsigned LEB128 |
frame_type: canonical unsigned LEB128 | request_id:u64 BE |
TLV: repeated (tag: canonical uvarint, len: canonical uvarint, bytes) |
payload: exactly payload_len bytes | optional checksum: CRC32C u32 BE
~~~

\`header_len\` counts bytes from \`frame_type\` through end of TLV; header length includes the fixed 8-byte \`request_id\` and excludes magic/version/length varints, payload and optional checksum. The complete frame is \`4 + varint_lengths + header_len + payload_len + checksum_size\`. The parser enforces max frame before buffer allocation, max TLV metadata, canonical varints and no integer overflow. Multi-byte fixed integers use network big endian; LEB128 is explicitly an exception.

TLV \`tag = (field_number << 1) | critical_bit\`; unknown critical tags fail UNSUPPORTED_CAPABILITY, unknown optional tags are skipped after bounds check. Duplicate singular fields and invalid UTF-8 names are rejected. A protocol registry assigns numeric field IDs before M1; do not silently invent incompatible IDs inside separate crates. For prototype, checksum absent by default over TLS/TCP; WAL always has its own checksums.

## Provisional frame registry

| ID | Frame | Request/response |
|---:|---|---|
| 1 | HELLO | client advertises version, capabilities and frame bounds |
| 2 | WELCOME | server chooses negotiated version, session ID and limits |
| 3 | AUTH | static test identity in loopback MVP; production credentials later |
| 4 | AUTH_OK | authenticated session activated |
| 5 | PING | client or server liveness |
| 6 | PONG | matched heartbeat |
| 7 | PUBLISH | envelope + opaque payload |
| 8 | PUB_ACK | ADMITTED, COMMITTED_LOCAL or COMMITTED_QUORUM; offset when durable |
| 9 | SUBSCRIBE | exact topic first; filter when enabled |
| 10 | SUB_ACK | subscription ID, effective policy |
| 11 | UNSUBSCRIBE | subscription ID |
| 12 | MESSAGE | delivery ID, QoS, offset/incarnation when durable |
| 13 | CONSUMER_ACK | delivery token, group generation and offset |
| 14 | CONSUMER_ACK_RESULT | ACK_COMMITTED / ACK_PENDING / typed failure |
| 15 | CONSUMER_NACK | retry request with reason |
| 16 | EXTEND_LEASE | request extension within configured bound |
| 17 | FETCH | log incarnation, offset, max messages/bytes, wait_ms |
| 18 | FLOW_CREDIT | bounded additional receive/delivery credit |
| 19 | REDIRECT | authoritative owner/node and epoch |
| 20 | ERROR | stable code/class/retry metadata |
| 21 | DRAIN | server shuts down or moves owner |
| 22 | CLOSE | final session close |

Reserve 0 as invalid. Numbers become immutable only when golden vectors are generated. Use stable named symbolic error codes from requirements; assign their IDs centrally by M1, not per component.

## Session sequence

\`CONNECT → HELLO → WELCOME → AUTH → AUTH_OK → ACTIVE\`. No PUBLISH/SUBSCRIBE before AUTH_OK. Version mismatch gives UNSUPPORTED_VERSION. In MVP test credentials are a fixed \`dev\` principal on loopback only; the session abstraction already carries \`principal_id\`, enabling production auth later.

Every request has u64 request ID unique among the outstanding requests in a session. Responses echo it. A reconnect may reuse request IDs in a new session but producer dedup uses a **persistent** independent identity/epoch/sequence tuple. SDK does not infer durable failure from socket loss.

## Encoding of durable identity

A durable record reference passed over wire is \`(tenant_id, topic_id, partition_id, durable_class, log_incarnation:u128, offset:u64)\`. Include \`log_incarnation\` in PUB_ACK, MESSAGE, FETCH, durable ACK tokens and REPLAY_NOT_AVAILABLE/current-incarnation response. The SDK rejects stale incarnation in all replay/ACK/retry paths; it does not treat the same integer offset in a new incarnation as the same record. This **explicitly closes** the log-incarnation wire gap identified after v1.1.1 review and must be verified before M1 registry freeze.

Producer stream identity includes \`principal_id, producer_id, producer_epoch, tenant/topic/partition/durable_lane\`. On irrecoverable Q1 history loss the SDK must negotiate the new incarnation and **not automatically replay old in-flight publishes with apparently fresh sequence numbers**; old outcomes become uncertain/lost within Q1's stated failure model. Resume only by explicit application choice/reset with a new scoped producer stream and visible warning. Do not allow resurrection of a stale producer epoch.

## Producer SDK API

~~~rust
struct PublishOptions { qos: QoS, priority: Priority, ttl: Duration, key: Bytes };
enum PublishOutcome { Admitted, CommittedLocal(DurableOffset), CommittedQuorum(DurableOffset) }
async fn publish(&self, topic: &str, body: Bytes, options: PublishOptions)
  -> Result<PublishOutcome, ClientError>;
async fn lookup_outcome(&self, token: ProducerToken) -> Result<Option<PublishOutcome>, ClientError>;
~~~

Client retries reuse identical content digest and scoped sequence. On TIMEOUT/disconnect after sending, result is OutcomeUnknown and retry/lookup, never definite failure. On Q2 NO_QUORUM surface the real error; do not silently resend as Q1.

## Subscriber/consumer API

~~~rust
async fn subscribe(&self, filter: TopicFilter) -> Result<Subscription, ClientError>;
async fn fetch(&self, group: GroupId, from: DurableOffset, limits: FetchLimits) -> Result<Vec<Delivery>, ClientError>;
async fn ack(&self, delivery: &Delivery) -> Result<AckOutcome, ClientError>;
~~~

A \`Delivery\` contains stable offset/incarnation, delivery ID, group generation, lease, ownership epoch. The SDK exposes no blanket 'exactly once processing' promise. ACK_COMMITTED is returned only after the appropriate Q1/Q2 durable barrier. An ACK timeout is unresolved, not a forced NACK; retry with original token can return a stored committed result read-only.

## Contract vectors

Before M1: create \`protocol/registry.yaml\` and committed hex/golden fixtures for every frame, malformed length and version cases, stale incarnation and unknown critical/optional tags. The parser must run with a fixed 1 MiB max frame and explicit test lower limits. Preserve a structured schema-to-wire table; tests perform encode→decode and compare expected binary exactly, not merely round-trip an internally wrong codec.

## Future transport

An identical frame codec works over TCP/TLS and QUIC reliable streams. QUIC DATAGRAM can carry only Q0 with its own length delimiter. Never use transport ACK in place of PUB_ACK. No 0-RTT state-changing operation.
