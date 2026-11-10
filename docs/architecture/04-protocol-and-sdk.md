# 04 — Wire protocol and Rust SDK implementation contract

The normative specification defines a versioned binary envelope and explicit error registry (§5). The [reference binary registry](protocol-registry.md) and `crates/protocol/src/registry.rs` freeze wire-major-1 frame IDs 1–22, error IDs 1–27 and field IDs 1–48. R0-05 preserves the A-01 implemented encoding and golden vectors; storage implementation does not allocate or renumber wire IDs. Reserved durable frames describe compatibility, not an implemented Q1/Q2 server capability.

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

## Frozen major-1 frame registry

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

Reserve 0 as invalid. Existing numeric assignments are immutable within wire major 1. Additions require a central registry entry and compatible codec fixtures; never assign conflicting IDs per component.

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

The central schema-to-wire table is [protocol-registry.md](protocol-registry.md); the executable registry is `crates/protocol/src/registry.rs`. Existing independent `crates/protocol/tests/fixtures/golden-v1.txt` vectors fix Q1/Q2 PUB_ACK and NO_QUORUM bytes, and codec tests fix HELLO and reject malformed lengths, versions, missing durable incarnation and unknown critical fields. `cargo test -p rovqet-protocol --locked` validates them. Add byte-exact fixtures when a reserved durable operation becomes executable; a Q1 PUB_ACK codec fixture does not authorize the current Q0 server to send durable success. Keep the 1 MiB parser limit and explicit smaller test limits.

## Future transport

An identical frame codec works over TCP/TLS and QUIC reliable streams. QUIC DATAGRAM can carry only Q0 with its own length delimiter. Never use transport ACK in place of PUB_ACK. No 0-RTT state-changing operation.


## B-02 implemented Q1 development profile

The opt-in TCP Q1 route uses the existing major-1 registry. In addition to the generic PUBLISH fields and producer ID/epoch/sequence, this directly addressed profile requires partition_id, log_incarnation and ownership_epoch. The server resolves topic_name against its explicit Q1 configuration and binds principal from successful AUTH; request identity claims cannot replace it. Successful Q1 PUB_ACK includes tenant/topic/partition IDs, incarnation and message offset with the exact request ID. No frame/field IDs or existing golden bytes changed.

`Client::publish_q1(topic, payload, Q1PublishOptions)` requires caller-owned producer ID, epoch, sequence, message ID, partition, incarnation, owner epoch and TTL. It validates response correlation, Q1/COMMITTED_LOCAL, partition and incarnation before decoding the offset. Transport loss, timeout, malformed/contradictory responses and OUTCOME_UNKNOWN errors produce `ClientError::OutcomeUnknown` and close the uncertain connection. Reconnect preserves caller-owned options; retry must explicitly reuse the original token and content. Definitive server errors remain typed. The original Q0 publish API remains compatible. B-03's timed dedup window and B-04 FETCH are not implemented by this API.
