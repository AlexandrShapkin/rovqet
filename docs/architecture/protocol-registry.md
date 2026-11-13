# Appendix P — Concrete reference wire registry for MVP implementation

**Scope:** resolves numeric and binary choices left open by the normative wire sketch, without changing its semantics. This is the **chosen architecture default**. Once the first golden vector is committed at M1, numbers become immutable for wire major 1.

## Byte-level grammar

~~~text
Frame =
  magic[2]("RQ" = 52 51 hex)
  major[1](01)
  flags[1]
  header_len[uvarint]
  payload_len[uvarint]
  frame_type[uvarint]
  request_id[u64 big-endian]
  repeated TLV fields, exactly header_len - size(frame_type) - 8 bytes
  payload[payload_len]
  [checksum_crc32c:u32 big-endian if flags bit0]
~~~

\`flags & 0x01\` means CRC32C present. All other flag bits are reserved in major 1 and MUST be zero unless negotiated as a capability. \`header_len\` is bounded independently at 4096 bytes; payload default ≤256 KiB, whole frame ≤1 MiB. Canonical uvarint uses unsigned LEB128: maximum ten bytes for u64, shortest representation only, reject >64-bit overflow or overlong encoding. Fixed integer widths are big endian. A TLV is \`tag:uvarint, length:uvarint, bytes[length]\`; \`field_number=tag>>1\`, \`critical=tag&1\`. Unknown critical fields fail; unknown optional fields are ignored after validation. One occurrence per singular field unless explicitly defined repeated.

Decoder order: confirm magic/version → parse bounded varint lengths without allocation → verify header/payload/full-frame lengths with checked arithmetic → parse frame type and request_id → validate TLV framing → optional checksum → frame-specific required fields → authorize → allocate bounded payload and dispatch. Never trust a client-supplied tenant ID as authority.

## Numeric frame types

| Code | Type | Payload |
|---:|---|---|
| 1 | HELLO | none |
| 2 | WELCOME | none |
| 3 | AUTH | none |
| 4 | AUTH_OK | none |
| 5 | PING | none |
| 6 | PONG | none |
| 7 | PUBLISH | opaque bytes |
| 8 | PUB_ACK | none |
| 9 | SUBSCRIBE | none |
| 10 | SUB_ACK | none |
| 11 | UNSUBSCRIBE | none |
| 12 | MESSAGE | opaque bytes |
| 13 | CONSUMER_ACK | none |
| 14 | CONSUMER_ACK_RESULT | none |
| 15 | CONSUMER_NACK | none |
| 16 | EXTEND_LEASE | none |
| 17 | FETCH | none |
| 18 | FLOW_CREDIT | none |
| 19 | REDIRECT | none |
| 20 | ERROR | UTF-8 diagnostics MAY be TLV, not raw payload |
| 21 | DRAIN | none |
| 22 | CLOSE | none |
| 23 | READ_FETCH | none; standalone Q1 inspection |
| 24 | READ_RECORD | committed application bytes; no lease |
| 25 | READ_END | none; terminal continuation/status |

## Field IDs, encoding and scope

All TLVs use \`(field_id << 1) | critical\` as encoded tag, **not** the field number alone. For example critical field 1 encodes tag 3.

| Field ID | Name | Type | Use |
|---:|---|---|---|
| 1 | protocol_minor | u16 BE | HELLO, WELCOME |
| 2 | capabilities | repeated u32 BE | HELLO, WELCOME |
| 3 | max_frame_bytes | u32 BE | HELLO, WELCOME |
| 4 | heartbeat_ms | u32 BE | HELLO, WELCOME |
| 5 | session_id | u128 BE | WELCOME |
| 6 | auth_method | u8 | AUTH |
| 7 | auth_data | bytes | AUTH; dev-only fake principal or future credential |
| 8 | principal_id | u64 BE | AUTH_OK, never caller authority |
| 9 | topic_name | canonical UTF-8 bytes | PUBLISH/SUBSCRIBE |
| 10 | topic_id | u64 BE | alternate to name when negotiated |
| 11 | partition_key | bytes | PUBLISH |
| 12 | qos | u8: 0=Q0,1=Q1,2=Q2 | PUBLISH/MESSAGE/PUB_ACK |
| 13 | priority | u8: 0=CONTROL,1=INTERACTIVE,2=TELEMETRY,3=BULK | PUBLISH/MESSAGE |
| 14 | ttl_ms | u32 BE | PUBLISH |
| 15 | message_id | u128 BE | PUBLISH/MESSAGE |
| 16 | producer_id | u128 BE | Q1/Q2 PUBLISH |
| 17 | producer_epoch | u64 BE | Q1/Q2 PUBLISH |
| 18 | producer_sequence | u64 BE | Q1/Q2 PUBLISH |
| 19 | partition_id | u32 BE | direct Q1 PUBLISH/PUB_ACK/MESSAGE/FETCH/ACK |
| 20 | log_incarnation | u128 BE | every durable offset reference |
| 21 | offset | u64 BE | every durable offset reference |
| 22 | pub_outcome | u8: 1=ADMITTED,2=COMMITTED_LOCAL,3=COMMITTED_QUORUM | PUB_ACK |
| 23 | subscription_id | u64 BE | SUB_ACK/UNSUBSCRIBE |
| 24 | group_id | u64 BE | group delivery, ACK and FETCH |
| 25 | group_generation | u64 BE | durable delivery/ACK |
| 26 | lease_id | u128 BE | durable delivery/ACK |
| 27 | ownership_epoch | u64 BE | durable delivery/ACK/REDIRECT |
| 28 | error_code | u16 BE | ERROR/rejected result |
| 29 | retry_after_ms | u32 BE | ERROR/DRAIN |
| 30 | owner_node_id | u64 BE | REDIRECT |
| 31 | max_messages | u32 BE | FETCH |
| 32 | max_bytes | u32 BE | FETCH |
| 33 | wait_ms | u32 BE | FETCH |
| 34 | flow_credit_messages | u32 BE | FLOW_CREDIT |
| 35 | flow_credit_bytes | u32 BE | FLOW_CREDIT |
| 36 | ack_outcome | u8: 1=ACK_COMMITTED,2=ACK_PENDING,3=REJECTED | CONSUMER_ACK_RESULT |
| 37 | delivery_attempt | u32 BE | MESSAGE/ACK |
| 38 | disposition_id | u128 BE | internal/optional terminal result |
| 39 | metadata_revision | u64 BE | REDIRECT/status |
| 40 | error_retryable | u8:0/1 | ERROR |
| 41 | payload_digest_sha256 | 32 bytes | durable publish optional integrity |
| 42 | filter | UTF-8 topic expression | SUBSCRIBE |
| 43 | tenant_id | u64 BE | response and internal only; never client authorization |
| 44 | ack_deadline_ms | u32 BE | MESSAGE |
| 45 | close_reason | u16 BE | CLOSE |
| 46 | nonce | u64 BE | PING/PONG correlation |
| 47 | source_log_incarnation | u128 BE | optional DLQ internal record |
| 48 | max_inflight | u32 BE | SUBSCRIBE/group configuration |
| 49 | read_status | u8:1 EMPTY,2 END,3 COUNT_LIMIT,4 BYTE_LIMIT,5 WAIT_EXPIRED | READ_END |
| 50 | has_more | u8:0/1 | READ_END |

Field IDs 51–127 reserved for compatible additions; every new field must be registered centrally. Keep name-based topic resolution for MVP until topic IDs are negotiated. Q0 PUBLISH does not need producer fields. Q1/Q2 requires producer_id/epoch/sequence and immutable class from policy. Public reference to an offset always includes incarnation; do not send offset alone as a durable cursor token.

## Required fields and message validation

| Frame | Required TLVs | Additional check |
|---|---|---|
| HELLO | minor, max_frame_bytes | major in fixed prefix; CAPABILITIES critical bit distinguishes required from optional; negotiate common minor/limits |
| WELCOME | minor, session_id, max_frame_bytes | server values replace requested maximum |
| AUTH | auth_method, auth_data | dev-only method restricted to loopback lab |
| AUTH_OK | principal_id | only server may issue |
| PUBLISH | topic_name or topic_id, partition_key, qos, message_id, ttl_ms | for Q1/Q2 producer_id, producer_epoch, producer_sequence required |
| PUB_ACK | qos, pub_outcome | durable result requires partition_id, log_incarnation, offset |
| SUBSCRIBE | filter or topic_name | validated topic syntax and subscription quota |
| SUB_ACK | subscription_id | bound to authenticated session |
| MESSAGE | topic_name or ID, qos, message_id | durable additionally group_id, generation, lease_id, incarnation, offset |
| CONSUMER_ACK, CONSUMER_NACK, EXTEND_LEASE | topic_name or topic_id, partition_id, group_id, generation, lease_id, ownership_epoch, incarnation, offset | full partition scope and previously delivered committed record |
| CONSUMER_ACK_RESULT | ack_outcome | correlation by identical request_id |
| FETCH | topic_name or topic_id, partition_id, group_id, incarnation, offset, max_bytes, max_messages | no unbounded or ambiguous group fetch |
| READ_FETCH | topic_name or topic_id, qos=1, partition_id, incarnation, offset, max_bytes, max_messages | optional wait_ms; no group/lease fields |
| READ_RECORD | tenant_id, topic_id, qos=1, partition_id, incarnation, offset, message_id | original committed payload; request correlation |
| READ_END | tenant_id, topic_id, qos=1, partition_id, incarnation, offset, read_status, has_more | mandatory terminal response; offset is continuation |
| FLOW_CREDIT | subscription_id or group_id, flow_credit_messages/bytes | cannot exceed negotiated credit |
| REDIRECT | owner_node_id, ownership_epoch, metadata_revision | bounded hop/retry |
| ERROR | error_code, error_retryable | diagnostic fields optional; never success |

For an absent/invalid required field return BAD_FRAME. Unknown optional TLVs are ignored; unknown critical TLVs return UNSUPPORTED_CAPABILITY. A duplicate singular TLV is BAD_FRAME. Do not expose internal host identifiers across tenants without an authorization policy.

## Stable error IDs (major 1)

~~~text
 1 BAD_FRAME                 2 UNSUPPORTED_VERSION
 3 UNSUPPORTED_CAPABILITY    4 UNAUTHENTICATED
 5 AUTH_EXPIRED              6 PERMISSION_DENIED
 7 QUOTA_EXCEEDED            8 SUBSCRIBER_LIMIT
 9 EGRESS_QUOTA             10 RESOURCE_EXHAUSTED
11 RETRY_AFTER              12 SERVER_DRAINING
13 OUTCOME_UNKNOWN          14 UNSUPPORTED_GUARANTEE
15 NO_QUORUM                16 STORAGE_UNAVAILABLE
17 STALE_EPOCH              18 STALE_GENERATION
19 LEASE_EXPIRED            20 SEQUENCE_GAP
21 SEQUENCE_CONFLICT        22 PRODUCER_FENCED
23 EXPIRED                  24 EXPIRED_BEFORE_DELIVERY
25 ACK_NOT_COMMITTED        26 REPLAY_NOT_AVAILABLE
27 DLQ_UNAVAILABLE
~~~

Error **classes and semantics** come from normative §5.1.1; numeric codes above are reference mapping. Client must interpret an unknown future code as UNKNOWN_REMOTE_ERROR while retaining retryable metadata without unsafe automatic retry.

## Producer and consumer retry framing

A producer persisted token is independent of request_id. A retry after socket loss uses original producer_id, epoch, sequence, message_id and content digest. Q1 loss of log incarnation causes explicit REPLAY_NOT_AVAILABLE or restart handshake boundary, not hidden new sequence. A consumer ACK uses original generation, lease and offset/incarnation; new stale-generation mutation fails even if the client reconnects. Already committed ACK may be checked read-only via repeat request_id/token.

## Golden fixture plan

Generate canonical vectors from a *reference encoder* and hand-checked decoder for: HELLO, AUTH, PUBLISH Q0, Q1 PUB_ACK, Q2 PUB_ACK, MESSAGE with incarnation, FETCH with old incarnation, CONSUMER_ACK/RESULT, ERROR each class, unknown optional field, unknown critical field, overlong varint, truncated payload, duplicate singular field, bad max_frame, checksum mismatch. Pin vectors in \`crates/protocol/tests/fixtures\` and assert byte-for-byte equivalence across codec revisions. The registry is design-frozen here; empirical freezing/validation is an M1 task.


B-02's implemented single-partition development route requires existing fields 19 (partition_id), 20 (log_incarnation) and 27 (ownership_epoch) on Q1 PUBLISH in addition to the generic row above. This fixes the direct-route profile without new numeric assignments. The Q1 response uses outcome 2 only after the actor barrier and includes the complete configured partition identity. General codec vectors remain unchanged; Q2 and consumer operations remain unsupported by the development server.

B-03 preserves every numeric assignment and the v1 storage encoding. The direct Q1
route validates optional field 41 against payload bytes on every attempt. Its effective
routing/priority is fixed; TTL is an admission deadline. Unsupported group/lease/
disposition publication attributes are rejected. Expired producer outcomes use existing
OUTCOME_UNKNOWN (13), never an invented success or a fresh insertion. The configured
24-hour-default window and independent producer/outcome quotas are advertised by the
operator-facing startup diagnostic and CLI; major 1 has no route-policy discovery field.

B-04 standalone READ_FETCH/READ_RECORD/READ_END is additive. Existing group FETCH and
group ACK/NACK/lease operations remain unsupported by the prototype. No placeholder leases
or consumer ACKs are produced. The exact standalone contract follows.

## B-04 standalone inspection extension (major 1, additive)

New frames **23 READ_FETCH**, **24 READ_RECORD**, **25 READ_END** use the existing
major-1 envelope, canonical varints and BE TLVs, flags zero (no optional CRC in this
profile). IDs 1–22 and fields 1–48, including FETCH/MESSAGE group requirements, stay
unchanged. New singular fields: **49 read_status:u8**, **50 has_more:u8 (0/1)**.
Older peers reject unknown frames/critical fields; new peers map unknown frame types to
UNSUPPORTED_CAPABILITY. This opt-in exchange never substitutes for group delivery.

READ_FETCH has no payload. Required: topic_name or topic_id, qos=1, partition_id,
log_incarnation, offset, max_messages:u32 (1–1024), max_bytes:u32 (78–4194304).
Optional wait_ms:u32 (0–30000, default zero); principal_id/tenant_id are assertions
checked against authenticated context, never authorization sources. Both topic forms
must match the configured route when supplied together. No group/lease/ACK fields
are legal. An odd/out-of-range offset or obsolete incarnation is REPLAY_NOT_AVAILABLE.

Each READ_RECORD echoes request_id and requires tenant_id, topic_id, partition_id,
log_incarnation, original offset, qos=1, message_id; payload is the original committed
application bytes. No producer-outcome metadata records or synthetic leases are sent.
READ_END echoes request_id and requires the same scope, qos=1, offset (continuation),
read_status and has_more. It has no payload. Status values are 1 EMPTY (offset zero,
no history), 2 END (current committed end), 3 COUNT_LIMIT, 4 BYTE_LIMIT, 5 WAIT_EXPIRED
(no record became available before a requested bounded wait ended). has_more is one
exactly for statuses 3/4 and zero otherwise. Empty/end without wait completes immediately;
nonzero wait at current end completes on new commit or expires boundedly. A bounded wait
that expires before any record becomes available returns status 5 with the requested
offset unchanged as continuation, so the continuation is zero only when the request
started at offset zero. End has precedence over count/byte limits when the same snapshot
is fully exhausted; count takes precedence over byte when both limits are reached. A
nonempty batch never has status 5.

Offsets remain sparse MESSAGE indexes 0,2,4,...; continuation is the last returned
MESSAGE offset plus two, or unchanged for an empty batch. Only READ_END makes a batch
complete. A stream failure, cancellation, ERROR or malformed response before READ_END
invalidates the entire batch. No consumer cursor or producer state is mutated.

max_bytes counts **all encoded READ_RECORD bytes plus READ_END**, including magic,
length varints and TLVs. Canonical response fields are ordered tenant, topic, partition,
incarnation, offset, qos, then message_id for records or status/has_more for end.
Canonical record header is 84 bytes: total = 89 + varint_size(payload_len) + payload_len.
Canonical READ_END is exactly 78 bytes. The actor uses this sizing at its existing read
seam so limits are exact, not estimates based on its memory-charge API. A first record
that cannot fit, or cannot fit the negotiated per-frame/payload limit, returns
RESOURCE_EXHAUSTED; no empty success or skipped offset. Extra memory reservations cover
encoding and retained batches; partition quotas may reject requests below hard ceilings.

The server serializes one batch per session, writes through a one-slot lossless reply
channel with per-write completion/timeout, and keeps the partition reservation until the
last retained write is released. Q0 delivery/credit queues and WAL independence remain
unchanged. Standalone readers must not pipeline requests within a batch; the Rust SDK
serializes requests and closes a cancelled/partial stream. The network task awaits the
actor asynchronously; filesystem IO remains on the existing partition owner.
