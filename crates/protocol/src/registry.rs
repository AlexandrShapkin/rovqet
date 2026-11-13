//! Centrally assigned major-v1 identifiers (architecture/protocol-registry.md).

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u64)]
pub enum FrameType {
    Hello = 1,
    Welcome = 2,
    Auth = 3,
    AuthOk = 4,
    Ping = 5,
    Pong = 6,
    Publish = 7,
    PubAck = 8,
    Subscribe = 9,
    SubAck = 10,
    Unsubscribe = 11,
    Message = 12,
    ConsumerAck = 13,
    ConsumerAckResult = 14,
    ConsumerNack = 15,
    ExtendLease = 16,
    Fetch = 17,
    FlowCredit = 18,
    Redirect = 19,
    Error = 20,
    Drain = 21,
    Close = 22,
    ReadFetch = 23,
    ReadRecord = 24,
    ReadEnd = 25,
    FetchEnd = 26,
}

impl TryFrom<u64> for FrameType {
    type Error = ();

    fn try_from(value: u64) -> Result<Self, ()> {
        use FrameType::*;
        Ok(match value {
            1 => Hello,
            2 => Welcome,
            3 => Auth,
            4 => AuthOk,
            5 => Ping,
            6 => Pong,
            7 => Publish,
            8 => PubAck,
            9 => Subscribe,
            10 => SubAck,
            11 => Unsubscribe,
            12 => Message,
            13 => ConsumerAck,
            14 => ConsumerAckResult,
            15 => ConsumerNack,
            16 => ExtendLease,
            17 => Fetch,
            18 => FlowCredit,
            19 => Redirect,
            20 => Error,
            21 => Drain,
            22 => Close,
            23 => ReadFetch,
            24 => ReadRecord,
            25 => ReadEnd,
            26 => FetchEnd,
            _ => return Err(()),
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u16)]
pub enum ErrorCode {
    BadFrame = 1,
    UnsupportedVersion = 2,
    UnsupportedCapability = 3,
    Unauthenticated = 4,
    AuthExpired = 5,
    PermissionDenied = 6,
    QuotaExceeded = 7,
    SubscriberLimit = 8,
    EgressQuota = 9,
    ResourceExhausted = 10,
    RetryAfter = 11,
    ServerDraining = 12,
    OutcomeUnknown = 13,
    UnsupportedGuarantee = 14,
    NoQuorum = 15,
    StorageUnavailable = 16,
    StaleEpoch = 17,
    StaleGeneration = 18,
    LeaseExpired = 19,
    SequenceGap = 20,
    SequenceConflict = 21,
    ProducerFenced = 22,
    Expired = 23,
    ExpiredBeforeDelivery = 24,
    AckNotCommitted = 25,
    ReplayNotAvailable = 26,
    DlqUnavailable = 27,
}

impl TryFrom<u16> for ErrorCode {
    type Error = ();

    fn try_from(value: u16) -> Result<Self, Self::Error> {
        use ErrorCode::*;
        Ok(match value {
            1 => BadFrame,
            2 => UnsupportedVersion,
            3 => UnsupportedCapability,
            4 => Unauthenticated,
            5 => AuthExpired,
            6 => PermissionDenied,
            7 => QuotaExceeded,
            8 => SubscriberLimit,
            9 => EgressQuota,
            10 => ResourceExhausted,
            11 => RetryAfter,
            12 => ServerDraining,
            13 => OutcomeUnknown,
            14 => UnsupportedGuarantee,
            15 => NoQuorum,
            16 => StorageUnavailable,
            17 => StaleEpoch,
            18 => StaleGeneration,
            19 => LeaseExpired,
            20 => SequenceGap,
            21 => SequenceConflict,
            22 => ProducerFenced,
            23 => Expired,
            24 => ExpiredBeforeDelivery,
            25 => AckNotCommitted,
            26 => ReplayNotAvailable,
            27 => DlqUnavailable,
            _ => return Err(()),
        })
    }
}

pub mod field {
    pub const PROTOCOL_MINOR: u64 = 1;
    pub const CAPABILITIES: u64 = 2;
    pub const MAX_FRAME_BYTES: u64 = 3;
    pub const HEARTBEAT_MS: u64 = 4;
    pub const SESSION_ID: u64 = 5;
    pub const AUTH_METHOD: u64 = 6;
    pub const AUTH_DATA: u64 = 7;
    pub const PRINCIPAL_ID: u64 = 8;
    pub const TOPIC_NAME: u64 = 9;
    pub const TOPIC_ID: u64 = 10;
    pub const PARTITION_KEY: u64 = 11;
    pub const QOS: u64 = 12;
    pub const PRIORITY: u64 = 13;
    pub const TTL_MS: u64 = 14;
    pub const MESSAGE_ID: u64 = 15;
    pub const PRODUCER_ID: u64 = 16;
    pub const PRODUCER_EPOCH: u64 = 17;
    pub const PRODUCER_SEQUENCE: u64 = 18;
    pub const PARTITION_ID: u64 = 19;
    pub const LOG_INCAR: u64 = 20;
    pub const OFFSET: u64 = 21;
    pub const PUB_OUTCOME: u64 = 22;
    pub const SUBSCRIPTION_ID: u64 = 23;
    pub const GROUP_ID: u64 = 24;
    pub const GROUP_GENERATION: u64 = 25;
    pub const LEASE_ID: u64 = 26;
    pub const OWNERSHIP_EPOCH: u64 = 27;
    pub const ERROR_CODE: u64 = 28;
    pub const RETRY_AFTER_MS: u64 = 29;
    pub const OWNER_NODE_ID: u64 = 30;
    pub const MAX_MESSAGES: u64 = 31;
    pub const MAX_BYTES: u64 = 32;
    pub const WAIT_MS: u64 = 33;
    pub const FLOW_CREDIT_MESSAGES: u64 = 34;
    pub const FLOW_CREDIT_BYTES: u64 = 35;
    pub const ACK_OUTCOME: u64 = 36;
    pub const DELIVERY_ATTEMPT: u64 = 37;
    pub const DISPOSITION_ID: u64 = 38;
    pub const METADATA_REVISION: u64 = 39;
    pub const ERROR_RETRYABLE: u64 = 40;
    pub const PAYLOAD_DIGEST_SHA256: u64 = 41;
    pub const FILTER: u64 = 42;
    pub const TENANT_ID: u64 = 43;
    pub const ACK_DEADLINE_MS: u64 = 44;
    pub const CLOSE_REASON: u64 = 45;
    pub const NONCE: u64 = 46;
    pub const SOURCE_LOG_INCAR: u64 = 47;
    pub const MAX_INFLIGHT: u64 = 48;
    pub const READ_STATUS: u64 = 49;
    pub const HAS_MORE: u64 = 50;
    pub const LAST_KNOWN: u64 = 50;
}
