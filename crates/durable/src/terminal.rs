//! B-06 retry scheduling and durable terminal disposition (quarantine / Q1 DLQ).
//!
//! Records keep the v1 WAL envelope. New payloads are versioned: `RQRT` v1 for retry
//! metadata and `RQTD` v1 for terminal state. The DLQ message payload is `RQDL` v1 and is
//! published as a normal Q1 application message into a separate explicitly configured log.
use super::*;

const RETRY_MAGIC: &[u8; 6] = b"RQRT\0\x01";
const RETRY_BYTES: usize = 6 + 8 + 16 + 8 + 8 + 8 + 4 + 1 + 4 + 1;
const TERMINAL_MAGIC: &[u8; 6] = b"RQTD\0\x01";
const TERMINAL_BYTES: usize = 6 + 1 + 1 + 8 + 8 + 8 + 16 + 8 + 16 + 1 + 4 + 16 + 16 + 8;
const DLQ_MAGIC: &[u8; 6] = b"RQDL\0\x01";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TerminalPolicy {
    /// Retry exhaustion leaves the source incomplete; an operator must intervene.
    None,
    Quarantine,
    Dlq,
}
impl TerminalPolicy {
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "none" => Some(Self::None),
            "quarantine" => Some(Self::Quarantine),
            "dlq" => Some(Self::Dlq),
            _ => None,
        }
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TerminalKind {
    Quarantine = 1,
    Dlq = 2,
}
impl TryFrom<u8> for TerminalKind {
    type Error = GroupError;
    fn try_from(value: u8) -> std::result::Result<Self, GroupError> {
        match value {
            1 => Ok(Self::Quarantine),
            2 => Ok(Self::Dlq),
            _ => Err(GroupError::Corruption("terminal kind")),
        }
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TerminalState {
    Pending = 1,
    Committed = 2,
}
impl TryFrom<u8> for TerminalState {
    type Error = GroupError;
    fn try_from(value: u8) -> std::result::Result<Self, GroupError> {
        match value {
            1 => Ok(Self::Pending),
            2 => Ok(Self::Committed),
            _ => Err(GroupError::Corruption("terminal state")),
        }
    }
}
pub const REASON_CONSUMER_NACK: u8 = 1;
pub const REASON_RETRY_EXHAUSTED: u8 = 2;

/// Deterministic disposition identity for one source application record.
pub fn disposition_id(group_id: u64, incarnation: u128, offset: u64) -> u128 {
    let mut hash: u128 = 0x6c62_272e_07bb_0142_62b8_2175_6295_c58d;
    for byte in group_id
        .to_be_bytes()
        .iter()
        .chain(incarnation.to_be_bytes().iter())
        .chain(offset.to_be_bytes().iter())
    {
        hash ^= u128::from(*byte);
        hash = hash.wrapping_mul(0x0000_0000_0100_0000_0000_0000_0000_013b);
    }
    hash
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct RetryRecord {
    pub group_id: u64,
    pub incarnation: u128,
    pub offset: u64,
    pub generation: u64,
    pub ownership_epoch: u64,
    pub attempt: u32,
    pub reason: u8,
    pub delay_ms: u32,
    pub exhausted: bool,
}
impl RetryRecord {
    pub fn encode(&self) -> Vec<u8> {
        let mut payload = Vec::with_capacity(RETRY_BYTES);
        payload.extend_from_slice(RETRY_MAGIC);
        payload.extend_from_slice(&self.group_id.to_be_bytes());
        payload.extend_from_slice(&self.incarnation.to_be_bytes());
        payload.extend_from_slice(&self.offset.to_be_bytes());
        payload.extend_from_slice(&self.generation.to_be_bytes());
        payload.extend_from_slice(&self.ownership_epoch.to_be_bytes());
        payload.extend_from_slice(&self.attempt.to_be_bytes());
        payload.push(self.reason);
        payload.extend_from_slice(&self.delay_ms.to_be_bytes());
        payload.push(u8::from(self.exhausted));
        payload
    }
    pub fn decode(payload: &[u8]) -> std::result::Result<Self, GroupError> {
        if payload.len() != RETRY_BYTES || &payload[..6] != RETRY_MAGIC {
            return Err(GroupError::Corruption("retry record encoding"));
        }
        let mut at = 6;
        let mut take = |bytes: usize| {
            let slice = &payload[at..at + bytes];
            at += bytes;
            slice
        };
        let group_id = u64::from_be_bytes(take(8).try_into().unwrap());
        let incarnation = u128::from_be_bytes(take(16).try_into().unwrap());
        let offset = u64::from_be_bytes(take(8).try_into().unwrap());
        let generation = u64::from_be_bytes(take(8).try_into().unwrap());
        let ownership_epoch = u64::from_be_bytes(take(8).try_into().unwrap());
        let attempt = u32::from_be_bytes(take(4).try_into().unwrap());
        let reason = take(1)[0];
        let delay_ms = u32::from_be_bytes(take(4).try_into().unwrap());
        let exhausted = take(1)[0];
        if exhausted > 1 {
            return Err(GroupError::Corruption("retry exhausted flag"));
        }
        Ok(Self {
            group_id,
            incarnation,
            offset,
            generation,
            ownership_epoch,
            attempt,
            reason,
            delay_ms,
            exhausted: exhausted == 1,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct TerminalRecord {
    pub state: TerminalState,
    pub kind: TerminalKind,
    pub group_id: u64,
    pub generation: u64,
    pub ownership_epoch: u64,
    pub source_incarnation: u128,
    pub source_offset: u64,
    pub disposition_id: u128,
    pub reason: u8,
    pub attempts: u32,
    pub original_message_id: u128,
    pub dlq_incarnation: u128,
    pub dlq_offset: u64,
}
impl TerminalRecord {
    pub fn encode(&self) -> Vec<u8> {
        let mut payload = Vec::with_capacity(TERMINAL_BYTES);
        payload.extend_from_slice(TERMINAL_MAGIC);
        payload.push(self.state as u8);
        payload.push(self.kind as u8);
        payload.extend_from_slice(&self.group_id.to_be_bytes());
        payload.extend_from_slice(&self.generation.to_be_bytes());
        payload.extend_from_slice(&self.ownership_epoch.to_be_bytes());
        payload.extend_from_slice(&self.source_incarnation.to_be_bytes());
        payload.extend_from_slice(&self.source_offset.to_be_bytes());
        payload.extend_from_slice(&self.disposition_id.to_be_bytes());
        payload.push(self.reason);
        payload.extend_from_slice(&self.attempts.to_be_bytes());
        payload.extend_from_slice(&self.original_message_id.to_be_bytes());
        payload.extend_from_slice(&self.dlq_incarnation.to_be_bytes());
        payload.extend_from_slice(&self.dlq_offset.to_be_bytes());
        payload
    }
    pub fn decode(payload: &[u8]) -> std::result::Result<Self, GroupError> {
        if payload.len() != TERMINAL_BYTES || &payload[..6] != TERMINAL_MAGIC {
            return Err(GroupError::Corruption("terminal record encoding"));
        }
        let mut at = 6;
        let mut take = |bytes: usize| {
            let slice = &payload[at..at + bytes];
            at += bytes;
            slice
        };
        let state = TerminalState::try_from(take(1)[0])?;
        let kind = TerminalKind::try_from(take(1)[0])?;
        let group_id = u64::from_be_bytes(take(8).try_into().unwrap());
        let generation = u64::from_be_bytes(take(8).try_into().unwrap());
        let ownership_epoch = u64::from_be_bytes(take(8).try_into().unwrap());
        let source_incarnation = u128::from_be_bytes(take(16).try_into().unwrap());
        let source_offset = u64::from_be_bytes(take(8).try_into().unwrap());
        let disposition_id = u128::from_be_bytes(take(16).try_into().unwrap());
        let reason = take(1)[0];
        let attempts = u32::from_be_bytes(take(4).try_into().unwrap());
        let original_message_id = u128::from_be_bytes(take(16).try_into().unwrap());
        let dlq_incarnation = u128::from_be_bytes(take(16).try_into().unwrap());
        let dlq_offset = u64::from_be_bytes(take(8).try_into().unwrap());
        if disposition_id
            != super::terminal::disposition_id(group_id, source_incarnation, source_offset)
        {
            return Err(GroupError::Corruption("disposition identity"));
        }
        if state == TerminalState::Committed {
            match kind {
                TerminalKind::Dlq => {
                    if dlq_incarnation == 0 || dlq_offset == u64::MAX {
                        return Err(GroupError::Corruption("DLQ commit proof"));
                    }
                }
                TerminalKind::Quarantine => {
                    if dlq_incarnation != 0 {
                        return Err(GroupError::Corruption("quarantine DLQ fields"));
                    }
                }
            }
        }
        Ok(Self {
            state,
            kind,
            group_id,
            generation,
            ownership_epoch,
            source_incarnation,
            source_offset,
            disposition_id,
            reason,
            attempts,
            original_message_id,
            dlq_incarnation,
            dlq_offset,
        })
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct PendingTerminal {
    pub kind: TerminalKind,
    pub disposition_id: u128,
    pub generation: u64,
    pub ownership_epoch: u64,
    pub attempts: u32,
    pub original_message_id: u128,
    pub reason: u8,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct RetryState {
    pub attempt: u32,
    pub eligible_ms: u64,
    pub exhausted: bool,
}

/// One full DLQ message as handed to the destination publisher.
#[derive(Debug, Clone)]
pub struct DlqRecord<'a> {
    pub disposition_id: u128,
    pub group_id: u64,
    pub source_partition: PartitionRef,
    pub source_incarnation: u128,
    pub source_offset: u64,
    pub original_message_id: u128,
    pub reason: u8,
    pub attempts: u32,
    pub payload: &'a [u8],
}
#[derive(Debug)]
pub enum DlqError {
    Unavailable,
    Capacity,
    Conflict,
    Corruption(&'static str),
}
pub trait DlqPublisher: Send + Sync {
    /// Idempotent by disposition identity: retrying the same disposition must return the
    /// original committed destination offset without inserting a duplicate message.
    fn publish(&self, record: &DlqRecord<'_>) -> std::result::Result<DurableOffset, DlqError>;
    /// Proves that the destination record exists with the expected identity.
    fn verify(
        &self,
        offset: DurableOffset,
        disposition_id: u128,
        original_message_id: u128,
    ) -> std::result::Result<bool, DlqError>;
}

/// Versioned destination payload; the original application bytes are preserved verbatim.
pub fn dlq_payload(record: &DlqRecord<'_>) -> Vec<u8> {
    let mut payload =
        Vec::with_capacity(6 + 16 + 8 + 8 + 8 + 4 + 16 + 8 + 16 + 1 + 4 + record.payload.len());
    payload.extend_from_slice(DLQ_MAGIC);
    payload.extend_from_slice(&record.disposition_id.to_be_bytes());
    payload.extend_from_slice(&record.group_id.to_be_bytes());
    payload.extend_from_slice(&record.source_partition.tenant_id.to_be_bytes());
    payload.extend_from_slice(&record.source_partition.topic_id.to_be_bytes());
    payload.extend_from_slice(&record.source_partition.partition_id.to_be_bytes());
    payload.extend_from_slice(&record.source_incarnation.to_be_bytes());
    payload.extend_from_slice(&record.source_offset.to_be_bytes());
    payload.extend_from_slice(&record.original_message_id.to_be_bytes());
    payload.push(record.reason);
    payload.extend_from_slice(&record.attempts.to_be_bytes());
    payload.extend_from_slice(record.payload);
    payload
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DlqMetadata {
    pub disposition_id: u128,
    pub group_id: u64,
    pub tenant_id: u64,
    pub topic_id: u64,
    pub partition_id: u32,
    pub source_incarnation: u128,
    pub source_offset: u64,
    pub original_message_id: u128,
    pub reason: u8,
    pub attempts: u32,
    pub payload: Vec<u8>,
}
pub fn parse_dlq_payload(payload: &[u8]) -> std::result::Result<DlqMetadata, DlqError> {
    if payload.len() < 6 + 16 + 8 + 8 + 8 + 4 + 16 + 8 + 16 + 1 + 4 || &payload[..6] != DLQ_MAGIC {
        return Err(DlqError::Corruption("DLQ payload"));
    }
    let mut at = 6;
    let mut take = |bytes: usize| {
        let slice = &payload[at..at + bytes];
        at += bytes;
        slice
    };
    let disposition_id = u128::from_be_bytes(take(16).try_into().unwrap());
    let group_id = u64::from_be_bytes(take(8).try_into().unwrap());
    let tenant_id = u64::from_be_bytes(take(8).try_into().unwrap());
    let topic_id = u64::from_be_bytes(take(8).try_into().unwrap());
    let partition_id = u32::from_be_bytes(take(4).try_into().unwrap());
    let source_incarnation = u128::from_be_bytes(take(16).try_into().unwrap());
    let source_offset = u64::from_be_bytes(take(8).try_into().unwrap());
    let original_message_id = u128::from_be_bytes(take(16).try_into().unwrap());
    let reason = take(1)[0];
    let attempts = u32::from_be_bytes(take(4).try_into().unwrap());
    Ok(DlqMetadata {
        disposition_id,
        group_id,
        tenant_id,
        topic_id,
        partition_id,
        source_incarnation,
        source_offset,
        original_message_id,
        reason,
        attempts,
        payload: payload[at..].to_vec(),
    })
}

#[derive(Debug, Clone)]
pub struct NackRequest {
    pub partition: PartitionRef,
    pub group_id: u64,
    pub generation: u64,
    pub lease_id: u128,
    pub ownership_epoch: u64,
    pub offset: DurableOffset,
    pub attempt: u32,
    pub principal: u128,
    pub reason: u8,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NackOutcome {
    RetryScheduled { retry_after_ms: u32 },
    RetryExhausted,
    TerminalCommitted { disposition_id: u128 },
    TerminalPending { disposition_id: u128 },
    AlreadyClosed { disposition_id: u128 },
}
pub struct PendingNack {
    pub(super) reply: oneshot::Receiver<std::result::Result<NackOutcome, GroupError>>,
}
impl PendingNack {
    pub fn wait(self) -> std::result::Result<NackOutcome, GroupError> {
        self.reply
            .blocking_recv()
            .unwrap_or(Err(GroupError::Unavailable))
    }
    pub async fn wait_async(self) -> std::result::Result<NackOutcome, GroupError> {
        self.reply.await.unwrap_or(Err(GroupError::Unavailable))
    }
}
pub(super) struct NackCommand {
    pub request: NackRequest,
    pub reply: oneshot::Sender<std::result::Result<NackOutcome, GroupError>>,
    pub _reservation: Reservation,
}
