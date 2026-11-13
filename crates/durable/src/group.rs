//! Durable consumer groups on one Q1 partition: leases, acknowledgements and cursors.
//!
//! Only acknowledgements, generation fences and cursor checkpoints are durable. Leases are
//! volatile delivery grants: after a restart or lease expiry an unacknowledged committed
//! message becomes eligible for redelivery with a higher delivery attempt. One serialized
//! actor thread owns every mutation.
use super::terminal::{
    self, DlqPublisher, DlqRecord, NackOutcome, NackRequest, PendingNack, PendingTerminal,
    RetryRecord, RetryState, TerminalKind, TerminalPolicy, TerminalRecord, TerminalState,
};
use super::*;
use rovqet_wal::RecordKind;
use std::collections::{BTreeSet, HashMap};

pub const MAX_GROUP_MESSAGES: usize = 1024;
pub const MAX_GROUP_BYTES: usize = 4 * 1024 * 1024;
pub const MAX_GROUP_WAIT: Duration = Duration::from_secs(30);
/// Conservative memory charges; wire sizing is applied separately by the adapter.
pub const GROUP_BATCH_BYTES: usize = 128;
pub const GROUP_MESSAGE_BYTES: usize = 128;

const ACK_MAGIC: &[u8; 6] = b"RQCA\0\x01";
const ACK_BYTES: usize = 6 + 8 + 8 + 8 + 16 + 8 + 4 + 16 + 16;
const CHECKPOINT_MAGIC: &[u8; 6] = b"RQCP\0\x01";

/// Bounds applied to one group. Defaults are prototype values, not calibrated capacity.
#[derive(Debug, Clone, Copy)]
pub struct GroupConfig {
    pub max_groups: usize,
    pub max_inflight: usize,
    pub max_sparse: usize,
    pub max_lease_bytes: usize,
    pub max_attempts: u32,
    /// Delay before the next delivery after a NACK; doubled per attempt up to `retry_max`.
    pub retry_initial: Duration,
    pub retry_max: Duration,
    pub terminal_policy: TerminalPolicy,
    pub max_terminal_pending: usize,
    pub max_retries: usize,
    pub lease: Duration,
    pub checkpoint_after: u32,
}
impl Default for GroupConfig {
    fn default() -> Self {
        Self {
            max_groups: 8,
            max_inflight: 256,
            max_sparse: 4096,
            max_lease_bytes: 4 * 1024 * 1024,
            max_attempts: 8,
            retry_initial: Duration::from_millis(100),
            retry_max: Duration::from_secs(30),
            terminal_policy: TerminalPolicy::None,
            max_terminal_pending: 64,
            max_retries: 4096,
            lease: Duration::from_secs(30),
            checkpoint_after: 64,
        }
    }
}

#[derive(Debug, Clone)]
pub struct GroupFetchRequest {
    pub partition: PartitionRef,
    pub group_id: u64,
    /// Zero claims a new generation; a nonzero value must match the current generation.
    pub generation: u64,
    /// Client continuation hint only; the durable group cursor governs delivery.
    pub from: DurableOffset,
    pub max_messages: usize,
    pub max_bytes: usize,
    pub wait: Duration,
}
#[derive(Debug, Clone)]
pub struct AckRequest {
    pub partition: PartitionRef,
    pub group_id: u64,
    pub generation: u64,
    pub lease_id: u128,
    pub ownership_epoch: u64,
    pub offset: DurableOffset,
    pub attempt: u32,
    pub principal: u128,
}
#[derive(Debug)]
pub enum GroupError {
    WrongPartition,
    IncarnationMismatch,
    InvalidRequest,
    GroupNotFound,
    StaleGeneration,
    NotLeased,
    Capacity,
    AckNotCommitted,
    DlqUnavailable,
    Storage(rovqet_wal::Error),
    Corruption(&'static str),
    Unavailable,
}
impl fmt::Display for GroupError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for GroupError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Storage(error) => Some(error),
            _ => None,
        }
    }
}
impl From<rovqet_wal::Error> for GroupError {
    fn from(error: rovqet_wal::Error) -> Self {
        Self::Storage(error)
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupDelivery {
    pub offset: DurableOffset,
    pub message_id: u128,
    pub payload: Vec<u8>,
    pub generation: u64,
    pub lease_id: u128,
    pub attempt: u32,
    pub ack_deadline_ms: u32,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GroupStop {
    /// Every committed application record is acknowledged.
    End,
    CountLimit,
    ByteLimit,
    /// Remaining records are currently leased, so no new delivery is possible yet.
    InFlight,
    WaitExpired,
}
#[derive(Debug)]
pub struct GroupFetchBatch {
    pub partition: PartitionRef,
    pub group_id: u64,
    pub generation: u64,
    pub deliveries: Vec<GroupDelivery>,
    pub next: DurableOffset,
    pub has_more: bool,
    pub stop: GroupStop,
    pub response_bytes: usize,
    _reservation: Reservation,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AckResult {
    pub offset: DurableOffset,
    pub generation: u64,
}
pub struct PendingGroupFetch {
    reply: oneshot::Receiver<std::result::Result<GroupFetchBatch, GroupError>>,
}
impl PendingGroupFetch {
    pub fn wait(self) -> std::result::Result<GroupFetchBatch, GroupError> {
        self.reply
            .blocking_recv()
            .unwrap_or(Err(GroupError::Unavailable))
    }
    pub async fn wait_async(self) -> std::result::Result<GroupFetchBatch, GroupError> {
        self.reply.await.unwrap_or(Err(GroupError::Unavailable))
    }
}
pub struct PendingAck {
    reply: oneshot::Receiver<std::result::Result<AckResult, GroupError>>,
}
impl PendingAck {
    pub fn wait(self) -> std::result::Result<AckResult, GroupError> {
        self.reply
            .blocking_recv()
            .unwrap_or(Err(GroupError::Unavailable))
    }
    pub async fn wait_async(self) -> std::result::Result<AckResult, GroupError> {
        self.reply.await.unwrap_or(Err(GroupError::Unavailable))
    }
}
pub(super) struct GroupFetchCommand {
    pub request: GroupFetchRequest,
    pub deadline: Instant,
    pub reply: oneshot::Sender<std::result::Result<GroupFetchBatch, GroupError>>,
    pub reservation: Reservation,
}
pub(super) struct AckCommand {
    pub request: AckRequest,
    pub reply: oneshot::Sender<std::result::Result<AckResult, GroupError>>,
    pub _reservation: Reservation,
}
#[derive(Clone)]
struct Lease {
    offset: u64,
    message_id: u128,
    attempt: u32,
    generation: u64,
    deadline: Instant,
    ack_deadline_ms: u32,
    bytes: usize,
}
pub(super) struct GroupState {
    generation: u64,
    /// Offset of the first application message not yet acknowledged, or the append watermark
    /// when every committed message is acknowledged.
    cursor: u64,
    acked: BTreeSet<u64>,
    leases: HashMap<u128, Lease>,
    attempts: HashMap<u64, u32>,
    retries: HashMap<u64, RetryState>,
    pending: HashMap<u64, PendingTerminal>,
    terminals: HashMap<u64, u128>,
    next_lease: u128,
    ack_records: u32,
}
pub(super) struct MessageMeta {
    pub offset: u64,
    pub message_id: u128,
}
impl GroupState {
    fn new(cursor: u64) -> Self {
        Self {
            generation: 0,
            cursor,
            acked: BTreeSet::new(),
            leases: HashMap::new(),
            attempts: HashMap::new(),
            retries: HashMap::new(),
            pending: HashMap::new(),
            terminals: HashMap::new(),
            next_lease: 1,
            ack_records: 0,
        }
    }
}
/// Moves the cursor over the contiguous acknowledged prefix, normalising gaps between
/// application records. `watermark` is the append position used when nothing remains unacked.
fn advance_cursor(messages: &[MessageMeta], group: &mut GroupState, watermark: u64) {
    let mut index = messages.partition_point(|m| m.offset < group.cursor);
    while index < messages.len() && group.acked.contains(&messages[index].offset) {
        index += 1;
    }
    group.cursor = messages.get(index).map_or(watermark, |m| m.offset);
    let cursor = group.cursor;
    group.acked = group.acked.split_off(&cursor);
}
fn expire_leases(group: &mut GroupState, now: Instant) {
    group.leases.retain(|_, lease| lease.deadline > now);
}
pub(super) struct ConsumerAckRecord {
    pub group_id: u64,
    pub generation: u64,
    pub ownership_epoch: u64,
    pub lease_id: u128,
    pub offset: u64,
    pub attempt: u32,
    pub principal: u128,
    pub message_id: u128,
}
impl ConsumerAckRecord {
    pub fn encode(
        group_id: u64,
        generation: u64,
        ownership_epoch: u64,
        lease_id: u128,
        offset: u64,
        attempt: u32,
        principal: u128,
        message_id: u128,
    ) -> Vec<u8> {
        let mut payload = Vec::with_capacity(ACK_BYTES);
        payload.extend_from_slice(ACK_MAGIC);
        payload.extend_from_slice(&group_id.to_be_bytes());
        payload.extend_from_slice(&generation.to_be_bytes());
        payload.extend_from_slice(&ownership_epoch.to_be_bytes());
        payload.extend_from_slice(&lease_id.to_be_bytes());
        payload.extend_from_slice(&offset.to_be_bytes());
        payload.extend_from_slice(&attempt.to_be_bytes());
        payload.extend_from_slice(&principal.to_be_bytes());
        payload.extend_from_slice(&message_id.to_be_bytes());
        payload
    }
    pub fn decode(payload: &[u8]) -> std::result::Result<Self, GroupError> {
        if payload.len() != ACK_BYTES || &payload[..6] != ACK_MAGIC {
            return Err(GroupError::Corruption("consumer ACK encoding"));
        }
        let mut position = 6;
        let mut take = |bytes: usize| {
            let slice = &payload[position..position + bytes];
            position += bytes;
            slice
        };
        let group_id = u64::from_be_bytes(take(8).try_into().unwrap());
        let generation = u64::from_be_bytes(take(8).try_into().unwrap());
        let ownership_epoch = u64::from_be_bytes(take(8).try_into().unwrap());
        let lease_id = u128::from_be_bytes(take(16).try_into().unwrap());
        let offset = u64::from_be_bytes(take(8).try_into().unwrap());
        let attempt = u32::from_be_bytes(take(4).try_into().unwrap());
        let principal = u128::from_be_bytes(take(16).try_into().unwrap());
        let message_id = u128::from_be_bytes(take(16).try_into().unwrap());
        Ok(Self {
            group_id,
            generation,
            ownership_epoch,
            lease_id,
            offset,
            attempt,
            principal,
            message_id,
        })
    }
}
pub(super) struct CursorCheckpoint {
    pub group_id: u64,
    pub generation: u64,
    pub cursor: u64,
    pub acked: Vec<u64>,
}
impl CursorCheckpoint {
    pub fn encode(group_id: u64, generation: u64, cursor: u64, acked: &BTreeSet<u64>) -> Vec<u8> {
        let mut payload = Vec::with_capacity(6 + 24 + 4 + acked.len() * 8);
        payload.extend_from_slice(CHECKPOINT_MAGIC);
        payload.extend_from_slice(&group_id.to_be_bytes());
        payload.extend_from_slice(&generation.to_be_bytes());
        payload.extend_from_slice(&cursor.to_be_bytes());
        payload.extend_from_slice(&u32::try_from(acked.len()).unwrap_or(u32::MAX).to_be_bytes());
        for offset in acked {
            payload.extend_from_slice(&offset.to_be_bytes());
        }
        payload
    }
    pub fn decode(payload: &[u8], max_sparse: usize) -> std::result::Result<Self, GroupError> {
        if payload.len() < 6 + 8 + 8 + 8 + 4 || &payload[..6] != CHECKPOINT_MAGIC {
            return Err(GroupError::Corruption("cursor checkpoint encoding"));
        }
        let group_id = u64::from_be_bytes(payload[6..14].try_into().unwrap());
        let generation = u64::from_be_bytes(payload[14..22].try_into().unwrap());
        let cursor = u64::from_be_bytes(payload[22..30].try_into().unwrap());
        let count = u32::from_be_bytes(payload[30..34].try_into().unwrap()) as usize;
        if count > max_sparse || payload.len() != 34 + count * 8 {
            return Err(GroupError::Corruption("cursor checkpoint size"));
        }
        let mut acked = Vec::with_capacity(count);
        for index in 0..count {
            let start = 34 + index * 8;
            acked.push(u64::from_be_bytes(
                payload[start..start + 8].try_into().unwrap(),
            ));
        }
        if acked.windows(2).any(|pair| pair[0] >= pair[1]) {
            return Err(GroupError::Corruption("cursor checkpoint order"));
        }
        Ok(Self {
            group_id,
            generation,
            cursor,
            acked,
        })
    }
}
impl Partition {
    /// Nonblocking group admission. `generation == 0` claims and durably fences a new generation.
    pub fn try_fetch_group(
        &self,
        request: GroupFetchRequest,
    ) -> std::result::Result<PendingGroupFetch, GroupError> {
        if request.partition != self.config.partition {
            return Err(GroupError::WrongPartition);
        }
        if request.max_messages == 0
            || request.max_messages > MAX_GROUP_MESSAGES
            || request.max_bytes < GROUP_BATCH_BYTES
            || request.max_bytes > MAX_GROUP_BYTES
            || request.wait > MAX_GROUP_WAIT
            || request.group_id == 0
        {
            return Err(GroupError::InvalidRequest);
        }
        let charge = self
            .max_record_bytes
            .checked_add(request.max_bytes)
            .and_then(|n| n.checked_add(request.max_messages * GROUP_MESSAGE_BYTES))
            .and_then(|n| n.checked_add(GROUP_BATCH_BYTES))
            .ok_or(GroupError::Capacity)?;
        let reservation =
            Budget::reserve(&[self.budget.clone()], charge).ok_or(GroupError::Capacity)?;
        let (reply, receiver) = oneshot::channel();
        let deadline = Instant::now()
            .checked_add(request.wait)
            .ok_or(GroupError::InvalidRequest)?;
        let command = Command::GroupFetch(GroupFetchCommand {
            request,
            deadline,
            reply,
            reservation,
        });
        match self
            .sender
            .as_ref()
            .ok_or(GroupError::Unavailable)?
            .try_send(command)
        {
            Ok(()) => Ok(PendingGroupFetch { reply: receiver }),
            Err(TrySendError::Full(_)) => Err(GroupError::Capacity),
            Err(TrySendError::Disconnected(_)) => Err(GroupError::Unavailable),
        }
    }
    /// Acknowledges one leased delivery. Success is returned only after the ACK record is durable.
    /// NACK one delivery. It schedules retry state or a terminal transition; it never
    /// acknowledges successful processing or closes the source on its own.
    pub fn try_nack(&self, request: NackRequest) -> std::result::Result<PendingNack, GroupError> {
        if request.partition != self.config.partition {
            return Err(GroupError::WrongPartition);
        }
        if request.group_id == 0 || request.lease_id == 0 {
            return Err(GroupError::InvalidRequest);
        }
        let reservation =
            Budget::reserve(&[self.budget.clone()], 512).ok_or(GroupError::Capacity)?;
        let (reply, receiver) = oneshot::channel();
        let command = Command::Nack(terminal::NackCommand {
            request,
            reply,
            _reservation: reservation,
        });
        match self
            .sender
            .as_ref()
            .ok_or(GroupError::Unavailable)?
            .try_send(command)
        {
            Ok(()) => Ok(PendingNack { reply: receiver }),
            Err(TrySendError::Full(_)) => Err(GroupError::Capacity),
            Err(TrySendError::Disconnected(_)) => Err(GroupError::Unavailable),
        }
    }
    pub fn try_ack(&self, request: AckRequest) -> std::result::Result<PendingAck, GroupError> {
        if request.partition != self.config.partition {
            return Err(GroupError::WrongPartition);
        }
        if request.group_id == 0 || request.lease_id == 0 {
            return Err(GroupError::InvalidRequest);
        }
        let reservation = Budget::reserve(&[self.budget.clone()], ACK_BYTES + 2 * RECORD_OVERHEAD)
            .ok_or(GroupError::Capacity)?;
        let (reply, receiver) = oneshot::channel();
        let command = Command::Ack(AckCommand {
            request,
            reply,
            _reservation: reservation,
        });
        match self
            .sender
            .as_ref()
            .ok_or(GroupError::Unavailable)?
            .try_send(command)
        {
            Ok(()) => Ok(PendingAck { reply: receiver }),
            Err(TrySendError::Full(_)) => Err(GroupError::Capacity),
            Err(TrySendError::Disconnected(_)) => Err(GroupError::Unavailable),
        }
    }
}
impl State {
    /// Installs the committed ownership authority. Standalone partitions keep no authority.
    /// An older epoch never replaces a newer one: committed metadata is monotonic.
    pub(super) fn set_authority(&mut self, authority: Option<Authority>) {
        if let (Some(current), Some(next)) = (self.fence.authority(), authority) {
            if next.epoch < current.epoch {
                return;
            }
        }
        self.fence.install(authority, Instant::now());
    }
    /// Validates a durable mutation against the installed ownership authority.
    pub(super) fn check_authority(&self) -> std::result::Result<(), Error> {
        match self.fence.check(self.config.node_id, None, Instant::now()) {
            Ok(()) => Ok(()),
            Err(FenceError::ExpiredLease) => Err(Error::FenceExpired),
            Err(_) => Err(Error::Fenced),
        }
    }
    fn next_lease_id(group: &mut GroupState) -> u128 {
        let id = group.next_lease;
        group.next_lease = group.next_lease.wrapping_add(1).max(1);
        id
    }
    fn first_application_offset(&self) -> u64 {
        self.messages.first().map_or(self.next, |m| m.offset)
    }
    /// Applies the group-relevant consequence of a newly committed application message.
    pub(super) fn note_message(&mut self, offset: u64, message_id: u128) {
        self.messages.push(MessageMeta { offset, message_id });
        self.message_index.insert(offset, message_id);
        let watermark = self.next;
        let messages = &self.messages;
        for group in self.groups.values_mut() {
            advance_cursor(messages, group, watermark);
        }
    }
    /// Recomputes every cursor against the final recovered watermark.
    pub(super) fn finalize_cursors(&mut self) {
        let watermark = self.next;
        let messages = &self.messages;
        for group in self.groups.values_mut() {
            advance_cursor(messages, group, watermark);
        }
    }
    pub(super) fn apply_group_record(&mut self, record: &Record) -> Result<()> {
        match record.kind {
            RecordKind::ConsumerAck => {
                let ack =
                    ConsumerAckRecord::decode(&record.payload).map_err(|error| match error {
                        GroupError::Corruption(reason) => Error::Corruption(reason),
                        _ => Error::Corruption("consumer ACK encoding"),
                    })?;
                if !self.message_index.contains_key(&ack.offset) {
                    return Err(Error::Corruption("consumer ACK without message"));
                }
                let cursor = self.first_application_offset();
                let group = self
                    .groups
                    .entry(ack.group_id)
                    .or_insert_with(|| GroupState::new(cursor));
                if ack.generation > group.generation {
                    // A durable ACK cannot belong to a generation that was never fenced in.
                    return Err(Error::Corruption("consumer ACK generation"));
                }
                group.acked.insert(ack.offset);
                if group.acked.len() > self.config.group.max_sparse {
                    return Err(Error::Capacity);
                }
                let watermark = group.cursor;
                advance_cursor(&self.messages, group, watermark);
                Ok(())
            }
            RecordKind::CursorCheckpoint => {
                let checkpoint =
                    CursorCheckpoint::decode(&record.payload, self.config.group.max_sparse)
                        .map_err(|_| Error::Corruption("cursor checkpoint encoding"))?;
                if checkpoint.cursor > record.logical_offset {
                    return Err(Error::Corruption("cursor checkpoint ahead of append"));
                }
                for offset in &checkpoint.acked {
                    if !self.message_index.contains_key(offset) {
                        return Err(Error::Corruption("checkpoint ACK without message"));
                    }
                }
                let cursor = self.first_application_offset();
                let group = self
                    .groups
                    .entry(checkpoint.group_id)
                    .or_insert_with(|| GroupState::new(cursor));
                if checkpoint.generation < group.generation {
                    return Err(Error::Corruption("generation regression"));
                }
                group.generation = checkpoint.generation;
                group.cursor = checkpoint.cursor;
                group.acked = checkpoint.acked.into_iter().collect();
                let watermark = group.cursor;
                advance_cursor(&self.messages, group, watermark);
                Ok(())
            }
            RecordKind::Retry => {
                let retry = RetryRecord::decode(&record.payload)
                    .map_err(|_| Error::Corruption("retry record encoding"))?;
                if !self.message_index.contains_key(&retry.offset) {
                    return Err(Error::Corruption("retry without message"));
                }
                if retry.ownership_epoch > self.config.ownership_epoch {
                    return Err(Error::StaleOwner);
                }
                let cursor = self.first_application_offset();
                let group = self
                    .groups
                    .entry(retry.group_id)
                    .or_insert_with(|| GroupState::new(cursor));
                if retry.generation > group.generation {
                    return Err(Error::Corruption("retry generation"));
                }
                group.attempts.insert(
                    retry.offset,
                    group
                        .attempts
                        .get(&retry.offset)
                        .copied()
                        .unwrap_or(0)
                        .max(retry.attempt),
                );
                if group.retries.len() >= self.config.group.max_retries
                    && !group.retries.contains_key(&retry.offset)
                {
                    return Err(Error::Capacity);
                }
                // Monotonic wall time is unavailable across restart; an already-scheduled
                // retry becomes immediately eligible rather than being lost or shortened
                // into a terminal decision.
                group.retries.insert(
                    retry.offset,
                    RetryState {
                        attempt: retry.attempt,
                        eligible_ms: self.logical_time,
                        exhausted: retry.exhausted,
                    },
                );
                Ok(())
            }
            RecordKind::TerminalDisposition => {
                let terminal = TerminalRecord::decode(&record.payload)
                    .map_err(|_| Error::Corruption("terminal record encoding"))?;
                if !self.message_index.contains_key(&terminal.source_offset) {
                    return Err(Error::Corruption("terminal without message"));
                }
                if terminal.ownership_epoch > self.config.ownership_epoch {
                    return Err(Error::StaleOwner);
                }
                let expected_message = self.message_index[&terminal.source_offset];
                if expected_message != terminal.original_message_id {
                    return Err(Error::Corruption("terminal message identity"));
                }
                let cursor = self.first_application_offset();
                let group = self
                    .groups
                    .entry(terminal.group_id)
                    .or_insert_with(|| GroupState::new(cursor));
                if terminal.generation > group.generation {
                    return Err(Error::Corruption("terminal generation"));
                }
                match terminal.state {
                    TerminalState::Pending => {
                        if terminal.dlq_incarnation != 0 || terminal.dlq_offset != 0 {
                            return Err(Error::Corruption("pending DLQ proof"));
                        }
                        if group.pending.len() >= self.config.group.max_terminal_pending
                            && !group.pending.contains_key(&terminal.source_offset)
                        {
                            return Err(Error::Capacity);
                        }
                        group.pending.insert(
                            terminal.source_offset,
                            PendingTerminal {
                                kind: terminal.kind,
                                disposition_id: terminal.disposition_id,
                                generation: terminal.generation,
                                ownership_epoch: terminal.ownership_epoch,
                                attempts: terminal.attempts,
                                original_message_id: terminal.original_message_id,
                                reason: terminal.reason,
                            },
                        );
                        self.pending_terminal.insert(
                            (terminal.group_id, terminal.source_offset),
                            PendingTerminal {
                                kind: terminal.kind,
                                disposition_id: terminal.disposition_id,
                                generation: terminal.generation,
                                ownership_epoch: terminal.ownership_epoch,
                                attempts: terminal.attempts,
                                original_message_id: terminal.original_message_id,
                                reason: terminal.reason,
                            },
                        );
                    }
                    TerminalState::Committed => {
                        if terminal.kind == TerminalKind::Dlq {
                            self.recovered_dlq_commits.push((
                                terminal.disposition_id,
                                terminal.dlq_incarnation,
                                terminal.dlq_offset,
                                terminal.original_message_id,
                            ));
                        }
                        group.pending.remove(&terminal.source_offset);
                        group
                            .terminals
                            .insert(terminal.source_offset, terminal.disposition_id);
                        group.retries.remove(&terminal.source_offset);
                        group
                            .leases
                            .retain(|_, lease| lease.offset != terminal.source_offset);
                        group.acked.insert(terminal.source_offset);
                        let watermark = group.cursor;
                        advance_cursor(&self.messages, group, watermark);
                        self.pending_terminal
                            .remove(&(terminal.group_id, terminal.source_offset));
                    }
                }
                Ok(())
            }
            RecordKind::Tombstone => Err(Error::Corruption("unsupported tombstone metadata")),
            _ => Err(Error::Corruption("unexpected consumer metadata")),
        }
    }
    fn append_checkpoint(
        &mut self,
        payload: Vec<u8>,
        trigger: Option<u64>,
    ) -> std::result::Result<(), GroupError> {
        #[cfg(not(feature = "fault-injection"))]
        let _ = trigger;
        let record = Record {
            kind: RecordKind::CursorCheckpoint,
            logical_offset: self.next,
            owner_epoch: self.config.ownership_epoch,
            producer_id: 0,
            producer_epoch: 0,
            sequence: 0,
            message_id: 0,
            server_timestamp: self.logical_time,
            payload,
        };
        self.wal.append(record)?;
        #[cfg(feature = "fault-injection")]
        if let Some(offset) = trigger {
            self.test_barrier(TestStage::CheckpointAfterAppendBeforeSync, offset)
                .map_err(|_| GroupError::AckNotCommitted)?;
        }
        self.wal.sync()?;
        self.next = self.next.checked_add(1).ok_or(GroupError::Capacity)?;
        self.committed
            .store(self.next, std::sync::atomic::Ordering::Release);
        Ok(())
    }
    fn claim_generation(&mut self, group_id: u64) -> std::result::Result<u64, GroupError> {
        if !self.groups.contains_key(&group_id) && self.groups.len() >= self.config.group.max_groups
        {
            return Err(GroupError::Capacity);
        }
        let cursor = self
            .groups
            .get(&group_id)
            .map_or_else(|| self.first_application_offset(), |group| group.cursor);
        let generation = self
            .groups
            .get(&group_id)
            .map_or(1, |group| group.generation.saturating_add(1).max(1));
        let candidate = GroupState {
            generation,
            cursor,
            acked: self
                .groups
                .get(&group_id)
                .map(|group| group.acked.clone())
                .unwrap_or_default(),
            leases: HashMap::new(),
            attempts: self
                .groups
                .get(&group_id)
                .map(|group| group.attempts.clone())
                .unwrap_or_default(),
            retries: self
                .groups
                .get(&group_id)
                .map(|group| group.retries.clone())
                .unwrap_or_default(),
            pending: self
                .groups
                .get(&group_id)
                .map(|group| group.pending.clone())
                .unwrap_or_default(),
            terminals: self
                .groups
                .get(&group_id)
                .map(|group| group.terminals.clone())
                .unwrap_or_default(),
            next_lease: self
                .groups
                .get(&group_id)
                .map_or(1, |group| group.next_lease),
            ack_records: 0,
        };
        let payload = CursorCheckpoint::encode(
            group_id,
            candidate.generation,
            candidate.cursor,
            &candidate.acked,
        );
        self.append_checkpoint(payload, Some(0))?;
        // Only after the fence is durable does the new generation hold authority.
        match self.groups.get_mut(&group_id) {
            Some(group) => {
                group.generation = generation;
                group.leases.clear();
            }
            None => {
                self.groups.insert(group_id, candidate);
            }
        }
        Ok(generation)
    }
    pub(super) fn complete_group_fetches(
        &mut self,
        waits: &mut Vec<GroupFetchCommand>,
        shutdown: bool,
    ) {
        let mut index = 0;
        while index < waits.len() {
            if waits[index].reply.is_closed() {
                waits.swap_remove(index);
                continue;
            }
            let command = &waits[index];
            let expired = Instant::now() >= command.deadline;
            if !shutdown && !self.unhealthy && !expired && !self.group_progress(&command.request) {
                index += 1;
                continue;
            }
            let command = waits.swap_remove(index);
            let result = self.group_fetch(command.request, command.reservation, expired);
            let _ = command.reply.send(result);
        }
    }
    /// True when a waiting fetch can stop: new deliverable records, an acked lease or shutdown.
    fn group_progress(&mut self, request: &GroupFetchRequest) -> bool {
        let Some(group) = self.groups.get(&request.group_id) else {
            return true;
        };
        if request.generation != 0 && request.generation != group.generation {
            return true;
        }
        let now = Instant::now();
        let mut capacity = group.leases.len() < self.config.group.max_inflight;
        if group.leases.values().any(|lease| lease.deadline <= now) {
            capacity = true;
        }
        if capacity {
            let index = self.messages.partition_point(|m| m.offset < group.cursor);
            for message in self.messages.iter().skip(index) {
                if !group.acked.contains(&message.offset)
                    && !group
                        .leases
                        .values()
                        .any(|lease| lease.offset == message.offset)
                {
                    return true;
                }
            }
        }
        // A fetch also has to observe cursor progress (ACK) while it waits.
        group
            .acked
            .iter()
            .next()
            .is_some_and(|&offset| offset == group.cursor)
    }
    fn group_fetch(
        &mut self,
        r: GroupFetchRequest,
        reservation: Reservation,
        expired: bool,
    ) -> std::result::Result<GroupFetchBatch, GroupError> {
        if self.unhealthy {
            return Err(GroupError::Unavailable);
        }
        if r.from.log_incarnation != self.wal_config.incarnation {
            return Err(GroupError::IncarnationMismatch);
        }
        if r.from.sequence > self.next {
            return Err(GroupError::InvalidRequest);
        }
        if r.generation == 0 {
            self.claim_generation(r.group_id)?;
        }
        // Fencing: a partition that is not the committed owner serves no consumer deliveries.
        self.check_authority()
            .map_err(|_| GroupError::StaleGeneration)?;
        self.advance_clock();
        // Retry exhaustion and expired attempts can require a durable terminal transition
        // before selection. Gather candidates, then release the group borrow for IO.
        let mut exhausted = Vec::new();
        let Some(group) = self.groups.get_mut(&r.group_id) else {
            return Err(GroupError::GroupNotFound);
        };
        if r.generation != 0 && r.generation != group.generation {
            return Err(GroupError::StaleGeneration);
        }
        let now = Instant::now();
        for lease in group.leases.values() {
            if lease.deadline <= now
                && lease.attempt >= self.config.group.max_attempts
                && self.config.group.terminal_policy != TerminalPolicy::None
            {
                exhausted.push(lease.offset);
            }
        }
        for (offset, retry) in &group.retries {
            if retry.exhausted && self.config.group.terminal_policy != TerminalPolicy::None {
                exhausted.push(*offset);
            }
        }
        expire_leases(group, now);
        for offset in exhausted {
            self.trigger_exhausted(r.group_id, offset)?;
        }
        let Some(group) = self.groups.get_mut(&r.group_id) else {
            return Err(GroupError::GroupNotFound);
        };
        let generation = group.generation;
        let mut response_bytes = GROUP_BATCH_BYTES;
        let mut deliveries = Vec::with_capacity(r.max_messages);
        let mut leased_bytes = group
            .leases
            .values()
            .map(|lease| lease.bytes)
            .sum::<usize>();
        let mut limit = None;
        let mut has_more = false;
        let index = self.messages.partition_point(|m| m.offset < group.cursor);
        for message in self.messages.iter().skip(index) {
            if group.acked.contains(&message.offset) {
                continue;
            }
            if group
                .leases
                .values()
                .any(|lease| lease.offset == message.offset)
            {
                has_more = true;
                limit.get_or_insert(GroupStop::InFlight);
                continue;
            }
            if let Some(retry) = group.retries.get(&message.offset) {
                if retry.exhausted {
                    // Exhaustion never closes the record by itself. Without a terminal
                    // policy the group stalls with the offset still incomplete.
                    has_more = true;
                    limit.get_or_insert(GroupStop::InFlight);
                    continue;
                }
                if retry.eligible_ms > self.logical_time {
                    has_more = true;
                    limit.get_or_insert(GroupStop::InFlight);
                    continue;
                }
            }
            let record = self
                .wal
                .read(
                    DurableOffset {
                        log_incarnation: self.wal_config.incarnation,
                        sequence: message.offset,
                    },
                    1,
                    self.wal_config.max_record_bytes,
                )
                .map_err(|error| {
                    self.unhealthy = true;
                    GroupError::Storage(error)
                })?;
            if record.len() != 1
                || record[0].kind != RecordKind::Message
                || record[0].message_id != message.message_id
            {
                self.unhealthy = true;
                return Err(GroupError::Corruption("leased message identity"));
            }
            let payload = record.into_iter().next().unwrap().payload;
            let required = GROUP_MESSAGE_BYTES
                .checked_add(payload.len())
                .ok_or(GroupError::Capacity)?;
            let attempt = group
                .attempts
                .get(&message.offset)
                .copied()
                .unwrap_or(0)
                .saturating_add(1);
            if attempt > self.config.group.max_attempts {
                has_more = true;
                limit.get_or_insert(GroupStop::InFlight);
                continue;
            }
            if response_bytes
                .checked_add(required)
                .is_none_or(|n| n > r.max_bytes)
                || deliveries.len() == r.max_messages
                || group.leases.len() >= self.config.group.max_inflight
                || leased_bytes.saturating_add(required) > self.config.group.max_lease_bytes
            {
                has_more = true;
                limit.get_or_insert(if deliveries.len() == r.max_messages {
                    GroupStop::CountLimit
                } else {
                    GroupStop::ByteLimit
                });
                break;
            }
            response_bytes += required;
            leased_bytes += required;
            group.attempts.insert(message.offset, attempt);
            group.retries.remove(&message.offset);
            let lease_id = Self::next_lease_id(group);
            let deadline = Instant::now()
                .checked_add(self.config.group.lease)
                .ok_or(GroupError::InvalidRequest)?;
            let ack_deadline_ms =
                u32::try_from(self.config.group.lease.as_millis()).unwrap_or(u32::MAX);
            group.leases.insert(
                lease_id,
                Lease {
                    offset: message.offset,
                    message_id: message.message_id,
                    attempt,
                    generation,
                    deadline,
                    ack_deadline_ms,
                    bytes: required,
                },
            );
            deliveries.push(GroupDelivery {
                offset: DurableOffset {
                    log_incarnation: self.wal_config.incarnation,
                    sequence: message.offset,
                },
                message_id: message.message_id,
                payload,
                generation,
                lease_id,
                attempt,
                ack_deadline_ms,
            });
        }
        let stop = if has_more {
            limit.unwrap_or(GroupStop::InFlight)
        } else if expired && !r.wait.is_zero() && deliveries.is_empty() {
            GroupStop::WaitExpired
        } else {
            GroupStop::End
        };
        let next = group.cursor;
        Ok(GroupFetchBatch {
            partition: r.partition,
            group_id: r.group_id,
            generation,
            deliveries,
            next: DurableOffset {
                log_incarnation: self.wal_config.incarnation,
                sequence: next,
            },
            has_more,
            stop,
            response_bytes,
            _reservation: reservation,
        })
    }
    pub(super) fn ack(&mut self, r: AckRequest) -> std::result::Result<AckResult, GroupError> {
        if self.unhealthy {
            return Err(GroupError::Unavailable);
        }
        // Fencing: consumer acknowledgements are fenced exactly like publications.
        self.check_authority()
            .map_err(|_| GroupError::StaleGeneration)?;
        if r.ownership_epoch != self.config.ownership_epoch {
            return Err(GroupError::StaleGeneration);
        }
        let offset = r.offset.sequence;
        let Some(&message_id) = self.message_index.get(&offset) else {
            return Err(GroupError::NotLeased);
        };
        let Some(group) = self.groups.get(&r.group_id) else {
            return Err(GroupError::GroupNotFound);
        };
        if offset < group.cursor || group.acked.contains(&offset) {
            // Saved committed outcome: read-only, no new durable mutation, valid across fencing.
            return Ok(AckResult {
                offset: r.offset,
                generation: group.generation,
            });
        }
        if r.generation != group.generation {
            return Err(GroupError::StaleGeneration);
        }
        let now = Instant::now();
        let lease = group.leases.get(&r.lease_id).cloned();
        let generation = group.generation;
        let sparse = group.acked.len();
        let checkpoint_due =
            group.ack_records.saturating_add(1) >= self.config.group.checkpoint_after;
        let Some(lease) = lease else {
            return Err(GroupError::NotLeased);
        };
        if lease.deadline <= now {
            return Err(GroupError::NotLeased);
        }
        if lease.offset != offset
            || lease.attempt != r.attempt
            || lease.generation != generation
            || lease.message_id != message_id
        {
            return Err(GroupError::NotLeased);
        }
        // The sparse set is bounded; accepting the gap-closing cursor offset never grows it.
        if sparse >= self.config.group.max_sparse && offset != group.cursor {
            return Err(GroupError::Capacity);
        }
        let payload = ConsumerAckRecord::encode(
            r.group_id,
            generation,
            r.ownership_epoch,
            r.lease_id,
            offset,
            r.attempt,
            r.principal,
            message_id,
        );
        let record = Record {
            kind: RecordKind::ConsumerAck,
            logical_offset: self.next,
            owner_epoch: self.config.ownership_epoch,
            producer_id: 0,
            producer_epoch: 0,
            sequence: u64::from(r.attempt),
            message_id,
            server_timestamp: self.logical_time,
            payload,
        };
        let record_offset = self.next;
        let result = (|| {
            #[cfg(feature = "fault-injection")]
            self.test_barrier(TestStage::BeforeAppend, offset)
                .map_err(|_| GroupError::AckNotCommitted)?;
            self.wal.append(record)?;
            #[cfg(feature = "fault-injection")]
            self.test_barrier(TestStage::AfterAppendBeforeSync, offset)
                .map_err(|_| GroupError::AckNotCommitted)?;
            self.wal.sync()?;
            #[cfg(feature = "fault-injection")]
            self.test_barrier(TestStage::AfterSyncBeforeReply, offset)
                .map_err(|_| GroupError::AckNotCommitted)?;
            Ok::<_, GroupError>(())
        })();
        if result.is_err() {
            self.unhealthy = true;
            // The append may or may not be durable; never report success.
            return Err(GroupError::AckNotCommitted);
        }
        self.next = record_offset.checked_add(1).ok_or(GroupError::Capacity)?;
        self.committed
            .store(self.next, std::sync::atomic::Ordering::Release);
        let group = self.groups.get_mut(&r.group_id).unwrap();
        group.leases.remove(&r.lease_id);
        group.acked.insert(offset);
        group.ack_records = group.ack_records.saturating_add(1);
        advance_cursor(&self.messages, group, self.next);
        if checkpoint_due {
            // Compaction includes the sparse set, so no committed ACK is lost.
            let payload =
                CursorCheckpoint::encode(r.group_id, group.generation, group.cursor, &group.acked);
            let _ = self.append_checkpoint(payload, Some(offset));
            if let Some(group) = self.groups.get_mut(&r.group_id) {
                group.ack_records = 0;
            }
        }
        Ok(AckResult {
            offset: r.offset,
            generation,
        })
    }
    fn retry_delay_ms(&self, attempt: u32) -> u64 {
        let initial = self.config.group.retry_initial.as_millis().max(1) as u64;
        let maximum = self
            .config
            .group
            .retry_max
            .as_millis()
            .max(u128::from(initial)) as u64;
        let shift = attempt.saturating_sub(1).min(31);
        initial.saturating_mul(1u64 << shift).min(maximum)
    }
    fn append_retry(
        &mut self,
        group_id: u64,
        offset: u64,
        attempt: u32,
        reason: u8,
        exhausted: bool,
    ) -> std::result::Result<u32, GroupError> {
        let delay = if exhausted {
            u32::MAX
        } else {
            u32::try_from(self.retry_delay_ms(attempt)).unwrap_or(u32::MAX)
        };
        let Some(group) = self.groups.get(&group_id) else {
            return Err(GroupError::GroupNotFound);
        };
        if group.retries.len() >= self.config.group.max_retries
            && !group.retries.contains_key(&offset)
        {
            return Err(GroupError::Capacity);
        }
        let generation = group.generation;
        let record = Record {
            kind: RecordKind::Retry,
            logical_offset: self.next,
            owner_epoch: self.config.ownership_epoch,
            producer_id: 0,
            producer_epoch: 0,
            sequence: u64::from(attempt),
            message_id: self.message_index[&offset],
            server_timestamp: self.logical_time,
            payload: RetryRecord {
                group_id,
                incarnation: self.wal_config.incarnation,
                offset,
                generation,
                ownership_epoch: self.config.ownership_epoch,
                attempt,
                reason,
                delay_ms: delay,
                exhausted,
            }
            .encode(),
        };
        let record_offset = self.next;
        let result = (|| {
            #[cfg(feature = "fault-injection")]
            self.test_barrier(TestStage::BeforeAppend, offset)
                .map_err(|_| GroupError::AckNotCommitted)?;
            self.wal.append(record)?;
            #[cfg(feature = "fault-injection")]
            self.test_barrier(TestStage::AfterAppendBeforeSync, offset)
                .map_err(|_| GroupError::AckNotCommitted)?;
            self.wal.sync()?;
            #[cfg(feature = "fault-injection")]
            self.test_barrier(TestStage::AfterSyncBeforeReply, offset)
                .map_err(|_| GroupError::AckNotCommitted)?;
            Ok::<_, GroupError>(())
        })();
        if result.is_err() {
            self.unhealthy = true;
            return Err(GroupError::AckNotCommitted);
        }
        self.next = record_offset.checked_add(1).ok_or(GroupError::Capacity)?;
        self.committed
            .store(self.next, std::sync::atomic::Ordering::Release);
        let group = self.groups.get_mut(&group_id).unwrap();
        group.attempts.insert(
            offset,
            group
                .attempts
                .get(&offset)
                .copied()
                .unwrap_or(0)
                .max(attempt),
        );
        group.retries.insert(
            offset,
            RetryState {
                attempt,
                eligible_ms: self.logical_time.saturating_add(u64::from(delay)),
                exhausted,
            },
        );
        Ok(delay)
    }
    pub(super) fn nack(&mut self, r: NackRequest) -> std::result::Result<NackOutcome, GroupError> {
        if self.unhealthy {
            return Err(GroupError::Unavailable);
        }
        // Fencing: retries and terminal transitions are fenced too.
        self.check_authority()
            .map_err(|_| GroupError::StaleGeneration)?;
        if r.ownership_epoch != self.config.ownership_epoch {
            return Err(GroupError::StaleGeneration);
        }
        self.advance_clock();
        let offset = r.offset.sequence;
        let Some(&message_id) = self.message_index.get(&offset) else {
            return Err(GroupError::NotLeased);
        };
        let Some(group) = self.groups.get(&r.group_id) else {
            return Err(GroupError::GroupNotFound);
        };
        if offset < group.cursor || group.acked.contains(&offset) {
            return Ok(NackOutcome::AlreadyClosed {
                disposition_id: group.terminals.get(&offset).copied().unwrap_or_else(|| {
                    terminal::disposition_id(r.group_id, self.wal_config.incarnation, offset)
                }),
            });
        }
        if r.generation != group.generation {
            return Err(GroupError::StaleGeneration);
        }
        // A repeated NACK of the same attempt is idempotent: it never schedules another
        // durable retry record or resets the backoff deadline.
        if let Some(retry) = group.retries.get(&offset).copied() {
            if retry.attempt == r.attempt {
                if retry.exhausted {
                    if self.config.group.terminal_policy == TerminalPolicy::None {
                        return Ok(NackOutcome::RetryExhausted);
                    }
                    return self.begin_terminal(
                        r.group_id,
                        offset,
                        terminal::REASON_RETRY_EXHAUSTED,
                    );
                }
                let remaining = retry
                    .eligible_ms
                    .saturating_sub(self.logical_time)
                    .min(u64::from(u32::MAX)) as u32;
                return Ok(NackOutcome::RetryScheduled {
                    retry_after_ms: remaining,
                });
            }
        }
        if let Some(pending) = group.pending.get(&offset).copied() {
            return self.finish_terminal(r.group_id, offset, pending);
        }
        let now = Instant::now();
        let lease = group.leases.get(&r.lease_id).cloned();
        let generation = group.generation;
        let Some(lease) = lease else {
            return Err(GroupError::NotLeased);
        };
        if lease.deadline <= now
            || lease.offset != offset
            || lease.attempt != r.attempt
            || lease.generation != generation
            || lease.message_id != message_id
        {
            return Err(GroupError::NotLeased);
        }
        // The lease is consumed by the NACK; it never becomes a durable completion.
        self.groups
            .get_mut(&r.group_id)
            .unwrap()
            .leases
            .remove(&r.lease_id);
        if r.attempt >= self.config.group.max_attempts {
            if self.config.group.terminal_policy == TerminalPolicy::None {
                // Persist exhaustion so a restart does not silently resume delivery.
                self.append_retry(
                    r.group_id,
                    offset,
                    r.attempt,
                    terminal::REASON_RETRY_EXHAUSTED,
                    true,
                )?;
                return Ok(NackOutcome::RetryExhausted);
            }
            // With a terminal policy the PENDING record itself preserves the attempt count.
            return self.begin_terminal(r.group_id, offset, terminal::REASON_RETRY_EXHAUSTED);
        }
        let delay = self.append_retry(r.group_id, offset, r.attempt, r.reason, false)?;
        Ok(NackOutcome::RetryScheduled {
            retry_after_ms: delay,
        })
    }
    fn trigger_exhausted(
        &mut self,
        group_id: u64,
        offset: u64,
    ) -> std::result::Result<(), GroupError> {
        if self.config.group.terminal_policy == TerminalPolicy::None {
            return Ok(());
        }
        if let Some(group) = self.groups.get(&group_id) {
            if offset < group.cursor || group.acked.contains(&offset) {
                return Ok(());
            }
        }
        self.begin_terminal(group_id, offset, terminal::REASON_RETRY_EXHAUSTED)?;
        Ok(())
    }
    fn begin_terminal(
        &mut self,
        group_id: u64,
        offset: u64,
        reason: u8,
    ) -> std::result::Result<NackOutcome, GroupError> {
        // Fencing: a terminal disposition is a durable mutation as well.
        self.check_authority()
            .map_err(|_| GroupError::StaleGeneration)?;
        let kind = match self.config.group.terminal_policy {
            TerminalPolicy::None => return Err(GroupError::InvalidRequest),
            TerminalPolicy::Quarantine => TerminalKind::Quarantine,
            TerminalPolicy::Dlq => TerminalKind::Dlq,
        };
        let Some(group) = self.groups.get(&group_id) else {
            return Err(GroupError::GroupNotFound);
        };
        if offset < group.cursor || group.acked.contains(&offset) {
            return Ok(NackOutcome::AlreadyClosed {
                disposition_id: group.terminals.get(&offset).copied().unwrap_or_else(|| {
                    terminal::disposition_id(group_id, self.wal_config.incarnation, offset)
                }),
            });
        }
        if let Some(pending) = group.pending.get(&offset).copied() {
            return self.finish_terminal(group_id, offset, pending);
        }
        if group.pending.len() >= self.config.group.max_terminal_pending {
            return Err(GroupError::Capacity);
        }
        let generation = group.generation;
        let attempts = group.attempts.get(&offset).copied().unwrap_or(0);
        let original_message_id = self.message_index[&offset];
        let disposition_id =
            terminal::disposition_id(group_id, self.wal_config.incarnation, offset);
        let pending = PendingTerminal {
            kind,
            disposition_id,
            generation,
            ownership_epoch: self.config.ownership_epoch,
            attempts,
            original_message_id,
            reason,
        };
        let record = TerminalRecord {
            state: TerminalState::Pending,
            kind,
            group_id,
            generation,
            ownership_epoch: self.config.ownership_epoch,
            source_incarnation: self.wal_config.incarnation,
            source_offset: offset,
            disposition_id,
            reason,
            attempts,
            original_message_id,
            dlq_incarnation: 0,
            dlq_offset: 0,
        };
        self.append_terminal_record(record, false)?;
        let group = self.groups.get_mut(&group_id).unwrap();
        group.pending.insert(offset, pending);
        group.leases.retain(|_, lease| lease.offset != offset);
        group.retries.remove(&offset);
        self.pending_terminal.insert((group_id, offset), pending);
        self.finish_terminal(group_id, offset, pending)
    }
    fn append_terminal_record(
        &mut self,
        terminal: TerminalRecord,
        committed: bool,
    ) -> std::result::Result<(), GroupError> {
        let record = Record {
            kind: RecordKind::TerminalDisposition,
            logical_offset: self.next,
            owner_epoch: terminal.ownership_epoch,
            producer_id: 0,
            producer_epoch: 0,
            sequence: u64::from(terminal.attempts),
            message_id: terminal.original_message_id,
            server_timestamp: self.logical_time,
            payload: terminal.encode(),
        };
        let offset = terminal.source_offset;
        let record_offset = self.next;
        let result = (|| {
            self.wal.append(record)?;
            #[cfg(feature = "fault-injection")]
            if committed {
                self.test_barrier(TestStage::TerminalCommitAfterAppendBeforeSync, offset)
                    .map_err(|_| GroupError::AckNotCommitted)?;
            } else {
                self.test_barrier(TestStage::TerminalPendingAfterAppendBeforeSync, offset)
                    .map_err(|_| GroupError::AckNotCommitted)?;
            }
            self.wal.sync()?;
            #[cfg(feature = "fault-injection")]
            if !committed {
                self.test_barrier(TestStage::TerminalPendingAfterSync, offset)
                    .map_err(|_| GroupError::AckNotCommitted)?;
            } else {
                self.test_barrier(TestStage::TerminalCommitAfterSyncBeforeReply, offset)
                    .map_err(|_| GroupError::AckNotCommitted)?;
            }
            Ok::<_, GroupError>(())
        })();
        if result.is_err() {
            self.unhealthy = true;
            return Err(GroupError::AckNotCommitted);
        }
        self.next = record_offset.checked_add(1).ok_or(GroupError::Capacity)?;
        self.committed
            .store(self.next, std::sync::atomic::Ordering::Release);
        Ok(())
    }
    fn finish_terminal(
        &mut self,
        group_id: u64,
        offset: u64,
        pending: PendingTerminal,
    ) -> std::result::Result<NackOutcome, GroupError> {
        let mut dlq_incarnation = 0;
        let mut dlq_offset = 0;
        if pending.kind == TerminalKind::Dlq {
            let dlq = self.dlq.clone().ok_or(GroupError::DlqUnavailable)?;
            let records = self
                .wal
                .read(
                    DurableOffset {
                        log_incarnation: self.wal_config.incarnation,
                        sequence: offset,
                    },
                    1,
                    self.wal_config.max_record_bytes,
                )
                .map_err(|error| {
                    self.unhealthy = true;
                    GroupError::Storage(error)
                })?;
            if records.len() != 1
                || records[0].kind != RecordKind::Message
                || records[0].message_id != pending.original_message_id
            {
                self.unhealthy = true;
                return Err(GroupError::Corruption("DLQ source message"));
            }
            let payload = records.into_iter().next().unwrap().payload;
            let record = DlqRecord {
                disposition_id: pending.disposition_id,
                group_id,
                source_partition: self.config.partition,
                source_incarnation: self.wal_config.incarnation,
                source_offset: offset,
                original_message_id: pending.original_message_id,
                reason: pending.reason,
                attempts: pending.attempts,
                payload: &payload,
            };
            let destination = dlq.publish(&record).map_err(|error| match error {
                super::terminal::DlqError::Unavailable => GroupError::DlqUnavailable,
                super::terminal::DlqError::Capacity => GroupError::Capacity,
                super::terminal::DlqError::Conflict => {
                    GroupError::Corruption("DLQ disposition conflict")
                }
                super::terminal::DlqError::Corruption(_) => GroupError::Corruption("DLQ payload"),
            })?;
            #[cfg(feature = "fault-injection")]
            self.test_barrier(TestStage::DlqAfterPublishBeforeTerminal, offset)
                .map_err(|_| GroupError::AckNotCommitted)?;
            if !dlq
                .verify(
                    destination,
                    pending.disposition_id,
                    pending.original_message_id,
                )
                .map_err(|_| GroupError::DlqUnavailable)?
            {
                return Err(GroupError::Corruption("DLQ commit proof"));
            }
            dlq_incarnation = destination.log_incarnation;
            dlq_offset = destination.sequence;
        }
        let record = TerminalRecord {
            state: TerminalState::Committed,
            kind: pending.kind,
            group_id,
            generation: pending.generation,
            ownership_epoch: pending.ownership_epoch,
            source_incarnation: self.wal_config.incarnation,
            source_offset: offset,
            disposition_id: pending.disposition_id,
            reason: pending.reason,
            attempts: pending.attempts,
            original_message_id: pending.original_message_id,
            dlq_incarnation,
            dlq_offset,
        };
        self.append_terminal_record(record, true)?;
        let group = self.groups.get_mut(&group_id).unwrap();
        group.pending.remove(&offset);
        group.retries.remove(&offset);
        group.leases.retain(|_, lease| lease.offset != offset);
        group.terminals.insert(offset, pending.disposition_id);
        group.acked.insert(offset);
        advance_cursor(&self.messages, group, self.next);
        self.pending_terminal.remove(&(group_id, offset));
        Ok(NackOutcome::TerminalCommitted {
            disposition_id: pending.disposition_id,
        })
    }
    pub(super) fn resume_pending_terminal(&mut self) {
        if self.pending_terminal.is_empty() || self.unhealthy {
            return;
        }
        let now = Instant::now();
        if self
            .last_terminal_attempt
            .is_some_and(|last| now.duration_since(last) < Duration::from_millis(250))
        {
            return;
        }
        self.last_terminal_attempt = Some(now);
        let pending: Vec<(u64, u64, PendingTerminal)> = self
            .pending_terminal
            .iter()
            .map(|(&(group, offset), &intent)| (group, offset, intent))
            .collect();
        for (group_id, offset, intent) in pending {
            let _ = self.finish_terminal(group_id, offset, intent);
        }
    }
    pub(super) fn reconcile_recovered_terminal(&mut self) -> Result<()> {
        let pending: Vec<(u64, u64, PendingTerminal)> = self
            .pending_terminal
            .iter()
            .map(|(&(group, offset), &intent)| (group, offset, intent))
            .collect();
        for (group_id, offset, intent) in pending {
            self.finish_terminal(group_id, offset, intent)
                .map_err(|error| match error {
                    GroupError::DlqUnavailable => Error::DlqUnavailable,
                    GroupError::Capacity => Error::Capacity,
                    GroupError::Corruption(reason) => Error::Corruption(reason),
                    GroupError::Storage(_) => Error::Corruption("terminal recovery IO"),
                    _ => Error::Corruption("terminal recovery"),
                })?;
        }
        if self.recovered_dlq_commits.is_empty() {
            return Ok(());
        }
        let dlq = self.dlq.clone().ok_or(Error::DlqUnavailable)?;
        for (disposition, incarnation, offset, original) in self.recovered_dlq_commits.clone() {
            let verified = dlq
                .verify(
                    DurableOffset {
                        log_incarnation: incarnation,
                        sequence: offset,
                    },
                    disposition,
                    original,
                )
                .map_err(|_| Error::DlqUnavailable)?;
            if !verified {
                return Err(Error::Corruption("committed DLQ proof missing"));
            }
        }
        Ok(())
    }
}
