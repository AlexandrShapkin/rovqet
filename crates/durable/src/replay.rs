//! Read-only committed history; group delivery and consumer acknowledgement are separate.
use super::*;
use rovqet_wal::RecordKind;

pub const MAX_REPLAY_MESSAGES: usize = 1024;
pub const MAX_REPLAY_BYTES: usize = 4 * 1024 * 1024;
pub const MAX_REPLAY_WAIT: Duration = Duration::from_secs(30);
/// Conservative application response/memory charges, not major-1 wire frame sizes.
pub const REPLAY_BATCH_BYTES: usize = 128;
pub const REPLAY_MESSAGE_BYTES: usize = 128;

#[derive(Debug, Clone)]
pub struct ReplayRequest {
    pub partition: PartitionRef,
    /// WAL position to start scanning from. Positions may contain consumer metadata records;
    /// they are skipped, so continuation is the next unread WAL position, not an arithmetic
    /// message step. Application message offsets are sparse and stable.
    pub from: DurableOffset,
    pub max_messages: usize,
    pub max_bytes: usize,
    /// Wait only when starting at the current committed end. Does not block publications.
    pub wait: Duration,
}
#[derive(Debug)]
pub enum ReplayError {
    WrongPartition,
    IncarnationMismatch,
    ReplayNotAvailable,
    InvalidLimits,
    ResponseTooSmall { required_bytes: usize },
    Capacity,
    Unavailable,
    Storage(rovqet_wal::Error),
    Corruption(&'static str),
}
impl fmt::Display for ReplayError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for ReplayError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Storage(error) => Some(error),
            _ => None,
        }
    }
}
#[derive(Debug, PartialEq, Eq)]
pub struct ReplayMessage {
    pub offset: DurableOffset,
    pub message_id: u128,
    pub producer_id: u128,
    pub producer_epoch: u64,
    pub sequence: u64,
    /// Conservative persisted logical milliseconds; not UTC or a delivery deadline.
    pub server_timestamp: u64,
    pub payload: Vec<u8>,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplayStop {
    Empty,
    End,
    CountLimit,
    ByteLimit,
    WaitExpired,
}
#[derive(Clone, Copy)]
struct Sizing {
    terminal: usize,
    record: Option<fn(usize) -> Option<usize>>,
}
pub struct ReplayBatch {
    pub partition: PartitionRef,
    pub messages: Vec<ReplayMessage>,
    pub next: DurableOffset,
    /// True at the committed snapshot boundary. Later publication may extend it.
    pub end_of_available_history: bool,
    /// Memory charge for ordinary reads; exact encoded batch bytes for the wire adapter.
    pub response_bytes: usize,
    pub stop: ReplayStop,
    // Retained until the caller drops the batch, including time spent in the reply channel.
    _reservation: Reservation,
}
impl fmt::Debug for ReplayBatch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ReplayBatch")
            .field("partition", &self.partition)
            .field("messages", &self.messages)
            .field("next", &self.next)
            .field("end_of_available_history", &self.end_of_available_history)
            .field("response_bytes", &self.response_bytes)
            .field("stop", &self.stop)
            .finish()
    }
}
pub struct PendingReplay {
    reply: oneshot::Receiver<std::result::Result<ReplayBatch, ReplayError>>,
}
impl PendingReplay {
    /// Cancellation is read-only. Dropping the waiter releases its slot on the next actor poll.
    pub fn wait(self) -> std::result::Result<ReplayBatch, ReplayError> {
        self.reply
            .blocking_recv()
            .unwrap_or(Err(ReplayError::Unavailable))
    }
    pub async fn wait_async(self) -> std::result::Result<ReplayBatch, ReplayError> {
        self.reply.await.unwrap_or(Err(ReplayError::Unavailable))
    }
}
pub(super) struct ReplayCommand {
    request: ReplayRequest,
    deadline: Instant,
    reply: oneshot::Sender<std::result::Result<ReplayBatch, ReplayError>>,
    reservation: Reservation,
    sizing: Sizing,
}
impl Partition {
    /// Trusted caller must authorize the principal/route before submitting. No lease or ACK exists.
    /// Queue, active read, waiting read and returned batch share the partition's hard budget.
    pub fn try_replay(
        &self,
        request: ReplayRequest,
    ) -> std::result::Result<PendingReplay, ReplayError> {
        self.submit_replay(
            request,
            Sizing {
                terminal: REPLAY_BATCH_BYTES,
                record: None,
            },
        )
    }
    /// Exact encoded response budgeting supplied by the trusted protocol adapter.
    /// Retains additional encoding scratch under the same hard partition budget.
    pub fn try_replay_encoded(
        &self,
        request: ReplayRequest,
        terminal_bytes: usize,
        record_bytes: fn(usize) -> Option<usize>,
    ) -> std::result::Result<PendingReplay, ReplayError> {
        if terminal_bytes == 0 || terminal_bytes > REPLAY_BATCH_BYTES {
            return Err(ReplayError::InvalidLimits);
        }
        self.submit_replay(
            request,
            Sizing {
                terminal: terminal_bytes,
                record: Some(record_bytes),
            },
        )
    }
    fn submit_replay(
        &self,
        request: ReplayRequest,
        sizing: Sizing,
    ) -> std::result::Result<PendingReplay, ReplayError> {
        if request.partition != self.config.partition {
            return Err(ReplayError::WrongPartition);
        }
        if request.max_messages == 0
            || request.max_messages > MAX_REPLAY_MESSAGES
            || request.max_bytes < sizing.terminal
            || request.max_bytes > MAX_REPLAY_BYTES
            || request.wait > MAX_REPLAY_WAIT
        {
            return Err(ReplayError::InvalidLimits);
        }
        // WAL read temporarily holds encoded bytes and decoded payload for a complete pair.
        // Reserve scratch as well as the full retained response, before any allocation or IO.
        let charge = self
            .max_record_bytes
            .checked_mul(2)
            .and_then(|scratch| scratch.checked_add(2 * (RECORD_OVERHEAD + OUTCOME_BYTES)))
            .and_then(|scratch| scratch.checked_add(request.max_messages * REPLAY_MESSAGE_BYTES))
            .and_then(|scratch| request.max_bytes.checked_add(scratch))
            .and_then(|charge| {
                charge.checked_add(if sizing.record.is_some() {
                    request.max_bytes + REPLAY_BATCH_BYTES
                } else {
                    0
                })
            })
            .ok_or(ReplayError::Capacity)?;
        let reservation =
            Budget::reserve(&[self.budget.clone()], charge).ok_or(ReplayError::Capacity)?;
        let (reply, receiver) = oneshot::channel();
        let deadline = Instant::now()
            .checked_add(request.wait)
            .ok_or(ReplayError::InvalidLimits)?;
        let command = Command::Replay(ReplayCommand {
            request,
            deadline,
            reply,
            reservation,
            sizing,
        });
        match self
            .sender
            .as_ref()
            .ok_or(ReplayError::Unavailable)?
            .try_send(command)
        {
            Ok(()) => Ok(PendingReplay { reply: receiver }),
            Err(TrySendError::Full(_)) => Err(ReplayError::Capacity),
            Err(TrySendError::Disconnected(_)) => Err(ReplayError::Unavailable),
        }
    }
}
impl State {
    pub(super) fn complete_replays(&mut self, reads: &mut Vec<ReplayCommand>, shutdown: bool) {
        let mut index = 0;
        while index < reads.len() {
            if reads[index].reply.is_closed() {
                reads.swap_remove(index);
                continue;
            }
            let command = &reads[index];
            let at_end = command.request.from.log_incarnation == self.wal_config.incarnation
                && command.request.from.sequence == self.next;
            if !shutdown && !self.unhealthy && at_end && Instant::now() < command.deadline {
                index += 1;
                continue;
            }
            let command = reads.swap_remove(index);
            let expired = !shutdown
                && at_end
                && !command.request.wait.is_zero()
                && Instant::now() >= command.deadline;
            let result = self.replay(
                command.request,
                command.reservation,
                command.sizing,
                expired,
            );
            let _ = command.reply.send(result);
        }
    }
    fn replay(
        &mut self,
        r: ReplayRequest,
        reservation: Reservation,
        sizing: Sizing,
        expired: bool,
    ) -> std::result::Result<ReplayBatch, ReplayError> {
        if self.unhealthy {
            return Err(ReplayError::Unavailable);
        }
        if r.from.log_incarnation != self.wal_config.incarnation {
            return Err(ReplayError::IncarnationMismatch);
        }
        if r.from.sequence > self.next {
            return Err(ReplayError::ReplayNotAvailable);
        }
        let mut cursor = r.from.sequence;
        let mut response_bytes = sizing.terminal;
        let mut stop = ReplayStop::CountLimit;
        let capacity = if sizing.record.is_some() {
            r.max_messages
        } else {
            r.max_messages
                .min((r.max_bytes - REPLAY_BATCH_BYTES) / REPLAY_MESSAGE_BYTES)
        };
        let mut messages = Vec::with_capacity(capacity);
        while cursor < self.next && messages.len() < r.max_messages {
            let mut records = self
                .wal
                .read(
                    DurableOffset {
                        log_incarnation: r.from.log_incarnation,
                        sequence: cursor,
                    },
                    1,
                    self.wal_config.max_record_bytes,
                )
                .map_err(|e| {
                    self.unhealthy = true;
                    ReplayError::Storage(e)
                })?;
            if records.len() != 1 {
                self.unhealthy = true;
                return Err(ReplayError::Corruption("missing committed record"));
            }
            let record = records.pop().unwrap();
            if record.kind != RecordKind::Message {
                match record.kind {
                    RecordKind::ProducerOutcome
                    | RecordKind::ConsumerAck
                    | RecordKind::CursorCheckpoint
                    | RecordKind::Retry
                    | RecordKind::TerminalDisposition
                    | RecordKind::Tombstone => {
                        // Durable metadata (including a publication outcome reached by a
                        // continuation) is not an application message.
                        cursor += 1;
                        continue;
                    }
                    _ => {
                        self.unhealthy = true;
                        return Err(ReplayError::Corruption("unexpected committed record"));
                    }
                }
            }
            let mut pair = self
                .wal
                .read(
                    DurableOffset {
                        log_incarnation: r.from.log_incarnation,
                        sequence: cursor + 1,
                    },
                    1,
                    self.wal_config.max_record_bytes,
                )
                .map_err(|e| {
                    self.unhealthy = true;
                    ReplayError::Storage(e)
                })?;
            if pair.len() != 1 || !valid_pair(&record, &pair[0]) {
                self.unhealthy = true;
                return Err(ReplayError::Corruption("committed replay pair"));
            }
            let message = record;
            let required = match sizing.record {
                Some(size) => size(message.payload.len()),
                None => REPLAY_MESSAGE_BYTES.checked_add(message.payload.len()),
            }
            .filter(|&bytes| bytes >= message.payload.len())
            .ok_or(ReplayError::InvalidLimits)?;
            if response_bytes
                .checked_add(required)
                .is_none_or(|n| n > r.max_bytes)
            {
                if messages.is_empty() {
                    return Err(ReplayError::ResponseTooSmall {
                        required_bytes: sizing.terminal + required,
                    });
                }
                stop = ReplayStop::ByteLimit;
                break;
            }
            // Capacity is charged by bytes as well as message count; never grow past preallocation.
            if messages.len() == capacity {
                break;
            }
            response_bytes += required;
            messages.push(ReplayMessage {
                offset: DurableOffset {
                    log_incarnation: r.from.log_incarnation,
                    sequence: cursor,
                },
                message_id: message.message_id,
                producer_id: message.producer_id,
                producer_epoch: message.producer_epoch,
                sequence: message.sequence,
                server_timestamp: message.server_timestamp,
                payload: message.payload,
            });
            cursor += 2;
        }
        Ok(ReplayBatch {
            partition: r.partition,
            messages,
            next: DurableOffset {
                log_incarnation: r.from.log_incarnation,
                sequence: cursor,
            },
            end_of_available_history: cursor == self.next,
            response_bytes,
            stop: if cursor == self.next {
                if expired {
                    ReplayStop::WaitExpired
                } else if self.next == 0 {
                    ReplayStop::Empty
                } else {
                    ReplayStop::End
                }
            } else {
                stop
            },
            _reservation: reservation,
        })
    }
}
/// Exact v1 binding; shared by recovery and runtime reads, including internal metadata integrity.
pub(super) fn valid_pair(message: &Record, outcome: &Record) -> bool {
    message.kind == RecordKind::Message
        && outcome.kind == RecordKind::ProducerOutcome
        && outcome.payload.len() == OUTCOME_BYTES
        && &outcome.payload[..6] == b"RQPO\0\x01"
        && u64::from_be_bytes(outcome.payload[22..30].try_into().unwrap()) == message.logical_offset
        && message.logical_offset.checked_add(1) == Some(outcome.logical_offset)
        && message.owner_epoch == outcome.owner_epoch
        && message.producer_id == outcome.producer_id
        && message.producer_epoch == outcome.producer_epoch
        && message.sequence == outcome.sequence
        && message.message_id == outcome.message_id
        && message.server_timestamp == outcome.server_timestamp
}
