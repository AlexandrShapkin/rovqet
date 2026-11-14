//! Dedicated-thread Q1 partition actor used by the opt-in TCP publication route.
//! Callers provide authenticated principal, routing authority and prevalidated TTL/ACL.
//! Open, blocking wait and shutdown must run outside a network/async executor.
//! Network callers await PendingPublish::wait_async without blocking their worker.
#![forbid(unsafe_code)]
use rovqet_core::{DurableOffset, PartitionRef, PublishOutcome, QoS};
use rovqet_scheduler::{Budget, Reservation, Usage};
use rovqet_wal::{Config as WalConfig, RECORD_OVERHEAD, Record, RecordKind, Wal};
use std::{
    collections::HashMap,
    fmt,
    path::Path,
    sync::{
        Arc,
        mpsc::{self, SyncSender, TrySendError},
    },
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};
use tokio::sync::oneshot;

const OUTCOME_BYTES: usize = 30;
mod authority;
mod fence;
pub use authority::{Authority, Fence, FenceError};
mod group;
mod terminal;
pub use fence::{FenceProof, FenceState};
use fence::{decode_fence, encode_fence, fence_digest};
use group::{AckCommand, GroupFetchCommand, GroupState, MessageMeta};
pub use group::{
    AckRequest, AckResult, GROUP_BATCH_BYTES, GROUP_MESSAGE_BYTES, GroupConfig, GroupDelivery,
    GroupError, GroupFetchBatch, GroupFetchRequest, GroupStop, MAX_GROUP_BYTES, MAX_GROUP_MESSAGES,
    MAX_GROUP_WAIT, PendingAck, PendingGroupFetch,
};
pub use terminal::{
    DlqError, DlqMetadata, DlqPublisher, DlqRecord, NackOutcome, NackRequest, PendingNack,
    REASON_CONSUMER_NACK, REASON_RETRY_EXHAUSTED, TerminalPolicy, dlq_payload, parse_dlq_payload,
};
mod replay;
use replay::ReplayCommand;
pub use replay::{
    MAX_REPLAY_BYTES, MAX_REPLAY_MESSAGES, MAX_REPLAY_WAIT, PendingReplay, REPLAY_BATCH_BYTES,
    REPLAY_MESSAGE_BYTES, ReplayBatch, ReplayError, ReplayMessage, ReplayRequest, ReplayStop,
};

struct WorkerOptions {
    clock: Arc<dyn Clock>,
    dlq: Option<Arc<dyn DlqPublisher>>,
    #[cfg(feature = "fault-injection")]
    control: Option<TestControl>,
}

/// Monotonic milliseconds in an arbitrary process-local epoch. None means unavailable evidence.
/// Regressions and unavailable samples freeze expiration; they never shorten the window.
pub trait Clock: Send + Sync {
    fn now_millis(&self) -> Option<u64>;
}
struct MonotonicClock(Instant);
impl Clock for MonotonicClock {
    fn now_millis(&self) -> Option<u64> {
        u64::try_from(self.0.elapsed().as_millis()).ok()
    }
}
impl Default for WorkerOptions {
    fn default() -> Self {
        Self {
            clock: Arc::new(MonotonicClock(Instant::now())),
            dlq: None,
            #[cfg(feature = "fault-injection")]
            control: None,
        }
    }
}
#[cfg(feature = "fault-injection")]
mod test_control;
#[cfg(feature = "fault-injection")]
pub use test_control::{TestControl, TestStage};
#[derive(Debug, Clone)]
pub struct PartitionConfig {
    pub partition: PartitionRef,
    pub ownership_epoch: u64,
    /// Local node identity used for ownership fencing. Zero means standalone mode: no
    /// control-plane authority is installed and durable mutations are not fenced.
    pub node_id: u64,
    pub queue_messages: usize,
    pub queue_bytes: usize,
    pub max_outcomes: usize,
    pub max_producers: usize,
    /// Minimum retained logical time; downtime and uncertain clock intervals do not count.
    pub dedup_window: Duration,
    pub group: GroupConfig,
}
#[derive(Debug, Clone)]
pub struct PublishRequest {
    pub partition: PartitionRef,
    pub qos: QoS,
    pub ownership_epoch: u64,
    pub principal: u128,
    pub producer_id: u128,
    pub producer_epoch: u64,
    pub sequence: u64,
    pub message_id: u128,
    pub payload: Vec<u8>,
}
#[derive(Debug)]
pub enum Error {
    InvalidConfig,
    WrongPartition,
    WrongClass,
    StaleOwner,
    ProducerFenced,
    SequenceGap,
    SequenceConflict,
    Capacity,
    Expired,
    Storage(rovqet_wal::Error),
    Corruption(&'static str),
    OutcomeUnknown,
    OutcomeUnavailable,
    HistoryUnavailable,
    DlqUnavailable,
    /// The local node is not the committed owner, or the epoch is stale.
    Fenced,
    /// The ownership lease expired; durable work fails closed.
    FenceExpired,
    Unavailable,
    WorkerStart(std::io::Error),
}
impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Storage(e) => Some(e),
            Self::WorkerStart(e) => Some(e),
            _ => None,
        }
    }
}
pub type Result<T> = std::result::Result<T, Error>;
pub struct PendingPublish {
    reply: oneshot::Receiver<Result<PublishOutcome>>,
}
impl PendingPublish {
    /// Wait for a known barrier outcome. Dropping this waiter never cancels submitted IO.
    pub fn wait(self) -> Result<PublishOutcome> {
        self.reply
            .blocking_recv()
            .unwrap_or(Err(Error::OutcomeUnknown))
    }
    /// Await storage completion without blocking a Tokio networking worker.
    pub async fn wait_async(self) -> Result<PublishOutcome> {
        self.reply.await.unwrap_or(Err(Error::OutcomeUnknown))
    }
}
struct PublishCommand {
    request: PublishRequest,
    reply: oneshot::Sender<Result<PublishOutcome>>,
    deadline: Option<Instant>,
    _reservation: Reservation,
}
enum Command {
    Publish(PublishCommand),
    Replay(ReplayCommand),
    GroupFetch(GroupFetchCommand),
    Ack(AckCommand),
    Nack(terminal::NackCommand),
    SetAuthority(Option<Authority>),
    Fence(FenceCommand),
}
/// A request to durably revoke the write authority of this owner.
struct FenceCommand {
    migration_id: u128,
    old_epoch: u64,
    next_epoch: u64,
    reply: oneshot::Sender<Result<FenceProof>>,
}
/// One caller-owned handle; shared references may submit concurrently. No socket or Q0 dependencies.
pub struct Partition {
    sender: Option<SyncSender<Command>>,
    worker: Option<JoinHandle<()>>,
    budget: Arc<Budget>,
    committed: Arc<std::sync::atomic::AtomicU64>,
    config: PartitionConfig,
    incarnation: u128,
    max_record_bytes: usize,
}
impl Partition {
    /// Explicit new-log initialization or recovery. The directory must already exist.
    /// The caller allocates a fresh incarnation for new storage; this is never a reset API.
    /// Resume a known incarnation with `open_existing` to fail closed on lost history.
    pub fn open(
        path: impl AsRef<Path>,
        wal_config: WalConfig,
        config: PartitionConfig,
    ) -> Result<Self> {
        Self::open_inner(path, wal_config, config, WorkerOptions::default())
    }
    /// Open already initialized history. Missing storage is unavailable, never recreated.
    /// Use this entry point whenever an existing incarnation is being resumed.
    pub fn open_existing(
        path: impl AsRef<Path>,
        wal_config: WalConfig,
        config: PartitionConfig,
    ) -> Result<Self> {
        if !path.as_ref().join("MANIFEST").is_file() {
            return Err(Error::HistoryUnavailable);
        }
        Self::open(path, wal_config, config)
    }
    /// Test-only deterministic storage barriers, unavailable in normal builds.
    #[cfg(feature = "fault-injection")]
    pub fn open_with_test_control(
        path: impl AsRef<Path>,
        wal_config: WalConfig,
        config: PartitionConfig,
        control: TestControl,
    ) -> Result<Self> {
        Self::open_inner(
            path,
            wal_config,
            config,
            WorkerOptions {
                control: Some(control),
                ..WorkerOptions::default()
            },
        )
    }
    /// Controlled time source for deterministic qualification; no client timestamp is trusted.
    pub fn open_with_clock(
        path: impl AsRef<Path>,
        wal_config: WalConfig,
        config: PartitionConfig,
        clock: Arc<dyn Clock>,
    ) -> Result<Self> {
        Self::open_inner(
            path,
            wal_config,
            config,
            WorkerOptions {
                clock,
                ..WorkerOptions::default()
            },
        )
    }
    /// Open with a Q1 DLQ destination. Required only when the terminal policy is DLQ or the
    /// log already contains pending DLQ terminal records.
    pub fn open_with_dlq(
        path: impl AsRef<Path>,
        wal_config: WalConfig,
        config: PartitionConfig,
        dlq: Arc<dyn DlqPublisher>,
    ) -> Result<Self> {
        Self::open_inner(
            path,
            wal_config,
            config,
            WorkerOptions {
                dlq: Some(dlq),
                ..WorkerOptions::default()
            },
        )
    }
    pub fn open_existing_with_dlq(
        path: impl AsRef<Path>,
        wal_config: WalConfig,
        config: PartitionConfig,
        dlq: Arc<dyn DlqPublisher>,
    ) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        if !path.join("MANIFEST").is_file() {
            return Err(Error::HistoryUnavailable);
        }
        Self::open_inner(
            path,
            wal_config,
            config,
            WorkerOptions {
                dlq: Some(dlq),
                ..WorkerOptions::default()
            },
        )
    }
    #[cfg(feature = "fault-injection")]
    pub fn open_with_test_control_and_dlq(
        path: impl AsRef<Path>,
        wal_config: WalConfig,
        config: PartitionConfig,
        control: TestControl,
        dlq: Arc<dyn DlqPublisher>,
    ) -> Result<Self> {
        Self::open_inner(
            path,
            wal_config,
            config,
            WorkerOptions {
                dlq: Some(dlq),
                control: Some(control),
                ..WorkerOptions::default()
            },
        )
    }
    pub fn open_with_dlq_and_clock(
        path: impl AsRef<Path>,
        wal_config: WalConfig,
        config: PartitionConfig,
        dlq: Arc<dyn DlqPublisher>,
        clock: Arc<dyn Clock>,
    ) -> Result<Self> {
        Self::open_inner(
            path,
            wal_config,
            config,
            WorkerOptions {
                clock,
                dlq: Some(dlq),
                ..WorkerOptions::default()
            },
        )
    }
    fn open_inner(
        path: impl AsRef<Path>,
        wal_config: WalConfig,
        config: PartitionConfig,
        _options: WorkerOptions,
    ) -> Result<Self> {
        if config.partition != wal_config.partition
            || config.queue_messages == 0
            || config.queue_bytes == 0
            || config.max_outcomes == 0
            || config.max_producers == 0
            || config.dedup_window.as_millis() == 0
            || config.dedup_window.as_millis() > u64::MAX as u128
            || wal_config.max_record_bytes < RECORD_OVERHEAD + OUTCOME_BYTES
        {
            return Err(Error::InvalidConfig);
        }
        let path = path.as_ref().to_path_buf();
        let max_record_bytes = wal_config.max_record_bytes;
        let incarnation = wal_config.incarnation;
        let budget = Budget::new(config.queue_messages, config.queue_bytes);
        let committed = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let committed_worker = committed.clone();
        let (tx, rx) = mpsc::sync_channel::<Command>(config.queue_messages);
        let (ready_tx, ready_rx) = mpsc::sync_channel(1);
        let actor_config = config.clone();
        let worker = thread::Builder::new()
            .name("rovqet-q1-partition".into())
            .spawn(move || {
                let state = State::open_clock(
                    &path,
                    wal_config,
                    actor_config,
                    _options.clock,
                    committed_worker,
                    _options.dlq,
                );
                match state {
                    Err(e) => {
                        let _ = ready_tx.send(Err(e));
                    }
                    Ok(mut state) => {
                        #[cfg(feature = "fault-injection")]
                        {
                            state.control = _options.control;
                        }
                        if ready_tx.send(Ok(())).is_err() {
                            return;
                        }
                        let mut reads = Vec::new();
                        let mut group_waits = Vec::new();
                        loop {
                            // Waiting reads never occupy the storage owner while idle. A short
                            // bounded poll also releases cancelled response reservations promptly.
                            let command = if !state.pending_terminal.is_empty() {
                                rx.recv_timeout(Duration::from_millis(50))
                            } else if reads.is_empty() && group_waits.is_empty() {
                                rx.recv().map_err(|_| mpsc::RecvTimeoutError::Disconnected)
                            } else {
                                rx.recv_timeout(Duration::from_millis(10))
                            };
                            match command {
                                Ok(Command::Publish(command)) => {
                                    let result =
                                        state.publish_before(command.request, command.deadline);
                                    drop(command._reservation);
                                    let _ = command.reply.send(result);
                                }
                                Ok(Command::Replay(command)) => reads.push(command),
                                Ok(Command::GroupFetch(command)) => group_waits.push(command),
                                Ok(Command::Ack(command)) => {
                                    let result = state.ack(command.request);
                                    drop(command._reservation);
                                    let _ = command.reply.send(result);
                                }
                                Ok(Command::Nack(command)) => {
                                    let result = state.nack(command.request);
                                    drop(command._reservation);
                                    let _ = command.reply.send(result);
                                }
                                Ok(Command::SetAuthority(authority)) => {
                                    state.set_authority(authority);
                                }
                                Ok(Command::Fence(command)) => {
                                    let result = state.fence_old_owner(
                                        command.migration_id,
                                        command.old_epoch,
                                        command.next_epoch,
                                    );
                                    let _ = command.reply.send(result);
                                }
                                Err(mpsc::RecvTimeoutError::Timeout) => {}
                                Err(mpsc::RecvTimeoutError::Disconnected) => {
                                    state.complete_replays(&mut reads, true);
                                    state.complete_group_fetches(&mut group_waits, true);
                                    break;
                                }
                            }
                            state.complete_replays(&mut reads, false);
                            state.complete_group_fetches(&mut group_waits, false);
                            state.resume_pending_terminal();
                        }
                    }
                }
            })
            .map_err(Error::WorkerStart)?;
        match ready_rx.recv() {
            Ok(Ok(())) => Ok(Self {
                sender: Some(tx),
                worker: Some(worker),
                budget,
                committed,
                config,
                incarnation,
                max_record_bytes,
            }),
            Ok(Err(e)) => {
                drop(tx);
                let _ = worker.join();
                Err(e)
            }
            Err(_) => {
                drop(tx);
                let _ = worker.join();
                Err(Error::OutcomeUnknown)
            }
        }
    }
    /// Nonblocking admission; the reservation covers queued and actively synchronizing records.
    pub fn try_publish(&self, request: PublishRequest) -> Result<PendingPublish> {
        self.submit(request, None)
    }
    /// A new publication that expires while queued is rejected before any WAL append.
    /// An identical already-committed retry still resolves its saved durable outcome.
    pub fn try_publish_before(
        &self,
        request: PublishRequest,
        deadline: Instant,
    ) -> Result<PendingPublish> {
        self.submit(request, Some(deadline))
    }
    fn submit(&self, request: PublishRequest, deadline: Option<Instant>) -> Result<PendingPublish> {
        if request.partition != self.config.partition {
            return Err(Error::WrongPartition);
        }
        if request.qos != QoS::Q1 {
            return Err(Error::WrongClass);
        }
        if request.ownership_epoch != self.config.ownership_epoch {
            return Err(Error::StaleOwner);
        }
        let encoded = request
            .payload
            .len()
            .checked_add(RECORD_OVERHEAD)
            .ok_or(Error::Capacity)?;
        if encoded > self.max_record_bytes {
            return Err(Error::Capacity);
        }
        let charge = request
            .payload
            .capacity()
            .checked_add(2 * RECORD_OVERHEAD + OUTCOME_BYTES)
            .ok_or(Error::Capacity)?;
        let reservation = Budget::reserve(&[self.budget.clone()], charge).ok_or(Error::Capacity)?;
        let (reply, rx) = oneshot::channel();
        let command = Command::Publish(PublishCommand {
            request,
            reply,
            deadline,
            _reservation: reservation,
        });
        match self
            .sender
            .as_ref()
            .ok_or(Error::Unavailable)?
            .try_send(command)
        {
            Ok(()) => Ok(PendingPublish { reply: rx }),
            Err(TrySendError::Full(_)) => Err(Error::Capacity),
            Err(TrySendError::Disconnected(_)) => Err(Error::Unavailable),
        }
    }
    pub fn usage(&self) -> Usage {
        self.budget.usage()
    }
    pub fn partition_ref(&self) -> PartitionRef {
        self.config.partition
    }
    pub fn incarnation(&self) -> u128 {
        self.incarnation
    }
    /// Durably revokes the write authority of this partition.
    ///
    /// The returned proof exists only after the fence record is durable. A fenced
    /// partition refuses every durable mutation, also after a restart: the revocation is
    /// a record of the Q1 WAL and is replayed on recovery.
    pub fn fence_old_owner(
        &self,
        migration_id: u128,
        old_epoch: u64,
        next_epoch: u64,
    ) -> Result<FenceProof> {
        let sender = self.sender.as_ref().ok_or(Error::Unavailable)?;
        let (tx, rx) = oneshot::channel();
        sender
            .try_send(Command::Fence(FenceCommand {
                migration_id,
                old_epoch,
                next_epoch,
                reply: tx,
            }))
            .map_err(|error| match error {
                TrySendError::Full(_) => Error::Capacity,
                TrySendError::Disconnected(_) => Error::Unavailable,
            })?;
        rx.blocking_recv().unwrap_or(Err(Error::OutcomeUnknown))
    }

    /// Durably revokes the write authority of this partition without blocking a worker.
    pub async fn fence_old_owner_async(
        &self,
        migration_id: u128,
        old_epoch: u64,
        next_epoch: u64,
    ) -> Result<FenceProof> {
        let sender = self.sender.as_ref().ok_or(Error::Unavailable)?;
        let (tx, rx) = oneshot::channel();
        sender
            .try_send(Command::Fence(FenceCommand {
                migration_id,
                old_epoch,
                next_epoch,
                reply: tx,
            }))
            .map_err(|error| match error {
                TrySendError::Full(_) => Error::Capacity,
                TrySendError::Disconnected(_) => Error::Unavailable,
            })?;
        rx.await.unwrap_or(Err(Error::OutcomeUnknown))
    }

    /// Installs (or clears) the committed ownership authority of this partition. The
    /// command is ordered with durable mutations in the actor, so a mutation accepted
    /// before the transition may still commit, and every later mutation is fenced.
    pub fn set_authority(&self, authority: Option<Authority>) -> Result<()> {
        let command = Command::SetAuthority(authority);
        match self
            .sender
            .as_ref()
            .ok_or(Error::Unavailable)?
            .try_send(command)
        {
            Ok(()) => Ok(()),
            Err(TrySendError::Full(_)) => Err(Error::Capacity),
            Err(TrySendError::Disconnected(_)) => Err(Error::Unavailable),
        }
    }
    /// Number of durably committed WAL records (diagnostics; never a delivery cursor).
    pub fn committed_records(&self) -> u64 {
        self.committed.load(std::sync::atomic::Ordering::Acquire)
    }
    /// Stop admission, drain accepted writes and join the worker. May block on filesystem IO.
    pub fn shutdown(mut self) -> Result<()> {
        drop(self.sender.take());
        self.worker
            .take()
            .unwrap()
            .join()
            .map_err(|_| Error::OutcomeUnknown)
    }
}
/// Dropping the sole handle drains submitted operations and joins the blocking worker.
impl Drop for Partition {
    fn drop(&mut self) {
        drop(self.sender.take());
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct Token {
    principal: u128,
    producer: u128,
    epoch: u64,
    sequence: u64,
}
struct State {
    wal: Wal,
    wal_config: WalConfig,
    config: PartitionConfig,
    outcomes: HashMap<Token, (u64, u64)>,
    clock: Arc<dyn Clock>,
    clock_sample: Option<u64>,
    logical_time: u64,
    producers: HashMap<(u128, u128), (u64, u64)>,
    /// Application messages in WAL order. Offsets are sparse once ACK/checkpoint records
    /// are interleaved, so they are never derived by incrementing a physical offset.
    messages: Vec<MessageMeta>,
    message_index: HashMap<u64, u128>,
    groups: HashMap<u64, GroupState>,
    fence: Fence,
    /// The durable revocation of this owner's write authority, if it was fenced. It is
    /// recovered from the WAL, so a restart cannot restore an obsolete authority.
    fenced: Option<FenceState>,
    dlq: Option<Arc<dyn DlqPublisher>>,
    pending_terminal: HashMap<(u64, u64), terminal::PendingTerminal>,
    recovered_dlq_commits: Vec<(u128, u128, u64, u128)>,
    last_terminal_attempt: Option<Instant>,
    committed: Arc<std::sync::atomic::AtomicU64>,
    next: u64,
    unhealthy: bool,
    #[cfg(feature = "fault-injection")]
    control: Option<TestControl>,
}
impl State {
    #[cfg(test)]
    fn open(path: &Path, wal_config: WalConfig, config: PartitionConfig) -> Result<Self> {
        Self::open_clock(
            path,
            wal_config,
            config,
            WorkerOptions::default().clock,
            Arc::new(std::sync::atomic::AtomicU64::new(0)),
            None,
        )
    }
    fn open_clock(
        path: &Path,
        wal_config: WalConfig,
        config: PartitionConfig,
        clock: Arc<dyn Clock>,
        committed: Arc<std::sync::atomic::AtomicU64>,
        dlq: Option<Arc<dyn DlqPublisher>>,
    ) -> Result<Self> {
        let wal = Wal::open(path, wal_config.clone()).map_err(Error::Storage)?;
        let mut s = Self {
            wal,
            wal_config,
            config,
            outcomes: HashMap::new(),
            clock_sample: clock.now_millis(),
            clock,
            logical_time: 1,
            producers: HashMap::new(),
            messages: Vec::new(),
            message_index: HashMap::new(),
            groups: HashMap::new(),
            fence: Fence::standalone(),
            fenced: None,
            dlq,
            pending_terminal: HashMap::new(),
            recovered_dlq_commits: Vec::new(),
            last_terminal_attempt: None,
            committed,
            next: 0,
            unhealthy: false,
            #[cfg(feature = "fault-injection")]
            control: None,
        };
        s.recover()?;
        Ok(s)
    }
    #[cfg(feature = "fault-injection")]
    fn test_barrier(&mut self, stage: TestStage, sequence: u64) -> Result<()> {
        let result = match &mut self.control {
            Some(control) => control.pause(stage, sequence),
            None => Ok(()),
        };
        if result.is_err() {
            self.unhealthy = true;
        }
        result
    }
    fn expire(&mut self) {
        let now = self.logical_time;
        let window = self.config.dedup_window.as_millis() as u64;
        // Zero is legacy/unavailable timestamp evidence: never expire it speculatively.
        self.outcomes
            .retain(|_, &mut (_, stamp)| stamp == 0 || now.saturating_sub(stamp) < window);
    }
    fn advance_clock(&mut self) {
        let sample = self.clock.now_millis();
        if let (Some(previous), Some(now)) = (self.clock_sample, sample) {
            if let Some(elapsed) = now.checked_sub(previous) {
                self.logical_time = self.logical_time.saturating_add(elapsed);
            }
        }
        // Keep a future anchor across rollback; unavailable intervals are discarded.
        self.clock_sample = match (self.clock_sample, sample) {
            (Some(a), Some(b)) => Some(a.max(b)),
            (_, other) => other,
        };
        self.expire();
    }
    fn recover(&mut self) -> Result<()> {
        let count = self
            .wal
            .committed_position()
            .map(|p| {
                p.sequence
                    .checked_add(1)
                    .ok_or(Error::Corruption("offset overflow"))
            })
            .transpose()?
            .unwrap_or(0);
        let mut offset = 0u64;
        while offset < count {
            let mut records = self
                .wal
                .read(
                    DurableOffset {
                        log_incarnation: self.wal_config.incarnation,
                        sequence: offset,
                    },
                    1,
                    self.wal_config.max_record_bytes,
                )
                .map_err(Error::Storage)?;
            if records.len() != 1 {
                return Err(Error::Corruption("missing committed record"));
            }
            let record = records.pop().unwrap();
            if record.logical_offset != offset {
                return Err(Error::Corruption("logical offset gap"));
            }
            match record.kind {
                RecordKind::Message => {
                    if offset.checked_add(1).is_none_or(|next| next >= count) {
                        return Err(Error::Corruption("incomplete publication pair"));
                    }
                    let mut pair = self
                        .wal
                        .read(
                            DurableOffset {
                                log_incarnation: self.wal_config.incarnation,
                                sequence: offset + 1,
                            },
                            1,
                            self.wal_config.max_record_bytes,
                        )
                        .map_err(Error::Storage)?;
                    if pair.len() != 1 {
                        return Err(Error::Corruption("missing publication pair"));
                    }
                    let outcome = pair.pop().unwrap();
                    if !replay::valid_pair(&record, &outcome) {
                        return Err(Error::Corruption("publication outcome binding"));
                    }
                    self.recover_publication(&record, &outcome)?;
                    self.messages.push(MessageMeta {
                        offset,
                        message_id: record.message_id,
                    });
                    self.message_index.insert(offset, record.message_id);
                    offset += 2;
                }
                RecordKind::ProducerOutcome => {
                    return Err(Error::Corruption("orphan publication outcome"));
                }
                RecordKind::Fence => {
                    // The durable revocation is replayed: the revoked owner stays fenced.
                    // The record travels with the transferred history, so it only revokes
                    // the node that wrote it: the successor is not fenced by it.
                    let (migration_id, old_owner, old_epoch, next_epoch) =
                        decode_fence(&record.payload)?;
                    if old_owner == self.config.node_id {
                        self.fenced = Some(FenceState {
                            migration_id,
                            old_owner,
                            old_epoch,
                            next_epoch,
                            offset: record.logical_offset,
                        });
                    }
                    offset += 1;
                }
                _ => {
                    self.apply_group_record(&record)?;
                    offset += 1;
                }
            }
        }
        self.next = count;
        self.committed
            .store(count, std::sync::atomic::Ordering::Release);
        self.finalize_cursors();
        self.reconcile_recovered_terminal()?;
        Ok(())
    }
    /// Durably revokes the write authority of this owner.
    ///
    /// The fence is a record in the Q1 WAL: it is written and synced before the proof is
    /// returned, so a proof exists only after the durable boundary. The command runs in
    /// the same actor as the durable mutations, so every mutation accepted before the
    /// fence is already committed and every mutation after it is refused.
    fn fence_old_owner(
        &mut self,
        migration_id: u128,
        old_epoch: u64,
        next_epoch: u64,
    ) -> Result<FenceProof> {
        if self.unhealthy {
            return Err(Error::Unavailable);
        }
        if self.fenced.is_some() {
            return Err(Error::Fenced);
        }
        // Only the committed owner may revoke itself, and only for the epoch it holds.
        match self.fence.authority() {
            Some(authority)
                if authority.owner == self.config.node_id && authority.epoch == old_epoch => {}
            _ => return Err(Error::Fenced),
        }
        if migration_id == 0 || next_epoch <= old_epoch {
            return Err(Error::StaleOwner);
        }
        let payload = encode_fence(migration_id, self.config.node_id, old_epoch, next_epoch);
        let digest = fence_digest(&payload);
        let offset = self.next;
        let record = Record {
            kind: RecordKind::Fence,
            logical_offset: offset,
            owner_epoch: old_epoch,
            producer_id: 0,
            producer_epoch: 0,
            sequence: 0,
            message_id: 0,
            server_timestamp: 0,
            payload,
        };
        let result = (|| {
            self.wal.append(record).map_err(Error::Storage)?;
            self.wal.sync().map_err(Error::Storage)?;
            Ok::<_, Error>(())
        })();
        if let Err(error) = result {
            self.unhealthy = true;
            return Err(error);
        }
        self.next += 1;
        let manifest_generation = self.wal.generation();
        self.fenced = Some(FenceState {
            migration_id,
            old_owner: self.config.node_id,
            old_epoch,
            next_epoch,
            offset,
        });
        Ok(FenceProof {
            migration_id,
            old_owner: self.config.node_id,
            old_epoch,
            next_epoch,
            fence_offset: offset,
            fence_digest: digest,
            manifest_generation,
        })
    }

    /// Rebuilds producer/dedup state for one committed MESSAGE/PRODUCER_OUTCOME pair.
    fn recover_publication(&mut self, message: &Record, outcome: &Record) -> Result<()> {
        let principal = u128::from_be_bytes(outcome.payload[6..22].try_into().unwrap());
        let offset = message.logical_offset;
        if message.owner_epoch > self.config.ownership_epoch {
            return Err(Error::StaleOwner);
        }
        let token = Token {
            principal,
            producer: message.producer_id,
            epoch: message.producer_epoch,
            sequence: message.sequence,
        };
        self.validate_token(token)
            .map_err(|_| Error::Corruption("producer history"))?;
        if message.server_timestamp != 0 && message.server_timestamp < self.logical_time {
            return Err(Error::Corruption("nonmonotonic producer timestamp"));
        }
        self.logical_time = self.logical_time.max(message.server_timestamp);
        self.expire();
        if !self.producers.contains_key(&(principal, token.producer))
            && self.producers.len() >= self.config.max_producers
        {
            return Err(Error::Capacity);
        }
        if self.outcomes.len() >= self.config.max_outcomes {
            return Err(Error::Capacity);
        }
        if self
            .outcomes
            .insert(token, (offset, message.server_timestamp))
            .is_some()
        {
            return Err(Error::Corruption("duplicate outcome"));
        }
        self.producers.insert(
            (principal, token.producer),
            (
                token.epoch,
                token
                    .sequence
                    .checked_add(1)
                    .ok_or(Error::Corruption("producer sequence overflow"))?,
            ),
        );
        Ok(())
    }
    fn validate_token(&self, t: Token) -> Result<()> {
        match self.producers.get(&(t.principal, t.producer)) {
            None => {
                if t.sequence != 0 {
                    Err(Error::SequenceGap)
                } else {
                    Ok(())
                }
            }
            Some(&(epoch, next)) => {
                if t.epoch < epoch {
                    return Err(Error::ProducerFenced);
                }
                if t.epoch > epoch {
                    if t.sequence == 0 {
                        Ok(())
                    } else {
                        Err(Error::SequenceGap)
                    }
                } else if t.sequence == next {
                    Ok(())
                } else if t.sequence < next {
                    Err(Error::OutcomeUnavailable)
                } else {
                    Err(Error::SequenceGap)
                }
            }
        }
    }
    #[cfg(test)]
    fn publish(&mut self, r: PublishRequest) -> Result<PublishOutcome> {
        self.publish_before(r, None)
    }
    fn publish_before(
        &mut self,
        r: PublishRequest,
        deadline: Option<Instant>,
    ) -> Result<PublishOutcome> {
        if self.unhealthy {
            return Err(Error::Unavailable);
        }
        if r.partition != self.config.partition {
            return Err(Error::WrongPartition);
        }
        if r.qos != QoS::Q1 {
            return Err(Error::WrongClass);
        }
        if r.ownership_epoch != self.config.ownership_epoch {
            return Err(Error::StaleOwner);
        }
        // Fencing: only the committed owner may start a durable mutation.
        self.check_authority()?;
        let token = Token {
            principal: r.principal,
            producer: r.producer_id,
            epoch: r.producer_epoch,
            sequence: r.sequence,
        };
        if self
            .producers
            .get(&(token.principal, token.producer))
            .is_some_and(|&(e, _)| token.epoch < e)
        {
            return Err(Error::ProducerFenced);
        }
        self.advance_clock();
        if let Some(&(offset, _)) = self.outcomes.get(&token) {
            let old = self
                .wal
                .read(
                    DurableOffset {
                        log_incarnation: self.wal_config.incarnation,
                        sequence: offset,
                    },
                    1,
                    self.wal_config.max_record_bytes,
                )
                .map_err(|e| {
                    self.unhealthy = true;
                    Error::Storage(e)
                })?;
            if old.len() != 1 {
                return Err(Error::Corruption("missing prior message"));
            }
            if old[0].payload != r.payload || old[0].message_id != r.message_id {
                return Err(Error::SequenceConflict);
            }
            return Ok(PublishOutcome::CommittedLocal(DurableOffset {
                log_incarnation: self.wal_config.incarnation,
                sequence: offset,
            }));
        }
        if deadline.is_some_and(|end| Instant::now() >= end) {
            return Err(Error::Expired);
        }
        self.validate_token(token)?;
        if (!self
            .producers
            .contains_key(&(token.principal, token.producer))
            && self.producers.len() >= self.config.max_producers)
            || self.outcomes.len() >= self.config.max_outcomes
            || self
                .next
                .checked_add(2)
                .is_none_or(|n| n > self.wal_config.max_records as u64)
            || token.sequence == u64::MAX
        {
            return Err(Error::Capacity);
        }
        #[cfg(feature = "fault-injection")]
        {
            self.test_barrier(TestStage::BeforeAppend, token.sequence)?;
            if let Some(control) = &mut self.control {
                if control.sequence == token.sequence {
                    if let Some(point) = control.wal_fault.take() {
                        self.wal.inject_fault(point);
                    }
                }
            }
        }
        if deadline.is_some_and(|end| Instant::now() >= end) {
            return Err(Error::Expired);
        }
        let offset = self.next;
        let timestamp = self.logical_time;
        let message = Record {
            kind: RecordKind::Message,
            logical_offset: offset,
            owner_epoch: r.ownership_epoch,
            producer_id: r.producer_id,
            producer_epoch: r.producer_epoch,
            sequence: r.sequence,
            message_id: r.message_id,
            server_timestamp: timestamp,
            payload: r.payload,
        };
        let mut outcome_payload = Vec::with_capacity(OUTCOME_BYTES);
        outcome_payload.extend_from_slice(b"RQPO\0\x01");
        outcome_payload.extend(r.principal.to_be_bytes());
        outcome_payload.extend(offset.to_be_bytes());
        let outcome = Record {
            kind: RecordKind::ProducerOutcome,
            logical_offset: offset + 1,
            owner_epoch: message.owner_epoch,
            producer_id: message.producer_id,
            producer_epoch: message.producer_epoch,
            sequence: message.sequence,
            message_id: message.message_id,
            server_timestamp: message.server_timestamp,
            payload: outcome_payload,
        };
        let result = (|| {
            self.wal.append(message).map_err(Error::Storage)?;
            self.wal.append(outcome).map_err(Error::Storage)?;
            #[cfg(feature = "fault-injection")]
            self.test_barrier(TestStage::AfterAppendBeforeSync, token.sequence)?;
            self.wal.sync().map_err(Error::Storage)?;
            #[cfg(feature = "fault-injection")]
            self.test_barrier(TestStage::AfterSyncBeforeReply, token.sequence)?;
            Ok::<_, Error>(())
        })();
        if let Err(e) = result {
            self.unhealthy = true;
            return Err(e);
        }
        // The lease may have expired while the barrier ran. The record is durable, so the
        // outcome is uncertain rather than a definite failure; the producer token resolves it.
        if self
            .fence
            .check(self.config.node_id, None, Instant::now())
            .is_err()
        {
            return Err(Error::OutcomeUnknown);
        }
        self.next += 2;
        // Durability IO is excluded from logical elapsed time, conservatively extending retention.
        self.clock_sample = match (self.clock_sample, self.clock.now_millis()) {
            (Some(a), Some(b)) => Some(a.max(b)),
            (_, other) => other,
        };
        self.outcomes.insert(token, (offset, timestamp));
        self.producers.insert(
            (token.principal, token.producer),
            (token.epoch, token.sequence + 1),
        );
        self.note_message(offset, r.message_id);
        self.committed
            .store(self.next, std::sync::atomic::Ordering::Release);
        Ok(PublishOutcome::CommittedLocal(DurableOffset {
            log_incarnation: self.wal_config.incarnation,
            sequence: offset,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rovqet_wal::FaultPoint;
    use std::sync::atomic::{AtomicU64, Ordering};
    static NEXT: AtomicU64 = AtomicU64::new(0);
    struct Dir(std::path::PathBuf);
    impl Dir {
        fn new() -> Self {
            let p = std::env::temp_dir().join(format!(
                "rovqet-actor-unit-{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            ));
            std::fs::create_dir(&p).unwrap();
            Self(p)
        }
    }
    impl Drop for Dir {
        fn drop(&mut self) {
            std::fs::remove_dir_all(&self.0).unwrap();
        }
    }
    fn config() -> (WalConfig, PartitionConfig) {
        let w = WalConfig::default();
        let p = PartitionConfig {
            partition: w.partition,
            ownership_epoch: 7,
            node_id: 0,
            queue_messages: 2,
            queue_bytes: 4096,
            max_outcomes: 32,
            max_producers: 32,
            dedup_window: Duration::from_secs(86400),
            group: GroupConfig::default(),
        };
        (w, p)
    }
    fn request(sequence: u64) -> PublishRequest {
        let (w, p) = config();
        PublishRequest {
            partition: w.partition,
            qos: QoS::Q1,
            ownership_epoch: p.ownership_epoch,
            principal: 11,
            producer_id: 22,
            producer_epoch: 1,
            sequence,
            message_id: 33 + sequence as u128,
            payload: vec![sequence as u8; 32],
        }
    }
    #[test]
    fn storage_faults_never_return_success_and_poison_actor() {
        for point in [
            FaultPoint::Append,
            FaultPoint::PartialAppend,
            FaultPoint::SegmentSync,
            FaultPoint::ManifestWrite,
            FaultPoint::ManifestSync,
            FaultPoint::ManifestRename,
            FaultPoint::DirectorySync,
        ] {
            let d = Dir::new();
            let (w, p) = config();
            let mut s = State::open(&d.0, w.clone(), p.clone()).unwrap();
            assert!(matches!(
                s.publish(request(0)).unwrap(),
                PublishOutcome::CommittedLocal(_)
            ));
            s.wal.inject_fault(point);
            assert!(
                matches!(s.publish(request(1)), Err(Error::Storage(_))),
                "{point:?}"
            );
            assert!(matches!(s.publish(request(1)), Err(Error::Unavailable)));
            drop(s);
            let mut reopened = State::open(&d.0, w, p).unwrap();
            assert_eq!(
                reopened.publish(request(0)).unwrap(),
                PublishOutcome::CommittedLocal(DurableOffset {
                    log_incarnation: 1,
                    sequence: 0
                })
            );
            assert_eq!(
                reopened.publish(request(1)).unwrap(),
                PublishOutcome::CommittedLocal(DurableOffset {
                    log_incarnation: 1,
                    sequence: 2
                })
            );
        }
    }
    #[test]
    fn admission_reservation_lives_through_active_work_and_cancelled_waiter() {
        let (_, mut p) = config();
        p.queue_messages = 1;
        let (tx, rx) = mpsc::sync_channel(1);
        let budget = Budget::new(1, p.queue_bytes);
        let partition = Partition {
            sender: Some(tx),
            worker: None,
            budget: budget.clone(),
            committed: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            config: p,
            incarnation: 42,
            max_record_bytes: 1024,
        };
        let waiter = partition.try_publish(request(0)).unwrap();
        drop(waiter);
        let cmd = rx.recv().unwrap();
        assert_eq!(budget.usage().messages, 1);
        assert_eq!(
            budget.usage().bytes,
            32 + 2 * RECORD_OVERHEAD + OUTCOME_BYTES
        );
        assert!(matches!(
            partition.try_publish(request(1)),
            Err(Error::Capacity)
        ));
        drop(cmd);
        assert_eq!(budget.usage(), Usage::default());
    }
    #[test]
    fn admission_charges_retained_payload_capacity() {
        let (_, mut config) = config();
        config.queue_bytes = 256;
        let (tx, _rx) = mpsc::sync_channel(1);
        let partition = Partition {
            sender: Some(tx),
            worker: None,
            budget: Budget::new(2, 256),
            committed: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            config,
            incarnation: 42,
            max_record_bytes: 1024,
        };
        let mut r = request(0);
        r.payload = Vec::with_capacity(1024);
        r.payload.push(1);
        assert!(matches!(partition.try_publish(r), Err(Error::Capacity)));
        assert_eq!(partition.usage(), Usage::default());
        let mut r = request(0);
        r.payload = vec![1];
        let _pending = partition.try_publish(r).unwrap();
    }
    #[test]
    fn disconnected_response_is_unknown() {
        let (tx, rx) = oneshot::channel();
        drop(tx);
        assert!(matches!(
            PendingPublish { reply: rx }.wait(),
            Err(Error::OutcomeUnknown)
        ));
    }
    #[test]
    fn drop_handle_drains_and_closes_worker_before_reopen() {
        let d = Dir::new();
        let (w, p) = config();
        let actor = Partition::open(&d.0, w.clone(), p.clone()).unwrap();
        drop(actor.try_publish(request(0)).unwrap());
        drop(actor);
        let reopened = Partition::open(&d.0, w, p).unwrap();
        assert_eq!(
            reopened.try_publish(request(0)).unwrap().wait().unwrap(),
            PublishOutcome::CommittedLocal(DurableOffset {
                log_incarnation: 1,
                sequence: 0
            })
        );
        reopened.shutdown().unwrap();
    }
    #[tokio::test(flavor = "current_thread")]
    async fn async_wait_yields_to_the_runtime_and_disconnect_is_unknown() {
        let (tx, rx) = oneshot::channel();
        tokio::spawn(async move {
            tokio::task::yield_now().await;
            let _ = tx.send(Ok(PublishOutcome::CommittedLocal(DurableOffset {
                log_incarnation: 1,
                sequence: 0,
            })));
        });
        assert_eq!(
            PendingPublish { reply: rx }.wait_async().await.unwrap(),
            PublishOutcome::CommittedLocal(DurableOffset {
                log_incarnation: 1,
                sequence: 0
            })
        );
        let (tx, rx) = oneshot::channel();
        drop(tx);
        assert!(matches!(
            PendingPublish { reply: rx }.wait_async().await,
            Err(Error::OutcomeUnknown)
        ));
    }

    #[test]
    fn expired_fresh_work_never_appends_but_committed_retry_still_resolves() {
        let d = Dir::new();
        let (w, p) = config();
        let mut state = State::open(&d.0, w, p).unwrap();
        assert!(matches!(
            state.publish_before(request(0), Some(Instant::now())),
            Err(Error::Expired)
        ));
        assert_eq!(state.wal.committed_position(), None);
        let committed = state.publish(request(0)).unwrap();
        assert_eq!(
            state
                .publish_before(request(0), Some(Instant::now()))
                .unwrap(),
            committed
        );
        assert!(matches!(
            state.publish_before(request(1), Some(Instant::now())),
            Err(Error::Expired)
        ));
        assert_eq!(state.wal.committed_position().unwrap().sequence, 1);
    }

    #[cfg(feature = "fault-injection")]
    #[test]
    fn deterministic_barriers_hold_only_the_storage_worker_until_release() {
        for stage in [
            TestStage::BeforeAppend,
            TestStage::AfterAppendBeforeSync,
            TestStage::AfterSyncBeforeReply,
        ] {
            let d = Dir::new();
            let marker = Dir::new();
            let (w, p) = config();
            let actor = Partition::open_with_test_control(
                &d.0,
                w,
                p,
                TestControl {
                    directory: marker.0.clone(),
                    stage: Some(stage),
                    sequence: 0,
                    wal_fault: None,
                },
            )
            .unwrap();
            let pending = actor.try_publish(request(0)).unwrap();
            let end = Instant::now() + std::time::Duration::from_secs(5);
            let reached = marker.0.join(format!("reached-{}", stage.as_str()));
            while !reached.exists() {
                assert!(
                    Instant::now() < end,
                    "storage worker did not reach {stage:?}"
                );
                thread::yield_now();
            }
            assert_eq!(actor.usage().messages, 1);
            std::fs::write(
                marker.0.join(format!("release-{}", stage.as_str())),
                b"release",
            )
            .unwrap();
            assert!(matches!(
                pending.wait(),
                Ok(PublishOutcome::CommittedLocal(_))
            ));
            actor.shutdown().unwrap();
        }
    }
}
