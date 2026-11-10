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

struct WorkerOptions {
    clock: Arc<dyn Clock>,
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
    pub queue_messages: usize,
    pub queue_bytes: usize,
    pub max_outcomes: usize,
    pub max_producers: usize,
    /// Minimum retained logical time; downtime and uncertain clock intervals do not count.
    pub dedup_window: Duration,
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
struct Command {
    request: PublishRequest,
    reply: oneshot::Sender<Result<PublishOutcome>>,
    deadline: Option<Instant>,
    _reservation: Reservation,
}
/// One caller-owned handle; shared references may submit concurrently. No socket or Q0 dependencies.
pub struct Partition {
    sender: Option<SyncSender<Command>>,
    worker: Option<JoinHandle<()>>,
    budget: Arc<Budget>,
    config: PartitionConfig,
    max_record_bytes: usize,
}
impl Partition {
    /// Blocking recovery and worker initialization. The WAL directory must already exist.
    pub fn open(
        path: impl AsRef<Path>,
        wal_config: WalConfig,
        config: PartitionConfig,
    ) -> Result<Self> {
        Self::open_inner(path, wal_config, config, WorkerOptions::default())
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
        let budget = Budget::new(config.queue_messages, config.queue_bytes);
        let (tx, rx) = mpsc::sync_channel::<Command>(config.queue_messages);
        let (ready_tx, ready_rx) = mpsc::sync_channel(1);
        let actor_config = config.clone();
        let worker = thread::Builder::new()
            .name("rovqet-q1-partition".into())
            .spawn(move || {
                let state = State::open_clock(&path, wal_config, actor_config, _options.clock);
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
                        while let Ok(command) = rx.recv() {
                            let result = state.publish_before(command.request, command.deadline);
                            drop(command._reservation);
                            let _ = command.reply.send(result);
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
                config,
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
        let command = Command {
            request,
            reply,
            deadline,
            _reservation: reservation,
        };
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
    next: u64,
    unhealthy: bool,
    #[cfg(feature = "fault-injection")]
    control: Option<TestControl>,
}
impl State {
    #[cfg(test)]
    fn open(path: &Path, wal_config: WalConfig, config: PartitionConfig) -> Result<Self> {
        Self::open_clock(path, wal_config, config, WorkerOptions::default().clock)
    }
    fn open_clock(
        path: &Path,
        wal_config: WalConfig,
        config: PartitionConfig,
        clock: Arc<dyn Clock>,
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
        if count % 2 != 0 {
            return Err(Error::Corruption("incomplete durable publication pair"));
        }
        while self.next < count {
            let records = self
                .wal
                .read(
                    DurableOffset {
                        log_incarnation: self.wal_config.incarnation,
                        sequence: self.next,
                    },
                    2,
                    self.wal_config
                        .max_record_bytes
                        .checked_mul(2)
                        .ok_or(Error::InvalidConfig)?,
                )
                .map_err(Error::Storage)?;
            if records.len() != 2 {
                return Err(Error::Corruption("missing publication pair"));
            }
            let message = &records[0];
            let outcome = &records[1];
            if message.kind != RecordKind::Message
                || outcome.kind != RecordKind::ProducerOutcome
                || outcome.payload.len() != OUTCOME_BYTES
                || &outcome.payload[..4] != b"RQPO"
                || outcome.payload[4..6] != [0, 1]
            {
                return Err(Error::Corruption("publication outcome format"));
            }
            let principal = u128::from_be_bytes(outcome.payload[6..22].try_into().unwrap());
            let offset = u64::from_be_bytes(outcome.payload[22..30].try_into().unwrap());
            if offset != message.logical_offset
                || outcome.logical_offset != message.logical_offset + 1
                || message.owner_epoch != outcome.owner_epoch
                || message.producer_id != outcome.producer_id
                || message.producer_epoch != outcome.producer_epoch
                || message.sequence != outcome.sequence
                || message.message_id != outcome.message_id
                || message.server_timestamp != outcome.server_timestamp
            {
                return Err(Error::Corruption("publication outcome binding"));
            }
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
            self.next += 2;
        }
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
            queue_messages: 2,
            queue_bytes: 4096,
            max_outcomes: 32,
            max_producers: 32,
            dedup_window: Duration::from_secs(86400),
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
            config: p,
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
            config,
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
