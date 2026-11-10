//! Blocking Q1 partition actor foundation. Not connected to the server wire path.
//! Callers provide authenticated principal, routing authority and prevalidated TTL/ACL.
//! Open, wait and shutdown must run outside a network/async executor.
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
        mpsc::{self, Receiver, SyncSender, TrySendError},
    },
    thread::{self, JoinHandle},
    time::{SystemTime, UNIX_EPOCH},
};
const OUTCOME_BYTES: usize = 30;
#[derive(Debug, Clone)]
pub struct PartitionConfig {
    pub partition: PartitionRef,
    pub ownership_epoch: u64,
    pub queue_messages: usize,
    pub queue_bytes: usize,
    pub max_outcomes: usize,
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
    Storage(rovqet_wal::Error),
    Corruption(&'static str),
    OutcomeUnknown,
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
    reply: Receiver<Result<PublishOutcome>>,
}
impl PendingPublish {
    /// Wait for a known barrier outcome. Dropping this waiter never cancels submitted IO.
    pub fn wait(self) -> Result<PublishOutcome> {
        self.reply.recv().unwrap_or(Err(Error::OutcomeUnknown))
    }
}
struct Command {
    request: PublishRequest,
    reply: mpsc::Sender<Result<PublishOutcome>>,
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
        if config.partition != wal_config.partition
            || config.queue_messages == 0
            || config.queue_bytes == 0
            || config.max_outcomes == 0
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
                let state = State::open(&path, wal_config, actor_config);
                match state {
                    Err(e) => {
                        let _ = ready_tx.send(Err(e));
                    }
                    Ok(mut state) => {
                        if ready_tx.send(Ok(())).is_err() {
                            return;
                        }
                        while let Ok(command) = rx.recv() {
                            let result = state.publish(command.request);
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
        let (reply, rx) = mpsc::channel();
        let command = Command {
            request,
            reply,
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
    outcomes: HashMap<Token, u64>,
    producers: HashMap<(u128, u128), (u64, u64)>,
    next: u64,
    unhealthy: bool,
}
impl State {
    fn open(path: &Path, wal_config: WalConfig, config: PartitionConfig) -> Result<Self> {
        let wal = Wal::open(path, wal_config.clone()).map_err(Error::Storage)?;
        let mut s = Self {
            wal,
            wal_config,
            config,
            outcomes: HashMap::new(),
            producers: HashMap::new(),
            next: 0,
            unhealthy: false,
        };
        s.recover()?;
        Ok(s)
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
            if self.outcomes.len() >= self.config.max_outcomes {
                return Err(Error::Capacity);
            }
            if self.outcomes.insert(token, offset).is_some() {
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
                    Err(Error::SequenceConflict)
                } else {
                    Err(Error::SequenceGap)
                }
            }
        }
    }
    fn publish(&mut self, r: PublishRequest) -> Result<PublishOutcome> {
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
        if let Some(&offset) = self.outcomes.get(&token) {
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
        self.validate_token(token)?;
        if self.outcomes.len() >= self.config.max_outcomes
            || self
                .next
                .checked_add(2)
                .is_none_or(|n| n > self.wal_config.max_records as u64)
            || token.sequence == u64::MAX
        {
            return Err(Error::Capacity);
        }
        let offset = self.next;
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis()
            .min(u64::MAX as u128) as u64;
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
            self.wal.append(message)?;
            self.wal.append(outcome)?;
            self.wal.sync()?;
            Ok::<_, rovqet_wal::Error>(())
        })();
        if let Err(e) = result {
            self.unhealthy = true;
            return Err(Error::Storage(e));
        }
        self.next += 2;
        self.outcomes.insert(token, offset);
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
        let (tx, rx) = mpsc::channel();
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
}
