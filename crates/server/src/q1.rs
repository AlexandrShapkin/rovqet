//! One explicitly configured Q1 route for the trusted loopback development profile.
use fs2::FileExt;
use rovqet_core::{PartitionRef, PublishOutcome, QoS};
use rovqet_durable::{Error as DurableError, Partition, PartitionConfig, PublishRequest};
use rovqet_protocol::{ErrorCode, Field, Frame, FrameType, registry::field as f};
use rovqet_q0::Broker;
use rovqet_transport::{Outbound, SessionHandler, SessionReply};
use std::{
    collections::HashMap,
    fs::{File, OpenOptions},
    io,
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};

#[derive(Debug, Clone)]
pub struct Options {
    pub directory: PathBuf,
    pub topic: String,
    pub partition: PartitionRef,
    pub incarnation: u128,
    pub owner_epoch: u64,
}
fn invalid(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message.into())
}
impl Options {
    pub fn parse(values: &HashMap<String, String>) -> io::Result<Option<Self>> {
        if values.is_empty() {
            return Ok(None);
        }
        let get = |key: &str| {
            values
                .get(key)
                .ok_or_else(|| invalid(format!("Q1 requires {key}")))
        };
        let number = |key: &str| -> io::Result<u64> {
            get(key)?
                .parse()
                .map_err(|_| invalid(format!("invalid {key}")))
        };
        let topic = get("--q1-topic")?.clone();
        if !rovqet_router::valid_topic_name(&topic)
            || matches!(topic.as_str(), "demo" | "telemetry")
        {
            return Err(invalid(
                "Q1 topic must be valid and separate from Q0 demo/telemetry",
            ));
        }
        let result = Self {
            directory: PathBuf::from(get("--q1-data-dir")?),
            topic,
            partition: PartitionRef {
                tenant_id: number("--q1-tenant-id")?,
                topic_id: number("--q1-topic-id")?,
                partition_id: u32::try_from(number("--q1-partition-id")?)
                    .map_err(|_| invalid("invalid Q1 partition ID"))?,
            },
            incarnation: get("--q1-incarnation")?
                .parse()
                .map_err(|_| invalid("invalid Q1 incarnation"))?,
            owner_epoch: number("--q1-owner-epoch")?,
        };
        if result.incarnation == 0
            || result.partition.tenant_id != 1
            || result.partition.topic_id <= 2
            || result.owner_epoch == 0
        {
            return Err(invalid(
                "development Q1 needs tenant 1, topic ID >2 and nonzero incarnation/owner epoch",
            ));
        }
        Ok(Some(result))
    }
}

pub struct Route {
    partition: Partition,
    options: Options,
    // Kept until the actor is drained and closed. Never unlink a lock file on crash/restart.
    _lock: File,
}
impl Route {
    /// Blocking startup: acquire exclusive ownership, validate/recover storage, then allow session processing.
    pub fn open(options: Options) -> io::Result<Self> {
        let dir = std::fs::canonicalize(&options.directory)?;
        let lock = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(dir.join("SERVER.lock"))?;
        lock.try_lock_exclusive().map_err(|e| {
            io::Error::new(
                e.kind(),
                format!("Q1 storage already owned or locking unavailable: {e}"),
            )
        })?;
        // Persist the caller-created directory and lock entry. No data-directory creation or reset.
        if let Some(parent) = dir.parent() {
            File::open(parent)?.sync_all()?;
        }
        File::open(&dir)?.sync_all()?;
        let wal = rovqet_wal::Config {
            partition: options.partition,
            incarnation: options.incarnation,
            ..Default::default()
        };
        let config = PartitionConfig {
            partition: options.partition,
            ownership_epoch: options.owner_epoch,
            queue_messages: 64,
            queue_bytes: 4 * 1024 * 1024,
            max_outcomes: 100_000,
        };
        #[cfg(feature = "fault-injection")]
        let partition = if let Some(control) = test_control()? {
            Partition::open_with_test_control(&dir, wal, config, control)
        } else {
            Partition::open(&dir, wal, config)
        };
        #[cfg(not(feature = "fault-injection"))]
        let partition = Partition::open(&dir, wal, config);
        let partition = partition.map_err(|e| {
            io::Error::other(format!("Q1 recovery failed (history preserved): {e}"))
        })?;
        Ok(Self {
            partition,
            options,
            _lock: lock,
        })
    }
    pub fn shutdown(self) -> io::Result<()> {
        self.partition
            .shutdown()
            .map_err(|e| io::Error::other(format!("Q1 shutdown: {e}")))
    }
    fn publish(&self, principal: u128, frame: Frame) -> Result<SessionReply, ErrorCode> {
        if frame.field(f::QOS) != Some(&[1][..]) {
            return Err(ErrorCode::UnsupportedGuarantee);
        }
        if principal != 1 {
            return Err(ErrorCode::PermissionDenied);
        }
        if let Some(claimed) = frame.field(f::PRINCIPAL_ID) {
            if uint::<8>(claimed)? != principal {
                return Err(ErrorCode::PermissionDenied);
            }
        }
        for (id, value) in [
            (f::TENANT_ID, self.options.partition.tenant_id),
            (f::TOPIC_ID, self.options.partition.topic_id),
        ] {
            if let Some(claimed) = frame.field(id) {
                if uint::<8>(claimed)? != value as u128 {
                    return Err(ErrorCode::PermissionDenied);
                }
            }
        }
        if number::<4>(&frame, f::PARTITION_ID)? != self.options.partition.partition_id as u128 {
            return Err(ErrorCode::UnsupportedGuarantee);
        }
        if number::<16>(&frame, f::LOG_INCAR)? != self.options.incarnation {
            return Err(ErrorCode::ReplayNotAvailable);
        }
        if number::<8>(&frame, f::OWNERSHIP_EPOCH)? != self.options.owner_epoch as u128 {
            return Err(ErrorCode::StaleEpoch);
        }
        let ttl = number::<4>(&frame, f::TTL_MS)? as u64;
        if ttl == 0 || ttl > 30_000 {
            return Err(ErrorCode::Expired);
        }
        let producer_id = number::<16>(&frame, f::PRODUCER_ID)?;
        if producer_id == 0 {
            return Err(ErrorCode::BadFrame);
        }
        let request_id = frame.request_id;
        let sequence = number::<8>(&frame, f::PRODUCER_SEQUENCE)? as u64;
        let request = PublishRequest {
            partition: self.options.partition,
            qos: QoS::Q1,
            ownership_epoch: self.options.owner_epoch,
            principal,
            producer_id,
            producer_epoch: number::<8>(&frame, f::PRODUCER_EPOCH)? as u64,
            sequence,
            message_id: number::<16>(&frame, f::MESSAGE_ID)?,
            payload: frame.payload,
        };
        let pending = self
            .partition
            .try_publish_before(request, Instant::now() + Duration::from_millis(ttl))
            .map_err(map_error)?;
        #[cfg(feature = "fault-injection")]
        eprintln!("Q1_TEST_ADMITTED {producer_id} {sequence}");
        let partition = self.options.partition;
        let incarnation = self.options.incarnation;
        Ok(SessionReply::Deferred(Box::pin(async move {
            match pending.wait_async().await.map_err(map_error)? {
                PublishOutcome::CommittedLocal(offset) if offset.log_incarnation == incarnation => {
                    let mut ack = Frame::new(FrameType::PubAck, request_id);
                    ack.fields = vec![
                        Field::required(f::QOS, [1]),
                        Field::required(f::PUB_OUTCOME, [2]),
                        Field::required(f::TENANT_ID, partition.tenant_id.to_be_bytes()),
                        Field::required(f::TOPIC_ID, partition.topic_id.to_be_bytes()),
                        Field::required(f::PARTITION_ID, partition.partition_id.to_be_bytes()),
                        Field::required(f::LOG_INCAR, offset.log_incarnation.to_be_bytes()),
                        Field::required(f::OFFSET, offset.sequence.to_be_bytes()),
                    ];
                    Ok(Some(ack))
                }
                _ => Err(ErrorCode::OutcomeUnknown),
            }
        })))
    }
}
fn uint<const N: usize>(bytes: &[u8]) -> Result<u128, ErrorCode> {
    if bytes.len() != N || N > 16 {
        return Err(ErrorCode::BadFrame);
    }
    let mut buf = [0; 16];
    buf[16 - N..].copy_from_slice(bytes);
    Ok(u128::from_be_bytes(buf))
}
fn number<const N: usize>(frame: &Frame, id: u64) -> Result<u128, ErrorCode> {
    uint::<N>(frame.field(id).ok_or(ErrorCode::BadFrame)?)
}
fn map_error(error: DurableError) -> ErrorCode {
    match error {
        DurableError::WrongPartition | DurableError::WrongClass => ErrorCode::UnsupportedGuarantee,
        DurableError::StaleOwner => ErrorCode::StaleEpoch,
        DurableError::ProducerFenced => ErrorCode::ProducerFenced,
        DurableError::SequenceGap => ErrorCode::SequenceGap,
        DurableError::SequenceConflict => ErrorCode::SequenceConflict,
        DurableError::Capacity => ErrorCode::ResourceExhausted,
        DurableError::Expired => ErrorCode::ExpiredBeforeDelivery,
        DurableError::Unavailable | DurableError::InvalidConfig | DurableError::WorkerStart(_) => {
            ErrorCode::StorageUnavailable
        }
        DurableError::Storage(_) | DurableError::Corruption(_) | DurableError::OutcomeUnknown => {
            ErrorCode::OutcomeUnknown
        }
    }
}

pub struct Dispatch {
    pub broker: Arc<Broker>,
    pub route: Option<Arc<Route>>,
    draining: AtomicBool,
}
impl Dispatch {
    pub fn new(broker: Arc<Broker>, route: Option<Arc<Route>>) -> Self {
        Self {
            broker,
            route,
            draining: AtomicBool::new(false),
        }
    }
    pub fn drain(&self) {
        self.draining.store(true, Ordering::Release);
        self.broker.drain();
    }
}
impl SessionHandler for Dispatch {
    fn handle(
        &self,
        session: u128,
        frame: Frame,
        outbound: Outbound,
    ) -> Result<Option<Frame>, ErrorCode> {
        self.broker.handle(session, frame, outbound)
    }
    fn request(
        &self,
        session: u128,
        principal: u128,
        frame: Frame,
        outbound: Outbound,
    ) -> SessionReply {
        if frame.frame_type == FrameType::Publish {
            if self.draining.load(Ordering::Acquire) {
                return SessionReply::Immediate(Err(ErrorCode::ServerDraining));
            }
            let topic = frame
                .field(f::TOPIC_NAME)
                .and_then(|b| std::str::from_utf8(b).ok());
            if let Some(route) = &self.route {
                if topic == Some(&route.options.topic) {
                    return route
                        .publish(principal, frame)
                        .unwrap_or_else(|e| SessionReply::Immediate(Err(e)));
                }
            }
            if frame.field(f::QOS) != Some(&[0][..]) {
                return SessionReply::Immediate(Err(ErrorCode::UnsupportedGuarantee));
            }
        }
        SessionReply::Immediate(self.handle(session, frame, outbound))
    }
    fn disconnected(&self, session: u128) {
        self.broker.disconnected(session)
    }
}

pub fn validate_test_environment() -> io::Result<()> {
    #[cfg(not(feature = "fault-injection"))]
    if std::env::vars_os().any(|(key, _)| key.to_string_lossy().starts_with("ROVQET_Q1_TEST_")) {
        return Err(invalid(
            "Q1 test hooks require the explicit fault-injection build feature",
        ));
    }
    Ok(())
}
#[cfg(feature = "fault-injection")]
fn test_control() -> io::Result<Option<rovqet_durable::TestControl>> {
    use rovqet_durable::{TestControl, TestStage};
    use rovqet_wal::FaultPoint;
    let Some(directory) = std::env::var_os("ROVQET_Q1_TEST_DIR") else {
        return Ok(None);
    };
    let stage = match std::env::var("ROVQET_Q1_TEST_STAGE").ok().as_deref() {
        None => None,
        Some("before-append") => Some(TestStage::BeforeAppend),
        Some("after-append-before-sync") => Some(TestStage::AfterAppendBeforeSync),
        Some("after-sync-before-reply") => Some(TestStage::AfterSyncBeforeReply),
        _ => return Err(invalid("invalid Q1 test stage")),
    };
    let wal_fault = match std::env::var("ROVQET_Q1_TEST_WAL_FAULT").ok().as_deref() {
        None => None,
        Some("append") => Some(FaultPoint::Append),
        Some("partial-append") => Some(FaultPoint::PartialAppend),
        Some("segment-sync") => Some(FaultPoint::SegmentSync),
        Some("manifest-write") => Some(FaultPoint::ManifestWrite),
        Some("manifest-sync") => Some(FaultPoint::ManifestSync),
        Some("manifest-rename") => Some(FaultPoint::ManifestRename),
        Some("directory-sync") => Some(FaultPoint::DirectorySync),
        _ => return Err(invalid("invalid Q1 WAL fault")),
    };
    let sequence = std::env::var("ROVQET_Q1_TEST_SEQUENCE")
        .unwrap_or_else(|_| "0".into())
        .parse()
        .map_err(|_| invalid("invalid Q1 test sequence"))?;
    Ok(Some(TestControl {
        directory: directory.into(),
        stage,
        sequence,
        wal_fault,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    fn values() -> HashMap<String, String> {
        [
            ("--q1-data-dir", "/unused"),
            ("--q1-topic", "durable"),
            ("--q1-tenant-id", "1"),
            ("--q1-topic-id", "3"),
            ("--q1-partition-id", "0"),
            ("--q1-incarnation", "42"),
            ("--q1-owner-epoch", "7"),
        ]
        .into_iter()
        .map(|(k, v)| (k.into(), v.into()))
        .collect()
    }
    #[test]
    fn q1_is_explicit_and_configuration_is_atomic() {
        assert!(Options::parse(&HashMap::new()).unwrap().is_none());
        let config = values();
        let parsed = Options::parse(&config).unwrap().unwrap();
        assert_eq!(
            parsed.partition,
            PartitionRef {
                tenant_id: 1,
                topic_id: 3,
                partition_id: 0
            }
        );
        assert_eq!(parsed.incarnation, 42);
        for key in config.keys() {
            let mut incomplete = config.clone();
            incomplete.remove(key);
            assert!(Options::parse(&incomplete).is_err(), "{key}");
        }
        for (key, value) in [
            ("--q1-topic", "demo"),
            ("--q1-topic", "bad//topic"),
            ("--q1-tenant-id", "2"),
            ("--q1-topic-id", "1"),
            ("--q1-incarnation", "0"),
            ("--q1-owner-epoch", "0"),
            ("--q1-partition-id", "4294967296"),
        ] {
            let mut invalid = values();
            invalid.insert(key.into(), value.into());
            assert!(Options::parse(&invalid).is_err(), "{key}={value}");
        }
    }
    #[test]
    fn admitted_storage_failures_are_uncertain_but_unavailable_is_explicit() {
        assert_eq!(
            map_error(DurableError::OutcomeUnknown),
            ErrorCode::OutcomeUnknown
        );
        assert_eq!(
            map_error(DurableError::Storage(rovqet_wal::Error::Poisoned)),
            ErrorCode::OutcomeUnknown
        );
        assert_eq!(
            map_error(DurableError::Unavailable),
            ErrorCode::StorageUnavailable
        );
        assert_eq!(
            map_error(DurableError::Expired),
            ErrorCode::ExpiredBeforeDelivery
        );
        assert_eq!(
            map_error(DurableError::Capacity),
            ErrorCode::ResourceExhausted
        );
    }
}
