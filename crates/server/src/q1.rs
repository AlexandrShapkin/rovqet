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
    pub dedup_window: Duration,
    pub max_producers: usize,
    pub max_outcomes: usize,
    pub initialize_new_log: bool,
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
            initialize_new_log: values
                .get("--q1-initialize-new-log")
                .map(|v| v.parse())
                .transpose()
                .map_err(|_| invalid("initialize-new-log must be true or false"))?
                .unwrap_or(false),
            dedup_window: Duration::from_millis(
                values
                    .get("--q1-dedup-window-ms")
                    .map(|v| v.parse())
                    .transpose()
                    .map_err(|_| invalid("invalid dedup window"))?
                    .unwrap_or(86_400_000),
            ),
            max_producers: values
                .get("--q1-max-producers")
                .map(|v| v.parse())
                .transpose()
                .map_err(|_| invalid("invalid producer limit"))?
                .unwrap_or(100_000),
            max_outcomes: values
                .get("--q1-max-outcomes")
                .map(|v| v.parse())
                .transpose()
                .map_err(|_| invalid("invalid outcome limit"))?
                .unwrap_or(100_000),
        };
        if result.dedup_window.is_zero()
            || result.max_producers == 0
            || result.max_outcomes == 0
            || result.incarnation == 0
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
        // Resuming an incarnation is fail-closed even if every WAL file was lost.
        // Explicit initialization is only for new empty storage with a caller-allocated fresh incarnation.
        if options.initialize_new_log {
            if std::fs::read_dir(&dir)?.next().is_some() {
                return Err(invalid(
                    "new-log initialization requires an empty directory and fresh incarnation",
                ));
            }
        } else if !dir.join("MANIFEST").is_file() {
            return Err(io::Error::other(
                "REPLAY_NOT_AVAILABLE: existing Q1 history missing; no automatic initialization",
            ));
        }
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
        if options.initialize_new_log {
            for entry in std::fs::read_dir(&dir)? {
                if entry?.file_name() != "SERVER.lock" {
                    return Err(invalid(
                        "new-log initialization raced with existing history",
                    ));
                }
            }
        }
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
            max_outcomes: options.max_outcomes,
            max_producers: options.max_producers,
            dedup_window: options.dedup_window,
        };
        #[cfg(feature = "fault-injection")]
        let partition = if let Some(control) = test_control()? {
            Partition::open_with_test_control(&dir, wal, config, control)
        } else if options.initialize_new_log {
            Partition::open(&dir, wal, config)
        } else {
            Partition::open_existing(&dir, wal, config)
        };
        #[cfg(not(feature = "fault-injection"))]
        let partition = if options.initialize_new_log {
            Partition::open(&dir, wal, config)
        } else {
            Partition::open_existing(&dir, wal, config)
        };
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
    fn read(
        &self,
        principal: u128,
        frame: Frame,
        outbound: Outbound,
    ) -> Result<SessionReply, ErrorCode> {
        use rovqet_durable::{ReplayRequest, ReplayStop};
        use rovqet_protocol::replay::{self, ReadScope, ReadStatus};
        if principal != 1 {
            return Err(ErrorCode::PermissionDenied);
        }
        if frame
            .field(f::PRINCIPAL_ID)
            .is_some_and(|v| v != 1u64.to_be_bytes())
        {
            return Err(ErrorCode::PermissionDenied);
        }
        for (id, expected) in [
            (f::TENANT_ID, self.options.partition.tenant_id),
            (f::TOPIC_ID, self.options.partition.topic_id),
        ] {
            if frame.field(id).is_some_and(|v| v != expected.to_be_bytes()) {
                return Err(ErrorCode::PermissionDenied);
            }
        }
        if frame
            .field(f::TOPIC_NAME)
            .is_some_and(|v| v != self.options.topic.as_bytes())
        {
            return Err(ErrorCode::UnsupportedGuarantee);
        }
        if frame.field(f::TOPIC_NAME).is_none() && frame.field(f::TOPIC_ID).is_none() {
            return Err(ErrorCode::BadFrame);
        }
        if number::<4>(&frame, f::PARTITION_ID)? != self.options.partition.partition_id as u128
            || number::<16>(&frame, f::LOG_INCAR)? != self.options.incarnation
        {
            return Err(ErrorCode::ReplayNotAvailable);
        }
        if frame.field(f::QOS) != Some(&[1][..]) {
            return Err(ErrorCode::UnsupportedGuarantee);
        }
        let request = ReplayRequest {
            partition: self.options.partition,
            from: rovqet_core::DurableOffset {
                log_incarnation: self.options.incarnation,
                sequence: number::<8>(&frame, f::OFFSET)? as u64,
            },
            max_messages: number::<4>(&frame, f::MAX_MESSAGES)? as usize,
            max_bytes: number::<4>(&frame, f::MAX_BYTES)? as usize,
            wait: Duration::from_millis(
                frame
                    .field(f::WAIT_MS)
                    .map(uint::<4>)
                    .transpose()?
                    .unwrap_or(0) as u64,
            ),
        };
        let pending = self
            .partition
            .try_replay_encoded(request, replay::END_BYTES, replay::record_bytes)
            .map_err(map_read_error)?;
        let id = frame.request_id;
        let scope = ReadScope {
            tenant: self.options.partition.tenant_id,
            topic: self.options.partition.topic_id,
            partition: self.options.partition.partition_id,
            incarnation: self.options.incarnation,
        };
        Ok(SessionReply::ReadOnly(Box::pin(async move {
            let batch = Arc::new(pending.wait_async().await.map_err(map_read_error)?);
            let limits = outbound.limits();
            // Validate the whole snapshot before the first write; no skipped oversized record.
            for message in &batch.messages {
                if message.payload.len() > limits.max_payload_bytes
                    || replay::record_bytes(message.payload.len())
                        .is_none_or(|n| n > limits.max_frame_bytes)
                {
                    return Err(ErrorCode::ResourceExhausted);
                }
            }
            for message in &batch.messages {
                let record = replay::record(
                    id,
                    scope,
                    message.offset.sequence,
                    message.message_id,
                    message.payload.clone(),
                );
                outbound.send_read(&record, batch.clone()).await?;
            }
            let status = match batch.stop {
                ReplayStop::Empty => ReadStatus::Empty,
                ReplayStop::End => ReadStatus::End,
                ReplayStop::CountLimit => ReadStatus::CountLimit,
                ReplayStop::ByteLimit => ReadStatus::ByteLimit,
                ReplayStop::WaitExpired => ReadStatus::WaitExpired,
            };
            outbound
                .send_read(
                    &replay::end(id, scope, batch.next.sequence, status),
                    batch.clone(),
                )
                .await?;
            Ok(None)
        })))
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
        // The direct route has fixed effective partition/priority. Key and priority hints
        // have no durable effect. TTL is an admission deadline, not persisted message TTL.
        // Reject unsupported durable attributes rather than silently dropping their meaning.
        for id in [
            f::GROUP_ID,
            f::GROUP_GENERATION,
            f::LEASE_ID,
            f::OFFSET,
            f::SOURCE_LOG_INCAR,
            f::DISPOSITION_ID,
            f::ACK_DEADLINE_MS,
        ] {
            if frame.field(id).is_some() {
                return Err(ErrorCode::UnsupportedCapability);
            }
        }
        if let Some(digest) = frame.field(f::PAYLOAD_DIGEST_SHA256) {
            use sha2::{Digest, Sha256};
            if digest != Sha256::digest(&frame.payload).as_slice() {
                return Err(ErrorCode::SequenceConflict);
            }
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
fn map_read_error(error: rovqet_durable::ReplayError) -> ErrorCode {
    use rovqet_durable::ReplayError::*;
    match error {
        WrongPartition | IncarnationMismatch | ReplayNotAvailable => ErrorCode::ReplayNotAvailable,
        InvalidLimits => ErrorCode::BadFrame,
        ResponseTooSmall { .. } | Capacity => ErrorCode::ResourceExhausted,
        Unavailable | Storage(_) | Corruption(_) => ErrorCode::StorageUnavailable,
    }
}
fn map_error(error: DurableError) -> ErrorCode {
    match error {
        DurableError::WrongPartition | DurableError::WrongClass => ErrorCode::UnsupportedGuarantee,
        DurableError::StaleOwner => ErrorCode::StaleEpoch,
        DurableError::ProducerFenced => ErrorCode::ProducerFenced,
        DurableError::SequenceGap => ErrorCode::SequenceGap,
        DurableError::SequenceConflict => ErrorCode::SequenceConflict,
        DurableError::OutcomeUnavailable => ErrorCode::OutcomeUnknown,
        DurableError::HistoryUnavailable => ErrorCode::ReplayNotAvailable,
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
        if frame.frame_type == FrameType::ReadFetch {
            if self.draining.load(Ordering::Acquire) {
                return SessionReply::Immediate(Err(ErrorCode::ServerDraining));
            }
            return match &self.route {
                Some(route) => route
                    .read(principal, frame, outbound)
                    .unwrap_or_else(|e| SessionReply::Immediate(Err(e))),
                None => SessionReply::Immediate(Err(ErrorCode::UnsupportedGuarantee)),
            };
        }
        // Major-1 durable MESSAGE requires a real group/generation/lease. Until B-05
        // there is no legal standalone FETCH response or terminal batch frame.
        if matches!(
            frame.frame_type,
            FrameType::Fetch
                | FrameType::ConsumerAck
                | FrameType::ConsumerNack
                | FrameType::ExtendLease
        ) {
            return SessionReply::Immediate(Err(ErrorCode::UnsupportedCapability));
        }
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
        assert!(!parsed.initialize_new_log);
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
    #[tokio::test]
    async fn canonical_route_checks_claims_digest_and_unsupported_attributes() {
        let path =
            std::env::temp_dir().join(format!("rovqet-route-canonical-{}", std::process::id()));
        std::fs::create_dir(&path).unwrap();
        let mut options = Options::parse(&values()).unwrap().unwrap();
        options.directory = path.clone();
        options.initialize_new_log = true;
        let route = Route::open(options).unwrap();
        let mut frame = Frame::new(FrameType::Publish, 1);
        frame.fields = vec![
            Field::required(f::TOPIC_NAME, b"durable"),
            Field::required(f::QOS, [1]),
            Field::required(f::PARTITION_ID, 0u32.to_be_bytes()),
            Field::required(f::LOG_INCAR, 42u128.to_be_bytes()),
            Field::required(f::OWNERSHIP_EPOCH, 7u64.to_be_bytes()),
            Field::required(f::TTL_MS, 30000u32.to_be_bytes()),
            Field::required(f::PRODUCER_ID, 11u128.to_be_bytes()),
            Field::required(f::PRODUCER_EPOCH, 0u64.to_be_bytes()),
            Field::required(f::PRODUCER_SEQUENCE, 0u64.to_be_bytes()),
            Field::required(f::MESSAGE_ID, 100u128.to_be_bytes()),
        ];
        frame.payload = b"event".to_vec();
        assert!(matches!(
            route.publish(2, frame.clone()),
            Err(ErrorCode::PermissionDenied)
        ));
        for (id, value, error) in [
            (
                f::PRINCIPAL_ID,
                2u64.to_be_bytes().to_vec(),
                ErrorCode::PermissionDenied,
            ),
            (
                f::TENANT_ID,
                2u64.to_be_bytes().to_vec(),
                ErrorCode::PermissionDenied,
            ),
            (
                f::TOPIC_ID,
                4u64.to_be_bytes().to_vec(),
                ErrorCode::PermissionDenied,
            ),
            (
                f::GROUP_ID,
                1u64.to_be_bytes().to_vec(),
                ErrorCode::UnsupportedCapability,
            ),
            (
                f::PAYLOAD_DIGEST_SHA256,
                vec![0; 32],
                ErrorCode::SequenceConflict,
            ),
        ] {
            let mut changed = frame.clone();
            changed.fields.push(Field::optional(id, value));
            assert!(matches!(route.publish(1, changed), Err(e) if e == error));
        }
        async fn settle(reply: SessionReply) -> Frame {
            match reply {
                SessionReply::Deferred(pending) => pending.await.unwrap().unwrap(),
                _ => panic!("expected deferred durable result"),
            }
        }
        let original = settle(route.publish(1, frame.clone()).unwrap()).await;
        // Request correlation, route hints and admission TTL are not durable identity.
        frame.request_id = 999;
        frame.fields.retain(|v| v.id != f::TTL_MS);
        frame
            .fields
            .push(Field::required(f::TTL_MS, 2000u32.to_be_bytes()));
        frame.fields.push(Field::optional(
            f::PARTITION_KEY,
            b"ignored-direct-route-hint",
        ));
        frame.fields.push(Field::optional(f::PRIORITY, [0]));
        use sha2::{Digest, Sha256};
        frame.fields.push(Field::optional(
            f::PAYLOAD_DIGEST_SHA256,
            Sha256::digest(&frame.payload).to_vec(),
        ));
        let retry = settle(route.publish(1, frame.clone()).unwrap()).await;
        assert_eq!(original.field(f::OFFSET), retry.field(f::OFFSET));
        assert_eq!(retry.request_id, 999);
        frame.payload.push(1); // even with the same request ID, digest must be checked again
        assert!(matches!(
            route.publish(1, frame),
            Err(ErrorCode::SequenceConflict)
        ));
        route.shutdown().unwrap();
        let wal = rovqet_wal::Wal::open(
            &path,
            rovqet_wal::Config {
                partition: PartitionRef {
                    tenant_id: 1,
                    topic_id: 3,
                    partition_id: 0,
                },
                incarnation: 42,
                ..Default::default()
            },
        )
        .unwrap();
        assert_eq!(wal.committed_position().unwrap().sequence, 1);
        drop(wal);
        std::fs::remove_dir_all(path).unwrap();
    }
    #[test]
    fn resumption_never_initializes_missing_history_and_initialization_never_resets_it() {
        let root = std::env::temp_dir().join(format!("rovqet-route-resume-{}", std::process::id()));
        std::fs::create_dir(&root).unwrap();
        let path = root.join("wal");
        std::fs::create_dir(&path).unwrap();
        let mut options = Options::parse(&values()).unwrap().unwrap();
        options.directory = path.clone();
        assert!(Route::open(options.clone()).is_err());
        assert_eq!(std::fs::read_dir(&path).unwrap().count(), 0);
        options.initialize_new_log = true;
        Route::open(options.clone()).unwrap().shutdown().unwrap();
        let manifest = std::fs::read(path.join("MANIFEST")).unwrap();
        assert!(Route::open(options.clone()).is_err());
        assert_eq!(std::fs::read(path.join("MANIFEST")).unwrap(), manifest);
        options.initialize_new_log = false;
        Route::open(options.clone()).unwrap().shutdown().unwrap();
        std::fs::rename(&path, root.join("unavailable-history")).unwrap();
        std::fs::create_dir(&path).unwrap();
        assert!(Route::open(options).is_err());
        assert_eq!(std::fs::read_dir(&path).unwrap().count(), 0);
        std::fs::remove_dir_all(root).unwrap();
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
    struct ReadServer {
        root: PathBuf,
        route: Arc<Route>,
        address: std::net::SocketAddr,
        stop: tokio::sync::oneshot::Sender<()>,
        task: tokio::task::JoinHandle<io::Result<()>>,
    }
    impl ReadServer {
        async fn start(label: &str) -> Self {
            let root =
                std::env::temp_dir().join(format!("rovqet-read-{label}-{}", std::process::id()));
            std::fs::create_dir(&root).unwrap();
            let mut options = Options::parse(&values()).unwrap().unwrap();
            options.directory = root.clone();
            let wal = rovqet_wal::Config {
                partition: options.partition,
                incarnation: 42,
                max_segment_bytes: 512,
                max_record_bytes: 256,
                ..Default::default()
            };
            let config = PartitionConfig {
                partition: options.partition,
                ownership_epoch: 7,
                queue_messages: 64,
                queue_bytes: 4 * 1024 * 1024,
                max_outcomes: 1000,
                max_producers: 100,
                dedup_window: Duration::from_secs(86400),
            };
            let partition = Partition::open(&root, wal, config).unwrap();
            let route = Arc::new(Route {
                partition,
                options,
                _lock: File::create(root.join("SERVER.lock")).unwrap(),
            });
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            let (stop, closed) = tokio::sync::oneshot::channel();
            let task = tokio::spawn(rovqet_transport::serve(
                listener,
                Default::default(),
                Arc::new(Dispatch::new(
                    crate::development_broker(1024),
                    Some(route.clone()),
                )),
                async {
                    let _ = closed.await;
                },
            ));
            Self {
                root,
                route,
                address,
                stop,
                task,
            }
        }
        async fn client(&self) -> rovqet_sdk::Client {
            rovqet_sdk::Client::connect(rovqet_sdk::ClientConfig {
                endpoint: self.address,
                ..Default::default()
            })
            .await
            .unwrap()
        }
        fn options(
            &self,
            offset: u64,
            messages: u32,
            bytes: u32,
            wait: u64,
        ) -> rovqet_sdk::ReadOptions {
            rovqet_sdk::ReadOptions {
                partition: self.route.options.partition,
                from: rovqet_sdk::DurableOffset {
                    log_incarnation: 42,
                    sequence: offset,
                },
                max_messages: messages,
                max_bytes: bytes,
                wait: Duration::from_millis(wait),
            }
        }
        async fn publish(client: &mut rovqet_sdk::Client, seq: u64) {
            client
                .publish_q1(
                    "durable",
                    format!("event-{seq}").as_bytes(),
                    rovqet_sdk::Q1PublishOptions {
                        producer_id: 11,
                        producer_epoch: 0,
                        sequence: seq,
                        message_id: 100 + seq as u128,
                        partition_id: 0,
                        log_incarnation: 42,
                        ownership_epoch: 7,
                        ttl: Duration::from_secs(30),
                    },
                )
                .await
                .unwrap();
        }
        async fn finish(self) {
            let _ = self.stop.send(());
            self.task.await.unwrap().unwrap();
            let route = Arc::try_unwrap(self.route)
                .ok()
                .expect("all sessions release route ownership");
            tokio::task::spawn_blocking(move || route.shutdown().unwrap())
                .await
                .unwrap();
            std::fs::remove_dir_all(self.root).unwrap();
        }
    }
    #[tokio::test]
    async fn sdk_tcp_replay_exact_bounds_continuation_segments_and_read_only_producer_state() {
        use rovqet_sdk::{ReadError, ReadStatus};
        let server = ReadServer::start("limits").await;
        let mut client = server.client().await;
        let empty = client
            .read_q1("durable", server.options(0, 8, 78, 0))
            .await
            .unwrap();
        assert_eq!(empty.status, ReadStatus::Empty);
        assert_eq!(empty.encoded_bytes, 78);
        for seq in 0..7 {
            ReadServer::publish(&mut client, seq).await;
        }
        let original_manifest = std::fs::read(server.root.join("MANIFEST")).unwrap();
        assert!(
            std::fs::read_dir(&server.root)
                .unwrap()
                .filter_map(Result::ok)
                .filter(|entry| entry.file_name().to_string_lossy().starts_with("segment-"))
                .count()
                > 1
        );
        let count = client
            .read_q1("durable", server.options(0, 2, 2048, 0))
            .await
            .unwrap();
        assert_eq!(count.status, ReadStatus::CountLimit);
        assert_eq!(count.next.sequence, 4);
        assert!(count.has_more);
        assert_eq!(
            count
                .records
                .iter()
                .map(|r| r.offset.sequence)
                .collect::<Vec<_>>(),
            vec![0, 2]
        );
        let byte_limit = 78 + rovqet_protocol::replay::record_bytes(7).unwrap() as u32;
        let bytes = client
            .read_q1("durable", server.options(4, 8, byte_limit, 0))
            .await
            .unwrap();
        assert_eq!(bytes.status, ReadStatus::ByteLimit);
        assert_eq!(bytes.encoded_bytes, byte_limit as usize);
        assert_eq!(bytes.next.sequence, 6);
        assert!(matches!(
            client
                .read_q1("durable", server.options(4, 8, byte_limit - 1, 0))
                .await,
            Err(ReadError::ResourceExhausted)
        ));
        let rest = client
            .read_q1("durable", server.options(6, 8, 2048, 0))
            .await
            .unwrap();
        assert_eq!(rest.records.len(), 4);
        assert_eq!(rest.next.sequence, 14);
        assert!(rest.end_of_current_history);
        for (i, record) in rest.records.iter().enumerate() {
            assert_eq!(record.message_id, 103 + i as u128);
            assert_eq!(record.payload, format!("event-{}", i + 3).as_bytes());
        }
        let end = client
            .read_q1("durable", server.options(14, 8, 78, 0))
            .await
            .unwrap();
        assert!(end.records.is_empty());
        assert_eq!(end.status, ReadStatus::End);
        assert_eq!(
            std::fs::read(server.root.join("MANIFEST")).unwrap(),
            original_manifest
        );
        // All reads leave producer high-water unchanged: exact duplicate still settles, next seq commits.
        ReadServer::publish(&mut client, 0).await;
        assert_eq!(
            std::fs::read(server.root.join("MANIFEST")).unwrap(),
            original_manifest
        );
        ReadServer::publish(&mut client, 7).await;
        assert_eq!(
            client
                .read_q1("durable", server.options(14, 8, 2048, 0))
                .await
                .unwrap()
                .records[0]
                .offset
                .sequence,
            14
        );
        client.close().await.unwrap();
        server.finish().await;
    }
    #[tokio::test]
    async fn tcp_long_poll_publication_wait_expiry_cancellation_and_authorization() {
        use rovqet_sdk::{ReadError, ReadStatus};
        let server = ReadServer::start("wait").await;
        let mut reader = server.client().await;
        let mut writer = server.client().await;
        let waiting = reader.read_q1("durable", server.options(0, 8, 2048, 30000));
        tokio::pin!(waiting);
        tokio::select! {result=&mut waiting=>panic!("unexpected result {result:?}"),_=tokio::task::yield_now()=>{}}
        ReadServer::publish(&mut writer, 0).await;
        let batch = waiting.await.unwrap();
        assert_eq!(batch.records.len(), 1);
        assert_eq!(batch.status, ReadStatus::End);
        let mut reader = server.client().await;
        // Expiry at a nonzero committed end keeps the requested offset as continuation.
        let expiry = server.options(2, 8, 2048, 1);
        let timeout = reader.read_q1("durable", expiry.clone()).await.unwrap();
        assert_eq!(timeout.status, ReadStatus::WaitExpired);
        assert!(timeout.records.is_empty());
        assert_eq!(timeout.next, expiry.from);
        assert!(!timeout.has_more);
        assert!(timeout.end_of_current_history);
        let mut wrong = server.options(0, 8, 2048, 0);
        wrong.partition.tenant_id = 2;
        assert!(matches!(
            reader.read_q1("durable", wrong).await,
            Err(ReadError::PermissionDenied)
        ));
        let mut wrong = server.options(0, 8, 2048, 0);
        wrong.partition.partition_id = 1;
        assert!(matches!(
            reader.read_q1("durable", wrong).await,
            Err(ReadError::ReplayNotAvailable)
        ));
        let mut wrong = server.options(0, 8, 2048, 0);
        wrong.from.log_incarnation = 41;
        assert!(matches!(
            reader.read_q1("durable", wrong).await,
            Err(ReadError::ReplayNotAvailable)
        ));
        assert!(matches!(
            reader
                .read_q1("durable", server.options(1, 8, 2048, 0))
                .await,
            Err(ReadError::ReplayNotAvailable)
        ));
        // More cancelled sessions than actor slots: EOF must release long-poll reservations promptly.
        for _ in 0..70 {
            let mut cancelled = server.client().await;
            assert!(
                tokio::time::timeout(
                    Duration::from_millis(2),
                    cancelled.read_q1("durable", server.options(2, 8, 2048, 30000))
                )
                .await
                .is_err()
            );
        }
        let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        while server.route.partition.usage().messages != 0 {
            assert!(tokio::time::Instant::now() < deadline);
            tokio::task::yield_now().await;
        }
        ReadServer::publish(&mut writer, 1).await;
        reader.close().await.unwrap();
        writer.close().await.unwrap();
        server.finish().await;
    }
    #[tokio::test]
    async fn tcp_replay_corrupt_or_missing_committed_segment_never_completes_a_batch() {
        for missing in [false, true] {
            let server = ReadServer::start(if missing { "missing" } else { "corrupt" }).await;
            let mut client = server.client().await;
            ReadServer::publish(&mut client, 0).await;
            let path = std::fs::read_dir(&server.root)
                .unwrap()
                .filter_map(Result::ok)
                .find(|entry| entry.file_name().to_string_lossy().starts_with("segment-"))
                .unwrap()
                .path();
            if missing {
                std::fs::rename(&path, server.root.join("unavailable-segment")).unwrap();
            } else {
                let mut bytes = std::fs::read(&path).unwrap();
                bytes[60] ^= 1;
                std::fs::write(&path, bytes).unwrap();
            }
            assert!(matches!(
                client
                    .read_q1("durable", server.options(0, 8, 2048, 0))
                    .await,
                Err(rovqet_sdk::ReadError::StorageUnavailable)
            ));
            client.close().await.unwrap();
            server.finish().await;
        }
    }
}
