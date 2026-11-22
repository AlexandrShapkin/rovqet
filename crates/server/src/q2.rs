//! The opt-in Q2 publish route of the development broker.
//!
//! The route embeds one shard voter of `rovqet-consensus` behind the existing Publish
//! frame: no new client protocol, no Q2 consumer/ACK work. It is configured with the
//! `--q2-*` flags, exists only in cluster mode (the voter needs the cluster node
//! identity and obeys the committed metadata authority) and is reached only for a
//! QoS-2 publication whose topic is the configured Q2 topic. Every other frame keeps
//! its Q0/Q1 behaviour byte for byte.
//!
//! Loopback only and bounded inline payload: the shard transport refuses a non-loopback
//! listener and [`MAX_INLINE_PAYLOAD_BYTES`] bounds what one Raft entry may carry.
use rovqet_consensus::{
    ShardAuthority, ShardNode, ShardNodeConfig,
    sm::ShardState,
    types::{
        MAX_INLINE_PAYLOAD_BYTES, PublishErrorCode, ShardCommand, ShardCommandResponse,
        ShardPublish,
    },
};
use rovqet_core::PartitionRef;
use rovqet_protocol::{ErrorCode, Field, Frame, FrameType, registry::field as f};
use rovqet_transport::SessionReply;
use std::{collections::HashMap, io, net::SocketAddr, path::PathBuf};

/// The Q2 route configuration, parsed from the `--q2-*` flags.
///
/// `--q2-peer` is repeatable on the command line; the caller hands the repeated values
/// over as one space-separated list, because the option collector is a map.
#[derive(Debug, Clone)]
pub struct Options {
    pub data_directory: PathBuf,
    pub topic: String,
    pub partition: PartitionRef,
    pub incarnation: u128,
    pub shard_id: u64,
    pub listen: SocketAddr,
    /// Every voter of the group, including this node.
    pub peers: Vec<(u64, SocketAddr)>,
    pub bootstrap: bool,
}

fn invalid(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message.into())
}

impl Options {
    /// Parses the Q2 flags. Absent flags mean "no Q2 route", exactly like Q1.
    ///
    /// The topic must be a valid router topic that is neither a Q0 topic nor the Q1
    /// topic; the caller owns the Q1 comparison, because the Q1 topic is not part of
    /// this map.
    pub fn parse(values: &HashMap<String, String>) -> io::Result<Option<Self>> {
        if values.is_empty() {
            return Ok(None);
        }
        let get = |key: &str| {
            values
                .get(key)
                .ok_or_else(|| invalid(format!("Q2 requires {key}")))
        };
        let number = |key: &str| -> io::Result<u64> {
            get(key)?
                .parse()
                .map_err(|_| invalid(format!("invalid {key}")))
        };
        let topic = get("--q2-topic")?.clone();
        if !rovqet_router::valid_topic_name(&topic)
            || matches!(topic.as_str(), "demo" | "telemetry")
        {
            return Err(invalid(
                "Q2 topic must be valid and separate from Q0 demo/telemetry",
            ));
        }
        let listen: SocketAddr = get("--q2-raft-listen")?
            .parse()
            .map_err(|_| invalid("invalid Q2 raft listen address"))?;
        if !listen.ip().is_loopback() {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "Q2 raft listener must be loopback",
            ));
        }
        let mut peers = Vec::new();
        for entry in get("--q2-peer")?.split_whitespace() {
            let (id, address) = entry
                .split_once('=')
                .ok_or_else(|| invalid("Q2 peer must be node=host:port"))?;
            let id: u64 = id.parse().map_err(|_| invalid("invalid Q2 peer node id"))?;
            if id == 0 {
                return Err(invalid("Q2 peer node id must be nonzero"));
            }
            let address: SocketAddr = address
                .parse()
                .map_err(|_| invalid("invalid Q2 peer address"))?;
            if !address.ip().is_loopback() {
                return Err(io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    "Q2 peers must be loopback",
                ));
            }
            if peers.iter().any(|(existing, _)| *existing == id) {
                return Err(invalid("duplicate Q2 peer node id"));
            }
            peers.push((id, address));
        }
        if peers.len() != 3 {
            return Err(invalid("Q2 needs exactly three peers"));
        }
        let bootstrap = match get("--q2-bootstrap")?.as_str() {
            "true" => true,
            "false" => false,
            _ => return Err(invalid("--q2-bootstrap must be true or false")),
        };
        let result = Self {
            data_directory: PathBuf::from(get("--q2-data-dir")?),
            topic,
            partition: PartitionRef {
                tenant_id: number("--q2-tenant-id")?,
                topic_id: number("--q2-topic-id")?,
                partition_id: u32::try_from(number("--q2-partition-id")?)
                    .map_err(|_| invalid("invalid Q2 partition ID"))?,
            },
            incarnation: get("--q2-incarnation")?
                .parse()
                .map_err(|_| invalid("invalid Q2 incarnation"))?,
            shard_id: number("--q2-shard-id")?,
            listen,
            peers,
            bootstrap,
        };
        if result.partition.tenant_id != 1
            || result.partition.topic_id <= 2
            || result.incarnation == 0
            || result.shard_id == 0
        {
            return Err(invalid(
                "development Q2 needs tenant 1, topic ID >2 and nonzero incarnation/shard ID",
            ));
        }
        Ok(Some(result))
    }
}

/// The shard-group authority of a committed placement, for the voter of this node.
pub fn shard_authority(authority: rovqet_durable::Authority) -> ShardAuthority {
    ShardAuthority {
        owner: authority.owner,
        epoch: authority.epoch,
    }
}

/// The Q2 publish route of this process: one embedded shard voter plus the admission
/// rules of the QoS-2 publication path.
///
/// # PubAck outcome code
///
/// A committed Q2 publication answers with `f::PUB_OUTCOME = 3`, the new
/// **COMMITTED_QUORUM** code: the publication is committed by the shard group quorum
/// and applied to the deterministic state machine. Q1 reports `2`
/// (**COMMITTED_LOCAL**) for a locally durable commit, and Q0 has no PubAck outcome.
/// A proposal that never became certain (no leader, no quorum, timeout) is neither a
/// success nor a rejection: it fails with `ErrorCode::OutcomeUnknown` and the caller
/// retries the idempotent publication.
pub struct Route {
    node: ShardNode,
    options: Options,
}

impl Route {
    /// Blocking startup of the embedded voter: bind the peer listener, open the durable
    /// group storage and join (or bootstrap) the shard group.
    ///
    /// The shard voter is an asynchronous Raft node, so this is awaited as a task
    /// instead of running in the blocking pool like the Q1 storage open.
    pub async fn open(options: Options, node_id: u64, cluster_id: u128) -> io::Result<Self> {
        if node_id == 0 {
            return Err(invalid("the Q2 node id must be nonzero"));
        }
        if !options.peers.iter().any(|(id, _)| *id == node_id) {
            return Err(invalid("the Q2 peer list must include this node"));
        }
        let node = ShardNode::start(ShardNodeConfig {
            id: node_id,
            cluster_id,
            shard_id: options.shard_id,
            listen: options.listen,
            peers: options.peers.clone(),
            data_directory: options.data_directory.clone(),
            bootstrap: options.bootstrap,
            election_timeout_ms: 300,
            heartbeat_interval_ms: 80,
            max_payload_entries: 64,
            snapshot_logs_since_last: 8,
        })
        .await
        .map_err(|error| io::Error::other(format!("Q2 route startup failed: {error}")))?;
        Ok(Self { node, options })
    }
    /// The configured Q2 topic this route serves.
    pub fn topic(&self) -> &str {
        &self.options.topic
    }
    /// The configured partition of this route.
    pub fn partition(&self) -> PartitionRef {
        self.options.partition
    }
    /// The configured data incarnation of this route.
    pub fn incarnation(&self) -> u128 {
        self.options.incarnation
    }
    /// Installs the committed ownership authority of the shard group. The voter never
    /// mints an epoch: a proposal from another epoch is refused before it becomes a
    /// Raft entry, and without an installed authority no frame can claim a valid one.
    pub fn install_authority(&self, authority: Option<ShardAuthority>) {
        self.node.install_authority(authority);
    }
    /// The applied, committed state of the shard group.
    pub fn applied_state(&self) -> ShardState {
        self.node.applied_state()
    }
    /// The current leader of the embedded group, if this node knows one.
    #[cfg(test)]
    pub(crate) async fn leader(&self) -> Option<u64> {
        self.node.current_leader().await
    }
    /// Stops the embedded voter and releases its resources.
    pub async fn shutdown(self) -> io::Result<()> {
        self.node
            .shutdown()
            .await
            .map_err(|error| io::Error::other(format!("Q2 shutdown: {error}")))
    }
    /// Admits one QoS-2 publication and proposes it to the shard group.
    ///
    /// The validation below is exact: nothing is proposed before every claim of the
    /// frame matches the configuration and the installed metadata epoch, so a refused
    /// publication creates no Raft entry at all.
    pub async fn publish(&self, principal: u128, frame: Frame) -> Result<SessionReply, ErrorCode> {
        if frame.field(f::QOS) != Some(&[2][..]) {
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
        if let Some(claimed) = frame.field(f::TOPIC_NAME) {
            if claimed != self.options.topic.as_bytes() {
                return Err(ErrorCode::PermissionDenied);
            }
        }
        if number::<4>(&frame, f::PARTITION_ID)? != self.options.partition.partition_id as u128 {
            return Err(ErrorCode::PermissionDenied);
        }
        if number::<16>(&frame, f::LOG_INCAR)? != self.options.incarnation {
            return Err(ErrorCode::ReplayNotAvailable);
        }
        // The metadata owns the epoch. Without an installed authority there is no
        // epoch a frame could legitimately claim, so the route stays fenced.
        let metadata_epoch = self
            .node
            .authority()
            .map(|authority| authority.epoch)
            .ok_or(ErrorCode::StaleEpoch)?;
        if number::<8>(&frame, f::OWNERSHIP_EPOCH)? != metadata_epoch as u128 {
            return Err(ErrorCode::StaleEpoch);
        }
        let producer_id = number::<16>(&frame, f::PRODUCER_ID)?;
        if producer_id == 0 {
            return Err(ErrorCode::BadFrame);
        }
        if frame.payload.is_empty() || frame.payload.len() > MAX_INLINE_PAYLOAD_BYTES {
            return Err(ErrorCode::ResourceExhausted);
        }
        let payload_digest = sha256(&frame.payload);
        if let Some(digest) = frame.field(f::PAYLOAD_DIGEST_SHA256) {
            if digest != payload_digest.as_slice() {
                return Err(ErrorCode::SequenceConflict);
            }
        }
        let request_id = frame.request_id;
        let message_id = number::<16>(&frame, f::MESSAGE_ID)?;
        let command = ShardCommand::Publish(ShardPublish {
            partition: self.options.partition,
            durable_class: 2,
            incarnation: self.options.incarnation,
            metadata_epoch,
            principal,
            producer_id,
            producer_epoch: number::<8>(&frame, f::PRODUCER_EPOCH)? as u64,
            sequence: number::<8>(&frame, f::PRODUCER_SEQUENCE)? as u64,
            message_id,
            payload: frame.payload,
            payload_digest,
        });
        match self.node.propose(command).await {
            Ok(ShardCommandResponse::Published { logical_offset, .. }) => {
                let mut ack = Frame::new(FrameType::PubAck, request_id);
                ack.fields = vec![
                    Field::required(f::QOS, [2]),
                    Field::required(f::PUB_OUTCOME, [3]),
                    Field::required(f::TENANT_ID, self.options.partition.tenant_id.to_be_bytes()),
                    Field::required(f::TOPIC_ID, self.options.partition.topic_id.to_be_bytes()),
                    Field::required(
                        f::PARTITION_ID,
                        self.options.partition.partition_id.to_be_bytes(),
                    ),
                    Field::required(f::LOG_INCAR, self.options.incarnation.to_be_bytes()),
                    Field::required(f::OFFSET, logical_offset.to_be_bytes()),
                    Field::required(f::MESSAGE_ID, message_id.to_be_bytes()),
                ];
                Ok(SessionReply::Immediate(Ok(Some(ack))))
            }
            Ok(ShardCommandResponse::PublishRejected { code, .. }) => Err(map_publish_error(code)),
            // Any other committed response of this command is not a publication
            // outcome this route can report: the caller must retry the idempotent
            // publication instead of guessing a definite result.
            Ok(_) | Err(_) => Err(ErrorCode::OutcomeUnknown),
        }
    }
}

/// The `sha256` of `payload`, as the deterministic command digest.
fn sha256(payload: &[u8]) -> [u8; 32] {
    use sha2::{Digest, Sha256};
    Sha256::digest(payload).into()
}

/// Maps a deterministic refusal of the state machine onto the broker error codes.
fn map_publish_error(code: PublishErrorCode) -> ErrorCode {
    match code {
        PublishErrorCode::WrongClass => ErrorCode::UnsupportedGuarantee,
        PublishErrorCode::StaleEpoch
        | PublishErrorCode::StaleOwnership
        | PublishErrorCode::IncarnationMismatch => ErrorCode::StaleEpoch,
        PublishErrorCode::ProducerFenced => ErrorCode::ProducerFenced,
        PublishErrorCode::SequenceGap => ErrorCode::SequenceGap,
        PublishErrorCode::SequenceConflict | PublishErrorCode::PayloadDigestMismatch => {
            ErrorCode::SequenceConflict
        }
        PublishErrorCode::PayloadTooLarge | PublishErrorCode::ResourceExhausted => {
            ErrorCode::ResourceExhausted
        }
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{Duration, Instant};

    fn values() -> HashMap<String, String> {
        [
            ("--q2-data-dir", "/unused"),
            ("--q2-topic", "durable-q2"),
            ("--q2-tenant-id", "1"),
            ("--q2-topic-id", "3"),
            ("--q2-partition-id", "0"),
            ("--q2-shard-id", "9"),
            ("--q2-incarnation", "42"),
            ("--q2-raft-listen", "127.0.0.1:7500"),
            (
                "--q2-peer",
                "1=127.0.0.1:7501 2=127.0.0.1:7502 3=127.0.0.1:7503",
            ),
            ("--q2-bootstrap", "true"),
        ]
        .into_iter()
        .map(|(key, value)| (key.into(), value.into()))
        .collect()
    }

    #[test]
    fn q2_is_explicit_and_configuration_is_atomic() {
        assert!(Options::parse(&HashMap::new()).unwrap().is_none());
        let config = values();
        let parsed = Options::parse(&config).unwrap().unwrap();
        assert_eq!(parsed.partition.topic_id, 3);
        assert_eq!(parsed.shard_id, 9);
        assert_eq!(parsed.peers.len(), 3);
        assert!(parsed.bootstrap);
        for key in config.keys() {
            let mut incomplete = config.clone();
            incomplete.remove(key);
            assert!(Options::parse(&incomplete).is_err(), "{key}");
        }
        for (key, value) in [
            ("--q2-topic", "demo"),
            ("--q2-topic", "bad//topic"),
            ("--q2-tenant-id", "2"),
            ("--q2-topic-id", "2"),
            ("--q2-incarnation", "0"),
            ("--q2-shard-id", "0"),
            ("--q2-partition-id", "4294967296"),
            ("--q2-raft-listen", "0.0.0.0:7500"),
            ("--q2-bootstrap", "maybe"),
        ] {
            let mut wrong = config.clone();
            wrong.insert(key.into(), value.into());
            assert!(Options::parse(&wrong).is_err(), "{key}={value}");
        }
        for peers in [
            "1=127.0.0.1:7501 2=127.0.0.1:7502",
            "1=127.0.0.1:7501 2=127.0.0.1:7502 1=127.0.0.1:7504",
            "1=127.0.0.1:7501 2=127.0.0.1:7502 3=10.0.0.1:7503",
            "1=127.0.0.1:7501 2=127.0.0.1:7502 3",
        ] {
            let mut wrong = config.clone();
            wrong.insert("--q2-peer".into(), peers.into());
            assert!(Options::parse(&wrong).is_err(), "{peers}");
        }
    }

    /// An unused loopback port, so parallel tests never share a listener.
    fn free_port() -> u16 {
        std::net::TcpListener::bind("127.0.0.1:0")
            .expect("loopback port")
            .local_addr()
            .expect("local address")
            .port()
    }

    /// A single-node Q2 group: bootstrap, one peer that is this node, loopback, temp dir.
    async fn open_route() -> (Route, tempfile::TempDir) {
        let directory = tempfile::tempdir().expect("temporary Q2 directory");
        let mut last = None;
        for _ in 0..5 {
            let port = free_port();
            let listen: SocketAddr = format!("127.0.0.1:{port}")
                .parse()
                .expect("loopback address");
            let options = Options {
                data_directory: directory.path().to_path_buf(),
                topic: "durable-q2".into(),
                partition: PartitionRef {
                    tenant_id: 1,
                    topic_id: 3,
                    partition_id: 0,
                },
                incarnation: 42,
                shard_id: 9,
                listen,
                peers: vec![(1, listen)],
                bootstrap: true,
            };
            match Route::open(options, 1, 7).await {
                Ok(route) => {
                    route.install_authority(Some(ShardAuthority { owner: 1, epoch: 5 }));
                    return (route, directory);
                }
                // The chosen port was taken in between; another one may still work.
                Err(error) => last = Some(error),
            }
        }
        panic!("could not start the single-node Q2 group: {last:?}");
    }

    /// Waits, bounded, until the single voter knows itself as the leader.
    async fn wait_for_leader(route: &Route) {
        let deadline = Instant::now() + Duration::from_secs(15);
        while route.leader().await.is_none() {
            assert!(
                Instant::now() < deadline,
                "no Q2 leader within the deadline"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    fn publish_frame(payload: Vec<u8>) -> Frame {
        let mut frame = Frame::new(FrameType::Publish, 1);
        frame.fields = vec![
            Field::required(f::TOPIC_NAME, b"durable-q2"),
            Field::required(f::QOS, [2]),
            Field::required(f::TENANT_ID, 1u64.to_be_bytes()),
            Field::required(f::TOPIC_ID, 3u64.to_be_bytes()),
            Field::required(f::PARTITION_ID, 0u32.to_be_bytes()),
            Field::required(f::LOG_INCAR, 42u128.to_be_bytes()),
            Field::required(f::OWNERSHIP_EPOCH, 5u64.to_be_bytes()),
            Field::required(f::PRODUCER_ID, 11u128.to_be_bytes()),
            Field::required(f::PRODUCER_EPOCH, 0u64.to_be_bytes()),
            Field::required(f::PRODUCER_SEQUENCE, 0u64.to_be_bytes()),
            Field::required(f::MESSAGE_ID, 100u128.to_be_bytes()),
        ];
        frame.payload = payload;
        frame
    }

    fn replace(frame: &mut Frame, id: u64, value: impl Into<Vec<u8>>) {
        frame.fields.retain(|field| field.id != id);
        frame.fields.push(Field::required(id, value));
    }

    /// Publishes with a bounded retry: an uncertain proposal is never a definite
    /// result, so the idempotent publication is offered again until it becomes certain.
    async fn publish_until_certain(route: &Route, frame: Frame) -> Frame {
        let deadline = Instant::now() + Duration::from_secs(20);
        loop {
            match route.publish(1, frame.clone()).await {
                Ok(SessionReply::Immediate(Ok(Some(ack)))) => return ack,
                Ok(SessionReply::Immediate(Err(error))) | Err(error)
                    if error == ErrorCode::OutcomeUnknown =>
                {
                    assert!(
                        Instant::now() < deadline,
                        "the publication never became certain within the deadline"
                    );
                    tokio::time::sleep(Duration::from_millis(25)).await;
                }
                Ok(SessionReply::Immediate(Err(error))) => {
                    panic!("the publication was refused: {error:?}")
                }
                _ => panic!("unexpected Q2 publication reply"),
            }
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_qos1_publication_to_the_q2_topic_is_refused() {
        let (route, directory) = open_route().await;
        let mut frame = publish_frame(b"q1".to_vec());
        replace(&mut frame, f::QOS, vec![1]);
        assert!(matches!(
            route.publish(1, frame).await,
            Err(ErrorCode::UnsupportedGuarantee)
        ));
        assert!(route.applied_state().partitions.is_empty());
        route.shutdown().await.unwrap();
        drop(directory);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_stale_ownership_epoch_creates_no_raft_entry() {
        let (route, directory) = open_route().await;
        wait_for_leader(&route).await;
        let before = route.applied_state();
        let mut frame = publish_frame(b"stale".to_vec());
        replace(&mut frame, f::OWNERSHIP_EPOCH, 4u64.to_be_bytes());
        assert!(matches!(
            route.publish(1, frame).await,
            Err(ErrorCode::StaleEpoch)
        ));
        let after = route.applied_state();
        assert_eq!(before.revision, after.revision);
        assert_eq!(
            before.last_applied.map(|log_id| log_id.index),
            after.last_applied.map(|log_id| log_id.index)
        );
        assert!(after.partitions.is_empty());
        route.shutdown().await.unwrap();
        drop(directory);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn an_oversized_payload_is_refused() {
        let (route, directory) = open_route().await;
        let frame = publish_frame(vec![0u8; MAX_INLINE_PAYLOAD_BYTES + 1]);
        assert!(matches!(
            route.publish(1, frame).await,
            Err(ErrorCode::ResourceExhausted)
        ));
        assert!(route.applied_state().partitions.is_empty());
        route.shutdown().await.unwrap();
        drop(directory);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_committed_publication_reports_the_quorum_outcome_and_replays_it() {
        let (route, directory) = open_route().await;
        wait_for_leader(&route).await;
        let frame = publish_frame(b"event".to_vec());
        let ack = publish_until_certain(&route, frame.clone()).await;
        assert_eq!(ack.request_id, 1);
        assert_eq!(ack.field(f::QOS), Some(&[2][..]));
        assert_eq!(ack.field(f::PUB_OUTCOME), Some(&[3][..]));
        assert_eq!(ack.field(f::TENANT_ID), Some(1u64.to_be_bytes().as_slice()));
        assert_eq!(ack.field(f::TOPIC_ID), Some(3u64.to_be_bytes().as_slice()));
        assert_eq!(
            ack.field(f::PARTITION_ID),
            Some(0u32.to_be_bytes().as_slice())
        );
        assert_eq!(
            ack.field(f::LOG_INCAR),
            Some(42u128.to_be_bytes().as_slice())
        );
        assert_eq!(ack.field(f::OFFSET), Some(0u64.to_be_bytes().as_slice()));
        assert_eq!(
            ack.field(f::MESSAGE_ID),
            Some(100u128.to_be_bytes().as_slice())
        );
        // An identical retry is the idempotent replay of the same token: the same
        // committed offset is reported and no second message is created.
        let retry = publish_until_certain(&route, frame).await;
        assert_eq!(
            retry.field(f::OFFSET),
            Some(0u64.to_be_bytes().as_slice()),
            "a replay must report the original logical offset"
        );
        assert_eq!(
            route
                .applied_state()
                .partitions
                .values()
                .next()
                .map(|state| state.next_logical_offset),
            Some(1),
            "the replay must not consume a second offset"
        );
        route.shutdown().await.unwrap();
        drop(directory);
    }
}
