//! The opt-in Q2 publish and leased consumer route of the development broker.
//!
//! The route embeds one shard voter of `rovqet-consensus` behind the existing Publish
//! and consumer frames: no second client protocol. It is configured with the
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
use std::{
    collections::HashMap,
    io,
    net::SocketAddr,
    path::PathBuf,
    time::{Duration, Instant},
};

const MAX_LEASES: usize = 4096;
const MAX_GROUP_INFLIGHT: usize = 128;
const MAX_GROUP_BATCH: usize = 128;
const MAX_GROUP_BYTES: usize = 4 * 1024 * 1024;
const LEASE_DURATION: Duration = Duration::from_secs(30);
const PROPOSAL_TIMEOUT: Duration = Duration::from_secs(3);

struct Lease {
    command: ShardCommand,
    expires: Instant,
    delivered: bool,
}

/// Process-local leases are not consumer progress. Only the replicated ACK closes
/// an offset. A fresh random namespace prevents restart from reusing old grants.
struct Leases {
    namespace: u64,
    sequence: u64,
    entries: HashMap<u128, Lease>,
}

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
    node_id: u64,
    leases: tokio::sync::Mutex<Leases>,
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
        let mut namespace = [0u8; 8];
        // Development broker is a Unix/loopback profile; fail closed if the OS
        // random source is unavailable instead of minting predictable lease IDs.
        std::io::Read::read_exact(&mut std::fs::File::open("/dev/urandom")?, &mut namespace)?;
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
        Ok(Self {
            node,
            options,
            node_id,
            leases: tokio::sync::Mutex::new(Leases {
                namespace: u64::from_be_bytes(namespace),
                sequence: 0,
                entries: HashMap::new(),
            }),
        })
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
    fn group_scope(&self, principal: u128, frame: &Frame) -> Result<u64, ErrorCode> {
        if principal != 1
            || frame
                .field(f::PRINCIPAL_ID)
                .is_some_and(|v| v != 1u64.to_be_bytes())
            || number::<8>(frame, f::TENANT_ID)? != self.options.partition.tenant_id as u128
            || number::<8>(frame, f::TOPIC_ID)? != self.options.partition.topic_id as u128
            || frame
                .field(f::TOPIC_NAME)
                .is_some_and(|v| v != self.options.topic.as_bytes())
        {
            return Err(ErrorCode::PermissionDenied);
        }
        if frame.field(f::QOS) != Some(&[2][..]) {
            return Err(ErrorCode::UnsupportedGuarantee);
        }
        if number::<4>(frame, f::PARTITION_ID)? != self.options.partition.partition_id as u128
            || number::<16>(frame, f::LOG_INCAR)? != self.options.incarnation
        {
            return Err(ErrorCode::ReplayNotAvailable);
        }
        let group = number::<8>(frame, f::GROUP_ID)? as u64;
        if group == 0 {
            return Err(ErrorCode::BadFrame);
        }
        Ok(group)
    }

    fn group_partition<'a>(
        &self,
        state: &'a ShardState,
    ) -> Result<&'a rovqet_consensus::sm::PartitionState, ErrorCode> {
        state
            .partition(&(&self.options.partition).into())
            .filter(|p| p.incarnation == self.options.incarnation && !p.messages.is_empty())
            .ok_or(ErrorCode::ReplayNotAvailable)
    }

    /// Installs the partition-wide fence through Raft, never a local counter.
    /// Claims have no group selector in the replicated contract; every prior
    /// group's generation is fenced. Concurrent claims may be rejected/retried.
    pub async fn claim_q2_group(&self, requested_generation: u64) -> Result<u64, ErrorCode> {
        let epoch = self.node.authority().ok_or(ErrorCode::StaleEpoch)?.epoch;
        let command = ShardCommand::GroupClaim {
            partition: self.options.partition,
            incarnation: self.options.incarnation,
            metadata_epoch: epoch,
            requested_generation,
        };
        match tokio::time::timeout(PROPOSAL_TIMEOUT, self.node.propose(command)).await {
            Ok(Ok(ShardCommandResponse::GroupClaimed { generation })) => Ok(generation),
            Ok(Ok(ShardCommandResponse::Rejected { reason })) => Err(if reason.contains("epoch") {
                ErrorCode::StaleEpoch
            } else if reason.contains("incarnation") || reason.contains("partition") {
                ErrorCode::ReplayNotAvailable
            } else {
                ErrorCode::StaleGeneration
            }),
            _ => Err(ErrorCode::OutcomeUnknown),
        }
    }

    /// Builds a bounded batch from applied committed history only. New leases
    /// start *undelivered*: the transport calls `delivered_q2` only after each
    /// MESSAGE's socket write completes. A cancelled batch cannot authorize ACKs
    /// for messages that never crossed that boundary.
    pub async fn fetch_q2_group(
        &self,
        principal: u128,
        frame: Frame,
    ) -> Result<Vec<Frame>, ErrorCode> {
        let group_id = self.group_scope(principal, &frame)?;
        let max_messages = number::<4>(&frame, f::MAX_MESSAGES)? as usize;
        let max_bytes = number::<4>(&frame, f::MAX_BYTES)? as usize;
        let wait = frame
            .field(f::WAIT_MS)
            .map(uint::<4>)
            .transpose()?
            .unwrap_or(0);
        if max_messages == 0
            || max_messages > MAX_GROUP_BATCH
            || !(128..=MAX_GROUP_BYTES).contains(&max_bytes)
            || wait > 30_000
        {
            return Err(ErrorCode::ResourceExhausted);
        }
        // Fetch serves an immediate committed snapshot (WAIT_MS is an upper
        // bound, not a promise to wait). Only the current leader grants leases.
        if self.node.current_leader().await != Some(self.node_id) {
            return Err(ErrorCode::OutcomeUnknown);
        }
        let mut leases = self.leases.lock().await;
        leases
            .entries
            .retain(|_, lease| lease.expires > Instant::now());
        let epoch = self.node.authority().ok_or(ErrorCode::StaleEpoch)?.epoch;
        let generation = frame
            .field(f::GROUP_GENERATION)
            .map(uint::<8>)
            .transpose()?
            .unwrap_or(0) as u64;
        let generation = if generation == 0 {
            let state = self.node.applied_state();
            let next = self
                .group_partition(&state)?
                .group_generation
                .checked_add(1)
                .ok_or(ErrorCode::ResourceExhausted)?;
            self.claim_q2_group(next).await?
        } else {
            generation
        };
        let state = self.node.applied_state();
        let partition = self.group_partition(&state)?;
        if generation != partition.group_generation {
            return Err(ErrorCode::StaleGeneration);
        }
        if epoch < partition.last_metadata_epoch
            || self.node.authority().map(|a| a.epoch) != Some(epoch)
        {
            return Err(ErrorCode::StaleEpoch);
        }
        // Keep fenced grants until their bounded expiry so a delivered old token
        // receives STALE_GENERATION/EPOCH instead of becoming an unknown lease.
        // Retired grants consume the global bound, not current inflight slots.
        let group = partition.groups.get(&group_id);
        let cursor = group.map_or(0, |g| g.cursor);
        let inflight = leases
            .entries
            .values()
            .filter(|l| {
                matches!(&l.command,
            ShardCommand::GroupAck { group_id: g, generation: leased, metadata_epoch, .. }
                if *g == group_id && *leased == generation && *metadata_epoch == epoch)
            })
            .count();
        let capacity = max_messages
            .min(MAX_GROUP_INFLIGHT.saturating_sub(inflight))
            .min(MAX_LEASES.saturating_sub(leases.entries.len()));
        let mut frames = Vec::new();
        let mut bytes = 0;
        for message in &partition.messages {
            if message.logical_offset < cursor
                || group.is_some_and(|g| g.outcomes.contains_key(&message.logical_offset))
                || leases.entries.values().any(|l| {
                    matches!(&l.command,
                    ShardCommand::GroupAck { group_id: g, offset, generation: leased, metadata_epoch, .. }
                        if *g == group_id && *offset == message.logical_offset
                            && *leased == generation && *metadata_epoch == epoch)
                })
            {
                continue;
            }
            if frames.len() == capacity || bytes + message.payload.len() > max_bytes {
                break;
            }
            leases.sequence = leases
                .sequence
                .checked_add(1)
                .ok_or(ErrorCode::ResourceExhausted)?;
            let attempt =
                u32::try_from(leases.sequence).map_err(|_| ErrorCode::ResourceExhausted)?;
            let lease_id = ((leases.namespace as u128) << 64) | leases.sequence as u128;
            let command = ShardCommand::GroupAck {
                partition: self.options.partition,
                incarnation: self.options.incarnation,
                metadata_epoch: epoch,
                generation,
                group_id,
                offset: message.logical_offset,
                message_id: message.message_id,
                principal,
                lease_id,
                attempt,
            };
            let mut delivery = self.group_frame(
                FrameType::Message,
                frame.request_id,
                group_id,
                generation,
                message.logical_offset,
            );
            delivery.fields.extend([
                Field::required(f::MESSAGE_ID, message.message_id.to_be_bytes()),
                Field::required(f::LEASE_ID, lease_id.to_be_bytes()),
                Field::required(f::OWNERSHIP_EPOCH, epoch.to_be_bytes()),
                Field::required(f::DELIVERY_ATTEMPT, attempt.to_be_bytes()),
                Field::required(
                    f::ACK_DEADLINE_MS,
                    (LEASE_DURATION.as_millis() as u32).to_be_bytes(),
                ),
            ]);
            delivery.payload = message.payload.clone();
            bytes += delivery.payload.len();
            frames.push(delivery);
            leases.entries.insert(
                lease_id,
                Lease {
                    command,
                    expires: Instant::now() + LEASE_DURATION,
                    delivered: false,
                },
            );
        }
        if frames.is_empty()
            && partition.messages.iter().any(|m| {
                m.logical_offset >= cursor
                    && !group.is_some_and(|g| g.outcomes.contains_key(&m.logical_offset))
                    && m.payload.len() > max_bytes
            })
        {
            return Err(ErrorCode::ResourceExhausted);
        }
        let mut end = self.group_frame(
            FrameType::FetchEnd,
            frame.request_id,
            group_id,
            generation,
            cursor,
        );
        end.fields.push(Field::required(
            f::HAS_MORE,
            [u8::from(cursor < partition.next_logical_offset)],
        ));
        frames.push(end);
        Ok(frames)
    }

    pub(crate) async fn delivered_q2(&self, frame: &Frame) -> Result<(), ErrorCode> {
        let lease_id = number::<16>(frame, f::LEASE_ID)?;
        let mut leases = self.leases.lock().await;
        let lease = leases
            .entries
            .get_mut(&lease_id)
            .ok_or(ErrorCode::LeaseExpired)?;
        if lease.expires <= Instant::now() {
            return Err(ErrorCode::LeaseExpired);
        }
        lease.delivered = true;
        Ok(())
    }

    fn group_frame(
        &self,
        kind: FrameType,
        request_id: u64,
        group: u64,
        generation: u64,
        offset: u64,
    ) -> Frame {
        let mut frame = Frame::new(kind, request_id);
        frame.fields = vec![
            Field::required(f::TENANT_ID, self.options.partition.tenant_id.to_be_bytes()),
            Field::required(f::TOPIC_ID, self.options.partition.topic_id.to_be_bytes()),
            Field::required(f::QOS, [2]),
            Field::required(
                f::PARTITION_ID,
                self.options.partition.partition_id.to_be_bytes(),
            ),
            Field::required(f::LOG_INCAR, self.options.incarnation.to_be_bytes()),
            Field::required(f::GROUP_ID, group.to_be_bytes()),
            Field::required(f::GROUP_GENERATION, generation.to_be_bytes()),
            Field::required(f::OFFSET, offset.to_be_bytes()),
        ];
        frame
    }

    /// Exact committed retries are read-only even after restart/lease expiry.
    /// For every NEW mutation: delivered live lease -> epoch -> generation ->
    /// committed message identity -> bounded Raft proposal -> applied outcome.
    pub async fn ack_q2_group(&self, principal: u128, frame: Frame) -> Result<Frame, ErrorCode> {
        let group_id = self.group_scope(principal, &frame)?;
        let generation = number::<8>(&frame, f::GROUP_GENERATION)? as u64;
        let lease_id = number::<16>(&frame, f::LEASE_ID)?;
        let offset = number::<8>(&frame, f::OFFSET)? as u64;
        let epoch = number::<8>(&frame, f::OWNERSHIP_EPOCH)? as u64;
        let message_id = number::<16>(&frame, f::MESSAGE_ID)?;
        let command = ShardCommand::GroupAck {
            partition: self.options.partition,
            incarnation: self.options.incarnation,
            metadata_epoch: epoch,
            generation,
            group_id,
            offset,
            message_id,
            principal,
            lease_id,
            attempt: number::<4>(&frame, f::DELIVERY_ATTEMPT)? as u32,
        };
        let mut leases = self.leases.lock().await;
        let state = self.node.applied_state();
        let partition = self.group_partition(&state)?;
        let outcome = if let Some(saved) = partition
            .groups
            .get(&group_id)
            .and_then(|g| g.outcomes.get(&offset))
        {
            if saved.command != command {
                return Err(ErrorCode::SequenceConflict);
            }
            saved.outcome.clone()
        } else {
            let lease = leases
                .entries
                .get(&lease_id)
                .filter(|l| l.delivered && l.expires > Instant::now())
                .ok_or(ErrorCode::LeaseExpired)?;
            // Check the live delivery's scope first. Epoch/generation and record
            // identity then receive their distinct typed validation below.
            match (&lease.command, &command) {
                (
                    ShardCommand::GroupAck {
                        partition: p,
                        incarnation: i,
                        group_id: g,
                        principal: who,
                        lease_id: l,
                        attempt: a,
                        ..
                    },
                    ShardCommand::GroupAck {
                        partition: p2,
                        incarnation: i2,
                        group_id: g2,
                        principal: who2,
                        lease_id: l2,
                        attempt: a2,
                        ..
                    },
                ) if p == p2 && i == i2 && g == g2 && who == who2 && l == l2 && a == a2 => {}
                _ => return Err(ErrorCode::LeaseExpired),
            }
            if self.node.authority().map(|a| a.epoch) != Some(epoch)
                || epoch < partition.last_metadata_epoch
                || !matches!(&lease.command, ShardCommand::GroupAck { metadata_epoch, .. } if *metadata_epoch == epoch)
            {
                return Err(ErrorCode::StaleEpoch);
            }
            if generation == 0
                || generation != partition.group_generation
                || !matches!(&lease.command, ShardCommand::GroupAck { generation: leased_generation, .. } if *leased_generation == generation)
            {
                return Err(ErrorCode::StaleGeneration);
            }
            if lease.command != command
                || !partition
                    .messages
                    .iter()
                    .any(|m| m.logical_offset == offset && m.message_id == message_id)
            {
                return Err(ErrorCode::SequenceConflict);
            }
            rovqet_consensus::postmajority::hold_group_ack(
                self.node_id,
                0,
                "before_proposal",
                &command,
            )
            .await;
            match tokio::time::timeout(PROPOSAL_TIMEOUT, self.node.propose(command.clone())).await {
                Ok(Ok(result @ ShardCommandResponse::GroupAcked { .. })) => {
                    let index = self
                        .node
                        .applied_state()
                        .last_applied
                        .map(|l| l.index)
                        .unwrap_or(0);
                    rovqet_consensus::postmajority::hold_group_ack(
                        self.node_id,
                        index,
                        "after_majority",
                        &command,
                    )
                    .await;
                    result
                }
                // A concurrent external claim/epoch may reject after validation.
                // Never infer success from proposal submission or a timeout.
                Ok(Ok(ShardCommandResponse::Rejected { reason })) => {
                    return Err(if reason.contains("generation") {
                        ErrorCode::StaleGeneration
                    } else if reason.contains("epoch") {
                        ErrorCode::StaleEpoch
                    } else if reason.contains("resource") {
                        ErrorCode::ResourceExhausted
                    } else {
                        ErrorCode::SequenceConflict
                    });
                }
                _ => return Err(ErrorCode::OutcomeUnknown),
            }
        };
        leases.entries.remove(&lease_id);
        match outcome {
            ShardCommandResponse::GroupAcked {
                generation, offset, ..
            } => {
                let mut reply = self.group_frame(
                    FrameType::ConsumerAckResult,
                    frame.request_id,
                    group_id,
                    generation,
                    offset,
                );
                reply.fields.extend([
                    Field::required(f::ACK_OUTCOME, [1]),
                    Field::required(f::MESSAGE_ID, message_id.to_be_bytes()),
                ]);
                Ok(reply)
            }
            _ => Err(ErrorCode::OutcomeUnknown),
        }
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

    fn fetch_frame(generation: Option<u64>) -> Frame {
        let mut frame = Frame::new(FrameType::Fetch, 2);
        frame.fields = vec![
            Field::required(f::TOPIC_NAME, b"durable-q2"),
            Field::required(f::TENANT_ID, 1u64.to_be_bytes()),
            Field::required(f::TOPIC_ID, 3u64.to_be_bytes()),
            Field::required(f::QOS, [2]),
            Field::required(f::PARTITION_ID, 0u32.to_be_bytes()),
            Field::required(f::LOG_INCAR, 42u128.to_be_bytes()),
            Field::required(f::GROUP_ID, 7u64.to_be_bytes()),
            Field::required(f::OFFSET, 0u64.to_be_bytes()),
            Field::required(f::MAX_MESSAGES, 8u32.to_be_bytes()),
            Field::required(f::MAX_BYTES, 65536u32.to_be_bytes()),
            Field::required(f::WAIT_MS, 0u32.to_be_bytes()),
        ];
        if let Some(generation) = generation {
            replace(&mut frame, f::GROUP_GENERATION, generation.to_be_bytes());
        }
        frame
    }

    fn ack_frame(delivery: &Frame) -> Frame {
        let mut frame = delivery.clone();
        frame.frame_type = FrameType::ConsumerAck;
        frame.request_id = 3;
        frame.payload.clear();
        replace(&mut frame, f::TOPIC_NAME, b"durable-q2");
        frame
    }

    async fn seed_group(route: &Route) {
        wait_for_leader(route).await;
        for sequence in 0..2u64 {
            let mut frame = publish_frame(format!("event-{sequence}").into_bytes());
            replace(&mut frame, f::PRODUCER_SEQUENCE, sequence.to_be_bytes());
            replace(
                &mut frame,
                f::MESSAGE_ID,
                (100 + sequence as u128).to_be_bytes(),
            );
            publish_until_certain(route, frame).await;
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn q2_groups_require_delivery_validate_tokens_and_resolve_applied_acks() {
        let (route, directory) = open_route().await;
        seed_group(&route).await;
        let batch = route.fetch_q2_group(1, fetch_frame(None)).await.unwrap();
        assert_eq!(batch.len(), 3);
        assert_eq!(batch[0].payload, b"event-0");
        assert_eq!(batch[1].payload, b"event-1");
        assert_eq!(batch[2].frame_type, FrameType::FetchEnd);
        assert_eq!(number::<8>(&batch[2], f::OFFSET).unwrap(), 0);
        for message in &batch[..2] {
            let state = route.applied_state();
            let partition = route.group_partition(&state).unwrap();
            assert!(partition.messages.iter().any(|m| m.logical_offset as u128
                == number::<8>(message, f::OFFSET).unwrap()
                && m.message_id == number::<16>(message, f::MESSAGE_ID).unwrap()));
        }
        let ack = ack_frame(&batch[0]);
        // Constructing a batch is not evidence of socket delivery.
        assert_eq!(
            route.ack_q2_group(1, ack.clone()).await.unwrap_err(),
            ErrorCode::LeaseExpired
        );
        route.delivered_q2(&batch[0]).await.unwrap();
        route.delivered_q2(&batch[1]).await.unwrap();
        for (field, value, expected) in [
            (
                f::LEASE_ID,
                999u128.to_be_bytes().to_vec(),
                ErrorCode::LeaseExpired,
            ),
            (
                f::OWNERSHIP_EPOCH,
                4u64.to_be_bytes().to_vec(),
                ErrorCode::StaleEpoch,
            ),
            (
                f::GROUP_GENERATION,
                0u64.to_be_bytes().to_vec(),
                ErrorCode::StaleGeneration,
            ),
            (
                f::OFFSET,
                999u64.to_be_bytes().to_vec(),
                ErrorCode::SequenceConflict,
            ),
            (
                f::MESSAGE_ID,
                999u128.to_be_bytes().to_vec(),
                ErrorCode::SequenceConflict,
            ),
            (
                f::TOPIC_ID,
                99u64.to_be_bytes().to_vec(),
                ErrorCode::PermissionDenied,
            ),
        ] {
            let mut invalid = ack.clone();
            replace(&mut invalid, field, value);
            let before = route.applied_state();
            assert_eq!(route.ack_q2_group(1, invalid).await.unwrap_err(), expected);
            assert_eq!(route.applied_state().revision, before.revision);
            assert_eq!(route.applied_state().last_applied, before.last_applied);
        }
        let reply = route.ack_q2_group(1, ack.clone()).await.unwrap();
        assert_eq!(reply.field(f::ACK_OUTCOME), Some(&[1][..]));
        let before = route.applied_state();
        let group = &route.group_partition(&before).unwrap().groups[&7];
        assert_eq!(group.cursor, 1, "ACK_COMMITTED follows actual application");
        assert_eq!(group.outcomes.len(), 1);
        let retry = route.ack_q2_group(1, ack.clone()).await.unwrap();
        assert_eq!(retry, reply);
        assert_eq!(
            route.applied_state().last_applied,
            before.last_applied,
            "no second proposal"
        );
        assert_eq!(route.applied_state().revision, before.revision);
        let mut conflict = ack;
        replace(&mut conflict, f::MESSAGE_ID, 999u128.to_be_bytes());
        assert_eq!(
            route.ack_q2_group(1, conflict).await.unwrap_err(),
            ErrorCode::SequenceConflict
        );
        assert_eq!(route.claim_q2_group(2).await.unwrap(), 2);
        assert_eq!(
            route
                .ack_q2_group(1, ack_frame(&batch[1]))
                .await
                .unwrap_err(),
            ErrorCode::StaleGeneration
        );
        assert_eq!(
            route.claim_q2_group(2).await.unwrap_err(),
            ErrorCode::StaleGeneration
        );
        route.shutdown().await.unwrap();
        drop(directory);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn q2_route_restart_redelivers_unacknowledged_records_but_preserves_saved_success() {
        let (route, directory) = open_route().await;
        seed_group(&route).await;
        let batch = route.fetch_q2_group(1, fetch_frame(None)).await.unwrap();
        for message in &batch[..2] {
            route.delivered_q2(message).await.unwrap();
        }
        let ack = ack_frame(&batch[0]);
        let saved = route.ack_q2_group(1, ack.clone()).await.unwrap();
        let options = route.options.clone();
        route.shutdown().await.unwrap();
        let restarted = Route::open(options, 1, 7).await.unwrap();
        restarted.install_authority(Some(ShardAuthority { owner: 1, epoch: 5 }));
        wait_for_leader(&restarted).await;
        let before = restarted.applied_state();
        assert_eq!(restarted.ack_q2_group(1, ack).await.unwrap(), saved);
        assert_eq!(restarted.applied_state().revision, before.revision);
        assert_eq!(restarted.applied_state().last_applied, before.last_applied);
        assert_eq!(
            restarted
                .ack_q2_group(1, ack_frame(&batch[1]))
                .await
                .unwrap_err(),
            ErrorCode::LeaseExpired
        );
        let redelivery = restarted
            .fetch_q2_group(1, fetch_frame(Some(1)))
            .await
            .unwrap();
        assert_eq!(redelivery.len(), 2);
        assert_eq!(redelivery[0].payload, b"event-1");
        assert_ne!(
            redelivery[0].field(f::LEASE_ID),
            batch[1].field(f::LEASE_ID)
        );
        assert_eq!(redelivery[1].frame_type, FrameType::FetchEnd);
        assert_eq!(number::<8>(&redelivery[1], f::OFFSET).unwrap(), 1);
        restarted.shutdown().await.unwrap();
        drop(directory);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn q2_leases_are_bounded_expire_and_do_not_complete_offsets() {
        let (route, directory) = open_route().await;
        seed_group(&route).await;
        let mut excessive = fetch_frame(None);
        replace(
            &mut excessive,
            f::MAX_MESSAGES,
            (MAX_GROUP_BATCH as u32 + 1).to_be_bytes(),
        );
        let before = route.applied_state();
        assert_eq!(
            route.fetch_q2_group(1, excessive).await.unwrap_err(),
            ErrorCode::ResourceExhausted
        );
        assert_eq!(route.applied_state().revision, before.revision);
        assert!(route.leases.lock().await.entries.is_empty());
        let mut single = fetch_frame(None);
        replace(&mut single, f::MAX_MESSAGES, 1u32.to_be_bytes());
        let batch = route.fetch_q2_group(1, single).await.unwrap();
        assert_eq!(batch.len(), 2);
        route.delivered_q2(&batch[0]).await.unwrap();
        let lease_id = number::<16>(&batch[0], f::LEASE_ID).unwrap();
        route
            .leases
            .lock()
            .await
            .entries
            .get_mut(&lease_id)
            .unwrap()
            .expires = Instant::now() - Duration::from_secs(1);
        assert_eq!(
            route
                .ack_q2_group(1, ack_frame(&batch[0]))
                .await
                .unwrap_err(),
            ErrorCode::LeaseExpired
        );
        let redelivery = route.fetch_q2_group(1, fetch_frame(Some(1))).await.unwrap();
        assert_eq!(
            redelivery[0].field(f::OFFSET),
            Some(0u64.to_be_bytes().as_slice())
        );
        assert_ne!(
            redelivery[0].field(f::LEASE_ID),
            batch[0].field(f::LEASE_ID)
        );
        assert_eq!(
            number::<8>(redelivery.last().unwrap(), f::OFFSET).unwrap(),
            0
        );
        route.install_authority(Some(ShardAuthority { owner: 1, epoch: 6 }));
        route.delivered_q2(&redelivery[0]).await.unwrap();
        assert_eq!(
            route
                .ack_q2_group(1, ack_frame(&redelivery[0]))
                .await
                .unwrap_err(),
            ErrorCode::StaleEpoch
        );
        route.shutdown().await.unwrap();
        drop(directory);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_replacement_fetch_preserves_the_old_tokens_typed_generation_fence() {
        let (route, directory) = open_route().await;
        seed_group(&route).await;
        let old = route.fetch_q2_group(1, fetch_frame(None)).await.unwrap();
        route.delivered_q2(&old[0]).await.unwrap();
        let replacement = route.fetch_q2_group(1, fetch_frame(None)).await.unwrap();
        assert_eq!(replacement[0].payload, b"event-0");
        assert_eq!(
            route.ack_q2_group(1, ack_frame(&old[0])).await.unwrap_err(),
            ErrorCode::StaleGeneration
        );
        route.delivered_q2(&replacement[0]).await.unwrap();
        assert_eq!(
            route
                .ack_q2_group(1, ack_frame(&replacement[0]))
                .await
                .unwrap()
                .field(f::ACK_OUTCOME),
            Some(&[1][..])
        );
        route.shutdown().await.unwrap();
        drop(directory);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn q2_ack_without_a_majority_is_uncertain_not_committed() {
        let directories: Vec<_> = (0..3).map(|_| tempfile::tempdir().unwrap()).collect();
        let peers: Vec<(u64, SocketAddr)> = (1..=3)
            .map(|id| (id, format!("127.0.0.1:{}", free_port()).parse().unwrap()))
            .collect();
        let mut routes = Vec::new();
        for index in 0..3 {
            let options = Options {
                data_directory: directories[index].path().to_path_buf(),
                topic: "durable-q2".into(),
                partition: PartitionRef {
                    tenant_id: 1,
                    topic_id: 3,
                    partition_id: 0,
                },
                incarnation: 42,
                shard_id: 9,
                listen: peers[index].1,
                peers: peers.clone(),
                bootstrap: index == 0,
            };
            let route = Route::open(options, index as u64 + 1, 7).await.unwrap();
            route.install_authority(Some(ShardAuthority { owner: 1, epoch: 5 }));
            routes.push(Some(route));
        }
        wait_for_leader(routes[0].as_ref().unwrap()).await;
        let leader_id = routes[0].as_ref().unwrap().leader().await.unwrap();
        let leader = leader_id as usize - 1;
        let route = routes[leader].as_ref().unwrap();
        seed_group(route).await;
        let batch = route.fetch_q2_group(1, fetch_frame(None)).await.unwrap();
        route.delivered_q2(&batch[0]).await.unwrap();
        route.delivered_q2(&batch[1]).await.unwrap();
        route.ack_q2_group(1, ack_frame(&batch[0])).await.unwrap();
        for (index, slot) in routes.iter_mut().enumerate() {
            if index != leader {
                slot.take().unwrap().shutdown().await.unwrap();
            }
        }
        let route = routes[leader].as_ref().unwrap();
        let before = route.applied_state();
        let result = route.ack_q2_group(1, ack_frame(&batch[1])).await;
        assert_eq!(result.unwrap_err(), ErrorCode::OutcomeUnknown);
        let after = route.applied_state();
        assert_eq!(after.revision, before.revision);
        assert!(
            !route.group_partition(&after).unwrap().groups[&7]
                .outcomes
                .contains_key(&1)
        );
        // The earlier committed ACK remains a read-only lookup without quorum.
        assert_eq!(
            route
                .ack_q2_group(1, ack_frame(&batch[0]))
                .await
                .unwrap()
                .field(f::ACK_OUTCOME),
            Some(&[1][..])
        );
        routes[leader].take().unwrap().shutdown().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn q2_group_frames_dispatch_over_tcp_through_the_public_sdk() {
        use rovqet_sdk::{AckOutcome, Client, ClientConfig, GroupError, GroupOptions};
        use std::sync::Arc;
        let (route, directory) = open_route().await;
        seed_group(&route).await;
        let route = Arc::new(route);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = listener.local_addr().unwrap();
        let handler = crate::q1::Dispatch::new(
            crate::development_broker(1024),
            crate::q1::RouteSlot::new(None),
        )
        .with_q2(Some(route.clone()));
        let (stop, closed) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(rovqet_transport::serve(
            listener,
            Default::default(),
            Arc::new(handler),
            async {
                let _ = closed.await;
            },
        ));
        let mut client = Client::connect(ClientConfig {
            endpoint,
            ..Default::default()
        })
        .await
        .unwrap();
        let options = |generation| GroupOptions {
            partition: route.partition(),
            incarnation: route.incarnation(),
            group_id: 7,
            generation,
            max_messages: 8,
            max_bytes: 65536,
            wait: Duration::ZERO,
        };
        let batch = client
            .fetch_q2_group(route.topic(), options(None))
            .await
            .unwrap();
        assert_eq!(batch.messages.len(), 2);
        assert_eq!(batch.messages[0].message_id, 100);
        assert_eq!(batch.messages[1].payload, b"event-1");
        assert_eq!(batch.next.sequence, 0);
        assert!(batch.has_more);
        let committed = client
            .ack_q2_group(route.topic(), &batch.messages[1])
            .await
            .unwrap();
        assert!(matches!(committed, AckOutcome::Committed { .. }));
        let before = route.applied_state();
        assert_eq!(route.group_partition(&before).unwrap().groups[&7].cursor, 0);
        assert_eq!(
            client
                .ack_q2_group(route.topic(), &batch.messages[1])
                .await
                .unwrap(),
            committed
        );
        assert_eq!(route.applied_state().last_applied, before.last_applied);
        client
            .ack_q2_group(route.topic(), &batch.messages[0])
            .await
            .unwrap();
        let end = client
            .fetch_q2_group(route.topic(), options(Some(batch.generation)))
            .await
            .unwrap();
        assert!(end.messages.is_empty());
        assert_eq!(end.next.sequence, 2);
        assert!(end.end_of_current_history);
        // An explicit new claim fences all earlier new mutations.
        let next = client
            .fetch_q2_group(route.topic(), options(None))
            .await
            .unwrap();
        assert_eq!(next.generation, 2);
        assert!(matches!(
            client.fetch_q2_group(route.topic(), options(Some(1))).await,
            Err(GroupError::StaleGeneration)
        ));
        // A rejected fetch closes the SDK's framing-uncertain batch stream.
        assert!(matches!(
            client.close().await,
            Err(rovqet_sdk::ClientError::Closed)
        ));
        let _ = stop.send(());
        task.await.unwrap().unwrap();
        Arc::try_unwrap(route)
            .ok()
            .expect("sessions released route")
            .shutdown()
            .await
            .unwrap();
        drop(directory);
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
        // The committed payload bytes are retained next to their digest in the
        // applied state, so the record survives Raft log compaction and snapshot
        // recovery instead of only its digest.
        {
            let applied = route.applied_state();
            let partition = applied
                .partitions
                .values()
                .next()
                .expect("the committed partition");
            assert_eq!(partition.messages[0].payload, b"event");
            assert_eq!(partition.retained_payload_bytes, 5);
        }
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
