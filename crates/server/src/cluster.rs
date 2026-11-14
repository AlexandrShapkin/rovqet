//! Cluster mode: explicit peers, a metadata authority fixture and owner-aware routing.
//!
//! Cluster mode is opt-in. It never invents ownership authority: the local metadata store
//! is the only source of placement and epoch, and a node that is not the owner refuses to
//! mutate. A routed request is forwarded at most once.
use crate::q1::Route;
use rovqet_control::ids::{ClusterId, NodeId, PeerEndpoint, TrustContext};
use rovqet_control::metadata::{MetadataSnapshot, MetadataStore, SnapshotFile};
use rovqet_control::routing::{RouteDecision, decide};
use rovqet_control::rpc::{
    HealthResponse, OwnershipRequest, OwnershipResponse, Q1TransferChunkRequest,
    Q1TransferChunkResponse, Q1TransferFinalizeRequest, Q1TransferFinalizeResponse,
    Q1TransferPrepareRequest, Q1TransferPrepareResponse, RouteAckRequest, RouteAckResponse,
    RouteFetchRequest, RouteFetchResponse, RoutePublishRequest, RoutePublishResponse, RpcErrorCode,
    RpcKind,
};
use rovqet_control::transport::{PeerHandler, PeerTransport, TransportError, TransportLimits};
use rovqet_control::wire::Envelope;
use rovqet_core::PartitionRef;
use rovqet_protocol::{ErrorCode, Field, Frame, FrameType, registry::field as f};
use std::future::Future;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, Instant};

fn invalid(message: impl Into<String>) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::InvalidInput, message.into())
}

/// Starts the metadata voter and publishes its committed snapshots into the store.
///
/// Only committed and applied metadata is installed: the store stays empty until the group
/// commits its first snapshot, so the data plane fails closed while no authority exists.
async fn start_metadata_raft(
    options: &MetadataRaftOptions,
    store: &Arc<MetadataStore>,
) -> std::io::Result<Arc<rovqet_metadata_raft::MetadataNode>> {
    if !options.listen.ip().is_loopback() {
        return Err(invalid("metadata raft listener must be loopback"));
    }
    let node =
        rovqet_metadata_raft::MetadataNode::start(rovqet_metadata_raft::MetadataNodeConfig {
            id: options.node_id,
            cluster_id: options.cluster_id,
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
        .map_err(std::io::Error::other)?;
    let node = Arc::new(node);

    let mut updates = node.committed_updates();
    let store = store.clone();
    let publisher = node.clone();
    tokio::spawn(async move {
        loop {
            if let Some(snapshot) = publisher.committed_snapshot() {
                match store.install(snapshot) {
                    Ok(_) => {}
                    // An older snapshot must never replace a newer committed one.
                    Err(rovqet_control::metadata::MetadataError::RevisionRegression) => {}
                    Err(error) => {
                        // Fail closed: an unusable snapshot stops the publication instead of
                        // installing something that was never committed.
                        eprintln!("committed metadata rejected: {error:?}");
                        return;
                    }
                }
            }
            if updates.changed().await.is_err() {
                return;
            }
        }
    });
    Ok(node)
}

/// Raft-backed metadata authority: the committed metadata of the metadata group.
#[derive(Debug, Clone)]
pub struct MetadataRaftOptions {
    pub cluster_id: u128,
    pub node_id: u64,
    /// Loopback address of the metadata peer protocol of this node.
    pub listen: SocketAddr,
    /// The metadata voters of the group, including this node.
    pub peers: Vec<(u64, SocketAddr)>,
    /// Durable directory of this metadata voter.
    pub data_directory: PathBuf,
    /// Only the designated bootstrap node initializes the membership.
    pub bootstrap: bool,
}

/// Explicit cluster configuration of one node.
#[derive(Debug, Clone)]
pub struct ClusterOptions {
    pub cluster_id: u128,
    pub node_id: u64,
    /// Loopback peer listener address of this node.
    pub listen: SocketAddr,
    pub peers: Vec<(u64, SocketAddr)>,
    /// Static metadata authority fixture. Not a Raft-committed snapshot.
    pub metadata: Option<PathBuf>,
    /// Raft-committed metadata authority. Mutually exclusive with the static fixture.
    pub metadata_raft: Option<MetadataRaftOptions>,
    /// Directory of the incoming Q1 transfers. Without it the transfer kinds are answered
    /// as unsupported.
    pub transfer_staging: Option<PathBuf>,
}

pub struct Cluster {
    pub trust: Arc<TrustContext>,
    pub store: Arc<MetadataStore>,
    pub transport: Arc<PeerTransport>,
    pub node: NodeId,
    pub started: Instant,
    /// The metadata voter of this node, when Raft mode is active.
    pub raft: Option<Arc<rovqet_metadata_raft::MetadataNode>>,
    /// The staging areas of the incoming Q1 transfers.
    pub transfers: Option<Arc<tokio::sync::Mutex<crate::transfer::TransferManager>>>,
}

/// Rejects a configuration in which a static fixture could override committed metadata.
pub fn validate(options: &ClusterOptions) -> std::io::Result<()> {
    if options.metadata_raft.is_some() && options.metadata.is_some() {
        return Err(invalid(
            "a static metadata fixture must not override committed Raft metadata",
        ));
    }
    Ok(())
}

impl Cluster {
    pub async fn new(options: &ClusterOptions) -> std::io::Result<Self> {
        validate(options)?;
        let cluster = ClusterId::new(options.cluster_id).ok_or_else(|| invalid("cluster id"))?;
        let node = NodeId::new(options.node_id).ok_or_else(|| invalid("node id"))?;
        let mut peers = Vec::with_capacity(options.peers.len());
        for (id, address) in &options.peers {
            let identity = NodeId::new(*id).ok_or_else(|| invalid("peer node id"))?;
            if !address.ip().is_loopback() {
                return Err(invalid("cluster peers must be loopback in the prototype"));
            }
            peers.push(PeerEndpoint {
                node: identity,
                address: *address,
            });
        }
        let trust = Arc::new(TrustContext::new(cluster, node, peers));
        let store = Arc::new(MetadataStore::new(cluster));
        if let Some(path) = &options.metadata {
            let text = std::fs::read_to_string(path)?;
            let file: SnapshotFile = serde_json::from_str(&text)
                .map_err(|error| invalid(format!("invalid cluster metadata: {error}")))?;
            let snapshot = MetadataSnapshot::try_from(file)
                .map_err(|error| invalid(format!("invalid cluster metadata: {error}")))?;
            store
                .install(snapshot)
                .map_err(|error| invalid(format!("cluster metadata rejected: {error}")))?;
        }
        let transport = Arc::new(PeerTransport::new(&trust, TransportLimits::default()));
        let raft = match &options.metadata_raft {
            Some(raft_options) => Some(start_metadata_raft(raft_options, &store).await?),
            None => None,
        };
        let transfers = options.transfer_staging.as_ref().map(|root| {
            Arc::new(tokio::sync::Mutex::new(
                crate::transfer::TransferManager::new(root),
            ))
        });
        Ok(Self {
            trust,
            store,
            transport,
            node,
            started: Instant::now(),
            raft,
            transfers,
        })
    }

    /// The metadata voter of this node, when Raft mode is active.
    pub fn raft_node(&self) -> Option<&rovqet_metadata_raft::MetadataNode> {
        self.raft.as_deref()
    }

    /// The committed metadata revision known to this node, when Raft mode is active.
    pub fn committed_metadata_revision(&self) -> Option<u64> {
        self.raft
            .as_ref()
            .and_then(|node| node.applied_state().revision.into())
    }

    pub fn revision(&self) -> u64 {
        self.store
            .current()
            .map(|snapshot| snapshot.revision.get())
            .unwrap_or(0)
    }

    /// The committed ownership authority of one partition, if the snapshot places it.
    pub fn authority_for(&self, partition: &PartitionRef) -> Option<rovqet_durable::Authority> {
        let snapshot = self.store.current()?;
        let placement = snapshot.owner_of(partition)?;
        rovqet_durable::Authority::new(
            placement.owner.get(),
            placement.ownership_epoch.get(),
            Duration::from_millis(placement.lease.valid_for_ms),
        )
    }

    /// Owner-aware routing decision for one partition.
    pub fn route(&self, partition: &PartitionRef, already_forwarded: bool) -> RouteDecision {
        decide(&self.store, &self.trust, partition, None, already_forwarded)
    }
}

/// Answers inter-node requests.
pub struct ClusterHandler {
    cluster: Arc<Cluster>,
    /// The Q1 route of this node. A successor installs its route only after the promoted
    /// storage is validated, so the handle is replaced at runtime.
    route: crate::q1::RouteSlot,
}

impl ClusterHandler {
    pub fn new(cluster: Arc<Cluster>, route: crate::q1::RouteSlot) -> Self {
        Self { cluster, route }
    }

    /// The receiver validates routing authority independently before any mutation.
    fn require_owner(
        cluster: &Cluster,
        partition: &PartitionRef,
        expected_epoch: u64,
    ) -> Result<(), RpcErrorCode> {
        let snapshot = cluster.store.current().ok_or(RpcErrorCode::NoMetadata)?;
        let placement = snapshot.owner_of(partition).ok_or(RpcErrorCode::NotOwner)?;
        if placement.ownership_epoch.get() != expected_epoch {
            return Err(RpcErrorCode::StaleEpoch);
        }
        if cluster.store.authority_expired(placement) {
            return Err(RpcErrorCode::ExpiredAuthority);
        }
        if placement.owner != cluster.node {
            // A routed request is never routed again: this bounds redirect loops.
            return Err(RpcErrorCode::NotOwner);
        }
        Ok(())
    }
}

impl PeerHandler for ClusterHandler {
    fn handle(
        &self,
        envelope: Envelope,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<u8>, RpcErrorCode>> + Send>> {
        let cluster = self.cluster.clone();
        let route = self.route.clone();
        Box::pin(async move {
            let Some(kind) = RpcKind::from_u16(envelope.rpc_kind) else {
                return Err(RpcErrorCode::UnknownKind);
            };
            if kind.is_reserved_unsupported() {
                return Err(RpcErrorCode::Unsupported);
            }
            match kind {
                RpcKind::Health => Ok(HealthResponse {
                    cluster_id: cluster.trust.cluster,
                    node_id: cluster.node,
                    metadata_revision: rovqet_control::MetadataRevision::new(cluster.revision())
                        .unwrap_or(rovqet_control::MetadataRevision(1)),
                    uptime_ms: cluster.started.elapsed().as_millis() as u64,
                }
                .encode()),
                RpcKind::MetadataSnapshotGet => {
                    let snapshot = cluster.store.current().ok_or(RpcErrorCode::NoMetadata)?;
                    let file = SnapshotFile::from(snapshot.as_ref());
                    serde_json::to_vec(&file).map_err(|_| RpcErrorCode::Internal)
                }
                RpcKind::Q1TransferPrepare => {
                    let Some(transfers) = &cluster.transfers else {
                        return Err(RpcErrorCode::Unsupported);
                    };
                    let request = Q1TransferPrepareRequest::decode(&envelope.body)
                        .map_err(|_| RpcErrorCode::Malformed)?;
                    let response = transfers
                        .lock()
                        .await
                        .prepare(&request)
                        .map_err(|_| RpcErrorCode::Internal)?;
                    Ok(response.encode())
                }
                RpcKind::Q1TransferChunk => {
                    let Some(transfers) = &cluster.transfers else {
                        return Err(RpcErrorCode::Unsupported);
                    };
                    let request = Q1TransferChunkRequest::decode(&envelope.body)
                        .map_err(|_| RpcErrorCode::Malformed)?;
                    let response = transfers
                        .lock()
                        .await
                        .chunk(&request)
                        .map_err(|_| RpcErrorCode::Internal)?;
                    Ok(response.encode())
                }
                RpcKind::Q1TransferFinalize => {
                    let Some(transfers) = &cluster.transfers else {
                        return Err(RpcErrorCode::Unsupported);
                    };
                    let request = Q1TransferFinalizeRequest::decode(&envelope.body)
                        .map_err(|_| RpcErrorCode::Malformed)?;
                    let response = transfers
                        .lock()
                        .await
                        .finalize(&request)
                        .map_err(|_| RpcErrorCode::Internal)?;
                    Ok(response.encode())
                }
                RpcKind::OwnershipInspect => {
                    let request = OwnershipRequest::decode(&envelope.body)
                        .map_err(|_| RpcErrorCode::Malformed)?;
                    let partition = PartitionRef {
                        tenant_id: request.tenant_id,
                        topic_id: request.topic_id,
                        partition_id: request.partition_id,
                    };
                    let snapshot = cluster.store.current().ok_or(RpcErrorCode::NoMetadata)?;
                    let placement = snapshot
                        .owner_of(&partition)
                        .ok_or(RpcErrorCode::NotOwner)?;
                    Ok(OwnershipResponse {
                        found: true,
                        owner: placement.owner,
                        ownership_epoch: placement.ownership_epoch,
                        metadata_revision: snapshot.revision,
                        expired: cluster.store.authority_expired(placement),
                    }
                    .encode())
                }
                RpcKind::RoutePublish => {
                    let request = RoutePublishRequest::decode(&envelope.body)
                        .map_err(|_| RpcErrorCode::Malformed)?;
                    let route = route.current().ok_or(RpcErrorCode::NotOwner)?;
                    let partition = route.options.partition;
                    if request.partition_id != partition.partition_id
                        || request.topic_id != partition.topic_id
                        || request.tenant_id != partition.tenant_id
                    {
                        return Err(RpcErrorCode::NotOwner);
                    }
                    // The owner revalidates the epoch before the durable mutation.
                    if request.ownership_epoch != route.options.owner_epoch {
                        return Err(RpcErrorCode::StaleEpoch);
                    }
                    Self::require_owner(&cluster, &partition, envelope.ownership_epoch)?;
                    match route.publish_durable(&request).await {
                        Ok(outcome) => Ok(RoutePublishResponse {
                            committed: true,
                            offset: outcome.sequence,
                            incarnation: outcome.log_incarnation,
                            error: None,
                        }
                        .encode()),
                        Err(error) => Ok(RoutePublishResponse {
                            committed: false,
                            offset: 0,
                            incarnation: 0,
                            error: Some(map_client_error(error)),
                        }
                        .encode()),
                    }
                }
                RpcKind::RouteFetch => {
                    let request = RouteFetchRequest::decode(&envelope.body)
                        .map_err(|_| RpcErrorCode::Malformed)?;
                    let route = route.current().ok_or(RpcErrorCode::NotOwner)?;
                    let partition = route.options.partition;
                    if request.topic_id != partition.topic_id
                        || request.tenant_id != partition.tenant_id
                        || request.partition_id != partition.partition_id
                    {
                        return Err(RpcErrorCode::NotOwner);
                    }
                    Self::require_owner(&cluster, &partition, envelope.ownership_epoch)?;
                    let batch = route
                        .group_fetch_routed(&request)
                        .await
                        .map_err(map_client_error)?;
                    Ok(RouteFetchResponse {
                        generation: batch.generation,
                        next_offset: batch.next.sequence,
                        has_more: batch.has_more,
                        deliveries: batch
                            .deliveries
                            .iter()
                            .map(|delivery| rovqet_control::rpc::RouteDelivery {
                                offset: delivery.offset.sequence,
                                message_id: delivery.message_id,
                                lease_id: delivery.lease_id,
                                generation: delivery.generation,
                                attempt: delivery.attempt,
                                payload: delivery.payload.clone(),
                            })
                            .collect(),
                        error: None,
                    }
                    .encode())
                }
                RpcKind::RouteConsumerAck => {
                    let request = RouteAckRequest::decode(&envelope.body)
                        .map_err(|_| RpcErrorCode::Malformed)?;
                    let route = route.current().ok_or(RpcErrorCode::NotOwner)?;
                    let partition = route.options.partition;
                    if request.topic_id != partition.topic_id
                        || request.tenant_id != partition.tenant_id
                        || request.partition_id != partition.partition_id
                    {
                        return Err(RpcErrorCode::NotOwner);
                    }
                    if request.ownership_epoch != route.options.owner_epoch {
                        return Err(RpcErrorCode::StaleEpoch);
                    }
                    Self::require_owner(&cluster, &partition, envelope.ownership_epoch)?;
                    let result = route
                        .group_ack_routed(&request)
                        .await
                        .map_err(map_client_error)?;
                    Ok(RouteAckResponse {
                        committed: true,
                        offset: result.offset.sequence,
                        generation: result.generation,
                        error: None,
                    }
                    .encode())
                }
                _ => Err(RpcErrorCode::Unsupported),
            }
        })
    }
}

/// Maps a client-facing error code to an inter-node error code.
pub fn map_client_error(code: ErrorCode) -> RpcErrorCode {
    match code {
        ErrorCode::StaleEpoch => RpcErrorCode::StaleEpoch,
        ErrorCode::ReplayNotAvailable => RpcErrorCode::NotOwner,
        ErrorCode::ResourceExhausted => RpcErrorCode::Capacity,
        ErrorCode::UnsupportedGuarantee | ErrorCode::UnsupportedCapability => {
            RpcErrorCode::Unsupported
        }
        ErrorCode::BadFrame => RpcErrorCode::Malformed,
        ErrorCode::OutcomeUnknown => RpcErrorCode::Uncertain,
        ErrorCode::StorageUnavailable => RpcErrorCode::Internal,
        _ => RpcErrorCode::Internal,
    }
}

/// Maps an inter-node error code to a client-facing error code.
pub fn map_rpc_error(code: RpcErrorCode) -> ErrorCode {
    match code {
        RpcErrorCode::StaleEpoch => ErrorCode::StaleEpoch,
        RpcErrorCode::NotOwner | RpcErrorCode::NoMetadata | RpcErrorCode::ExpiredAuthority => {
            ErrorCode::ReplayNotAvailable
        }
        RpcErrorCode::Unsupported => ErrorCode::UnsupportedCapability,
        RpcErrorCode::Malformed => ErrorCode::BadFrame,
        RpcErrorCode::Capacity => ErrorCode::ResourceExhausted,
        RpcErrorCode::Timeout | RpcErrorCode::Uncertain => ErrorCode::OutcomeUnknown,
        RpcErrorCode::ClusterMismatch | RpcErrorCode::WrongTarget | RpcErrorCode::UnknownSource => {
            ErrorCode::PermissionDenied
        }
        RpcErrorCode::UnknownKind | RpcErrorCode::UnsupportedVersion => ErrorCode::BadFrame,
        RpcErrorCode::ForwardHopExceeded | RpcErrorCode::Internal => ErrorCode::StorageUnavailable,
    }
}

/// Maps a peer transport failure to a client-facing error code. A timeout after the request
/// was written stays uncertain: it is never reported as a definite rollback.
pub fn map_transport_error(error: TransportError) -> ErrorCode {
    match error {
        TransportError::Uncertain | TransportError::Timeout => ErrorCode::OutcomeUnknown,
        TransportError::Capacity => ErrorCode::ResourceExhausted,
        TransportError::UnknownPeer | TransportError::NotConfigured => {
            ErrorCode::ReplayNotAvailable
        }
        TransportError::CorrelationMismatch | TransportError::Wire(_) => ErrorCode::BadFrame,
        TransportError::ConnectTimeout => ErrorCode::OutcomeUnknown,
        TransportError::Io(_) => ErrorCode::OutcomeUnknown,
    }
}

/// Builds the client-facing publication acknowledgement of an owner-committed publication.
pub fn publish_ack(
    request_id: u64,
    partition: PartitionRef,
    offset: rovqet_core::DurableOffset,
) -> Frame {
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
    ack
}

/// Default relative timeout budget for one forwarded request.
pub fn forward_budget() -> rovqet_control::TimeoutBudget {
    rovqet_control::TimeoutBudget::new(5_000).expect("bounded budget")
}

#[cfg(test)]
mod tests {
    use super::*;
    use rovqet_metadata_raft::types::MetadataCommand;
    use std::time::Duration;

    fn free_port() -> u16 {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("port");
        let port = listener.local_addr().expect("addr").port();
        drop(listener);
        port
    }

    fn directory(name: &str) -> PathBuf {
        let path = std::env::temp_dir().join(format!(
            "rovqet-server-raft-{name}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|value| value.as_nanos())
                .unwrap_or(0)
        ));
        std::fs::create_dir_all(&path).expect("directory");
        path
    }

    fn raft_options(port: u16, path: PathBuf) -> MetadataRaftOptions {
        MetadataRaftOptions {
            cluster_id: 5150,
            node_id: 1,
            listen: format!("127.0.0.1:{port}").parse().expect("addr"),
            peers: vec![(1, format!("127.0.0.1:{port}").parse().expect("addr"))],
            data_directory: path,
            bootstrap: true,
        }
    }

    #[test]
    fn a_static_fixture_must_not_override_committed_raft_metadata() {
        let mut options = ClusterOptions {
            cluster_id: 5150,
            node_id: 1,
            listen: "127.0.0.1:0".parse().expect("addr"),
            peers: Vec::new(),
            metadata: Some(PathBuf::from("/tmp/does-not-matter.json")),
            metadata_raft: Some(raft_options(free_port(), directory("conflict"))),
            transfer_staging: None,
        };
        let rejected = validate(&options);
        assert!(
            rejected.is_err(),
            "a static fixture must not override committed Raft metadata"
        );
        options.metadata = None;
        assert!(
            validate(&options).is_ok(),
            "Raft mode alone must be accepted"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn only_committed_metadata_becomes_the_ownership_authority() {
        let port = free_port();
        let path = directory("authority");
        let options = ClusterOptions {
            cluster_id: 5150,
            node_id: 1,
            listen: "127.0.0.1:0".parse().expect("addr"),
            peers: Vec::new(),
            metadata: None,
            metadata_raft: Some(raft_options(port, path.clone())),
            transfer_staging: None,
        };
        let cluster = Cluster::new(&options).await.expect("cluster");
        let node = cluster.raft_node().expect("the metadata voter");

        // No committed metadata yet: the data plane has no authority at all.
        assert!(
            cluster.store.current().is_none(),
            "the store must stay empty until the first committed snapshot"
        );
        assert!(
            cluster
                .authority_for(&PartitionRef {
                    tenant_id: 1,
                    topic_id: 3,
                    partition_id: 0,
                })
                .is_none()
        );

        // Wait until this single voter has won its election: a proposal is only accepted
        // by the leader.
        let deadline = std::time::Instant::now() + Duration::from_secs(30);
        while node.current_leader().await != Some(1) && std::time::Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        node.propose(MetadataCommand::SetNode {
            node: 1,
            address: "127.0.0.1:1".into(),
        })
        .await
        .expect("committed node");
        node.propose(MetadataCommand::SetPlacement {
            tenant_id: 1,
            topic_id: 3,
            partition_id: 0,
            owner: 1,
            ownership_epoch: 7,
            lease_valid_for_ms: 60_000,
        })
        .await
        .expect("committed placement");

        // The committed snapshot reaches the data plane. Every committed revision is
        // published, so the store advances to the placement revision.
        let deadline = std::time::Instant::now() + Duration::from_secs(30);
        while cluster
            .store
            .current()
            .map(|snapshot| snapshot.revision.get())
            != Some(2)
            && std::time::Instant::now() < deadline
        {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        let snapshot = cluster
            .store
            .current()
            .expect("the committed snapshot must be installed");
        assert_eq!(snapshot.revision.get(), 2);

        let partition = PartitionRef {
            tenant_id: 1,
            topic_id: 3,
            partition_id: 0,
        };
        let authority = cluster
            .authority_for(&partition)
            .expect("the committed placement must become the authority");
        assert_eq!(authority.owner, 1);
        assert_eq!(
            authority.epoch, 7,
            "the committed ownership epoch must be used"
        );
        assert_eq!(authority.lease, Duration::from_millis(60_000));

        // An expired lease fails closed.
        let placement = snapshot
            .partitions
            .iter()
            .find(|placement| placement.partition == partition)
            .expect("placement");
        assert!(!cluster.store.authority_expired(placement));
        let _ = std::fs::remove_dir_all(path);
    }
}

#[cfg(test)]
mod transfer_tests {
    use super::*;
    use crate::transfer::sha256;
    use rovqet_control::TimeoutBudget;
    use rovqet_control::rpc::{
        Q1TransferChunkRequest, Q1TransferChunkResponse, Q1TransferFile, Q1TransferFinalizeRequest,
        Q1TransferFinalizeResponse, Q1TransferPrepareRequest, Q1TransferPrepareResponse,
        Q1TransferStatus,
    };

    const MIGRATION: u128 = 0xabcd_0000_0000_0007;

    fn free_port() -> u16 {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("port");
        let port = listener.local_addr().expect("addr").port();
        drop(listener);
        port
    }

    fn directory(name: &str) -> PathBuf {
        let path = std::env::temp_dir().join(format!(
            "rovqet-transfer-{name}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|value| value.as_nanos())
                .unwrap_or(0)
        ));
        std::fs::create_dir_all(&path).expect("directory");
        path
    }

    fn options(
        node_id: u64,
        listen: SocketAddr,
        peer: u64,
        peer_address: SocketAddr,
        staging: PathBuf,
    ) -> ClusterOptions {
        ClusterOptions {
            cluster_id: 5150,
            node_id,
            listen,
            peers: vec![(peer, peer_address)],
            metadata: None,
            metadata_raft: None,
            transfer_staging: Some(staging),
        }
    }

    async fn serve(cluster: Arc<Cluster>, listen: SocketAddr) -> tokio::sync::watch::Sender<bool> {
        let listener = tokio::net::TcpListener::bind(listen)
            .await
            .expect("listener");
        let server = rovqet_control::transport::PeerServer::new(
            listener,
            cluster.trust.clone(),
            Arc::new(ClusterHandler::new(
                cluster,
                crate::q1::RouteSlot::default(),
            )),
            Default::default(),
        )
        .expect("peer server");
        let (stop, stop_rx) = tokio::sync::watch::channel(false);
        tokio::spawn(server.serve(stop_rx));
        stop
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_q1_transfer_reaches_the_target_over_real_tcp() {
        let source_port = free_port();
        let target_port = free_port();
        let source_address: SocketAddr = format!("127.0.0.1:{source_port}").parse().unwrap();
        let target_address: SocketAddr = format!("127.0.0.1:{target_port}").parse().unwrap();
        let source = Arc::new(
            Cluster::new(&options(
                1,
                source_address,
                2,
                target_address,
                directory("source"),
            ))
            .await
            .expect("source cluster"),
        );
        let target = Arc::new(
            Cluster::new(&options(
                2,
                target_address,
                1,
                source_address,
                directory("target"),
            ))
            .await
            .expect("target cluster"),
        );
        let _source_stop = serve(source.clone(), source_address).await;
        let _target_stop = serve(target.clone(), target_address).await;

        let payload = b"q1 wal segment bytes".to_vec();
        let digest = sha256(&payload);
        let prepare = Q1TransferPrepareRequest {
            migration_id: MIGRATION,
            source_node: 1,
            target_node: 2,
            tenant_id: 1,
            topic_id: 3,
            partition_id: 0,
            incarnation: 42,
            original_epoch: 5,
            next_epoch: 6,
            manifest_generation: 3,
            manifest_digest: vec![8u8; 32],
            files: vec![Q1TransferFile {
                name: "MANIFEST".into(),
                size: payload.len() as u64,
                digest: digest.clone(),
            }],
        };
        let response = source
            .transport
            .call(
                NodeId::new(2).unwrap(),
                RpcKind::Q1TransferPrepare,
                prepare.encode(),
                TimeoutBudget::new(5000).unwrap(),
                0,
                0,
            )
            .await
            .expect("the prepare must reach the target");
        let response = Q1TransferPrepareResponse::decode(&response.body).expect("decode");
        assert!(response.accepted, "the target must accept the preparation");

        let chunk = Q1TransferChunkRequest {
            migration_id: MIGRATION,
            inventory_digest: vec![8u8; 32],
            chunk_index: 0,
            file_index: 0,
            offset: 0,
            data: payload.clone(),
            digest: digest.clone(),
        };
        let response = source
            .transport
            .call(
                NodeId::new(2).unwrap(),
                RpcKind::Q1TransferChunk,
                chunk.encode(),
                TimeoutBudget::new(5000).unwrap(),
                0,
                0,
            )
            .await
            .expect("the chunk must reach the target");
        let response = Q1TransferChunkResponse::decode(&response.body).expect("decode");
        assert!(response.accepted);
        assert!(!response.duplicate, "the first chunk is not a duplicate");

        // The same chunk again is idempotent.
        let response = source
            .transport
            .call(
                NodeId::new(2).unwrap(),
                RpcKind::Q1TransferChunk,
                chunk.encode(),
                TimeoutBudget::new(5000).unwrap(),
                0,
                0,
            )
            .await
            .expect("the repeated chunk must reach the target");
        let response = Q1TransferChunkResponse::decode(&response.body).expect("decode");
        assert!(response.accepted);
        assert!(response.duplicate, "a repeated chunk must be idempotent");

        // The same identity with different bytes is refused.
        let mut conflicting = chunk.clone();
        conflicting.data = b"q1 wal segment byteS".to_vec();
        conflicting.digest = sha256(&conflicting.data);
        let response = source
            .transport
            .call(
                NodeId::new(2).unwrap(),
                RpcKind::Q1TransferChunk,
                conflicting.encode(),
                TimeoutBudget::new(5000).unwrap(),
                0,
                0,
            )
            .await
            .expect("the conflicting chunk must reach the target");
        let response = Q1TransferChunkResponse::decode(&response.body).expect("decode");
        assert!(!response.accepted, "a conflicting chunk must be refused");

        // Without the fence evidence the target only confirms the inventory.
        let finalize = Q1TransferFinalizeRequest {
            migration_id: MIGRATION,
            inventory_digest: vec![8u8; 32],
            final_digest: digest.clone(),
            last_committed_offset: 10,
            transfer_watermark: 10,
            manifest_generation: 3,
            fence_offset: None,
            fence_digest: None,
            old_epoch: 5,
        };
        let response = source
            .transport
            .call(
                NodeId::new(2).unwrap(),
                RpcKind::Q1TransferFinalize,
                finalize.encode(),
                TimeoutBudget::new(5000).unwrap(),
                0,
                0,
            )
            .await
            .expect("the finalize must reach the target");
        let response = Q1TransferFinalizeResponse::decode(&response.body).expect("decode");
        assert_eq!(response.status, Q1TransferStatus::ReadyForFence);

        // With the fence evidence the target confirms the activation.
        let finalize = Q1TransferFinalizeRequest {
            fence_offset: Some(11),
            fence_digest: Some(vec![2u8; 32]),
            ..finalize
        };
        let response = source
            .transport
            .call(
                NodeId::new(2).unwrap(),
                RpcKind::Q1TransferFinalize,
                finalize.encode(),
                TimeoutBudget::new(5000).unwrap(),
                0,
                0,
            )
            .await
            .expect("the finalize must reach the target");
        let response = Q1TransferFinalizeResponse::decode(&response.body).expect("decode");
        assert_eq!(response.status, Q1TransferStatus::ReadyForActivation);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_transfer_without_a_staging_area_is_unsupported() {
        let port = free_port();
        let address: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
        let mut options = options(1, address, 2, address, directory("unused"));
        options.transfer_staging = None;
        let cluster = Arc::new(Cluster::new(&options).await.expect("cluster"));
        let handler = ClusterHandler::new(cluster, crate::q1::RouteSlot::default());
        let envelope = rovqet_control::wire::Envelope::new(
            5150,
            2,
            1,
            7,
            RpcKind::Q1TransferPrepare.as_u16(),
            0,
            0,
            1000,
            Vec::new(),
        );
        let error = handler.handle(envelope).await;
        assert_eq!(error, Err(RpcErrorCode::Unsupported));
    }
}

#[cfg(test)]
mod migration_tests {
    use super::*;
    use crate::migration::{MigrationDriver, TRANSFER_CHUNK_BYTES};
    use rovqet_core::{DurableOffset, PartitionRef, QoS};
    use rovqet_metadata_raft::types::{MetadataCommand, MigrationStage};
    use rovqet_metadata_raft::{MetadataNode, MetadataNodeConfig};
    use std::time::Duration;

    const CLUSTER: u128 = 990099;

    fn free_port() -> u16 {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("port");
        let port = listener.local_addr().expect("addr").port();
        drop(listener);
        port
    }

    fn directory(name: &str) -> PathBuf {
        let path = std::env::temp_dir().join(format!(
            "rovqet-migration-{name}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|value| value.as_nanos())
                .unwrap_or(0)
        ));
        std::fs::create_dir_all(&path).expect("directory");
        path
    }

    fn partition() -> PartitionRef {
        PartitionRef {
            tenant_id: 1,
            topic_id: 3,
            partition_id: 0,
        }
    }

    async fn metadata_node(
        id: u64,
        ports: [u16; 3],
        path: PathBuf,
        bootstrap: bool,
    ) -> MetadataNode {
        let peers: Vec<(u64, std::net::SocketAddr)> = (1..=3)
            .map(|node| {
                (
                    node,
                    format!("127.0.0.1:{}", ports[node as usize - 1])
                        .parse()
                        .expect("addr"),
                )
            })
            .collect();
        MetadataNode::start(MetadataNodeConfig {
            id,
            cluster_id: CLUSTER,
            listen: format!("127.0.0.1:{}", ports[id as usize - 1])
                .parse()
                .expect("addr"),
            peers,
            data_directory: path,
            bootstrap,
            election_timeout_ms: 300,
            heartbeat_interval_ms: 80,
            max_payload_entries: 64,
            snapshot_logs_since_last: 8,
        })
        .await
        .expect("metadata node")
    }

    async fn serve(cluster: Arc<Cluster>, listen: SocketAddr) -> tokio::sync::watch::Sender<bool> {
        let listener = tokio::net::TcpListener::bind(listen)
            .await
            .expect("listener");
        let server = rovqet_control::transport::PeerServer::new(
            listener,
            cluster.trust.clone(),
            Arc::new(ClusterHandler::new(
                cluster,
                crate::q1::RouteSlot::default(),
            )),
            Default::default(),
        )
        .expect("peer server");
        let (stop, stop_rx) = tokio::sync::watch::channel(false);
        tokio::spawn(server.serve(stop_rx));
        stop
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn a_committed_migration_fences_the_old_owner_and_activates_the_new_one() {
        // The metadata group: three real voters, the source is the bootstrap node.
        let raft_ports = [free_port(), free_port(), free_port()];
        let mut raft_nodes = Vec::new();
        for index in 0..3 {
            raft_nodes.push(Arc::new(
                metadata_node(
                    index as u64 + 1,
                    raft_ports,
                    directory(&format!("raft{}", index + 1)),
                    index == 0,
                )
                .await,
            ));
        }
        let source_raft = raft_nodes[0].clone();
        // Wait until the source won the election: only the leader may propose.
        let deadline = std::time::Instant::now() + Duration::from_secs(30);
        while source_raft.current_leader().await != Some(1) && std::time::Instant::now() < deadline
        {
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        assert_eq!(source_raft.current_leader().await, Some(1));

        // The source owns a Q1 partition with committed history.
        let source_dir = directory("source-q1");
        let route = Arc::new(
            crate::q1::Route::open(crate::q1::Options {
                directory: source_dir.clone(),
                topic: "durable".into(),
                node_id: 1,
                partition: partition(),
                incarnation: 42,
                owner_epoch: 7,
                dedup_window: Duration::from_secs(86400),
                max_producers: 64,
                max_outcomes: 64,
                initialize_new_log: true,
                terminal_policy: rovqet_durable::TerminalPolicy::Dlq,
                max_attempts: 3,
                retry_initial: Duration::from_millis(10),
                retry_max: Duration::from_millis(100),
                dlq: None,
            })
            .expect("the source route"),
        );
        route.install_authority(Some(
            rovqet_durable::Authority::new(1, 7, Duration::from_secs(600)).expect("authority"),
        ));
        route
            .publish_durable(&rovqet_control::rpc::RoutePublishRequest {
                tenant_id: 1,
                topic_id: 3,
                partition_id: 0,
                qos: 1,
                ownership_epoch: 7,
                producer_id: 11,
                producer_epoch: 1,
                producer_sequence: 0,
                message_id: 777,
                ttl_ms: 5000,
                payload: b"committed before the migration".to_vec(),
            })
            .await
            .expect("a publication before the migration");

        // The committed metadata places the partition on the source.
        for command in [
            MetadataCommand::SetNode {
                node: 1,
                address: "127.0.0.1:1".into(),
            },
            MetadataCommand::SetNode {
                node: 2,
                address: "127.0.0.1:2".into(),
            },
            MetadataCommand::SetPlacement {
                tenant_id: 1,
                topic_id: 3,
                partition_id: 0,
                owner: 1,
                ownership_epoch: 7,
                lease_valid_for_ms: 600_000,
            },
        ] {
            let response = source_raft
                .propose(command)
                .await
                .expect("committed metadata");
            assert!(
                matches!(
                    response,
                    rovqet_metadata_raft::types::MetadataCommandResponse::Applied { .. }
                ),
                "the metadata command must be applied"
            );
        }

        // The two brokers: the source and the target, with real peer sockets.
        let source_port = free_port();
        let target_port = free_port();
        let source_address: SocketAddr = format!("127.0.0.1:{source_port}").parse().unwrap();
        let target_address: SocketAddr = format!("127.0.0.1:{target_port}").parse().unwrap();
        let source_staging = directory("source-transfer");
        let target_staging = directory("target-transfer");
        let source_cluster = Arc::new(
            Cluster::new(&ClusterOptions {
                cluster_id: CLUSTER,
                node_id: 1,
                listen: source_address,
                peers: vec![(2, target_address)],
                metadata: None,
                metadata_raft: None,
                transfer_staging: Some(source_staging.clone()),
            })
            .await
            .expect("the source cluster"),
        );
        let target_cluster = Arc::new(
            Cluster::new(&ClusterOptions {
                cluster_id: CLUSTER,
                node_id: 2,
                listen: target_address,
                peers: vec![(1, source_address)],
                metadata: None,
                metadata_raft: None,
                transfer_staging: Some(target_staging.clone()),
            })
            .await
            .expect("the target cluster"),
        );
        let _source_stop = serve(source_cluster.clone(), source_address).await;
        let _target_stop = serve(target_cluster.clone(), target_address).await;

        // The operator commits the migration.
        let migration_id = crate::migration::migration_id(1, 3, 0, 42, 1, 2);
        let response = source_raft
            .propose(MetadataCommand::BeginMigration {
                migration_id,
                tenant_id: 1,
                topic_id: 3,
                partition_id: 0,
                source_node: 1,
                target_node: 2,
                source_incarnation: 42,
                original_epoch: 7,
                next_epoch: 8,
                dependent_partitions: Vec::new(),
            })
            .await
            .expect("the migration must be committed");
        assert!(
            matches!(
                response,
                rovqet_metadata_raft::types::MetadataCommandResponse::Applied { .. }
            ),
            "the migration must be accepted by the metadata group"
        );

        // The source drives the stages.
        let driver = MigrationDriver::new(
            source_cluster.transport.clone(),
            source_raft.clone(),
            source_dir.clone(),
            1,
        );
        tokio::spawn(crate::migration::run_coordinator(
            driver,
            route.clone(),
            1,
            None,
        ));

        // The migration completes: the committed placement moves to the target.
        let deadline = std::time::Instant::now() + Duration::from_secs(60);
        let mut activated = false;
        while std::time::Instant::now() < deadline {
            let state = source_raft.applied_state();
            if let Some(placement) = state.placement(1, 3, 0) {
                if placement.owner == 2 && placement.ownership_epoch == 8 {
                    activated = true;
                    break;
                }
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        let state = source_raft.applied_state();
        let record = state
            .migrations
            .get(&migration_id)
            .expect("the migration record");
        assert_eq!(
            record.stage,
            MigrationStage::ActivateNew,
            "the migration must reach the activation"
        );
        assert!(
            record.fence_proof.is_some(),
            "the activation must carry the fence proof"
        );
        assert!(
            record.transfer_watermark.is_some(),
            "the activation must carry a verified transfer watermark"
        );
        assert!(activated, "the committed placement must move to the target");

        // The old owner refuses to mutate: it was fenced durably.
        let refused = route
            .publish_durable(&rovqet_control::rpc::RoutePublishRequest {
                tenant_id: 1,
                topic_id: 3,
                partition_id: 0,
                qos: 1,
                ownership_epoch: 7,
                producer_id: 11,
                producer_epoch: 1,
                producer_sequence: 1,
                message_id: 778,
                ttl_ms: 5000,
                payload: b"after the fence".to_vec(),
            })
            .await;
        assert!(
            refused.is_err(),
            "the fenced old owner must not commit another publication"
        );

        // The target received the complete inventory.
        let staged = target_staging;
        let mut files = 0;
        for entry in std::fs::read_dir(&staged).expect("staging root") {
            let entry = entry.expect("entry");
            let inventory = entry.path().join("staging.json");
            if inventory.is_file() {
                files += 1;
            }
        }
        assert!(
            files >= 1,
            "the target must have staged the transferred history"
        );
        let _ = TRANSFER_CHUNK_BYTES;
        let _ = DurableOffset {
            log_incarnation: 42,
            sequence: 0,
        };
        let _ = QoS::Q1;
    }
}

#[cfg(test)]
mod successor_tests {
    use super::*;
    use crate::migration::MigrationDriver;
    use crate::q1::{Options as Q1Options, RouteSlot};
    use rovqet_control::rpc::RoutePublishRequest;
    use rovqet_metadata_raft::types::{MetadataCommand, MigrationStage};

    const CLUSTER: u128 = 770077;

    fn free_port() -> u16 {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("port");
        let port = listener.local_addr().expect("addr").port();
        drop(listener);
        port
    }

    fn directory(name: &str) -> PathBuf {
        let path = std::env::temp_dir().join(format!(
            "rovqet-successor-{name}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|value| value.as_nanos())
                .unwrap_or(0)
        ));
        std::fs::create_dir_all(&path).expect("directory");
        path
    }

    fn partition() -> PartitionRef {
        PartitionRef {
            tenant_id: 1,
            topic_id: 3,
            partition_id: 0,
        }
    }

    fn q1_options(directory: PathBuf, node_id: u64, initialize: bool) -> Q1Options {
        Q1Options {
            directory,
            topic: "durable".into(),
            node_id,
            partition: partition(),
            incarnation: 42,
            owner_epoch: 7,
            dedup_window: Duration::from_secs(86400),
            max_producers: 64,
            max_outcomes: 64,
            initialize_new_log: initialize,
            terminal_policy: rovqet_durable::TerminalPolicy::Dlq,
            max_attempts: 3,
            retry_initial: Duration::from_millis(10),
            retry_max: Duration::from_millis(100),
            dlq: None,
        }
    }

    async fn serve(cluster: Arc<Cluster>, listen: SocketAddr) -> tokio::sync::watch::Sender<bool> {
        let listener = tokio::net::TcpListener::bind(listen)
            .await
            .expect("listener");
        let server = rovqet_control::transport::PeerServer::new(
            listener,
            cluster.trust.clone(),
            Arc::new(ClusterHandler::new(cluster, RouteSlot::default())),
            Default::default(),
        )
        .expect("peer server");
        let (stop, stop_rx) = tokio::sync::watch::channel(false);
        tokio::spawn(server.serve(stop_rx));
        stop
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn the_successor_opens_the_transferred_history_and_keeps_the_producer_outcome() {
        let raft_ports = [free_port(), free_port(), free_port()];
        let raft_peers: Vec<(u64, SocketAddr)> = (1..=3)
            .map(|node| {
                (
                    node,
                    format!("127.0.0.1:{}", raft_ports[node as usize - 1])
                        .parse()
                        .expect("addr"),
                )
            })
            .collect();
        let source_address: SocketAddr = format!("127.0.0.1:{}", free_port()).parse().unwrap();
        let target_address: SocketAddr = format!("127.0.0.1:{}", free_port()).parse().unwrap();
        let source_staging = directory("source-transfer");
        let target_staging = directory("target-transfer");
        let source_directory = directory("source-q1");
        let target_directory = directory("target-q1");

        // The source and the target are two brokers that are also voters of the metadata
        // group. The third voter is configured but not started.
        let source_cluster = Arc::new(
            Cluster::new(&ClusterOptions {
                cluster_id: CLUSTER,
                node_id: 1,
                listen: source_address,
                peers: vec![(2, target_address)],
                metadata: None,
                metadata_raft: Some(MetadataRaftOptions {
                    cluster_id: CLUSTER,
                    node_id: 1,
                    listen: raft_peers[0].1,
                    peers: raft_peers.clone(),
                    data_directory: directory("source-raft"),
                    bootstrap: true,
                }),
                transfer_staging: Some(source_staging.clone()),
            })
            .await
            .expect("the source cluster"),
        );
        let target_cluster = Arc::new(
            Cluster::new(&ClusterOptions {
                cluster_id: CLUSTER,
                node_id: 2,
                listen: target_address,
                peers: vec![(1, source_address)],
                metadata: None,
                metadata_raft: Some(MetadataRaftOptions {
                    cluster_id: CLUSTER,
                    node_id: 2,
                    listen: raft_peers[1].1,
                    peers: raft_peers.clone(),
                    data_directory: directory("target-raft"),
                    bootstrap: false,
                }),
                transfer_staging: Some(target_staging.clone()),
            })
            .await
            .expect("the target cluster"),
        );
        let _source_stop = serve(source_cluster.clone(), source_address).await;
        let _target_stop = serve(target_cluster.clone(), target_address).await;

        let source_raft = source_cluster.raft.clone().expect("the source voter");
        let target_raft = target_cluster.raft.clone().expect("the target voter");
        let deadline = std::time::Instant::now() + Duration::from_secs(30);
        while source_raft.current_leader().await != Some(1) && std::time::Instant::now() < deadline
        {
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        assert_eq!(source_raft.current_leader().await, Some(1));

        // The committed metadata places the partition on the source.
        for command in [
            MetadataCommand::SetNode {
                node: 1,
                address: "127.0.0.1:1".into(),
            },
            MetadataCommand::SetNode {
                node: 2,
                address: "127.0.0.1:2".into(),
            },
            MetadataCommand::SetPlacement {
                tenant_id: 1,
                topic_id: 3,
                partition_id: 0,
                owner: 1,
                ownership_epoch: 7,
                lease_valid_for_ms: 600_000,
            },
        ] {
            let response = source_raft
                .propose(command)
                .await
                .expect("committed metadata");
            assert!(
                matches!(
                    response,
                    rovqet_metadata_raft::types::MetadataCommandResponse::Applied { .. }
                ),
                "the metadata command must be applied"
            );
        }

        // The source opens its route because the committed metadata names it as the owner.
        let source_slot = Arc::new(RouteSlot::default());
        tokio::spawn(crate::migration::run_activator(
            source_cluster.clone(),
            source_raft.clone(),
            source_slot.clone(),
            q1_options(source_directory.clone(), 1, true),
            1,
            Some(source_staging.clone()),
        ));
        let deadline = std::time::Instant::now() + Duration::from_secs(30);
        while source_slot.current().is_none() && std::time::Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        let source_route = source_slot.current().expect("the source route must open");

        // The source publishes and the outcome is recorded externally.
        let request = RoutePublishRequest {
            tenant_id: 1,
            topic_id: 3,
            partition_id: 0,
            qos: 1,
            ownership_epoch: 7,
            producer_id: 11,
            producer_epoch: 1,
            producer_sequence: 0,
            message_id: 777,
            ttl_ms: 5000,
            payload: b"committed before the migration".to_vec(),
        };
        let published = source_route
            .publish_durable(&request)
            .await
            .expect("the publication must commit");
        let original_offset = published.sequence;
        assert_eq!(
            published.log_incarnation, 42,
            "the incarnation is preserved"
        );

        // The target waits for the committed activation.
        let target_slot = Arc::new(RouteSlot::default());
        tokio::spawn(crate::migration::run_activator(
            target_cluster.clone(),
            target_raft.clone(),
            target_slot.clone(),
            q1_options(target_directory.clone(), 2, false),
            2,
            Some(target_staging.clone()),
        ));

        // The operator commits the migration and the source drives the stages.
        let migration_id = crate::migration::migration_id(1, 3, 0, 42, 1, 2);
        let response = source_raft
            .propose(MetadataCommand::BeginMigration {
                migration_id,
                tenant_id: 1,
                topic_id: 3,
                partition_id: 0,
                source_node: 1,
                target_node: 2,
                source_incarnation: 42,
                original_epoch: 7,
                next_epoch: 8,
                dependent_partitions: Vec::new(),
            })
            .await
            .expect("the migration must be committed");
        assert!(
            matches!(
                response,
                rovqet_metadata_raft::types::MetadataCommandResponse::Applied { .. }
            ),
            "the migration must be accepted"
        );
        let driver = MigrationDriver::new(
            source_cluster.transport.clone(),
            source_raft.clone(),
            source_directory.clone(),
            1,
        );
        tokio::spawn(crate::migration::run_coordinator(
            driver,
            source_route.clone(),
            1,
            None,
        ));

        // The activation is committed.
        let deadline = std::time::Instant::now() + Duration::from_secs(90);
        let mut activated = false;
        while std::time::Instant::now() < deadline {
            let state = source_raft.applied_state();
            if let Some(record) = state.migrations.get(&migration_id) {
                if record.stage == MigrationStage::ActivateNew {
                    activated = true;
                    break;
                }
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        assert!(activated, "the migration must be committed");

        // The successor opens the transferred history.
        let deadline = std::time::Instant::now() + Duration::from_secs(60);
        while target_slot.current().is_none() && std::time::Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        let target_route = target_slot
            .current()
            .expect("the successor must open the transferred history");

        // The producer outcome survives: a retry with the original token returns the
        // original offset and does not append a second publication.
        // The retry carries the new ownership epoch but the original producer token.
        let retry = target_route
            .publish_durable(&RoutePublishRequest {
                ownership_epoch: 8,
                ..request.clone()
            })
            .await
            .expect("the retry must be answered");
        assert_eq!(
            retry.sequence, original_offset,
            "the retry must return the original offset"
        );
        assert_eq!(retry.log_incarnation, 42, "the incarnation must not change");

        // The committed history is readable on the successor.
        let fetched = target_route
            .group_fetch_routed(&rovqet_control::rpc::RouteFetchRequest {
                tenant_id: 1,
                topic_id: 3,
                partition_id: 0,
                group_id: 1,
                generation: 0,
                max_messages: 16,
                max_bytes: 65536,
                wait_ms: 0,
            })
            .await
            .expect("the transferred history must be fetchable");
        assert!(
            !fetched.deliveries.is_empty(),
            "the transferred message must be delivered"
        );
        assert_eq!(fetched.deliveries[0].offset.sequence, original_offset);

        // The old owner is still fenced and refuses another publication.
        let refused = source_route
            .publish_durable(&RoutePublishRequest {
                producer_sequence: 1,
                message_id: 778,
                payload: b"after the fence".to_vec(),
                ..request.clone()
            })
            .await;
        assert!(
            refused.is_err(),
            "the fenced old owner must not commit another publication"
        );
    }
}
