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
    HealthResponse, OwnershipRequest, OwnershipResponse, RouteAckRequest, RouteAckResponse,
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
}

pub struct Cluster {
    pub trust: Arc<TrustContext>,
    pub store: Arc<MetadataStore>,
    pub transport: Arc<PeerTransport>,
    pub node: NodeId,
    pub started: Instant,
}

impl Cluster {
    pub fn new(options: &ClusterOptions) -> std::io::Result<Self> {
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
        Ok(Self {
            trust,
            store,
            transport,
            node,
            started: Instant::now(),
        })
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
    route: Option<Arc<Route>>,
}

impl ClusterHandler {
    pub fn new(cluster: Arc<Cluster>, route: Option<Arc<Route>>) -> Self {
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
                    let route = route.as_ref().ok_or(RpcErrorCode::NotOwner)?;
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
                    let route = route.as_ref().ok_or(RpcErrorCode::NotOwner)?;
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
                    let route = route.as_ref().ok_or(RpcErrorCode::NotOwner)?;
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
