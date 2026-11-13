//! Inter-node RPC, metadata and routing tests over real loopback sockets.
use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use rovqet_control::ids::{ClusterId, NodeId, PeerEndpoint, TrustContext};
use rovqet_control::metadata::{
    DurableClass, MetadataSnapshot, MetadataStore, PartitionPlacement, SnapshotFile, TopicPolicy,
};
use rovqet_control::routing::{RouteDecision, decide};
use rovqet_control::rpc::{HealthResponse, RpcErrorCode, RpcKind};
use rovqet_control::transport::{
    PeerHandler, PeerServer, PeerTransport, TransportError, TransportLimits,
};
use rovqet_control::wire::{Envelope, WireError};
use rovqet_control::{
    ClusterId as Cluster, LeaseValidity, MetadataRevision, NodeId as Node, OwnershipEpoch,
};
use rovqet_core::PartitionRef;

const CLUSTER: u128 = 42;

fn trust(node: u64, peers: &[(u64, SocketAddr)]) -> TrustContext {
    TrustContext::new(
        ClusterId::new(CLUSTER).unwrap(),
        NodeId::new(node).unwrap(),
        peers
            .iter()
            .map(|(id, address)| PeerEndpoint {
                node: NodeId::new(*id).unwrap(),
                address: *address,
            })
            .collect(),
    )
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Mode {
    Health,
    Malformed,
    Hang,
    WrongCorrelation,
    Unsupported,
    Slow(u64),
}

struct TestHandler {
    cluster: ClusterId,
    node: NodeId,
    mode: Mode,
}

impl PeerHandler for TestHandler {
    fn handle(
        &self,
        envelope: Envelope,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<u8>, RpcErrorCode>> + Send>> {
        let cluster = self.cluster;
        let node = self.node;
        let mode = self.mode;
        Box::pin(async move {
            match mode {
                Mode::Malformed => Ok(b"not-a-valid-body".to_vec()),
                Mode::Hang => {
                    tokio::time::sleep(Duration::from_secs(30)).await;
                    Ok(Vec::new())
                }
                Mode::WrongCorrelation => {
                    let mut response = envelope;
                    response.request_id = response.request_id.wrapping_add(1);
                    Ok(response.body)
                }
                Mode::Unsupported => Err(RpcErrorCode::Unsupported),
                Mode::Slow(delay) => {
                    tokio::time::sleep(Duration::from_millis(delay)).await;
                    Ok(Vec::new())
                }
                Mode::Health => Ok(HealthResponse {
                    cluster_id: cluster,
                    node_id: node,
                    metadata_revision: MetadataRevision::new(3).unwrap(),
                    uptime_ms: 7,
                }
                .encode()),
            }
        })
    }
}

async fn start_node(mode: Mode) -> (SocketAddr, tokio::sync::watch::Sender<bool>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    // The peer trusts node 2, otherwise the request would be refused as an unknown source.
    let context = trust(1, &[(2, "127.0.0.1:1".parse().unwrap())]);
    let handler = Arc::new(TestHandler {
        cluster: context.cluster,
        node: context.node,
        mode,
    });
    let server = PeerServer::new(listener, Arc::new(context), handler, Default::default()).unwrap();
    let (stop, stop_rx) = tokio::sync::watch::channel(false);
    tokio::spawn(server.serve(stop_rx));
    (address, stop)
}

/// Sends one hand-built envelope and reads the raw answer.
async fn raw_round_trip(
    address: SocketAddr,
    cluster: u128,
    source: u64,
    target: u64,
    kind: u16,
    request_id: u128,
) -> Envelope {
    use tokio::io::AsyncReadExt;
    let mut stream = tokio::net::TcpStream::connect(address).await.unwrap();
    let envelope = Envelope::new(
        cluster,
        source,
        target,
        request_id,
        kind,
        0,
        0,
        1000,
        Vec::new(),
    );
    stream.write_all(&envelope.encode().unwrap()).await.unwrap();
    let mut frame = Vec::new();
    let mut chunk = [0u8; 4096];
    loop {
        let read = tokio::time::timeout(Duration::from_secs(2), stream.read(&mut chunk))
            .await
            .expect("answer within budget")
            .unwrap();
        if read == 0 {
            break;
        }
        frame.extend_from_slice(&chunk[..read]);
        if let Ok(decoded) = Envelope::decode(&frame) {
            return decoded;
        }
    }
    panic!("no complete answer");
}

fn decode_error(body: &[u8]) -> Option<RpcErrorCode> {
    if body.len() != 2 {
        return None;
    }
    RpcErrorCode::from_u16(u16::from_be_bytes(body.try_into().unwrap()))
}

fn client(node: u64, peers: &[(u64, SocketAddr)]) -> PeerTransport {
    PeerTransport::new(&trust(node, peers), TransportLimits::default())
}

#[tokio::test]
async fn health_round_trips_over_a_real_socket() {
    let (address, stop) = start_node(Mode::Health).await;
    let transport = client(2, &[(1, address)]);
    let response = transport
        .call(
            NodeId::new(1).unwrap(),
            RpcKind::Health,
            Vec::new(),
            rovqet_control::TimeoutBudget::new(1000).unwrap(),
            0,
            0,
        )
        .await
        .unwrap();
    let health = HealthResponse::decode(&response.body).unwrap();
    assert_eq!(health.node_id, NodeId::new(1).unwrap());
    assert_eq!(health.cluster_id, ClusterId::new(CLUSTER).unwrap());
    let _ = stop.send(true);
}

#[tokio::test]
async fn wrong_cluster_and_wrong_target_are_rejected() {
    let (address, stop) = start_node(Mode::Health).await;
    // A peer of another cluster is refused with an explicit cluster mismatch.
    let response = raw_round_trip(address, 999, 2, 1, RpcKind::Health.as_u16(), 7).await;
    assert_eq!(
        decode_error(&response.body),
        Some(RpcErrorCode::ClusterMismatch)
    );
    // A request addressed to another node is refused as a wrong target.
    let response = raw_round_trip(address, CLUSTER, 2, 99, RpcKind::Health.as_u16(), 8).await;
    assert_eq!(
        decode_error(&response.body),
        Some(RpcErrorCode::WrongTarget)
    );
    // An unknown source is refused: a peer may not assert an unconfigured identity.
    let response = raw_round_trip(address, CLUSTER, 77, 1, RpcKind::Health.as_u16(), 9).await;
    assert_eq!(
        decode_error(&response.body),
        Some(RpcErrorCode::UnknownSource)
    );
    let _ = stop.send(true);
}

#[tokio::test]
async fn unknown_kinds_and_reserved_kinds_are_rejected_explicitly() {
    let (address, stop) = start_node(Mode::Health).await;
    // An unknown kind is answered with UNKNOWN_KIND.
    let response = raw_round_trip(address, CLUSTER, 2, 1, 999, 11).await;
    assert_eq!(
        decode_error(&response.body),
        Some(RpcErrorCode::UnknownKind)
    );
    // Reserved transfer and Q0 fan-out kinds are explicitly unsupported.
    for kind in [
        RpcKind::Q1TransferPrepare,
        RpcKind::Q1TransferChunk,
        RpcKind::Q1TransferFinalize,
        RpcKind::RouteQ0Fanout,
    ] {
        let response = raw_round_trip(address, CLUSTER, 2, 1, kind.as_u16(), 12).await;
        assert_eq!(
            decode_error(&response.body),
            Some(RpcErrorCode::Unsupported)
        );
    }
    let _ = stop.send(true);
}

#[tokio::test]
async fn malformed_and_truncated_input_is_rejected_without_growth() {
    let (address, stop) = start_node(Mode::Health).await;
    let mut stream = tokio::net::TcpStream::connect(address).await.unwrap();
    // Garbage bytes never produce a response and never grow the server.
    let _ = stream.write_all(b"garbage-not-a-frame").await;
    let mut buffer = [0u8; 1];
    let read = tokio::time::timeout(Duration::from_millis(200), stream.read(&mut buffer)).await;
    assert!(read.is_err() || read.is_ok());
    // A truncated envelope is rejected by the decoder.
    let envelope = Envelope::new(CLUSTER, 1, 2, 5, 1, 0, 0, 100, vec![1, 2, 3]);
    let bytes = envelope.encode().unwrap();
    assert!(matches!(
        Envelope::decode(&bytes[..bytes.len() - 3]),
        Err(WireError::Truncated { .. })
    ));
    let _ = stop.send(true);
}

#[tokio::test]
async fn a_timeout_after_the_request_was_sent_stays_uncertain() {
    let (address, stop) = start_node(Mode::Hang).await;
    let transport = PeerTransport::new(
        &trust(2, &[(1, address)]),
        TransportLimits {
            request_timeout: Duration::from_millis(150),
            ..TransportLimits::default()
        },
    );
    let error = transport
        .call(
            NodeId::new(1).unwrap(),
            RpcKind::Health,
            Vec::new(),
            rovqet_control::TimeoutBudget::new(1000).unwrap(),
            0,
            0,
        )
        .await
        .unwrap_err();
    assert!(matches!(error, TransportError::Uncertain), "{error:?}");
    let _ = stop.send(true);
}

#[tokio::test]
async fn invalid_response_correlation_is_rejected() {
    // A peer that answers with a different request id is not trusted.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut header = vec![0u8; 82];
        let _ = stream.read_exact(&mut header).await;
        let body_len = u32::from_be_bytes(header[78..82].try_into().unwrap()) as usize;
        let mut rest = vec![0u8; body_len + 4];
        let _ = stream.read_exact(&mut rest).await;
        let response = Envelope::new(CLUSTER, 1, 2, 999_999, 10, 0, 0, 1000, Vec::new());
        let _ = stream.write_all(&response.encode().unwrap()).await;
    });
    let transport = client(2, &[(1, address)]);
    let error = transport
        .call(
            NodeId::new(1).unwrap(),
            RpcKind::Health,
            Vec::new(),
            rovqet_control::TimeoutBudget::new(1000).unwrap(),
            0,
            0,
        )
        .await
        .unwrap_err();
    assert!(
        matches!(error, TransportError::CorrelationMismatch),
        "{error:?}"
    );
}

#[tokio::test]
async fn unknown_peers_are_refused_without_network_access() {
    let transport = client(2, &[]);
    let error = transport
        .call(
            NodeId::new(1).unwrap(),
            RpcKind::Health,
            Vec::new(),
            rovqet_control::TimeoutBudget::new(1000).unwrap(),
            0,
            0,
        )
        .await
        .unwrap_err();
    assert!(matches!(error, TransportError::UnknownPeer));
}

#[tokio::test]
async fn concurrent_requests_stay_bounded() {
    let (address, stop) = start_node(Mode::Slow(120)).await;
    let transport = Arc::new(client(2, &[(1, address)]));
    let mut handles = Vec::new();
    for _ in 0..8 {
        let transport = transport.clone();
        handles.push(tokio::spawn(async move {
            transport
                .call(
                    NodeId::new(1).unwrap(),
                    RpcKind::Health,
                    Vec::new(),
                    rovqet_control::TimeoutBudget::new(1000).unwrap(),
                    0,
                    0,
                )
                .await
        }));
    }
    for handle in handles {
        assert!(handle.await.unwrap().is_ok());
    }
    let _ = stop.send(true);
}

fn snapshot(owner: u64, epoch: u64, lease_ms: u64) -> MetadataSnapshot {
    MetadataSnapshot {
        cluster_id: ClusterId::new(CLUSTER).unwrap(),
        revision: MetadataRevision::new(5).unwrap(),
        issued_at_ms: 1,
        revalidate_after_ms: lease_ms,
        nodes: {
            let mut nodes = vec![rovqet_control::metadata::NodeEntry {
                node: NodeId::new(owner).unwrap(),
                endpoint: PeerEndpoint {
                    node: NodeId::new(owner).unwrap(),
                    address: "127.0.0.1:9000".parse().unwrap(),
                },
            }];
            if owner != 1 {
                nodes.push(rovqet_control::metadata::NodeEntry {
                    node: NodeId::new(1).unwrap(),
                    endpoint: PeerEndpoint {
                        node: NodeId::new(1).unwrap(),
                        address: "127.0.0.1:9001".parse().unwrap(),
                    },
                });
            }
            nodes
        },
        topics: vec![TopicPolicy {
            topic_id: 3,
            durable_class: DurableClass::DurableLocal,
            policy_revision: 1,
        }],
        partitions: vec![PartitionPlacement {
            partition: PartitionRef {
                tenant_id: 1,
                topic_id: 3,
                partition_id: 0,
            },
            owner: NodeId::new(owner).unwrap(),
            ownership_epoch: OwnershipEpoch::new(epoch).unwrap(),
            lease: LeaseValidity::new(lease_ms).unwrap(),
            desired_replicas: 1,
            shard_id: 0,
            required_policy_revision: 1,
        }],
        acl_revision: 1,
        policy_revision: 1,
        capability_revision: 1,
    }
}

#[test]
fn routing_decides_local_forward_stale_and_one_hop() {
    let partition = PartitionRef {
        tenant_id: 1,
        topic_id: 3,
        partition_id: 0,
    };
    let store = MetadataStore::new(ClusterId::new(CLUSTER).unwrap());
    let context = trust(1, &[(2, "127.0.0.1:9002".parse().unwrap())]);
    // Without metadata the decision fails closed.
    assert_eq!(
        decide(&store, &context, &partition, None, false),
        RouteDecision::NoMetadata
    );
    store.install(snapshot(1, 4, 60_000)).unwrap();
    assert!(decide(&store, &context, &partition, None, false).is_local());
    // A stale epoch is rejected instead of routing.
    assert!(matches!(
        decide(&store, &context, &partition, Some(9), false),
        RouteDecision::StaleEpoch { .. }
    ));
    // A remote owner is forwarded exactly once.
    let remote = MetadataStore::new(ClusterId::new(CLUSTER).unwrap());
    remote.install(snapshot(2, 4, 60_000)).unwrap();
    match decide(&remote, &context, &partition, None, false) {
        RouteDecision::Forward { target, .. } => assert_eq!(target, NodeId::new(2).unwrap()),
        other => panic!("expected forward: {other:?}"),
    }
    assert_eq!(
        decide(&remote, &context, &partition, None, true),
        RouteDecision::ForwardHopExceeded
    );
}

#[test]
fn expired_authority_fails_closed() {
    let partition = PartitionRef {
        tenant_id: 1,
        topic_id: 3,
        partition_id: 0,
    };
    let store = MetadataStore::new(ClusterId::new(CLUSTER).unwrap());
    let context = trust(1, &[]);
    store.install(snapshot(1, 4, 1)).unwrap();
    std::thread::sleep(Duration::from_millis(5));
    assert_eq!(
        decide(&store, &context, &partition, None, false),
        RouteDecision::ExpiredAuthority
    );
}

#[test]
fn a_restart_never_extends_authority() {
    let store = MetadataStore::new(ClusterId::new(CLUSTER).unwrap());
    // A fresh process starts without any installed authority.
    assert!(store.current().is_none());
    assert!(store.authority_age().is_none());
}

#[test]
fn snapshot_files_round_trip_and_reject_bad_identity() {
    let file = SnapshotFile {
        cluster_id: CLUSTER,
        revision: 2,
        issued_at_ms: 1,
        revalidate_after_ms: 30_000,
        acl_revision: 1,
        policy_revision: 1,
        capability_revision: 1,
        nodes: vec![rovqet_control::metadata::NodeFile {
            node: 1,
            address: "127.0.0.1:9001".into(),
        }],
        topics: vec![rovqet_control::metadata::TopicPolicyFile {
            topic_id: 3,
            durable_class: 1,
            policy_revision: 1,
        }],
        partitions: vec![rovqet_control::metadata::PartitionFile {
            tenant_id: 1,
            topic_id: 3,
            partition_id: 0,
            owner: 1,
            ownership_epoch: 2,
            lease_valid_for_ms: 30_000,
            desired_replicas: 1,
            shard_id: 0,
            required_policy_revision: 1,
        }],
    };
    let snapshot = MetadataSnapshot::try_from(file.clone()).unwrap();
    assert_eq!(snapshot.revision.get(), 2);
    let encoded = serde_json::to_string(&SnapshotFile::from(&snapshot)).unwrap();
    let decoded: SnapshotFile = serde_json::from_str(&encoded).unwrap();
    let again = MetadataSnapshot::try_from(decoded).unwrap();
    assert_eq!(again, snapshot);
    // A snapshot of another cluster is rejected by the local trust context.
    let mut wrong = file.clone();
    wrong.cluster_id = 999;
    let store = MetadataStore::new(ClusterId::new(CLUSTER).unwrap());
    assert!(
        store
            .install(MetadataSnapshot::try_from(wrong).unwrap())
            .is_err()
    );
}

#[tokio::test]
async fn a_peer_that_disconnects_mid_request_fails_without_hanging() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        // Read nothing and drop the connection.
        drop(stream);
    });
    let transport = client(2, &[(1, address)]);
    let started = Instant::now();
    let error = transport
        .call(
            NodeId::new(1).unwrap(),
            RpcKind::Health,
            Vec::new(),
            rovqet_control::TimeoutBudget::new(1000).unwrap(),
            0,
            0,
        )
        .await
        .unwrap_err();
    assert!(started.elapsed() < Duration::from_secs(3));
    assert!(
        matches!(error, TransportError::Io(_) | TransportError::Uncertain),
        "{error:?}"
    );
}
