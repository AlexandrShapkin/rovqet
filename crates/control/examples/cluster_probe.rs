//! Minimal inter-node probe: health, metadata snapshot and ownership inspection.
//!
//! Usage: cluster_probe cluster_id node_id target_node peer_node=host:port kind [partition]
use rovqet_control::ids::{ClusterId, NodeId, PeerEndpoint, TrustContext};
use rovqet_control::rpc::{OwnershipRequest, OwnershipResponse, RpcKind};
use rovqet_control::transport::{PeerTransport, TransportLimits};
use std::net::SocketAddr;
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if args.len() < 5 {
        return Err(
            "usage: cluster_probe cluster_id node_id target_node peer_node=host:port kind [partition]"
                .into(),
        );
    }
    let cluster = ClusterId::new(args[0].parse()?).ok_or("invalid cluster id")?;
    let node = NodeId::new(args[1].parse()?).ok_or("invalid node id")?;
    let target = NodeId::new(args[2].parse()?).ok_or("invalid target node")?;
    let (peer_node, address) = args[3]
        .split_once('=')
        .ok_or("peer must be node=host:port")?;
    let peer = PeerEndpoint {
        node: NodeId::new(peer_node.parse()?).ok_or("invalid peer node")?,
        address: address.parse::<SocketAddr>()?,
    };
    let kind = match args[4].as_str() {
        "health" => RpcKind::Health,
        "metadata" => RpcKind::MetadataSnapshotGet,
        "ownership" => RpcKind::OwnershipInspect,
        other => return Err(format!("unknown probe kind {other}").into()),
    };
    let body = if kind == RpcKind::OwnershipInspect {
        let partition: u32 = args.get(5).map(|v| v.parse()).transpose()?.unwrap_or(0);
        OwnershipRequest {
            tenant_id: 1,
            topic_id: 3,
            partition_id: partition,
        }
        .encode()
    } else {
        Vec::new()
    };
    let trust = TrustContext::new(cluster, node, vec![peer]);
    let transport = PeerTransport::new(
        &trust,
        TransportLimits {
            request_timeout: Duration::from_secs(5),
            ..TransportLimits::default()
        },
    );
    let response = transport
        .call(
            target,
            kind,
            body,
            rovqet_control::TimeoutBudget::new(4_000).ok_or("invalid budget")?,
            0,
            0,
        )
        .await
        .map_err(|error| format!("{error:?}"))?;
    if kind == RpcKind::OwnershipInspect {
        let parsed =
            OwnershipResponse::decode(&response.body).map_err(|error| format!("{error:?}"))?;
        println!(
            "OWNERSHIP found={} owner={} epoch={} revision={} expired={}",
            parsed.found,
            parsed.owner,
            parsed.ownership_epoch,
            parsed.metadata_revision,
            parsed.expired
        );
    } else if kind == RpcKind::MetadataSnapshotGet {
        let text = String::from_utf8_lossy(&response.body);
        println!("SNAPSHOT {text}");
    } else {
        let parsed = rovqet_control::HealthResponse::decode(&response.body)
            .map_err(|error| format!("{error:?}"))?;
        println!(
            "HEALTH node={} cluster={} revision={} uptime_ms={}",
            parsed.node_id, parsed.cluster_id, parsed.metadata_revision, parsed.uptime_ms
        );
    }
    Ok(())
}
