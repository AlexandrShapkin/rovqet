//! The asymmetric partition safety property of a handover.
//!
//! When the old owner is isolated before it could fence itself, the successor must not be
//! able to commit the activation: the committed migration state machine refuses an
//! activation without a durable fence proof, so the group stays safe even though the
//! isolated owner may still serve within its previously valid authority interval.
//!
//! An isolated owner that keeps committing inside its lease is not itself a violation. The
//! safety property is that the successor never starts committing while the old owner still
//! has unfenced mutation authority.
use rovqet_metadata_raft::types::{
    FenceProof, MetadataCommand, MetadataCommandResponse, MigrationStage,
};
use rovqet_metadata_raft::{MetadataNode, MetadataNodeConfig};
use std::net::TcpListener;
use std::path::PathBuf;
use std::time::{Duration, Instant};

const CLUSTER: u128 = 660066;

fn free_port() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").expect("port");
    let port = listener.local_addr().expect("addr").port();
    drop(listener);
    port
}

fn directory(name: &str) -> PathBuf {
    let path = std::env::temp_dir().join(format!(
        "rovqet-asymmetric-{name}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|value| value.as_nanos())
            .unwrap_or(0)
    ));
    std::fs::create_dir_all(&path).expect("directory");
    path
}

async fn voter(id: u64, ports: [u16; 3], path: PathBuf, bootstrap: bool) -> MetadataNode {
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

async fn propose_until_answered(
    node: &MetadataNode,
    command: MetadataCommand,
) -> MetadataCommandResponse {
    let deadline = Instant::now() + Duration::from_secs(30);
    let mut last = String::new();
    while Instant::now() < deadline {
        match node.propose(command.clone()).await {
            Ok(response) => return response,
            Err(error) => {
                last = format!("{error:?}");
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
    }
    panic!("the metadata group did not answer: {last}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn an_isolated_old_owner_blocks_the_activation() {
    let ports = [free_port(), free_port(), free_port()];
    let mut nodes = Vec::new();
    for index in 0..3 {
        nodes.push(
            voter(
                index as u64 + 1,
                ports,
                directory(&format!("voter{}", index + 1)),
                index == 0,
            )
            .await,
        );
    }
    // The committed metadata places the partition on node 1.
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
        let response = propose_until_answered(&nodes[0], command).await;
        assert!(
            matches!(response, MetadataCommandResponse::Applied { .. }),
            "the metadata command must be applied"
        );
    }

    // The handover is committed.
    let migration_id = 0x5151_5151_5151_5151;
    let response = propose_until_answered(
        &nodes[0],
        MetadataCommand::BeginMigration {
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
        },
    )
    .await;
    assert!(matches!(response, MetadataCommandResponse::Applied { .. }));

    // The old owner is unreachable: it never gets the chance to fence itself durably.
    let mut isolated = nodes.remove(0);
    isolated
        .shutdown()
        .await
        .expect("shutdown the isolated owner");
    // The remaining majority elects a new leader.
    let deadline = Instant::now() + Duration::from_secs(30);
    let mut leader = None;
    while Instant::now() < deadline {
        for node in &nodes {
            if node.current_leader().await == Some(node.id()) {
                leader = Some(node.id());
                break;
            }
        }
        if leader.is_some() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let leader = leader.expect("the remaining majority must elect a leader");
    let leader_node = nodes
        .iter()
        .find(|node| node.id() == leader)
        .expect("the new leader");

    // The activation is refused: there is no durable fence proof, because the old owner
    // never fenced itself.
    let activation = propose_until_answered(
        leader_node,
        MetadataCommand::AdvanceMigration {
            migration_id,
            stage: MigrationStage::ActivateNew,
            transfer_watermark: Some(10),
            manifest_id: Some("abcd".into()),
            fence_proof: None,
            target_ready: true,
            dependencies: Vec::new(),
        },
    )
    .await;
    assert!(
        matches!(activation, MetadataCommandResponse::Rejected { .. }),
        "an activation without a fence proof must be refused: {activation:?}"
    );

    // The committed placement still names the old owner: the successor owns nothing.
    let state = leader_node.applied_state();
    let placement = state.placement(1, 3, 0).expect("the placement");
    assert_eq!(
        placement.owner, 1,
        "the old owner keeps the committed placement"
    );
    assert_eq!(
        placement.ownership_epoch, 7,
        "the epoch may not be advanced"
    );

    // A fence proof of another migration is refused as well.
    let foreign = propose_until_answered(
        leader_node,
        MetadataCommand::AdvanceMigration {
            migration_id,
            stage: MigrationStage::FenceOld,
            transfer_watermark: Some(10),
            manifest_id: Some("abcd".into()),
            fence_proof: Some(FenceProof {
                migration_id: migration_id + 1,
                old_owner: 1,
                old_epoch: 7,
                fence_offset: 11,
                fence_digest: "d".into(),
                manifest_generation: 3,
            }),
            target_ready: false,
            dependencies: Vec::new(),
        },
    )
    .await;
    assert!(
        matches!(foreign, MetadataCommandResponse::Rejected { .. }),
        "a fence proof of another migration must be refused: {foreign:?}"
    );

    for node in nodes {
        node.shutdown().await.expect("shutdown");
    }
}
