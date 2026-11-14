//! Migration transitions under metadata quorum loss.
//!
//! A migration stage is only committed by a majority. A minority cannot advance a migration,
//! a restarted voter recovers the committed migration record, and a new leader keeps it.
use rovqet_metadata_raft::types::{MetadataCommand, MetadataCommandResponse, MigrationStage};
use rovqet_metadata_raft::{MetadataNode, MetadataNodeConfig};
use std::net::TcpListener;
use std::path::PathBuf;
use std::time::{Duration, Instant};

const CLUSTER: u128 = 550055;
const MIGRATION: u128 = 0x2222_3333_4444_5555;

fn free_port() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").expect("port");
    let port = listener.local_addr().expect("addr").port();
    drop(listener);
    port
}

fn directory(name: &str) -> PathBuf {
    let path = std::env::temp_dir().join(format!(
        "rovqet-migration-quorum-{name}-{}-{}",
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

async fn wait_for_leader(nodes: &[MetadataNode], deadline: Duration) -> Option<u64> {
    let started = Instant::now();
    while started.elapsed() < deadline {
        for node in nodes {
            if node.current_leader().await == Some(node.id()) {
                return Some(node.id());
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    None
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn a_migration_survives_a_leader_crash_and_a_minority_cannot_advance_it() {
    let ports = [free_port(), free_port(), free_port()];
    let paths: Vec<PathBuf> = (1..=3)
        .map(|index| directory(&format!("voter{index}")))
        .collect();
    let mut nodes = Vec::new();
    for index in 0..3 {
        nodes.push(voter(index as u64 + 1, ports, paths[index].clone(), index == 0).await);
    }
    let leader = wait_for_leader(&nodes, Duration::from_secs(30))
        .await
        .expect("a leader must be elected");

    // A placement and a migration are committed.
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
        MetadataCommand::BeginMigration {
            migration_id: MIGRATION,
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
    ] {
        let node = nodes
            .iter()
            .find(|node| node.id() == leader)
            .expect("the leader");
        let response = propose_until_answered(node, command).await;
        assert!(
            matches!(response, MetadataCommandResponse::Applied { .. }),
            "the metadata command must be applied: {response:?}"
        );
    }
    let revision_before = nodes[0].applied_state().revision;
    assert!(revision_before >= 4);

    // The leader crashes. The remaining majority elects a replacement that keeps the
    // committed migration record.
    let leader_index = (leader - 1) as usize;
    let stopped = nodes.remove(leader_index);
    let stopped_id = stopped.id();
    stopped.shutdown().await.expect("shutdown the leader");
    let replacement = wait_for_leader(&nodes, Duration::from_secs(60))
        .await
        .expect("the remaining majority must elect a leader");
    let state = {
        let replacement_node = nodes
            .iter()
            .find(|node| node.id() == replacement)
            .expect("the replacement");
        replacement_node.applied_state()
    };
    let record = state
        .migrations
        .get(&MIGRATION)
        .expect("the committed migration record must survive the failover");
    assert_eq!(record.stage, MigrationStage::Prepare);
    assert_eq!(record.source_node, 1);
    assert_eq!(record.target_node, 2);

    // One more voter goes away: the remaining voter is a minority and cannot advance the
    // migration, whatever it proposes.
    let survivor_index = (0..nodes.len())
        .find(|index| nodes[*index].id() != replacement)
        .expect("a second voter");
    let survivor = nodes.remove(survivor_index);
    survivor.shutdown().await.expect("shutdown");
    let minority = nodes.into_iter().next().expect("the remaining voter");
    let proposal = minority.propose(MetadataCommand::AdvanceMigration {
        migration_id: MIGRATION,
        stage: MigrationStage::CatchUp,
        transfer_watermark: Some(10),
        manifest_id: Some("abcd".into()),
        fence_proof: None,
        target_ready: false,
        dependencies: Vec::new(),
    });
    let result = tokio::time::timeout(Duration::from_secs(3), proposal).await;
    match result {
        Err(_elapsed) => {}
        Ok(Ok(MetadataCommandResponse::Applied { .. })) => {
            panic!("a minority must not advance a migration")
        }
        Ok(Ok(_)) => {}
        Ok(Err(_)) => {}
    }
    assert_eq!(
        minority.applied_state().revision,
        revision_before,
        "a minority must not advance the committed revision"
    );
    let state = minority.applied_state();
    assert_eq!(
        state.migrations.get(&MIGRATION).map(|record| record.stage),
        Some(MigrationStage::Prepare),
        "the migration stays at its committed stage"
    );

    // The restarted voter recovers the committed migration state.
    let restarted = voter(
        stopped_id,
        ports,
        paths[(stopped_id - 1) as usize].clone(),
        false,
    )
    .await;
    let deadline = Instant::now() + Duration::from_secs(30);
    while Instant::now() < deadline
        && restarted
            .applied_state()
            .migrations
            .get(&MIGRATION)
            .is_none()
    {
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(
        restarted
            .applied_state()
            .migrations
            .get(&MIGRATION)
            .map(|record| record.stage),
        Some(MigrationStage::Prepare),
        "a restarted voter must recover the committed migration state"
    );

    minority.shutdown().await.expect("shutdown");
    restarted.shutdown().await.expect("shutdown");
}
