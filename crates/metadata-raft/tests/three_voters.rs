//! Real three-voter metadata Raft tests over loopback TCP.
//!
//! The voters run as independent Raft instances with separate persistent directories. The
//! tests observe leadership, committed application state and quorum behaviour through the
//! library's own results, never through internal metrics.
use rovqet_metadata_raft::types::{MetadataCommand, MetadataCommandResponse};
use rovqet_metadata_raft::{MetadataNode, MetadataNodeConfig};
use std::net::TcpListener;
use std::path::PathBuf;
use std::time::{Duration, Instant};

const CLUSTER: u128 = 990011;

fn free_port() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").expect("port");
    let port = listener.local_addr().expect("addr").port();
    drop(listener);
    port
}

fn directory(name: &str) -> PathBuf {
    let path = std::env::temp_dir().join(format!(
        "rovqet-metadata-raft-{}-{}-{}",
        name,
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|value| value.as_nanos())
            .unwrap_or(0)
    ));
    std::fs::create_dir_all(&path).expect("directory");
    path
}

fn config(id: u64, ports: [u16; 3], path: PathBuf, bootstrap: bool) -> MetadataNodeConfig {
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
    MetadataNodeConfig {
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
        // A low threshold keeps log compaction and snapshot installation observable.
        snapshot_logs_since_last: 2,
    }
}

fn init_tracing() {
    use std::sync::Once;
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        let _ = tracing_subscriber::fmt()
            .with_env_filter(
                tracing_subscriber::EnvFilter::try_from_default_env()
                    .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("openraft=warn")),
            )
            .with_test_writer()
            .try_init();
    });
}

async fn wait_for_leader(nodes: &[MetadataNode], deadline: Duration) -> Option<u64> {
    let started = Instant::now();
    while started.elapsed() < deadline {
        for node in nodes {
            // Only a node that reports itself as leader has actually won an election: a
            // follower may still name the previous, now dead, leader for a while.
            if node.current_leader().await == Some(node.id()) {
                return Some(node.id());
            }
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    None
}

async fn wait_for<F, Fut>(deadline: Duration, mut check: F) -> bool
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let started = Instant::now();
    while started.elapsed() < deadline {
        if check().await {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    false
}

fn node_command(node: u64) -> MetadataCommand {
    MetadataCommand::SetNode {
        node,
        address: format!("127.0.0.1:{}", 9000 + node),
    }
}

async fn start_group(ports: [u16; 3], directories: &[PathBuf]) -> Vec<MetadataNode> {
    let mut nodes = Vec::new();
    for index in 0..3 {
        let node = MetadataNode::start(config(
            index as u64 + 1,
            ports,
            directories[index].clone(),
            index == 0,
        ))
        .await
        .expect("metadata node");
        nodes.push(node);
    }
    nodes
}

async fn converged(nodes: &[MetadataNode], node: u64) -> bool {
    for candidate in nodes {
        if !candidate.applied_state().nodes.contains_key(&node) {
            return false;
        }
    }
    true
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn three_voters_elect_a_leader() {
    init_tracing();
    let ports = [free_port(), free_port(), free_port()];
    let directories: Vec<PathBuf> = (1..=3)
        .map(|index| directory(&format!("node{index}")))
        .collect();
    let nodes = start_group(ports, &directories).await;

    // A real election happens through the library: the bootstrap node is elected leader.
    let leader = wait_for_leader(&nodes, Duration::from_secs(20))
        .await
        .expect("a leader must be elected");
    assert!((1..=3).contains(&leader));

    // The elected leader persists its own log entry.
    let leader_node = nodes
        .iter()
        .find(|node| node.id() == leader)
        .expect("leader node");
    assert!(leader_node.last_log_index().await.unwrap_or(0) > 0);

    for node in nodes {
        node.shutdown().await.expect("shutdown");
    }
    for path in directories {
        let _ = std::fs::remove_dir_all(path);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn commits_replicate_and_survive_a_leader_failure() {
    init_tracing();
    let ports = [free_port(), free_port(), free_port()];
    let directories: Vec<PathBuf> = (1..=3)
        .map(|index| directory(&format!("commit{index}")))
        .collect();
    let nodes = start_group(ports, &directories).await;
    let leader = wait_for_leader(&nodes, Duration::from_secs(20))
        .await
        .expect("a leader must be elected");
    let leader_node = nodes
        .iter()
        .find(|node| node.id() == leader)
        .expect("leader node");
    let response = tokio::time::timeout(
        Duration::from_secs(10),
        leader_node.propose(node_command(9)),
    )
    .await
    .expect("commit within the deadline")
    .expect("committed command");
    assert!(matches!(
        response,
        MetadataCommandResponse::Applied { revision } if revision >= 1
    ));
    let replicated = wait_for(Duration::from_secs(20), || converged(&nodes, 9)).await;
    assert!(replicated, "every voter must apply the committed command");

    // The leader process dies. The remaining two voters must elect a replacement and keep the
    // committed metadata.
    let mut survivors = Vec::new();
    for node in nodes {
        if node.id() == leader {
            node.shutdown().await.expect("shutdown leader");
        } else {
            survivors.push(node);
        }
    }
    let replacement = wait_for_leader(&survivors, Duration::from_secs(30))
        .await
        .expect("the remaining majority must elect a new leader");
    assert_ne!(replacement, leader, "leadership must actually change");
    let new_leader = survivors
        .iter()
        .find(|node| node.id() == replacement)
        .expect("new leader node");
    assert!(
        new_leader.applied_state().nodes.contains_key(&9),
        "the new leader must keep the committed metadata"
    );
    let response = tokio::time::timeout(
        Duration::from_secs(15),
        new_leader.propose(node_command(10)),
    )
    .await
    .expect("commit after failover within the deadline")
    .expect("committed command after failover");
    assert!(matches!(response, MetadataCommandResponse::Applied { .. }));
    let replicated = wait_for(Duration::from_secs(20), || converged(&survivors, 10)).await;
    assert!(
        replicated,
        "the surviving majority must apply the new command"
    );

    for node in survivors {
        node.shutdown().await.expect("shutdown");
    }
    for path in directories {
        let _ = std::fs::remove_dir_all(path);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_minority_cannot_commit_a_new_command() {
    init_tracing();
    let ports = [free_port(), free_port(), free_port()];
    let directories: Vec<PathBuf> = (1..=3)
        .map(|index| directory(&format!("minority{index}")))
        .collect();
    let nodes = start_group(ports, &directories).await;
    let leader = wait_for_leader(&nodes, Duration::from_secs(20))
        .await
        .expect("a leader must be elected");
    let leader_index = (leader - 1) as usize;

    // Two of three voters go away: the remaining voter loses quorum.
    let stopped = [leader_index, (leader_index + 1) % 3];
    let mut kept = Vec::new();
    for (index, node) in nodes.into_iter().enumerate() {
        if stopped.contains(&index) {
            node.shutdown().await.expect("shutdown");
        } else {
            kept.push(node);
        }
    }
    // The surviving voter cannot commit without a majority.
    let proposal = kept[0].propose(node_command(42));
    let result = tokio::time::timeout(Duration::from_secs(3), proposal).await;
    match result {
        Err(_elapsed) => {}
        Ok(Ok(response)) => panic!("a minority must not commit: {response:?}"),
        Ok(Err(error)) => assert!(
            matches!(
                error,
                rovqet_metadata_raft::MetadataError::Raft(_)
                    | rovqet_metadata_raft::MetadataError::Network(_)
            ),
            "unexpected error {error:?}"
        ),
    }
    assert!(!kept[0].applied_state().nodes.contains_key(&42));
    for node in kept {
        node.shutdown().await.expect("shutdown");
    }
    for path in directories {
        let _ = std::fs::remove_dir_all(path);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_restarted_follower_catches_up_on_the_committed_log() {
    init_tracing();
    let ports = [free_port(), free_port(), free_port()];
    let directories: Vec<PathBuf> = (1..=3)
        .map(|index| directory(&format!("catchup{index}")))
        .collect();
    let nodes = start_group(ports, &directories).await;
    let leader = wait_for_leader(&nodes, Duration::from_secs(20))
        .await
        .expect("a leader must be elected");
    let leader_node = nodes
        .iter()
        .find(|node| node.id() == leader)
        .expect("leader node");
    leader_node
        .propose(node_command(9))
        .await
        .expect("committed command");
    assert!(wait_for(Duration::from_secs(20), || converged(&nodes, 9)).await);

    // One follower is stopped while two further commands commit.
    let follower = (1..=3).find(|id| *id != leader).expect("a follower exists");
    let mut running = Vec::new();
    for node in nodes {
        if node.id() == follower {
            node.shutdown().await.expect("shutdown follower");
        } else {
            running.push(node);
        }
    }
    let leader_node = running
        .iter()
        .find(|node| node.id() == leader)
        .expect("leader node");
    leader_node
        .propose(node_command(10))
        .await
        .expect("committed command");
    leader_node
        .propose(node_command(11))
        .await
        .expect("committed command");

    // The follower restarts from its own storage and must catch up.
    let restarted = MetadataNode::start(config(
        follower,
        ports,
        directories[(follower - 1) as usize].clone(),
        false,
    ))
    .await
    .expect("restarted follower");
    running.push(restarted);
    let caught_up = wait_for(Duration::from_secs(30), || async {
        let state = running
            .iter()
            .find(|node| node.id() == follower)
            .expect("restarted follower")
            .applied_state();
        state.nodes.contains_key(&10) && state.nodes.contains_key(&11)
    })
    .await;
    assert!(caught_up, "a restarted follower must catch up");

    for node in running {
        node.shutdown().await.expect("shutdown");
    }
    for path in directories {
        let _ = std::fs::remove_dir_all(path);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn all_three_nodes_restart_with_their_original_storage() {
    init_tracing();
    let ports = [free_port(), free_port(), free_port()];
    let directories: Vec<PathBuf> = (1..=3)
        .map(|index| directory(&format!("allrestart{index}")))
        .collect();
    let nodes = start_group(ports, &directories).await;
    let leader = wait_for_leader(&nodes, Duration::from_secs(20))
        .await
        .expect("a leader must be elected");
    let leader_node = nodes
        .iter()
        .find(|node| node.id() == leader)
        .expect("leader node");
    leader_node
        .propose(node_command(9))
        .await
        .expect("committed command");
    assert!(wait_for(Duration::from_secs(20), || converged(&nodes, 9)).await);
    let revision_before = nodes[0].applied_state().revision;

    for node in nodes {
        node.shutdown().await.expect("shutdown");
    }

    // The whole group restarts from the same directories: no cluster is recreated.
    let mut restarted = Vec::new();
    for index in 0..3 {
        restarted.push(
            MetadataNode::start(config(
                index as u64 + 1,
                ports,
                directories[index].clone(),
                false,
            ))
            .await
            .expect("restarted node"),
        );
    }
    let recovered = wait_for(Duration::from_secs(20), || async {
        restarted.iter().all(|node| {
            let state = node.applied_state();
            state.nodes.contains_key(&9) && state.revision >= revision_before
        })
    })
    .await;
    assert!(
        recovered,
        "every restarted voter must recover its committed metadata"
    );
    let leader = wait_for_leader(&restarted, Duration::from_secs(30))
        .await
        .expect("the restarted group must elect a leader");
    let leader_node = restarted
        .iter()
        .find(|node| node.id() == leader)
        .expect("leader node");
    leader_node
        .propose(node_command(12))
        .await
        .expect("a restarted group must accept a new committed command");
    assert!(wait_for(Duration::from_secs(20), || converged(&restarted, 12)).await);

    for node in restarted {
        node.shutdown().await.expect("shutdown");
    }
    for path in directories {
        let _ = std::fs::remove_dir_all(path);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn revisions_are_monotonic_and_ownership_epochs_never_regress() {
    init_tracing();
    let ports = [free_port(), free_port(), free_port()];
    let directories: Vec<PathBuf> = (1..=3)
        .map(|index| directory(&format!("epoch{index}")))
        .collect();
    let nodes = start_group(ports, &directories).await;
    let leader = wait_for_leader(&nodes, Duration::from_secs(20))
        .await
        .expect("a leader must be elected");
    let leader_node = nodes
        .iter()
        .find(|node| node.id() == leader)
        .expect("leader node");

    leader_node
        .propose(node_command(1))
        .await
        .expect("owner node");
    let first = leader_node
        .propose(MetadataCommand::SetPlacement {
            tenant_id: 1,
            topic_id: 3,
            partition_id: 0,
            owner: 1,
            ownership_epoch: 3,
            lease_valid_for_ms: 60_000,
        })
        .await
        .expect("committed placement");
    let MetadataCommandResponse::Applied {
        revision: after_first,
    } = first
    else {
        panic!("a valid placement must be applied: {first:?}");
    };

    // An older epoch for the same partition is rejected deterministically, by every voter.
    let stale = leader_node
        .propose(MetadataCommand::SetPlacement {
            tenant_id: 1,
            topic_id: 3,
            partition_id: 0,
            owner: 1,
            ownership_epoch: 2,
            lease_valid_for_ms: 60_000,
        })
        .await
        .expect("deterministic rejection");
    assert!(
        matches!(stale, MetadataCommandResponse::Rejected { .. }),
        "an ownership epoch must never regress: {stale:?}"
    );

    // A newer epoch is applied and the revision keeps increasing.
    let newer = leader_node
        .propose(MetadataCommand::SetPlacement {
            tenant_id: 1,
            topic_id: 3,
            partition_id: 0,
            owner: 1,
            ownership_epoch: 4,
            lease_valid_for_ms: 60_000,
        })
        .await
        .expect("committed placement");
    let MetadataCommandResponse::Applied {
        revision: after_newer,
    } = newer
    else {
        panic!("a newer epoch must be applied: {newer:?}");
    };
    assert!(
        after_newer > after_first,
        "revisions must increase monotonically"
    );
    assert!(
        wait_for(Duration::from_secs(20), || async {
            nodes.iter().all(|node| {
                let state = node.applied_state();
                state.revision >= after_newer
                    && state
                        .placement(1, 3, 0)
                        .map(|placement| placement.ownership_epoch)
                        == Some(4)
            })
        })
        .await
    );

    for node in nodes {
        node.shutdown().await.expect("shutdown");
    }
    for path in directories {
        let _ = std::fs::remove_dir_all(path);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_lagging_follower_catches_up_after_log_compaction() {
    init_tracing();
    let ports = [free_port(), free_port(), free_port()];
    let directories: Vec<PathBuf> = (1..=3)
        .map(|index| directory(&format!("snapshot{index}")))
        .collect();
    let nodes = start_group(ports, &directories).await;
    let leader = wait_for_leader(&nodes, Duration::from_secs(20))
        .await
        .expect("a leader must be elected");
    let leader_node = nodes
        .iter()
        .find(|node| node.id() == leader)
        .expect("leader node");
    leader_node
        .propose(node_command(9))
        .await
        .expect("committed command");
    assert!(wait_for(Duration::from_secs(20), || converged(&nodes, 9)).await);

    // A follower is stopped while the leader compacts its log through snapshots.
    let follower = (1..=3).find(|id| *id != leader).expect("a follower exists");
    let mut running = Vec::new();
    for node in nodes {
        if node.id() == follower {
            node.shutdown().await.expect("shutdown follower");
        } else {
            running.push(node);
        }
    }
    let leader_node = running
        .iter()
        .find(|node| node.id() == leader)
        .expect("leader node");
    for node in 20..26u64 {
        leader_node
            .propose(node_command(node))
            .await
            .expect("committed command");
    }
    assert!(
        wait_for(Duration::from_secs(30), || async {
            running.iter().all(|node| {
                let state = node.applied_state();
                (20..26).all(|node| state.nodes.contains_key(&node))
            })
        })
        .await
    );

    // The follower returns far behind a compacted log: only a snapshot can bring it back.
    let restarted = MetadataNode::start(config(
        follower,
        ports,
        directories[(follower - 1) as usize].clone(),
        false,
    ))
    .await
    .expect("restarted follower");
    running.push(restarted);
    let caught_up = wait_for(Duration::from_secs(40), || async {
        let state = running
            .iter()
            .find(|node| node.id() == follower)
            .expect("restarted follower")
            .applied_state();
        (20..26).all(|node| state.nodes.contains_key(&node))
    })
    .await;
    assert!(
        caught_up,
        "a follower behind a compacted log must install a snapshot"
    );

    for node in running {
        node.shutdown().await.expect("shutdown");
    }
    for path in directories {
        let _ = std::fs::remove_dir_all(path);
    }
}
