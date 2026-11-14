//! The data-plane view of committed metadata: only a committed and applied snapshot may
//! become authoritative, and an expired lease must fail closed.
use rovqet_metadata_raft::types::MetadataCommand;
use rovqet_metadata_raft::{MetadataNode, MetadataNodeConfig};
use std::net::TcpListener;
use std::time::Duration;

const CLUSTER: u128 = 770077;

fn free_port() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").expect("port");
    let port = listener.local_addr().expect("addr").port();
    drop(listener);
    port
}

fn directory() -> std::path::PathBuf {
    let path = std::env::temp_dir().join(format!(
        "rovqet-metadata-authority-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|value| value.as_nanos())
            .unwrap_or(0)
    ));
    std::fs::create_dir_all(&path).expect("directory");
    path
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn only_committed_metadata_becomes_authoritative() {
    let port = free_port();
    let path = directory();
    let node = MetadataNode::start(MetadataNodeConfig {
        id: 1,
        cluster_id: CLUSTER,
        listen: format!("127.0.0.1:{port}").parse().expect("addr"),
        peers: vec![(1, format!("127.0.0.1:{port}").parse().expect("addr"))],
        data_directory: path.clone(),
        bootstrap: true,
        election_timeout_ms: 300,
        heartbeat_interval_ms: 80,
        max_payload_entries: 64,
        snapshot_logs_since_last: 8,
    })
    .await
    .expect("metadata node");

    // Before the first committed command there is no authoritative metadata at all.
    assert!(
        node.committed_snapshot().is_none(),
        "an empty metadata group must not publish a snapshot"
    );
    let store = rovqet_control::MetadataStore::new(
        rovqet_control::ClusterId::new(CLUSTER).expect("cluster id"),
    );
    assert!(
        store.current().is_none(),
        "the data plane must start without any authority"
    );

    // A proposal is only accepted by the leader: wait for the election.
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

    let snapshot = node
        .committed_snapshot()
        .expect("the committed metadata must publish a snapshot");
    let installed = store
        .install(snapshot)
        .expect("the committed snapshot installs");
    assert_eq!(installed.revision.get(), 2);
    let placement = installed
        .partitions
        .iter()
        .find(|placement| placement.partition.partition_id == 0)
        .expect("the committed placement");
    assert_eq!(placement.owner.get(), 1);
    assert_eq!(placement.ownership_epoch.get(), 7);
    assert_eq!(placement.lease.valid_for_ms, 60_000);
    assert!(
        !store.authority_expired(placement),
        "a fresh committed authority must not be expired"
    );

    // An older snapshot must never replace a newer committed one.
    let stale = node.committed_snapshot().expect("snapshot");
    assert!(
        store.install(stale).is_err(),
        "an equal or older revision must be rejected"
    );

    // A short lease expires and the data plane must fail closed.
    node.propose(MetadataCommand::SetPlacement {
        tenant_id: 1,
        topic_id: 3,
        partition_id: 0,
        owner: 1,
        ownership_epoch: 8,
        lease_valid_for_ms: 1,
    })
    .await
    .expect("committed placement");
    let short = node.committed_snapshot().expect("snapshot");
    let installed = store.install(short).expect("the newer snapshot installs");
    let placement = installed
        .partitions
        .iter()
        .find(|placement| placement.partition.partition_id == 0)
        .expect("the committed placement");
    assert_eq!(placement.ownership_epoch.get(), 8);
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(
        store.authority_expired(placement),
        "an expired lease must fail closed"
    );

    node.shutdown().await.expect("shutdown");
    let _ = std::fs::remove_dir_all(path);
}
