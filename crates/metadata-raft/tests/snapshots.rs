//! Durable metadata snapshots: publication, validation and rejection of bad images.
use openraft::storage::{RaftSnapshotBuilder, RaftStateMachine};
use openraft::{Entry, EntryPayload, LeaderId, LogId};
use rovqet_metadata_raft::sm::{MetadataState, MetadataStateMachine};
use rovqet_metadata_raft::types::{MetadataCommand, MetadataRaftTypeConfig};

const CLUSTER: u128 = 4242;
const OTHER_CLUSTER: u128 = 4343;

fn entry(index: u64, command: MetadataCommand) -> Entry<MetadataRaftTypeConfig> {
    Entry {
        log_id: LogId::new(LeaderId::new(2, 1), index),
        payload: EntryPayload::Normal(command),
    }
}

async fn machine_with_state(directory: &std::path::Path, cluster_id: u128) -> MetadataStateMachine {
    let mut machine = MetadataStateMachine::open(directory, cluster_id).expect("open machine");
    machine
        .apply(vec![
            entry(
                0,
                MetadataCommand::SetNode {
                    node: 1,
                    address: "127.0.0.1:1".into(),
                },
            ),
            entry(
                1,
                MetadataCommand::SetPlacement {
                    tenant_id: 1,
                    topic_id: 3,
                    partition_id: 0,
                    owner: 1,
                    ownership_epoch: 7,
                    lease_valid_for_ms: 60_000,
                },
            ),
        ])
        .await
        .expect("apply");
    machine
}

#[tokio::test]
async fn a_snapshot_round_trips_into_a_second_state_machine() {
    let source = tempfile::tempdir().expect("temp dir");
    let target = tempfile::tempdir().expect("temp dir");
    let mut machine = machine_with_state(source.path(), CLUSTER).await;

    let mut builder = machine.get_snapshot_builder().await;
    let snapshot = builder.build_snapshot().await.expect("build snapshot");
    let bytes = snapshot.snapshot.get_ref().clone();

    let mut other = MetadataStateMachine::open(target.path(), CLUSTER).expect("open machine");
    other
        .install_snapshot(&snapshot.meta, Box::new(std::io::Cursor::new(bytes)))
        .await
        .expect("install snapshot");

    let state: &MetadataState = other.state();
    assert_eq!(
        state.revision, 2,
        "the snapshot carries the applied revision"
    );
    assert!(state.nodes.contains_key(&1));
    let placement = state.placement(1, 3, 0).expect("placement");
    assert_eq!(placement.ownership_epoch, 7);
    assert_eq!(placement.lease_valid_for_ms, 60_000);
    assert_eq!(state.cluster_id, CLUSTER);
}

#[tokio::test]
async fn a_corrupted_snapshot_is_rejected() {
    let source = tempfile::tempdir().expect("temp dir");
    let target = tempfile::tempdir().expect("temp dir");
    let mut machine = machine_with_state(source.path(), CLUSTER).await;
    let mut builder = machine.get_snapshot_builder().await;
    let snapshot = builder.build_snapshot().await.expect("build snapshot");
    let mut bytes = snapshot.snapshot.get_ref().clone();
    // Corrupt the payload of the image.
    let last = bytes.len() - 1;
    bytes[last] ^= 0xff;

    let mut other = MetadataStateMachine::open(target.path(), CLUSTER).expect("open machine");
    let rejected = other
        .install_snapshot(&snapshot.meta, Box::new(std::io::Cursor::new(bytes)))
        .await;
    assert!(
        rejected.is_err(),
        "a corrupted snapshot must never be installed"
    );
    assert_eq!(
        other.state().revision,
        0,
        "a rejected snapshot must not change the applied state"
    );
}

#[tokio::test]
async fn a_snapshot_of_another_cluster_is_rejected() {
    let source = tempfile::tempdir().expect("temp dir");
    let target = tempfile::tempdir().expect("temp dir");
    let mut machine = machine_with_state(source.path(), OTHER_CLUSTER).await;
    let mut builder = machine.get_snapshot_builder().await;
    let snapshot = builder.build_snapshot().await.expect("build snapshot");
    let bytes = snapshot.snapshot.get_ref().clone();

    let mut other = MetadataStateMachine::open(target.path(), CLUSTER).expect("open machine");
    let rejected = other
        .install_snapshot(&snapshot.meta, Box::new(std::io::Cursor::new(bytes)))
        .await;
    assert!(
        rejected.is_err(),
        "a snapshot of another cluster must never be installed"
    );
}

#[tokio::test]
async fn an_interrupted_publication_keeps_the_previous_snapshot() {
    let directory = tempfile::tempdir().expect("temp dir");
    let mut machine = machine_with_state(directory.path(), CLUSTER).await;
    let mut builder = machine.get_snapshot_builder().await;
    let published = builder.build_snapshot().await.expect("build snapshot");
    let expected = published.snapshot.get_ref().clone();

    // An interrupted publication leaves a temporary file behind.
    std::fs::write(directory.path().join("snapshot.bin.tmp"), b"partial").expect("write tmp");
    std::fs::write(directory.path().join("snapshot-meta.json.tmp"), b"{").expect("write tmp");

    let mut reopened = MetadataStateMachine::open(directory.path(), CLUSTER).expect("open machine");
    let current = reopened
        .get_current_snapshot()
        .await
        .expect("current snapshot")
        .expect("a published snapshot must survive an interrupted publication");
    assert_eq!(current.snapshot.get_ref(), &expected);
}

#[tokio::test]
async fn a_corrupted_published_snapshot_is_never_served() {
    let directory = tempfile::tempdir().expect("temp dir");
    let mut machine = machine_with_state(directory.path(), CLUSTER).await;
    let mut builder = machine.get_snapshot_builder().await;
    builder.build_snapshot().await.expect("build snapshot");

    let path = directory.path().join("snapshot.bin");
    let mut bytes = std::fs::read(&path).expect("read snapshot");
    let last = bytes.len() - 1;
    bytes[last] ^= 0xff;
    std::fs::write(&path, &bytes).expect("write snapshot");

    let mut reopened = MetadataStateMachine::open(directory.path(), CLUSTER).expect("open machine");
    let served = reopened.get_current_snapshot().await;
    assert!(
        served.is_err(),
        "a corrupted published snapshot must fail closed"
    );
}

#[tokio::test]
async fn a_restarted_state_machine_keeps_the_applied_metadata() {
    let directory = tempfile::tempdir().expect("temp dir");
    let revision = {
        let machine = machine_with_state(directory.path(), CLUSTER).await;
        machine.state().revision
    };
    let reopened = MetadataStateMachine::open(directory.path(), CLUSTER).expect("open machine");
    assert_eq!(
        reopened.state().revision,
        revision,
        "a restarted state machine must recover the applied metadata"
    );
    assert_eq!(reopened.state().cluster_id, CLUSTER);
}

#[tokio::test]
async fn a_state_machine_of_another_cluster_is_refused() {
    let directory = tempfile::tempdir().expect("temp dir");
    let _machine = machine_with_state(directory.path(), CLUSTER).await;
    let refused = MetadataStateMachine::open(directory.path(), OTHER_CLUSTER);
    assert!(
        refused.is_err(),
        "storage of another cluster identity must be refused"
    );
}
