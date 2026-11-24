//! In-process three-voter integration test of the Q2 Raft adapter.
//!
//! The test drives the real node lifecycle: three voters on free loopback ports with
//! separate data directories, TCP transport, a leader change, a restart and a log
//! compaction that forces a snapshot transfer. Every wait is bounded.
use rovqet_consensus::sm::ShardState;
use rovqet_consensus::types::{
    ShardCommand, ShardCommandResponse, ShardNodeId, ShardPayload, ShardPublish, ShardRequest,
};
use rovqet_consensus::{ShardAuthority, ShardError, ShardNode, ShardNodeConfig};
use rovqet_core::PartitionRef;
use std::collections::BTreeMap;
use std::net::{SocketAddr, TcpListener};
use std::time::{Duration, Instant};

const CLUSTER_ID: u128 = 7;
const SHARD_ID: u64 = 3;
const EPOCH: u64 = 1;

/// A per-partition view of the committed state, comparable across nodes.
type PartitionView = BTreeMap<String, (u64, String, String)>;

fn free_address() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind a free loopback port");
    listener.local_addr().expect("a bound address")
}

fn request(request_id: u128, partition_id: u32) -> ShardRequest {
    ShardRequest {
        request_id,
        partition: PartitionRef {
            tenant_id: 1,
            topic_id: 9,
            partition_id,
        },
        payload: ShardPayload::Inline(vec![request_id as u8; 4]),
        metadata_epoch: EPOCH,
    }
}

fn command(request_id: u128, partition_id: u32) -> ShardCommand {
    ShardCommand::Request(request(request_id, partition_id))
}

/// `sha256("abc")`, the published FIPS 180-4 test vector, so the integration test
/// carries a well-formed publish digest without pulling in a hash dependency.
const ABC_DIGEST: [u8; 32] = [
    0xba, 0x78, 0x16, 0xbf, 0x8f, 0x01, 0xcf, 0xea, 0x41, 0x41, 0x40, 0xde, 0x5d, 0xae, 0x22, 0x23,
    0xb0, 0x03, 0x61, 0xa3, 0x96, 0x17, 0x7a, 0x9c, 0xb4, 0x10, 0xff, 0x61, 0xf2, 0x00, 0x15, 0xad,
];

/// A Q2 publication of `("abc", message_id)` for one partition, sequence 0 of a fresh
/// producer.
fn publish(partition_id: u32, message_id: u128) -> ShardCommand {
    ShardCommand::Publish(ShardPublish {
        partition: PartitionRef {
            tenant_id: 1,
            topic_id: 9,
            partition_id,
        },
        durable_class: 2,
        incarnation: 1,
        metadata_epoch: EPOCH,
        principal: 1,
        producer_id: 7,
        producer_epoch: 1,
        sequence: 0,
        message_id,
        payload: b"abc".to_vec(),
        payload_digest: ABC_DIGEST,
    })
}

fn hex(bytes: &[u8]) -> String {
    let mut rendered = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        rendered.push_str(&format!("{byte:02x}"));
    }
    rendered
}

fn partition_view(state: &ShardState) -> PartitionView {
    state
        .partitions
        .iter()
        .map(|(partition, partition_state)| {
            (
                partition.to_string(),
                (
                    partition_state.applied_count,
                    partition_state.last_request_id.to_string(),
                    hex(&partition_state.chain_digest),
                ),
            )
        })
        .collect()
}

fn applied_fields(response: &ShardCommandResponse) -> (u64, u64, u32) {
    match response {
        ShardCommandResponse::Applied {
            index,
            applied_count,
            partition,
            ..
        } => (*index, *applied_count, partition.partition_id),
        other => panic!("expected an applied response, got {other:?}"),
    }
}

struct VoterSet {
    configs: Vec<ShardNodeConfig>,
    nodes: Vec<Option<ShardNode>>,
    // Keeps the temporary directories alive for the whole test.
    _directories: Vec<tempfile::TempDir>,
}

impl VoterSet {
    fn new(size: usize) -> Self {
        let addresses: Vec<SocketAddr> = (0..size).map(|_| free_address()).collect();
        let directories: Vec<tempfile::TempDir> = (0..size)
            .map(|_| tempfile::tempdir().expect("a temporary directory"))
            .collect();
        let configs = (0..size)
            .map(|id| ShardNodeConfig {
                id: id as ShardNodeId,
                cluster_id: CLUSTER_ID,
                shard_id: SHARD_ID,
                listen: addresses[id],
                peers: (0..size)
                    .filter(|other| *other != id)
                    .map(|other| (other as ShardNodeId, addresses[other]))
                    .collect(),
                data_directory: directories[id].path().to_path_buf(),
                bootstrap: id == 0,
                election_timeout_ms: 300,
                heartbeat_interval_ms: 60,
                max_payload_entries: 64,
                snapshot_logs_since_last: 4,
            })
            .collect();
        Self {
            configs,
            nodes: (0..size).map(|_| None).collect(),
            _directories: directories,
        }
    }

    fn live(&self) -> Vec<usize> {
        self.nodes
            .iter()
            .enumerate()
            .filter(|(_, node)| node.is_some())
            .map(|(id, _)| id)
            .collect()
    }

    fn node(&self, id: usize) -> &ShardNode {
        self.nodes[id]
            .as_ref()
            .unwrap_or_else(|| panic!("voter {id} is not running"))
    }

    async fn start(&mut self, id: usize) {
        let node = ShardNode::start(self.configs[id].clone())
            .await
            .unwrap_or_else(|error| panic!("voter {id} failed to start: {error}"));
        self.nodes[id] = Some(node);
    }

    /// Restarts a voter from its original data directory. The port is released during
    /// shutdown, but the bind is retried briefly so the restart is not racy.
    async fn restart(&mut self, id: usize) {
        let mut config = self.configs[id].clone();
        config.bootstrap = false;
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            match ShardNode::start(config.clone()).await {
                Ok(node) => {
                    self.nodes[id] = Some(node);
                    return;
                }
                Err(error) => {
                    if Instant::now() > deadline {
                        panic!("voter {id} failed to restart: {error}");
                    }
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
            }
        }
    }

    async fn shutdown(&mut self, id: usize) {
        if let Some(node) = self.nodes[id].take() {
            node.shutdown()
                .await
                .unwrap_or_else(|error| panic!("voter {id} failed to stop: {error}"));
        }
    }

    /// Any live node that knows a leader, as `(leader, term)`.
    async fn leader_view(&self) -> Option<(ShardNodeId, u64)> {
        for id in self.live() {
            if let Some(leader) = self.node(id).leader_with_term().await {
                return Some(leader);
            }
        }
        None
    }

    /// Waits for a leader other than `exclude`.
    async fn wait_for_leader(&self, exclude: Option<(ShardNodeId, u64)>) -> (ShardNodeId, u64) {
        let deadline = Instant::now() + Duration::from_secs(20);
        while Instant::now() < deadline {
            if let Some(leader) = self.leader_view().await {
                if Some(leader) != exclude {
                    return leader;
                }
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        panic!("no leader other than {exclude:?} was elected within 20s");
    }

    /// Waits until a voter's published Raft metrics show it applied every entry of its
    /// own log. The two metric reads are taken applied-first so a log entry appended
    /// in between can only make the condition harder to reach, never falsely pass.
    ///
    /// Both `last_applied` and `last_log_index` are published by the Raft core only
    /// after it processes the state machine's result, while the state machine (and the
    /// revision watch) advances first. Under load that publication lags, so a one-shot
    /// read can catch a value that is still catching up.
    async fn wait_for_log_caught_up(&self, id: usize) {
        let deadline = Instant::now() + Duration::from_secs(20);
        loop {
            let applied = self.node(id).last_applied_index().await;
            let log_index = self.node(id).last_log_index().await;
            match (applied, log_index) {
                (Some(applied), Some(log_index)) if applied >= log_index => return,
                (None, None) => return,
                _ => {}
            }
            if Instant::now() >= deadline {
                panic!(
                    "voter {id} did not apply its whole log within 20s, \
                     observed applied={applied:?} log={log_index:?}"
                );
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }

    /// Waits until one voter's published `last_applied` reaches `min_index`.
    ///
    /// The revision watch reports the applied state before the Raft core publishes
    /// `last_applied`, so the published index must be waited for rather than read once.
    async fn wait_for_applied_index(&self, id: usize, min_index: u64) -> u64 {
        let deadline = Instant::now() + Duration::from_secs(20);
        loop {
            if let Some(applied) = self.node(id).last_applied_index().await {
                if applied >= min_index {
                    return applied;
                }
            }
            if Instant::now() >= deadline {
                panic!(
                    "voter {id} did not reach applied index {min_index} within 20s, \
                     observed {:?}",
                    self.node(id).last_applied_index().await
                );
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }

    /// Waits until one voter applied `revision` or more.
    async fn wait_for_revision(&self, id: usize, revision: u64) {
        let mut updates = self.node(id).applied_updates();
        let outcome = tokio::time::timeout(
            Duration::from_secs(20),
            updates.wait_for(|current| *current >= revision),
        )
        .await;
        match outcome {
            Ok(Ok(_)) => {}
            Ok(Err(_)) => panic!("the applied watch of voter {id} closed"),
            Err(_) => panic!(
                "voter {id} did not reach revision {revision} within 20s, observed {}",
                self.node(id).applied_state().revision
            ),
        }
    }
}

/// Proposes through `node` until the command is committed. A transient forwarding error
/// is retried; the state machine applies a repeated request id idempotently.
async fn propose(node: &ShardNode, command: ShardCommand) -> ShardCommandResponse {
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        match node.propose(command.clone()).await {
            Ok(response) => return response,
            Err(ShardError::Rejected(reason)) => panic!("the proposal was rejected: {reason}"),
            Err(error) => {
                if Instant::now() > deadline {
                    panic!("the proposal did not commit within 20s: {error}");
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_three_voter_shard_group_replicates_and_recovers() {
    let mut cluster = VoterSet::new(3);
    cluster.start(0).await;
    cluster.start(1).await;
    cluster.start(2).await;

    // 1. Bootstrap and elect.
    let (leader, term) = cluster.wait_for_leader(None).await;
    assert!(term >= 1, "an elected leader has a term, observed {term}");
    assert!(
        cluster.live().iter().any(|id| *id as u64 == leader),
        "the leader must be a live voter"
    );
    for id in cluster.live() {
        cluster.node(id).install_authority(Some(ShardAuthority {
            owner: leader,
            epoch: EPOCH,
        }));
    }

    // A stale metadata epoch is refused before it creates a Raft entry. The applied
    // index is read from the published metrics, so first wait until the leader has
    // applied its whole log: the election's own entries must not land between the two
    // reads below and masquerade as an entry created by the refusal.
    let leader_id = leader as usize;
    cluster.wait_for_log_caught_up(leader_id).await;
    let applied_before = cluster.node(leader_id).last_applied_index().await;
    let refusal = cluster
        .node(leader_id)
        .propose(ShardCommand::Request(ShardRequest {
            metadata_epoch: EPOCH + 1,
            ..request(0, 0)
        }))
        .await
        .expect_err("a foreign metadata epoch must be refused");
    assert!(
        matches!(refusal, ShardError::Rejected(_)),
        "a foreign epoch must be rejected, observed {refusal:?}"
    );
    assert_eq!(
        cluster.node(leader_id).last_applied_index().await,
        applied_before,
        "a refused proposal must not create a Raft entry"
    );

    // Three commands over two partitions, submitted through a follower so the
    // forward-to-leader path is exercised.
    let submitter = cluster
        .live()
        .into_iter()
        .find(|id| *id as u64 != leader)
        .expect("a follower");
    let first = propose(cluster.node(submitter), command(1, 0)).await;
    let second = propose(cluster.node(submitter), command(2, 0)).await;
    let third = propose(cluster.node(submitter), command(3, 1)).await;

    let first_fields = applied_fields(&first);
    let second_fields = applied_fields(&second);
    let third_fields = applied_fields(&third);
    assert!(
        first_fields.0 < second_fields.0 && second_fields.0 < third_fields.0,
        "committed indexes must increase: {first_fields:?} {second_fields:?} {third_fields:?}"
    );
    assert_eq!(
        (first_fields.1, first_fields.2),
        (1, 0),
        "the first command of a partition is its first applied command"
    );
    assert_eq!((second_fields.1, second_fields.2), (2, 0));
    assert_eq!((third_fields.1, third_fields.2), (1, 1));

    for id in cluster.live() {
        cluster.wait_for_revision(id, 3).await;
    }
    let committed = partition_view(&cluster.node(0).applied_state());
    for id in cluster.live() {
        let state = cluster.node(id).applied_state();
        assert_eq!(state.revision, 3, "voter {id} applied another prefix");
        assert_eq!(
            partition_view(&state),
            committed,
            "voter {id} applied another chain"
        );
    }
    assert_eq!(committed.len(), 2, "two partitions were touched");

    // 2. Lose the leader: the surviving majority elects another one.
    cluster.shutdown(leader_id).await;
    let (new_leader, new_term) = cluster.wait_for_leader(Some((leader, term))).await;
    assert_ne!(new_leader, leader, "a new leader must be elected");
    assert!(new_term >= term, "terms never move backwards");
    for id in cluster.live() {
        cluster.node(id).install_authority(Some(ShardAuthority {
            owner: new_leader,
            epoch: EPOCH,
        }));
    }

    let fourth = propose(cluster.node(new_leader as usize), command(4, 2)).await;
    let fourth_fields = applied_fields(&fourth);
    assert_eq!(
        (fourth_fields.1, fourth_fields.2),
        (1, 2),
        "the fourth command opens the third partition"
    );
    assert!(
        fourth_fields.0 > third_fields.0,
        "the new term commits a later index"
    );

    for id in cluster.live() {
        cluster.wait_for_revision(id, 4).await;
        let state = cluster.node(id).applied_state();
        assert_eq!(state.revision, 4);
        // The committed prefix is intact: the first two partitions are untouched.
        let view = partition_view(&state);
        for (partition, entry) in &committed {
            assert_eq!(
                view.get(partition),
                Some(entry),
                "voter {id} lost a committed partition of the prefix"
            );
        }
        assert_eq!(
            view.get("1:9:2").map(|entry| entry.0),
            Some(1),
            "voter {id} did not apply the fourth command"
        );
    }
    let committed_after_four = partition_view(&cluster.node(new_leader as usize).applied_state());

    // 3. The stopped voter restarts from its original directory and catches up.
    cluster.restart(leader_id).await;
    cluster
        .node(leader_id)
        .install_authority(Some(ShardAuthority {
            owner: new_leader,
            epoch: EPOCH,
        }));
    cluster.wait_for_revision(leader_id, 4).await;
    assert_eq!(
        partition_view(&cluster.node(leader_id).applied_state()),
        committed_after_four,
        "the restarted voter did not catch up to the committed prefix"
    );

    // 4. Compact the leader's log while one voter is down, then bring it back: the
    // entries it still needs are gone, so it can only recover through a snapshot.
    let stopped = cluster
        .live()
        .into_iter()
        .find(|id| *id as u64 != new_leader)
        .expect("a follower to stop");
    // Read the follower's applied index from the published metrics only after it has
    // applied its whole log; a stale-low value here would let the leader pass the
    // compaction check without really purging what the stopped voter still needs.
    cluster.wait_for_log_caught_up(stopped).await;
    let stopped_index = cluster
        .node(stopped)
        .last_applied_index()
        .await
        .expect("the follower applied a committed entry");
    cluster.shutdown(stopped).await;

    let leader_node = cluster.node(new_leader as usize);
    leader_node
        .trigger_snapshot()
        .await
        .expect("force a snapshot");
    // Enough committed commands for the snapshot policy to build and compact again.
    let mut last_index = fourth_fields.0;
    for request_id in 5..=10 {
        let response = propose(leader_node, command(request_id, 2)).await;
        last_index = applied_fields(&response).0;
    }
    leader_node
        .trigger_snapshot()
        .await
        .expect("force a snapshot");

    let deadline = Instant::now() + Duration::from_secs(20);
    let mut purged = None;
    let mut snapshot = None;
    while Instant::now() < deadline {
        snapshot = leader_node.last_snapshot().await;
        purged = leader_node.purged_index().await;
        if let (Some(snapshot), Some(purged)) = (snapshot, purged) {
            if purged > stopped_index && snapshot.0 >= last_index {
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let purged = purged.unwrap_or_else(|| {
        panic!(
            "the leader did not compact its log after the snapshot, observed snapshot={snapshot:?}"
        )
    });
    assert!(
        purged > stopped_index,
        "the leader must purge the entries the stopped voter still needs, \
         observed purged={purged} stopped_index={stopped_index}"
    );

    let expected_revision = cluster.node(new_leader as usize).applied_state().revision;
    assert_eq!(
        expected_revision, 10,
        "three plus one plus six committed commands"
    );
    let committed_final = partition_view(&cluster.node(new_leader as usize).applied_state());

    cluster.restart(stopped).await;
    cluster
        .node(stopped)
        .install_authority(Some(ShardAuthority {
            owner: new_leader,
            epoch: EPOCH,
        }));
    cluster.wait_for_revision(stopped, expected_revision).await;
    let recovered = cluster.node(stopped).applied_state();
    assert_eq!(
        partition_view(&recovered),
        committed_final,
        "the voter recovered another chain than the committed one"
    );
    // The committed chain digests of the first commands survived the compaction.
    for (partition, entry) in &committed {
        assert_eq!(
            partition_view(&recovered).get(partition),
            Some(entry),
            "the recovered chain differs on a committed partition"
        );
    }
    // The revision watch reports the applied state before the Raft core publishes
    // `last_applied`, so wait for the published index instead of reading it once.
    let recovered_applied = cluster.wait_for_applied_index(stopped, last_index).await;
    assert!(
        recovered_applied >= last_index,
        "the recovered voter must reach the last committed index, observed {recovered_applied}"
    );

    // 5. A Q2 PUBLISH commits a logical offset that is not the Raft index and
    // converges on every live voter — through the leader change and the compaction
    // that happened above.
    let (publish_leader, _) = cluster.wait_for_leader(None).await;
    let before_publish = cluster
        .node(publish_leader as usize)
        .applied_state()
        .revision;
    let published = propose(cluster.node(publish_leader as usize), publish(3, 77)).await;
    match &published {
        ShardCommandResponse::Published {
            index,
            partition,
            logical_offset,
            message_id,
            duplicate,
        } => {
            assert!(
                *index > last_index,
                "the publication commits after the compacted prefix, observed index {index}"
            );
            assert_eq!(partition.partition_id, 3, "the publication partition");
            assert_eq!(
                *logical_offset, 0,
                "the first publication of a partition is logical offset 0, not the raft index {index}"
            );
            assert_eq!(*message_id, 77);
            assert!(!*duplicate, "the first publication is not a duplicate");
        }
        other => panic!("expected a published response, got {other:?}"),
    }

    for id in cluster.live() {
        cluster.wait_for_revision(id, before_publish + 1).await;
        let state = cluster.node(id).applied_state();
        let partition = state
            .partitions
            .get(&rovqet_consensus::sm::PartitionKey {
                tenant_id: 1,
                topic_id: 9,
                partition_id: 3,
            })
            .unwrap_or_else(|| panic!("voter {id} did not apply the publication"));
        assert_eq!(
            partition.next_logical_offset, 1,
            "voter {id} recorded exactly one offset"
        );
        assert_eq!(partition.messages.len(), 1, "voter {id} has one message");
        assert_eq!(partition.messages[0].logical_offset, 0);
        assert_eq!(
            partition.incarnation, 1,
            "voter {id} locked the incarnation"
        );
        assert_eq!(partition.applied_count, 1);
    }

    for id in cluster.live() {
        cluster.shutdown(id).await;
    }
}
