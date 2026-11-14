//! Metadata Raft node lifecycle: bootstrap, peer server and committed metadata access.
//!
//! The node exposes the applied metadata state so that the data plane can install a
//! committed snapshot; the control-plane model of `rovqet-control` is not changed here.
pub mod network;
pub mod sm;
pub mod storage;
pub mod types;

use crate::network::{RaftNetworkFactoryImpl, RaftRpc, RaftRpcResponse, read_rpc, write_rpc};
use crate::sm::{MetadataState, MetadataStateMachine};
use crate::storage::FileLogStorage;
use crate::types::{MetaNode, MetaNodeId, MetadataCommand, MetadataRaftTypeConfig};
use openraft::SnapshotPolicy;
use openraft::error::RaftError;
use openraft::raft::Raft;
use openraft::{Config, EntryPayload};
use std::collections::BTreeSet;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpListener;

/// Configuration of one metadata voter.
#[derive(Debug, Clone)]
pub struct MetadataNodeConfig {
    pub id: MetaNodeId,
    pub cluster_id: u128,
    /// Address this node listens on for peer RPCs.
    pub listen: SocketAddr,
    pub peers: Vec<(MetaNodeId, SocketAddr)>,
    pub data_directory: PathBuf,
    /// Only the designated bootstrap node initializes the membership.
    pub bootstrap: bool,
    pub election_timeout_ms: u64,
    pub heartbeat_interval_ms: u64,
    pub max_payload_entries: u64,
    /// Build a snapshot once this many entries were applied since the last one. A low value
    /// makes log compaction and snapshot installation observable in tests.
    pub snapshot_logs_since_last: u64,
}

/// Failures of the metadata node lifecycle.
#[derive(Debug)]
pub enum MetadataError {
    Storage(String),
    Raft(String),
    Network(String),
    NotLeader,
}

impl std::fmt::Display for MetadataError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for MetadataError {}

/// One running metadata voter.
pub struct MetadataNode {
    raft: Raft<MetadataRaftTypeConfig>,
    /// Notifies subscribers about committed metadata revisions.
    notify: tokio::sync::watch::Sender<u64>,
    state: Arc<std::sync::Mutex<MetadataState>>,
    /// The voters of the group, used to forward a proposal to the leader.
    peers: Vec<(MetaNodeId, SocketAddr)>,
    id: MetaNodeId,
    cluster_id: u128,
    shutdown: tokio::sync::watch::Sender<bool>,
    server: tokio::task::JoinHandle<()>,
}

impl MetadataNode {
    /// Starts a metadata voter: opens the durable storage, joins the group and serves peers.
    pub async fn start(config: MetadataNodeConfig) -> Result<Self, MetadataError> {
        let log = FileLogStorage::open(config.data_directory.join("raft"))
            .map_err(|error| MetadataError::Storage(format!("{error:?}")))?;
        let (machine, shared) = MetadataStateMachine::open_shared(
            config.data_directory.join("state"),
            config.cluster_id,
        )
        .map_err(|error| MetadataError::Storage(format!("{error:?}")))?;
        let notify = machine.notify();
        let network = RaftNetworkFactoryImpl::new(
            config.peers.clone(),
            Duration::from_millis(config.election_timeout_ms.max(500)),
        );
        let raft_config = Config {
            cluster_name: format!("rovqet-metadata-{}", config.cluster_id),
            election_timeout_min: config.election_timeout_ms.max(150),
            election_timeout_max: config.election_timeout_ms.max(150) * 2,
            heartbeat_interval: config.heartbeat_interval_ms.max(50),
            max_payload_entries: config.max_payload_entries.max(1),
            max_in_snapshot_log_to_keep: 1,
            snapshot_policy: SnapshotPolicy::LogsSinceLast(config.snapshot_logs_since_last.max(1)),
            ..Default::default()
        };
        let raft = Raft::new(config.id, Arc::new(raft_config), network, log, machine)
            .await
            .map_err(|error| MetadataError::Raft(format!("{error:?}")))?;

        // Explicit bootstrap: only the designated node initializes the membership and
        // only when the storage is still uninitialized.
        if config.bootstrap {
            let initialized = raft
                .is_initialized()
                .await
                .map_err(|error| MetadataError::Raft(format!("{error:?}")))?;
            if !initialized {
                let mut voters = BTreeSet::new();
                let mut nodes = std::collections::BTreeMap::new();
                for (id, address) in &config.peers {
                    voters.insert(*id);
                    nodes.insert(
                        *id,
                        MetaNode {
                            id: *id,
                            address: address.to_string(),
                        },
                    );
                }
                voters.insert(config.id);
                nodes.insert(
                    config.id,
                    MetaNode {
                        id: config.id,
                        address: config.listen.to_string(),
                    },
                );
                let _ = voters;
                raft.initialize(nodes)
                    .await
                    .map_err(|error| MetadataError::Raft(format!("{error:?}")))?;
            }
        }

        let listener = TcpListener::bind(config.listen)
            .await
            .map_err(|error| MetadataError::Network(format!("{error:?}")))?;
        let (shutdown, mut stop_rx) = tokio::sync::watch::channel(false);
        let server_raft = raft.clone();
        let server = tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = stop_rx.changed() => {
                        if *stop_rx.borrow() {
                            return;
                        }
                    }
                    accepted = listener.accept() => {
                        let Ok((mut stream, _)) = accepted else { continue };
                        let raft = server_raft.clone();
                        tokio::spawn(async move {
                            let response = match read_rpc(&mut stream).await {
                                Ok(RaftRpc::ClientWrite(command)) => {
                                    // A follower forwards the proposal to the leader of the
                                    // group; the committed answer is returned verbatim.
                                    match raft.client_write(command).await {
                                        Ok(response) => {
                                            RaftRpcResponse::ClientWrite(response.data)
                                        }
                                        Err(error) => {
                                            RaftRpcResponse::Error(format!("{error:?}"))
                                        }
                                    }
                                }
                                Ok(RaftRpc::Vote(rpc)) => {
                                    match raft.vote(rpc).await {
                                        Ok(response) => RaftRpcResponse::Vote(response),
                                        Err(error) => RaftRpcResponse::Error(format!("{error:?}")),
                                    }
                                }
                                Ok(RaftRpc::AppendEntries(rpc)) => {
                                    match raft.append_entries(rpc).await {
                                        Ok(response) => RaftRpcResponse::AppendEntries(response),
                                        Err(error) => RaftRpcResponse::Error(format!("{error:?}")),
                                    }
                                }
                                Ok(RaftRpc::InstallSnapshot {
                                    vote,
                                    meta,
                                    offset,
                                    data,
                                    done,
                                }) => {
                                    let request = openraft::raft::InstallSnapshotRequest {
                                        vote,
                                        meta,
                                        offset,
                                        data,
                                        done,
                                    };
                                    match raft.install_snapshot(request).await {
                                        Ok(response) => {
                                            RaftRpcResponse::InstallSnapshot(response)
                                        }
                                        Err(error) => {
                                            RaftRpcResponse::Error(format!("{error:?}"))
                                        }
                                    }
                                }
                                Err(error) => RaftRpcResponse::Error(format!("{error:?}")),
                            };
                            let _ = write_rpc(&mut stream, &response).await;
                        });
                    }
                }
            }
        });

        Ok(Self {
            raft,
            notify,
            state: shared,
            peers: config.peers.clone(),
            id: config.id,
            cluster_id: config.cluster_id,
            shutdown,
            server,
        })
    }

    pub fn id(&self) -> MetaNodeId {
        self.id
    }

    pub fn cluster_id(&self) -> u128 {
        self.cluster_id
    }

    /// The applied authoritative metadata state of this node.
    pub fn applied_state(&self) -> MetadataState {
        self.state
            .lock()
            .map(|state| state.clone())
            .unwrap_or_default()
    }

    /// The last log index this node knows about.
    pub async fn last_log_index(&self) -> Option<u64> {
        self.raft.metrics().borrow().last_log_index
    }

    /// A short summary of the Raft state (diagnostics only).
    pub async fn state_summary(&self) -> String {
        let metrics = self.raft.metrics().borrow().clone();
        format!(
            "node={} state={:?} leader={:?} last_log={:?}",
            self.id, metrics.state, metrics.current_leader, metrics.last_log_index
        )
    }

    /// The committed and applied metadata of this node as an immutable control-plane
    /// snapshot. It is `None` until the first command has been committed: an uncommitted
    /// proposal never becomes authoritative.
    pub fn committed_snapshot(&self) -> Option<rovqet_control::MetadataSnapshot> {
        let state = self.applied_state();
        control_snapshot(&state)
    }

    /// Subscribes to committed metadata revisions.
    pub fn committed_updates(&self) -> tokio::sync::watch::Receiver<u64> {
        self.notify.subscribe()
    }

    /// The current leader id and term, if this node knows a leader.
    pub async fn leader_with_term(&self) -> Option<(MetaNodeId, u64)> {
        let metrics = self.raft.metrics().borrow().clone();
        metrics
            .current_leader
            .map(|leader| (leader, metrics.current_term))
    }

    /// The index of the last entry applied to the state machine.
    pub async fn last_applied_index(&self) -> Option<u64> {
        self.raft
            .metrics()
            .borrow()
            .last_applied
            .map(|log_id| log_id.index)
    }

    /// The current Raft leader, if this node knows one.
    pub async fn current_leader(&self) -> Option<MetaNodeId> {
        self.raft.current_leader().await
    }

    /// Proposes a metadata command and waits for its committed and applied result.
    pub async fn propose(
        &self,
        command: MetadataCommand,
    ) -> Result<crate::types::MetadataCommandResponse, MetadataError> {
        match self.raft.client_write(command.clone()).await {
            Ok(response) => Ok(response.data),
            Err(error) => {
                // A follower forwards the proposal to the committed leader instead of
                // pretending that it can commit on its own.
                let Some(leader) = error.forward_to_leader() else {
                    return Err(MetadataError::Raft(format!("{error:?}")));
                };
                let Some(target) = leader.leader_id else {
                    return Err(MetadataError::Raft(format!("{error:?}")));
                };
                let Some((_, address)) = self
                    .peers
                    .iter()
                    .find(|(node, _)| *node == target)
                    .map(|(node, address)| (*node, *address))
                else {
                    return Err(MetadataError::Raft(format!("{error:?}")));
                };
                let response = crate::network::forward_client_write(
                    address,
                    command,
                    std::time::Duration::from_secs(30),
                )
                .await
                .map_err(|error| MetadataError::Network(format!("{error:?}")))?;
                Ok(response)
            }
        }
    }

    /// Stops the node and releases its resources.
    pub async fn shutdown(self) -> Result<(), MetadataError> {
        let _ = self.shutdown.send(true);
        let _ = self.raft.shutdown().await;
        let _ = self.server.await;
        Ok(())
    }
}

/// Converts an applied metadata state into the immutable control-plane snapshot model.
pub fn control_snapshot(state: &MetadataState) -> Option<rovqet_control::MetadataSnapshot> {
    let cluster = rovqet_control::ClusterId::new(state.cluster_id)?;
    let revision = rovqet_control::MetadataRevision::new(state.revision)?;
    let nodes = state
        .nodes
        .iter()
        .filter_map(|(id, address)| {
            let node = rovqet_control::NodeId::new(*id)?;
            let parsed = address.parse().ok()?;
            Some(rovqet_control::metadata::NodeEntry {
                node,
                endpoint: rovqet_control::PeerEndpoint {
                    node,
                    address: parsed,
                },
            })
        })
        .collect();
    let topics = state
        .topics
        .values()
        .map(|topic| rovqet_control::metadata::TopicPolicy {
            topic_id: topic.topic_id,
            durable_class: match topic.durable_class {
                0 => rovqet_control::metadata::DurableClass::Volatile,
                1 => rovqet_control::metadata::DurableClass::DurableLocal,
                _ => rovqet_control::metadata::DurableClass::DurableQuorum,
            },
            policy_revision: topic.policy_revision,
        })
        .collect();
    let partitions = state
        .placements
        .iter()
        .filter_map(|placement| {
            Some(rovqet_control::metadata::PartitionPlacement {
                partition: rovqet_core::PartitionRef {
                    tenant_id: placement.tenant_id,
                    topic_id: placement.topic_id,
                    partition_id: placement.partition_id,
                },
                owner: rovqet_control::NodeId::new(placement.owner)?,
                ownership_epoch: rovqet_control::OwnershipEpoch::new(placement.ownership_epoch)?,
                lease: rovqet_control::LeaseValidity::new(placement.lease_valid_for_ms)?,
                desired_replicas: 1,
                shard_id: 0,
                required_policy_revision: 1,
            })
        })
        .collect();
    // The snapshot is revalidated after the shortest lease it carries: the oldest authority
    // must expire first. A snapshot without any placement still needs a non-zero deadline.
    let revalidate_after_ms = state
        .placements
        .iter()
        .map(|placement| placement.lease_valid_for_ms)
        .min()
        .filter(|lease| *lease > 0)
        .unwrap_or(60_000);
    Some(rovqet_control::MetadataSnapshot {
        cluster_id: cluster,
        revision,
        issued_at_ms: 0,
        revalidate_after_ms,
        nodes,
        topics,
        partitions,
        acl_revision: 1,
        policy_revision: 1,
        capability_revision: 1,
    })
}

/// Applies one committed entry to the shared state (used by recovery tooling).
pub fn entry_command(entry: &openraft::Entry<MetadataRaftTypeConfig>) -> Option<MetadataCommand> {
    match &entry.payload {
        EntryPayload::Normal(command) => Some(command.clone()),
        _ => None,
    }
}

/// Convenience alias for the Raft error of this configuration.
pub type MetadataRaftError = RaftError<MetaNodeId>;
