//! Q2 shard consensus: the OpenRaft adapter for one shard group.
//!
//! # Durability domains are separate
//!
//! The domains are deliberately not interchangeable, and nothing in this crate may blur
//! them:
//!
//! - A Q2 Raft entry stored by [`log::FileLogStorage`] is **not** a metadata Raft entry
//!   of `rovqet-metadata-raft`. The two formats carry different magic values (`QRL1`
//!   versus `MRL1`), so a Q2 log file can never be opened, scanned or mistaken for a
//!   metadata log, and vice versa.
//! - A Q1 locally committed WAL record is **not** a Q2 quorum-committed entry. Local
//!   durability of a record only means the owning node persisted it; it becomes a Q2
//!   committed entry only after the shard group commits it through Raft. Code that
//!   reports or consumes a commit must state which domain it means.
//!
//! The crate holds the Q2 type configuration, the durable Raft log, the deterministic
//! shard state machine, the bounded TCP transport and the lifecycle of one shard voter.
//!
//! # The metadata ownership epoch is not minted here
//!
//! Ownership of a shard is decided by the metadata group, not by this Raft group. A local
//! election never mints or extends [`ShardAuthority::epoch`]; the authority is installed
//! from outside and a proposal carrying another epoch is refused before it becomes a Raft
//! entry.
#![forbid(unsafe_code)]

pub mod log;
pub mod network;
pub mod sm;
pub mod types;

use crate::log::FileLogStorage;
use crate::network::{
    ShardNetworkFactory, ShardPeerHandler, ShardPeerServer, ShardRpc, ShardRpcResponse,
    forward_client_write,
};
use crate::sm::{ShardState, ShardStateMachine};
use crate::types::{
    ShardCommand, ShardCommandResponse, ShardGroupIdentity, ShardId, ShardNode as ShardPeer,
    ShardNodeId, ShardRaftTypeConfig,
};
use openraft::raft::{InstallSnapshotRequest, Raft};
use openraft::{Config, SnapshotPolicy};
use std::collections::BTreeMap;
use std::future::Future;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;
use tokio::net::TcpListener;

/// File in the data directory that pins the shard group identity of the storage.
const IDENTITY_FILE: &str = "identity.json";

/// Configuration of one Q2 shard voter.
#[derive(Debug, Clone)]
pub struct ShardNodeConfig {
    pub id: ShardNodeId,
    pub cluster_id: u128,
    pub shard_id: ShardId,
    /// Address this node listens on for peer RPCs.
    pub listen: SocketAddr,
    pub peers: Vec<(ShardNodeId, SocketAddr)>,
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

/// The metadata ownership authority of one shard group.
///
/// The shard group stores it but never mints it: the epoch moves only when the metadata
/// group says so, and a proposal from another epoch is refused locally.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ShardAuthority {
    /// The node the metadata assigns the shard to.
    pub owner: ShardNodeId,
    /// The metadata ownership epoch.
    pub epoch: u64,
}

/// Failures of the Q2 node lifecycle.
#[derive(Debug)]
pub enum ShardError {
    Storage(String),
    Raft(String),
    Network(String),
    Identity(String),
    /// The proposal was refused before it became a Raft entry.
    Rejected(String),
}

impl std::fmt::Display for ShardError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ShardError::Storage(message) => write!(f, "shard storage failure: {message}"),
            ShardError::Raft(message) => write!(f, "shard raft failure: {message}"),
            ShardError::Network(message) => write!(f, "shard network failure: {message}"),
            ShardError::Identity(message) => write!(f, "shard identity failure: {message}"),
            ShardError::Rejected(message) => write!(f, "shard proposal rejected: {message}"),
        }
    }
}

impl std::error::Error for ShardError {}

/// One running voter of one Q2 shard group.
pub struct ShardNode {
    raft: Raft<ShardRaftTypeConfig>,
    /// The applied state the state machine publishes to this node.
    applied: Arc<Mutex<ShardState>>,
    /// Committed revisions, fed from the applied state.
    revisions: tokio::sync::watch::Receiver<u64>,
    /// The installed metadata authority, if any.
    authority: RwLock<Option<ShardAuthority>>,
    /// The voters of the group, used to forward a proposal to the leader.
    peers: Vec<(ShardNodeId, SocketAddr)>,
    id: ShardNodeId,
    identity: ShardGroupIdentity,
    timeout: Duration,
    shutdown: tokio::sync::watch::Sender<bool>,
    server: tokio::task::JoinHandle<()>,
    monitor: tokio::task::JoinHandle<()>,
}

impl ShardNode {
    /// Starts a shard voter: opens the durable storage, joins the group and serves peers.
    pub async fn start(config: ShardNodeConfig) -> Result<Self, ShardError> {
        if !config.listen.ip().is_loopback() {
            return Err(ShardError::Network(format!(
                "refusing the non-loopback listen address {}: the shard transport is a \
                 development-profile loopback listener",
                config.listen
            )));
        }
        let identity = ShardGroupIdentity {
            cluster_id: config.cluster_id,
            shard_id: config.shard_id,
        };
        // The group identity is part of the storage format: a directory that belongs to
        // another group is refused before anything is opened.
        verify_identity(&config.data_directory, identity)?;

        let log = FileLogStorage::open(config.data_directory.join("raft"))
            .map_err(|error| ShardError::Storage(format!("{error:?}")))?;
        let (machine, applied) =
            ShardStateMachine::open_shared(config.data_directory.join("state"), identity)
                .map_err(|error| ShardError::Storage(format!("{error:?}")))?;

        // Bind before the Raft core starts so a restart that cannot get its port back
        // fails before it has touched the group.
        let listener = TcpListener::bind(config.listen)
            .await
            .map_err(|error| ShardError::Network(format!("{error:?}")))?;

        let peers = config.peers.clone();
        let timeout = Duration::from_millis(config.election_timeout_ms.max(500));
        let network = ShardNetworkFactory::new(peers.clone(), timeout);
        let raft_config = Config {
            cluster_name: format!("rovqet-shard-{}-{}", identity.cluster_id, identity.shard_id),
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
            .map_err(|error| ShardError::Raft(format!("{error:?}")))?;

        // Explicit bootstrap: only the designated node initializes the membership, and
        // only when the storage is still uninitialized. Existing storage is never
        // re-initialized: the group identity and the log decide the membership.
        if config.bootstrap {
            let initialized = raft
                .is_initialized()
                .await
                .map_err(|error| ShardError::Raft(format!("{error:?}")))?;
            if !initialized {
                let mut nodes = BTreeMap::new();
                for (id, address) in &config.peers {
                    nodes.insert(
                        *id,
                        ShardPeer {
                            id: *id,
                            address: address.to_string(),
                        },
                    );
                }
                nodes.insert(
                    config.id,
                    ShardPeer {
                        id: config.id,
                        address: config.listen.to_string(),
                    },
                );
                raft.initialize(nodes)
                    .await
                    .map_err(|error| ShardError::Raft(format!("{error:?}")))?;
            }
        }

        let (shutdown, stop_rx) = tokio::sync::watch::channel(false);
        let handler = Arc::new(NodePeerHandler {
            raft: raft.clone(),
            peers: peers.clone(),
            id: config.id,
            timeout,
        });
        let server = ShardPeerServer::new(listener, peers.clone(), handler, Default::default());
        let server = tokio::spawn(async move {
            let _ = server.serve(stop_rx).await;
        });

        // The state machine publishes its applied state; this monitor turns the published
        // revision into a watch notification. It is the only bridge, so a subscriber can
        // never see a revision that was not applied.
        let (revision_tx, revisions) = tokio::sync::watch::channel(0u64);
        let monitor_applied = applied.clone();
        let mut monitor_stop = shutdown.subscribe();
        let monitor = tokio::spawn(async move {
            let mut last = monitor_applied
                .lock()
                .map(|state| state.revision)
                .unwrap_or_default();
            let _ = revision_tx.send(last);
            loop {
                tokio::select! {
                    changed = monitor_stop.changed() => {
                        match changed {
                            Ok(()) => if *monitor_stop.borrow() { return },
                            Err(_) => return,
                        }
                    }
                    _ = tokio::time::sleep(Duration::from_millis(20)) => {
                        let revision = monitor_applied
                            .lock()
                            .map(|state| state.revision)
                            .unwrap_or(last);
                        if revision != last {
                            last = revision;
                            if revision_tx.send(revision).is_err() {
                                return;
                            }
                        }
                    }
                }
            }
        });

        Ok(Self {
            raft,
            applied,
            revisions,
            authority: RwLock::new(None),
            peers,
            id: config.id,
            identity,
            timeout,
            shutdown,
            server,
            monitor,
        })
    }

    /// Installs the metadata authority this node must obey.
    ///
    /// A local election never mints or extends the epoch: installing `None` removes the
    /// constraint, installing an authority makes every proposal of another epoch fail
    /// before it becomes a Raft entry.
    pub fn install_authority(&self, authority: Option<ShardAuthority>) {
        let mut guard = self
            .authority
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        *guard = authority;
    }

    /// The installed metadata authority, if any.
    pub fn authority(&self) -> Option<ShardAuthority> {
        *self
            .authority
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// The applied, committed state of this node.
    pub fn applied_state(&self) -> ShardState {
        self.applied
            .lock()
            .map(|state| state.clone())
            .unwrap_or_else(|poisoned| poisoned.into_inner().clone())
    }

    /// Subscribes to committed state revisions. The value is the current revision at
    /// subscription time and advances only when a committed command mutated the state.
    pub fn applied_updates(&self) -> tokio::sync::watch::Receiver<u64> {
        self.revisions.clone()
    }

    /// The current leader id and term, if this node knows a leader.
    pub async fn leader_with_term(&self) -> Option<(ShardNodeId, u64)> {
        let metrics = self.raft.metrics().borrow().clone();
        metrics
            .current_leader
            .map(|leader| (leader, metrics.current_term))
    }

    /// The current Raft leader, if this node knows one.
    pub async fn current_leader(&self) -> Option<ShardNodeId> {
        self.raft.current_leader().await
    }

    /// The last log index this node knows about.
    pub async fn last_log_index(&self) -> Option<u64> {
        self.raft.metrics().borrow().last_log_index
    }

    /// The index of the last entry applied to the state machine.
    pub async fn last_applied_index(&self) -> Option<u64> {
        self.raft
            .metrics()
            .borrow()
            .last_applied
            .map(|log_id| log_id.index)
    }

    /// The last snapshot this node built or installed, as `(index, term)`.
    pub async fn last_snapshot(&self) -> Option<(u64, u64)> {
        self.raft
            .metrics()
            .borrow()
            .snapshot
            .map(|log_id| (log_id.index, log_id.leader_id.term))
    }

    /// The last log index this node purged, if the log was compacted.
    pub async fn purged_index(&self) -> Option<u64> {
        self.raft
            .metrics()
            .borrow()
            .purged
            .map(|log_id| log_id.index)
    }

    /// Builds a snapshot of the applied state now.
    pub async fn trigger_snapshot(&self) -> Result<(), ShardError> {
        self.raft
            .trigger()
            .snapshot()
            .await
            .map_err(|error| ShardError::Raft(format!("{error:?}")))
    }

    /// The identity of the group this node belongs to.
    pub fn identity(&self) -> ShardGroupIdentity {
        self.identity
    }

    /// Proposes a shard command and waits for its committed and applied result.
    pub async fn propose(&self, command: ShardCommand) -> Result<ShardCommandResponse, ShardError> {
        // The metadata owns the epoch. A stale or foreign epoch is refused here, before
        // the command becomes a Raft entry: it must not be replicated at all.
        if let Some(authority) = self.authority() {
            if command.metadata_epoch != authority.epoch {
                return Err(ShardError::Rejected(format!(
                    "metadata epoch {} is not the installed epoch {} owned by node {}",
                    command.metadata_epoch, authority.epoch, authority.owner
                )));
            }
        }
        match self.raft.client_write(command.clone()).await {
            Ok(response) => Ok(response.data),
            Err(error) => {
                // A follower forwards the proposal to the committed leader instead of
                // pretending that it can commit on its own.
                let Some(forward) = error.forward_to_leader() else {
                    return Err(ShardError::Raft(format!("{error:?}")));
                };
                let Some(target) = forward.leader_id else {
                    return Err(ShardError::Raft(format!("{error:?}")));
                };
                let Some((_, address)) = self
                    .peers
                    .iter()
                    .find(|(node, _)| *node == target)
                    .map(|(node, address)| (*node, *address))
                else {
                    return Err(ShardError::Raft(format!("{error:?}")));
                };
                forward_client_write(address, target, command, self.timeout)
                    .await
                    .map_err(|error| ShardError::Network(format!("{error:?}")))
            }
        }
    }

    /// Stops the node and releases its resources.
    pub async fn shutdown(self) -> Result<(), ShardError> {
        let _ = self.shutdown.send(true);
        let _ = self.raft.shutdown().await;
        let _ = self.server.await;
        let _ = self.monitor.await;
        Ok(())
    }
}

impl std::fmt::Debug for ShardNode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ShardNode")
            .field("id", &self.id)
            .field("identity", &self.identity)
            .field("peers", &self.peers)
            .finish_non_exhaustive()
    }
}

/// Answers one peer request from the local Raft core.
struct NodePeerHandler {
    raft: Raft<ShardRaftTypeConfig>,
    peers: Vec<(ShardNodeId, SocketAddr)>,
    id: ShardNodeId,
    timeout: Duration,
}

impl NodePeerHandler {
    /// A client write on this node: the leader commits it, a follower forwards it once.
    async fn client_write(&self, command: ShardCommand) -> ShardRpcResponse {
        match self.raft.client_write(command.clone()).await {
            Ok(response) => ShardRpcResponse::ClientWrite(response.data),
            Err(error) => {
                let Some(forward) = error.forward_to_leader() else {
                    return ShardRpcResponse::Error(format!("{error:?}"));
                };
                let Some(leader) = forward.leader_id else {
                    return ShardRpcResponse::Error(format!("{error:?}"));
                };
                // Never forward to this node: the local write already failed here.
                if leader == self.id {
                    return ShardRpcResponse::Error(format!("{error:?}"));
                }
                let Some((_, address)) = self
                    .peers
                    .iter()
                    .find(|(node, _)| *node == leader)
                    .map(|(node, address)| (*node, *address))
                else {
                    return ShardRpcResponse::Error(format!(
                        "the committed leader {leader} is not a known voter"
                    ));
                };
                match forward_client_write(address, leader, command, self.timeout).await {
                    Ok(response) => ShardRpcResponse::ClientWrite(response),
                    Err(error) => ShardRpcResponse::Error(format!("{error:?}")),
                }
            }
        }
    }
}

impl ShardPeerHandler for NodePeerHandler {
    fn handle(
        &self,
        envelope: ShardRpc,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<u8>, String>> + Send + '_>> {
        Box::pin(async move {
            let response = match envelope {
                ShardRpc::Vote(rpc) => match self.raft.vote(rpc).await {
                    Ok(response) => ShardRpcResponse::Vote(response),
                    Err(error) => ShardRpcResponse::Error(format!("{error:?}")),
                },
                ShardRpc::AppendEntries(rpc) => match self.raft.append_entries(rpc).await {
                    Ok(response) => ShardRpcResponse::AppendEntries(response),
                    Err(error) => ShardRpcResponse::Error(format!("{error:?}")),
                },
                ShardRpc::InstallSnapshot {
                    vote,
                    meta,
                    offset,
                    data,
                    done,
                } => {
                    let request = InstallSnapshotRequest {
                        vote,
                        meta,
                        offset,
                        data,
                        done,
                    };
                    match self.raft.install_snapshot(request).await {
                        Ok(response) => ShardRpcResponse::InstallSnapshot(response),
                        Err(error) => ShardRpcResponse::Error(format!("{error:?}")),
                    }
                }
                ShardRpc::ClientWrite(command) => self.client_write(command).await,
            };
            serde_json::to_vec(&response).map_err(|error| format!("{error}"))
        })
    }
}

/// The identity pinned in a data directory, as stored on disk.
#[derive(serde::Serialize, serde::Deserialize)]
struct StoredIdentity {
    cluster_id: u128,
    shard_id: ShardId,
}

/// Writes the group identity on first start and refuses a foreign one afterwards.
///
/// The identity is part of the storage format: silently reusing a directory of another
/// group would replay another group's committed history into this state machine.
fn verify_identity(directory: &Path, identity: ShardGroupIdentity) -> Result<(), ShardError> {
    std::fs::create_dir_all(directory)
        .map_err(|error| ShardError::Storage(format!("{error:?}")))?;
    let path = directory.join(IDENTITY_FILE);
    match std::fs::read(&path) {
        Ok(bytes) => {
            let stored: StoredIdentity = serde_json::from_slice(&bytes).map_err(|error| {
                ShardError::Identity(format!(
                    "the identity file {} is unreadable: {error}",
                    path.display()
                ))
            })?;
            let stored = ShardGroupIdentity {
                cluster_id: stored.cluster_id,
                shard_id: stored.shard_id,
            };
            if stored != identity {
                return Err(ShardError::Identity(format!(
                    "the data directory {} belongs to shard group {stored}, not {identity}",
                    directory.display()
                )));
            }
            Ok(())
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            let stored = StoredIdentity {
                cluster_id: identity.cluster_id,
                shard_id: identity.shard_id,
            };
            let bytes = serde_json::to_vec(&stored)
                .map_err(|error| ShardError::Identity(format!("{error}")))?;
            let temporary = directory.join("identity.json.tmp");
            {
                use std::io::Write;
                let mut file = std::fs::File::create(&temporary)
                    .map_err(|error| ShardError::Storage(format!("{error:?}")))?;
                file.write_all(&bytes)
                    .and_then(|()| file.sync_all())
                    .map_err(|error| ShardError::Storage(format!("{error:?}")))?;
            }
            std::fs::rename(&temporary, &path)
                .map_err(|error| ShardError::Storage(format!("{error:?}")))?;
            // Make the rename itself durable.
            if let Ok(directory) = std::fs::File::open(directory) {
                let _ = directory.sync_all();
            }
            Ok(())
        }
        Err(error) => Err(ShardError::Storage(format!("{error:?}"))),
    }
}
