//! The source side of a Q1 ownership migration.
//!
//! The driver observes the committed migration record and performs the stages in order:
//! it transfers the committed history, fences itself durably, transfers the final delta
//! and only then asks the metadata group to commit the activation. The committed metadata
//! is the only authority: a local timer never advances a stage.
use crate::transfer::{bytes_to_hex, hex_to_bytes, sha256};
use rovqet_control::TimeoutBudget;
use rovqet_control::ids::NodeId;
use rovqet_control::rpc::{
    Q1TransferChunkRequest, Q1TransferChunkResponse, Q1TransferFile, Q1TransferFinalizeRequest,
    Q1TransferFinalizeResponse, Q1TransferPrepareRequest, Q1TransferPrepareResponse,
    Q1TransferStatus, RpcErrorCode, RpcKind,
};
use rovqet_control::transport::PeerTransport;
use rovqet_metadata_raft::MetadataNode;
use rovqet_metadata_raft::types::{FenceProof, MetadataCommand, MigrationStage};
use sha2::{Digest, Sha256};
use std::path::Path;
use std::time::Duration;

/// Largest chunk that is transferred in one RPC.
pub const TRANSFER_CHUNK_BYTES: usize = 256 * 1024;

/// Why a migration step failed.
#[derive(Debug)]
pub enum MigrationError {
    /// The peer answered with an error.
    Rpc(RpcErrorCode),
    /// The transfer transport failed.
    Transport(String),
    /// The local storage failed.
    Io(String),
    /// The metadata group refused the transition.
    Metadata(String),
}

impl std::fmt::Display for MigrationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}

impl std::error::Error for MigrationError {}

/// One file of a partition inventory.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InventoryFile {
    /// The plain file name inside the partition directory.
    pub name: String,
    pub size: u64,
    /// The digest of the whole file.
    pub digest: Vec<u8>,
}

/// Reads the inventory of a Q1 partition directory.
///
/// Only the durable WAL files belong to the inventory: the manifest and the sealed and
/// open segments. A staging directory inside the partition is never part of it.
pub fn partition_inventory(directory: &Path) -> Result<Vec<InventoryFile>, MigrationError> {
    let mut files = Vec::new();
    let entries =
        std::fs::read_dir(directory).map_err(|error| MigrationError::Io(error.to_string()))?;
    for entry in entries {
        let entry = entry.map_err(|error| MigrationError::Io(error.to_string()))?;
        let name = entry.file_name().to_string_lossy().to_string();
        let path = entry.path();
        if path.is_dir() {
            // A staging area is never part of the transferred history.
            continue;
        }
        if name != "MANIFEST" && !name.starts_with("segment-") {
            continue;
        }
        let bytes = std::fs::read(&path).map_err(|error| MigrationError::Io(error.to_string()))?;
        files.push(InventoryFile {
            name,
            size: bytes.len() as u64,
            digest: sha256(&bytes),
        });
    }
    files.sort_by(|left, right| left.name.cmp(&right.name));
    Ok(files)
}

/// The driver of one migration on the source node.
pub struct MigrationDriver {
    transport: std::sync::Arc<PeerTransport>,
    raft: std::sync::Arc<MetadataNode>,
    directory: std::path::PathBuf,
    node: u64,
}

impl MigrationDriver {
    /// Creates a driver for the given partition directory.
    pub fn new(
        transport: std::sync::Arc<PeerTransport>,
        raft: std::sync::Arc<MetadataNode>,
        directory: std::path::PathBuf,
        node: u64,
    ) -> Self {
        Self {
            transport,
            raft,
            directory,
            node,
        }
    }

    /// The metadata node of this driver.
    pub fn raft(&self) -> &std::sync::Arc<MetadataNode> {
        &self.raft
    }

    /// The durable directory of this partition.
    pub fn directory(&self) -> &std::path::Path {
        &self.directory
    }

    /// Transfers the committed history of this partition to the target.
    ///
    /// The inventory is declared first, then every file is sent in bounded chunks. A chunk
    /// that was already received is accepted idempotently by the target.
    pub async fn transfer_history(
        &self,
        target: NodeId,
        migration_id: u128,
        incarnation: u128,
        original_epoch: u64,
        next_epoch: u64,
        manifest_generation: u64,
    ) -> Result<(Vec<InventoryFile>, Vec<u8>), MigrationError> {
        let files = partition_inventory(&self.directory)?;
        let declared: Vec<Q1TransferFile> = files
            .iter()
            .map(|file| Q1TransferFile {
                name: file.name.clone(),
                size: file.size,
                digest: file.digest.clone(),
            })
            .collect();
        let inventory = crate::transfer::inventory_digest(&declared);
        let prepare = Q1TransferPrepareRequest {
            migration_id,
            source_node: self.node,
            target_node: target.get(),
            tenant_id: 0,
            topic_id: 0,
            partition_id: 0,
            incarnation,
            original_epoch,
            next_epoch,
            manifest_generation,
            manifest_digest: inventory.clone(),
            files: declared,
        };
        let response = self
            .transport
            .call(
                target,
                RpcKind::Q1TransferPrepare,
                prepare.encode(),
                TimeoutBudget::new(30_000).expect("budget"),
                0,
                0,
            )
            .await
            .map_err(|error| MigrationError::Transport(format!("{error:?}")))?;
        let response = Q1TransferPrepareResponse::decode(&response.body)
            .map_err(|_| MigrationError::Rpc(RpcErrorCode::Malformed))?;
        if !response.accepted {
            return Err(MigrationError::Rpc(
                response.error.unwrap_or(RpcErrorCode::Internal),
            ));
        }

        for (index, file) in files.iter().enumerate() {
            let bytes = std::fs::read(self.directory.join(&file.name))
                .map_err(|error| MigrationError::Io(error.to_string()))?;
            let mut offset = 0usize;
            let mut chunk_index = 0u64;
            while offset < bytes.len() || (bytes.is_empty() && chunk_index == 0) {
                let end = (offset + TRANSFER_CHUNK_BYTES).min(bytes.len());
                let data = bytes[offset..end].to_vec();
                let chunk = Q1TransferChunkRequest {
                    migration_id,
                    inventory_digest: inventory.clone(),
                    chunk_index,
                    file_index: index as u32,
                    offset: offset as u64,
                    digest: sha256(&data),
                    data,
                };
                let response = self
                    .transport
                    .call(
                        target,
                        RpcKind::Q1TransferChunk,
                        chunk.encode(),
                        TimeoutBudget::new(30_000).expect("budget"),
                        0,
                        0,
                    )
                    .await
                    .map_err(|error| MigrationError::Transport(format!("{error:?}")))?;
                let response = Q1TransferChunkResponse::decode(&response.body)
                    .map_err(|_| MigrationError::Rpc(RpcErrorCode::Malformed))?;
                if !response.accepted {
                    return Err(MigrationError::Rpc(
                        response.error.unwrap_or(RpcErrorCode::Internal),
                    ));
                }
                offset = end;
                chunk_index += 1;
                if bytes.is_empty() {
                    break;
                }
            }
        }
        Ok((files, inventory))
    }

    /// Asks the target to verify the transferred inventory.
    pub async fn finalize(
        &self,
        target: NodeId,
        migration_id: u128,
        inventory_digest: &[u8],
        final_digest: Vec<u8>,
        last_committed_offset: u64,
        transfer_watermark: u64,
        manifest_generation: u64,
        fence: Option<&FenceProof>,
        old_epoch: u64,
    ) -> Result<Q1TransferStatus, MigrationError> {
        let request = Q1TransferFinalizeRequest {
            migration_id,
            inventory_digest: inventory_digest.to_vec(),
            final_digest,
            last_committed_offset,
            transfer_watermark,
            manifest_generation,
            fence_offset: fence.map(|proof| proof.fence_offset),
            fence_digest: fence.map(|proof| proof.fence_digest.as_bytes().to_vec()),
            old_epoch,
        };
        let response = self
            .transport
            .call(
                target,
                RpcKind::Q1TransferFinalize,
                request.encode(),
                TimeoutBudget::new(30_000).expect("budget"),
                0,
                0,
            )
            .await
            .map_err(|error| MigrationError::Transport(format!("{error:?}")))?;
        let response = Q1TransferFinalizeResponse::decode(&response.body)
            .map_err(|_| MigrationError::Rpc(RpcErrorCode::Malformed))?;
        Ok(response.status)
    }

    /// Commits a migration transition.
    ///
    /// The proposal is retried until the metadata group accepts it: a node that is not the
    /// leader is not allowed to advance a stage on its own.
    pub async fn commit(
        &self,
        command: MetadataCommand,
        deadline: Duration,
    ) -> Result<(), MigrationError> {
        let started = std::time::Instant::now();
        let mut last = String::new();
        while started.elapsed() < deadline {
            match self.raft.propose(command.clone()).await {
                Ok(response) => {
                    return match response {
                        rovqet_metadata_raft::types::MetadataCommandResponse::Applied {
                            ..
                        } => Ok(()),
                        rovqet_metadata_raft::types::MetadataCommandResponse::Rejected {
                            reason,
                        } => Err(MigrationError::Metadata(format!(
                            "the metadata group rejected the transition: {reason}"
                        ))),
                        rovqet_metadata_raft::types::MetadataCommandResponse::Noop { .. } => {
                            Err(MigrationError::Metadata(
                                "the metadata group ignored the transition".into(),
                            ))
                        }
                    };
                }
                Err(error) => {
                    let message = format!("{error:?}");
                    if message != last {
                        last = message.clone();
                        eprintln!("Q1 coordinator: the proposal was not accepted: {last}");
                    }
                    tokio::time::sleep(Duration::from_millis(200)).await;
                }
            }
        }
        Err(MigrationError::Metadata(last))
    }
}

/// Reports one committed migration transition on the diagnostic stream.
///
/// The line is machine readable so that an external oracle can compare the committed
/// transitions with what the brokers actually did.
pub fn report(event: &str, record: &rovqet_metadata_raft::sm::MigrationState, revision: u64) {
    let fence = record.fence_proof.as_ref().map(|proof| {
        serde_json::json!({
            "migration_id": proof.migration_id.to_string(),
            "old_owner": proof.old_owner,
            "old_epoch": proof.old_epoch,
            "fence_offset": proof.fence_offset,
            "fence_digest": proof.fence_digest,
            "manifest_generation": proof.manifest_generation,
        })
    });
    eprintln!(
        "MIGRATION {} {}",
        event,
        serde_json::json!({
            "migration_id": record.migration_id.to_string(),
            "stage": format!("{:?}", record.stage).to_lowercase(),
            "source": record.source_node,
            "target": record.target_node,
            "original_epoch": record.original_epoch,
            "next_epoch": record.next_epoch,
            "incarnation": record.source_incarnation,
            "transfer_watermark": record.transfer_watermark,
            "manifest_id": record.manifest_id,
            "target_ready": record.target_ready,
            "fence": fence,
            "metadata_revision": revision,
        })
    );
}

/// The operator request that asks a broker to hand its partition over.
#[derive(Debug, Clone)]
pub struct OperatorRequest {
    /// The node that receives the partition.
    pub target_node: u64,
    pub tenant_id: u64,
    pub topic_id: u64,
    pub partition_id: u32,
    pub incarnation: u128,
    /// The ownership epoch the source holds today.
    pub epoch: u64,
    /// The endpoints of the metadata group, used to register the handover's nodes.
    pub peers: Vec<(u64, std::net::SocketAddr)>,
    /// The local file that asks the source to start the committed handover.
    pub trigger_path: std::path::PathBuf,
}

/// The name of the local operator trigger that starts a committed handover.
///
/// The trigger only asks the source to drive the migration; every stage is still committed
/// by the metadata group.
pub const MIGRATE_TRIGGER: &str = "MIGRATE";

/// The stage that follows the given one, if any.
pub fn next_stage(stage: MigrationStage) -> Option<MigrationStage> {
    stage.next()
}

/// The stable identity of a migration.
///
/// The identity is derived from the partition and the two nodes, so a restart of the
/// operator resumes the same migration instead of starting a second one.
pub fn migration_id(
    tenant_id: u64,
    topic_id: u64,
    partition_id: u32,
    incarnation: u128,
    source: u64,
    target: u64,
) -> u128 {
    let mut hasher = Sha256::new();
    hasher.update(tenant_id.to_be_bytes());
    hasher.update(topic_id.to_be_bytes());
    hasher.update(partition_id.to_be_bytes());
    hasher.update(incarnation.to_be_bytes());
    hasher.update(source.to_be_bytes());
    hasher.update(target.to_be_bytes());
    let digest = hasher.finalize();
    u128::from_be_bytes(digest[..16].try_into().unwrap()) | 1
}

/// Drives the committed migration of this node when it is the source.
///
/// The driver never advances a stage on its own: every transition is committed by the
/// metadata group, and the durable fence of the old owner is the only thing that allows
/// the activation. A failure at any stage leaves the old owner authoritative.
pub async fn run_coordinator(
    driver: MigrationDriver,
    route_slot: std::sync::Arc<crate::q1::RouteSlot>,
    node: u64,
    target_request: Option<OperatorRequest>,
) {
    let raft = driver.raft().clone();
    let mut updates = raft.committed_updates();
    let mut last_request: Option<std::time::Instant> = None;
    loop {
        // The current committed state is examined first: a migration that was committed
        // before this process started must still be driven.
        async {
            let state = raft.applied_state();
            let Some(record) = state
                .migrations
                .values()
                .find(|record| {
                    record.source_node == node && record.stage != MigrationStage::Cleanup
                })
                .cloned()
            else {
                // No migration is committed yet. The operator may have asked for one.
                if let Some(request) = target_request.clone() {
                    // The operator asked for a handover of this partition. The endpoints of
                    // the handover are registered first: a placement of an unknown owner is
                    // rejected by the metadata group, so the handover could never start.
                    let mut missing_endpoint = false;
                    for (peer, address) in &request.peers {
                        if !state.nodes.contains_key(peer) {
                            missing_endpoint = true;
                            let _ = driver
                                .commit(
                                    MetadataCommand::SetNode {
                                        node: *peer,
                                        address: address.to_string(),
                                    },
                                    Duration::from_secs(30),
                                )
                                .await;
                        }
                    }
                    if missing_endpoint {
                        return;
                    }
                    // The partition must be placed on this node, and that placement is
                    // committed by the metadata group like every other topology change.
                    let placement = state
                        .placements
                        .iter()
                        .find(|placement| {
                            placement.tenant_id == request.tenant_id
                                && placement.topic_id == request.topic_id
                                && placement.partition_id == request.partition_id
                        })
                        .cloned();
                    if placement.is_none() {
                        let _ = driver
                            .commit(
                                MetadataCommand::SetPlacement {
                                    tenant_id: request.tenant_id,
                                    topic_id: request.topic_id,
                                    partition_id: request.partition_id,
                                    owner: node,
                                    ownership_epoch: request.epoch,
                                    lease_valid_for_ms: 600_000,
                                },
                                Duration::from_secs(30),
                            )
                            .await;
                        // The migration may be proposed right away.
                        last_request = None;
                        return;
                    }
                    // The migration request is repeated at a bounded rate: the metadata
                    // group may not have a leader yet.
                    let now = std::time::Instant::now();
                    if last_request
                        .map(|last| now.duration_since(last) < Duration::from_secs(5))
                        .unwrap_or(false)
                    {
                        return;
                    }
                    last_request = Some(now);
                    // The handover starts when the operator asks for it locally: the source
                    // first serves the partition with its committed authority, and only then
                    // begins the transfer.
                    let Ok(text) = std::fs::read_to_string(&request.trigger_path) else {
                        return;
                    };
                    let Ok(target_node) = text.trim().parse::<u64>() else {
                        return;
                    };
                    let request = OperatorRequest {
                        target_node,
                        ..request
                    };
                    let _ = driver
                        .commit(
                            MetadataCommand::BeginMigration {
                                migration_id: migration_id(
                                    request.tenant_id,
                                    request.topic_id,
                                    request.partition_id,
                                    request.incarnation,
                                    node,
                                    request.target_node,
                                ),
                                tenant_id: request.tenant_id,
                                topic_id: request.topic_id,
                                partition_id: request.partition_id,
                                source_node: node,
                                target_node: request.target_node,
                                source_incarnation: request.incarnation,
                                original_epoch: placement
                                    .as_ref()
                                    .map(|placement| placement.ownership_epoch)
                                    .unwrap_or(0),
                                next_epoch: placement
                                    .as_ref()
                                    .map(|placement| placement.ownership_epoch + 1)
                                    .unwrap_or(0),
                                dependent_partitions: Vec::new(),
                            },
                            Duration::from_secs(30),
                        )
                        .await;
                }
                return;
            };
            let Some(target) = NodeId::new(record.target_node) else {
                return;
            };
            // The route is opened by the activator once the committed metadata allows it.
            // The transfer and the fence need it.
            let Some(route) = route_slot.current() else {
                return;
            };
            match record.stage {
                MigrationStage::Prepare => {
                    // Transfer the committed history and let the target verify it.
                    let transferred = match driver
                        .transfer_history(
                            target,
                            record.migration_id,
                            record.source_incarnation,
                            record.original_epoch,
                            record.next_epoch,
                            record.transfer_watermark.unwrap_or_default(),
                        )
                        .await
                    {
                        Ok(value) => value,
                        Err(error) => {
                            eprintln!("metadata migration transfer failed: {error}");
                            return;
                        }
                    };
                    let (files, inventory) = transferred;
                    let digest = crate::transfer::inventory_digest(
                        &files
                            .iter()
                            .map(|file| Q1TransferFile {
                                name: file.name.clone(),
                                size: file.size,
                                digest: file.digest.clone(),
                            })
                            .collect::<Vec<_>>(),
                    );
                    let watermark = route.committed_count();
                    let status = match driver
                        .finalize(
                            target,
                            record.migration_id,
                            &inventory,
                            digest,
                            watermark,
                            watermark,
                            0,
                            None,
                            record.original_epoch,
                        )
                        .await
                    {
                        Ok(status) => status,
                        Err(error) => {
                            eprintln!("metadata migration finalize failed: {error}");
                            return;
                        }
                    };
                    if status != Q1TransferStatus::ReadyForFence {
                        return;
                    }
                    let command = MetadataCommand::AdvanceMigration {
                        migration_id: record.migration_id,
                        stage: MigrationStage::CatchUp,
                        transfer_watermark: Some(watermark),
                        manifest_id: Some(crate::transfer::bytes_to_hex(&inventory)),
                        fence_proof: None,
                        target_ready: false,
                    };
                    if let Err(error) = driver.commit(command, Duration::from_secs(30)).await {
                        eprintln!("metadata migration catch up was not committed: {error}");
                    } else if let Some(updated) =
                        raft.applied_state().migrations.get(&record.migration_id)
                    {
                        report("committed", updated, raft.applied_state().revision);
                    }
                }
                MigrationStage::CatchUp => {
                    // The old owner revokes its authority durably. Only then may the
                    // successor be activated.
                    let proof = match route
                        .fence_old_owner(
                            record.migration_id,
                            record.original_epoch,
                            record.next_epoch,
                        )
                        .await
                    {
                        Ok(proof) => proof,
                        Err(error) => {
                            eprintln!("metadata migration fence failed: {error:?}");
                            return;
                        }
                    };
                    let proof = rovqet_metadata_raft::types::FenceProof {
                        migration_id: proof.migration_id,
                        old_owner: proof.old_owner,
                        old_epoch: proof.old_epoch,
                        fence_offset: proof.fence_offset,
                        fence_digest: proof.fence_digest.clone(),
                        manifest_generation: proof.manifest_generation,
                    };
                    let fence_command = MetadataCommand::AdvanceMigration {
                        migration_id: record.migration_id,
                        stage: MigrationStage::FenceOld,
                        transfer_watermark: record.transfer_watermark,
                        manifest_id: record.manifest_id.clone(),
                        fence_proof: Some(proof.clone()),
                        target_ready: false,
                    };
                    if let Err(error) = driver.commit(fence_command, Duration::from_secs(30)).await
                    {
                        eprintln!("metadata migration fence was not committed: {error}");
                    } else if let Some(updated) =
                        raft.applied_state().migrations.get(&record.migration_id)
                    {
                        report("committed", updated, raft.applied_state().revision);
                    }
                    // The log is final now: transfer the remaining history and verify it.
                    let transferred = match driver
                        .transfer_history(
                            target,
                            record.migration_id,
                            record.source_incarnation,
                            record.original_epoch,
                            record.next_epoch,
                            proof.manifest_generation,
                        )
                        .await
                    {
                        Ok(value) => value,
                        Err(error) => {
                            eprintln!("metadata migration delta transfer failed: {error}");
                            return;
                        }
                    };
                    let (files, inventory) = transferred;
                    let digest = crate::transfer::inventory_digest(
                        &files
                            .iter()
                            .map(|file| Q1TransferFile {
                                name: file.name.clone(),
                                size: file.size,
                                digest: file.digest.clone(),
                            })
                            .collect::<Vec<_>>(),
                    );
                    let watermark = route.committed_count();
                    let status = match driver
                        .finalize(
                            target,
                            record.migration_id,
                            &inventory,
                            digest,
                            watermark,
                            watermark,
                            proof.manifest_generation,
                            Some(&proof),
                            record.original_epoch,
                        )
                        .await
                    {
                        Ok(status) => status,
                        Err(error) => {
                            eprintln!("metadata migration final finalize failed: {error}");
                            return;
                        }
                    };
                    if status != Q1TransferStatus::ReadyForActivation {
                        return;
                    }
                    let activation = MetadataCommand::AdvanceMigration {
                        migration_id: record.migration_id,
                        stage: MigrationStage::ActivateNew,
                        transfer_watermark: Some(watermark),
                        manifest_id: Some(crate::transfer::bytes_to_hex(&inventory)),
                        fence_proof: None,
                        target_ready: true,
                    };
                    if let Err(error) = driver.commit(activation, Duration::from_secs(30)).await {
                        eprintln!("metadata migration activation was not committed: {error}");
                    } else if let Some(updated) =
                        raft.applied_state().migrations.get(&record.migration_id)
                    {
                        report("committed", updated, raft.applied_state().revision);
                    }
                }
                MigrationStage::FenceOld
                | MigrationStage::ActivateNew
                | MigrationStage::Cleanup => {}
            }
        }
        .await;
        // A committed metadata change wakes the coordinator, but the operator trigger
        // file is local: it must also be re-read when nothing else has changed yet
        // (for example right after the placement commit that precedes the handover).
        tokio::select! {
            changed = updates.changed() => {
                if changed.is_err() {
                    return;
                }
            }
            _ = tokio::time::sleep(Duration::from_millis(200)) => {}
        }
    }
}

/// The storage lifecycle of a successor.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ActivationState {
    /// The transferred bytes are staged and not yet verified.
    Staging,
    /// The inventory was verified by the target.
    Verified,
    /// The old owner confirmed its durable fence.
    FenceConfirmed,
    /// The metadata group committed the activation.
    ActivationCommitted,
    /// The verified storage was published into the partition directory.
    Promoted,
    /// The route is open and the committed authority is installed.
    Active,
}

/// Opens the Q1 route of this node as soon as the committed metadata allows it.
///
/// A node without a committed placement stays inactive. A successor of a migration waits
/// for the committed activation, promotes the verified inventory and only then opens the
/// transferred history: an empty log is never a substitute for the transferred history.
pub async fn run_activator(
    cluster: std::sync::Arc<crate::cluster::Cluster>,
    raft: std::sync::Arc<MetadataNode>,
    route_slot: std::sync::Arc<crate::q1::RouteSlot>,
    options: crate::q1::Options,
    node: u64,
    staging_root: Option<std::path::PathBuf>,
) {
    let mut updates = raft.committed_updates();
    loop {
        // The current committed state is examined first, then the activator waits for the
        // next committed change. The iteration always ends in the wait, so a state without
        // a placement never spins.
        let outcome = async {
            let state = raft.applied_state();
            let Some(placement) = state.placement(
                options.partition.tenant_id,
                options.partition.topic_id,
                options.partition.partition_id,
            ) else {
                return None;
            };
            if placement.owner != node {
                return None;
            }
            // A committed migration that targets this node decides how the storage is
            // obtained.
            let migration = state
                .migrations
                .values()
                .find(|record| {
                    record.target_node == node
                        && record.tenant_id == options.partition.tenant_id
                        && record.topic_id == options.partition.topic_id
                        && record.partition_id == options.partition.partition_id
                })
                .cloned();
            let mut route_options = options.clone();
            route_options.owner_epoch = placement.ownership_epoch;
            // A successor must never initialize an empty log as a substitute for the
            // transferred history: it opens the promoted storage instead.
            route_options.initialize_new_log = options.initialize_new_log && migration.is_none();
            if let Some(record) = &migration {
                // The activation must be committed and the successor must be the target.
                if record.stage != MigrationStage::ActivateNew {
                    return None;
                }
                // The dependent partitions must be available: a terminal disposition that
                // references a DLQ record needs a recoverable destination.
                if !dependencies_available(&route_options, record) {
                    eprintln!(
                        "the dependent history of the migration is not available locally; \
                         the successor stays inactive"
                    );
                    return None;
                }
                // The transferred storage is promoted before the route is opened.
                let Some(staging_root) = staging_root.clone() else {
                    return None;
                };
                let inventory = hex_to_bytes(record.manifest_id.as_deref().unwrap_or_default());
                let staging =
                    crate::promotion::staging_area(&staging_root, record.migration_id, &inventory);
                let plan = crate::promotion::PromotionRecord {
                    migration_id: record.migration_id,
                    inventory_digest: bytes_to_hex(&inventory),
                    incarnation: record.source_incarnation,
                    tenant_id: options.partition.tenant_id,
                    topic_id: options.partition.topic_id,
                    partition_id: options.partition.partition_id,
                    target_epoch: placement.ownership_epoch,
                    activation_revision: state.revision,
                    files: Vec::new(),
                    state: crate::promotion::PromotionState::Promoted,
                };
                if let Err(error) = crate::promotion::promote_with_manifest(
                    &staging,
                    &route_options.directory,
                    &plan,
                ) {
                    eprintln!("the promoted storage was refused: {error}");
                    return None;
                }
                // The transferred history keeps its incarnation.
                route_options.incarnation = record.source_incarnation;
            }
            let partition = route_options.partition;
            match crate::q1::Route::open(route_options) {
                Ok(route) => {
                    let route = std::sync::Arc::new(route);
                    eprintln!(
                        "Q1 route active: partition={:?} epoch={} incarnation={}",
                        partition, route.options.owner_epoch, route.options.incarnation
                    );
                    if let Some(authority) = cluster.authority_for(&partition) {
                        route.install_authority(Some(authority));
                    }
                    Some(route)
                }
                Err(error) => {
                    // A failed reopen leaves the node inactive and reports the error.
                    eprintln!("the Q1 route stays inactive: {error}");
                    None
                }
            }
        }
        .await;
        if let Some(route) = outcome {
            route_slot.install(route);
            return;
        }
        if updates.changed().await.is_err() {
            return;
        }
    }
}

/// True when every dependent partition of the migration is available locally.
///
/// A dependency is only satisfied by storage that exists and can be recovered: the
/// existence of the dependency field is never proof of durability.
pub fn dependencies_available(
    options: &crate::q1::Options,
    record: &rovqet_metadata_raft::sm::MigrationState,
) -> bool {
    record
        .dependent_partitions
        .iter()
        .all(|dependency| dependency_recoverable(options, dependency))
}

/// True when one dependent partition is a recoverable Q1 history of exactly the expected
/// identity and incarnation.
///
/// The check runs the real Q1 recovery on the destination: the WAL identity, the
/// incarnation, the manifest, the segment inventory, the record checksums and the committed
/// watermark are all validated. A file named `MANIFEST` is not proof of anything, and an
/// unrelated or corrupt history must never satisfy a dependency.
///
/// The committed terminal references themselves are verified when the source is opened with
/// this destination configured: the durable recovery checks every committed terminal
/// disposition against the destination, and the successor stays inactive when that fails.
pub fn dependency_recoverable(
    options: &crate::q1::Options,
    dependency: &rovqet_metadata_raft::types::DependentPartition,
) -> bool {
    let Some(dlq) = &options.dlq else {
        return false;
    };
    if dependency.tenant_id != dlq.partition.tenant_id
        || dependency.topic_id != dlq.partition.topic_id
        || dependency.partition_id != dlq.partition.partition_id
    {
        return false;
    }
    if dependency.incarnation != dlq.incarnation {
        return false;
    }
    // The durable class of a dependent Q1 partition is local durability.
    if dependency.durable_class != 1 {
        return false;
    }
    let config = rovqet_wal::Config {
        partition: dlq.partition,
        incarnation: dlq.incarnation,
        ..Default::default()
    };
    // The recovery validates the identity, the incarnation, the manifest, the segment
    // inventory, every checksum and the committed watermark, and it fails closed.
    rovqet_wal::Wal::open(&dlq.directory, config).is_ok()
}

#[cfg(test)]
mod dependency_tests {
    use super::*;
    use rovqet_core::{PartitionRef, QoS};
    use rovqet_durable::{Partition, PartitionConfig, PublishRequest};
    use rovqet_metadata_raft::sm::MigrationState;
    use rovqet_metadata_raft::types::DependentPartition;

    fn dlq_partition() -> PartitionRef {
        PartitionRef {
            tenant_id: 1,
            topic_id: 3,
            partition_id: 9,
        }
    }

    fn options(
        directory: std::path::PathBuf,
        dlq: Option<std::path::PathBuf>,
    ) -> crate::q1::Options {
        crate::q1::Options {
            directory,
            topic: "durable".into(),
            node_id: 2,
            partition: PartitionRef {
                tenant_id: 1,
                topic_id: 3,
                partition_id: 0,
            },
            incarnation: 42,
            owner_epoch: 8,
            dedup_window: std::time::Duration::from_secs(86400),
            max_producers: 64,
            max_outcomes: 64,
            initialize_new_log: false,
            terminal_policy: rovqet_durable::TerminalPolicy::Dlq,
            max_attempts: 3,
            retry_initial: std::time::Duration::from_millis(10),
            retry_max: std::time::Duration::from_millis(100),
            dlq: dlq.map(|directory| crate::q1::DlqOptions {
                directory,
                topic: "durable-dlq".into(),
                partition: dlq_partition(),
                incarnation: 43,
                owner_epoch: 8,
                max_outcomes: 64,
                max_producers: 64,
            }),
        }
    }

    /// A genuinely committed DLQ history: a real Q1 partition with a durable message.
    fn committed_dlq(directory: &std::path::Path) {
        let partition = Partition::open(
            directory,
            rovqet_wal::Config {
                partition: dlq_partition(),
                incarnation: 43,
                ..Default::default()
            },
            PartitionConfig {
                partition: dlq_partition(),
                ownership_epoch: 8,
                node_id: 2,
                queue_messages: 16,
                queue_bytes: 1024 * 1024,
                max_outcomes: 64,
                max_producers: 64,
                dedup_window: std::time::Duration::from_secs(86400),
                group: rovqet_durable::GroupConfig::default(),
            },
        )
        .expect("the DLQ partition opens");
        partition
            .try_publish(PublishRequest {
                partition: dlq_partition(),
                qos: QoS::Q1,
                ownership_epoch: 8,
                principal: 1,
                producer_id: 7,
                producer_epoch: 1,
                sequence: 0,
                message_id: 500,
                payload: b"dlq disposition".to_vec(),
            })
            .expect("the DLQ publication is admitted")
            .wait()
            .expect("the DLQ publication commits");
        partition.shutdown().expect("the DLQ partition shuts down");
    }

    fn record(dependent: Vec<DependentPartition>) -> MigrationState {
        MigrationState {
            migration_id: 7,
            tenant_id: 1,
            topic_id: 3,
            partition_id: 0,
            stage: MigrationStage::ActivateNew,
            source_node: 1,
            target_node: 2,
            source_incarnation: 42,
            original_epoch: 7,
            next_epoch: 8,
            transfer_watermark: Some(10),
            manifest_id: Some("abcd".into()),
            fence_proof: None,
            target_ready: true,
            dependent_partitions: dependent,
        }
    }

    fn dependency(incarnation: u128) -> DependentPartition {
        DependentPartition {
            tenant_id: 1,
            topic_id: 3,
            partition_id: 9,
            durable_class: 1,
            incarnation,
        }
    }

    #[test]
    fn a_migration_without_dependencies_needs_no_local_history() {
        let directory = tempfile::tempdir().expect("temp dir");
        let configured = options(directory.path().to_path_buf(), None);
        assert!(
            dependencies_available(&configured, &record(Vec::new())),
            "a migration without dependent partitions is not blocked"
        );
    }

    #[test]
    fn a_manifest_file_is_not_proof_of_a_recoverable_destination() {
        // The previous implementation accepted this: a file named MANIFEST with arbitrary
        // bytes. It must never satisfy a dependency.
        let directory = tempfile::tempdir().expect("temp dir");
        let dlq = tempfile::tempdir().expect("dlq");
        std::fs::write(dlq.path().join("MANIFEST"), b"manifest").expect("manifest");
        let configured = options(
            directory.path().to_path_buf(),
            Some(dlq.path().to_path_buf()),
        );
        assert!(
            !dependencies_available(&configured, &record(vec![dependency(43)])),
            "an arbitrary MANIFEST file must not prove a recoverable destination"
        );
    }

    #[test]
    fn a_committed_dlq_history_satisfies_the_dependency() {
        let directory = tempfile::tempdir().expect("temp dir");
        let dlq = tempfile::tempdir().expect("dlq");
        committed_dlq(dlq.path());
        let configured = options(
            directory.path().to_path_buf(),
            Some(dlq.path().to_path_buf()),
        );
        assert!(
            dependencies_available(&configured, &record(vec![dependency(43)])),
            "a genuinely committed DLQ history satisfies the dependency"
        );
    }

    #[test]
    fn a_destination_of_another_incarnation_is_refused() {
        let directory = tempfile::tempdir().expect("temp dir");
        let dlq = tempfile::tempdir().expect("dlq");
        committed_dlq(dlq.path());
        let configured = options(
            directory.path().to_path_buf(),
            Some(dlq.path().to_path_buf()),
        );
        assert!(
            !dependencies_available(&configured, &record(vec![dependency(99)])),
            "a dependency of another incarnation must be refused"
        );
    }

    #[test]
    fn a_destination_of_another_partition_is_refused() {
        let directory = tempfile::tempdir().expect("temp dir");
        let dlq = tempfile::tempdir().expect("dlq");
        committed_dlq(dlq.path());
        let configured = options(
            directory.path().to_path_buf(),
            Some(dlq.path().to_path_buf()),
        );
        let mut foreign = dependency(43);
        foreign.partition_id = 8;
        assert!(
            !dependencies_available(&configured, &record(vec![foreign])),
            "a dependency of another partition must be refused"
        );
    }

    #[test]
    fn a_corrupt_destination_segment_is_refused() {
        let directory = tempfile::tempdir().expect("temp dir");
        let dlq = tempfile::tempdir().expect("dlq");
        committed_dlq(dlq.path());
        // Corrupt a byte inside the committed segment.
        let segment = std::fs::read_dir(dlq.path())
            .expect("dir")
            .filter_map(|entry| entry.ok())
            .map(|entry| entry.path())
            .find(|path| {
                path.file_name()
                    .map(|name| name.to_string_lossy().starts_with("segment-"))
                    .unwrap_or(false)
            })
            .expect("a segment exists");
        let mut bytes = std::fs::read(&segment).expect("segment");
        let last = bytes.len() - 1;
        bytes[last] ^= 0xff;
        std::fs::write(&segment, &bytes).expect("segment");
        let configured = options(
            directory.path().to_path_buf(),
            Some(dlq.path().to_path_buf()),
        );
        assert!(
            !dependencies_available(&configured, &record(vec![dependency(43)])),
            "a corrupt destination segment must be refused"
        );
    }

    #[test]
    fn a_destination_without_its_manifest_is_refused() {
        let directory = tempfile::tempdir().expect("temp dir");
        let dlq = tempfile::tempdir().expect("dlq");
        committed_dlq(dlq.path());
        std::fs::remove_file(dlq.path().join("MANIFEST")).expect("remove manifest");
        let configured = options(
            directory.path().to_path_buf(),
            Some(dlq.path().to_path_buf()),
        );
        assert!(
            !dependencies_available(&configured, &record(vec![dependency(43)])),
            "a destination without its manifest must be refused"
        );
    }

    #[test]
    fn a_destination_of_another_partition_history_is_refused() {
        // A valid history of a different partition is not this dependency.
        let directory = tempfile::tempdir().expect("temp dir");
        let dlq = tempfile::tempdir().expect("dlq");
        let other = PartitionRef {
            tenant_id: 1,
            topic_id: 3,
            partition_id: 8,
        };
        let partition = Partition::open(
            dlq.path(),
            rovqet_wal::Config {
                partition: other,
                incarnation: 43,
                ..Default::default()
            },
            PartitionConfig {
                partition: other,
                ownership_epoch: 8,
                node_id: 2,
                queue_messages: 16,
                queue_bytes: 1024 * 1024,
                max_outcomes: 64,
                max_producers: 64,
                dedup_window: std::time::Duration::from_secs(86400),
                group: rovqet_durable::GroupConfig::default(),
            },
        )
        .expect("the other partition opens");
        partition.shutdown().expect("shutdown");
        let configured = options(
            directory.path().to_path_buf(),
            Some(dlq.path().to_path_buf()),
        );
        assert!(
            !dependencies_available(&configured, &record(vec![dependency(43)])),
            "a valid history of another partition must be refused"
        );
    }
}
