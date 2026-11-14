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
                    last = format!("{error:?}");
                    tokio::time::sleep(Duration::from_millis(200)).await;
                }
            }
        }
        Err(MigrationError::Metadata(last))
    }
}

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
    route: std::sync::Arc<crate::q1::Route>,
    node: u64,
    target_request: Option<(u64, u64, u64, u32, u128)>,
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
                if let Some((target_node, tenant_id, topic_id, partition_id, incarnation)) =
                    target_request
                {
                    // The request is repeated at a bounded rate: the metadata group may not
                    // have a leader yet.
                    let now = std::time::Instant::now();
                    if last_request
                        .map(|last| now.duration_since(last) < Duration::from_secs(5))
                        .unwrap_or(false)
                    {
                        return;
                    }
                    last_request = Some(now);
                    let _ = driver
                        .commit(
                            MetadataCommand::BeginMigration {
                                migration_id: migration_id(
                                    tenant_id,
                                    topic_id,
                                    partition_id,
                                    incarnation,
                                    node,
                                    target_node,
                                ),
                                tenant_id,
                                topic_id,
                                partition_id,
                                source_node: node,
                                target_node,
                                source_incarnation: incarnation,
                                original_epoch: state
                                    .placements
                                    .iter()
                                    .find(|placement| {
                                        placement.tenant_id == tenant_id
                                            && placement.topic_id == topic_id
                                            && placement.partition_id == partition_id
                                    })
                                    .map(|placement| placement.ownership_epoch)
                                    .unwrap_or(0),
                                next_epoch: state
                                    .placements
                                    .iter()
                                    .find(|placement| {
                                        placement.tenant_id == tenant_id
                                            && placement.topic_id == topic_id
                                            && placement.partition_id == partition_id
                                    })
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
                        return;
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
                    }
                }
                MigrationStage::FenceOld
                | MigrationStage::ActivateNew
                | MigrationStage::Cleanup => {}
            }
        }
        .await;
        if updates.changed().await.is_err() {
            return;
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
        {
            let state = raft.applied_state();
            let Some(placement) = state.placement(
                options.partition.tenant_id,
                options.partition.topic_id,
                options.partition.partition_id,
            ) else {
                // No committed placement: this node stays inactive.
                if updates.changed().await.is_err() {
                    return;
                }
                continue;
            };
            if placement.owner != node {
                if updates.changed().await.is_err() {
                    return;
                }
                continue;
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
                    if updates.changed().await.is_err() {
                        return;
                    }
                    continue;
                }
                // The dependent partitions must be available: a terminal disposition that
                // references a DLQ record needs a recoverable destination.
                if !dependencies_available(&route_options, record) {
                    eprintln!(
                        "the dependent history of the migration is not available locally; \
                         the successor stays inactive"
                    );
                    if updates.changed().await.is_err() {
                        return;
                    }
                    continue;
                }
                // The transferred storage is promoted before the route is opened.
                let Some(staging_root) = staging_root.clone() else {
                    if updates.changed().await.is_err() {
                        return;
                    }
                    continue;
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
                    if updates.changed().await.is_err() {
                        return;
                    }
                    continue;
                }
                // The transferred history keeps its incarnation.
                route_options.incarnation = record.source_incarnation;
            }
            let partition = route_options.partition;
            match crate::q1::Route::open(route_options) {
                Ok(route) => {
                    let route = std::sync::Arc::new(route);
                    if let Some(authority) = cluster.authority_for(&partition) {
                        route.install_authority(Some(authority));
                    }
                    route_slot.install(route);
                    return;
                }
                Err(error) => {
                    // A failed reopen leaves the node inactive and reports the error.
                    eprintln!("the Q1 route stays inactive: {error}");
                    if updates.changed().await.is_err() {
                        return;
                    }
                    continue;
                }
            }
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
    record.dependent_partitions.iter().all(|dependency| {
        let Some(dlq) = &options.dlq else {
            return false;
        };
        if dependency.tenant_id != dlq.partition.tenant_id
            || dependency.topic_id != dlq.partition.topic_id
            || dependency.partition_id != dlq.partition.partition_id
        {
            return false;
        }
        std::fs::canonicalize(&dlq.directory)
            .map(|path| path.join("MANIFEST").is_file())
            .unwrap_or(false)
    })
}
