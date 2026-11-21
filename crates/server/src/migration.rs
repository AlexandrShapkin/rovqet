//! The source side of a Q1 ownership migration.
//!
//! The driver observes the committed migration record and performs the stages in order:
//! it transfers the committed history, fences itself durably, transfers the final delta
//! and only then asks the metadata group to commit the activation. The committed metadata
//! is the only authority: a local timer never advances a stage. A process that dies after
//! the fence stage was committed but before the activation resumed the final transfer on
//! the committed fence proofs instead of stalling the handover for ever.
use crate::transfer::{bytes_to_hex, hex_to_bytes, sha256};
use rovqet_control::TimeoutBudget;
use rovqet_control::ids::NodeId;
use rovqet_control::rpc::{
    Q1TransferChunkRequest, Q1TransferChunkResponse, Q1TransferFile, Q1TransferFinalizeRequest,
    Q1TransferFinalizeResponse, Q1TransferMember, Q1TransferPrepareRequest,
    Q1TransferPrepareResponse, Q1TransferStatus, RpcErrorCode, RpcKind,
};
use rovqet_control::transport::PeerTransport;
use rovqet_core::{DurableOffset, PartitionRef};
use rovqet_metadata_raft::MetadataNode;
use rovqet_metadata_raft::sm::{MigrationState, PlacementState};
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

    /// Transfers one inventory of one bundle member to the target.
    ///
    /// The member is part of every request, so the two inventories of a bundle can never
    /// share a staging area on the receiver even when they describe identical files.
    #[allow(clippy::too_many_arguments)]
    pub async fn transfer_member(
        &self,
        target: NodeId,
        migration_id: u128,
        member: Q1TransferMember,
        directory: &Path,
        partition: rovqet_core::PartitionRef,
        incarnation: u128,
        original_epoch: u64,
        next_epoch: u64,
        manifest_generation: u64,
    ) -> Result<(Vec<InventoryFile>, Vec<u8>), MigrationError> {
        let files = partition_inventory(directory)?;
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
            member: member.as_u8(),
            source_node: self.node,
            target_node: target.get(),
            tenant_id: partition.tenant_id,
            topic_id: partition.topic_id,
            partition_id: partition.partition_id,
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
            let bytes = std::fs::read(directory.join(&file.name))
                .map_err(|error| MigrationError::Io(error.to_string()))?;
            let mut offset = 0usize;
            let mut chunk_index = 0u64;
            while offset < bytes.len() || (bytes.is_empty() && chunk_index == 0) {
                let end = (offset + TRANSFER_CHUNK_BYTES).min(bytes.len());
                let data = bytes[offset..end].to_vec();
                let chunk = Q1TransferChunkRequest {
                    migration_id,
                    member: member.as_u8(),
                    inventory_digest: inventory.clone(),
                    chunk_index,
                    file_index: index as u32,
                    offset: offset as u64,
                    digest: sha256(&data),
                    data,
                };
                // The sender announces the chunk before it hands it to the transport: the
                // parent can kill the sender in the middle of a transfer.
                crate::barrier::barrier("chunk_before_send");
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

    /// Asks the target to verify the transferred inventory of one bundle member.
    #[allow(clippy::too_many_arguments)]
    pub async fn finalize(
        &self,
        target: NodeId,
        migration_id: u128,
        member: Q1TransferMember,
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
            member: member.as_u8(),
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
    let dependencies: Vec<serde_json::Value> = record
        .dependent_evidence
        .iter()
        .map(|evidence| {
            serde_json::json!({
                "tenant_id": evidence.tenant_id,
                "topic_id": evidence.topic_id,
                "partition_id": evidence.partition_id,
                "incarnation": evidence.incarnation.to_string(),
                "original_epoch": evidence.original_epoch,
                "next_epoch": evidence.next_epoch,
                "transfer_watermark": evidence.transfer_watermark,
                "manifest_id": evidence.manifest_id,
                "fence": evidence.fence_proof.as_ref().map(|proof| serde_json::json!({
                    "migration_id": proof.migration_id.to_string(),
                    "old_owner": proof.old_owner,
                    "old_epoch": proof.old_epoch,
                    "fence_offset": proof.fence_offset,
                    "fence_digest": proof.fence_digest,
                    "manifest_generation": proof.manifest_generation,
                })),
                "ready": evidence.ready,
            })
        })
        .collect();
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
            "dependent_partitions": record.dependent_partitions.iter().map(|dependency| {
                serde_json::json!({
                    "tenant_id": dependency.tenant_id,
                    "topic_id": dependency.topic_id,
                    "partition_id": dependency.partition_id,
                    "durable_class": dependency.durable_class,
                    "incarnation": dependency.incarnation.to_string(),
                })
            }).collect::<Vec<_>>(),
            "dependencies": dependencies,
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

/// One verified member of a coordinated bundle.
///
/// A member carries the identity of the durable history it moves: the partition, the
/// incarnation that must survive unchanged, the committed epoch it holds today, the epoch
/// the successor owns after the activation, and the local directory of the history.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BundleMember {
    pub member: Q1TransferMember,
    pub partition: PartitionRef,
    pub incarnation: u128,
    pub original_epoch: u64,
    pub next_epoch: u64,
    pub directory: std::path::PathBuf,
}

/// The two members of a coordinated handover.
///
/// A source-only migration has no dependent member: the source alone moves. Nothing else in
/// the coordinator changes shape, so the source-only path keeps its committed semantics.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Bundle {
    pub source: BundleMember,
    pub dependent: Option<BundleMember>,
}

/// Resolves the committed dependency of a migration against the verified local configuration.
///
/// The resolved identity is never invented locally: the partition and incarnation come from
/// the committed dependency record of the metadata group, the epochs come from the committed
/// placements, and the local configuration must agree with both. A bundle whose dependency
/// is not configured here, is not the committed dependency, or holds another incarnation or
/// epoch is refused before a single byte moves.
pub fn resolve_bundle(
    record: &MigrationState,
    placements: &[PlacementState],
    route: &crate::q1::Route,
    source_directory: &Path,
) -> Result<Bundle, MigrationError> {
    let source_placement = placement_of(
        placements,
        record.tenant_id,
        record.topic_id,
        record.partition_id,
    )
    .ok_or_else(|| {
        MigrationError::Metadata("the source partition has no committed placement".into())
    })?;
    if source_placement.owner != record.source_node {
        return Err(MigrationError::Metadata(
            "the committed placement of the source is not this node".into(),
        ));
    }
    if source_placement.ownership_epoch != record.original_epoch {
        return Err(MigrationError::Metadata(
            "the committed source epoch is not the epoch the migration froze".into(),
        ));
    }
    let source = BundleMember {
        member: Q1TransferMember::Source,
        partition: PartitionRef {
            tenant_id: record.tenant_id,
            topic_id: record.topic_id,
            partition_id: record.partition_id,
        },
        incarnation: record.source_incarnation,
        original_epoch: record.original_epoch,
        next_epoch: record.next_epoch,
        directory: source_directory.to_path_buf(),
    };
    if record.dependent_partitions.is_empty() {
        return Ok(Bundle {
            source,
            dependent: None,
        });
    }
    let [dependency] = record.dependent_partitions.as_slice() else {
        return Err(MigrationError::Metadata(
            "a coordinated handover moves exactly one dependent partition".into(),
        ));
    };
    let identity = route.dlq_identity().ok_or_else(|| {
        MigrationError::Metadata(
            "the committed dependency of the migration is not configured on this node".into(),
        )
    })?;
    let expected = PartitionRef {
        tenant_id: dependency.tenant_id,
        topic_id: dependency.topic_id,
        partition_id: dependency.partition_id,
    };
    if identity.partition != expected {
        return Err(MigrationError::Metadata(format!(
            "the configured dependent partition {:?} is not the committed dependency {expected:?}",
            identity.partition
        )));
    }
    if identity.incarnation != dependency.incarnation {
        return Err(MigrationError::Metadata(format!(
            "the configured dependent incarnation {} is not the committed incarnation {}",
            identity.incarnation, dependency.incarnation
        )));
    }
    if dependency.durable_class != 1 {
        return Err(MigrationError::Metadata(
            "the dependent partition of a Q1 bundle must be locally durable".into(),
        ));
    }
    let dependent_placement = placement_of(
        placements,
        dependency.tenant_id,
        dependency.topic_id,
        dependency.partition_id,
    )
    .ok_or_else(|| {
        MigrationError::Metadata(
            "the dependent partition has no committed placement of its own".into(),
        )
    })?;
    if dependent_placement.owner != record.source_node {
        return Err(MigrationError::Metadata(
            "the committed placement of the dependent partition is not this node".into(),
        ));
    }
    if dependent_placement.ownership_epoch != identity.owner_epoch {
        return Err(MigrationError::Metadata(format!(
            "the configured dependent epoch {} is not the committed epoch {}",
            identity.owner_epoch, dependent_placement.ownership_epoch
        )));
    }
    let directory = route
        .dlq_directory()
        .map(std::path::Path::to_path_buf)
        .ok_or_else(|| {
            MigrationError::Metadata("the dependent partition has no local directory".into())
        })?;
    Ok(Bundle {
        source,
        dependent: Some(BundleMember {
            member: Q1TransferMember::Dependent,
            partition: expected,
            incarnation: dependency.incarnation,
            original_epoch: dependent_placement.ownership_epoch,
            // The dependent partition moves to the next epoch of its own committed
            // placement: never an epoch invented from the source side.
            next_epoch: dependent_placement.ownership_epoch + 1,
            directory,
        }),
    })
}

/// The committed placement of one partition.
fn placement_of(
    placements: &[PlacementState],
    tenant_id: u64,
    topic_id: u64,
    partition_id: u32,
) -> Option<&PlacementState> {
    placements.iter().find(|placement| {
        placement.tenant_id == tenant_id
            && placement.topic_id == topic_id
            && placement.partition_id == partition_id
    })
}

/// Converts the durable fence proof of the old owner into committed evidence.
fn committed_fence(proof: &rovqet_durable::FenceProof) -> FenceProof {
    FenceProof {
        migration_id: proof.migration_id,
        old_owner: proof.old_owner,
        old_epoch: proof.old_epoch,
        fence_offset: proof.fence_offset,
        fence_digest: proof.fence_digest.clone(),
        manifest_generation: proof.manifest_generation,
    }
}

/// Verifies that a returned fence proof really is the revocation of this handover.
///
/// A proof is only evidence when it is bound to the migration, to the old owner and to the
/// epoch that was revoked. A proof of another migration, node or epoch is never accepted.
fn verify_fence(
    proof: &rovqet_durable::FenceProof,
    migration_id: u128,
    node: u64,
    old_epoch: u64,
) -> Result<(), MigrationError> {
    if proof.migration_id != migration_id {
        return Err(MigrationError::Metadata(
            "the fence proof belongs to another migration".into(),
        ));
    }
    if proof.old_owner != node {
        return Err(MigrationError::Metadata(
            "the fence proof was produced by another node".into(),
        ));
    }
    if proof.old_epoch != old_epoch {
        return Err(MigrationError::Metadata(
            "the fence proof revoked another ownership epoch".into(),
        ));
    }
    if proof.fence_digest.is_empty() {
        return Err(MigrationError::Metadata(
            "the fence proof carries no durable digest".into(),
        ));
    }
    Ok(())
}

/// Verifies that a committed fence proof is the fence record of a promoted history.
///
/// The state machine binds a submitted proof to the migration, the old owner and the
/// revoked epoch, but only this check binds it to storage: the record at
/// `proof.fence_offset` of the promoted history must exist, must be a FENCE record, must
/// revoke exactly the migration, owner and epoch of the proof, and its payload digest
/// must be the `fence_digest` of the proof. A successor therefore never activates on a
/// transferred history whose committed fence proof was fabricated.
///
/// Every deviation — a missing record, another record kind, another identity or another
/// digest — is refused with a message that names it. The verification only reads
/// already-promoted storage and never panics.
pub fn verify_fence_against_history(
    directory: &Path,
    partition: PartitionRef,
    incarnation: u128,
    proof: &FenceProof,
) -> Result<(), String> {
    if !directory.join("MANIFEST").is_file() {
        return Err(format!(
            "the promoted history of partition {partition:?} incarnation {incarnation} \
             carries no manifest"
        ));
    }
    let config = rovqet_wal::Config {
        partition,
        incarnation,
        ..Default::default()
    };
    let max_record_bytes = config.max_record_bytes;
    let wal = rovqet_wal::Wal::open(directory, config).map_err(|error| {
        format!(
            "the promoted history of partition {partition:?} incarnation {incarnation} \
             is unreadable: {error}"
        )
    })?;
    let records = wal
        .read(
            DurableOffset {
                log_incarnation: incarnation,
                sequence: proof.fence_offset,
            },
            1,
            max_record_bytes,
        )
        .map_err(|error| {
            format!(
                "the fence record at offset {} of the promoted history is missing: {error}",
                proof.fence_offset
            )
        })?;
    let Some(record) = records.into_iter().next() else {
        return Err(format!(
            "the fence record at offset {} of the promoted history is missing",
            proof.fence_offset
        ));
    };
    if record.kind != rovqet_wal::RecordKind::Fence {
        return Err(format!(
            "the record at offset {} of the promoted history is not a fence record ({:?})",
            proof.fence_offset, record.kind
        ));
    }
    let (migration_id, old_owner, old_epoch, _) = rovqet_durable::decode_fence(&record.payload)
        .map_err(|error| {
            format!(
                "the fence record at offset {} of the promoted history is not decodable: \
                 {error}",
                proof.fence_offset
            )
        })?;
    if migration_id != proof.migration_id
        || old_owner != proof.old_owner
        || old_epoch != proof.old_epoch
    {
        return Err(format!(
            "the fence record at offset {} revokes migration {migration_id}, owner \
             {old_owner} epoch {old_epoch}, but the proof names migration {}, owner {} \
             epoch {}",
            proof.fence_offset, proof.migration_id, proof.old_owner, proof.old_epoch
        ));
    }
    let digest = rovqet_durable::fence_digest(&record.payload);
    if digest != proof.fence_digest {
        return Err(format!(
            "the fence digest of the proof ({}) is not the digest of the durable fence \
             record ({digest})",
            proof.fence_digest
        ));
    }
    Ok(())
}

/// One verified transferred inventory.
#[derive(Debug, Clone)]
struct TransferredInventory {
    watermark: u64,
    manifest_id: String,
}

/// Transfers one member of a bundle and lets the target verify the complete inventory.
async fn transfer_member(
    driver: &MigrationDriver,
    target: NodeId,
    record: &MigrationState,
    member: &BundleMember,
    manifest_generation: u64,
    fence: Option<&FenceProof>,
    watermark: u64,
) -> Result<TransferredInventory, MigrationError> {
    let (files, inventory) = driver
        .transfer_member(
            target,
            record.migration_id,
            member.member,
            &member.directory,
            member.partition,
            member.incarnation,
            member.original_epoch,
            member.next_epoch,
            manifest_generation,
        )
        .await?;
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
    let status = driver
        .finalize(
            target,
            record.migration_id,
            member.member,
            &inventory,
            digest,
            watermark,
            watermark,
            manifest_generation,
            fence,
            member.original_epoch,
        )
        .await?;
    let expected = if fence.is_some() {
        Q1TransferStatus::ReadyForActivation
    } else {
        Q1TransferStatus::ReadyForFence
    };
    if status != expected {
        return Err(MigrationError::Metadata(format!(
            "the target did not verify the {:?} inventory: {status:?}",
            member.member
        )));
    }
    Ok(TransferredInventory {
        watermark,
        manifest_id: crate::transfer::bytes_to_hex(&inventory),
    })
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
    options: crate::q1::Options,
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
                    // A configured DLQ that the terminal policy actually uses is a dependent
                    // partition of the bundle. It needs a committed placement of its own
                    // before the migration can record its identity.
                    let dependent = match &options.dlq {
                        Some(dlq)
                            if options.terminal_policy == rovqet_durable::TerminalPolicy::Dlq =>
                        {
                            let committed = placement_of(
                                &state.placements,
                                dlq.partition.tenant_id,
                                dlq.partition.topic_id,
                                dlq.partition.partition_id,
                            );
                            match committed {
                                Some(placement)
                                    if placement.owner == node
                                        && placement.ownership_epoch == dlq.owner_epoch => {}
                                Some(placement) => {
                                    eprintln!(
                                        "the committed DLQ placement is owner={} epoch={}, \
                                         the configured dependency is owner={} epoch={}; \
                                         the handover stays blocked",
                                        placement.owner,
                                        placement.ownership_epoch,
                                        node,
                                        dlq.owner_epoch
                                    );
                                    return;
                                }
                                None => {
                                    // The placement of the dependency is committed exactly
                                    // like the placement of the source: it is authoritative
                                    // metadata, never a local assumption.
                                    if driver
                                        .commit(
                                            MetadataCommand::SetPlacement {
                                                tenant_id: dlq.partition.tenant_id,
                                                topic_id: dlq.partition.topic_id,
                                                partition_id: dlq.partition.partition_id,
                                                owner: node,
                                                ownership_epoch: dlq.owner_epoch,
                                                lease_valid_for_ms: 600_000,
                                            },
                                            Duration::from_secs(30),
                                        )
                                        .await
                                        .is_err()
                                    {
                                        return;
                                    }
                                    last_request = None;
                                    return;
                                }
                            }
                            Some(rovqet_metadata_raft::types::DependentPartition {
                                tenant_id: dlq.partition.tenant_id,
                                topic_id: dlq.partition.topic_id,
                                partition_id: dlq.partition.partition_id,
                                durable_class: 1,
                                incarnation: dlq.incarnation,
                            })
                        }
                        _ => None,
                    };
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
                                dependent_partitions: dependent.into_iter().collect(),
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
                    match prepare_stage(&driver, &route, &state.placements, &record, target).await {
                        Ok(()) => {}
                        Err(error) => {
                            eprintln!("metadata migration preparation failed: {error}");
                            return;
                        }
                    }
                }
                MigrationStage::CatchUp => {
                    match catch_up_stage(&driver, &route, &state.placements, &record, target, node)
                        .await
                    {
                        Ok(()) => {}
                        Err(error) => {
                            eprintln!("metadata migration handover failed: {error}");
                            return;
                        }
                    }
                }
                MigrationStage::FenceOld => {
                    // CRASH RESUME: a committed fence without a committed activation is
                    // exactly the interrupted window of the final post-fence delta. The
                    // committed proofs are the authority on this path: it never quiesces
                    // and never fences again, it only repeats the final transfer of both
                    // members on the proofs the metadata group already committed and
                    // commits the one activation the fresh path would have committed.
                    let Some(source_fence) = record.fence_proof.clone() else {
                        eprintln!(
                            "the committed fence stage of migration {} carries no source \
                             fence proof; the handover stays interrupted",
                            record.migration_id
                        );
                        return;
                    };
                    let dependent_fence = if record.dependent_partitions.is_empty() {
                        None
                    } else {
                        let [dependency] = record.dependent_partitions.as_slice() else {
                            eprintln!(
                                "a coordinated handover moves exactly one dependent \
                                 partition; the handover stays interrupted"
                            );
                            return;
                        };
                        let evidence = record.dependent_evidence.iter().find(|evidence| {
                            evidence.tenant_id == dependency.tenant_id
                                && evidence.topic_id == dependency.topic_id
                                && evidence.partition_id == dependency.partition_id
                        });
                        match evidence.and_then(|evidence| evidence.fence_proof.clone()) {
                            Some(proof) => Some(proof),
                            None => {
                                eprintln!(
                                    "the committed fence stage of migration {} carries no \
                                     dependent fence proof; the handover stays interrupted",
                                    record.migration_id
                                );
                                return;
                            }
                        }
                    };
                    match finalize_stage(
                        &driver,
                        &route,
                        &state.placements,
                        &record,
                        target,
                        &source_fence,
                        dependent_fence.as_ref(),
                    )
                    .await
                    {
                        Ok(()) => {}
                        Err(error) => {
                            eprintln!("metadata migration finalization failed: {error}");
                            return;
                        }
                    }
                }
                MigrationStage::ActivateNew | MigrationStage::Cleanup => {}
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

/// Reports one bundle member on the diagnostic stream.
fn report_member(
    event: &str,
    migration_id: u128,
    member: &BundleMember,
    fields: serde_json::Value,
) {
    let mut object = serde_json::json!({
        "migration_id": migration_id.to_string(),
        "member": member.member.label(),
        "tenant_id": member.partition.tenant_id,
        "topic_id": member.partition.topic_id,
        "partition_id": member.partition.partition_id,
        "incarnation": member.incarnation.to_string(),
        "original_epoch": member.original_epoch,
        "next_epoch": member.next_epoch,
    });
    if let (Some(target), Some(source)) = (object.as_object_mut(), fields.as_object()) {
        for (key, value) in source {
            target.insert(key.clone(), value.clone());
        }
    }
    eprintln!("MIGRATION {event} {object}");
}

/// The committed dependency evidence of one bundle member, if the bundle has one.
fn dependent_evidence(
    bundle: &Bundle,
    fence: Option<FenceProof>,
    transfer_watermark: Option<u64>,
    manifest_id: Option<String>,
    ready: bool,
) -> Vec<rovqet_metadata_raft::types::DependentEvidence> {
    bundle
        .dependent
        .iter()
        .map(|member| rovqet_metadata_raft::types::DependentEvidence {
            tenant_id: member.partition.tenant_id,
            topic_id: member.partition.topic_id,
            partition_id: member.partition.partition_id,
            incarnation: member.incarnation,
            original_epoch: member.original_epoch,
            next_epoch: member.next_epoch,
            transfer_watermark,
            manifest_id: manifest_id.clone(),
            fence_proof: fence.clone(),
            ready,
        })
        .collect()
}

/// PREPARE: transfer both inventories and commit the verified preliminary evidence.
async fn prepare_stage(
    driver: &MigrationDriver,
    route: &crate::q1::Route,
    placements: &[PlacementState],
    record: &MigrationState,
    target: NodeId,
) -> Result<(), MigrationError> {
    let bundle = resolve_bundle(record, placements, route, driver.directory())?;
    // The source inventory is declared, chunked and verified by the target.
    let source_watermark = route.committed_count();
    let source = transfer_member(
        driver,
        target,
        record,
        &bundle.source,
        0,
        None,
        source_watermark,
    )
    .await?;
    report_member(
        "inventory",
        record.migration_id,
        &bundle.source,
        serde_json::json!({
            "phase": "catch_up",
            "watermark": source.watermark,
            "manifest_id": source.manifest_id,
        }),
    );
    // The dependent inventory is a second, independently identified history. It shares the
    // parent migration identity but never its staging identity.
    let mut dependencies = Vec::new();
    if let Some(dependent) = &bundle.dependent {
        let watermark = route.dlq_committed_count().ok_or_else(|| {
            MigrationError::Metadata("the dependent log has no committed position".into())
        })?;
        let transferred =
            transfer_member(driver, target, record, dependent, 0, None, watermark).await?;
        report_member(
            "inventory",
            record.migration_id,
            dependent,
            serde_json::json!({
                "phase": "catch_up",
                "watermark": transferred.watermark,
                "manifest_id": transferred.manifest_id,
            }),
        );
        dependencies = dependent_evidence(
            &bundle,
            None,
            Some(transferred.watermark),
            Some(transferred.manifest_id),
            true,
        );
    }
    // The verified inventory is durable on the target, but the stage transition is not
    // committed yet: a crash here must not fabricate the missing Raft transition.
    crate::barrier::barrier("before_catch_up_commit");
    let command = MetadataCommand::AdvanceMigration {
        migration_id: record.migration_id,
        stage: MigrationStage::CatchUp,
        transfer_watermark: Some(source.watermark),
        manifest_id: Some(source.manifest_id),
        fence_proof: None,
        target_ready: false,
        dependencies,
    };
    driver.commit(command, Duration::from_secs(30)).await?;
    if let Some(updated) = driver
        .raft()
        .applied_state()
        .migrations
        .get(&record.migration_id)
    {
        report("committed", updated, driver.raft().applied_state().revision);
    }
    Ok(())
}

/// CATCH_UP: quiesce, fence both members, commit the fences and activate on verified evidence.
///
/// The two durable fences are ordered deliberately:
///
/// 1. the terminal path of the source is quiesced, so no new disposition can cross to the
///    dependent log;
/// 2. the source is fenced durably, so the source cannot append anything at all;
/// 3. the dependent is fenced durably, so no publication can reach it afterwards.
///
/// A failure between the two fences leaves the source fenced and the migration at CATCH_UP:
/// the coordinator retries and the repeated fence returns the same durable proof.
///
/// The tail of this stage — the final post-fence transfer of both members and the committed
/// activation — is [`finalize_stage`], the same step the coordinator runs when it restarts
/// onto an already committed `FENCE_OLD` stage.
async fn catch_up_stage(
    driver: &MigrationDriver,
    route: &crate::q1::Route,
    placements: &[PlacementState],
    record: &MigrationState,
    target: NodeId,
    node: u64,
) -> Result<(), MigrationError> {
    let bundle = resolve_bundle(record, placements, route, driver.directory())?;
    // 1. The bundle quiescence barrier.
    crate::barrier::barrier("before_fence");
    let quiesce = route
        .quiesce_terminal()
        .await
        .map_err(|error| MigrationError::Io(format!("the quiescence barrier failed: {error:?}")))?;
    eprintln!(
        "MIGRATION quiesced {}",
        serde_json::json!({
            "migration_id": record.migration_id.to_string(),
            "source_watermark": quiesce.watermark,
            "pending_terminal": quiesce.pending,
        })
    );
    // 2. The source revokes its authority durably.
    let source_fence = route
        .fence_old_owner(
            record.migration_id,
            record.original_epoch,
            record.next_epoch,
        )
        .await
        .map_err(|error| MigrationError::Io(format!("the source fence failed: {error:?}")))?;
    verify_fence(
        &source_fence,
        record.migration_id,
        node,
        record.original_epoch,
    )?;
    let source_fence = committed_fence(&source_fence);
    report_member(
        "fence",
        record.migration_id,
        &bundle.source,
        serde_json::json!({
            "fence_offset": source_fence.fence_offset,
            "fence_digest": source_fence.fence_digest,
            "manifest_generation": source_fence.manifest_generation,
        }),
    );
    // 3. The dependent revokes its authority durably. Without it the bundle never advances.
    let dependent_fence = match &bundle.dependent {
        Some(dependent) => {
            let proof = route
                .fence_dlq_old_owner(
                    record.migration_id,
                    dependent.original_epoch,
                    dependent.next_epoch,
                )
                .await
                .map_err(|error| {
                    MigrationError::Io(format!("the dependent fence failed: {error:?}"))
                })?;
            verify_fence(&proof, record.migration_id, node, dependent.original_epoch)?;
            let proof = committed_fence(&proof);
            report_member(
                "fence",
                record.migration_id,
                dependent,
                serde_json::json!({
                    "fence_offset": proof.fence_offset,
                    "fence_digest": proof.fence_digest,
                    "manifest_generation": proof.manifest_generation,
                }),
            );
            Some(proof)
        }
        None => None,
    };
    // 4. Both fences are committed together. The dependent evidence keeps the preliminary
    // transfer evidence that was already verified in the prepare stage.
    let preliminary = record
        .dependent_evidence
        .first()
        .map(|evidence| (evidence.transfer_watermark, evidence.manifest_id.clone()));
    let fence_command = MetadataCommand::AdvanceMigration {
        migration_id: record.migration_id,
        stage: MigrationStage::FenceOld,
        transfer_watermark: record.transfer_watermark,
        manifest_id: record.manifest_id.clone(),
        fence_proof: Some(source_fence.clone()),
        target_ready: false,
        dependencies: dependent_evidence(
            &bundle,
            dependent_fence.clone(),
            preliminary.as_ref().and_then(|(watermark, _)| *watermark),
            preliminary
                .as_ref()
                .and_then(|(_, manifest)| manifest.clone()),
            true,
        ),
    };
    crate::barrier::barrier("after_fence");
    driver
        .commit(fence_command, Duration::from_secs(30))
        .await?;
    if let Some(updated) = driver
        .raft()
        .applied_state()
        .migrations
        .get(&record.migration_id)
    {
        report("committed", updated, driver.raft().applied_state().revision);
    }
    // 5..7. The final inventories and the committed activation: the very same step the
    // coordinator resumes with when this process dies after the committed fence stage.
    finalize_stage(
        driver,
        route,
        placements,
        record,
        target,
        &source_fence,
        dependent_fence.as_ref(),
    )
    .await
}

/// FINALE: transfer the final inventories on existing fence evidence and commit the
/// activation.
///
/// This step never quiesces and never fences: the two durable fences are an input, not an
/// action. On the fresh path the caller just minted and committed them; on the resumed path
/// (the committed stage is already `FENCE_OLD`) the caller passes the committed proofs of
/// the record itself, because a committed fence is the authority and a restart must never
/// mint a second one. Both paths therefore commit exactly one identical
/// `AdvanceMigration { stage: ActivateNew, .. }`, so the state machine's idempotency and
/// dependency checks behave the same for a fresh handover and for a resumed one.
async fn finalize_stage(
    driver: &MigrationDriver,
    route: &crate::q1::Route,
    placements: &[PlacementState],
    record: &MigrationState,
    target: NodeId,
    source_fence: &FenceProof,
    dependent_fence: Option<&FenceProof>,
) -> Result<(), MigrationError> {
    let bundle = resolve_bundle(record, placements, route, driver.directory())?;
    // 5. Final catch up: the logs are final now, so both watermarks are stable.
    // The fences are already durable: a crash here leaves both old partitions fenced and
    // the target without the final history.
    crate::barrier::barrier("before_final_transfer");
    let source_watermark = route.committed_count();
    let source = transfer_member(
        driver,
        target,
        record,
        &bundle.source,
        source_fence.manifest_generation,
        Some(source_fence),
        source_watermark,
    )
    .await?;
    report_member(
        "inventory",
        record.migration_id,
        &bundle.source,
        serde_json::json!({
            "phase": "final",
            "watermark": source.watermark,
            "manifest_id": source.manifest_id,
        }),
    );
    let mut dependencies = Vec::new();
    if let Some(dependent) = &bundle.dependent {
        let watermark = route.dlq_committed_count().ok_or_else(|| {
            MigrationError::Metadata("the dependent log has no committed position".into())
        })?;
        let transferred = transfer_member(
            driver,
            target,
            record,
            dependent,
            dependent_fence
                .map(|proof| proof.manifest_generation)
                .unwrap_or_default(),
            dependent_fence,
            watermark,
        )
        .await?;
        report_member(
            "inventory",
            record.migration_id,
            dependent,
            serde_json::json!({
                "phase": "final",
                "watermark": transferred.watermark,
                "manifest_id": transferred.manifest_id,
            }),
        );
        // 6. Every committed terminal disposition of DLQ kind must reference a destination
        // record that exists with exactly the recorded identity. A manifest alone is never
        // proof of a terminal destination.
        let references = route
            .terminal_references()
            .await
            .map_err(|error| MigrationError::Io(format!("the terminal audit failed: {error:?}")))?;
        for reference in &references {
            let verified = route
                .verify_dlq_reference(reference)
                .await
                .map_err(|error| {
                    MigrationError::Io(format!("the dependent destination failed: {error:?}"))
                })?;
            if !verified {
                return Err(MigrationError::Metadata(format!(
                    "the committed terminal disposition {} has no matching dependent record",
                    reference.disposition_id
                )));
            }
            eprintln!(
                "MIGRATION terminal {}",
                serde_json::json!({
                    "migration_id": record.migration_id.to_string(),
                    "disposition_id": reference.disposition_id.to_string(),
                    "source_offset": reference.source_offset,
                    "dlq_incarnation": reference.destination_incarnation.to_string(),
                    "dlq_offset": reference.destination_offset,
                    "original_message_id": reference.original_message_id.to_string(),
                    "verified": true,
                })
            );
        }
        dependencies = dependent_evidence(
            &bundle,
            dependent_fence.cloned(),
            Some(transferred.watermark),
            Some(transferred.manifest_id),
            true,
        );
    }
    // 7. One committed activation moves both placements. The source never activates while a
    // dependency is incomplete.
    let activation = MetadataCommand::AdvanceMigration {
        migration_id: record.migration_id,
        stage: MigrationStage::ActivateNew,
        transfer_watermark: Some(source.watermark),
        manifest_id: Some(source.manifest_id),
        fence_proof: None,
        target_ready: true,
        dependencies,
    };
    crate::barrier::barrier("before_activate_commit");
    driver.commit(activation, Duration::from_secs(30)).await?;
    if let Some(updated) = driver
        .raft()
        .applied_state()
        .migrations
        .get(&record.migration_id)
    {
        report("committed", updated, driver.raft().applied_state().revision);
    }
    Ok(())
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
                let Some(staging_root) = staging_root.clone() else {
                    return None;
                };
                // A coordinated bundle promotes both histories before either route opens.
                if !record.dependent_partitions.is_empty() {
                    match promote_bundle(
                        record,
                        &route_options,
                        &staging_root,
                        state.revision,
                        placement.ownership_epoch,
                    ) {
                        Ok(()) => {
                            // The promoted dependent history must be recoverable before the
                            // source may open against it.
                            if !dependencies_available(&route_options, record) {
                                eprintln!(
                                    "the promoted dependent history of the bundle is not \
                                     recoverable; the successor stays inactive"
                                );
                                return None;
                            }
                            // The committed fence proof of the source member must be the
                            // fence record the promoted source history really carries:
                            // a fabricated proof never opens the route.
                            let Some(fence) = record.fence_proof.as_ref() else {
                                eprintln!(
                                    "the committed activation carries no source fence \
                                     proof; the successor stays inactive"
                                );
                                return None;
                            };
                            if let Err(error) = verify_fence_against_history(
                                &route_options.directory,
                                options.partition,
                                record.source_incarnation,
                                fence,
                            ) {
                                eprintln!(
                                    "the committed fence proof of the source member was \
                                     refused: {error}; the successor stays inactive"
                                );
                                return None;
                            }
                            // The dependent member is verified against its own promoted
                            // DLQ history: its fence offset names the DLQ log.
                            let Some(dependency) = record.dependent_partitions.first() else {
                                eprintln!(
                                    "the committed bundle carries no dependent partition; \
                                     the successor stays inactive"
                                );
                                return None;
                            };
                            let Some(evidence) =
                                record.dependent_evidence.iter().find(|evidence| {
                                    evidence.tenant_id == dependency.tenant_id
                                        && evidence.topic_id == dependency.topic_id
                                        && evidence.partition_id == dependency.partition_id
                                })
                            else {
                                eprintln!(
                                    "the committed activation carries no evidence of the \
                                     dependent member; the successor stays inactive"
                                );
                                return None;
                            };
                            let Some(dependent_fence) = evidence.fence_proof.as_ref() else {
                                eprintln!(
                                    "the committed activation carries no dependent fence \
                                     proof; the successor stays inactive"
                                );
                                return None;
                            };
                            let Some(dlq) = route_options.dlq.as_ref() else {
                                eprintln!(
                                    "the committed dependency is not configured on this \
                                     successor; the successor stays inactive"
                                );
                                return None;
                            };
                            if let Err(error) = verify_fence_against_history(
                                &dlq.directory,
                                dlq.partition,
                                evidence.incarnation,
                                dependent_fence,
                            ) {
                                eprintln!(
                                    "the committed fence proof of the dependent member was \
                                     refused: {error}; the successor stays inactive"
                                );
                                return None;
                            }
                        }
                        Err(error) => {
                            eprintln!("the bundle promotion was refused: {error}");
                            return None;
                        }
                    }
                    let evidence = record.dependent_evidence.first();
                    if let Some(evidence) = evidence {
                        route_options.incarnation = record.source_incarnation;
                        if let Some(dlq) = route_options.dlq.as_mut() {
                            if let Some(epoch) = evidence.next_epoch.checked_sub(0) {
                                dlq.owner_epoch = epoch;
                            }
                        }
                    }
                    crate::barrier::barrier("before_route_open");
                } else {
                    // The dependent partitions must be available: a terminal disposition
                    // that references a DLQ record needs a recoverable destination.
                    if !dependencies_available(&route_options, record) {
                        eprintln!(
                            "the dependent history of the migration is not available locally; \
                             the successor stays inactive"
                        );
                        return None;
                    }
                    // The transferred storage is promoted before the route is opened.
                    let inventory = hex_to_bytes(record.manifest_id.as_deref().unwrap_or_default());
                    if inventory.len() != 32 {
                        eprintln!("the committed inventory identity of the migration is invalid");
                        return None;
                    }
                    let staging = crate::promotion::staging_area(
                        &staging_root,
                        record.migration_id,
                        Q1TransferMember::Source,
                        &inventory,
                    );
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
                    // The committed fence proof must be the fence record the promoted
                    // source history really carries: a fabricated proof never opens the
                    // route.
                    let Some(fence) = record.fence_proof.as_ref() else {
                        eprintln!(
                            "the committed activation carries no fence proof; \
                             the successor stays inactive"
                        );
                        return None;
                    };
                    if let Err(error) = verify_fence_against_history(
                        &route_options.directory,
                        options.partition,
                        record.source_incarnation,
                        fence,
                    ) {
                        eprintln!(
                            "the committed fence proof was refused: {error}; \
                             the successor stays inactive"
                        );
                        return None;
                    }
                    // The transferred history keeps its incarnation.
                    route_options.incarnation = record.source_incarnation;
                    crate::barrier::barrier("before_route_open");
                }
            }
            let partition = route_options.partition;
            let dlq_partition = route_options.dlq.as_ref().map(|dlq| dlq.partition);
            match crate::q1::Route::open(route_options) {
                Ok(route) => {
                    let route = std::sync::Arc::new(route);
                    eprintln!(
                        "Q1 route active: partition={:?} epoch={} incarnation={}",
                        partition, route.options.owner_epoch, route.options.incarnation
                    );
                    if let Some(dlq_partition) = dlq_partition {
                        eprintln!(
                            "Q1 DLQ route active: partition={:?} epoch={} incarnation={}",
                            dlq_partition,
                            route
                                .dlq_configured_authority()
                                .map(|value| value.0)
                                .unwrap_or(0),
                            route
                                .dlq_configured_authority()
                                .map(|value| value.1)
                                .unwrap_or(0)
                        );
                    }
                    crate::barrier::barrier("after_route_open");
                    // The successor installs the committed authority of both partitions only
                    // after both histories were recovered.
                    if let Some(authority) = cluster.authority_for(&partition) {
                        route.install_authority(Some(authority));
                    }
                    if let Some(dlq_partition) = dlq_partition {
                        route.install_dlq_authority(cluster.authority_for(&dlq_partition));
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

/// Promotes both inventories of a committed bundle, dependent first.
///
/// The two promotions happen in two independent directories: this is deliberately not one
/// atomic filesystem transaction. The order is what makes it safe: the dependent history is
/// published and verified before the source is published, so a crash between the two leaves
/// the source directory without its history and the successor inactive until the remaining
/// promotion completes.
fn promote_bundle(
    record: &MigrationState,
    options: &crate::q1::Options,
    staging_root: &Path,
    activation_revision: u64,
    target_epoch: u64,
) -> Result<(), String> {
    if !record.target_ready {
        return Err("the committed activation carries no target readiness".into());
    }
    let [dependency] = record.dependent_partitions.as_slice() else {
        return Err("a coordinated handover moves exactly one dependent partition".into());
    };
    let Some(dlq) = &options.dlq else {
        return Err("the committed dependency is not configured on this successor".into());
    };
    if dlq.partition.tenant_id != dependency.tenant_id
        || dlq.partition.topic_id != dependency.topic_id
        || dlq.partition.partition_id != dependency.partition_id
    {
        return Err("the configured dependent partition is not the committed dependency".into());
    }
    if dlq.incarnation != dependency.incarnation {
        return Err("the configured dependent incarnation is not the committed dependency".into());
    }
    let Some(evidence) = record.dependent_evidence.iter().find(|evidence| {
        evidence.tenant_id == dependency.tenant_id
            && evidence.topic_id == dependency.topic_id
            && evidence.partition_id == dependency.partition_id
    }) else {
        return Err("the committed activation carries no evidence of the dependency".into());
    };
    if !evidence.ready || evidence.fence_proof.is_none() || evidence.transfer_watermark.is_none() {
        return Err("the committed evidence of the dependency is incomplete".into());
    }
    let fence = evidence
        .fence_proof
        .as_ref()
        .expect("the fence was checked above");
    if fence.migration_id != record.migration_id
        || fence.old_owner != record.source_node
        || fence.old_epoch != evidence.original_epoch
    {
        return Err("the committed fence of the dependency does not match the bundle".into());
    }
    let source_inventory = hex_to_bytes(record.manifest_id.as_deref().unwrap_or_default());
    if source_inventory.len() != 32 {
        return Err("the committed source inventory identity is invalid".into());
    }
    let dependent_inventory = hex_to_bytes(evidence.manifest_id.as_deref().unwrap_or_default());
    if dependent_inventory.len() != 32 {
        return Err("the committed dependent inventory identity is invalid".into());
    }
    if dependent_inventory == source_inventory {
        return Err("the two members of the bundle carry the same inventory identity".into());
    }
    // The durable readiness record: it states which promotion completed, so an interrupted
    // paired promotion is resumed instead of being guessed.
    let mut readiness = crate::promotion::BundleReadiness::load(staging_root, record.migration_id)
        .map_err(|error| format!("the bundle readiness record is unreadable: {error}"))?
        .unwrap_or(crate::promotion::BundleReadiness {
            migration_id: record.migration_id,
            activation_revision,
            target_epoch,
            dependent_inventory_digest: bytes_to_hex(&dependent_inventory),
            dependent_incarnation: evidence.incarnation,
            dependent_partition: [
                dependency.tenant_id,
                dependency.topic_id,
                u64::from(dependency.partition_id),
            ],
            dependent_promoted: false,
            source_inventory_digest: bytes_to_hex(&source_inventory),
            source_incarnation: record.source_incarnation,
            source_partition: [
                record.tenant_id,
                record.topic_id,
                u64::from(record.partition_id),
            ],
            source_promoted: false,
        });
    if readiness.dependent_inventory_digest != bytes_to_hex(&dependent_inventory)
        || readiness.source_inventory_digest != bytes_to_hex(&source_inventory)
        || readiness.dependent_incarnation != evidence.incarnation
        || readiness.source_incarnation != record.source_incarnation
    {
        return Err("the durable bundle readiness belongs to another activation".into());
    }
    let dependent_staging = crate::promotion::staging_area(
        staging_root,
        record.migration_id,
        Q1TransferMember::Dependent,
        &dependent_inventory,
    );
    let source_staging = crate::promotion::staging_area(
        staging_root,
        record.migration_id,
        Q1TransferMember::Source,
        &source_inventory,
    );
    // 1. The dependent history is published first, into its own configured directory.
    if !readiness.dependent_promoted {
        std::fs::create_dir_all(&dlq.directory)
            .map_err(|error| format!("the dependent directory is unavailable: {error}"))?;
        let plan = crate::promotion::PromotionRecord {
            migration_id: record.migration_id,
            inventory_digest: readiness.dependent_inventory_digest.clone(),
            incarnation: evidence.incarnation,
            tenant_id: dependency.tenant_id,
            topic_id: dependency.topic_id,
            partition_id: dependency.partition_id,
            target_epoch: evidence.next_epoch,
            activation_revision,
            files: Vec::new(),
            state: crate::promotion::PromotionState::Promoted,
        };
        crate::promotion::promote_with_manifest(&dependent_staging, &dlq.directory, &plan)
            .map_err(|error| format!("the dependent promotion was refused: {error}"))?;
        crate::barrier::barrier("after_dependent_promotion");
        readiness.dependent_promoted = true;
        readiness
            .store(staging_root)
            .map_err(|error| format!("the bundle readiness record was not stored: {error}"))?;
    }
    verify_promotion(
        &dlq.directory,
        record.migration_id,
        &evidence.incarnation,
        dependency.tenant_id,
        dependency.topic_id,
        dependency.partition_id,
    )?;
    // 2. Only then the source history is published.
    if !readiness.source_promoted {
        crate::barrier::barrier("before_source_promotion");
        let plan = crate::promotion::PromotionRecord {
            migration_id: record.migration_id,
            inventory_digest: readiness.source_inventory_digest.clone(),
            incarnation: record.source_incarnation,
            tenant_id: record.tenant_id,
            topic_id: record.topic_id,
            partition_id: record.partition_id,
            target_epoch: record.next_epoch,
            activation_revision,
            files: Vec::new(),
            state: crate::promotion::PromotionState::Promoted,
        };
        crate::promotion::promote_with_manifest(&source_staging, &options.directory, &plan)
            .map_err(|error| format!("the source promotion was refused: {error}"))?;
        crate::barrier::barrier("after_source_promotion");
        readiness.source_promoted = true;
        readiness
            .store(staging_root)
            .map_err(|error| format!("the bundle readiness record was not stored: {error}"))?;
    }
    verify_promotion(
        &options.directory,
        record.migration_id,
        &record.source_incarnation,
        record.tenant_id,
        record.topic_id,
        record.partition_id,
    )?;
    eprintln!(
        "MIGRATION promoted {}",
        serde_json::json!({
            "migration_id": record.migration_id.to_string(),
            "activation_revision": activation_revision,
            "target_epoch": target_epoch,
            "dependent_inventory_digest": bytes_to_hex(&dependent_inventory),
            "dependent_incarnation": evidence.incarnation.to_string(),
            "dependent_partition": readiness.dependent_partition,
            "dependent_promoted": readiness.dependent_promoted,
            "source_inventory_digest": bytes_to_hex(&source_inventory),
            "source_incarnation": record.source_incarnation.to_string(),
            "source_partition": readiness.source_partition,
            "source_promoted": readiness.source_promoted,
        })
    );
    Ok(())
}

/// Verifies one completed durable promotion record.
fn verify_promotion(
    directory: &Path,
    migration_id: u128,
    incarnation: &u128,
    tenant_id: u64,
    topic_id: u64,
    partition_id: u32,
) -> Result<(), String> {
    let record = crate::promotion::promotion_record(directory)
        .map_err(|error| format!("the promotion record is unreadable: {error}"))?
        .ok_or_else(|| "the promotion record is missing".to_string())?;
    if record.state != crate::promotion::PromotionState::Promoted {
        return Err("the promotion did not complete".into());
    }
    if record.migration_id != migration_id
        || record.incarnation != *incarnation
        || record.tenant_id != tenant_id
        || record.topic_id != topic_id
        || record.partition_id != partition_id
    {
        return Err("the promotion record belongs to another activation".into());
    }
    Ok(())
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
            dependent_evidence: Vec::new(),
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

#[cfg(test)]
mod bundle_tests {
    //! The negative cases of a coordinated bundle activation.
    //!
    //! Every unsafe configuration must leave the source route inactive on the target: a
    //! bundle that cannot be promoted completely never opens the source partition.
    use super::*;
    use rovqet_core::PartitionRef;

    const MIGRATION: u128 = 0x5151_5151_5151_5151;

    fn source_partition() -> PartitionRef {
        PartitionRef {
            tenant_id: 1,
            topic_id: 3,
            partition_id: 0,
        }
    }

    fn dependent_partition() -> PartitionRef {
        PartitionRef {
            tenant_id: 1,
            topic_id: 7,
            partition_id: 0,
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
            partition: source_partition(),
            incarnation: 42,
            owner_epoch: 8,
            dedup_window: std::time::Duration::from_secs(86400),
            max_producers: 64,
            max_outcomes: 64,
            initialize_new_log: false,
            terminal_policy: rovqet_durable::TerminalPolicy::Dlq,
            max_attempts: 1,
            retry_initial: std::time::Duration::from_millis(10),
            retry_max: std::time::Duration::from_millis(100),
            dlq: dlq.map(|directory| crate::q1::DlqOptions {
                directory,
                topic: "durable-dlq".into(),
                partition: dependent_partition(),
                incarnation: 77,
                owner_epoch: 8,
                max_outcomes: 64,
                max_producers: 64,
            }),
        }
    }

    /// A committed activation of a bundle with both members.
    fn activation(
        source_manifest: &str,
        dependent_manifest: &str,
        dependent_incarnation: u128,
    ) -> MigrationState {
        MigrationState {
            migration_id: MIGRATION,
            tenant_id: 1,
            topic_id: 3,
            partition_id: 0,
            stage: MigrationStage::ActivateNew,
            source_node: 1,
            target_node: 2,
            source_incarnation: 42,
            original_epoch: 7,
            next_epoch: 8,
            transfer_watermark: Some(11),
            manifest_id: Some(source_manifest.to_string()),
            fence_proof: Some(FenceProof {
                migration_id: MIGRATION,
                old_owner: 1,
                old_epoch: 7,
                fence_offset: 11,
                fence_digest: "aa".into(),
                manifest_generation: 4,
            }),
            target_ready: true,
            dependent_partitions: vec![rovqet_metadata_raft::types::DependentPartition {
                tenant_id: 1,
                topic_id: 7,
                partition_id: 0,
                durable_class: 1,
                incarnation: dependent_incarnation,
            }],
            dependent_evidence: vec![rovqet_metadata_raft::types::DependentEvidence {
                tenant_id: 1,
                topic_id: 7,
                partition_id: 0,
                incarnation: dependent_incarnation,
                original_epoch: 7,
                next_epoch: 8,
                transfer_watermark: Some(2),
                manifest_id: Some(dependent_manifest.to_string()),
                fence_proof: Some(FenceProof {
                    migration_id: MIGRATION,
                    old_owner: 1,
                    old_epoch: 7,
                    fence_offset: 2,
                    fence_digest: "bb".into(),
                    manifest_generation: 3,
                }),
                ready: true,
            }],
        }
    }

    /// Writes a staging area with the given files and returns its inventory identity.
    fn staging(root: &Path, member: Q1TransferMember, files: &[(&str, &[u8])]) -> String {
        let declared: Vec<Q1TransferFile> = files
            .iter()
            .map(|(name, bytes)| Q1TransferFile {
                name: name.to_string(),
                size: bytes.len() as u64,
                digest: sha256(bytes),
            })
            .collect();
        let digest = crate::transfer::inventory_digest(&declared);
        let directory = crate::promotion::staging_area(root, MIGRATION, member, &digest);
        std::fs::create_dir_all(&directory).expect("staging directory");
        let document = serde_json::json!({
            "migration_id": MIGRATION.to_string(),
            "member": member.as_u8(),
            "files": declared.iter().map(|file| serde_json::json!({
                "name": file.name,
                "size": file.size,
                "digest": bytes_to_hex(&file.digest),
            })).collect::<Vec<_>>(),
        });
        std::fs::write(
            directory.join("staging.json"),
            serde_json::to_vec(&document).expect("manifest"),
        )
        .expect("manifest");
        for (index, (_, bytes)) in files.iter().enumerate() {
            std::fs::write(directory.join(format!("part-{index:06}.bin")), bytes).expect("part");
        }
        bytes_to_hex(&digest)
    }

    fn directories() -> (tempfile::TempDir, std::path::PathBuf, std::path::PathBuf) {
        let root = tempfile::tempdir().expect("temp dir");
        let source = root.path().join("source");
        let dlq = root.path().join("dlq");
        std::fs::create_dir_all(&source).expect("source directory");
        std::fs::create_dir_all(&dlq).expect("dlq directory");
        (root, source, dlq)
    }

    #[test]
    fn a_bundle_without_the_dependent_inventory_is_refused() {
        let (root, source, dlq) = directories();
        let staging_root = root.path().join("staging");
        std::fs::create_dir_all(&staging_root).expect("staging root");
        let source_manifest = staging(
            &staging_root,
            Q1TransferMember::Source,
            &[("MANIFEST", b"source manifest")],
        );
        // The dependent staging area was never received.
        let record = activation(&source_manifest, &"cd".repeat(32), 77);
        let configured = options(source, Some(dlq));
        let error = promote_bundle(&record, &configured, &staging_root, 9, 8)
            .expect_err("the missing dependent inventory must be refused");
        assert!(
            error.contains("dependent promotion"),
            "the refusal must name the missing promotion: {error}"
        );
        assert!(
            !configured
                .dlq
                .as_ref()
                .expect("dlq")
                .directory
                .join("MANIFEST")
                .is_file(),
            "a refused bundle must not publish the dependent history"
        );
    }

    #[test]
    fn a_bundle_with_a_foreign_dependent_incarnation_is_refused() {
        let (root, source, dlq) = directories();
        let staging_root = root.path().join("staging");
        std::fs::create_dir_all(&staging_root).expect("staging root");
        let source_manifest = staging(
            &staging_root,
            Q1TransferMember::Source,
            &[("MANIFEST", b"source manifest")],
        );
        let dependent_manifest = staging(
            &staging_root,
            Q1TransferMember::Dependent,
            &[("MANIFEST", b"dependent manifest")],
        );
        // The committed dependency carries another incarnation than the configured one.
        let record = activation(&source_manifest, &dependent_manifest, 99);
        let configured = options(source, Some(dlq));
        let error = promote_bundle(&record, &configured, &staging_root, 9, 8)
            .expect_err("a foreign dependent incarnation must be refused");
        assert!(
            error.contains("incarnation"),
            "the refusal must name the incarnation: {error}"
        );
    }

    #[test]
    fn an_uncommitted_activation_is_refused() {
        let (root, source, dlq) = directories();
        let staging_root = root.path().join("staging");
        std::fs::create_dir_all(&staging_root).expect("staging root");
        let source_manifest = staging(
            &staging_root,
            Q1TransferMember::Source,
            &[("MANIFEST", b"source manifest")],
        );
        let dependent_manifest = staging(
            &staging_root,
            Q1TransferMember::Dependent,
            &[("MANIFEST", b"dependent manifest")],
        );
        let mut record = activation(&source_manifest, &dependent_manifest, 77);
        record.target_ready = false;
        let configured = options(source, Some(dlq));
        let error = promote_bundle(&record, &configured, &staging_root, 9, 8)
            .expect_err("an activation without readiness must be refused");
        assert!(
            error.contains("readiness"),
            "the refusal must name the missing readiness: {error}"
        );
    }

    #[test]
    fn a_conflicting_destination_storage_is_refused() {
        let (root, source, dlq) = directories();
        let staging_root = root.path().join("staging");
        std::fs::create_dir_all(&staging_root).expect("staging root");
        let source_manifest = staging(
            &staging_root,
            Q1TransferMember::Source,
            &[("MANIFEST", b"source manifest")],
        );
        let dependent_manifest = staging(
            &staging_root,
            Q1TransferMember::Dependent,
            &[("MANIFEST", b"dependent manifest")],
        );
        // The destination holds unrelated storage.
        std::fs::write(dlq.join("unrelated.blob"), b"foreign").expect("foreign file");
        let record = activation(&source_manifest, &dependent_manifest, 77);
        let configured = options(source, Some(dlq));
        let error = promote_bundle(&record, &configured, &staging_root, 9, 8)
            .expect_err("conflicting destination storage must be refused");
        assert!(
            error.contains("dependent promotion"),
            "the refusal must name the dependent promotion: {error}"
        );
    }

    #[test]
    fn a_one_sided_promotion_leaves_the_source_unavailable() {
        let (root, source, dlq) = directories();
        let staging_root = root.path().join("staging");
        std::fs::create_dir_all(&staging_root).expect("staging root");
        let source_manifest = staging(
            &staging_root,
            Q1TransferMember::Source,
            &[("MANIFEST", b"source manifest")],
        );
        let dependent_manifest = staging(
            &staging_root,
            Q1TransferMember::Dependent,
            &[("MANIFEST", b"dependent manifest")],
        );
        let record = activation(&source_manifest, &dependent_manifest, 77);
        let configured = options(source.clone(), Some(dlq.clone()));
        // The dependent inventory is missing: the promotion stops after the dependent
        // history was published, and the source stays without its history.
        let mut incomplete = record.clone();
        incomplete.dependent_evidence[0].manifest_id = Some("cd".repeat(32));
        let error = promote_bundle(&incomplete, &configured, &staging_root, 9, 8)
            .expect_err("the missing dependent inventory must stop the promotion");
        assert!(error.contains("dependent promotion"), "{error}");
        // The source directory never received a history.
        assert!(
            !source.join("MANIFEST").is_file(),
            "a one-sided promotion must not publish the source history"
        );
        // The completed promotion publishes both histories.
        promote_bundle(&record, &configured, &staging_root, 9, 8)
            .expect("the repeated promotion completes the bundle");
        assert!(source.join("MANIFEST").is_file());
        assert!(dlq.join("MANIFEST").is_file());
        let readiness = crate::promotion::BundleReadiness::load(&staging_root, MIGRATION)
            .expect("readiness")
            .expect("a readiness record");
        assert!(readiness.dependent_promoted);
        assert!(readiness.source_promoted);
    }

    #[test]
    fn a_bundle_readiness_of_another_activation_is_refused() {
        let (root, source, dlq) = directories();
        let staging_root = root.path().join("staging");
        std::fs::create_dir_all(&staging_root).expect("staging root");
        let source_manifest = staging(
            &staging_root,
            Q1TransferMember::Source,
            &[("MANIFEST", b"source manifest")],
        );
        let dependent_manifest = staging(
            &staging_root,
            Q1TransferMember::Dependent,
            &[("MANIFEST", b"dependent manifest")],
        );
        let record = activation(&source_manifest, &dependent_manifest, 77);
        let configured = options(source, Some(dlq));
        // A readiness record of another inventory identity is never accepted.
        let foreign = crate::promotion::BundleReadiness {
            migration_id: MIGRATION,
            activation_revision: 9,
            target_epoch: 8,
            dependent_inventory_digest: "00".repeat(32),
            dependent_incarnation: 77,
            dependent_partition: [1, 7, 0],
            dependent_promoted: true,
            source_inventory_digest: source_manifest.clone(),
            source_incarnation: 42,
            source_partition: [1, 3, 0],
            source_promoted: false,
        };
        foreign.store(&staging_root).expect("readiness");
        let error = promote_bundle(&record, &configured, &staging_root, 9, 8)
            .expect_err("a foreign readiness record must be refused");
        assert!(
            error.contains("readiness"),
            "the refusal must name the readiness record: {error}"
        );
    }

    #[test]
    fn a_dependent_staging_area_of_another_inventory_is_refused() {
        let (root, source, dlq) = directories();
        let staging_root = root.path().join("staging");
        std::fs::create_dir_all(&staging_root).expect("staging root");
        let source_manifest = staging(
            &staging_root,
            Q1TransferMember::Source,
            &[("MANIFEST", b"source manifest")],
        );
        let dependent_manifest = staging(
            &staging_root,
            Q1TransferMember::Dependent,
            &[("MANIFEST", b"dependent manifest")],
        );
        let record = activation(&source_manifest, &dependent_manifest, 77);
        // The staging area that is named after the committed dependent inventory holds the
        // bytes of a different inventory: the promotion must refuse it instead of publishing
        // the unstaged bytes into the dependent directory.
        let dependent_staging = crate::promotion::staging_area(
            &staging_root,
            MIGRATION,
            Q1TransferMember::Dependent,
            &hex_to_bytes(&dependent_manifest),
        );
        std::fs::write(
            dependent_staging.join("part-000000.bin"),
            b"another inventory",
        )
        .expect("foreign staged bytes");
        let configured = options(source.clone(), Some(dlq.clone()));
        let error = promote_bundle(&record, &configured, &staging_root, 9, 8)
            .expect_err("a dependent staging area of another inventory must be refused");
        assert!(
            error.contains("dependent promotion"),
            "the refusal must name the dependent promotion: {error}"
        );
        assert!(
            error.contains("DigestMismatch"),
            "the refused bytes must be named as an inventory mismatch: {error}"
        );
        assert!(
            !dlq.join("MANIFEST").is_file(),
            "a refused dependent promotion must not publish the dependent history"
        );
        assert!(
            !source.join("MANIFEST").is_file(),
            "the source must never be published after a refused dependent promotion"
        );
    }

    #[test]
    fn a_bundle_without_the_source_staging_area_never_publishes_the_source() {
        let (root, source, dlq) = directories();
        let staging_root = root.path().join("staging");
        std::fs::create_dir_all(&staging_root).expect("staging root");
        let source_manifest = staging(
            &staging_root,
            Q1TransferMember::Source,
            &[("MANIFEST", b"source manifest")],
        );
        let dependent_manifest = staging(
            &staging_root,
            Q1TransferMember::Dependent,
            &[("MANIFEST", b"dependent manifest")],
        );
        let record = activation(&source_manifest, &dependent_manifest, 77);
        // The source inventory was verified, but its staging area is gone.
        let source_staging = crate::promotion::staging_area(
            &staging_root,
            MIGRATION,
            Q1TransferMember::Source,
            &hex_to_bytes(&source_manifest),
        );
        std::fs::remove_dir_all(&source_staging).expect("remove the source staging area");
        let configured = options(source.clone(), Some(dlq.clone()));
        let error = promote_bundle(&record, &configured, &staging_root, 9, 8)
            .expect_err("a missing source staging area must stop the promotion");
        assert!(
            error.contains("source promotion"),
            "the refusal must name the source promotion: {error}"
        );
        assert!(
            !source.join("MANIFEST").is_file(),
            "the successor must not become active without its source history"
        );
        // The dependent history was published first and stays: the interrupted bundle is
        // resumable and never hidden behind a half-published source.
        assert!(dlq.join("MANIFEST").is_file());
    }

    #[test]
    fn a_promotion_record_of_another_activation_is_refused() {
        let (root, source, dlq) = directories();
        let staging_root = root.path().join("staging");
        std::fs::create_dir_all(&staging_root).expect("staging root");
        let source_manifest = staging(
            &staging_root,
            Q1TransferMember::Source,
            &[("MANIFEST", b"source manifest")],
        );
        let dependent_manifest = staging(
            &staging_root,
            Q1TransferMember::Dependent,
            &[("MANIFEST", b"dependent manifest")],
        );
        let record = activation(&source_manifest, &dependent_manifest, 77);
        let configured = options(source.clone(), Some(dlq));
        promote_bundle(&record, &configured, &staging_root, 9, 8)
            .expect("the paired promotion completes");
        let committed = crate::promotion::promotion_record(&source)
            .expect("the promotion record is readable")
            .expect("the source promotion is recorded");
        assert_eq!(committed.migration_id, MIGRATION);
        assert_eq!(committed.state, crate::promotion::PromotionState::Promoted);
        // A promotion record of another activation in the same directory is never accepted.
        let error = verify_promotion(
            &source,
            MIGRATION ^ 1,
            &record.source_incarnation,
            record.tenant_id,
            record.topic_id,
            record.partition_id,
        )
        .expect_err("a promotion record of another activation must be refused");
        assert!(
            error.contains("another activation"),
            "the refusal must name the foreign activation: {error}"
        );
        // The committed promotion record of the activated bundle stays intact.
        let intact = crate::promotion::promotion_record(&source)
            .expect("the promotion record is still readable")
            .expect("the source promotion is still recorded");
        assert_eq!(
            intact, committed,
            "refusing a foreign activation must not rewrite the committed promotion"
        );
    }
}

#[cfg(test)]
mod adversarial_recovery_tests {
    //! The recovery cases of the C-02 safety assignment.
    //!
    //! Every test builds a real Q1 history with the durable partition, promotes it with the
    //! real promotion protocol and then requires that the real recovery refuses bytes that
    //! are not exactly the history the activation committed. A refused history never becomes
    //! an active route.
    use super::*;
    use rovqet_core::{DurableOffset, PartitionRef, PublishOutcome, QoS};
    use rovqet_durable::{
        DlqError, DlqPublisher, DlqRecord, GroupConfig, Partition, PartitionConfig, PublishRequest,
        TerminalPolicy,
    };
    use std::path::Path;
    use std::sync::Arc;
    use std::time::Duration;

    const MIGRATION: u128 = 0x7171_7171_7171_7171;
    const INCARNATION: u128 = 42;
    const EPOCH: u64 = 8;
    const NODE: u64 = 2;
    const DLQ_INCARNATION: u128 = 77;

    fn source_partition() -> PartitionRef {
        PartitionRef {
            tenant_id: 1,
            topic_id: 3,
            partition_id: 0,
        }
    }

    fn dependent_partition() -> PartitionRef {
        PartitionRef {
            tenant_id: 1,
            topic_id: 7,
            partition_id: 0,
        }
    }

    fn options(
        directory: std::path::PathBuf,
        dlq: Option<std::path::PathBuf>,
    ) -> crate::q1::Options {
        crate::q1::Options {
            directory,
            topic: "durable".into(),
            node_id: NODE,
            partition: source_partition(),
            incarnation: INCARNATION,
            owner_epoch: EPOCH,
            dedup_window: Duration::from_secs(86400),
            max_producers: 64,
            max_outcomes: 64,
            initialize_new_log: false,
            terminal_policy: TerminalPolicy::Dlq,
            max_attempts: 1,
            retry_initial: Duration::from_millis(10),
            retry_max: Duration::from_millis(100),
            dlq: dlq.map(|directory| crate::q1::DlqOptions {
                directory,
                topic: "durable-dlq".into(),
                partition: dependent_partition(),
                incarnation: DLQ_INCARNATION,
                owner_epoch: EPOCH,
                max_outcomes: 64,
                max_producers: 64,
            }),
        }
    }

    fn partition_config(partition: PartitionRef, group: GroupConfig) -> PartitionConfig {
        PartitionConfig {
            partition,
            ownership_epoch: EPOCH,
            node_id: NODE,
            queue_messages: 64,
            queue_bytes: 8 * 1024 * 1024,
            max_outcomes: 64,
            max_producers: 64,
            dedup_window: Duration::from_secs(86400),
            group,
        }
    }

    fn publish_request(partition: PartitionRef, message_id: u128) -> PublishRequest {
        PublishRequest {
            partition,
            qos: QoS::Q1,
            ownership_epoch: EPOCH,
            principal: 1,
            producer_id: 7,
            producer_epoch: 1,
            sequence: 0,
            message_id,
            payload: b"committed history".to_vec(),
        }
    }

    /// Builds a real, committed Q1 history of the given identity and incarnation.
    fn real_history(directory: &Path, partition: PartitionRef, incarnation: u128) {
        std::fs::create_dir_all(directory).expect("the history directory exists");
        let history = Partition::open(
            directory,
            rovqet_wal::Config {
                partition,
                incarnation,
                ..Default::default()
            },
            partition_config(partition, GroupConfig::default()),
        )
        .expect("the source history opens");
        match history
            .try_publish(publish_request(partition, 500))
            .expect("the publication is admitted")
            .wait()
            .expect("the publication commits")
        {
            PublishOutcome::CommittedLocal(_) => {}
            other => panic!("the source record must commit locally: {other:?}"),
        }
        history.shutdown().expect("the history shuts down");
    }

    /// Promotes the files of a real history with the real promotion protocol.
    fn promote_history(
        history: &Path,
        staging: &Path,
        target: &Path,
        partition: PartitionRef,
        incarnation: u128,
    ) {
        let files = partition_inventory(history).expect("the real inventory is readable");
        assert!(
            !files.is_empty(),
            "a real history carries at least a manifest and a segment"
        );
        std::fs::create_dir_all(staging).expect("the staging area exists");
        let declared: Vec<serde_json::Value> = files
            .iter()
            .map(|file| {
                serde_json::json!({
                    "name": file.name,
                    "size": file.size,
                    "digest": bytes_to_hex(&file.digest),
                })
            })
            .collect();
        let document = serde_json::json!({
            "migration_id": MIGRATION.to_string(),
            "files": declared,
        });
        std::fs::write(
            staging.join("staging.json"),
            serde_json::to_vec(&document).expect("the staging manifest"),
        )
        .expect("the staging manifest");
        for (index, file) in files.iter().enumerate() {
            std::fs::copy(
                history.join(&file.name),
                staging.join(format!("part-{index:06}.bin")),
            )
            .expect("the staged file is copied");
        }
        std::fs::create_dir_all(target).expect("the target directory exists");
        // The file list is taken from the staging manifest, exactly like the real promotion:
        // the plan only carries the identity of the activation.
        let plan = crate::promotion::PromotionRecord {
            migration_id: MIGRATION,
            inventory_digest: "00".repeat(32),
            incarnation,
            tenant_id: partition.tenant_id,
            topic_id: partition.topic_id,
            partition_id: partition.partition_id,
            target_epoch: EPOCH,
            activation_revision: 9,
            files: Vec::new(),
            state: crate::promotion::PromotionState::Promoted,
        };
        crate::promotion::promote_with_manifest(staging, target, &plan)
            .expect("the verified history is promoted");
        assert!(
            target.join("MANIFEST").is_file(),
            "the promotion publishes the manifest of the inventory"
        );
    }

    fn promoted_history(
        root: &Path,
    ) -> (std::path::PathBuf, std::path::PathBuf, std::path::PathBuf) {
        let history = root.join("history");
        let staging = root.join("staging");
        let installed = root.join("installed");
        (history, staging, installed)
    }

    /// Builds a real Q1 history that carries a genuine durable fence and promotes it with
    /// the real promotion protocol.
    ///
    /// The returned proof is the committed form of the durable proof of
    /// `Partition::fence_old_owner`, exactly like the source side commits it.
    fn promoted_fence(root: &Path) -> (std::path::PathBuf, FenceProof) {
        let (history, staging, installed) = promoted_history(root);
        std::fs::create_dir_all(&history).expect("the history directory exists");
        let partition = source_partition();
        let handle = Partition::open(
            &history,
            rovqet_wal::Config {
                partition,
                incarnation: INCARNATION,
                ..Default::default()
            },
            partition_config(partition, GroupConfig::default()),
        )
        .expect("the fenced history opens");
        handle
            .set_authority(Some(
                rovqet_durable::Authority::new(NODE, EPOCH, Duration::from_secs(3600))
                    .expect("the authority is valid"),
            ))
            .expect("the authority is installed");
        match handle
            .try_publish(publish_request(partition, 501))
            .expect("the publication is admitted")
            .wait()
            .expect("the publication commits")
        {
            PublishOutcome::CommittedLocal(_) => {}
            other => panic!("the source record must commit locally: {other:?}"),
        }
        let proof = handle
            .fence_old_owner(MIGRATION, EPOCH, EPOCH + 1)
            .expect("the durable fence commits");
        handle.shutdown().expect("the history shuts down");
        promote_history(&history, &staging, &installed, partition, INCARNATION);
        (installed, committed_fence(&proof))
    }

    /// Verifies a proof against the promoted history and returns the refusal message.
    /// An accepted proof is a test failure.
    fn refused(installed: &Path, proof: &FenceProof) -> String {
        verify_fence_against_history(installed, source_partition(), INCARNATION, proof)
            .expect_err("a forged fence proof must never verify")
    }

    #[test]
    fn a_genuine_fence_proof_verifies_against_the_promoted_history() {
        let root = tempfile::tempdir().expect("temp dir");
        let (installed, proof) = promoted_fence(root.path());
        verify_fence_against_history(&installed, source_partition(), INCARNATION, &proof)
            .expect("the genuine proof is the fence record of the promoted history");
    }

    #[test]
    fn a_fabricated_fence_digest_is_refused_against_the_promoted_history() {
        let root = tempfile::tempdir().expect("temp dir");
        let (installed, mut proof) = promoted_fence(root.path());
        proof.fence_digest = rovqet_durable::fence_digest(b"a fabricated fence record");
        let error = refused(&installed, &proof);
        assert!(error.contains("digest"), "{error}");
    }

    #[test]
    fn a_proof_pointing_at_a_non_fence_record_is_refused() {
        let root = tempfile::tempdir().expect("temp dir");
        let (installed, mut proof) = promoted_fence(root.path());
        assert_ne!(
            proof.fence_offset, 0,
            "the fence follows the published record and its outcome"
        );
        // Offset 0 is the published message of this history, never a fence record.
        proof.fence_offset = 0;
        let error = refused(&installed, &proof);
        assert!(error.contains("not a fence record"), "{error}");
    }

    #[test]
    fn a_proof_of_another_migration_owner_or_epoch_is_refused() {
        let root = tempfile::tempdir().expect("temp dir");
        let (installed, genuine) = promoted_fence(root.path());
        let mut other_migration = genuine.clone();
        other_migration.migration_id += 1;
        let mut other_owner = genuine.clone();
        other_owner.old_owner += 1;
        let mut other_epoch = genuine.clone();
        other_epoch.old_epoch += 1;
        for proof in [&other_migration, &other_owner, &other_epoch] {
            let error = refused(&installed, proof);
            assert!(error.contains("revokes migration"), "{error}");
        }
    }

    fn block_on<F: std::future::Future>(future: F) -> F::Output {
        tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("a current-thread runtime")
            .block_on(future)
    }

    /// Opens a route and returns the refusal message. An opened route is a test failure.
    fn open_failure(options: crate::q1::Options) -> String {
        match crate::q1::Route::open(options) {
            Ok(_) => panic!("a refused history must never become an active route"),
            Err(error) => error.to_string(),
        }
    }

    #[test]
    fn a_promoted_history_with_a_corrupted_segment_never_opens() {
        let root = tempfile::tempdir().expect("temp dir");
        let (history, staging, installed) = promoted_history(root.path());
        real_history(&history, source_partition(), INCARNATION);
        promote_history(
            &history,
            &staging,
            &installed,
            source_partition(),
            INCARNATION,
        );
        // The segment is corrupted after the promotion completed: the recovery validates the
        // record checksums and must refuse the history.
        let segment = std::fs::read_dir(&installed)
            .expect("the promoted directory is readable")
            .filter_map(|entry| entry.ok())
            .map(|entry| entry.path())
            .find(|path| {
                path.file_name()
                    .map(|name| name.to_string_lossy().starts_with("segment-"))
                    .unwrap_or(false)
            })
            .expect("a promoted segment exists");
        let mut bytes = std::fs::read(&segment).expect("the promoted segment is readable");
        let last = bytes.len() - 1;
        bytes[last] ^= 0xff;
        std::fs::write(&segment, &bytes).expect("the corrupted segment is written");
        let error = open_failure(options(installed, None));
        assert!(
            error.contains("Q1 recovery failed"),
            "the refusal must come from the recovery: {error}"
        );
    }

    #[test]
    fn a_promoted_history_without_its_manifest_never_opens() {
        let root = tempfile::tempdir().expect("temp dir");
        let (history, staging, installed) = promoted_history(root.path());
        real_history(&history, source_partition(), INCARNATION);
        promote_history(
            &history,
            &staging,
            &installed,
            source_partition(),
            INCARNATION,
        );
        std::fs::remove_file(installed.join("MANIFEST")).expect("the promoted manifest is removed");
        let error = open_failure(options(installed, None));
        assert!(
            error.contains("REPLAY_NOT_AVAILABLE"),
            "a manifest-less history is unavailable, never recreated: {error}"
        );
    }

    #[test]
    fn a_promoted_history_of_another_partition_never_opens() {
        let root = tempfile::tempdir().expect("temp dir");
        let (history, staging, installed) = promoted_history(root.path());
        let foreign = PartitionRef {
            tenant_id: 1,
            topic_id: 3,
            partition_id: 1,
        };
        real_history(&history, foreign, INCARNATION);
        promote_history(&history, &staging, &installed, foreign, INCARNATION);
        let error = open_failure(options(installed, None));
        assert!(
            error.contains("Q1 recovery failed"),
            "the durable identity must be validated by the recovery: {error}"
        );
    }

    #[test]
    fn a_promoted_history_of_another_incarnation_never_opens() {
        let root = tempfile::tempdir().expect("temp dir");
        let (history, staging, installed) = promoted_history(root.path());
        real_history(&history, source_partition(), 99);
        promote_history(&history, &staging, &installed, source_partition(), 99);
        let error = open_failure(options(installed, None));
        assert!(
            error.contains("Q1 recovery failed"),
            "the incarnation must be validated by the recovery: {error}"
        );
    }

    /// A real `DlqPublisher` over a real destination partition.
    struct TestDlq {
        partition: Arc<Partition>,
        partition_ref: PartitionRef,
        incarnation: u128,
    }

    impl DlqPublisher for TestDlq {
        fn publish(&self, record: &DlqRecord<'_>) -> std::result::Result<DurableOffset, DlqError> {
            let payload = rovqet_durable::dlq_payload(record);
            self.partition
                .try_publish(PublishRequest {
                    partition: self.partition_ref,
                    qos: QoS::Q1,
                    ownership_epoch: EPOCH,
                    principal: 0,
                    producer_id: record.disposition_id,
                    producer_epoch: 0,
                    sequence: 0,
                    message_id: record.disposition_id,
                    payload,
                })
                .and_then(|pending| pending.wait())
                .map_err(|_| DlqError::Unavailable)
                .and_then(|outcome| match outcome {
                    PublishOutcome::CommittedLocal(offset) => Ok(offset),
                    _ => Err(DlqError::Unavailable),
                })
        }

        fn verify(
            &self,
            offset: DurableOffset,
            disposition_id: u128,
            original_message_id: u128,
        ) -> std::result::Result<bool, DlqError> {
            let batch = self
                .partition
                .try_replay(rovqet_durable::ReplayRequest {
                    partition: self.partition_ref,
                    from: offset,
                    max_messages: 1,
                    max_bytes: 64 * 1024,
                    wait: Duration::ZERO,
                })
                .and_then(|pending| pending.wait())
                .map_err(|_| DlqError::Unavailable)?;
            let Some(message) = batch.messages.first() else {
                return Ok(false);
            };
            if message.offset != offset
                || message.message_id != disposition_id
                || message.offset.log_incarnation != self.incarnation
            {
                return Ok(false);
            }
            let metadata = rovqet_durable::parse_dlq_payload(&message.payload)
                .map_err(|_| DlqError::Corruption("DLQ payload"))?;
            Ok(metadata.disposition_id == disposition_id
                && metadata.original_message_id == original_message_id)
        }
    }

    #[test]
    fn a_committed_dlq_disposition_without_its_destination_never_opens() {
        let root = tempfile::tempdir().expect("temp dir");
        let history = root.path().join("history");
        let destination = root.path().join("dlq");
        let empty_destination = root.path().join("empty-dlq");
        std::fs::create_dir_all(&history).expect("the history directory exists");
        std::fs::create_dir_all(&destination).expect("the destination directory exists");
        let dlq = Arc::new(
            Partition::open(
                &destination,
                rovqet_wal::Config {
                    partition: dependent_partition(),
                    incarnation: DLQ_INCARNATION,
                    ..Default::default()
                },
                partition_config(dependent_partition(), GroupConfig::default()),
            )
            .expect("the destination opens"),
        );
        let publisher = Arc::new(TestDlq {
            partition: dlq.clone(),
            partition_ref: dependent_partition(),
            incarnation: DLQ_INCARNATION,
        });
        let source = Partition::open_with_dlq(
            &history,
            rovqet_wal::Config {
                partition: source_partition(),
                incarnation: INCARNATION,
                ..Default::default()
            },
            partition_config(
                source_partition(),
                GroupConfig {
                    max_attempts: 1,
                    terminal_policy: TerminalPolicy::Dlq,
                    ..Default::default()
                },
            ),
            publisher.clone(),
        )
        .expect("the source opens with its destination");
        let offset = match source
            .try_publish(publish_request(source_partition(), 500))
            .expect("the publication is admitted")
            .wait()
            .expect("the publication commits")
        {
            PublishOutcome::CommittedLocal(offset) => offset,
            other => panic!("the source record must commit locally: {other:?}"),
        };
        // The single record is delivered once and NACKed: `max_attempts = 1` with the DLQ
        // terminal policy commits a terminal disposition whose proof lives in the DLQ.
        let batch = source
            .try_fetch_group(rovqet_durable::GroupFetchRequest {
                partition: source_partition(),
                group_id: 1,
                generation: 0,
                from: DurableOffset {
                    log_incarnation: INCARNATION,
                    sequence: 0,
                },
                max_messages: 1,
                max_bytes: 64 * 1024,
                wait: Duration::ZERO,
            })
            .expect("the group fetch is admitted")
            .wait()
            .expect("the group fetch completes");
        let delivery = batch.deliveries.first().expect("one delivery");
        assert_eq!(delivery.offset, offset);
        let outcome = source
            .try_nack(rovqet_durable::NackRequest {
                partition: source_partition(),
                group_id: 1,
                generation: delivery.generation,
                lease_id: delivery.lease_id,
                ownership_epoch: EPOCH,
                offset: delivery.offset,
                attempt: delivery.attempt,
                principal: 1,
                reason: 1,
            })
            .expect("the nack is admitted")
            .wait()
            .expect("the nack commits");
        assert!(
            matches!(
                outcome,
                rovqet_durable::NackOutcome::TerminalCommitted { .. }
            ),
            "the exhausted record must commit a terminal DLQ disposition: {outcome:?}"
        );
        source.shutdown().expect("the source shuts down");
        // The successor is opened with an EMPTY destination: the committed proof of the
        // terminal disposition cannot be verified, so the recovery fails closed.
        std::fs::create_dir_all(&empty_destination).expect("the empty destination exists");
        let error = open_failure(options(history, Some(empty_destination)));
        assert!(
            error.contains("Q1 recovery failed"),
            "the terminal proof must be verified by the recovery: {error}"
        );
    }

    #[test]
    fn a_route_that_does_not_hold_the_committed_epoch_refuses_a_durable_publication() {
        let directory = tempfile::tempdir().expect("temp dir");
        let mut configured = options(directory.path().to_path_buf(), None);
        configured.initialize_new_log = true;
        let route = crate::q1::Route::open(configured).expect("the new successor history opens");
        // The successor resumes at its committed epoch 8. An older authority (the revoked
        // epoch 7) is installed locally, and the requester claims the committed epoch 9:
        // this route does not hold that epoch and must never commit with it. The durable
        // actor validates the mutation against the configured ownership epoch.
        route.install_authority(Some(
            rovqet_durable::Authority::new(NODE, EPOCH - 1, Duration::from_secs(30))
                .expect("a valid authority"),
        ));
        let request = rovqet_control::RoutePublishRequest {
            tenant_id: 1,
            topic_id: 3,
            partition_id: 0,
            qos: 1,
            ownership_epoch: EPOCH + 1,
            producer_id: 7,
            producer_epoch: 1,
            producer_sequence: 0,
            message_id: 500,
            ttl_ms: 1_000,
            payload: b"stale successor".to_vec(),
        };
        let error = block_on(route.publish_durable(&request))
            .expect_err("a route that does not hold the committed epoch must refuse");
        assert_eq!(
            format!("{error:?}"),
            "StaleEpoch",
            "a successor must never commit with a stale ownership epoch"
        );
        assert_eq!(
            route.committed_count(),
            0,
            "a refused publication must not commit anything"
        );
        route.shutdown().expect("the route shuts down");
    }
}
