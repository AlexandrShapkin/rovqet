//! Deterministic metadata state machine with durable, checksummed snapshots.
//!
//! Committed commands are applied strictly in Raft log order. The applied state, the last
//! applied log id and the stored membership are persisted atomically, so a restart never
//! loses a committed ownership change. Snapshots are the same container and are validated
//! before they replace the live state.
use crate::storage::crc32c;
use crate::types::{
    DependentPartition, FenceProof, MetaNode, MetaNodeId, MetadataCommand, MetadataCommandResponse,
    MetadataRaftTypeConfig, MigrationStage,
};
use openraft::storage::{RaftSnapshotBuilder, RaftStateMachine};
use openraft::{
    Entry, EntryPayload, ErrorSubject, ErrorVerb, LogId, Snapshot, SnapshotMeta, StorageError,
    StorageIOError, StoredMembership,
};
use std::collections::BTreeMap;
use std::io::Cursor;
use std::path::{Path, PathBuf};
use tokio::io::AsyncReadExt;

const STATE_MAGIC: &[u8; 4] = b"MRTS";
const STATE_VERSION: u32 = 1;

/// One committed placement.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct PlacementState {
    pub tenant_id: u64,
    pub topic_id: u64,
    pub partition_id: u32,
    pub owner: MetaNodeId,
    pub ownership_epoch: u64,
    pub lease_valid_for_ms: u64,
}

/// One committed topic policy.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct TopicState {
    pub topic_id: u64,
    pub durable_class: u8,
    pub policy_revision: u64,
}

/// One committed migration record.
///
/// The record is the authoritative description of an ownership handover: it names the source
/// and the target, the incarnation that must survive the transfer, the epochs on both sides
/// and the evidence that each stage produced.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct MigrationState {
    pub migration_id: u128,
    pub tenant_id: u64,
    pub topic_id: u64,
    pub partition_id: u32,
    pub stage: MigrationStage,
    /// The node that owns the partition until the activation is committed.
    pub source_node: MetaNodeId,
    /// The node that will own the partition after the activation.
    pub target_node: MetaNodeId,
    /// The log incarnation that must be transferred unchanged.
    pub source_incarnation: u128,
    /// The ownership epoch the source held when the migration started.
    pub original_epoch: u64,
    /// The ownership epoch the target owns after the activation.
    pub next_epoch: u64,
    /// Last committed source offset that was transferred and verified.
    pub transfer_watermark: Option<u64>,
    /// Identity of the verified manifest of the transferred inventory.
    pub manifest_id: Option<String>,
    /// Durable proof that the old owner revoked its write authority.
    pub fence_proof: Option<FenceProof>,
    /// The target published that it validated the complete transferred inventory.
    pub target_ready: bool,
    /// Partitions whose durable state the migration depends on, such as a DLQ partition.
    pub dependent_partitions: Vec<DependentPartition>,
}

/// Authoritative metadata state of the group.
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct MetadataState {
    pub cluster_id: u128,
    pub revision: u64,
    pub nodes: BTreeMap<MetaNodeId, String>,
    pub topics: BTreeMap<u64, TopicState>,
    pub placements: Vec<PlacementState>,
    pub migrations: BTreeMap<u128, MigrationState>,
}

/// A deterministic rejection of a committed command.
fn rejected(reason: &str) -> MetadataCommandResponse {
    MetadataCommandResponse::Rejected {
        reason: reason.to_string(),
    }
}

impl MetadataState {
    /// Applies one committed command deterministically.
    pub fn apply(&mut self, command: &MetadataCommand) -> MetadataCommandResponse {
        match command {
            MetadataCommand::SetNode { node, address } => {
                if *node == 0 || address.is_empty() {
                    return MetadataCommandResponse::Rejected {
                        reason: "invalid node".into(),
                    };
                }
                self.nodes.insert(*node, address.clone());
                self.revision += 1;
                MetadataCommandResponse::Applied {
                    revision: self.revision,
                }
            }
            MetadataCommand::SetTopicPolicy {
                topic_id,
                durable_class,
                policy_revision,
            } => {
                if *topic_id == 0 || *durable_class > 2 || *policy_revision == 0 {
                    return MetadataCommandResponse::Rejected {
                        reason: "invalid topic policy".into(),
                    };
                }
                self.topics.insert(
                    *topic_id,
                    TopicState {
                        topic_id: *topic_id,
                        durable_class: *durable_class,
                        policy_revision: *policy_revision,
                    },
                );
                self.revision += 1;
                MetadataCommandResponse::Applied {
                    revision: self.revision,
                }
            }
            MetadataCommand::SetPlacement {
                tenant_id,
                topic_id,
                partition_id,
                owner,
                ownership_epoch,
                lease_valid_for_ms,
            } => {
                if *owner == 0 || *ownership_epoch == 0 || *lease_valid_for_ms == 0 {
                    return MetadataCommandResponse::Rejected {
                        reason: "invalid placement".into(),
                    };
                }
                if !self.nodes.contains_key(owner) {
                    return MetadataCommandResponse::Rejected {
                        reason: "unknown owner".into(),
                    };
                }
                // Ownership epochs never move backwards for one partition.
                if let Some(existing) = self.placement(*tenant_id, *topic_id, *partition_id) {
                    if *ownership_epoch < existing.ownership_epoch {
                        return MetadataCommandResponse::Rejected {
                            reason: "ownership epoch regression".into(),
                        };
                    }
                }
                let placement = PlacementState {
                    tenant_id: *tenant_id,
                    topic_id: *topic_id,
                    partition_id: *partition_id,
                    owner: *owner,
                    ownership_epoch: *ownership_epoch,
                    lease_valid_for_ms: *lease_valid_for_ms,
                };
                self.placements.retain(|existing| {
                    !(existing.tenant_id == *tenant_id
                        && existing.topic_id == *topic_id
                        && existing.partition_id == *partition_id)
                });
                self.placements.push(placement);
                self.placements
                    .sort_by_key(|entry| (entry.tenant_id, entry.topic_id, entry.partition_id));
                self.revision += 1;
                MetadataCommandResponse::Applied {
                    revision: self.revision,
                }
            }
            MetadataCommand::BeginMigration {
                migration_id,
                tenant_id,
                topic_id,
                partition_id,
                source_node,
                target_node,
                source_incarnation,
                original_epoch,
                next_epoch,
                dependent_partitions,
            } => {
                if *migration_id == 0
                    || *source_node == 0
                    || *target_node == 0
                    || *source_node == *target_node
                {
                    return rejected("invalid migration identity");
                }
                if *source_incarnation == 0
                    || *original_epoch == 0
                    || *next_epoch <= *original_epoch
                {
                    return rejected("invalid migration epochs");
                }
                if !self.nodes.contains_key(source_node) || !self.nodes.contains_key(target_node) {
                    return rejected("unknown migration node");
                }
                let Some(placement) = self.placement(*tenant_id, *topic_id, *partition_id) else {
                    return rejected("unknown partition");
                };
                if placement.owner != *source_node {
                    return rejected("the source is not the committed owner");
                }
                if placement.ownership_epoch != *original_epoch {
                    return rejected("stale expected ownership epoch");
                }
                // One migration per partition at a time.
                for existing in self.migrations.values() {
                    if existing.tenant_id != *tenant_id
                        || existing.topic_id != *topic_id
                        || existing.partition_id != *partition_id
                        || existing.stage == MigrationStage::Cleanup
                    {
                        continue;
                    }
                    if existing.migration_id == *migration_id {
                        // The same command again: idempotent, no new revision.
                        return MetadataCommandResponse::Applied {
                            revision: self.revision,
                        };
                    }
                    return rejected("a migration for this partition is already active");
                }
                if let Some(existing) = self.migrations.get(migration_id) {
                    if existing.tenant_id != *tenant_id
                        || existing.topic_id != *topic_id
                        || existing.partition_id != *partition_id
                        || existing.source_node != *source_node
                        || existing.target_node != *target_node
                        || existing.source_incarnation != *source_incarnation
                        || existing.original_epoch != *original_epoch
                        || existing.next_epoch != *next_epoch
                    {
                        return rejected("conflicting migration id");
                    }
                    return MetadataCommandResponse::Applied {
                        revision: self.revision,
                    };
                }
                self.migrations.insert(
                    *migration_id,
                    MigrationState {
                        migration_id: *migration_id,
                        tenant_id: *tenant_id,
                        topic_id: *topic_id,
                        partition_id: *partition_id,
                        stage: MigrationStage::Prepare,
                        source_node: *source_node,
                        target_node: *target_node,
                        source_incarnation: *source_incarnation,
                        original_epoch: *original_epoch,
                        next_epoch: *next_epoch,
                        transfer_watermark: None,
                        manifest_id: None,
                        fence_proof: None,
                        target_ready: false,
                        dependent_partitions: dependent_partitions.clone(),
                    },
                );
                self.revision += 1;
                MetadataCommandResponse::Applied {
                    revision: self.revision,
                }
            }
            MetadataCommand::AdvanceMigration {
                migration_id,
                stage,
                transfer_watermark,
                manifest_id,
                fence_proof,
                target_ready,
            } => {
                let Some(current) = self.migrations.get(migration_id) else {
                    return rejected("unknown migration");
                };
                if current.stage == *stage {
                    // The same stage with the same evidence again: idempotent.
                    if current.transfer_watermark == *transfer_watermark
                        && current.manifest_id == *manifest_id
                        && current.fence_proof == *fence_proof
                        && current.target_ready == *target_ready
                    {
                        return MetadataCommandResponse::Applied {
                            revision: self.revision,
                        };
                    }
                    return rejected("conflicting migration evidence");
                }
                if current.stage.next() != Some(*stage) {
                    return rejected("migration stages may not be skipped");
                }
                match stage {
                    MigrationStage::Prepare => {
                        return rejected("a migration never returns to prepare");
                    }
                    MigrationStage::CatchUp => {
                        if transfer_watermark.is_none() {
                            return rejected("catch up without a verified transfer watermark");
                        }
                        if manifest_id.is_none() {
                            return rejected("catch up without a verified manifest");
                        }
                    }
                    MigrationStage::FenceOld => {
                        if current.transfer_watermark.is_none() {
                            return rejected("fence without a verified transfer watermark");
                        }
                        let Some(proof) = fence_proof else {
                            return rejected("fence without durable fence evidence");
                        };
                        if proof.migration_id != *migration_id
                            || proof.old_owner != current.source_node
                            || proof.old_epoch != current.original_epoch
                        {
                            return rejected("the fence evidence does not match the migration");
                        }
                    }
                    MigrationStage::ActivateNew => {
                        if current.fence_proof.is_none() {
                            return rejected("activation without fence evidence");
                        }
                        if transfer_watermark.is_none() {
                            return rejected("activation without verified transfer completion");
                        }
                        if !*target_ready {
                            return rejected("activation without target readiness evidence");
                        }
                    }
                    MigrationStage::Cleanup => {
                        if current.fence_proof.is_none() {
                            return rejected("cleanup without fence evidence");
                        }
                    }
                }
                let entry = self
                    .migrations
                    .get_mut(migration_id)
                    .expect("the migration exists");
                entry.stage = *stage;
                if transfer_watermark.is_some() {
                    entry.transfer_watermark = *transfer_watermark;
                }
                if manifest_id.is_some() {
                    entry.manifest_id = manifest_id.clone();
                }
                if fence_proof.is_some() {
                    entry.fence_proof = fence_proof.clone();
                }
                entry.target_ready = *target_ready;
                if *stage == MigrationStage::ActivateNew {
                    // The committed activation moves the placement to the new owner and the
                    // new ownership epoch. Until this entry is committed the old owner stays
                    // authoritative.
                    let (tenant_id, topic_id, partition_id, target_node, next_epoch) = (
                        entry.tenant_id,
                        entry.topic_id,
                        entry.partition_id,
                        entry.target_node,
                        entry.next_epoch,
                    );
                    self.set_placement_owner(
                        tenant_id,
                        topic_id,
                        partition_id,
                        target_node,
                        next_epoch,
                    );
                }
                self.revision += 1;
                MetadataCommandResponse::Applied {
                    revision: self.revision,
                }
            }
        }
    }

    /// Moves a placement to a new owner with a new ownership epoch.
    fn set_placement_owner(
        &mut self,
        tenant_id: u64,
        topic_id: u64,
        partition_id: u32,
        owner: MetaNodeId,
        ownership_epoch: u64,
    ) {
        if let Some(placement) = self.placements.iter_mut().find(|entry| {
            entry.tenant_id == tenant_id
                && entry.topic_id == topic_id
                && entry.partition_id == partition_id
        }) {
            placement.owner = owner;
            placement.ownership_epoch = ownership_epoch;
        }
    }

    pub fn placement(
        &self,
        tenant_id: u64,
        topic_id: u64,
        partition_id: u32,
    ) -> Option<&PlacementState> {
        self.placements.iter().find(|entry| {
            entry.tenant_id == tenant_id
                && entry.topic_id == topic_id
                && entry.partition_id == partition_id
        })
    }
}

/// Persisted container of the applied state.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct StateFile {
    version: u32,
    state: MetadataState,
    last_applied: Option<LogId<MetaNodeId>>,
    membership: Option<StoredMembership<MetaNodeId, MetaNode>>,
}

/// The metadata state machine of one node.
pub struct MetadataStateMachine {
    directory: PathBuf,
    /// Notifies subscribers whenever the committed metadata revision changes.
    notify: tokio::sync::watch::Sender<u64>,
    state: MetadataState,
    shared: std::sync::Arc<std::sync::Mutex<MetadataState>>,
    last_applied: Option<LogId<MetaNodeId>>,
    membership: Option<StoredMembership<MetaNodeId, MetaNode>>,
}

impl MetadataStateMachine {
    /// Opens the state machine, restoring the durable state of a previous run.
    pub fn open(
        directory: impl AsRef<Path>,
        cluster_id: u128,
    ) -> Result<Self, StorageError<MetaNodeId>> {
        let directory = directory.as_ref().to_path_buf();
        std::fs::create_dir_all(&directory)
            .map_err(|error| storage_error(ErrorSubject::StateMachine, ErrorVerb::Write, error))?;
        let path = directory.join("metadata-state.bin");
        let mut state = MetadataState {
            cluster_id,
            ..MetadataState::default()
        };
        let mut last_applied = None;
        let mut membership = None;
        match std::fs::read(&path) {
            Ok(bytes) => {
                let file: StateFile = decode_state(&bytes).map_err(|error| {
                    storage_error(
                        ErrorSubject::StateMachine,
                        ErrorVerb::Read,
                        std::io::Error::new(std::io::ErrorKind::InvalidData, error),
                    )
                })?;
                if file.state.cluster_id != cluster_id {
                    return Err(storage_error(
                        ErrorSubject::StateMachine,
                        ErrorVerb::Read,
                        std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            "metadata cluster identity mismatch",
                        ),
                    ));
                }
                state = file.state;
                last_applied = file.last_applied;
                membership = file.membership;
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                return Err(storage_error(
                    ErrorSubject::StateMachine,
                    ErrorVerb::Read,
                    error,
                ));
            }
        }
        let shared = std::sync::Arc::new(std::sync::Mutex::new(state.clone()));
        let (notify, _receiver) = tokio::sync::watch::channel(state.revision);
        Ok(Self {
            directory,
            notify,
            state,
            shared,
            last_applied,
            membership,
        })
    }

    /// Opens the state machine and returns a handle to the shared applied state.
    pub fn open_shared(
        directory: impl AsRef<Path>,
        cluster_id: u128,
    ) -> Result<(Self, std::sync::Arc<std::sync::Mutex<MetadataState>>), StorageError<MetaNodeId>>
    {
        let machine = Self::open(directory, cluster_id)?;
        let shared = machine.shared.clone();
        Ok((machine, shared))
    }

    /// The shared handle of the applied authoritative state.
    pub fn shared_state(&self) -> std::sync::Arc<std::sync::Mutex<MetadataState>> {
        self.shared.clone()
    }

    /// Notifies subscribers about committed metadata revisions.
    pub fn notify(&self) -> tokio::sync::watch::Sender<u64> {
        self.notify.clone()
    }

    /// The currently applied authoritative metadata.
    pub fn state(&self) -> &MetadataState {
        &self.state
    }

    fn persist(&self) -> Result<(), StorageError<MetaNodeId>> {
        let file = StateFile {
            version: STATE_VERSION,
            state: self.state.clone(),
            last_applied: self.last_applied,
            membership: self.membership.clone(),
        };
        let payload = serde_json::to_vec(&file).map_err(|error| {
            storage_error(
                ErrorSubject::StateMachine,
                ErrorVerb::Write,
                std::io::Error::new(std::io::ErrorKind::InvalidData, error),
            )
        })?;
        let mut bytes = Vec::with_capacity(payload.len() + 12);
        bytes.extend_from_slice(STATE_MAGIC);
        bytes.extend_from_slice(&STATE_VERSION.to_be_bytes());
        bytes.extend_from_slice(&crc32c(&payload).to_be_bytes());
        bytes.extend_from_slice(&payload);
        let path = self.directory.join("metadata-state.bin");
        let temporary = self.directory.join("metadata-state.bin.tmp");
        {
            let mut handle = std::fs::File::create(&temporary).map_err(|error| {
                storage_error(ErrorSubject::StateMachine, ErrorVerb::Write, error)
            })?;
            use std::io::Write;
            handle.write_all(&bytes).map_err(|error| {
                storage_error(ErrorSubject::StateMachine, ErrorVerb::Write, error)
            })?;
            handle.sync_all().map_err(|error| {
                storage_error(ErrorSubject::StateMachine, ErrorVerb::Write, error)
            })?;
        }
        std::fs::rename(&temporary, &path)
            .map_err(|error| storage_error(ErrorSubject::StateMachine, ErrorVerb::Write, error))?;
        if let Some(parent) = self.directory.parent() {
            let _ = std::fs::File::open(parent).and_then(|dir| dir.sync_all());
        }
        Ok(())
    }
}

/// Encodes the applied state as a self-describing, checksummed snapshot image.
fn encode_snapshot(
    state: &MetadataState,
    last_applied: Option<LogId<MetaNodeId>>,
    membership: &Option<StoredMembership<MetaNodeId, MetaNode>>,
) -> Result<Vec<u8>, StorageError<MetaNodeId>> {
    let file = StateFile {
        version: STATE_VERSION,
        state: state.clone(),
        last_applied,
        membership: membership.clone(),
    };
    let payload = serde_json::to_vec(&file).map_err(|error| {
        storage_error(
            ErrorSubject::Snapshot(None),
            ErrorVerb::Write,
            std::io::Error::new(std::io::ErrorKind::InvalidData, error),
        )
    })?;
    let mut bytes = Vec::with_capacity(payload.len() + 12);
    bytes.extend_from_slice(STATE_MAGIC);
    bytes.extend_from_slice(&STATE_VERSION.to_be_bytes());
    bytes.extend_from_slice(&crc32c(&payload).to_be_bytes());
    bytes.extend_from_slice(&payload);
    Ok(bytes)
}

/// Atomically replaces one durable file and syncs its directory.
fn write_atomic(path: &Path, bytes: &[u8]) -> Result<(), StorageError<MetaNodeId>> {
    use std::io::Write;
    let mut name = path
        .file_name()
        .map(|name| name.to_string_lossy().to_string())
        .unwrap_or_default();
    name.push_str(".tmp");
    let temporary = path.with_file_name(name);
    {
        let mut handle = std::fs::File::create(&temporary)
            .map_err(|error| storage_error(ErrorSubject::Store, ErrorVerb::Write, error))?;
        handle
            .write_all(bytes)
            .and_then(|()| handle.sync_all())
            .map_err(|error| storage_error(ErrorSubject::Store, ErrorVerb::Write, error))?;
    }
    std::fs::rename(&temporary, path)
        .map_err(|error| storage_error(ErrorSubject::Store, ErrorVerb::Write, error))?;
    if let Some(parent) = path.parent() {
        let _ = std::fs::File::open(parent).and_then(|dir| dir.sync_all());
    }
    Ok(())
}

/// Publishes a snapshot durably: the image first, then the metadata that makes it
/// discoverable. An interrupted publication leaves the previous snapshot in place.
fn publish_snapshot(
    directory: &Path,
    meta: &SnapshotMeta<MetaNodeId, MetaNode>,
    bytes: &[u8],
) -> Result<(), StorageError<MetaNodeId>> {
    write_atomic(&directory.join("snapshot.bin"), bytes)?;
    let meta_bytes = serde_json::to_vec(meta).map_err(|error| {
        storage_error(
            ErrorSubject::Snapshot(None),
            ErrorVerb::Write,
            std::io::Error::new(std::io::ErrorKind::InvalidData, error),
        )
    })?;
    write_atomic(&directory.join("snapshot-meta.json"), &meta_bytes)
}

/// The last durably published snapshot of this node, if any.
fn read_published_snapshot(
    directory: &Path,
) -> Result<Option<Snapshot<MetadataRaftTypeConfig>>, StorageError<MetaNodeId>> {
    let meta_bytes = match std::fs::read(directory.join("snapshot-meta.json")) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => {
            return Err(storage_error(
                ErrorSubject::Snapshot(None),
                ErrorVerb::Read,
                error,
            ));
        }
    };
    let bytes = std::fs::read(directory.join("snapshot.bin"))
        .map_err(|error| storage_error(ErrorSubject::Snapshot(None), ErrorVerb::Read, error))?;
    // A snapshot that does not validate is never served.
    decode_state(&bytes).map_err(|error| {
        storage_error(
            ErrorSubject::Snapshot(None),
            ErrorVerb::Read,
            std::io::Error::new(std::io::ErrorKind::InvalidData, error),
        )
    })?;
    let meta: SnapshotMeta<MetaNodeId, MetaNode> =
        serde_json::from_slice(&meta_bytes).map_err(|error| {
            storage_error(
                ErrorSubject::Snapshot(None),
                ErrorVerb::Read,
                std::io::Error::new(std::io::ErrorKind::InvalidData, error),
            )
        })?;
    Ok(Some(Snapshot {
        meta,
        snapshot: Box::new(Cursor::new(bytes)),
    }))
}

fn decode_state(bytes: &[u8]) -> Result<StateFile, String> {
    if bytes.len() < 12 || &bytes[..4] != STATE_MAGIC {
        return Err("metadata state magic mismatch".into());
    }
    let version = u32::from_be_bytes(bytes[4..8].try_into().unwrap());
    if version != STATE_VERSION {
        return Err(format!("unsupported metadata state version {version}"));
    }
    let checksum = u32::from_be_bytes(bytes[8..12].try_into().unwrap());
    let payload = &bytes[12..];
    if crc32c(payload) != checksum {
        return Err("metadata state checksum mismatch".into());
    }
    serde_json::from_slice(payload).map_err(|error| error.to_string())
}

fn storage_error(
    subject: ErrorSubject<MetaNodeId>,
    verb: ErrorVerb,
    error: std::io::Error,
) -> StorageError<MetaNodeId> {
    StorageError::IO {
        source: StorageIOError::new(subject, verb, openraft::AnyError::new(&error)),
    }
}

impl RaftStateMachine<MetadataRaftTypeConfig> for MetadataStateMachine {
    type SnapshotBuilder = MetadataSnapshotBuilder;

    async fn applied_state(
        &mut self,
    ) -> Result<
        (
            Option<LogId<MetaNodeId>>,
            StoredMembership<MetaNodeId, MetaNode>,
        ),
        StorageError<MetaNodeId>,
    > {
        Ok((
            self.last_applied,
            self.membership.clone().unwrap_or_default(),
        ))
    }

    async fn apply<I>(
        &mut self,
        entries: I,
    ) -> Result<Vec<MetadataCommandResponse>, StorageError<MetaNodeId>>
    where
        I: IntoIterator<Item = Entry<MetadataRaftTypeConfig>>,
        I::IntoIter: openraft::OptionalSend,
    {
        let mut responses = Vec::new();
        for entry in entries {
            self.last_applied = Some(entry.log_id);
            // Exactly one result per applied entry: OpenRaft asserts that the number of
            // results equals the number of entries it handed over.
            match entry.payload {
                EntryPayload::Blank => responses.push(MetadataCommandResponse::Noop {
                    revision: self.state.revision,
                }),
                EntryPayload::Normal(command) => {
                    let response = self.state.apply(&command);
                    responses.push(response);
                }
                EntryPayload::Membership(membership) => {
                    self.membership = Some(StoredMembership::new(Some(entry.log_id), membership));
                    responses.push(MetadataCommandResponse::Noop {
                        revision: self.state.revision,
                    });
                }
            }
        }
        self.persist()?;
        if let Ok(mut shared) = self.shared.lock() {
            *shared = self.state.clone();
        }
        // The revision only advances on committed entries, so a subscriber of this channel
        // never observes an uncommitted metadata change.
        let _ = self.notify.send(self.state.revision);
        Ok(responses)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        MetadataSnapshotBuilder {
            directory: self.directory.clone(),
            state: self.state.clone(),
            last_applied: self.last_applied,
            membership: self.membership.clone(),
        }
    }

    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<Box<Cursor<Vec<u8>>>, StorageError<MetaNodeId>> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<MetaNodeId, MetaNode>,
        snapshot: Box<Cursor<Vec<u8>>>,
    ) -> Result<(), StorageError<MetaNodeId>> {
        let mut bytes = Vec::new();
        let mut reader = snapshot;
        // The received image is written through a cursor that is left at the end of the
        // last chunk; the whole image must be read from the start.
        tokio::io::AsyncSeekExt::seek(&mut reader, std::io::SeekFrom::Start(0))
            .await
            .map_err(|error| storage_error(ErrorSubject::StateMachine, ErrorVerb::Seek, error))?;
        reader
            .read_to_end(&mut bytes)
            .await
            .map_err(|error| storage_error(ErrorSubject::StateMachine, ErrorVerb::Read, error))?;
        let file: StateFile = decode_state(&bytes).map_err(|error| {
            storage_error(
                ErrorSubject::StateMachine,
                ErrorVerb::Read,
                std::io::Error::new(std::io::ErrorKind::InvalidData, error),
            )
        })?;
        if file.state.cluster_id != self.state.cluster_id {
            return Err(storage_error(
                ErrorSubject::StateMachine,
                ErrorVerb::Read,
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "metadata snapshot cluster identity mismatch",
                ),
            ));
        }
        self.state = file.state;
        self.last_applied = file.last_applied;
        self.membership = file.membership;
        if let Ok(mut shared) = self.shared.lock() {
            *shared = self.state.clone();
        }
        self.persist()?;
        // Publish the installed image too, so a restart of this node can serve it onwards.
        publish_snapshot(&self.directory, meta, &bytes)?;
        Ok(())
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<MetadataRaftTypeConfig>>, StorageError<MetaNodeId>> {
        read_published_snapshot(&self.directory)
    }
}

/// Builds a durable snapshot image of the applied metadata.
pub struct MetadataSnapshotBuilder {
    directory: PathBuf,
    state: MetadataState,
    last_applied: Option<LogId<MetaNodeId>>,
    membership: Option<StoredMembership<MetaNodeId, MetaNode>>,
}

impl RaftSnapshotBuilder<MetadataRaftTypeConfig> for MetadataSnapshotBuilder {
    async fn build_snapshot(
        &mut self,
    ) -> Result<Snapshot<MetadataRaftTypeConfig>, StorageError<MetaNodeId>> {
        let bytes = encode_snapshot(&self.state, self.last_applied, &self.membership)?;
        let meta = SnapshotMeta {
            last_log_id: self.last_applied,
            last_membership: self.membership.clone().unwrap_or_default(),
            snapshot_id: self
                .last_applied
                .map(|log_id| log_id.index.to_string())
                .unwrap_or_else(|| "0".to_string()),
        };
        publish_snapshot(&self.directory, &meta, &bytes)?;
        Ok(Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(bytes)),
        })
    }
}
