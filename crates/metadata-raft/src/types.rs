//! Metadata command and response types plus the Raft type configuration.
//!
//! The public control-plane model of `rovqet-control` is not changed here: OpenRaft types
//! stay inside this adapter.
use openraft::Entry;
use openraft::{RaftTypeConfig, TokioRuntime};

/// Node identity inside one metadata cluster.
pub type MetaNodeId = u64;

/// One metadata member with its peer endpoint.
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct MetaNode {
    pub id: MetaNodeId,
    pub address: String,
}

/// Committed metadata mutations. Everything authoritative enters through these commands.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum MetadataCommand {
    /// Register or update a node endpoint.
    SetNode { node: MetaNodeId, address: String },
    /// Declare a topic and its durable class.
    SetTopicPolicy {
        topic_id: u64,
        durable_class: u8,
        policy_revision: u64,
    },
    /// Place a partition with an ownership epoch and a bounded lease.
    SetPlacement {
        tenant_id: u64,
        topic_id: u64,
        partition_id: u32,
        owner: MetaNodeId,
        ownership_epoch: u64,
        lease_valid_for_ms: u64,
    },
    /// Start a Q1 ownership migration.
    ///
    /// The current committed owner stays authoritative: this command only records the
    /// intent and freezes the target identity.
    BeginMigration {
        migration_id: u128,
        tenant_id: u64,
        topic_id: u64,
        partition_id: u32,
        source_node: MetaNodeId,
        target_node: MetaNodeId,
        /// The log incarnation that must be transferred unchanged.
        source_incarnation: u128,
        /// The ownership epoch the caller believes the partition currently has.
        original_epoch: u64,
        /// The ownership epoch the target will own after the activation.
        next_epoch: u64,
        /// Dependent partitions whose durable state the migration needs, such as the DLQ
        /// partition of the same tenant and topic.
        dependent_partitions: Vec<DependentPartition>,
    },
    /// Advance a committed migration by exactly one stage.
    AdvanceMigration {
        migration_id: u128,
        stage: MigrationStage,
        /// Last committed source offset that was transferred and verified.
        transfer_watermark: Option<u64>,
        /// Identity of the verified manifest of the transferred inventory.
        manifest_id: Option<String>,
        /// Durable proof that the old owner revoked its write authority.
        fence_proof: Option<FenceProof>,
        /// The target published that it validated the complete transferred inventory.
        target_ready: bool,
        /// The evidence of every dependent partition of the bundle. A source-only
        /// migration carries none; a bundle must carry one entry per dependency.
        dependencies: Vec<DependentEvidence>,
    },
}

/// The committed evidence of one dependent partition of a bundle.
///
/// A bundle moves the source and its dependent partitions (the local DLQ) as one
/// authoritative activation: the source may not become active while a required dependency
/// is incomplete.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct DependentEvidence {
    pub tenant_id: u64,
    pub topic_id: u64,
    pub partition_id: u32,
    /// The incarnation that must be transferred unchanged.
    pub incarnation: u128,
    /// The ownership epoch the dependency held when the migration started.
    pub original_epoch: u64,
    /// The ownership epoch the target owns after the activation.
    pub next_epoch: u64,
    /// Last committed offset of the dependency that was transferred and verified.
    pub transfer_watermark: Option<u64>,
    /// Identity of the verified manifest of the transferred dependency inventory.
    pub manifest_id: Option<String>,
    /// Durable proof that the old owner revoked its write authority on the dependency.
    pub fence_proof: Option<FenceProof>,
    /// The target published that it validated the dependency inventory.
    pub ready: bool,
}

/// Ordered stages of a Q1 ownership migration.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, serde::Serialize, serde::Deserialize,
)]
pub enum MigrationStage {
    /// The migration is committed; the old owner is still the sole writer.
    Prepare,
    /// The committed history is copied and verified.
    CatchUp,
    /// The old owner durably revoked its write authority.
    FenceOld,
    /// The target owns the partition with the new ownership epoch.
    ActivateNew,
    /// The old copy may be released under an explicit retention policy.
    Cleanup,
}

impl MigrationStage {
    /// The one stage that may follow this one.
    pub fn next(self) -> Option<MigrationStage> {
        match self {
            MigrationStage::Prepare => Some(MigrationStage::CatchUp),
            MigrationStage::CatchUp => Some(MigrationStage::FenceOld),
            MigrationStage::FenceOld => Some(MigrationStage::ActivateNew),
            MigrationStage::ActivateNew => Some(MigrationStage::Cleanup),
            MigrationStage::Cleanup => None,
        }
    }

    /// The stable ordinal of the stage.
    pub fn ordinal(self) -> u8 {
        match self {
            MigrationStage::Prepare => 0,
            MigrationStage::CatchUp => 1,
            MigrationStage::FenceOld => 2,
            MigrationStage::ActivateNew => 3,
            MigrationStage::Cleanup => 4,
        }
    }
}

/// Durable proof that the old owner revoked its write authority.
///
/// The proof is produced by the old owner itself and is bound to the migration and to the
/// old ownership epoch, so it can never be replayed as an authorization for another
/// migration.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct FenceProof {
    pub migration_id: u128,
    /// The node that revoked its authority.
    pub old_owner: MetaNodeId,
    /// The ownership epoch that was revoked.
    pub old_epoch: u64,
    /// Durable offset of the fence record in the old owner's WAL.
    pub fence_offset: u64,
    /// Digest of the durable fence record.
    pub fence_digest: String,
    /// Manifest generation that contains the fence record.
    pub manifest_generation: u64,
}

/// A partition whose durable state a migration depends on, such as a DLQ partition.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct DependentPartition {
    pub tenant_id: u64,
    pub topic_id: u64,
    pub partition_id: u32,
    /// Durable class of the dependent partition.
    pub durable_class: u8,
    /// The log incarnation the dependency refers to.
    pub incarnation: u128,
}

/// Applied command outcome.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum MetadataCommandResponse {
    /// A committed command mutated the metadata state.
    Applied { revision: u64 },
    /// A committed entry that is not an application command (blank or membership).
    /// The state machine must return exactly one result per applied entry.
    Noop { revision: u64 },
    /// A committed command was rejected deterministically by the state machine.
    Rejected { reason: String },
}

/// Raft type configuration of the metadata group.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord)]
pub struct MetadataRaftTypeConfig;

impl RaftTypeConfig for MetadataRaftTypeConfig {
    type D = MetadataCommand;
    type R = MetadataCommandResponse;
    type NodeId = MetaNodeId;
    type Node = MetaNode;
    type Entry = Entry<MetadataRaftTypeConfig>;
    /// Snapshots are byte images of the applied metadata state.
    type SnapshotData = std::io::Cursor<Vec<u8>>;
    type AsyncRuntime = TokioRuntime;
    type Responder = openraft::impls::OneshotResponder<Self>;
}
