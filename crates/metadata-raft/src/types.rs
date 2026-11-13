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
    /// Record migration intent. The migration protocol itself is a later milestone; the
    /// committed schema exists so that later stages never need a new command family.
    SetMigration {
        migration_id: u128,
        tenant_id: u64,
        topic_id: u64,
        partition_id: u32,
        stage: u8,
    },
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
