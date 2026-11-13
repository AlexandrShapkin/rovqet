//! Immutable, versioned metadata snapshots and their atomic installation.
//!
//! A snapshot is a development authority fixture: it is *not* a Raft-committed snapshot.
//! It is validated against the local cluster identity, installed atomically and never
//! replaced by an older revision. Ownership authority is time-bounded from the moment the
//! snapshot was installed locally, so a process restart cannot extend authority.
use crate::ids::{
    ClusterId, LeaseValidity, MetadataRevision, NodeId, OwnershipEpoch, PeerEndpoint,
};
use rovqet_core::PartitionRef;
use std::collections::HashSet;
use std::fmt;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DurableClass {
    /// Q0-like volatile class.
    Volatile,
    /// Q1 local durability class.
    DurableLocal,
    /// Q2 quorum durability class (not implemented in C-01).
    DurableQuorum,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeEntry {
    pub node: NodeId,
    pub endpoint: PeerEndpoint,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopicPolicy {
    pub topic_id: u64,
    pub durable_class: DurableClass,
    pub policy_revision: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionPlacement {
    pub partition: PartitionRef,
    pub owner: NodeId,
    pub ownership_epoch: OwnershipEpoch,
    /// Relative lease validity, counted from local installation of the snapshot.
    pub lease: LeaseValidity,
    pub desired_replicas: u32,
    pub shard_id: u64,
    pub required_policy_revision: u64,
}

/// One immutable metadata snapshot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetadataSnapshot {
    pub cluster_id: ClusterId,
    pub revision: MetadataRevision,
    /// Relative issue time of the snapshot (never a cross-host absolute clock).
    pub issued_at_ms: u64,
    /// Relative revalidation deadline after local installation.
    pub revalidate_after_ms: u64,
    pub nodes: Vec<NodeEntry>,
    pub topics: Vec<TopicPolicy>,
    pub partitions: Vec<PartitionPlacement>,
    pub acl_revision: u64,
    pub policy_revision: u64,
    pub capability_revision: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MetadataError {
    ClusterMismatch,
    RevisionRegression,
    DuplicateNode,
    DuplicatePartition,
    UnknownOwner,
    InvalidEpoch,
    MissingLease,
    IncompatiblePolicy,
    Malformed(&'static str),
}

impl fmt::Display for MetadataError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for MetadataError {}

impl MetadataSnapshot {
    /// Structural validation independent of the local cluster identity.
    pub fn validate(&self) -> Result<(), MetadataError> {
        if self.revision.get() == 0 {
            return Err(MetadataError::Malformed("revision"));
        }
        if self.revalidate_after_ms == 0 {
            return Err(MetadataError::Malformed("revalidate deadline"));
        }
        let mut nodes = HashSet::new();
        for entry in &self.nodes {
            if !nodes.insert(entry.node) {
                return Err(MetadataError::DuplicateNode);
            }
        }
        let mut partitions = HashSet::new();
        for placement in &self.partitions {
            if !partitions.insert(placement.partition) {
                return Err(MetadataError::DuplicatePartition);
            }
            if !nodes.contains(&placement.owner) {
                return Err(MetadataError::UnknownOwner);
            }
            if placement.ownership_epoch.get() == 0 {
                return Err(MetadataError::InvalidEpoch);
            }
            if placement.lease.valid_for_ms == 0 {
                return Err(MetadataError::MissingLease);
            }
            if placement.required_policy_revision > self.policy_revision {
                return Err(MetadataError::IncompatiblePolicy);
            }
            if placement.desired_replicas == 0 {
                return Err(MetadataError::Malformed("desired replicas"));
            }
            let policy = self
                .topics
                .iter()
                .find(|policy| policy.topic_id == placement.partition.topic_id);
            if let Some(policy) = policy {
                if policy.policy_revision > self.policy_revision {
                    return Err(MetadataError::IncompatiblePolicy);
                }
            }
        }
        Ok(())
    }

    pub fn owner_of(&self, partition: &PartitionRef) -> Option<&PartitionPlacement> {
        self.partitions
            .iter()
            .find(|placement| &placement.partition == partition)
    }

    pub fn node(&self, node: NodeId) -> Option<&NodeEntry> {
        self.nodes.iter().find(|entry| entry.node == node)
    }

    pub fn policy_of(&self, topic_id: u64) -> Option<&TopicPolicy> {
        self.topics
            .iter()
            .find(|policy| policy.topic_id == topic_id)
    }
}

/// File representation of a snapshot fixture (explicit integers, no native layout).
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct SnapshotFile {
    pub cluster_id: u128,
    pub revision: u64,
    #[serde(default)]
    pub issued_at_ms: u64,
    #[serde(default)]
    pub revalidate_after_ms: u64,
    #[serde(default)]
    pub acl_revision: u64,
    pub policy_revision: u64,
    pub capability_revision: u64,
    #[serde(default)]
    pub nodes: Vec<NodeFile>,
    #[serde(default)]
    pub topics: Vec<TopicPolicyFile>,
    #[serde(default)]
    pub partitions: Vec<PartitionFile>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct NodeFile {
    pub node: u64,
    pub address: String,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct TopicPolicyFile {
    pub topic_id: u64,
    pub durable_class: u8,
    pub policy_revision: u64,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct PartitionFile {
    pub tenant_id: u64,
    pub topic_id: u64,
    pub partition_id: u32,
    pub owner: u64,
    pub ownership_epoch: u64,
    pub lease_valid_for_ms: u64,
    #[serde(default = "one")]
    pub desired_replicas: u32,
    #[serde(default)]
    pub shard_id: u64,
    pub required_policy_revision: u64,
}

fn one() -> u32 {
    1
}

impl From<&MetadataSnapshot> for SnapshotFile {
    fn from(snapshot: &MetadataSnapshot) -> Self {
        Self {
            cluster_id: snapshot.cluster_id.get(),
            revision: snapshot.revision.get(),
            issued_at_ms: snapshot.issued_at_ms,
            revalidate_after_ms: snapshot.revalidate_after_ms,
            acl_revision: snapshot.acl_revision,
            policy_revision: snapshot.policy_revision,
            capability_revision: snapshot.capability_revision,
            nodes: snapshot
                .nodes
                .iter()
                .map(|entry| NodeFile {
                    node: entry.node.get(),
                    address: entry.endpoint.address.to_string(),
                })
                .collect(),
            topics: snapshot
                .topics
                .iter()
                .map(|policy| TopicPolicyFile {
                    topic_id: policy.topic_id,
                    durable_class: match policy.durable_class {
                        DurableClass::Volatile => 0,
                        DurableClass::DurableLocal => 1,
                        DurableClass::DurableQuorum => 2,
                    },
                    policy_revision: policy.policy_revision,
                })
                .collect(),
            partitions: snapshot
                .partitions
                .iter()
                .map(|placement| PartitionFile {
                    tenant_id: placement.partition.tenant_id,
                    topic_id: placement.partition.topic_id,
                    partition_id: placement.partition.partition_id,
                    owner: placement.owner.get(),
                    ownership_epoch: placement.ownership_epoch.get(),
                    lease_valid_for_ms: placement.lease.valid_for_ms,
                    desired_replicas: placement.desired_replicas,
                    shard_id: placement.shard_id,
                    required_policy_revision: placement.required_policy_revision,
                })
                .collect(),
        }
    }
}

impl TryFrom<SnapshotFile> for MetadataSnapshot {
    type Error = MetadataError;
    fn try_from(file: SnapshotFile) -> Result<Self, MetadataError> {
        let cluster_id =
            ClusterId::new(file.cluster_id).ok_or(MetadataError::Malformed("cluster id"))?;
        let revision =
            MetadataRevision::new(file.revision).ok_or(MetadataError::Malformed("revision"))?;
        let mut nodes = Vec::with_capacity(file.nodes.len());
        for node in file.nodes {
            let identity = NodeId::new(node.node).ok_or(MetadataError::Malformed("node id"))?;
            let address = node
                .address
                .parse()
                .map_err(|_| MetadataError::Malformed("node address"))?;
            nodes.push(NodeEntry {
                node: identity,
                endpoint: PeerEndpoint {
                    node: identity,
                    address,
                },
            });
        }
        let mut topics = Vec::with_capacity(file.topics.len());
        for topic in file.topics {
            topics.push(TopicPolicy {
                topic_id: topic.topic_id,
                durable_class: match topic.durable_class {
                    0 => DurableClass::Volatile,
                    1 => DurableClass::DurableLocal,
                    2 => DurableClass::DurableQuorum,
                    _ => return Err(MetadataError::Malformed("durable class")),
                },
                policy_revision: topic.policy_revision,
            });
        }
        let mut partitions = Vec::with_capacity(file.partitions.len());
        for placement in file.partitions {
            partitions.push(PartitionPlacement {
                partition: PartitionRef {
                    tenant_id: placement.tenant_id,
                    topic_id: placement.topic_id,
                    partition_id: placement.partition_id,
                },
                owner: NodeId::new(placement.owner).ok_or(MetadataError::Malformed("owner id"))?,
                ownership_epoch: OwnershipEpoch::new(placement.ownership_epoch)
                    .ok_or(MetadataError::InvalidEpoch)?,
                lease: LeaseValidity::new(placement.lease_valid_for_ms)
                    .ok_or(MetadataError::MissingLease)?,
                desired_replicas: placement.desired_replicas.max(1),
                shard_id: placement.shard_id,
                required_policy_revision: placement.required_policy_revision,
            });
        }
        let snapshot = MetadataSnapshot {
            cluster_id,
            revision,
            issued_at_ms: file.issued_at_ms,
            revalidate_after_ms: file.revalidate_after_ms,
            nodes,
            topics,
            partitions,
            acl_revision: file.acl_revision,
            policy_revision: file.policy_revision,
            capability_revision: file.capability_revision,
        };
        snapshot.validate()?;
        Ok(snapshot)
    }
}

struct Installed {
    snapshot: Arc<MetadataSnapshot>,
    installed_at: Instant,
}

impl std::fmt::Debug for Installed {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Installed")
            .field("revision", &self.snapshot.revision)
            .finish()
    }
}

/// Atomic snapshot store. One writer at a time, readers observe an immutable snapshot.
#[derive(Debug)]
pub struct MetadataStore {
    cluster: ClusterId,
    current: RwLock<Option<Installed>>,
}

impl MetadataStore {
    pub fn new(cluster: ClusterId) -> Self {
        Self {
            cluster,
            current: RwLock::new(None),
        }
    }

    /// Installs a snapshot. Older or equal revisions are rejected; a stale snapshot can
    /// never replace a newer committed one.
    pub fn install(
        &self,
        snapshot: MetadataSnapshot,
    ) -> Result<Arc<MetadataSnapshot>, MetadataError> {
        if snapshot.cluster_id != self.cluster {
            return Err(MetadataError::ClusterMismatch);
        }
        snapshot.validate()?;
        let mut guard = self
            .current
            .write()
            .map_err(|_| MetadataError::Malformed("store"))?;
        if let Some(installed) = guard.as_ref() {
            if snapshot.revision <= installed.snapshot.revision {
                return Err(MetadataError::RevisionRegression);
            }
        }
        let snapshot = Arc::new(snapshot);
        *guard = Some(Installed {
            snapshot: snapshot.clone(),
            installed_at: Instant::now(),
        });
        Ok(snapshot)
    }

    pub fn current(&self) -> Option<Arc<MetadataSnapshot>> {
        self.current
            .read()
            .ok()
            .and_then(|guard| guard.as_ref().map(|installed| installed.snapshot.clone()))
    }

    /// Age of the installed authority. A restart starts with no authority at all.
    pub fn authority_age(&self) -> Option<Duration> {
        self.current.read().ok().and_then(|guard| {
            guard
                .as_ref()
                .map(|installed| installed.installed_at.elapsed())
        })
    }

    /// True when the installed authority for the placement is past its lease validity.
    pub fn authority_expired(&self, placement: &PartitionPlacement) -> bool {
        match self.authority_age() {
            Some(age) => age > Duration::from_millis(placement.lease.valid_for_ms),
            None => true,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn snapshot(revision: u64) -> MetadataSnapshot {
        MetadataSnapshot {
            cluster_id: ClusterId::new(7).unwrap(),
            revision: MetadataRevision::new(revision).unwrap(),
            nodes: vec![NodeEntry {
                node: NodeId::new(1).unwrap(),
                endpoint: PeerEndpoint {
                    node: NodeId::new(1).unwrap(),
                    address: "127.0.0.1:9000".parse().unwrap(),
                },
            }],
            topics: vec![TopicPolicy {
                topic_id: 3,
                durable_class: DurableClass::DurableLocal,
                policy_revision: 1,
            }],
            partitions: vec![PartitionPlacement {
                partition: PartitionRef {
                    tenant_id: 1,
                    topic_id: 3,
                    partition_id: 0,
                },
                owner: NodeId::new(1).unwrap(),
                ownership_epoch: OwnershipEpoch::new(4).unwrap(),
                lease: LeaseValidity::new(60_000).unwrap(),
                desired_replicas: 1,
                shard_id: 0,
                required_policy_revision: 1,
            }],
            issued_at_ms: 1,
            revalidate_after_ms: 60_000,
            acl_revision: 1,
            policy_revision: 1,
            capability_revision: 1,
        }
    }

    #[test]
    fn snapshots_install_and_never_regress() {
        let store = MetadataStore::new(ClusterId::new(7).unwrap());
        assert!(store.current().is_none());
        store.install(snapshot(2)).unwrap();
        assert_eq!(store.current().unwrap().revision.get(), 2);
        assert!(matches!(
            store.install(snapshot(1)),
            Err(MetadataError::RevisionRegression)
        ));
        assert!(matches!(
            store.install(snapshot(2)),
            Err(MetadataError::RevisionRegression)
        ));
        store.install(snapshot(3)).unwrap();
        assert_eq!(store.current().unwrap().revision.get(), 3);
    }

    #[test]
    fn wrong_cluster_is_rejected() {
        let store = MetadataStore::new(ClusterId::new(9).unwrap());
        assert!(matches!(
            store.install(snapshot(1)),
            Err(MetadataError::ClusterMismatch)
        ));
    }

    #[test]
    fn structurally_invalid_snapshots_are_rejected() {
        let mut broken = snapshot(1);
        broken.partitions[0].owner = NodeId::new(42).unwrap();
        assert!(matches!(
            broken.validate(),
            Err(MetadataError::UnknownOwner)
        ));
        let mut broken = snapshot(1);
        broken.partitions.push(broken.partitions[0].clone());
        assert!(matches!(
            broken.validate(),
            Err(MetadataError::DuplicatePartition)
        ));
        let mut broken = snapshot(1);
        broken.partitions[0].ownership_epoch = OwnershipEpoch(0);
        assert!(matches!(
            broken.validate(),
            Err(MetadataError::InvalidEpoch)
        ));
    }
}
