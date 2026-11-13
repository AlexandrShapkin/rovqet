//! Explicit cluster identities.
//!
//! Cluster and node identities are distinct from tenant, topic, producer and consumer
//! identifiers. A peer may assert an identity, but the local trust context decides which
//! identities are acceptable, and only a validated metadata snapshot grants ownership.
use std::fmt;
use std::net::SocketAddr;

macro_rules! id_type {
    ($name:ident, $inner:ty, $doc:literal) => {
        #[doc = $doc]
        #[derive(
            Debug,
            Clone,
            Copy,
            PartialEq,
            Eq,
            PartialOrd,
            Ord,
            Hash,
            serde::Serialize,
            serde::Deserialize,
        )]
        #[serde(transparent)]
        pub struct $name(pub $inner);

        impl $name {
            /// Identities must be non-zero: zero is the reserved "unknown" value.
            pub fn new(value: $inner) -> Option<Self> {
                (value != 0).then_some(Self(value))
            }
            pub fn get(self) -> $inner {
                self.0
            }
        }
        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, "{}", self.0)
            }
        }
    };
}

id_type!(
    ClusterId,
    u128,
    "Cluster identity shared by all nodes of one deployment."
);
id_type!(NodeId, u64, "Node identity inside one cluster.");
id_type!(
    MetadataRevision,
    u64,
    "Monotonically increasing metadata snapshot revision."
);
id_type!(
    OwnershipEpoch,
    u64,
    "Ownership epoch of a partition assignment."
);
id_type!(
    RpcRequestId,
    u128,
    "Transport correlation identifier of one inter-node request."
);

/// One configured peer of the local node.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct PeerEndpoint {
    pub node: NodeId,
    pub address: SocketAddr,
}

/// A relative timeout budget. Absolute monotonic time is never shared between hosts.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct TimeoutBudget {
    pub millis: u32,
}

impl TimeoutBudget {
    pub fn new(millis: u32) -> Option<Self> {
        (millis > 0 && millis <= 60_000).then_some(Self { millis })
    }
    pub fn as_duration(self) -> std::time::Duration {
        std::time::Duration::from_millis(u64::from(self.millis))
    }
}

/// Ownership lease validity of a snapshot: the local monotonic deadline after which the
/// authority must be revalidated. `None` means "no lease modelled" and is rejected for
/// authoritative placements.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct LeaseValidity {
    /// Milliseconds of validity counted from the moment the snapshot was installed locally.
    pub valid_for_ms: u64,
}

impl LeaseValidity {
    pub fn new(valid_for_ms: u64) -> Option<Self> {
        (valid_for_ms > 0).then_some(Self { valid_for_ms })
    }
}

/// Ownership authority for one partition.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct PartitionOwner {
    pub node: NodeId,
    pub epoch: OwnershipEpoch,
}

/// Local trust context: the identities this node accepts from configured peers.
#[derive(Debug, Clone)]
pub struct TrustContext {
    pub cluster: ClusterId,
    pub node: NodeId,
    /// Explicitly configured peers. A source identity outside this set is rejected.
    pub peers: Vec<PeerEndpoint>,
}

impl TrustContext {
    pub fn new(cluster: ClusterId, node: NodeId, peers: Vec<PeerEndpoint>) -> Self {
        Self {
            cluster,
            node,
            peers,
        }
    }
    pub fn peer(&self, node: NodeId) -> Option<&PeerEndpoint> {
        self.peers.iter().find(|peer| peer.node == node)
    }
    pub fn accepts_source(&self, node: NodeId) -> bool {
        self.peers.iter().any(|peer| peer.node == node)
    }
}
