//! Owner-aware routing decisions.
//!
//! A routing decision reads exactly one validated metadata snapshot. A routed request may
//! be forwarded at most once: a node that receives a routed request never forwards it
//! again, which bounds redirect loops.
use crate::ids::TrustContext;
use crate::ids::{MetadataRevision, NodeId, OwnershipEpoch};
use crate::metadata::MetadataStore;
use rovqet_core::PartitionRef;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RouteDecision {
    /// This node is the authorized owner and may act locally.
    Local {
        epoch: OwnershipEpoch,
        revision: MetadataRevision,
    },
    /// Forward exactly once to the authorized owner.
    Forward {
        target: NodeId,
        epoch: OwnershipEpoch,
        revision: MetadataRevision,
    },
    /// The caller's ownership epoch does not match the installed snapshot.
    StaleEpoch { expected: u64, actual: u64 },
    /// The installed authority is past its lease validity.
    ExpiredAuthority,
    /// No metadata snapshot is installed (for example directly after a restart).
    NoMetadata,
    /// The partition is not placed in the installed snapshot.
    UnknownPartition,
    /// The request already travelled one hop and may not be forwarded again.
    ForwardHopExceeded,
}

impl RouteDecision {
    pub fn is_local(&self) -> bool {
        matches!(self, Self::Local { .. })
    }
}

/// Decides how a partition request must be served.
pub fn decide(
    store: &MetadataStore,
    trust: &TrustContext,
    partition: &PartitionRef,
    expected_epoch: Option<u64>,
    already_forwarded: bool,
) -> RouteDecision {
    let Some(snapshot) = store.current() else {
        return RouteDecision::NoMetadata;
    };
    let Some(placement) = snapshot.owner_of(partition) else {
        return RouteDecision::UnknownPartition;
    };
    if store.authority_expired(placement) {
        return RouteDecision::ExpiredAuthority;
    }
    if let Some(expected) = expected_epoch {
        if expected != placement.ownership_epoch.get() {
            return RouteDecision::StaleEpoch {
                expected,
                actual: placement.ownership_epoch.get(),
            };
        }
    }
    if placement.owner == trust.node {
        return RouteDecision::Local {
            epoch: placement.ownership_epoch,
            revision: snapshot.revision,
        };
    }
    if already_forwarded {
        // A routed request is never routed again: this is the loop bound.
        return RouteDecision::ForwardHopExceeded;
    }
    RouteDecision::Forward {
        target: placement.owner,
        epoch: placement.ownership_epoch,
        revision: snapshot.revision,
    }
}
