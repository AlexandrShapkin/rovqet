//! Cluster control plane: identities, inter-node framing, metadata snapshots and routing.
//!
//! C-01 provides the network and routing foundation for later cluster work. It does not
//! implement metadata consensus, ownership handover, WAL migration or Q2 replication.
#![forbid(unsafe_code)]

pub mod ids;
pub mod metadata;
pub mod routing;
pub mod rpc;
pub mod transport;
pub mod wire;

pub use ids::{
    ClusterId, LeaseValidity, MetadataRevision, NodeId, OwnershipEpoch, PeerEndpoint, RpcRequestId,
    TimeoutBudget, TrustContext,
};
pub use metadata::{MetadataSnapshot, MetadataStore, PartitionPlacement, SnapshotFile};
pub use routing::{RouteDecision, decide};
pub use rpc::{
    HealthResponse, OwnershipRequest, OwnershipResponse, RouteAckRequest, RouteAckResponse,
    RouteDelivery, RouteFetchRequest, RouteFetchResponse, RoutePublishRequest,
    RoutePublishResponse, RpcErrorCode, RpcKind,
};
pub use transport::{PeerTransport, TransportError, TransportLimits};
pub use wire::{Envelope, WireError};
