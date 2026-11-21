//! Q2 shard command and response types plus the Raft type configuration.
//!
//! A Q2 group is identified by the pair (cluster_id, [`ShardId`]). OpenRaft types stay
//! inside this adapter: the public data-plane model of `rovqet-core` is not changed here.
use openraft::{Entry, RaftTypeConfig, TokioRuntime};

/// Node identity inside one Q2 shard group.
pub type ShardNodeId = u64;

/// Q2 shard identity. A Q2 group is identified by the pair (cluster_id, shard_id).
pub type ShardId = u64;

/// Largest inline payload a replicated command may carry.
///
/// Every inline byte is replicated to all members of the shard group, so the command
/// payload stays small; larger data travels as a durable [`ShardPayload::Reference`]
/// whose bytes live in the Q1 WAL instead of in the Raft log.
pub const MAX_INLINE_PAYLOAD_BYTES: usize = 64 * 1024;

/// One shard-group member with its peer endpoint.
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ShardNode {
    pub id: ShardNodeId,
    pub address: String,
}

/// The replicated command of one shard group.
///
/// The command is the unit the state machine applies: it names the request, the partition
/// it belongs to, the payload to apply and the metadata ownership epoch the submitter
/// observed.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ShardCommand {
    /// Caller-visible identity of the request, echoed in the response.
    pub request_id: u128,
    /// The partition this command applies to.
    #[serde(with = "partition_ref_serde")]
    pub partition: rovqet_core::PartitionRef,
    /// What the command carries: bounded inline bytes or a durable data reference.
    pub payload: ShardPayload,
    /// The metadata ownership epoch the submitter believed to be current. A state machine
    /// that applies a command from a stale epoch rejects it deterministically.
    pub metadata_epoch: u64,
}

/// What a replicated command carries.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum ShardPayload {
    /// The payload bytes themselves, bounded by [`MAX_INLINE_PAYLOAD_BYTES`].
    Inline(Vec<u8>),
    /// A durable data reference: the bytes live in the Q1 WAL of `incarnation` at
    /// `offset`, not in the Raft entry.
    Reference { incarnation: u128, offset: u64 },
}

impl ShardPayload {
    /// Wraps `bytes` as an inline payload and refuses an oversized one, so the bound is
    /// enforced wherever a payload is built.
    pub fn inline(bytes: Vec<u8>) -> Result<Self, InlinePayloadTooLarge> {
        let payload = ShardPayload::Inline(bytes);
        payload.validate()?;
        Ok(payload)
    }

    /// Checks the payload against [`MAX_INLINE_PAYLOAD_BYTES`].
    pub fn validate(&self) -> Result<(), InlinePayloadTooLarge> {
        match self {
            ShardPayload::Inline(bytes) if bytes.len() > MAX_INLINE_PAYLOAD_BYTES => {
                Err(InlinePayloadTooLarge { len: bytes.len() })
            }
            ShardPayload::Inline(_) | ShardPayload::Reference { .. } => Ok(()),
        }
    }
}

/// Refused because the inline payload exceeds [`MAX_INLINE_PAYLOAD_BYTES`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InlinePayloadTooLarge {
    /// The rejected payload length in bytes.
    pub len: usize,
}

impl std::fmt::Display for InlinePayloadTooLarge {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "inline payload of {} bytes exceeds the bound of {}",
            self.len, MAX_INLINE_PAYLOAD_BYTES
        )
    }
}

impl std::error::Error for InlinePayloadTooLarge {}

/// Outcome of applying one committed entry.
///
/// The state machine must return exactly one response per applied entry.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum ShardCommandResponse {
    /// A committed command mutated the shard state.
    Applied {
        index: u64,
        request_id: u128,
        #[serde(with = "partition_ref_serde")]
        partition: rovqet_core::PartitionRef,
        applied_count: u64,
    },
    /// A committed command was rejected deterministically by the state machine.
    Rejected { reason: String },
    /// A committed entry that is not an application command (blank or membership).
    Noop { index: u64 },
}

/// Raft type configuration of one Q2 shard group.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord)]
pub struct ShardRaftTypeConfig;

impl RaftTypeConfig for ShardRaftTypeConfig {
    type D = ShardCommand;
    type R = ShardCommandResponse;
    type NodeId = ShardNodeId;
    type Node = ShardNode;
    type Entry = Entry<ShardRaftTypeConfig>;
    /// Snapshots are byte images of the applied shard state.
    type SnapshotData = std::io::Cursor<Vec<u8>>;
    type AsyncRuntime = TokioRuntime;
    type Responder = openraft::impls::OneshotResponder<Self>;
}

/// Identity of one Q2 group: the pair (cluster_id, shard_id).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct ShardGroupIdentity {
    /// The cluster the shard belongs to.
    pub cluster_id: u128,
    /// The shard inside that cluster.
    pub shard_id: ShardId,
}

impl ShardGroupIdentity {
    /// The stable string used for the storage format and for snapshot names.
    ///
    /// The rendering is fixed and unambiguous: fixed `q2:` prefix plus two decimal
    /// fields separated by `:`, so two different identities can never render to the
    /// same string and the same identity always renders identically.
    pub fn render(&self) -> String {
        format!("q2:{}:{}", self.cluster_id, self.shard_id)
    }
}

impl std::fmt::Display for ShardGroupIdentity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.render())
    }
}

/// `rovqet_core::PartitionRef` carries no serde implementation of its own; this module
/// gives it a stable wire representation without changing the core type.
mod partition_ref_serde {
    use rovqet_core::PartitionRef;
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    #[derive(Serialize, Deserialize)]
    struct Repr {
        tenant_id: u64,
        topic_id: u64,
        partition_id: u32,
    }

    pub fn serialize<S: Serializer>(
        value: &PartitionRef,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        Repr {
            tenant_id: value.tenant_id,
            topic_id: value.topic_id,
            partition_id: value.partition_id,
        }
        .serialize(serializer)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<PartitionRef, D::Error> {
        let repr = Repr::deserialize(deserializer)?;
        Ok(PartitionRef {
            tenant_id: repr.tenant_id,
            topic_id: repr.topic_id,
            partition_id: repr.partition_id,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn identity(cluster_id: u128, shard_id: ShardId) -> ShardGroupIdentity {
        ShardGroupIdentity {
            cluster_id,
            shard_id,
        }
    }

    #[test]
    fn the_group_identity_renders_stably_and_unambiguously() {
        assert_eq!(identity(7, 9).render(), "q2:7:9");
        assert_eq!(identity(7, 9).to_string(), "q2:7:9");
        assert_ne!(
            identity(7, 9).render(),
            identity(9, 7).render(),
            "different identities must not collide in one rendering"
        );
        assert_ne!(
            identity(7, 9).render(),
            identity(7, 10).render(),
            "different identities must not collide in one rendering"
        );
    }

    #[test]
    fn an_oversized_inline_payload_is_refused() {
        let bounded = vec![0u8; MAX_INLINE_PAYLOAD_BYTES];
        assert_eq!(
            ShardPayload::inline(bounded).expect("bounded"),
            ShardPayload::Inline(vec![0u8; MAX_INLINE_PAYLOAD_BYTES])
        );

        let oversized = vec![0u8; MAX_INLINE_PAYLOAD_BYTES + 1];
        let error = ShardPayload::inline(oversized).expect_err("oversized");
        assert_eq!(error.len, MAX_INLINE_PAYLOAD_BYTES + 1);

        let reference = ShardPayload::Reference {
            incarnation: 1,
            offset: 2,
        };
        reference
            .validate()
            .expect("a reference is never inline-bounded");
    }

    #[test]
    fn a_command_with_a_partition_round_trips_through_serde() {
        let command = ShardCommand {
            request_id: 42,
            partition: rovqet_core::PartitionRef {
                tenant_id: 1,
                topic_id: 2,
                partition_id: 3,
            },
            payload: ShardPayload::Inline(vec![9, 9]),
            metadata_epoch: 5,
        };
        let bytes = serde_json::to_vec(&command).expect("serialize");
        let decoded: ShardCommand = serde_json::from_slice(&bytes).expect("deserialize");
        assert_eq!(decoded, command);
    }
}
