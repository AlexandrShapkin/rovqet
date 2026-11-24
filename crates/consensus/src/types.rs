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
///
/// A Q2 publication ([`ShardPublish`]) carries inline bytes only: a
/// [`ShardPayload::Reference`] can never satisfy Q2 durability, because a reference
/// points at bytes in a Q1 WAL that the consensus members do not share — if that WAL
/// is lost or fenced, the committed publication would reference data that no member
/// can read back. The Q2 payload bound therefore applies to the publish payload
/// itself.
pub const MAX_INLINE_PAYLOAD_BYTES: usize = 64 * 1024;

/// One shard-group member with its peer endpoint.
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ShardNode {
    pub id: ShardNodeId,
    pub address: String,
}

/// The replicated command of one shard group — the C-03 adapter request.
///
/// The request is the unit the state machine applies: it names the request, the partition
/// it belongs to, the payload to apply and the metadata ownership epoch the submitter
/// observed.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ShardRequest {
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

/// One replicated command of a shard group.
///
/// The state machine applies exactly one variant per committed entry:
///
/// * [`ShardCommand::Request`] is the C-03 adapter command with its original,
///   unchanged idempotency semantics.
/// * [`ShardCommand::Publish`] is the Q2 PUBLISH: an idempotent, scoped producer
///   publication whose committed result is a per-partition **logical offset**
///   allocated at apply time. A physical Raft index never becomes that offset.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum ShardCommand {
    /// The C-03 adapter request.
    Request(ShardRequest),
    /// A Q2 idempotent publication.
    Publish(ShardPublish),
    /// Partition-wide consumer generation fence (the claim has no group selector).
    GroupClaim {
        #[serde(with = "partition_ref_serde")]
        partition: rovqet_core::PartitionRef,
        incarnation: u128,
        metadata_epoch: u64,
        requested_generation: u64,
    },
    /// Commit a broker-validated delivery ACK. Before proposing, the broker MUST
    /// validate the volatile live delivery lease, authorized principal and attempt.
    /// Apply validates committed message identity, incarnation, epoch, generation
    /// and retry identity; message existence alone is NOT proof of delivery.
    GroupAck {
        #[serde(with = "partition_ref_serde")]
        partition: rovqet_core::PartitionRef,
        incarnation: u128,
        metadata_epoch: u64,
        generation: u64,
        group_id: u64,
        offset: u64,
        message_id: u128,
        principal: u128,
        lease_id: u128,
        attempt: u32,
    },
}

impl ShardCommand {
    /// The metadata ownership epoch this command carries, for both variants.
    pub fn metadata_epoch(&self) -> u64 {
        match self {
            ShardCommand::Request(request) => request.metadata_epoch,
            ShardCommand::Publish(publish) => publish.metadata_epoch,
            ShardCommand::GroupClaim { metadata_epoch, .. }
            | ShardCommand::GroupAck { metadata_epoch, .. } => *metadata_epoch,
        }
    }
}

/// A Q2 PUBLISH command: one idempotent producer publication of one partition.
///
/// The publication is scoped by `(principal, producer_id)` and deduplicated by
/// `(producer_epoch, sequence)`, mirroring the Q1 producer rules: a replay of an
/// already-committed token returns the original logical offset instead of writing a
/// second message. `payload_digest` is carried in the command so that every member
/// validates and hashes the same bytes deterministically; the state machine refuses
/// the command when it does not equal `sha256(payload)`.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ShardPublish {
    /// The partition this publication applies to.
    #[serde(with = "partition_ref_serde")]
    pub partition: rovqet_core::PartitionRef,
    /// Durable class of the publication. Q2 requires class `2`; any other class is
    /// rejected with [`PublishErrorCode::WrongClass`].
    pub durable_class: u8,
    /// Data incarnation the publication belongs to; a partition locks onto the
    /// incarnation of its first committed publication.
    pub incarnation: u128,
    /// The metadata ownership epoch the submitter believed to be current.
    pub metadata_epoch: u64,
    /// Principal (tenant-scoped identity) the producer acts for.
    pub principal: u128,
    /// Producer identity inside the principal's scope.
    pub producer_id: u128,
    /// Producer epoch: a newer epoch fences an older one.
    pub producer_epoch: u64,
    /// Producer sequence inside `producer_epoch`; `0` starts a new epoch.
    pub sequence: u64,
    /// Identity of the message this publication creates. Replays must repeat it
    /// unchanged; a different message under the same token is a conflict.
    pub message_id: u128,
    /// The inline payload bytes themselves, bounded by [`MAX_INLINE_PAYLOAD_BYTES`].
    /// A Q2 publication never carries a [`ShardPayload::Reference`].
    pub payload: Vec<u8>,
    /// `sha256(payload)`, validated at apply.
    pub payload_digest: [u8; 32],
}

/// Deterministic refusal codes of a committed Q2 PUBLISH.
///
/// The variant names are part of the wire and storage contract and stay stable.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum PublishErrorCode {
    /// The publication is not of the required durable class.
    WrongClass,
    /// The publication observed a stale ownership view (reserved for ownership
    /// fencing at the submitter side of the pipeline).
    StaleOwnership,
    /// A newer producer epoch has already committed for this producer.
    ProducerFenced,
    /// The sequence skips ahead of the producer's next expected sequence.
    SequenceGap,
    /// The same token already committed with different content.
    SequenceConflict,
    /// The inline payload exceeds [`MAX_INLINE_PAYLOAD_BYTES`].
    PayloadTooLarge,
    /// The carried digest does not equal `sha256(payload)`.
    PayloadDigestMismatch,
    /// A documented prototype bound would be exceeded by this admission.
    ResourceExhausted,
    /// The metadata ownership epoch is older than the partition's committed epoch.
    StaleEpoch,
    /// The partition already committed publications under a different incarnation.
    IncarnationMismatch,
}

impl PublishErrorCode {
    /// The stable name used in reasons and on the wire.
    pub fn as_str(self) -> &'static str {
        match self {
            PublishErrorCode::WrongClass => "WrongClass",
            PublishErrorCode::StaleOwnership => "StaleOwnership",
            PublishErrorCode::ProducerFenced => "ProducerFenced",
            PublishErrorCode::SequenceGap => "SequenceGap",
            PublishErrorCode::SequenceConflict => "SequenceConflict",
            PublishErrorCode::PayloadTooLarge => "PayloadTooLarge",
            PublishErrorCode::PayloadDigestMismatch => "PayloadDigestMismatch",
            PublishErrorCode::ResourceExhausted => "ResourceExhausted",
            PublishErrorCode::StaleEpoch => "StaleEpoch",
            PublishErrorCode::IncarnationMismatch => "IncarnationMismatch",
        }
    }
}

impl std::fmt::Display for PublishErrorCode {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// What a replicated command carries.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum ShardPayload {
    /// The payload bytes themselves, bounded by [`MAX_INLINE_PAYLOAD_BYTES`].
    Inline(Vec<u8>),
    /// A durable data reference: the bytes live in the Q1 WAL of `incarnation` at
    /// `offset`, not in the Raft entry.
    ///
    /// A reference belongs to the C-03 adapter command only. It is NEVER used by a
    /// [`ShardCommand::Publish`]: a reference cannot satisfy Q2 durability, because
    /// the referenced bytes live in a Q1 WAL that the consensus members do not share,
    /// so a committed publication could outlive the data it points at. A Q2
    /// publication always carries its bytes inline in [`ShardPublish::payload`].
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
    /// A partition-wide consumer fence was installed.
    GroupClaimed { generation: u64 },
    /// Saved committed ACK result. Cursor is the next incomplete logical offset
    /// at the original commit, not the cursor at retry time.
    GroupAcked {
        generation: u64,
        group_id: u64,
        offset: u64,
        cursor: u64,
    },
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
    /// A committed Q2 PUBLISH created a new message at `logical_offset`, or
    /// replayed an already-committed token.
    ///
    /// `logical_offset` is the per-partition logical offset allocated at committed
    /// apply — never the Raft `index`. `duplicate: true` means the token was already
    /// committed: the original offset is reported, no new message was created and no
    /// new offset was consumed.
    Published {
        index: u64,
        #[serde(with = "partition_ref_serde")]
        partition: rovqet_core::PartitionRef,
        logical_offset: u64,
        message_id: u128,
        duplicate: bool,
    },
    /// A committed Q2 PUBLISH was refused deterministically; no offset was consumed
    /// and no message was created.
    PublishRejected {
        index: u64,
        code: PublishErrorCode,
        reason: String,
    },
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
        let command = ShardCommand::Request(ShardRequest {
            request_id: 42,
            partition: rovqet_core::PartitionRef {
                tenant_id: 1,
                topic_id: 2,
                partition_id: 3,
            },
            payload: ShardPayload::Inline(vec![9, 9]),
            metadata_epoch: 5,
        });
        let bytes = serde_json::to_vec(&command).expect("serialize");
        let decoded: ShardCommand = serde_json::from_slice(&bytes).expect("deserialize");
        assert_eq!(decoded, command);
        assert_eq!(decoded.metadata_epoch(), 5);
    }

    #[test]
    fn a_publish_command_round_trips_through_serde_with_stable_error_names() {
        let publish = ShardPublish {
            partition: rovqet_core::PartitionRef {
                tenant_id: 1,
                topic_id: 2,
                partition_id: 3,
            },
            durable_class: 2,
            incarnation: 11,
            metadata_epoch: 5,
            principal: 1,
            producer_id: 2,
            producer_epoch: 3,
            sequence: 4,
            message_id: 5,
            payload: vec![1, 2, 3],
            payload_digest: [7u8; 32],
        };
        let command = ShardCommand::Publish(publish.clone());
        let bytes = serde_json::to_vec(&command).expect("serialize");
        let decoded: ShardCommand = serde_json::from_slice(&bytes).expect("deserialize");
        assert_eq!(decoded, command);
        assert_eq!(decoded.metadata_epoch(), 5);
        assert_eq!(
            serde_json::to_string(&PublishErrorCode::IncarnationMismatch).expect("encode code"),
            "\"IncarnationMismatch\"",
            "error code names are stable on the wire"
        );
        assert_eq!(
            PublishErrorCode::SequenceGap.to_string(),
            "SequenceGap",
            "Display follows the stable name"
        );
    }
}
