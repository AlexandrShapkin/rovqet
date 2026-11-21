//! Deterministic shard state machine of one Q2 group, with durable snapshots.
//!
//! One Raft group hosts multiple logical Q2 partitions. The machine applies only
//! committed entries, in log order, and returns exactly one response per entry — blank
//! and membership entries included — so the applied result is known before any
//! acknowledgement leaves the node. A physical Raft index never becomes a public logical
//! offset: the index is only the position in the replicated history, while each partition
//! keeps its own `applied_count` and its own digest chain.
//!
//! Every partition carries a [`PartitionState::chain_digest`]:
//!
//! ```text
//! sha256(previous_chain_digest || raft_index || request_id
//!         || tenant || topic || partition || payload_digest)
//! ```
//!
//! where `payload_digest` is `sha256` of the inline bytes, or of `(incarnation, offset)`
//! for a `Reference` payload. The chain proves committed-prefix continuity across
//! restarts, log compaction and snapshot installation: the same prefix always rebuilds
//! the same digests, and a divergent state can never reproduce them.
//!
//! Durability follows the same contract as the metadata machine: the applied state is
//! written to `state.json` through `temp + fsync + rename + directory fsync` *before*
//! `apply` returns, and a snapshot is published as `snapshot.json` (plus its
//! `snapshot-meta.json`) under a temporary name that is renamed over the previous image,
//! so an interrupted publication always leaves the previous snapshot intact. A snapshot
//! is validated — version, checksum, group identity, included index and internal
//! consistency — before it is activated or served; any deviation fails closed with a
//! [`StorageError`] and leaves the previous state untouched.
//!
//! Determinism is absolute: no clocks, no randomness, no leader-local counters appear in
//! the applied result or in the persisted state.
#![forbid(unsafe_code)]

use crate::types::{
    ShardCommand, ShardCommandResponse, ShardGroupIdentity, ShardNode, ShardNodeId, ShardPayload,
    ShardRaftTypeConfig,
};
use openraft::storage::{RaftSnapshotBuilder, RaftStateMachine};
use openraft::{
    Entry, EntryPayload, ErrorSubject, ErrorVerb, LogId, Snapshot, SnapshotMeta, StorageError,
    StorageIOError, StoredMembership,
};
use std::collections::BTreeMap;
use std::io::Cursor;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use tokio::io::AsyncReadExt;

/// Durable file that carries the applied state of the machine.
const STATE_FILE: &str = "state.json";
/// Durable image of the most recently published snapshot document.
const SNAPSHOT_FILE: &str = "snapshot.json";
/// Durable metadata of the most recently published snapshot.
const SNAPSHOT_META_FILE: &str = "snapshot-meta.json";

/// Format version of [`ShardStateFile`].
const STATE_VERSION: u16 = 1;
/// Format version of the snapshot document. An unsupported version is refused.
pub const SNAPSHOT_VERSION: u16 = 1;
/// Largest number of partitions one snapshot document may carry. The bound keeps a
/// snapshot a bounded object no matter how the log grows.
pub const MAX_SNAPSHOT_PARTITIONS: usize = 65_536;
/// Largest snapshot image accepted while receiving or serving one.
pub const MAX_SNAPSHOT_BYTES: usize = 64 * 1024 * 1024;

/// Chain origin of a partition that has not been applied yet.
const ZERO_DIGEST: [u8; 32] = [0u8; 32];

/// Key of one logical Q2 partition inside a shard group.
///
/// The key is a JSON map key inside [`ShardState`], so it carries an explicit string
/// representation, `"{tenant_id}:{topic_id}:{partition_id}"`. A structured encoding is
/// not usable there — `serde_json` only accepts string map keys — and the string form is
/// also the snapshot's representation inside its `(key, state)` pairs. Encoding and
/// decoding are strict: a value with any other shape is refused, never defaulted.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PartitionKey {
    /// Tenant the partition belongs to.
    pub tenant_id: u64,
    /// Topic the partition belongs to.
    pub topic_id: u64,
    /// Partition number inside the topic.
    pub partition_id: u32,
}

impl PartitionKey {
    /// Parses the canonical `"{tenant_id}:{topic_id}:{partition_id}"` string form.
    ///
    /// Exactly three `':'`-separated, non-empty, in-range decimal parts are accepted; any
    /// other shape is an error naming the offending value.
    fn parse(text: &str) -> Result<Self, String> {
        let parts: Vec<&str> = text.split(':').collect();
        if parts.len() != 3 {
            return Err(format!(
                "partition key {text:?} must have exactly three ':'-separated parts, found {}",
                parts.len()
            ));
        }
        Ok(Self {
            tenant_id: parse_part(text, "tenant_id", parts[0])?,
            topic_id: parse_part(text, "topic_id", parts[1])?,
            partition_id: parse_part(text, "partition_id", parts[2])?,
        })
    }
}

/// Parses one unsigned decimal part of a [`PartitionKey`] string, refusing an empty or
/// out-of-range part instead of defaulting it.
fn parse_part<T>(text: &str, name: &str, part: &str) -> Result<T, String>
where
    T: std::str::FromStr,
    T::Err: std::fmt::Display,
{
    if part.is_empty() {
        return Err(format!("partition key {text:?} has an empty {name}"));
    }
    part.parse::<T>()
        .map_err(|error| format!("partition key {text:?} has an invalid {name} {part:?}: {error}"))
}

impl std::fmt::Display for PartitionKey {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "{}:{}:{}",
            self.tenant_id, self.topic_id, self.partition_id
        )
    }
}

impl serde::Serialize for PartitionKey {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> serde::Deserialize<'de> for PartitionKey {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct PartitionKeyVisitor;

        impl serde::de::Visitor<'_> for PartitionKeyVisitor {
            type Value = PartitionKey;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter
                    .write_str("a string of the form \"{tenant_id}:{topic_id}:{partition_id}\"")
            }

            fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                PartitionKey::parse(value).map_err(E::custom)
            }
        }

        deserializer.deserialize_str(PartitionKeyVisitor)
    }
}

impl From<&rovqet_core::PartitionRef> for PartitionKey {
    fn from(partition: &rovqet_core::PartitionRef) -> Self {
        Self {
            tenant_id: partition.tenant_id,
            topic_id: partition.topic_id,
            partition_id: partition.partition_id,
        }
    }
}

impl From<PartitionKey> for rovqet_core::PartitionRef {
    fn from(key: PartitionKey) -> Self {
        Self {
            tenant_id: key.tenant_id,
            topic_id: key.topic_id,
            partition_id: key.partition_id,
        }
    }
}

/// Committed state of one logical partition.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct PartitionState {
    /// Number of committed commands this partition has applied. It never decreases and
    /// a rejected or duplicate command never increments it.
    pub applied_count: u64,
    /// Request id of the most recently applied command, used for idempotent replay.
    pub last_request_id: u128,
    /// Highest metadata ownership epoch seen by an applied command of this partition.
    pub last_metadata_epoch: u64,
    /// Digest chain of this partition, see the module documentation.
    pub chain_digest: [u8; 32],
}

/// Committed, deterministic state of one Q2 shard group.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ShardState {
    /// Identity of the group this state belongs to. A state of another group is never
    /// accepted, neither from disk nor from a snapshot.
    pub identity: ShardGroupIdentity,
    /// Per-partition committed state, ordered by [`PartitionKey`].
    pub partitions: BTreeMap<PartitionKey, PartitionState>,
    /// Last applied log id, `None` before the first entry is applied.
    pub last_applied: Option<LogId<ShardNodeId>>,
    /// Stored membership as of the applied prefix.
    pub membership: Option<StoredMembership<ShardNodeId, ShardNode>>,
    /// Monotonic revision of the committed state. It advances only when a committed
    /// command actually mutates a partition.
    pub revision: u64,
}

impl ShardState {
    /// The initial state of a group: no partitions, no applied entry, revision zero.
    pub fn new(identity: ShardGroupIdentity) -> Self {
        Self {
            identity,
            partitions: BTreeMap::new(),
            last_applied: None,
            membership: None,
            revision: 0,
        }
    }

    /// The committed state of one partition, if it has been applied at least once.
    pub fn partition(&self, key: &PartitionKey) -> Option<&PartitionState> {
        self.partitions.get(key)
    }

    /// Applies one committed command deterministically.
    ///
    /// The outcome is a pure function of the committed state and the command: the same
    /// prefix always produces the same responses and the same resulting state. A
    /// rejection is part of the replicated history — it consumes no partition state but
    /// the caller still advances `last_applied` to the entry's own log id.
    pub fn apply_command(
        &mut self,
        raft_index: u64,
        command: &ShardCommand,
    ) -> ShardCommandResponse {
        // Structural bound: the type already enforces it when a payload is built, but a
        // replicated command may have been produced by an older or foreign encoder.
        if let Err(error) = command.payload.validate() {
            return rejected(&error.to_string());
        }

        let key = PartitionKey::from(&command.partition);

        if let Some(existing) = self.partitions.get(&key) {
            // Idempotent replay: the same request for the same partition reports the
            // already committed count instead of applying twice.
            if existing.last_request_id == command.request_id {
                return ShardCommandResponse::Applied {
                    index: raft_index,
                    request_id: command.request_id,
                    partition: command.partition.clone(),
                    applied_count: existing.applied_count,
                };
            }
            // Ownership epochs never move backwards for one partition.
            if command.metadata_epoch < existing.last_metadata_epoch {
                return rejected("stale metadata epoch");
            }
        }

        let previous = self
            .partitions
            .get(&key)
            .map(|partition| partition.chain_digest)
            .unwrap_or(ZERO_DIGEST);
        let applied_count = self
            .partitions
            .get(&key)
            .map(|partition| partition.applied_count)
            .unwrap_or(0)
            .saturating_add(1);
        let digest = chain_digest(
            previous,
            raft_index,
            command.request_id,
            &key,
            &payload_digest(&command.payload),
        );
        self.partitions.insert(
            key,
            PartitionState {
                applied_count,
                last_request_id: command.request_id,
                last_metadata_epoch: command.metadata_epoch,
                chain_digest: digest,
            },
        );
        self.revision = self.revision.saturating_add(1);

        ShardCommandResponse::Applied {
            index: raft_index,
            request_id: command.request_id,
            partition: command.partition.clone(),
            applied_count,
        }
    }
}

/// A deterministic rejection of a committed command.
fn rejected(reason: &str) -> ShardCommandResponse {
    ShardCommandResponse::Rejected {
        reason: reason.to_string(),
    }
}

/// Digest of what a command carries: the inline bytes themselves, or the durable
/// `(incarnation, offset)` reference pair for a payload that lives in the Q1 WAL.
fn payload_digest(payload: &ShardPayload) -> [u8; 32] {
    match payload {
        ShardPayload::Inline(bytes) => sha256(bytes),
        ShardPayload::Reference {
            incarnation,
            offset,
        } => {
            let mut buffer = [0u8; 24];
            buffer[..16].copy_from_slice(&incarnation.to_be_bytes());
            buffer[16..].copy_from_slice(&offset.to_be_bytes());
            sha256(&buffer)
        }
    }
}

/// One link of the per-partition digest chain. All integers are fixed-width
/// big-endian, so the encoding is unambiguous and identical on every member.
fn chain_digest(
    previous: [u8; 32],
    raft_index: u64,
    request_id: u128,
    key: &PartitionKey,
    payload_digest: &[u8; 32],
) -> [u8; 32] {
    let mut buffer = Vec::with_capacity(32 + 8 + 16 + 8 + 8 + 4 + 32);
    buffer.extend_from_slice(&previous);
    buffer.extend_from_slice(&raft_index.to_be_bytes());
    buffer.extend_from_slice(&request_id.to_be_bytes());
    buffer.extend_from_slice(&key.tenant_id.to_be_bytes());
    buffer.extend_from_slice(&key.topic_id.to_be_bytes());
    buffer.extend_from_slice(&key.partition_id.to_be_bytes());
    buffer.extend_from_slice(payload_digest);
    sha256(&buffer)
}

/// SHA-256 (FIPS 180-4).
///
/// The digest pins committed data without pulling a cryptographic dependency into the
/// consensus crate: it is a plain deterministic hash of bytes, verified against the
/// published test vectors in the unit tests below.
fn sha256(data: &[u8]) -> [u8; 32] {
    const K: [u32; 64] = [
        0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5, 0x3956c25b, 0x59f111f1, 0x923f82a4,
        0xab1c5ed5, 0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3, 0x72be5d74, 0x80deb1fe,
        0x9bdc06a7, 0xc19bf174, 0xe49b69c1, 0xefbe4786, 0x0fc19dc6, 0x240ca1cc, 0x2de92c6f,
        0x4a7484aa, 0x5cb0a9dc, 0x76f988da, 0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7,
        0xc6e00bf3, 0xd5a79147, 0x06ca6351, 0x14292967, 0x27b70a85, 0x2e1b2138, 0x4d2c6dfc,
        0x53380d13, 0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85, 0xa2bfe8a1, 0xa81a664b,
        0xc24b8b70, 0xc76c51a3, 0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070, 0x19a4c116,
        0x1e376c08, 0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a, 0x5b9cca4f, 0x682e6ff3,
        0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208, 0x90befffa, 0xa4506ceb, 0xbef9a3f7,
        0xc67178f2,
    ];
    let mut state: [u32; 8] = [
        0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a, 0x510e527f, 0x9b05688c, 0x1f83d9ab,
        0x5be0cd19,
    ];

    // Padding: 0x80, zeros to 56 mod 64, then the bit length as 64-bit big-endian.
    let mut message = data.to_vec();
    let bit_length = (data.len() as u64).wrapping_mul(8);
    message.push(0x80);
    while message.len() % 64 != 56 {
        message.push(0);
    }
    message.extend_from_slice(&bit_length.to_be_bytes());

    for block in message.chunks(64) {
        let mut schedule = [0u32; 64];
        for (index, word) in schedule.iter_mut().take(16).enumerate() {
            *word = u32::from_be_bytes([
                block[4 * index],
                block[4 * index + 1],
                block[4 * index + 2],
                block[4 * index + 3],
            ]);
        }
        for index in 16..64 {
            let previous = schedule[index - 15];
            let recent = schedule[index - 2];
            let sigma0 = previous.rotate_right(7) ^ previous.rotate_right(18) ^ (previous >> 3);
            let sigma1 = recent.rotate_right(17) ^ recent.rotate_right(19) ^ (recent >> 10);
            schedule[index] = schedule[index - 16]
                .wrapping_add(sigma0)
                .wrapping_add(schedule[index - 7])
                .wrapping_add(sigma1);
        }

        let [mut a, mut b, mut c, mut d, mut e, mut f, mut g, mut h] = state;
        for (round, word) in schedule.iter().enumerate() {
            let sigma1 = e.rotate_right(6) ^ e.rotate_right(11) ^ e.rotate_right(25);
            let choice = (e & f) ^ ((!e) & g);
            let temporary1 = h
                .wrapping_add(sigma1)
                .wrapping_add(choice)
                .wrapping_add(K[round])
                .wrapping_add(*word);
            let sigma0 = a.rotate_right(2) ^ a.rotate_right(13) ^ a.rotate_right(22);
            let majority = (a & b) ^ (a & c) ^ (b & c);
            let temporary2 = sigma0.wrapping_add(majority);

            h = g;
            g = f;
            f = e;
            e = d.wrapping_add(temporary1);
            d = c;
            c = b;
            b = a;
            a = temporary1.wrapping_add(temporary2);
        }

        state[0] = state[0].wrapping_add(a);
        state[1] = state[1].wrapping_add(b);
        state[2] = state[2].wrapping_add(c);
        state[3] = state[3].wrapping_add(d);
        state[4] = state[4].wrapping_add(e);
        state[5] = state[5].wrapping_add(f);
        state[6] = state[6].wrapping_add(g);
        state[7] = state[7].wrapping_add(h);
    }

    let mut digest = [0u8; 32];
    for (chunk, word) in digest.chunks_mut(4).zip(state.iter()) {
        chunk.copy_from_slice(&word.to_be_bytes());
    }
    digest
}

/// Which durable file a write or read belongs to, used to build the error subject.
#[derive(Debug, Clone, Copy)]
enum Target {
    State,
    Snapshot,
}

impl Target {
    fn subject(self) -> ErrorSubject<ShardNodeId> {
        match self {
            Target::State => ErrorSubject::StateMachine,
            Target::Snapshot => ErrorSubject::Snapshot(None),
        }
    }
}

fn storage_error(
    subject: ErrorSubject<ShardNodeId>,
    verb: ErrorVerb,
    error: std::io::Error,
) -> StorageError<ShardNodeId> {
    StorageError::IO {
        source: StorageIOError::new(subject, verb, openraft::AnyError::new(&error)),
    }
}

fn invalid(message: impl Into<String>) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::InvalidData, message.into())
}

/// Serializes any snapshot or state document, failing closed on an encoding error.
fn to_json<T: serde::Serialize>(
    value: &T,
    target: Target,
    verb: ErrorVerb,
) -> Result<Vec<u8>, StorageError<ShardNodeId>> {
    serde_json::to_vec(value).map_err(|error| {
        storage_error(
            target.subject(),
            verb,
            invalid(format!("shard state encoding failed: {error}")),
        )
    })
}

/// Atomically replaces one durable file: `temp + fsync + rename + directory fsync`.
///
/// An interrupted publication therefore always leaves the previous file in place.
fn write_atomic(
    path: &Path,
    bytes: &[u8],
    target: Target,
) -> Result<(), StorageError<ShardNodeId>> {
    use std::io::Write;
    let mut name = path
        .file_name()
        .map(|name| name.to_string_lossy().to_string())
        .unwrap_or_default();
    name.push_str(".tmp");
    let temporary = path.with_file_name(name);
    {
        let mut handle = std::fs::File::create(&temporary)
            .map_err(|error| storage_error(target.subject(), ErrorVerb::Write, error))?;
        handle
            .write_all(bytes)
            .and_then(|()| handle.sync_all())
            .map_err(|error| storage_error(target.subject(), ErrorVerb::Write, error))?;
    }
    std::fs::rename(&temporary, path)
        .map_err(|error| storage_error(target.subject(), ErrorVerb::Write, error))?;
    let directory = path.parent().unwrap_or_else(|| Path::new("."));
    std::fs::File::open(directory)
        .and_then(|directory| directory.sync_all())
        .map_err(|error| storage_error(target.subject(), ErrorVerb::Write, error))?;
    Ok(())
}

/// Persisted container of the applied state.
#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct ShardStateFile {
    version: u16,
    state: ShardState,
    /// `sha256` of the JSON encoding of `state` alone.
    checksum: [u8; 32],
}

fn state_checksum(state: &ShardState) -> Result<[u8; 32], StorageError<ShardNodeId>> {
    let bytes = to_json(state, Target::State, ErrorVerb::Write)?;
    Ok(sha256(&bytes))
}

fn encode_state(state: &ShardState) -> Result<Vec<u8>, StorageError<ShardNodeId>> {
    let file = ShardStateFile {
        version: STATE_VERSION,
        state: state.clone(),
        checksum: state_checksum(state)?,
    };
    to_json(&file, Target::State, ErrorVerb::Write)
}

fn decode_state(bytes: &[u8]) -> Result<ShardState, String> {
    let file: ShardStateFile =
        serde_json::from_slice(bytes).map_err(|error| format!("shard state decode: {error}"))?;
    if file.version != STATE_VERSION {
        return Err(format!("unsupported shard state version {}", file.version));
    }
    let expected = serde_json::to_vec(&file.state)
        .map(|bytes| sha256(&bytes))
        .map_err(|error| format!("shard state re-encoding failed: {error}"))?;
    if expected != file.checksum {
        return Err("shard state checksum mismatch".to_string());
    }
    Ok(file.state)
}

/// The versioned snapshot document, encoded as JSON inside the snapshot cursor.
///
/// The `checksum` covers the document *without* the checksum field, so any mutation of
/// a published or received image is detected before the image is trusted.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ShardSnapshot {
    /// Document format version, [`SNAPSHOT_VERSION`].
    pub version: u16,
    /// Group the snapshot belongs to.
    pub identity: ShardGroupIdentity,
    /// Raft index included in this snapshot; must match the snapshot metadata.
    pub last_included_index: u64,
    /// Raft term of `last_included_index`; must match the snapshot metadata.
    pub last_included_term: u64,
    /// Membership as of the included index.
    pub membership: Option<StoredMembership<ShardNodeId, ShardNode>>,
    /// Monotonic revision of the snapshotted state.
    pub revision: u64,
    /// Partition states, ordered by key, bounded by [`MAX_SNAPSHOT_PARTITIONS`].
    pub partitions: Vec<(PartitionKey, PartitionState)>,
    /// `sha256` of the document without this field.
    pub checksum: [u8; 32],
}

/// The snapshot document without its checksum field; the checksum is computed over
/// exactly this encoding.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct ShardSnapshotBody {
    version: u16,
    identity: ShardGroupIdentity,
    last_included_index: u64,
    last_included_term: u64,
    membership: Option<StoredMembership<ShardNodeId, ShardNode>>,
    revision: u64,
    partitions: Vec<(PartitionKey, PartitionState)>,
}

impl ShardSnapshot {
    fn body(&self) -> ShardSnapshotBody {
        ShardSnapshotBody {
            version: self.version,
            identity: self.identity,
            last_included_index: self.last_included_index,
            last_included_term: self.last_included_term,
            membership: self.membership.clone(),
            revision: self.revision,
            partitions: self.partitions.clone(),
        }
    }

    /// Seals a body into a document whose checksum covers the body alone.
    fn seal(body: ShardSnapshotBody) -> Result<Self, String> {
        let bytes = serde_json::to_vec(&body)
            .map_err(|error| format!("shard snapshot encoding failed: {error}"))?;
        let checksum = sha256(&bytes);
        Ok(Self {
            version: body.version,
            identity: body.identity,
            last_included_index: body.last_included_index,
            last_included_term: body.last_included_term,
            membership: body.membership,
            revision: body.revision,
            partitions: body.partitions,
            checksum,
        })
    }

    /// Builds the document of one applied state.
    fn from_state(state: &ShardState) -> Result<Self, StorageError<ShardNodeId>> {
        if state.partitions.len() > MAX_SNAPSHOT_PARTITIONS {
            return Err(storage_error(
                Target::Snapshot.subject(),
                ErrorVerb::Write,
                invalid("shard snapshot exceeds the partition bound"),
            ));
        }
        let body = ShardSnapshotBody {
            version: SNAPSHOT_VERSION,
            identity: state.identity,
            last_included_index: state.last_applied.map(|log_id| log_id.index).unwrap_or(0),
            last_included_term: state
                .last_applied
                .map(|log_id| log_id.leader_id.term)
                .unwrap_or(0),
            membership: state.membership.clone(),
            revision: state.revision,
            partitions: state
                .partitions
                .iter()
                .map(|(key, partition)| (*key, partition.clone()))
                .collect(),
        };
        Self::seal(body).map_err(|error| {
            storage_error(Target::Snapshot.subject(), ErrorVerb::Write, invalid(error))
        })
    }

    /// Decodes and validates the envelope of a snapshot image: size bound, supported
    /// version and checksum. Corruption fails closed.
    fn decode(bytes: &[u8]) -> Result<Self, String> {
        if bytes.len() > MAX_SNAPSHOT_BYTES {
            return Err("shard snapshot exceeds the size bound".to_string());
        }
        let document: Self = serde_json::from_slice(bytes)
            .map_err(|error| format!("shard snapshot decode: {error}"))?;
        if document.version != SNAPSHOT_VERSION {
            return Err(format!(
                "unsupported shard snapshot version {}",
                document.version
            ));
        }
        if document.partitions.len() > MAX_SNAPSHOT_PARTITIONS {
            return Err("shard snapshot exceeds the partition bound".to_string());
        }
        let expected = serde_json::to_vec(&document.body())
            .map(|bytes| sha256(&bytes))
            .map_err(|error| format!("shard snapshot re-encoding failed: {error}"))?;
        if expected != document.checksum {
            return Err("shard snapshot checksum mismatch".to_string());
        }
        Ok(document)
    }

    /// Validates a decoded snapshot against this group before it may be activated:
    /// foreign identity, included index versus the snapshot metadata, and internal
    /// consistency of the carried state. Any deviation fails closed.
    fn validate_for(
        &self,
        identity: ShardGroupIdentity,
        meta: &SnapshotMeta<ShardNodeId, ShardNode>,
    ) -> Result<(), String> {
        if self.identity != identity {
            return Err(if self.identity.cluster_id != identity.cluster_id {
                "shard snapshot cluster identity mismatch".to_string()
            } else {
                "shard snapshot shard identity mismatch".to_string()
            });
        }
        let expected_index = meta.last_log_id.map(|log_id| log_id.index).unwrap_or(0);
        let expected_term = meta
            .last_log_id
            .map(|log_id| log_id.leader_id.term)
            .unwrap_or(0);
        if self.last_included_index != expected_index || self.last_included_term != expected_term {
            return Err(
                "shard snapshot included index does not match the snapshot metadata".to_string(),
            );
        }
        let mut previous: Option<PartitionKey> = None;
        for (key, partition) in &self.partitions {
            if partition.applied_count == 0 {
                return Err(
                    "shard snapshot carries a partition without an applied command".to_string(),
                );
            }
            if let Some(previous) = previous {
                if *key <= previous {
                    return Err(
                        "shard snapshot carries duplicate or unordered partitions".to_string()
                    );
                }
            }
            previous = Some(*key);
        }
        if let Some(membership) = self.membership.as_ref() {
            if let Some(membership_log_id) = membership.log_id() {
                if membership_log_id.index > self.last_included_index {
                    return Err(
                        "shard snapshot membership log id exceeds the included index".to_string(),
                    );
                }
            }
        }
        Ok(())
    }

    /// Rebuilds the full state this document describes, for `last_applied` taken from
    /// the validated snapshot metadata.
    fn to_state(&self, last_applied: Option<LogId<ShardNodeId>>) -> ShardState {
        ShardState {
            identity: self.identity,
            partitions: self.partitions.iter().cloned().collect(),
            last_applied,
            membership: self.membership.clone(),
            revision: self.revision,
        }
    }
}

/// The state machine of one Q2 shard node.
pub struct ShardStateMachine {
    directory: PathBuf,
    state: ShardState,
    /// Handle the node (stage 3) uses to publish the applied state.
    shared: Arc<Mutex<ShardState>>,
}

impl ShardStateMachine {
    /// Opens the state machine, recovering the durable state of a previous run.
    ///
    /// Corruption and a foreign group identity both fail closed: the machine never
    /// silently resets a persisted state.
    pub fn open(
        directory: impl AsRef<Path>,
        identity: ShardGroupIdentity,
    ) -> Result<Self, StorageError<ShardNodeId>> {
        let directory = directory.as_ref().to_path_buf();
        std::fs::create_dir_all(&directory)
            .map_err(|error| storage_error(Target::State.subject(), ErrorVerb::Write, error))?;
        let state = match std::fs::read(directory.join(STATE_FILE)) {
            Ok(bytes) => {
                let state = decode_state(&bytes).map_err(|error| {
                    storage_error(Target::State.subject(), ErrorVerb::Read, invalid(error))
                })?;
                if state.identity != identity {
                    return Err(storage_error(
                        Target::State.subject(),
                        ErrorVerb::Read,
                        invalid("shard state identity mismatch"),
                    ));
                }
                state
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => ShardState::new(identity),
            Err(error) => {
                return Err(storage_error(
                    Target::State.subject(),
                    ErrorVerb::Read,
                    error,
                ));
            }
        };
        let shared = Arc::new(Mutex::new(state.clone()));
        Ok(Self {
            directory,
            state,
            shared,
        })
    }

    /// Opens the state machine and returns a handle to the shared applied state.
    pub fn open_shared(
        directory: impl AsRef<Path>,
        identity: ShardGroupIdentity,
    ) -> Result<(Self, Arc<Mutex<ShardState>>), StorageError<ShardNodeId>> {
        let machine = Self::open(directory, identity)?;
        let shared = machine.shared.clone();
        Ok((machine, shared))
    }

    /// The shared handle of the applied state, for the node to publish.
    pub fn shared_state(&self) -> Arc<Mutex<ShardState>> {
        self.shared.clone()
    }

    /// The currently applied state.
    pub fn state(&self) -> &ShardState {
        &self.state
    }

    /// Writes the applied state durably: `temp + fsync + rename + directory fsync`.
    fn persist(&self) -> Result<(), StorageError<ShardNodeId>> {
        let bytes = encode_state(&self.state)?;
        write_atomic(&self.directory.join(STATE_FILE), &bytes, Target::State)
    }

    /// Publishes the shared view of the applied state.
    fn publish_shared(&self) {
        if let Ok(mut shared) = self.shared.lock() {
            *shared = self.state.clone();
        }
    }
}

/// Publishes a snapshot durably: the image first, then the metadata that makes it
/// discoverable. An interrupted publication leaves the previous snapshot in place.
fn publish_snapshot(
    directory: &Path,
    meta: &SnapshotMeta<ShardNodeId, ShardNode>,
    bytes: &[u8],
) -> Result<(), StorageError<ShardNodeId>> {
    write_atomic(&directory.join(SNAPSHOT_FILE), bytes, Target::Snapshot)?;
    let meta_bytes = to_json(meta, Target::Snapshot, ErrorVerb::Write)?;
    write_atomic(
        &directory.join(SNAPSHOT_META_FILE),
        &meta_bytes,
        Target::Snapshot,
    )
}

/// The last durably published snapshot of this node, if any.
///
/// A published image that does not validate is never served.
fn read_published_snapshot(
    directory: &Path,
    identity: ShardGroupIdentity,
) -> Result<Option<Snapshot<ShardRaftTypeConfig>>, StorageError<ShardNodeId>> {
    let meta_bytes = match std::fs::read(directory.join(SNAPSHOT_META_FILE)) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => {
            return Err(storage_error(
                Target::Snapshot.subject(),
                ErrorVerb::Read,
                error,
            ));
        }
    };
    let meta: SnapshotMeta<ShardNodeId, ShardNode> =
        serde_json::from_slice(&meta_bytes).map_err(|error| {
            storage_error(
                Target::Snapshot.subject(),
                ErrorVerb::Read,
                invalid(format!("shard snapshot metadata decode: {error}")),
            )
        })?;
    let bytes = std::fs::read(directory.join(SNAPSHOT_FILE))
        .map_err(|error| storage_error(Target::Snapshot.subject(), ErrorVerb::Read, error))?;
    let decode_and_validate = || -> Result<ShardSnapshot, String> {
        let document = ShardSnapshot::decode(&bytes)?;
        document.validate_for(identity, &meta)?;
        Ok(document)
    };
    decode_and_validate().map_err(|error| {
        storage_error(Target::Snapshot.subject(), ErrorVerb::Read, invalid(error))
    })?;
    Ok(Some(Snapshot {
        meta,
        snapshot: Box::new(Cursor::new(bytes)),
    }))
}

impl RaftStateMachine<ShardRaftTypeConfig> for ShardStateMachine {
    type SnapshotBuilder = ShardSnapshotBuilder;

    async fn applied_state(
        &mut self,
    ) -> Result<
        (
            Option<LogId<ShardNodeId>>,
            StoredMembership<ShardNodeId, ShardNode>,
        ),
        StorageError<ShardNodeId>,
    > {
        Ok((
            self.state.last_applied,
            self.state.membership.clone().unwrap_or_default(),
        ))
    }

    async fn apply<I>(
        &mut self,
        entries: I,
    ) -> Result<Vec<ShardCommandResponse>, StorageError<ShardNodeId>>
    where
        I: IntoIterator<Item = Entry<ShardRaftTypeConfig>>,
        I::IntoIter: openraft::OptionalSend,
    {
        let mut responses = Vec::new();
        for entry in entries {
            let index = entry.log_id.index;
            // Only committed entries reach this method, and the applied result is known
            // here before any acknowledgement is produced upstream.
            self.state.last_applied = Some(entry.log_id);
            // Exactly one response per applied entry: OpenRaft asserts that the number
            // of results equals the number of entries it handed over.
            match entry.payload {
                EntryPayload::Blank => responses.push(ShardCommandResponse::Noop { index }),
                EntryPayload::Membership(membership) => {
                    self.state.membership =
                        Some(StoredMembership::new(Some(entry.log_id), membership));
                    responses.push(ShardCommandResponse::Noop { index });
                }
                EntryPayload::Normal(command) => {
                    let response = self.state.apply_command(index, &command);
                    responses.push(response);
                }
            }
        }
        // The batch is durable before any of its responses can be observed.
        self.persist()?;
        self.publish_shared();
        Ok(responses)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        ShardSnapshotBuilder {
            directory: self.directory.clone(),
            state: self.state.clone(),
        }
    }

    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<Box<Cursor<Vec<u8>>>, StorageError<ShardNodeId>> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<ShardNodeId, ShardNode>,
        snapshot: Box<Cursor<Vec<u8>>>,
    ) -> Result<(), StorageError<ShardNodeId>> {
        let mut bytes = Vec::new();
        let mut reader = snapshot;
        // The received image is written through a cursor that is left at the end of the
        // last chunk; the whole image must be read from the start.
        tokio::io::AsyncSeekExt::seek(&mut reader, std::io::SeekFrom::Start(0))
            .await
            .map_err(|error| storage_error(Target::State.subject(), ErrorVerb::Seek, error))?;
        reader
            .read_to_end(&mut bytes)
            .await
            .map_err(|error| storage_error(Target::State.subject(), ErrorVerb::Read, error))?;

        // Everything is validated before a single field of the live state is touched.
        let document = ShardSnapshot::decode(&bytes).map_err(|error| {
            storage_error(Target::State.subject(), ErrorVerb::Read, invalid(error))
        })?;
        document
            .validate_for(self.state.identity, meta)
            .map_err(|error| {
                storage_error(Target::State.subject(), ErrorVerb::Read, invalid(error))
            })?;

        self.state = document.to_state(meta.last_log_id);
        self.persist()?;
        self.publish_shared();
        // Publish the installed image too, so a restart of this node can serve it onwards.
        publish_snapshot(&self.directory, meta, &bytes)?;
        Ok(())
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<ShardRaftTypeConfig>>, StorageError<ShardNodeId>> {
        read_published_snapshot(&self.directory, self.state.identity)
    }
}

/// Builds a durable snapshot image of the applied shard state.
pub struct ShardSnapshotBuilder {
    directory: PathBuf,
    state: ShardState,
}

impl RaftSnapshotBuilder<ShardRaftTypeConfig> for ShardSnapshotBuilder {
    async fn build_snapshot(
        &mut self,
    ) -> Result<Snapshot<ShardRaftTypeConfig>, StorageError<ShardNodeId>> {
        let document = ShardSnapshot::from_state(&self.state)?;
        let bytes = to_json(&document, Target::Snapshot, ErrorVerb::Write)?;
        let meta = SnapshotMeta {
            last_log_id: self.state.last_applied,
            last_membership: self.state.membership.clone().unwrap_or_default(),
            snapshot_id: self
                .state
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::MAX_INLINE_PAYLOAD_BYTES;
    use openraft::{LeaderId, Membership};
    use std::collections::BTreeSet;

    fn identity() -> ShardGroupIdentity {
        ShardGroupIdentity {
            cluster_id: 7,
            shard_id: 9,
        }
    }

    fn log_id(index: u64) -> LogId<ShardNodeId> {
        LogId::new(LeaderId::new(2, 1), index)
    }

    fn blank_entry(index: u64) -> Entry<ShardRaftTypeConfig> {
        Entry {
            log_id: log_id(index),
            payload: EntryPayload::Blank,
        }
    }

    fn membership_entry(index: u64) -> Entry<ShardRaftTypeConfig> {
        let membership = Membership::new(
            vec![BTreeSet::from([1u64, 2u64])],
            BTreeMap::from([
                (
                    1u64,
                    ShardNode {
                        id: 1,
                        address: "127.0.0.1:1".to_string(),
                    },
                ),
                (
                    2u64,
                    ShardNode {
                        id: 2,
                        address: "127.0.0.1:2".to_string(),
                    },
                ),
            ]),
        );
        Entry {
            log_id: log_id(index),
            payload: EntryPayload::Membership(membership),
        }
    }

    fn command(
        request_id: u128,
        tenant_id: u64,
        topic_id: u64,
        partition_id: u32,
        payload: &[u8],
        metadata_epoch: u64,
    ) -> ShardCommand {
        ShardCommand {
            request_id,
            partition: rovqet_core::PartitionRef {
                tenant_id,
                topic_id,
                partition_id,
            },
            payload: ShardPayload::Inline(payload.to_vec()),
            metadata_epoch,
        }
    }

    fn command_entry(index: u64, command: ShardCommand) -> Entry<ShardRaftTypeConfig> {
        Entry {
            log_id: log_id(index),
            payload: EntryPayload::Normal(command),
        }
    }

    fn key(tenant_id: u64, topic_id: u64, partition_id: u32) -> PartitionKey {
        PartitionKey {
            tenant_id,
            topic_id,
            partition_id,
        }
    }

    fn snapshot_bytes(snapshot: &Snapshot<ShardRaftTypeConfig>) -> Vec<u8> {
        let mut reader = (*snapshot.snapshot).clone();
        let mut bytes = Vec::new();
        std::io::Read::read_to_end(&mut reader, &mut bytes).expect("snapshot cursor");
        bytes
    }

    /// Re-encodes a document after mutating it, keeping only the mutated field's own
    /// checksum valid, so a refusal can only come from the intended validation.
    fn reseal(mut document: ShardSnapshot) -> Vec<u8> {
        let checksum = ShardSnapshot::seal(document.body())
            .expect("reseal")
            .checksum;
        document.checksum = checksum;
        serde_json::to_vec(&document).expect("encode")
    }

    async fn install_bytes(
        machine: &mut ShardStateMachine,
        meta: &SnapshotMeta<ShardNodeId, ShardNode>,
        bytes: &[u8],
    ) -> Result<(), StorageError<ShardNodeId>> {
        let mut receiving = machine.begin_receiving_snapshot().await.expect("begin");
        receiving.get_mut().extend_from_slice(bytes);
        machine.install_snapshot(meta, receiving).await
    }

    #[tokio::test]
    async fn blank_and_membership_entries_each_produce_exactly_one_noop_and_advance_the_applied_index()
     {
        let directory = tempfile::tempdir().expect("temp dir");
        let mut machine =
            ShardStateMachine::open(directory.path(), identity()).expect("open machine");

        let responses = machine
            .apply(vec![blank_entry(0), membership_entry(1), blank_entry(2)])
            .await
            .expect("apply");

        assert_eq!(
            responses,
            vec![
                ShardCommandResponse::Noop { index: 0 },
                ShardCommandResponse::Noop { index: 1 },
                ShardCommandResponse::Noop { index: 2 },
            ],
            "every non-command entry produces exactly one Noop in order"
        );
        let (last_applied, membership) = machine.applied_state().await.expect("applied state");
        assert_eq!(last_applied, Some(log_id(2)), "the applied index advanced");
        assert_eq!(
            membership,
            StoredMembership::new(
                Some(log_id(1)),
                Membership::new(
                    vec![BTreeSet::from([1u64, 2u64])],
                    BTreeMap::from([
                        (
                            1u64,
                            ShardNode {
                                id: 1,
                                address: "127.0.0.1:1".to_string()
                            }
                        ),
                        (
                            2u64,
                            ShardNode {
                                id: 2,
                                address: "127.0.0.1:2".to_string()
                            }
                        ),
                    ])
                )
            ),
            "the membership entry is stored with its own log id"
        );
        assert!(
            machine.state().partitions.is_empty(),
            "non-command entries never touch a partition"
        );
    }

    #[tokio::test]
    async fn a_normal_command_applies_deterministically_and_chains_the_partition_digest() {
        let entry = command_entry(0, command(42, 1, 2, 3, b"hello", 5));

        // Two independent machines, the same committed entry: byte-identical state.
        let first_directory = tempfile::tempdir().expect("temp dir");
        let mut first = ShardStateMachine::open(first_directory.path(), identity()).expect("open");
        let responses = first.apply(vec![entry.clone()]).await.expect("apply");

        let second_directory = tempfile::tempdir().expect("temp dir");
        let mut second =
            ShardStateMachine::open(second_directory.path(), identity()).expect("open");
        second.apply(vec![entry]).await.expect("apply");

        assert_eq!(
            responses,
            vec![ShardCommandResponse::Applied {
                index: 0,
                request_id: 42,
                partition: rovqet_core::PartitionRef {
                    tenant_id: 1,
                    topic_id: 2,
                    partition_id: 3
                },
                applied_count: 1,
            }]
        );
        assert_eq!(
            first.state(),
            second.state(),
            "the same committed prefix must produce the same state"
        );

        let partition = first.state().partition(&key(1, 2, 3)).expect("applied");
        assert_eq!(partition.applied_count, 1, "one applied command");
        assert_eq!(partition.last_request_id, 42);
        assert_eq!(partition.last_metadata_epoch, 5);
        assert_ne!(
            partition.chain_digest, ZERO_DIGEST,
            "the first command starts a real digest chain"
        );
        let expected = chain_digest(
            ZERO_DIGEST,
            0,
            42,
            &key(1, 2, 3),
            &payload_digest(&ShardPayload::Inline(b"hello".to_vec())),
        );
        assert_eq!(
            partition.chain_digest, expected,
            "the digest follows the documented chain definition"
        );
        assert_eq!(first.state().revision, 1, "one committed mutation");
        assert_eq!(first.state().last_applied, Some(log_id(0)));
    }

    #[tokio::test]
    async fn the_same_request_id_is_idempotent_and_never_double_counts() {
        let directory = tempfile::tempdir().expect("temp dir");
        let mut machine = ShardStateMachine::open(directory.path(), identity()).expect("open");

        machine
            .apply(vec![command_entry(0, command(7, 1, 2, 3, b"first", 5))])
            .await
            .expect("first apply");
        let before = machine.state().clone();

        let responses = machine
            .apply(vec![command_entry(1, command(7, 1, 2, 3, b"replayed", 5))])
            .await
            .expect("second apply");

        assert_eq!(
            responses,
            vec![ShardCommandResponse::Applied {
                index: 1,
                request_id: 7,
                partition: rovqet_core::PartitionRef {
                    tenant_id: 1,
                    topic_id: 2,
                    partition_id: 3
                },
                applied_count: 1,
            }],
            "a replay reports the already committed count"
        );
        let partition = machine.state().partition(&key(1, 2, 3)).expect("applied");
        assert_eq!(
            partition.applied_count, 1,
            "the replay must not apply twice"
        );
        assert_eq!(
            partition.chain_digest,
            before.partitions[&key(1, 2, 3)].chain_digest,
            "the replay must not extend the digest chain"
        );
        assert_eq!(machine.state().revision, before.revision, "no new revision");
        assert_eq!(
            machine.state().last_applied,
            Some(log_id(1)),
            "the replay still consumes its own raft index"
        );
    }

    #[tokio::test]
    async fn a_stale_metadata_epoch_is_rejected_but_still_advances_the_applied_index() {
        let directory = tempfile::tempdir().expect("temp dir");
        let mut machine = ShardStateMachine::open(directory.path(), identity()).expect("open");

        machine
            .apply(vec![command_entry(0, command(1, 1, 2, 3, b"current", 5))])
            .await
            .expect("first apply");
        let before = machine.state().clone();

        let responses = machine
            .apply(vec![command_entry(1, command(2, 1, 2, 3, b"stale", 4))])
            .await
            .expect("second apply");

        assert_eq!(
            responses,
            vec![ShardCommandResponse::Rejected {
                reason: "stale metadata epoch".to_string()
            }],
            "an older epoch is rejected deterministically"
        );
        assert_eq!(
            machine.state().partitions,
            before.partitions,
            "a rejected command never mutates the partition state"
        );
        assert_eq!(
            machine.state().revision,
            before.revision,
            "a rejection is not a mutation"
        );
        assert_eq!(
            machine.state().last_applied,
            Some(log_id(1)),
            "a rejection still consumes its raft index and advances last_applied"
        );
    }

    #[tokio::test]
    async fn two_partitions_in_one_batch_keep_independent_counters() {
        let directory = tempfile::tempdir().expect("temp dir");
        let mut machine = ShardStateMachine::open(directory.path(), identity()).expect("open");

        let responses = machine
            .apply(vec![
                command_entry(0, command(11, 1, 2, 3, b"alpha", 5)),
                command_entry(1, command(12, 4, 5, 6, b"beta", 1)),
            ])
            .await
            .expect("apply");

        assert_eq!(
            responses,
            vec![
                ShardCommandResponse::Applied {
                    index: 0,
                    request_id: 11,
                    partition: rovqet_core::PartitionRef {
                        tenant_id: 1,
                        topic_id: 2,
                        partition_id: 3
                    },
                    applied_count: 1,
                },
                ShardCommandResponse::Applied {
                    index: 1,
                    request_id: 12,
                    partition: rovqet_core::PartitionRef {
                        tenant_id: 4,
                        topic_id: 5,
                        partition_id: 6
                    },
                    applied_count: 1,
                },
            ],
            "each partition gets its own result"
        );
        assert_eq!(machine.state().partitions.len(), 2);
        let first = machine.state().partition(&key(1, 2, 3)).expect("first");
        let second = machine.state().partition(&key(4, 5, 6)).expect("second");
        assert_eq!(first.applied_count, 1);
        assert_eq!(second.applied_count, 1);
        assert_ne!(
            first.chain_digest, second.chain_digest,
            "the chains cover different partitions and payloads"
        );
        assert_eq!(machine.state().last_applied, Some(log_id(1)));
    }

    #[tokio::test]
    async fn a_snapshot_round_trip_reproduces_the_identical_state_in_a_fresh_machine() {
        let source_directory = tempfile::tempdir().expect("temp dir");
        let mut source =
            ShardStateMachine::open(source_directory.path(), identity()).expect("open");
        let responses = source
            .apply(vec![
                blank_entry(0),
                command_entry(1, command(11, 1, 2, 3, b"alpha", 5)),
                membership_entry(2),
                command_entry(3, command(12, 1, 2, 3, b"beta", 5)),
                command_entry(4, command(13, 4, 5, 6, b"gamma", 1)),
            ])
            .await
            .expect("apply");
        assert_eq!(responses.len(), 5, "one response per entry");

        let snapshot = source
            .get_snapshot_builder()
            .await
            .build_snapshot()
            .await
            .expect("build snapshot");
        assert_eq!(snapshot.meta.last_log_id, Some(log_id(4)));
        let bytes = snapshot_bytes(&snapshot);

        let target_directory = tempfile::tempdir().expect("temp dir");
        let mut target =
            ShardStateMachine::open(target_directory.path(), identity()).expect("open target");
        let mut receiving = target.begin_receiving_snapshot().await.expect("begin");
        assert!(
            receiving.get_ref().is_empty(),
            "a fresh receiving cursor starts empty"
        );
        receiving.get_mut().extend_from_slice(&bytes);
        target
            .install_snapshot(&snapshot.meta, receiving)
            .await
            .expect("install");

        assert_eq!(
            target.state(),
            source.state(),
            "the installed state must be identical"
        );
        assert_eq!(target.state().identity, source.state().identity);
        assert_eq!(target.state().revision, source.state().revision);
        assert_eq!(target.state().partitions, source.state().partitions);
        assert_eq!(target.state().last_applied, source.state().last_applied);
        assert_eq!(
            target.state().partitions[&key(1, 2, 3)].chain_digest,
            source.state().partitions[&key(1, 2, 3)].chain_digest,
            "the digest chain survives the installation"
        );
        assert_eq!(
            target.applied_state().await.expect("applied state"),
            source.applied_state().await.expect("applied state"),
            "the last applied log id and membership must match"
        );
    }

    #[tokio::test]
    async fn an_invalid_snapshot_is_refused_and_leaves_the_previous_state_intact() {
        let source_directory = tempfile::tempdir().expect("temp dir");
        let mut source =
            ShardStateMachine::open(source_directory.path(), identity()).expect("open");
        source
            .apply(vec![
                command_entry(0, command(11, 1, 2, 3, b"alpha", 5)),
                membership_entry(1),
            ])
            .await
            .expect("apply");
        let snapshot = source
            .get_snapshot_builder()
            .await
            .build_snapshot()
            .await
            .expect("build snapshot");
        let good_bytes = snapshot_bytes(&snapshot);
        let good_document: ShardSnapshot =
            serde_json::from_slice(&good_bytes).expect("decode our own snapshot");

        // A target with its own committed state that every bad install must preserve.
        let target_directory = tempfile::tempdir().expect("temp dir");
        let mut target =
            ShardStateMachine::open(target_directory.path(), identity()).expect("open target");
        target
            .apply(vec![command_entry(0, command(99, 9, 9, 9, b"own", 1))])
            .await
            .expect("apply");
        let before = target.state().clone();
        let before_applied = target.applied_state().await.expect("applied state");

        async fn refused(
            target: &mut ShardStateMachine,
            meta: &SnapshotMeta<ShardNodeId, ShardNode>,
            bytes: &[u8],
            before: &ShardState,
            before_applied: &(
                Option<LogId<ShardNodeId>>,
                StoredMembership<ShardNodeId, ShardNode>,
            ),
        ) {
            let result = install_bytes(target, meta, bytes).await;
            assert!(
                result.is_err(),
                "an invalid snapshot must be refused, got {result:?}"
            );
            assert_eq!(
                target.state(),
                before,
                "the previous state must stay intact after a refusal"
            );
            assert_eq!(
                target.applied_state().await.expect("applied state"),
                *before_applied,
                "the previous applied log id must stay intact after a refusal"
            );
        }

        // 1. A corrupted checksum.
        let mut corrupted = good_document.clone();
        corrupted.checksum[0] ^= 0xff;
        refused(
            &mut target,
            &snapshot.meta,
            &serde_json::to_vec(&corrupted).expect("encode"),
            &before,
            &before_applied,
        )
        .await;

        // 2. A foreign cluster id (checksum resealed, so only the identity check fires).
        let mut foreign_cluster = good_document.clone();
        foreign_cluster.identity.cluster_id += 1;
        refused(
            &mut target,
            &snapshot.meta,
            &reseal(foreign_cluster),
            &before,
            &before_applied,
        )
        .await;

        // 3. A foreign shard id.
        let mut foreign_shard = good_document.clone();
        foreign_shard.identity.shard_id += 1;
        refused(
            &mut target,
            &snapshot.meta,
            &reseal(foreign_shard),
            &before,
            &before_applied,
        )
        .await;

        // 4. An included index that disagrees with the snapshot metadata.
        let mut wrong_index = good_document.clone();
        wrong_index.last_included_index += 1;
        refused(
            &mut target,
            &snapshot.meta,
            &reseal(wrong_index),
            &before,
            &before_applied,
        )
        .await;

        // 5. An unsupported document version.
        let mut wrong_version = good_document.clone();
        wrong_version.version = SNAPSHOT_VERSION + 1;
        refused(
            &mut target,
            &snapshot.meta,
            &reseal(wrong_version),
            &before,
            &before_applied,
        )
        .await;

        // A valid snapshot still installs afterwards: the refusals changed nothing.
        install_bytes(&mut target, &snapshot.meta, &good_bytes)
            .await
            .expect("a valid snapshot must still install");
        assert_eq!(
            target.state(),
            source.state(),
            "the valid snapshot replaces the refused attempts with the source state"
        );
    }

    #[tokio::test]
    async fn a_reopened_machine_recovers_the_state_and_the_last_applied_log_id() {
        let directory = tempfile::tempdir().expect("temp dir");
        let (expected_state, expected_applied) = {
            let mut machine =
                ShardStateMachine::open(directory.path(), identity()).expect("open machine");
            machine
                .apply(vec![
                    blank_entry(0),
                    command_entry(1, command(11, 1, 2, 3, b"alpha", 5)),
                    command_entry(2, command(12, 4, 5, 6, b"beta", 1)),
                ])
                .await
                .expect("apply");
            (
                machine.state().clone(),
                machine.applied_state().await.expect("applied"),
            )
        };

        let mut reopened =
            ShardStateMachine::open(directory.path(), identity()).expect("reopen machine");
        assert_eq!(
            reopened.state(),
            &expected_state,
            "the persisted state is recovered unchanged"
        );
        assert_eq!(
            reopened.applied_state().await.expect("applied state"),
            expected_applied,
            "the last applied log id and membership are recovered"
        );
        let shared = reopened.shared_state().lock().expect("shared").clone();
        assert_eq!(
            shared, expected_state,
            "the shared handle publishes the recovered state"
        );

        // A foreign group identity never adopts another group's persisted state.
        let foreign = ShardGroupIdentity {
            cluster_id: identity().cluster_id + 1,
            shard_id: identity().shard_id,
        };
        assert!(
            ShardStateMachine::open(directory.path(), foreign).is_err(),
            "a foreign identity must fail closed"
        );
    }

    #[test]
    fn a_partition_key_round_trips_as_a_json_string_and_refuses_malformed_forms() {
        let partition = PartitionKey {
            tenant_id: 1,
            topic_id: 3,
            partition_id: 0,
        };
        let state = PartitionState {
            applied_count: 2,
            last_request_id: 7,
            last_metadata_epoch: 5,
            chain_digest: [9u8; 32],
        };

        // Canonical encoding: a plain string, never a struct or a map.
        assert_eq!(
            serde_json::to_string(&partition).expect("encode key"),
            "\"1:3:0\"",
            "a partition key is a ':'-joined string"
        );
        assert_eq!(
            serde_json::from_str::<PartitionKey>("\"1:3:0\"").expect("decode key"),
            partition
        );

        // Position 1: the key of a `BTreeMap`, as `ShardState::partitions` uses it.
        let map: BTreeMap<PartitionKey, PartitionState> =
            BTreeMap::from([(partition, state.clone())]);
        let encoded_map = serde_json::to_string(&map).expect("encode map");
        assert!(
            encoded_map.contains("\"1:3:0\""),
            "the map key is encoded as a JSON string: {encoded_map}"
        );
        let decoded_map: BTreeMap<PartitionKey, PartitionState> =
            serde_json::from_str(&encoded_map).expect("decode map");
        assert_eq!(decoded_map, map, "the map key round trips");

        // Position 2: an element of a `Vec<(key, state)>`, as the snapshot uses it.
        let pairs: Vec<(PartitionKey, PartitionState)> = vec![(partition, state.clone())];
        let encoded_pairs = serde_json::to_string(&pairs).expect("encode pairs");
        assert!(
            encoded_pairs.contains("\"1:3:0\""),
            "the pair key is encoded as a JSON string: {encoded_pairs}"
        );
        let decoded_pairs: Vec<(PartitionKey, PartitionState)> =
            serde_json::from_str(&encoded_pairs).expect("decode pairs");
        assert_eq!(decoded_pairs, pairs, "the pair key round trips");

        // A full state carrying a map key survives the same round trip.
        let mut full = ShardState::new(identity());
        full.partitions.insert(partition, state);
        let encoded_state = serde_json::to_vec(&full).expect("encode state");
        assert_eq!(
            serde_json::from_slice::<ShardState>(&encoded_state).expect("decode state"),
            full,
            "the whole state document round trips"
        );

        // Malformed shapes are refused, never defaulted.
        for malformed in [
            "\"1:3\"",
            "\"1:3:x\"",
            "\"1::0\"",
            "\"1:3:0:4\"",
            "\"\"",
            "\":3:0\"",
            "\"1:3:0 \"",
            "\"1:3:4294967296\"",
            "\"-1:3:0\"",
            "{\"tenant_id\":1,\"topic_id\":3,\"partition_id\":0}",
            "[1,3,0]",
        ] {
            assert!(
                serde_json::from_str::<PartitionKey>(malformed).is_err(),
                "{malformed} must be refused"
            );
        }
    }

    #[test]
    fn the_sha256_digest_matches_the_standard_vectors() {
        assert_eq!(
            sha256(b""),
            [
                0xe3, 0xb0, 0xc4, 0x42, 0x98, 0xfc, 0x1c, 0x14, 0x9a, 0xfb, 0xf4, 0xc8, 0x99, 0x6f,
                0xb9, 0x24, 0x27, 0xae, 0x41, 0xe4, 0x64, 0x9b, 0x93, 0x4c, 0xa4, 0x95, 0x99, 0x1b,
                0x78, 0x52, 0xb8, 0x55,
            ],
            "sha256 of the empty string"
        );
        assert_eq!(
            sha256(b"abc"),
            [
                0xba, 0x78, 0x16, 0xbf, 0x8f, 0x01, 0xcf, 0xea, 0x41, 0x41, 0x40, 0xde, 0x5d, 0xae,
                0x22, 0x23, 0xb0, 0x03, 0x61, 0xa3, 0x96, 0x17, 0x7a, 0x9c, 0xb4, 0x10, 0xff, 0x61,
                0xf2, 0x00, 0x15, 0xad,
            ],
            "sha256 of \"abc\""
        );
    }

    #[test]
    fn the_inline_payload_bound_still_rejects_an_oversized_command() {
        let mut state = ShardState::new(identity());
        let oversized = vec![0u8; MAX_INLINE_PAYLOAD_BYTES + 1];
        let response = state.apply_command(
            0,
            &ShardCommand {
                request_id: 1,
                partition: rovqet_core::PartitionRef {
                    tenant_id: 1,
                    topic_id: 2,
                    partition_id: 3,
                },
                payload: ShardPayload::Inline(oversized),
                metadata_epoch: 1,
            },
        );
        match response {
            ShardCommandResponse::Rejected { reason } => {
                assert!(
                    reason.contains("exceeds the bound"),
                    "the refusal names the bound: {reason}"
                );
            }
            other => panic!("an oversized payload must be rejected, got {other:?}"),
        }
        assert!(state.partitions.is_empty(), "nothing was applied");
        assert_eq!(state.revision, 0, "a rejection is not a mutation");
    }
}
