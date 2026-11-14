//! Typed inter-node RPC kinds and their explicit portable bodies.
//!
//! Bodies are encoded field by field with big-endian integers and length-prefixed byte
//! strings. No native Rust structure is serialized directly.
use crate::ids::{ClusterId, MetadataRevision, NodeId, OwnershipEpoch};
use std::fmt;

/// Numeric kinds follow docs/architecture/inter-node-rpc.md:20-35 exactly.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RpcKind {
    RoutePublish,
    RouteFetch,
    RouteConsumerAck,
    /// Remote Q0 fan-out is not enabled in C-01 and is answered as unsupported.
    RouteQ0Fanout,
    MetadataSnapshotGet,
    OwnershipInspect,
    /// Q1 WAL transfer stages are reserved for a later migration implementation.
    Q1TransferPrepare,
    Q1TransferChunk,
    Q1TransferFinalize,
    Health,
}

impl RpcKind {
    pub fn as_u16(self) -> u16 {
        match self {
            Self::RoutePublish => 1,
            Self::RouteFetch => 2,
            Self::RouteConsumerAck => 3,
            Self::RouteQ0Fanout => 4,
            Self::MetadataSnapshotGet => 5,
            Self::OwnershipInspect => 6,
            Self::Q1TransferPrepare => 7,
            Self::Q1TransferChunk => 8,
            Self::Q1TransferFinalize => 9,
            Self::Health => 10,
        }
    }
    pub fn from_u16(value: u16) -> Option<Self> {
        match value {
            1 => Some(Self::RoutePublish),
            2 => Some(Self::RouteFetch),
            3 => Some(Self::RouteConsumerAck),
            4 => Some(Self::RouteQ0Fanout),
            5 => Some(Self::MetadataSnapshotGet),
            6 => Some(Self::OwnershipInspect),
            7 => Some(Self::Q1TransferPrepare),
            8 => Some(Self::Q1TransferChunk),
            9 => Some(Self::Q1TransferFinalize),
            10 => Some(Self::Health),
            _ => None,
        }
    }
    /// Kinds that the cluster answers explicitly as unsupported instead of pretending to
    /// work. The Q1 transfer kinds are implemented by the migration path.
    pub fn is_reserved_unsupported(self) -> bool {
        matches!(self, Self::RouteQ0Fanout)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RpcErrorCode {
    UnknownKind = 1,
    UnsupportedVersion = 2,
    ClusterMismatch = 3,
    WrongTarget = 4,
    UnknownSource = 5,
    Malformed = 6,
    StaleEpoch = 7,
    NotOwner = 8,
    NoMetadata = 9,
    ExpiredAuthority = 10,
    Unsupported = 11,
    ForwardHopExceeded = 12,
    Capacity = 13,
    Timeout = 14,
    Uncertain = 15,
    Internal = 16,
}

impl RpcErrorCode {
    pub fn as_u16(self) -> u16 {
        self as u16
    }
    pub fn from_u16(value: u16) -> Option<Self> {
        Some(match value {
            1 => Self::UnknownKind,
            2 => Self::UnsupportedVersion,
            3 => Self::ClusterMismatch,
            4 => Self::WrongTarget,
            5 => Self::UnknownSource,
            6 => Self::Malformed,
            7 => Self::StaleEpoch,
            8 => Self::NotOwner,
            9 => Self::NoMetadata,
            10 => Self::ExpiredAuthority,
            11 => Self::Unsupported,
            12 => Self::ForwardHopExceeded,
            13 => Self::Capacity,
            14 => Self::Timeout,
            15 => Self::Uncertain,
            _ => Self::Internal,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BodyError {
    Truncated,
    Malformed(&'static str),
    TooLarge,
}
impl fmt::Display for BodyError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for BodyError {}

/// Little writer with explicit bounds.
#[derive(Debug, Default)]
pub struct BodyWriter {
    bytes: Vec<u8>,
}
impl BodyWriter {
    pub fn new() -> Self {
        Self { bytes: Vec::new() }
    }
    pub fn u8(&mut self, value: u8) -> &mut Self {
        self.bytes.push(value);
        self
    }
    pub fn u16(&mut self, value: u16) -> &mut Self {
        self.bytes.extend_from_slice(&value.to_be_bytes());
        self
    }
    pub fn u32(&mut self, value: u32) -> &mut Self {
        self.bytes.extend_from_slice(&value.to_be_bytes());
        self
    }
    pub fn u64(&mut self, value: u64) -> &mut Self {
        self.bytes.extend_from_slice(&value.to_be_bytes());
        self
    }
    pub fn u128(&mut self, value: u128) -> &mut Self {
        self.bytes.extend_from_slice(&value.to_be_bytes());
        self
    }
    pub fn bytes(&mut self, value: &[u8]) -> &mut Self {
        self.u32(value.len() as u32);
        self.bytes.extend_from_slice(value);
        self
    }
    pub fn finish(self) -> Vec<u8> {
        self.bytes
    }
}

/// Little reader with strict bounds; every read is length checked.
#[derive(Debug)]
pub struct BodyReader<'a> {
    bytes: &'a [u8],
    at: usize,
}
impl<'a> BodyReader<'a> {
    pub fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, at: 0 }
    }
    fn take(&mut self, count: usize) -> Result<&'a [u8], BodyError> {
        let end = self.at.checked_add(count).ok_or(BodyError::Truncated)?;
        if end > self.bytes.len() {
            return Err(BodyError::Truncated);
        }
        let slice = &self.bytes[self.at..end];
        self.at = end;
        Ok(slice)
    }
    pub fn u8(&mut self) -> Result<u8, BodyError> {
        Ok(self.take(1)?[0])
    }
    pub fn u16(&mut self) -> Result<u16, BodyError> {
        Ok(u16::from_be_bytes(self.take(2)?.try_into().unwrap()))
    }
    pub fn u32(&mut self) -> Result<u32, BodyError> {
        Ok(u32::from_be_bytes(self.take(4)?.try_into().unwrap()))
    }
    pub fn u64(&mut self) -> Result<u64, BodyError> {
        Ok(u64::from_be_bytes(self.take(8)?.try_into().unwrap()))
    }
    pub fn u128(&mut self) -> Result<u128, BodyError> {
        Ok(u128::from_be_bytes(self.take(16)?.try_into().unwrap()))
    }
    pub fn bytes(&mut self) -> Result<&'a [u8], BodyError> {
        let length = self.u32()? as usize;
        if length > crate::wire::MAX_BODY_BYTES {
            return Err(BodyError::TooLarge);
        }
        self.take(length)
    }
    pub fn finished(&self) -> bool {
        self.at == self.bytes.len()
    }
}

/// Largest accepted transfer chunk payload. The inter-node body limit is 1 MiB, so a chunk
/// leaves room for its own header fields.
pub const MAX_TRANSFER_CHUNK_BYTES: usize = 256 * 1024;
/// Largest accepted number of files in one transfer.
pub const MAX_TRANSFER_FILES: usize = 64;
/// Largest accepted length of a transfer file name.
pub const MAX_TRANSFER_FILE_NAME: usize = 128;

/// Rejects an absolute path, a traversal, a separator or a hidden name.
///
/// The transfer never accepts a path from the network: only a plain file name inside the
/// staging directory is allowed.
pub fn validate_transfer_file_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= MAX_TRANSFER_FILE_NAME
        && !name.starts_with('/')
        && !name.starts_with('.')
        && !name.contains('/')
        && !name.contains('\\')
        && name != ".."
        && name.chars().all(|value| {
            value.is_ascii_alphanumeric() || value == '.' || value == '_' || value == '-'
        })
}

/// The result of a transfer finalization.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Q1TransferStatus {
    /// The transferred inventory was validated and the target waits for the fence.
    ReadyForFence = 1,
    /// The transferred inventory including the fence evidence was validated.
    ReadyForActivation = 2,
    /// The inventory does not match the declared manifest.
    Mismatch = 3,
}

impl Q1TransferStatus {
    pub fn from_u16(value: u16) -> Option<Self> {
        match value {
            1 => Some(Self::ReadyForFence),
            2 => Some(Self::ReadyForActivation),
            3 => Some(Self::Mismatch),
            _ => None,
        }
    }
}

/// One file of the transferred inventory.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Q1TransferFile {
    /// Plain file name inside the staging directory.
    pub name: String,
    pub size: u64,
    /// Digest of the whole file.
    pub digest: Vec<u8>,
}

impl Q1TransferFile {
    pub fn encode(&self) -> Vec<u8> {
        let mut writer = BodyWriter::new();
        writer.bytes(self.name.as_bytes());
        writer.u64(self.size);
        writer.bytes(&self.digest);
        writer.finish()
    }

    pub fn decode(body: &[u8]) -> Result<Self, BodyError> {
        let mut reader = BodyReader::new(body);
        let name = String::from_utf8(reader.bytes()?.to_vec())
            .map_err(|_| BodyError::Malformed("transfer file name"))?;
        let size = reader.u64()?;
        let digest = reader.bytes()?.to_vec();
        if !reader.finished() {
            return Err(BodyError::Malformed("trailing transfer file bytes"));
        }
        Ok(Self { name, size, digest })
    }
}

/// Q1_TRANSFER_PREPARE: the source announces the inventory it will transfer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Q1TransferPrepareRequest {
    pub migration_id: u128,
    pub source_node: u64,
    pub target_node: u64,
    pub tenant_id: u64,
    pub topic_id: u64,
    pub partition_id: u32,
    pub incarnation: u128,
    pub original_epoch: u64,
    pub next_epoch: u64,
    pub manifest_generation: u64,
    pub manifest_digest: Vec<u8>,
    pub files: Vec<Q1TransferFile>,
}

impl Q1TransferPrepareRequest {
    pub fn encode(&self) -> Vec<u8> {
        let mut writer = BodyWriter::new();
        writer.u128(self.migration_id);
        writer.u64(self.source_node);
        writer.u64(self.target_node);
        writer.u64(self.tenant_id);
        writer.u64(self.topic_id);
        writer.u32(self.partition_id);
        writer.u128(self.incarnation);
        writer.u64(self.original_epoch);
        writer.u64(self.next_epoch);
        writer.u64(self.manifest_generation);
        writer.bytes(&self.manifest_digest);
        writer.u32(self.files.len() as u32);
        for file in &self.files {
            writer.bytes(&file.encode());
        }
        writer.finish()
    }

    pub fn decode(body: &[u8]) -> Result<Self, BodyError> {
        let mut reader = BodyReader::new(body);
        let migration_id = reader.u128()?;
        let source_node = reader.u64()?;
        let target_node = reader.u64()?;
        let tenant_id = reader.u64()?;
        let topic_id = reader.u64()?;
        let partition_id = reader.u32()?;
        let incarnation = reader.u128()?;
        let original_epoch = reader.u64()?;
        let next_epoch = reader.u64()?;
        let manifest_generation = reader.u64()?;
        let manifest_digest = reader.bytes()?.to_vec();
        let count = reader.u32()? as usize;
        if count > MAX_TRANSFER_FILES {
            return Err(BodyError::TooLarge);
        }
        let mut files = Vec::with_capacity(count);
        for _ in 0..count {
            files.push(Q1TransferFile::decode(reader.bytes()?)?);
        }
        if !reader.finished() {
            return Err(BodyError::Malformed("trailing transfer prepare bytes"));
        }
        Ok(Self {
            migration_id,
            source_node,
            target_node,
            tenant_id,
            topic_id,
            partition_id,
            incarnation,
            original_epoch,
            next_epoch,
            manifest_generation,
            manifest_digest,
            files,
        })
    }
}

/// The answer to a transfer prepare.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Q1TransferPrepareResponse {
    pub accepted: bool,
    /// The identity of the staging area the target created for this migration.
    pub staging_id: u128,
    pub error: Option<RpcErrorCode>,
}

impl Q1TransferPrepareResponse {
    pub fn encode(&self) -> Vec<u8> {
        let mut writer = BodyWriter::new();
        writer.u8(u8::from(self.accepted));
        writer.u128(self.staging_id);
        writer.u16(self.error.map(|code| code.as_u16()).unwrap_or(0));
        writer.finish()
    }

    pub fn decode(body: &[u8]) -> Result<Self, BodyError> {
        let mut reader = BodyReader::new(body);
        let accepted = reader.u8()? != 0;
        let staging_id = reader.u128()?;
        let code = reader.u16()?;
        if !reader.finished() {
            return Err(BodyError::Malformed(
                "trailing transfer prepare response bytes",
            ));
        }
        Ok(Self {
            accepted,
            staging_id,
            error: if code == 0 {
                None
            } else {
                RpcErrorCode::from_u16(code)
            },
        })
    }
}

/// Q1_TRANSFER_CHUNK: one bounded, digest protected piece of a transferred file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Q1TransferChunkRequest {
    pub migration_id: u128,
    /// The identity of the inventory this chunk belongs to. A migration may transfer more
    /// than one inventory: the final one contains the history after the fence.
    pub inventory_digest: Vec<u8>,
    pub chunk_index: u64,
    /// Index into the file list of the prepare request. A chunk never carries a path.
    pub file_index: u32,
    pub offset: u64,
    pub data: Vec<u8>,
    /// Digest of the chunk payload.
    pub digest: Vec<u8>,
}

impl Q1TransferChunkRequest {
    pub fn encode(&self) -> Vec<u8> {
        let mut writer = BodyWriter::new();
        writer.u128(self.migration_id);
        writer.bytes(&self.inventory_digest);
        writer.u64(self.chunk_index);
        writer.u32(self.file_index);
        writer.u64(self.offset);
        writer.bytes(&self.data);
        writer.bytes(&self.digest);
        writer.finish()
    }

    pub fn decode(body: &[u8]) -> Result<Self, BodyError> {
        let mut reader = BodyReader::new(body);
        let migration_id = reader.u128()?;
        let inventory_digest = reader.bytes()?.to_vec();
        let chunk_index = reader.u64()?;
        let file_index = reader.u32()?;
        let offset = reader.u64()?;
        let data = reader.bytes()?.to_vec();
        if data.len() > MAX_TRANSFER_CHUNK_BYTES {
            return Err(BodyError::TooLarge);
        }
        let digest = reader.bytes()?.to_vec();
        if !reader.finished() {
            return Err(BodyError::Malformed("trailing transfer chunk bytes"));
        }
        Ok(Self {
            migration_id,
            inventory_digest,
            chunk_index,
            file_index,
            offset,
            data,
            digest,
        })
    }
}

/// The answer to a transfer chunk.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Q1TransferChunkResponse {
    pub accepted: bool,
    /// The chunk was already stored with the same content.
    pub duplicate: bool,
    pub error: Option<RpcErrorCode>,
}

impl Q1TransferChunkResponse {
    pub fn encode(&self) -> Vec<u8> {
        let mut writer = BodyWriter::new();
        writer.u8(u8::from(self.accepted));
        writer.u8(u8::from(self.duplicate));
        writer.u16(self.error.map(|code| code.as_u16()).unwrap_or(0));
        writer.finish()
    }

    pub fn decode(body: &[u8]) -> Result<Self, BodyError> {
        let mut reader = BodyReader::new(body);
        let accepted = reader.u8()? != 0;
        let duplicate = reader.u8()? != 0;
        let code = reader.u16()?;
        if !reader.finished() {
            return Err(BodyError::Malformed(
                "trailing transfer chunk response bytes",
            ));
        }
        Ok(Self {
            accepted,
            duplicate,
            error: if code == 0 {
                None
            } else {
                RpcErrorCode::from_u16(code)
            },
        })
    }
}

/// Q1_TRANSFER_FINALIZE: the source declares the complete inventory.
///
/// The fence evidence is optional in the first round: without it the target only validates
/// the inventory and answers READY_FOR_FENCE. With it the target validates the complete
/// post-fence inventory and answers READY_FOR_ACTIVATION.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Q1TransferFinalizeRequest {
    pub migration_id: u128,
    /// The identity of the inventory that is finalized.
    pub inventory_digest: Vec<u8>,
    /// Digest over the complete transferred inventory.
    pub final_digest: Vec<u8>,
    /// Last committed offset of the source at the time of the finalize.
    pub last_committed_offset: u64,
    /// The verified transfer watermark.
    pub transfer_watermark: u64,
    pub manifest_generation: u64,
    /// Durable fence proof of the old owner, if the fence already happened.
    pub fence_offset: Option<u64>,
    pub fence_digest: Option<Vec<u8>>,
    pub old_epoch: u64,
}

impl Q1TransferFinalizeRequest {
    pub fn encode(&self) -> Vec<u8> {
        let mut writer = BodyWriter::new();
        writer.u128(self.migration_id);
        writer.bytes(&self.inventory_digest);
        writer.bytes(&self.final_digest);
        writer.u64(self.last_committed_offset);
        writer.u64(self.transfer_watermark);
        writer.u64(self.manifest_generation);
        writer.u8(u8::from(self.fence_offset.is_some()));
        if let Some(offset) = self.fence_offset {
            writer.u64(offset);
            writer.bytes(self.fence_digest.as_deref().unwrap_or_default());
        }
        writer.u64(self.old_epoch);
        writer.finish()
    }

    pub fn decode(body: &[u8]) -> Result<Self, BodyError> {
        let mut reader = BodyReader::new(body);
        let migration_id = reader.u128()?;
        let inventory_digest = reader.bytes()?.to_vec();
        let final_digest = reader.bytes()?.to_vec();
        let last_committed_offset = reader.u64()?;
        let transfer_watermark = reader.u64()?;
        let manifest_generation = reader.u64()?;
        let has_fence = reader.u8()? != 0;
        let (fence_offset, fence_digest) = if has_fence {
            let offset = reader.u64()?;
            let digest = reader.bytes()?.to_vec();
            (Some(offset), Some(digest))
        } else {
            (None, None)
        };
        let old_epoch = reader.u64()?;
        if !reader.finished() {
            return Err(BodyError::Malformed("trailing transfer finalize bytes"));
        }
        Ok(Self {
            migration_id,
            inventory_digest,
            final_digest,
            last_committed_offset,
            transfer_watermark,
            manifest_generation,
            fence_offset,
            fence_digest,
            old_epoch,
        })
    }
}

/// The answer to a transfer finalize.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Q1TransferFinalizeResponse {
    pub status: Q1TransferStatus,
    pub error: Option<RpcErrorCode>,
}

impl Q1TransferFinalizeResponse {
    pub fn encode(&self) -> Vec<u8> {
        let mut writer = BodyWriter::new();
        writer.u16(self.status as u16);
        writer.u16(self.error.map(|code| code.as_u16()).unwrap_or(0));
        writer.finish()
    }

    pub fn decode(body: &[u8]) -> Result<Self, BodyError> {
        let mut reader = BodyReader::new(body);
        let status = reader.u16()?;
        let code = reader.u16()?;
        if !reader.finished() {
            return Err(BodyError::Malformed(
                "trailing transfer finalize response bytes",
            ));
        }
        let status = Q1TransferStatus::from_u16(status)
            .ok_or(BodyError::Malformed("unknown transfer status"))?;
        Ok(Self {
            status,
            error: if code == 0 {
                None
            } else {
                RpcErrorCode::from_u16(code)
            },
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HealthResponse {
    pub cluster_id: ClusterId,
    pub node_id: NodeId,
    pub metadata_revision: MetadataRevision,
    pub uptime_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OwnershipResponse {
    pub found: bool,
    pub owner: NodeId,
    pub ownership_epoch: OwnershipEpoch,
    pub metadata_revision: MetadataRevision,
    pub expired: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RoutePublishRequest {
    pub tenant_id: u64,
    pub topic_id: u64,
    pub partition_id: u32,
    pub qos: u8,
    pub ownership_epoch: u64,
    pub producer_id: u128,
    pub producer_epoch: u64,
    pub producer_sequence: u64,
    pub message_id: u128,
    pub ttl_ms: u32,
    pub payload: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RoutePublishResponse {
    /// 1 = COMMITTED_LOCAL, 0 = not committed.
    pub committed: bool,
    pub offset: u64,
    pub incarnation: u128,
    pub error: Option<RpcErrorCode>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RouteFetchRequest {
    pub tenant_id: u64,
    pub topic_id: u64,
    pub partition_id: u32,
    pub group_id: u64,
    pub generation: u64,
    pub max_messages: u32,
    pub max_bytes: u32,
    pub wait_ms: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RouteDelivery {
    pub offset: u64,
    pub message_id: u128,
    pub lease_id: u128,
    pub generation: u64,
    pub attempt: u32,
    pub payload: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OwnershipRequest {
    pub tenant_id: u64,
    pub topic_id: u64,
    pub partition_id: u32,
}

impl OwnershipRequest {
    pub fn encode(&self) -> Vec<u8> {
        let mut writer = BodyWriter::new();
        writer
            .u64(self.tenant_id)
            .u64(self.topic_id)
            .u32(self.partition_id);
        writer.finish()
    }
    pub fn decode(body: &[u8]) -> Result<Self, BodyError> {
        let mut reader = BodyReader::new(body);
        let value = Self {
            tenant_id: reader.u64()?,
            topic_id: reader.u64()?,
            partition_id: reader.u32()?,
        };
        if !reader.finished() {
            return Err(BodyError::Malformed("trailing bytes"));
        }
        Ok(value)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RouteFetchResponse {
    pub generation: u64,
    pub next_offset: u64,
    pub has_more: bool,
    pub deliveries: Vec<RouteDelivery>,
    pub error: Option<RpcErrorCode>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RouteAckRequest {
    pub tenant_id: u64,
    pub topic_id: u64,
    pub partition_id: u32,
    pub group_id: u64,
    pub generation: u64,
    pub lease_id: u128,
    pub ownership_epoch: u64,
    pub offset: u64,
    pub attempt: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RouteAckResponse {
    pub committed: bool,
    pub offset: u64,
    pub generation: u64,
    pub error: Option<RpcErrorCode>,
}

impl HealthResponse {
    pub fn encode(&self) -> Vec<u8> {
        let mut writer = BodyWriter::new();
        writer
            .u128(self.cluster_id.get())
            .u64(self.node_id.get())
            .u64(self.metadata_revision.get())
            .u64(self.uptime_ms);
        writer.finish()
    }
    pub fn decode(body: &[u8]) -> Result<Self, BodyError> {
        let mut reader = BodyReader::new(body);
        let value = Self {
            cluster_id: ClusterId::new(reader.u128()?).ok_or(BodyError::Malformed("cluster"))?,
            node_id: NodeId::new(reader.u64()?).ok_or(BodyError::Malformed("node"))?,
            metadata_revision: MetadataRevision::new(reader.u64()?)
                .ok_or(BodyError::Malformed("revision"))?,
            uptime_ms: reader.u64()?,
        };
        if !reader.finished() {
            return Err(BodyError::Malformed("trailing bytes"));
        }
        Ok(value)
    }
}

impl OwnershipResponse {
    pub fn encode(&self) -> Vec<u8> {
        let mut writer = BodyWriter::new();
        writer
            .u8(u8::from(self.found))
            .u64(self.owner.get())
            .u64(self.ownership_epoch.get())
            .u64(self.metadata_revision.get())
            .u8(u8::from(self.expired));
        writer.finish()
    }
    pub fn decode(body: &[u8]) -> Result<Self, BodyError> {
        let mut reader = BodyReader::new(body);
        let value = Self {
            found: reader.u8()? == 1,
            owner: NodeId::new(reader.u64()?).unwrap_or(NodeId(1)),
            ownership_epoch: OwnershipEpoch::new(reader.u64()?).unwrap_or(OwnershipEpoch(1)),
            metadata_revision: MetadataRevision::new(reader.u64()?).unwrap_or(MetadataRevision(1)),
            expired: reader.u8()? == 1,
        };
        if !reader.finished() {
            return Err(BodyError::Malformed("trailing bytes"));
        }
        Ok(value)
    }
}

impl RoutePublishRequest {
    pub fn encode(&self) -> Vec<u8> {
        let mut writer = BodyWriter::new();
        writer
            .u64(self.tenant_id)
            .u64(self.topic_id)
            .u32(self.partition_id)
            .u8(self.qos)
            .u64(self.ownership_epoch)
            .u128(self.producer_id)
            .u64(self.producer_epoch)
            .u64(self.producer_sequence)
            .u128(self.message_id)
            .u32(self.ttl_ms)
            .bytes(&self.payload);
        writer.finish()
    }
    pub fn decode(body: &[u8]) -> Result<Self, BodyError> {
        let mut reader = BodyReader::new(body);
        let value = Self {
            tenant_id: reader.u64()?,
            topic_id: reader.u64()?,
            partition_id: reader.u32()?,
            qos: reader.u8()?,
            ownership_epoch: reader.u64()?,
            producer_id: reader.u128()?,
            producer_epoch: reader.u64()?,
            producer_sequence: reader.u64()?,
            message_id: reader.u128()?,
            ttl_ms: reader.u32()?,
            payload: reader.bytes()?.to_vec(),
        };
        if !reader.finished() {
            return Err(BodyError::Malformed("trailing bytes"));
        }
        Ok(value)
    }
}

impl RoutePublishResponse {
    pub fn encode(&self) -> Vec<u8> {
        let mut writer = BodyWriter::new();
        writer
            .u8(u8::from(self.committed))
            .u64(self.offset)
            .u128(self.incarnation)
            .u16(self.error.map_or(0, |error| error.as_u16()));
        writer.finish()
    }
    pub fn decode(body: &[u8]) -> Result<Self, BodyError> {
        let mut reader = BodyReader::new(body);
        let committed = reader.u8()? == 1;
        let offset = reader.u64()?;
        let incarnation = reader.u128()?;
        let error = reader.u16()?;
        if !reader.finished() {
            return Err(BodyError::Malformed("trailing bytes"));
        }
        Ok(Self {
            committed,
            offset,
            incarnation,
            error: if error == 0 {
                None
            } else {
                RpcErrorCode::from_u16(error)
            },
        })
    }
}

impl RouteFetchRequest {
    pub fn encode(&self) -> Vec<u8> {
        let mut writer = BodyWriter::new();
        writer
            .u64(self.tenant_id)
            .u64(self.topic_id)
            .u32(self.partition_id)
            .u64(self.group_id)
            .u64(self.generation)
            .u32(self.max_messages)
            .u32(self.max_bytes)
            .u32(self.wait_ms);
        writer.finish()
    }
    pub fn decode(body: &[u8]) -> Result<Self, BodyError> {
        let mut reader = BodyReader::new(body);
        let value = Self {
            tenant_id: reader.u64()?,
            topic_id: reader.u64()?,
            partition_id: reader.u32()?,
            group_id: reader.u64()?,
            generation: reader.u64()?,
            max_messages: reader.u32()?,
            max_bytes: reader.u32()?,
            wait_ms: reader.u32()?,
        };
        if !reader.finished() {
            return Err(BodyError::Malformed("trailing bytes"));
        }
        Ok(value)
    }
}

impl RouteFetchResponse {
    pub fn encode(&self) -> Vec<u8> {
        let mut writer = BodyWriter::new();
        writer
            .u64(self.generation)
            .u64(self.next_offset)
            .u8(u8::from(self.has_more))
            .u32(self.deliveries.len() as u32);
        for delivery in &self.deliveries {
            writer
                .u64(delivery.offset)
                .u128(delivery.message_id)
                .u128(delivery.lease_id)
                .u64(delivery.generation)
                .u32(delivery.attempt)
                .bytes(&delivery.payload);
        }
        writer.finish()
    }
    pub fn decode(body: &[u8]) -> Result<Self, BodyError> {
        let mut reader = BodyReader::new(body);
        let generation = reader.u64()?;
        let next_offset = reader.u64()?;
        let has_more = reader.u8()? == 1;
        let count = reader.u32()? as usize;
        if count > crate::wire::MAX_BODY_BYTES / 16 {
            return Err(BodyError::TooLarge);
        }
        let mut deliveries = Vec::with_capacity(count.min(1024));
        for _ in 0..count {
            deliveries.push(RouteDelivery {
                offset: reader.u64()?,
                message_id: reader.u128()?,
                lease_id: reader.u128()?,
                generation: reader.u64()?,
                attempt: reader.u32()?,
                payload: reader.bytes()?.to_vec(),
            });
        }
        if !reader.finished() {
            return Err(BodyError::Malformed("trailing bytes"));
        }
        Ok(Self {
            generation,
            next_offset,
            has_more,
            deliveries,
            error: None,
        })
    }
}

impl RouteAckRequest {
    pub fn encode(&self) -> Vec<u8> {
        let mut writer = BodyWriter::new();
        writer
            .u64(self.tenant_id)
            .u64(self.topic_id)
            .u32(self.partition_id)
            .u64(self.group_id)
            .u64(self.generation)
            .u128(self.lease_id)
            .u64(self.ownership_epoch)
            .u64(self.offset)
            .u32(self.attempt);
        writer.finish()
    }
    pub fn decode(body: &[u8]) -> Result<Self, BodyError> {
        let mut reader = BodyReader::new(body);
        let value = Self {
            tenant_id: reader.u64()?,
            topic_id: reader.u64()?,
            partition_id: reader.u32()?,
            group_id: reader.u64()?,
            generation: reader.u64()?,
            lease_id: reader.u128()?,
            ownership_epoch: reader.u64()?,
            offset: reader.u64()?,
            attempt: reader.u32()?,
        };
        if !reader.finished() {
            return Err(BodyError::Malformed("trailing bytes"));
        }
        Ok(value)
    }
}

impl RouteAckResponse {
    pub fn encode(&self) -> Vec<u8> {
        let mut writer = BodyWriter::new();
        writer
            .u8(u8::from(self.committed))
            .u64(self.offset)
            .u64(self.generation)
            .u16(self.error.map_or(0, |error| error.as_u16()));
        writer.finish()
    }
    pub fn decode(body: &[u8]) -> Result<Self, BodyError> {
        let mut reader = BodyReader::new(body);
        let committed = reader.u8()? == 1;
        let offset = reader.u64()?;
        let generation = reader.u64()?;
        let error = reader.u16()?;
        if !reader.finished() {
            return Err(BodyError::Malformed("trailing bytes"));
        }
        Ok(Self {
            committed,
            offset,
            generation,
            error: if error == 0 {
                None
            } else {
                RpcErrorCode::from_u16(error)
            },
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rpc_kinds_round_trip_and_unknown_kinds_are_rejected() {
        for kind in [
            RpcKind::RoutePublish,
            RpcKind::RouteFetch,
            RpcKind::RouteConsumerAck,
            RpcKind::RouteQ0Fanout,
            RpcKind::MetadataSnapshotGet,
            RpcKind::OwnershipInspect,
            RpcKind::Q1TransferPrepare,
            RpcKind::Q1TransferChunk,
            RpcKind::Q1TransferFinalize,
            RpcKind::Health,
        ] {
            assert_eq!(RpcKind::from_u16(kind.as_u16()), Some(kind));
        }
        // The normative registry numbering is frozen by the architecture document.
        assert_eq!(RpcKind::RoutePublish.as_u16(), 1);
        assert_eq!(RpcKind::Health.as_u16(), 10);
        assert_eq!(RpcKind::from_u16(999), None);
        // The Q1 transfer kinds are implemented by the migration path; only the remote Q0
        // fan-out stays unsupported.
        assert!(!RpcKind::Q1TransferPrepare.is_reserved_unsupported());
        assert!(!RpcKind::Q1TransferChunk.is_reserved_unsupported());
        assert!(!RpcKind::Q1TransferFinalize.is_reserved_unsupported());
        assert!(RpcKind::RouteQ0Fanout.is_reserved_unsupported());
    }

    #[test]
    fn transfer_prepare_bodies_round_trip() {
        let request = Q1TransferPrepareRequest {
            migration_id: 0xfeed_0000_0000_0001,
            source_node: 1,
            target_node: 2,
            tenant_id: 1,
            topic_id: 3,
            partition_id: 0,
            incarnation: 77,
            original_epoch: 5,
            next_epoch: 6,
            manifest_generation: 3,
            manifest_digest: vec![7u8; 32],
            files: vec![Q1TransferFile {
                name: "seg-000000.wal".into(),
                size: 4096,
                digest: vec![9u8; 32],
            }],
        };
        let encoded = request.encode();
        assert_eq!(
            Q1TransferPrepareRequest::decode(&encoded).expect("decode"),
            request
        );
        // A truncated body is rejected, never silently accepted.
        assert!(Q1TransferPrepareRequest::decode(&encoded[..encoded.len() - 1]).is_err());
        // Trailing bytes are rejected.
        let mut extended = encoded.clone();
        extended.push(0);
        assert!(Q1TransferPrepareRequest::decode(&extended).is_err());

        let response = Q1TransferPrepareResponse {
            accepted: true,
            staging_id: 12,
            error: None,
        };
        assert_eq!(
            Q1TransferPrepareResponse::decode(&response.encode()).expect("decode"),
            response
        );
    }

    #[test]
    fn transfer_chunk_bodies_round_trip_and_bound_the_payload() {
        let request = Q1TransferChunkRequest {
            migration_id: 5,
            inventory_digest: vec![7u8; 32],
            chunk_index: 3,
            file_index: 0,
            offset: 1024,
            data: vec![1u8; 128],
            digest: vec![2u8; 32],
        };
        let encoded = request.encode();
        assert_eq!(
            Q1TransferChunkRequest::decode(&encoded).expect("decode"),
            request
        );
        let oversized = Q1TransferChunkRequest {
            data: vec![0u8; MAX_TRANSFER_CHUNK_BYTES + 1],
            ..request
        };
        assert_eq!(
            Q1TransferChunkRequest::decode(&oversized.encode()),
            Err(BodyError::TooLarge)
        );
        let response = Q1TransferChunkResponse {
            accepted: true,
            duplicate: true,
            error: None,
        };
        assert_eq!(
            Q1TransferChunkResponse::decode(&response.encode()).expect("decode"),
            response
        );
    }

    #[test]
    fn transfer_finalize_bodies_round_trip_with_and_without_a_fence() {
        let without_fence = Q1TransferFinalizeRequest {
            migration_id: 5,
            final_digest: vec![3u8; 32],
            inventory_digest: vec![6u8; 32],
            last_committed_offset: 42,
            transfer_watermark: 40,
            manifest_generation: 3,
            fence_offset: None,
            fence_digest: None,
            old_epoch: 5,
        };
        let decoded = Q1TransferFinalizeRequest::decode(&without_fence.encode()).expect("decode");
        assert_eq!(decoded, without_fence);
        assert!(decoded.fence_offset.is_none());

        let with_fence = Q1TransferFinalizeRequest {
            fence_offset: Some(43),
            fence_digest: Some(vec![4u8; 32]),
            ..without_fence
        };
        let decoded = Q1TransferFinalizeRequest::decode(&with_fence.encode()).expect("decode");
        assert_eq!(decoded, with_fence);
        assert_eq!(decoded.fence_offset, Some(43));

        for status in [
            Q1TransferStatus::ReadyForFence,
            Q1TransferStatus::ReadyForActivation,
            Q1TransferStatus::Mismatch,
        ] {
            let response = Q1TransferFinalizeResponse {
                status,
                error: None,
            };
            assert_eq!(
                Q1TransferFinalizeResponse::decode(&response.encode()).expect("decode"),
                response
            );
        }
    }

    #[test]
    fn transfer_file_names_reject_paths_and_traversal() {
        assert!(validate_transfer_file_name("seg-000000.wal"));
        assert!(validate_transfer_file_name("MANIFEST"));
        for name in [
            "/etc/passwd",
            "../escape",
            "nested/file",
            "nested\\file",
            ".hidden",
            "..",
            "",
        ] {
            assert!(
                !validate_transfer_file_name(name),
                "{name} must not be accepted as a transfer file name"
            );
        }
    }

    #[test]
    fn route_publish_bodies_round_trip() {
        let request = RoutePublishRequest {
            tenant_id: 1,
            topic_id: 3,
            partition_id: 0,
            qos: 1,
            ownership_epoch: 7,
            producer_id: 11,
            producer_epoch: 0,
            producer_sequence: 4,
            message_id: 100,
            ttl_ms: 30_000,
            payload: b"event".to_vec(),
        };
        let bytes = request.encode();
        assert_eq!(RoutePublishRequest::decode(&bytes).unwrap(), request);
        let mut broken = bytes.clone();
        broken.truncate(bytes.len() - 1);
        assert!(RoutePublishRequest::decode(&broken).is_err());
    }

    #[test]
    fn response_bodies_reject_trailing_bytes() {
        let response = RoutePublishResponse {
            committed: true,
            offset: 8,
            incarnation: 42,
            error: None,
        };
        let mut bytes = response.encode();
        assert_eq!(RoutePublishResponse::decode(&bytes).unwrap(), response);
        bytes.push(0);
        assert!(RoutePublishResponse::decode(&bytes).is_err());
    }
}
