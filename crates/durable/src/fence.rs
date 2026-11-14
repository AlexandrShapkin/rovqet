//! Durable revocation of a Q1 owner's write authority.
//!
//! The fence is a record in the Q1 WAL itself. It is written by the same actor that
//! serializes the durable mutations, it is covered by the same record checksum and the
//! same manifest boundary, and a restart replays it. A restarted owner therefore cannot
//! regain its mutation authority just because its in-memory authority is empty again.
use crate::Error;
use sha2::{Digest, Sha256};

/// The magic of a fence payload.
pub const FENCE_MAGIC: &[u8; 4] = b"RQFX";
/// The payload version.
pub const FENCE_VERSION: u16 = 1;
/// The fixed payload size: magic, version, migration id, owner, epochs.
pub const FENCE_PAYLOAD_BYTES: usize = 4 + 2 + 16 + 8 + 8 + 8;

/// The durable state of a revoked owner.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FenceState {
    /// The migration this revocation belongs to.
    pub migration_id: u128,
    /// The node that revoked its authority.
    pub old_owner: u64,
    /// The ownership epoch that was revoked.
    pub old_epoch: u64,
    /// The ownership epoch the successor will own.
    pub next_epoch: u64,
    /// The logical offset of the durable fence record.
    pub offset: u64,
}

/// The verifiable proof of a durable fence.
///
/// The proof exists only after the fence record is durable. It is bound to the migration
/// and to the revoked epoch, so it can never authorize another handover.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FenceProof {
    pub migration_id: u128,
    pub old_owner: u64,
    pub old_epoch: u64,
    pub next_epoch: u64,
    /// The logical offset of the durable fence record.
    pub fence_offset: u64,
    /// The digest of the durable fence record payload.
    pub fence_digest: String,
    /// The manifest generation that contains the fence record.
    pub manifest_generation: u64,
}

/// Encodes the payload of a durable fence record.
pub fn encode_fence(
    migration_id: u128,
    old_owner: u64,
    old_epoch: u64,
    next_epoch: u64,
) -> Vec<u8> {
    let mut payload = Vec::with_capacity(FENCE_PAYLOAD_BYTES);
    payload.extend_from_slice(FENCE_MAGIC);
    payload.extend_from_slice(&FENCE_VERSION.to_be_bytes());
    payload.extend_from_slice(&migration_id.to_be_bytes());
    payload.extend_from_slice(&old_owner.to_be_bytes());
    payload.extend_from_slice(&old_epoch.to_be_bytes());
    payload.extend_from_slice(&next_epoch.to_be_bytes());
    payload
}

/// Decodes the payload of a durable fence record.
pub fn decode_fence(payload: &[u8]) -> Result<(u128, u64, u64, u64), Error> {
    if payload.len() != FENCE_PAYLOAD_BYTES {
        return Err(Error::Corruption("fence payload length"));
    }
    if &payload[..4] != FENCE_MAGIC {
        return Err(Error::Corruption("fence payload magic"));
    }
    let version = u16::from_be_bytes(payload[4..6].try_into().unwrap());
    if version != FENCE_VERSION {
        return Err(Error::Corruption("fence payload version"));
    }
    let migration_id = u128::from_be_bytes(payload[6..22].try_into().unwrap());
    let old_owner = u64::from_be_bytes(payload[22..30].try_into().unwrap());
    let old_epoch = u64::from_be_bytes(payload[30..38].try_into().unwrap());
    let next_epoch = u64::from_be_bytes(payload[38..46].try_into().unwrap());
    Ok((migration_id, old_owner, old_epoch, next_epoch))
}

/// The digest of a fence payload, used to verify a transferred proof.
pub fn fence_digest(payload: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(payload);
    let digest = hasher.finalize();
    let mut encoded = String::with_capacity(digest.len() * 2);
    for byte in digest {
        encoded.push_str(&format!("{byte:02x}"));
    }
    encoded
}
