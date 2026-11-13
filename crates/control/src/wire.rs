//! Portable binary inter-node framing.
//!
//! The envelope is a fixed-size header followed by a bounded body. Every integer is
//! big-endian; lengths are validated against the configured bounds *before* any body
//! allocation. The cluster namespace is separate from the client-facing protocol registry.
use std::fmt;

pub const MAGIC: [u8; 4] = *b"RQCL";
/// Wire version of the inter-node envelope. A peer that does not speak it is rejected.
pub const API_VERSION: u16 = 1;
/// Bytes before the body: magic, version, kind, flags and the fixed envelope fields.
pub const HEADER_BYTES: usize = 82;
/// Header plus the trailing CRC32C.
pub const FRAME_OVERHEAD: usize = HEADER_BYTES + 4;
/// Maximum accepted request/response body. Bodies are validated before allocation.
pub const MAX_BODY_BYTES: usize = 1024 * 1024;
/// Minimum accepted timeout budget for a forwarded request.
pub const MIN_TIMEOUT_MS: u32 = 1;
/// Maximum accepted timeout budget; a peer cannot request an unbounded wait.
pub const MAX_TIMEOUT_MS: u32 = 60_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WireError {
    InvalidMagic,
    UnsupportedVersion(u16),
    InvalidFlags(u16),
    BodyTooLarge(u32),
    Truncated { expected: usize, found: usize },
    ChecksumMismatch,
    InvalidField(&'static str),
}

impl fmt::Display for WireError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for WireError {}

/// One inter-node request or response envelope.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Envelope {
    pub cluster_id: u128,
    pub source_node: u64,
    pub target_node: u64,
    pub request_id: u128,
    pub rpc_kind: u16,
    pub api_version: u16,
    pub metadata_revision: u64,
    pub ownership_epoch: u64,
    pub timeout_budget_ms: u32,
    pub body: Vec<u8>,
}

impl Envelope {
    pub fn new(
        cluster_id: u128,
        source_node: u64,
        target_node: u64,
        request_id: u128,
        rpc_kind: u16,
        metadata_revision: u64,
        ownership_epoch: u64,
        timeout_budget_ms: u32,
        body: Vec<u8>,
    ) -> Self {
        Self {
            cluster_id,
            source_node,
            target_node,
            request_id,
            rpc_kind,
            api_version: API_VERSION,
            metadata_revision,
            ownership_epoch,
            timeout_budget_ms,
            body,
        }
    }

    pub fn encoded_len(&self) -> usize {
        FRAME_OVERHEAD + self.body.len()
    }

    /// Portable, explicit encoding. Never serializes native Rust structures.
    pub fn encode(&self) -> Result<Vec<u8>, WireError> {
        if self.body.len() > MAX_BODY_BYTES {
            return Err(WireError::BodyTooLarge(self.body.len() as u32));
        }
        if self.api_version != API_VERSION {
            return Err(WireError::UnsupportedVersion(self.api_version));
        }
        if self.timeout_budget_ms > MAX_TIMEOUT_MS {
            return Err(WireError::InvalidField("timeout budget"));
        }
        let mut bytes = Vec::with_capacity(self.encoded_len());
        bytes.extend_from_slice(&MAGIC);
        bytes.extend_from_slice(&self.api_version.to_be_bytes());
        bytes.extend_from_slice(&self.rpc_kind.to_be_bytes());
        bytes.extend_from_slice(&0u16.to_be_bytes());
        bytes.extend_from_slice(&self.cluster_id.to_be_bytes());
        bytes.extend_from_slice(&self.source_node.to_be_bytes());
        bytes.extend_from_slice(&self.target_node.to_be_bytes());
        bytes.extend_from_slice(&self.request_id.to_be_bytes());
        bytes.extend_from_slice(&self.metadata_revision.to_be_bytes());
        bytes.extend_from_slice(&self.ownership_epoch.to_be_bytes());
        bytes.extend_from_slice(&self.timeout_budget_ms.to_be_bytes());
        bytes.extend_from_slice(&(self.body.len() as u32).to_be_bytes());
        bytes.extend_from_slice(&self.body);
        let checksum = crc32c(&bytes);
        bytes.extend_from_slice(&checksum.to_be_bytes());
        Ok(bytes)
    }

    /// Strict decoding of one complete frame. `frame` must contain exactly one envelope.
    pub fn decode(frame: &[u8]) -> Result<Self, WireError> {
        if frame.len() < HEADER_BYTES {
            return Err(WireError::Truncated {
                expected: HEADER_BYTES,
                found: frame.len(),
            });
        }
        if frame[..4] != MAGIC {
            return Err(WireError::InvalidMagic);
        }
        let api_version = u16::from_be_bytes(frame[4..6].try_into().unwrap());
        if api_version != API_VERSION {
            return Err(WireError::UnsupportedVersion(api_version));
        }
        let rpc_kind = u16::from_be_bytes(frame[6..8].try_into().unwrap());
        let flags = u16::from_be_bytes(frame[8..10].try_into().unwrap());
        if flags != 0 {
            return Err(WireError::InvalidFlags(flags));
        }
        let cluster_id = u128::from_be_bytes(frame[10..26].try_into().unwrap());
        let source_node = u64::from_be_bytes(frame[26..34].try_into().unwrap());
        let target_node = u64::from_be_bytes(frame[34..42].try_into().unwrap());
        let request_id = u128::from_be_bytes(frame[42..58].try_into().unwrap());
        let metadata_revision = u64::from_be_bytes(frame[58..66].try_into().unwrap());
        let ownership_epoch = u64::from_be_bytes(frame[66..74].try_into().unwrap());
        let timeout_budget_ms = u32::from_be_bytes(frame[74..78].try_into().unwrap());
        let body_len = u32::from_be_bytes(frame[78..82].try_into().unwrap());
        if body_len as usize > MAX_BODY_BYTES {
            return Err(WireError::BodyTooLarge(body_len));
        }
        let body_end = HEADER_BYTES
            .checked_add(body_len as usize)
            .ok_or(WireError::BodyTooLarge(body_len))?;
        if frame.len() != body_end + 4 {
            return Err(WireError::Truncated {
                expected: body_end + 4,
                found: frame.len(),
            });
        }
        let checksum = u32::from_be_bytes(frame[body_end..body_end + 4].try_into().unwrap());
        let body = frame[HEADER_BYTES..body_end].to_vec();
        if crc32c(&frame[..body_end]) != checksum {
            return Err(WireError::ChecksumMismatch);
        }
        if timeout_budget_ms > MAX_TIMEOUT_MS {
            return Err(WireError::InvalidField("timeout budget"));
        }
        Ok(Self {
            cluster_id,
            source_node,
            target_node,
            request_id,
            rpc_kind,
            api_version,
            metadata_revision,
            ownership_epoch,
            timeout_budget_ms,
            body,
        })
    }
}

/// CRC32C (Castagnoli), computed bitwise to avoid a table dependency.
pub fn crc32c(data: &[u8]) -> u32 {
    let mut crc = 0xffff_ffffu32;
    for byte in data {
        crc ^= u32::from(*byte);
        for _ in 0..8 {
            let mask = (crc & 1).wrapping_neg();
            crc = (crc >> 1) ^ (0x82f6_3b78 & mask);
        }
    }
    !crc
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> Envelope {
        Envelope::new(7, 1, 2, 99, 1, 5, 11, 250, b"body".to_vec())
    }

    #[test]
    fn envelope_round_trips_byte_exactly() {
        let envelope = sample();
        let bytes = envelope.encode().unwrap();
        assert_eq!(bytes.len(), FRAME_OVERHEAD + 4);
        assert_eq!(Envelope::decode(&bytes).unwrap(), envelope);
    }

    #[test]
    fn crc32c_matches_the_reference_vector() {
        assert_eq!(crc32c(b"123456789"), 0xe306_9283);
    }

    #[test]
    fn malformed_frames_are_rejected_without_allocation() {
        let bytes = sample().encode().unwrap();
        let mut broken = bytes.clone();
        broken[0] = b'X';
        assert!(matches!(
            Envelope::decode(&broken),
            Err(WireError::InvalidMagic)
        ));
        let mut broken = bytes.clone();
        broken[4] = 0;
        broken[5] = 9;
        assert!(matches!(
            Envelope::decode(&broken),
            Err(WireError::UnsupportedVersion(9))
        ));
        let mut broken = bytes.clone();
        broken[9] = 1;
        assert!(matches!(
            Envelope::decode(&broken),
            Err(WireError::InvalidFlags(1))
        ));
        assert!(matches!(
            Envelope::decode(&bytes[..bytes.len() - 2]),
            Err(WireError::Truncated { .. })
        ));
        assert!(matches!(
            Envelope::decode(&bytes[..10]),
            Err(WireError::Truncated { .. })
        ));
        let mut broken = bytes.clone();
        let last = broken.len() - 1;
        broken[last] ^= 0xff;
        assert!(matches!(
            Envelope::decode(&broken),
            Err(WireError::ChecksumMismatch)
        ));
    }

    #[test]
    fn oversized_bodies_are_rejected_before_allocation() {
        let envelope = Envelope::new(7, 1, 2, 99, 1, 5, 11, 250, vec![0; 8]);
        let mut bytes = envelope.encode().unwrap();
        bytes[78..82].copy_from_slice(&(MAX_BODY_BYTES as u32 + 1).to_be_bytes());
        assert!(matches!(
            Envelope::decode(&bytes),
            Err(WireError::BodyTooLarge(_))
        ));
    }
}
