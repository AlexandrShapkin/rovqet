//! Shared protocol constants. Frame encoding is implemented in A-01.
//!
//! This crate does not yet claim to implement the v1 wire contract.
//! See docs/architecture/protocol-registry.md for its proposed binary layout.

#![forbid(unsafe_code)]

/// Major version proposed by the normative protocol specification.
pub const WIRE_MAJOR_VERSION: u8 = 1;

/// Initial maximum frame size (1 MiB), subject to negotiated lower limits.
pub const MAX_FRAME_BYTES: usize = 1_048_576;

/// Initial maximum opaque message payload size (256 KiB).
pub const DEFAULT_MAX_PAYLOAD_BYTES: usize = 262_144;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reference_limits_are_bounded() {
        assert_eq!(WIRE_MAJOR_VERSION, 1);
        assert!(DEFAULT_MAX_PAYLOAD_BYTES < MAX_FRAME_BYTES);
    }
}
