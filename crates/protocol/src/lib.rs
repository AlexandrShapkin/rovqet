//! Native binary protocol for Rovqet (wire major v1).
//!
//! The parser validates declared sizes before allocating payload memory.
//! Protocol encoding and decoding do not perform authentication, authorization,
//! broker durability or message delivery.

#![forbid(unsafe_code)]

mod codec;
pub mod registry;

pub use codec::{CodecError, Field, Frame, Limits, decode_prefix, encode};
pub use registry::{ErrorCode, FrameType};

pub const WIRE_MAJOR_VERSION: u8 = 1;
pub const MAX_FRAME_BYTES: usize = 1_048_576;
pub const DEFAULT_MAX_PAYLOAD_BYTES: usize = 262_144;
pub const MAX_HEADER_BYTES: usize = 4_096;
