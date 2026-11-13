//! Bounds-first, incremental-friendly v1 frame codec.
//! Unknown optional TLVs are safely skipped; unknown critical ones fail.

use std::collections::HashSet;

use crate::registry::{FrameType, field};
use crate::{DEFAULT_MAX_PAYLOAD_BYTES, MAX_FRAME_BYTES, MAX_HEADER_BYTES, WIRE_MAJOR_VERSION};

const MAGIC: [u8; 2] = *b"RQ";
const FLAG_CHECKSUM: u8 = 0x01;
const CRC32C_POLYNOMIAL: u32 = 0x82F63B78;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Limits {
    pub max_frame_bytes: usize,
    pub max_header_bytes: usize,
    pub max_payload_bytes: usize,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            max_frame_bytes: MAX_FRAME_BYTES,
            max_header_bytes: MAX_HEADER_BYTES,
            max_payload_bytes: DEFAULT_MAX_PAYLOAD_BYTES,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Field {
    pub id: u64,
    pub critical: bool,
    pub value: Vec<u8>,
}

impl Field {
    pub fn required(id: u64, value: impl Into<Vec<u8>>) -> Self {
        Self {
            id,
            critical: true,
            value: value.into(),
        }
    }

    pub fn optional(id: u64, value: impl Into<Vec<u8>>) -> Self {
        Self {
            id,
            critical: false,
            value: value.into(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Frame {
    pub frame_type: FrameType,
    pub request_id: u64,
    pub checksum: bool,
    pub fields: Vec<Field>,
    pub payload: Vec<u8>,
}

impl Frame {
    pub fn new(frame_type: FrameType, request_id: u64) -> Self {
        Self {
            frame_type,
            request_id,
            checksum: false,
            fields: Vec::new(),
            payload: Vec::new(),
        }
    }

    pub fn field(&self, id: u64) -> Option<&[u8]> {
        self.fields
            .iter()
            .find(|field| field.id == id)
            .map(|field| field.value.as_slice())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CodecError {
    InvalidMagic,
    UnsupportedVersion(u8),
    InvalidFlags(u8),
    VarintOverflow,
    NonCanonicalVarint,
    InvalidLength,
    FrameTooLarge,
    HeaderTooLarge,
    PayloadTooLarge,
    UnknownFrameType(u64),
    InvalidField(u64),
    InvalidFieldValue(u64),
    DuplicateField(u64),
    UnsupportedCriticalField(u64),
    MissingField(u64),
    ChecksumMismatch,
}

fn read_uvarint(bytes: &[u8], cursor: &mut usize) -> Result<Option<u64>, CodecError> {
    let mut value = 0_u64;
    for index in 0..10 {
        let Some(&part) = bytes.get(*cursor) else {
            return Ok(None);
        };
        *cursor += 1;
        if index == 9 && part > 1 {
            return Err(CodecError::VarintOverflow);
        }
        value |= u64::from(part & 0x7f) << (index * 7);
        if part & 0x80 == 0 {
            if index > 0 && part == 0 {
                return Err(CodecError::NonCanonicalVarint);
            }
            return Ok(Some(value));
        }
    }
    Err(CodecError::VarintOverflow)
}

fn put_uvarint(mut value: u64, result: &mut Vec<u8>) {
    while value >= 0x80 {
        result.push((value as u8 & 0x7f) | 0x80);
        value >>= 7;
    }
    result.push(value as u8);
}

fn checked_add(left: usize, right: usize) -> Result<usize, CodecError> {
    left.checked_add(right).ok_or(CodecError::InvalidLength)
}

fn validate_fields(frame: &Frame) -> Result<(), CodecError> {
    use field as f;

    let mut seen = HashSet::new();
    for item in &frame.fields {
        if item.id == 0 {
            return Err(CodecError::InvalidField(item.id));
        }
        if item.id > f::LAST_KNOWN {
            if item.critical {
                return Err(CodecError::UnsupportedCriticalField(item.id));
            }
            continue;
        }
        if item.id != f::CAPABILITIES && !seen.insert(item.id) {
            return Err(CodecError::DuplicateField(item.id));
        }
        let expected = match item.id {
            f::PROTOCOL_MINOR | f::ERROR_CODE | f::CLOSE_REASON => Some(2),
            f::CAPABILITIES
            | f::MAX_FRAME_BYTES
            | f::HEARTBEAT_MS
            | f::PARTITION_ID
            | f::TTL_MS
            | f::RETRY_AFTER_MS
            | f::MAX_MESSAGES
            | f::MAX_BYTES
            | f::WAIT_MS
            | f::FLOW_CREDIT_MESSAGES
            | f::FLOW_CREDIT_BYTES
            | f::DELIVERY_ATTEMPT
            | f::ACK_DEADLINE_MS
            | f::MAX_INFLIGHT => Some(4),
            f::AUTH_METHOD
            | f::QOS
            | f::PRIORITY
            | f::PUB_OUTCOME
            | f::ACK_OUTCOME
            | f::ERROR_RETRYABLE
            | f::READ_STATUS
            | f::HAS_MORE => Some(1),
            f::SESSION_ID
            | f::MESSAGE_ID
            | f::PRODUCER_ID
            | f::LEASE_ID
            | f::DISPOSITION_ID
            | f::LOG_INCAR
            | f::SOURCE_LOG_INCAR => Some(16),
            f::PRINCIPAL_ID
            | f::TOPIC_ID
            | f::PRODUCER_EPOCH
            | f::PRODUCER_SEQUENCE
            | f::OFFSET
            | f::SUBSCRIPTION_ID
            | f::GROUP_ID
            | f::GROUP_GENERATION
            | f::OWNERSHIP_EPOCH
            | f::OWNER_NODE_ID
            | f::METADATA_REVISION
            | f::TENANT_ID
            | f::NONCE => Some(8),
            f::PAYLOAD_DIGEST_SHA256 => Some(32),
            _ => None,
        };
        if expected.is_some_and(|width| item.value.len() != width) {
            return Err(CodecError::InvalidFieldValue(item.id));
        }
        if (item.id == f::TOPIC_NAME || item.id == f::FILTER)
            && (item.value.is_empty()
                || item.value.len() > 255
                || std::str::from_utf8(&item.value).is_err())
        {
            return Err(CodecError::InvalidFieldValue(item.id));
        }
        if (item.id == f::QOS && item.value[0] > 2)
            || (item.id == f::PRIORITY && item.value[0] > 3)
            || (item.id == f::PUB_OUTCOME && !(1..=3).contains(&item.value[0]))
            || (item.id == f::ACK_OUTCOME && !(1..=3).contains(&item.value[0]))
            || (item.id == f::ERROR_RETRYABLE && item.value[0] > 1)
            || (item.id == f::HAS_MORE && item.value[0] > 1)
            || (item.id == f::READ_STATUS && !(1..=5).contains(&item.value[0]))
        {
            return Err(CodecError::InvalidFieldValue(item.id));
        }
    }

    let has = |id| frame.field(id).is_some();
    let require = |id| {
        if has(id) {
            Ok(())
        } else {
            Err(CodecError::MissingField(id))
        }
    };
    let topic = || {
        if has(f::TOPIC_NAME) || has(f::TOPIC_ID) {
            Ok(())
        } else {
            Err(CodecError::MissingField(f::TOPIC_NAME))
        }
    };
    let durable_ref = || -> Result<(), CodecError> {
        require(f::PARTITION_ID)?;
        require(f::LOG_INCAR)?;
        require(f::OFFSET)
    };
    if matches!(
        frame.frame_type,
        FrameType::ReadFetch | FrameType::ReadRecord | FrameType::ReadEnd
    ) {
        crate::replay::validate(frame)?;
    }
    match frame.frame_type {
        FrameType::Hello => {
            require(f::PROTOCOL_MINOR)?;
            require(f::MAX_FRAME_BYTES)?;
        }
        FrameType::Welcome => {
            require(f::PROTOCOL_MINOR)?;
            require(f::SESSION_ID)?;
            require(f::MAX_FRAME_BYTES)?;
        }
        FrameType::Auth => {
            require(f::AUTH_METHOD)?;
            require(f::AUTH_DATA)?;
        }
        FrameType::AuthOk => require(f::PRINCIPAL_ID)?,
        FrameType::Publish => {
            topic()?;
            require(f::PARTITION_KEY)?;
            require(f::QOS)?;
            require(f::TTL_MS)?;
            require(f::MESSAGE_ID)?;
            if frame.field(f::QOS).is_some_and(|qos| qos[0] != 0) {
                require(f::PRODUCER_ID)?;
                require(f::PRODUCER_EPOCH)?;
                require(f::PRODUCER_SEQUENCE)?;
            }
        }
        FrameType::PubAck => {
            require(f::QOS)?;
            require(f::PUB_OUTCOME)?;
            let qos = frame.field(f::QOS).unwrap()[0];
            let outcome = frame.field(f::PUB_OUTCOME).unwrap()[0];
            if (qos, outcome) != (0, 1) && (qos, outcome) != (1, 2) && (qos, outcome) != (2, 3) {
                return Err(CodecError::InvalidFieldValue(f::PUB_OUTCOME));
            }
            if qos != 0 {
                durable_ref()?;
            }
        }
        FrameType::Subscribe => {
            if !has(f::FILTER) {
                topic()?;
            }
        }
        FrameType::SubAck | FrameType::Unsubscribe => require(f::SUBSCRIPTION_ID)?,
        FrameType::Message => {
            topic()?;
            require(f::QOS)?;
            require(f::MESSAGE_ID)?;
            if frame.field(f::QOS).is_some_and(|qos| qos[0] != 0) {
                durable_ref()?;
                require(f::GROUP_ID)?;
                require(f::GROUP_GENERATION)?;
                require(f::LEASE_ID)?;
            }
        }
        FrameType::ConsumerAck | FrameType::ConsumerNack | FrameType::ExtendLease => {
            topic()?;
            require(f::PARTITION_ID)?;
            require(f::GROUP_ID)?;
            require(f::GROUP_GENERATION)?;
            require(f::LEASE_ID)?;
            require(f::OWNERSHIP_EPOCH)?;
            require(f::LOG_INCAR)?;
            require(f::OFFSET)?;
        }
        FrameType::ConsumerAckResult => require(f::ACK_OUTCOME)?,
        FrameType::ConsumerNackResult => {
            topic()?;
            require(f::QOS)?;
            if frame.field(f::QOS) != Some(&[1][..]) {
                return Err(CodecError::InvalidFieldValue(f::QOS));
            }
            for id in [
                f::PARTITION_ID,
                f::GROUP_ID,
                f::GROUP_GENERATION,
                f::LOG_INCAR,
                f::OFFSET,
                f::NACK_OUTCOME,
                f::DELIVERY_ATTEMPT,
            ] {
                require(id)?;
            }
            let outcome = frame.field(f::NACK_OUTCOME).unwrap()[0];
            if !(1..=5).contains(&outcome) {
                return Err(CodecError::InvalidFieldValue(f::NACK_OUTCOME));
            }
            if outcome == 1 && frame.field(f::RETRY_AFTER_MS).is_none() {
                return Err(CodecError::MissingField(f::RETRY_AFTER_MS));
            }
            if outcome >= 3 && frame.field(f::DISPOSITION_ID).is_none() {
                return Err(CodecError::MissingField(f::DISPOSITION_ID));
            }
        }
        FrameType::Fetch => {
            topic()?;
            require(f::PARTITION_ID)?;
            require(f::GROUP_ID)?;
            require(f::LOG_INCAR)?;
            require(f::OFFSET)?;
            require(f::MAX_BYTES)?;
            require(f::MAX_MESSAGES)?;
        }
        FrameType::FetchEnd => {
            topic()?;
            require(f::QOS)?;
            if frame.field(f::QOS) != Some(&[1][..]) {
                return Err(CodecError::InvalidFieldValue(f::QOS));
            }
            require(f::PARTITION_ID)?;
            require(f::GROUP_ID)?;
            require(f::GROUP_GENERATION)?;
            require(f::LOG_INCAR)?;
            require(f::OFFSET)?;
            require(f::HAS_MORE)?;
            match frame.field(f::HAS_MORE) {
                Some([0] | [1]) => {}
                Some(_) => return Err(CodecError::InvalidFieldValue(f::HAS_MORE)),
                None => return Err(CodecError::MissingField(f::HAS_MORE)),
            }
        }
        FrameType::FlowCredit => {
            if !(has(f::SUBSCRIPTION_ID) || has(f::GROUP_ID)) {
                return Err(CodecError::MissingField(f::SUBSCRIPTION_ID));
            }
            if !(has(f::FLOW_CREDIT_MESSAGES) || has(f::FLOW_CREDIT_BYTES)) {
                return Err(CodecError::MissingField(f::FLOW_CREDIT_MESSAGES));
            }
        }
        FrameType::Redirect => {
            require(f::OWNER_NODE_ID)?;
            require(f::OWNERSHIP_EPOCH)?;
            require(f::METADATA_REVISION)?;
        }
        FrameType::Error => {
            require(f::ERROR_CODE)?;
            require(f::ERROR_RETRYABLE)?;
        }
        _ => {}
    }
    Ok(())
}

fn crc32c(bytes: &[u8]) -> u32 {
    let mut crc = !0_u32;
    for &byte in bytes {
        crc ^= u32::from(byte);
        for _ in 0..8 {
            let mask = 0_u32.wrapping_sub(crc & 1);
            crc = (crc >> 1) ^ (CRC32C_POLYNOMIAL & mask);
        }
    }
    !crc
}

pub fn encode(frame: &Frame, limits: Limits) -> Result<Vec<u8>, CodecError> {
    validate_fields(frame)?;
    let mut header = Vec::new();
    put_uvarint(frame.frame_type as u64, &mut header);
    header.extend_from_slice(&frame.request_id.to_be_bytes());
    for item in &frame.fields {
        let tag = item
            .id
            .checked_mul(2)
            .and_then(|value| value.checked_add(u64::from(item.critical)))
            .ok_or(CodecError::InvalidField(item.id))?;
        put_uvarint(tag, &mut header);
        put_uvarint(item.value.len() as u64, &mut header);
        header.extend_from_slice(&item.value);
    }
    if header.len() > limits.max_header_bytes {
        return Err(CodecError::HeaderTooLarge);
    }
    if frame.payload.len() > limits.max_payload_bytes {
        return Err(CodecError::PayloadTooLarge);
    }
    let mut result = Vec::new();
    result.extend_from_slice(&MAGIC);
    result.push(WIRE_MAJOR_VERSION);
    result.push(if frame.checksum { FLAG_CHECKSUM } else { 0 });
    put_uvarint(header.len() as u64, &mut result);
    put_uvarint(frame.payload.len() as u64, &mut result);
    let length = checked_add(
        checked_add(result.len(), header.len())?,
        checked_add(frame.payload.len(), if frame.checksum { 4 } else { 0 })?,
    )?;
    if length > limits.max_frame_bytes {
        return Err(CodecError::FrameTooLarge);
    }
    result.reserve(length - result.len());
    result.extend_from_slice(&header);
    result.extend_from_slice(&frame.payload);
    if frame.checksum {
        let checksum = crc32c(&result);
        result.extend_from_slice(&checksum.to_be_bytes());
    }
    Ok(result)
}

/// Decode one complete frame from a byte stream.
/// Returns Ok(None) if further bytes are needed; no payload allocation occurs.
/// A trailing frame remains in the unconsumed input returned via consumed size.
pub fn decode_prefix(input: &[u8], limits: Limits) -> Result<Option<(Frame, usize)>, CodecError> {
    if input.len() < 4 {
        return Ok(None);
    }
    if input[..2] != MAGIC {
        return Err(CodecError::InvalidMagic);
    }
    if input[2] != WIRE_MAJOR_VERSION {
        return Err(CodecError::UnsupportedVersion(input[2]));
    }
    let flags = input[3];
    if flags & !FLAG_CHECKSUM != 0 {
        return Err(CodecError::InvalidFlags(flags));
    }
    let mut cursor = 4;
    let Some(header_len) = read_uvarint(input, &mut cursor)? else {
        return Ok(None);
    };
    if header_len > limits.max_header_bytes as u64 {
        return Err(CodecError::HeaderTooLarge);
    }
    let Some(payload_len) = read_uvarint(input, &mut cursor)? else {
        return Ok(None);
    };
    if payload_len > limits.max_payload_bytes as u64 {
        return Err(CodecError::PayloadTooLarge);
    }
    let header_len = usize::try_from(header_len).map_err(|_| CodecError::HeaderTooLarge)?;
    let payload_len = usize::try_from(payload_len).map_err(|_| CodecError::PayloadTooLarge)?;
    let header_end = checked_add(cursor, header_len)?;
    let body_end = checked_add(header_end, payload_len)?;
    let full_len = checked_add(body_end, if flags & FLAG_CHECKSUM != 0 { 4 } else { 0 })?;
    if full_len > limits.max_frame_bytes {
        return Err(CodecError::FrameTooLarge);
    }
    if input.len() < full_len {
        return Ok(None);
    }
    let Some(kind) = read_uvarint(&input[..header_end], &mut cursor)? else {
        return Err(CodecError::InvalidLength);
    };
    let frame_type = FrameType::try_from(kind).map_err(|_| CodecError::UnknownFrameType(kind))?;
    let request_end = checked_add(cursor, 8)?;
    if request_end > header_end {
        return Err(CodecError::InvalidLength);
    }
    let request_id = u64::from_be_bytes(
        input[cursor..request_end]
            .try_into()
            .map_err(|_| CodecError::InvalidLength)?,
    );
    cursor = request_end;
    let mut fields = Vec::new();
    while cursor < header_end {
        let Some(tag) = read_uvarint(&input[..header_end], &mut cursor)? else {
            return Err(CodecError::InvalidLength);
        };
        let Some(length) = read_uvarint(&input[..header_end], &mut cursor)? else {
            return Err(CodecError::InvalidLength);
        };
        let field_id = tag >> 1;
        let critical = tag & 1 != 0;
        if field_id == 0 {
            return Err(CodecError::InvalidField(field_id));
        }
        let length = usize::try_from(length).map_err(|_| CodecError::InvalidLength)?;
        let end = checked_add(cursor, length)?;
        if end > header_end {
            return Err(CodecError::InvalidLength);
        }
        if field_id > field::LAST_KNOWN {
            if critical {
                return Err(CodecError::UnsupportedCriticalField(field_id));
            }
        } else {
            fields.push(Field {
                id: field_id,
                critical,
                value: input[cursor..end].to_vec(),
            });
        }
        cursor = end;
    }
    if flags & FLAG_CHECKSUM != 0 {
        let expected = u32::from_be_bytes(
            input[body_end..full_len]
                .try_into()
                .map_err(|_| CodecError::InvalidLength)?,
        );
        if crc32c(&input[..body_end]) != expected {
            return Err(CodecError::ChecksumMismatch);
        }
    }
    let frame = Frame {
        frame_type,
        request_id,
        checksum: flags & FLAG_CHECKSUM != 0,
        fields,
        payload: input[header_end..body_end].to_vec(),
    };
    validate_fields(&frame)?;
    Ok(Some((frame, full_len)))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hello() -> Frame {
        let mut frame = Frame::new(FrameType::Hello, 0x0102030405060708);
        frame.fields.push(Field::required(1, 0_u16.to_be_bytes()));
        frame
            .fields
            .push(Field::required(3, 1_048_576_u32.to_be_bytes()));
        frame
    }

    #[test]
    fn golden_hello_is_stable() {
        let bytes = encode(&hello(), Limits::default()).unwrap();
        let expected: &[u8] = &[
            0x52, 0x51, 0x01, 0x00, 0x13, 0x00, 0x01, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07,
            0x08, 0x03, 0x02, 0x00, 0x00, 0x07, 0x04, 0x00, 0x10, 0x00, 0x00,
        ];
        assert_eq!(bytes, expected);
        assert_eq!(
            decode_prefix(&bytes, Limits::default()),
            Ok(Some((hello(), bytes.len())))
        );
    }

    #[test]
    fn fragments_are_incomplete_without_allocating_payload() {
        let bytes = encode(&hello(), Limits::default()).unwrap();
        for len in 0..bytes.len() {
            assert_eq!(decode_prefix(&bytes[..len], Limits::default()), Ok(None));
        }
    }

    #[test]
    fn declared_oversized_frame_is_rejected_before_payload() {
        let bytes = [0x52, 0x51, 1, 0, 0x13, 0x80, 0x80, 0x20];
        assert_eq!(
            decode_prefix(&bytes, Limits::default()),
            Err(CodecError::PayloadTooLarge)
        );
    }

    #[test]
    fn noncanonical_varint_rejected() {
        let bytes = [0x52, 0x51, 1, 0, 0x80, 0x00, 0];
        assert_eq!(
            decode_prefix(&bytes, Limits::default()),
            Err(CodecError::NonCanonicalVarint)
        );
    }

    #[test]
    fn crc_matches_standard_castagnoli_vector() {
        assert_eq!(crc32c(b"123456789"), 0xE3069283);
        let mut frame = hello();
        frame.checksum = true;
        let mut bytes = encode(&frame, Limits::default()).unwrap();
        assert_eq!(
            decode_prefix(&bytes, Limits::default()),
            Ok(Some((frame, bytes.len())))
        );
        bytes[15] ^= 0x01;
        assert_eq!(
            decode_prefix(&bytes, Limits::default()),
            Err(CodecError::ChecksumMismatch)
        );
    }

    #[test]
    fn duplicate_known_singular_field_fails() {
        let mut frame = hello();
        frame.fields.push(Field::required(1, 0_u16.to_be_bytes()));
        assert_eq!(
            encode(&frame, Limits::default()),
            Err(CodecError::DuplicateField(1))
        );
    }

    #[test]
    fn unknown_optional_field_skipped_but_unknown_critical_rejected() {
        let mut frame = hello();
        frame.fields.push(Field::optional(90, [1, 2]));
        let bytes = encode(&frame, Limits::default()).unwrap();
        let (decoded, _) = decode_prefix(&bytes, Limits::default()).unwrap().unwrap();
        assert_eq!(decoded.fields.len(), 2);
        frame.fields.pop();
        frame.fields.push(Field::required(90, [1, 2]));
        assert_eq!(
            encode(&frame, Limits::default()),
            Err(CodecError::UnsupportedCriticalField(90))
        );
    }

    #[test]
    fn durable_publish_requires_producer_token() {
        let mut frame = Frame::new(FrameType::Publish, 8);
        frame
            .fields
            .push(Field::required(field::TOPIC_NAME, b"events".to_vec()));
        frame
            .fields
            .push(Field::required(field::PARTITION_KEY, b"k".to_vec()));
        frame.fields.push(Field::required(field::QOS, [1]));
        frame
            .fields
            .push(Field::required(field::TTL_MS, 1000_u32.to_be_bytes()));
        frame
            .fields
            .push(Field::required(field::MESSAGE_ID, [7_u8; 16]));
        frame.payload = b"payload".to_vec();
        assert_eq!(
            encode(&frame, Limits::default()),
            Err(CodecError::MissingField(field::PRODUCER_ID))
        );
        frame
            .fields
            .push(Field::required(field::PRODUCER_ID, [9_u8; 16]));
        frame
            .fields
            .push(Field::required(field::PRODUCER_EPOCH, 1_u64.to_be_bytes()));
        frame.fields.push(Field::required(
            field::PRODUCER_SEQUENCE,
            0_u64.to_be_bytes(),
        ));
        let encoded = encode(&frame, Limits::default()).unwrap();
        assert_eq!(
            decode_prefix(&encoded, Limits::default()),
            Ok(Some((frame, encoded.len())))
        );
    }

    #[test]
    fn durable_ack_requires_incarnation() {
        let mut frame = Frame::new(FrameType::ConsumerAck, 1);
        frame
            .fields
            .push(Field::required(field::TOPIC_NAME, b"events".to_vec()));
        frame
            .fields
            .push(Field::required(field::PARTITION_ID, 0_u32.to_be_bytes()));
        frame
            .fields
            .push(Field::required(field::GROUP_ID, 1_u64.to_be_bytes()));
        frame.fields.push(Field::required(
            field::GROUP_GENERATION,
            1_u64.to_be_bytes(),
        ));
        frame
            .fields
            .push(Field::required(field::LEASE_ID, [1_u8; 16]));
        frame
            .fields
            .push(Field::required(field::OWNERSHIP_EPOCH, 1_u64.to_be_bytes()));
        assert_eq!(
            encode(&frame, Limits::default()),
            Err(CodecError::MissingField(field::LOG_INCAR))
        );
    }

    #[test]
    fn concatenated_frames_consume_exactly_one() {
        let first = encode(&hello(), Limits::default()).unwrap();
        let mut joined = first.clone();
        joined.extend_from_slice(&first);
        let decoded = decode_prefix(&joined, Limits::default()).unwrap().unwrap();
        assert_eq!(decoded.0, hello());
        assert_eq!(decoded.1, first.len());
    }
}
