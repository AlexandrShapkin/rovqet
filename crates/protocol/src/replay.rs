//! Additive standalone Q1 replay contract. No group delivery or ACK semantics.
use crate::{CodecError, Field, Frame, FrameType, registry::field as f};
pub const MAX_MESSAGES: usize = 1024;
pub const MAX_BYTES: usize = 4 * 1024 * 1024;
pub const MAX_WAIT_MS: u32 = 30_000;
pub const END_BYTES: usize = 78;
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum ReadStatus {
    Empty = 1,
    End = 2,
    CountLimit = 3,
    ByteLimit = 4,
    WaitExpired = 5,
}
impl TryFrom<u8> for ReadStatus {
    type Error = ();
    fn try_from(n: u8) -> Result<Self, ()> {
        Ok(match n {
            1 => Self::Empty,
            2 => Self::End,
            3 => Self::CountLimit,
            4 => Self::ByteLimit,
            5 => Self::WaitExpired,
            _ => return Err(()),
        })
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReadScope {
    pub tenant: u64,
    pub topic: u64,
    pub partition: u32,
    pub incarnation: u128,
}
fn base(kind: FrameType, request: u64, scope: ReadScope, offset: u64) -> Frame {
    let mut frame = Frame::new(kind, request);
    frame.fields = vec![
        Field::required(f::TENANT_ID, scope.tenant.to_be_bytes()),
        Field::required(f::TOPIC_ID, scope.topic.to_be_bytes()),
        Field::required(f::PARTITION_ID, scope.partition.to_be_bytes()),
        Field::required(f::LOG_INCAR, scope.incarnation.to_be_bytes()),
        Field::required(f::OFFSET, offset.to_be_bytes()),
        Field::required(f::QOS, [1]),
    ];
    frame
}
pub fn record(
    request: u64,
    scope: ReadScope,
    offset: u64,
    message_id: u128,
    payload: Vec<u8>,
) -> Frame {
    let mut frame = base(FrameType::ReadRecord, request, scope, offset);
    frame
        .fields
        .push(Field::required(f::MESSAGE_ID, message_id.to_be_bytes()));
    frame.payload = payload;
    frame
}
pub fn end(request: u64, scope: ReadScope, next: u64, status: ReadStatus) -> Frame {
    let mut frame = base(FrameType::ReadEnd, request, scope, next);
    frame
        .fields
        .push(Field::required(f::READ_STATUS, [status as u8]));
    frame.fields.push(Field::required(
        f::HAS_MORE,
        [u8::from(matches!(
            status,
            ReadStatus::CountLimit | ReadStatus::ByteLimit
        ))],
    ));
    frame
}
/// Exact canonical READ_RECORD length. Header=84 bytes, flags=0, no checksum.
pub fn record_bytes(payload: usize) -> Option<usize> {
    let mut n = payload;
    let mut varint = 1;
    while n >= 128 {
        varint += 1;
        n >>= 7;
    }
    payload.checked_add(89 + varint)
}
pub(crate) fn validate(frame: &Frame) -> Result<(), CodecError> {
    let require = |id| {
        if frame.field(id).is_none() {
            Err(CodecError::MissingField(id))
        } else {
            Ok(())
        }
    };
    let common = [
        f::TENANT_ID,
        f::TOPIC_ID,
        f::PARTITION_ID,
        f::LOG_INCAR,
        f::OFFSET,
        f::QOS,
    ];
    if frame.checksum {
        return Err(CodecError::InvalidFlags(1));
    }
    for id in &common[2..] {
        require(*id)?;
    }
    if frame.field(f::QOS) != Some(&[1][..]) {
        return Err(CodecError::InvalidFieldValue(f::QOS));
    }
    for item in &frame.fields {
        if item.id > f::LAST_KNOWN {
            continue;
        } // compatible optional extensions
        let allowed = match frame.frame_type {
            FrameType::ReadFetch => {
                common.contains(&item.id)
                    || [
                        f::TOPIC_NAME,
                        f::PRINCIPAL_ID,
                        f::MAX_MESSAGES,
                        f::MAX_BYTES,
                        f::WAIT_MS,
                    ]
                    .contains(&item.id)
            }
            FrameType::ReadRecord => common.contains(&item.id) || item.id == f::MESSAGE_ID,
            FrameType::ReadEnd => {
                common.contains(&item.id) || [f::READ_STATUS, f::HAS_MORE].contains(&item.id)
            }
            _ => unreachable!(),
        };
        if !allowed {
            return Err(CodecError::InvalidField(item.id));
        }
    }
    match frame.frame_type {
        FrameType::ReadFetch => {
            if frame.field(f::TOPIC_ID).is_none() {
                require(f::TOPIC_NAME)?;
            }
            require(f::MAX_MESSAGES)?;
            require(f::MAX_BYTES)?;
            let number = |id| u32::from_be_bytes(frame.field(id).unwrap().try_into().unwrap());
            if !(1..=MAX_MESSAGES as u32).contains(&number(f::MAX_MESSAGES)) {
                return Err(CodecError::InvalidFieldValue(f::MAX_MESSAGES));
            }
            if !(END_BYTES as u32..=MAX_BYTES as u32).contains(&number(f::MAX_BYTES)) {
                return Err(CodecError::InvalidFieldValue(f::MAX_BYTES));
            }
            if frame.field(f::WAIT_MS).is_some() && number(f::WAIT_MS) > MAX_WAIT_MS {
                return Err(CodecError::InvalidFieldValue(f::WAIT_MS));
            }
        }
        FrameType::ReadRecord => {
            require(f::TENANT_ID)?;
            require(f::TOPIC_ID)?;
            require(f::MESSAGE_ID)?;
            if u64::from_be_bytes(frame.field(f::OFFSET).unwrap().try_into().unwrap()) % 2 != 0 {
                return Err(CodecError::InvalidFieldValue(f::OFFSET));
            }
        }
        FrameType::ReadEnd => {
            require(f::TENANT_ID)?;
            require(f::TOPIC_ID)?;
            require(f::READ_STATUS)?;
            require(f::HAS_MORE)?;
            let offset = u64::from_be_bytes(frame.field(f::OFFSET).unwrap().try_into().unwrap());
            if offset % 2 != 0
                || (frame.field(f::READ_STATUS) == Some(&[1][..]) && offset != 0)
                || (matches!(frame.field(f::READ_STATUS), Some([2] | [3] | [4])) && offset == 0)
            {
                return Err(CodecError::InvalidFieldValue(f::OFFSET));
            }
            let more = matches!(frame.field(f::READ_STATUS), Some([3] | [4]));
            if frame.field(f::HAS_MORE) != Some(&[u8::from(more)][..]) {
                return Err(CodecError::InvalidFieldValue(f::HAS_MORE));
            }
        }
        _ => unreachable!(),
    }
    if frame.frame_type != FrameType::ReadRecord && !frame.payload.is_empty() {
        return Err(CodecError::InvalidLength);
    }
    Ok(())
}
