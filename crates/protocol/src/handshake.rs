//! Pure negotiation of supported protocol minors, frame bounds and capabilities.

use crate::registry::{FrameType, field};
use crate::{CodecError, Frame};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Negotiated {
    pub minor: u16,
    pub max_frame_bytes: u32,
    pub capabilities: Vec<u32>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NegotiationError {
    NotHello,
    MissingRequiredCapability(u32),
    InvalidFrameLimit,
    InvalidFields(CodecError),
}

pub fn negotiate_hello(
    hello: &Frame,
    server_minor: u16,
    server_max_frame_bytes: u32,
    supported: &[u32],
) -> Result<Negotiated, NegotiationError> {
    if hello.frame_type != FrameType::Hello {
        return Err(NegotiationError::NotHello);
    }
    let minor = hello
        .field(field::PROTOCOL_MINOR)
        .ok_or(NegotiationError::InvalidFields(CodecError::MissingField(
            field::PROTOCOL_MINOR,
        )))?;
    let limit = hello
        .field(field::MAX_FRAME_BYTES)
        .ok_or(NegotiationError::InvalidFields(CodecError::MissingField(
            field::MAX_FRAME_BYTES,
        )))?;
    let minor: [u8; 2] = minor.try_into().map_err(|_| {
        NegotiationError::InvalidFields(CodecError::InvalidFieldValue(field::PROTOCOL_MINOR))
    })?;
    let limit: [u8; 4] = limit.try_into().map_err(|_| {
        NegotiationError::InvalidFields(CodecError::InvalidFieldValue(field::MAX_FRAME_BYTES))
    })?;
    let size = u32::from_be_bytes(limit).min(server_max_frame_bytes);
    if size == 0 {
        return Err(NegotiationError::InvalidFrameLimit);
    }
    let mut capabilities = Vec::new();
    for item in hello
        .fields
        .iter()
        .filter(|item| item.id == field::CAPABILITIES)
    {
        let bytes: [u8; 4] = item.value.as_slice().try_into().map_err(|_| {
            NegotiationError::InvalidFields(CodecError::InvalidFieldValue(field::CAPABILITIES))
        })?;
        let capability = u32::from_be_bytes(bytes);
        if supported.contains(&capability) {
            if !capabilities.contains(&capability) {
                capabilities.push(capability);
            }
        } else if item.critical {
            return Err(NegotiationError::MissingRequiredCapability(capability));
        }
    }
    Ok(Negotiated {
        minor: u16::from_be_bytes(minor).min(server_minor),
        max_frame_bytes: size,
        capabilities,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Field;

    fn hello() -> Frame {
        let mut f = Frame::new(FrameType::Hello, 1);
        f.fields
            .push(Field::required(field::PROTOCOL_MINOR, [0, 2]));
        f.fields.push(Field::required(
            field::MAX_FRAME_BYTES,
            10000_u32.to_be_bytes(),
        ));
        f
    }

    #[test]
    fn negotiate_common_minor_limit_and_optional_caps() {
        let mut f = hello();
        f.fields
            .push(Field::optional(field::CAPABILITIES, 11_u32.to_be_bytes()));
        f.fields
            .push(Field::optional(field::CAPABILITIES, 99_u32.to_be_bytes()));
        assert_eq!(
            negotiate_hello(&f, 1, 5000, &[11]),
            Ok(Negotiated {
                minor: 1,
                max_frame_bytes: 5000,
                capabilities: vec![11],
            })
        );
    }

    #[test]
    fn reject_unknown_required_capability() {
        let mut f = hello();
        f.fields
            .push(Field::required(field::CAPABILITIES, 99_u32.to_be_bytes()));
        assert_eq!(
            negotiate_hello(&f, 1, 5000, &[11]),
            Err(NegotiationError::MissingRequiredCapability(99))
        );
    }

    #[test]
    fn reject_non_hello_and_zero_limits() {
        let mut f = hello();
        f.frame_type = FrameType::Publish;
        assert_eq!(
            negotiate_hello(&f, 0, 1000, &[]),
            Err(NegotiationError::NotHello)
        );
        f.frame_type = FrameType::Hello;
        assert_eq!(
            negotiate_hello(&f, 0, 0, &[]),
            Err(NegotiationError::InvalidFrameLimit)
        );
    }
}
