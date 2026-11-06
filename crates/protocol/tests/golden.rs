use rovqet_protocol::registry::field;
use rovqet_protocol::{CodecError, Field, Frame, FrameType, Limits, decode_prefix, encode};

fn hex_bytes(hex: &str) -> Vec<u8> {
    hex.as_bytes()
        .chunks_exact(2)
        .map(|part| {
            let chars = std::str::from_utf8(part).unwrap();
            u8::from_str_radix(chars, 16).unwrap()
        })
        .collect()
}

fn pub_ack(qos: u8, outcome: u8, request_id: u64) -> Frame {
    let mut frame = Frame::new(FrameType::PubAck, request_id);
    frame.fields.push(Field::required(field::QOS, [qos]));
    frame
        .fields
        .push(Field::required(field::PUB_OUTCOME, [outcome]));
    frame
        .fields
        .push(Field::required(field::PARTITION_ID, 2_u32.to_be_bytes()));
    frame
        .fields
        .push(Field::required(field::LOG_INCAR, [0x11_u8; 16]));
    frame
        .fields
        .push(Field::required(field::OFFSET, 7_u64.to_be_bytes()));
    frame
}

fn no_quorum() -> Frame {
    let mut frame = Frame::new(FrameType::Error, 0x102);
    frame.fields.push(Field::required(
        field::ERROR_CODE,
        15_u16.to_be_bytes(),
    ));
    frame
        .fields
        .push(Field::required(field::ERROR_RETRYABLE, [0]));
    frame
}

#[test]
fn independent_golden_vectors_match_encoder_and_decoder() {
    for line in include_str!("fixtures/golden-v1.txt").lines() {
        if line.starts_with('#') || line.trim().is_empty() {
            continue;
        }
        let (name, hex) = line.split_once(' ').unwrap();
        let frame = match name {
            "q1_puback" => pub_ack(1, 2, 0x100),
            "q2_puback" => pub_ack(2, 3, 0x101),
            "no_quorum_error" => no_quorum(),
            _ => panic!("unexpected fixture {name}"),
        };
        let expected = hex_bytes(hex);
        assert_eq!(encode(&frame, Limits::default()).unwrap(), expected, "{name}");
        assert_eq!(
            decode_prefix(&expected, Limits::default()),
            Ok(Some((frame, expected.len()))),
            "{name}"
        );
    }
}

#[test]
fn inconsistent_ack_class_is_not_encoded() {
    assert_eq!(
        encode(&pub_ack(1, 3, 11), Limits::default()),
        Err(CodecError::InvalidFieldValue(field::PUB_OUTCOME))
    );
}

#[test]
fn non_utf8_topic_is_rejected_before_routing() {
    let mut frame = Frame::new(FrameType::Subscribe, 10);
    frame
        .fields
        .push(Field::required(field::TOPIC_NAME, [0xff]));
    assert_eq!(
        encode(&frame, Limits::default()),
        Err(CodecError::InvalidFieldValue(field::TOPIC_NAME))
    );
}
