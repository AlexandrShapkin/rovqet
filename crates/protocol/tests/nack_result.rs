use rovqet_protocol::{
    CodecError, Field, Frame, FrameType, Limits, decode_prefix, encode, registry::field as f,
};

fn result(outcome: u8) -> Frame {
    let mut frame = Frame::new(FrameType::ConsumerNackResult, 0x400);
    frame.fields = vec![
        Field::required(f::TENANT_ID, 1u64.to_be_bytes()),
        Field::required(f::TOPIC_NAME, b"durable"),
        Field::required(f::QOS, [1]),
        Field::required(f::PARTITION_ID, 0u32.to_be_bytes()),
        Field::required(f::LOG_INCAR, 42u128.to_be_bytes()),
        Field::required(f::GROUP_ID, 9u64.to_be_bytes()),
        Field::required(f::GROUP_GENERATION, 1u64.to_be_bytes()),
        Field::required(f::OFFSET, 0u64.to_be_bytes()),
        Field::required(f::DELIVERY_ATTEMPT, 1u32.to_be_bytes()),
        Field::required(f::NACK_OUTCOME, [outcome]),
    ];
    if outcome >= 3 {
        frame
            .fields
            .push(Field::required(f::DISPOSITION_ID, 12345u128.to_be_bytes()));
    }
    if outcome == 1 {
        frame
            .fields
            .push(Field::required(f::RETRY_AFTER_MS, 100u32.to_be_bytes()));
    }
    frame
}

#[test]
fn independent_nack_result_golden_vector_round_trips() {
    let line = include_str!("fixtures/nack-v1.txt")
        .lines()
        .find(|line| !line.is_empty() && !line.starts_with('#'))
        .unwrap();
    let (name, hex) = line.split_once(' ').unwrap();
    assert_eq!(name, "nack_result");
    let bytes: Vec<u8> = hex
        .as_bytes()
        .chunks_exact(2)
        .map(|s| u8::from_str_radix(std::str::from_utf8(s).unwrap(), 16).unwrap())
        .collect();
    let frame = result(3);
    assert_eq!(encode(&frame, Limits::default()).unwrap(), bytes);
    assert_eq!(
        decode_prefix(&bytes, Limits::default()).unwrap(),
        Some((frame, bytes.len()))
    );
}

#[test]
fn nack_result_rejects_ambiguous_or_incomplete_outcomes() {
    // Outcome 0 is undefined.
    assert!(matches!(
        encode(&result(0), Limits::default()),
        Err(CodecError::InvalidFieldValue(f::NACK_OUTCOME))
    ));
    // Retry scheduling requires a delay.
    let mut retry = result(1);
    retry.fields.retain(|field| field.id != f::RETRY_AFTER_MS);
    assert!(matches!(
        encode(&retry, Limits::default()),
        Err(CodecError::MissingField(f::RETRY_AFTER_MS))
    ));
    // Terminal outcomes require a disposition identity.
    let mut terminal = result(3);
    terminal
        .fields
        .retain(|field| field.id != f::DISPOSITION_ID);
    assert!(matches!(
        encode(&terminal, Limits::default()),
        Err(CodecError::MissingField(f::DISPOSITION_ID))
    ));
    // NACK acceptance can never be expressed as ACK_COMMITTED.
    assert_ne!(FrameType::ConsumerNackResult, FrameType::ConsumerAckResult);
}
