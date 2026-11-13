use rovqet_protocol::{
    CodecError, Field, Frame, FrameType, Limits, decode_prefix, encode, registry::field as f,
};

fn terminal() -> Frame {
    let mut frame = Frame::new(FrameType::FetchEnd, 0x300);
    frame.fields = vec![
        Field::required(f::TENANT_ID, 1u64.to_be_bytes()),
        Field::required(f::TOPIC_NAME, b"durable"),
        Field::required(f::PARTITION_ID, 0u32.to_be_bytes()),
        Field::required(f::GROUP_ID, 9u64.to_be_bytes()),
        Field::required(f::GROUP_GENERATION, 3u64.to_be_bytes()),
        Field::required(f::LOG_INCAR, 42u128.to_be_bytes()),
        Field::required(f::OFFSET, 100u64.to_be_bytes()),
        Field::required(f::QOS, [1]),
        Field::required(f::HAS_MORE, [0]),
    ];
    frame
}

#[test]
fn independent_fetch_end_golden_vector_round_trips() {
    let line = include_str!("fixtures/fetch-v1.txt")
        .lines()
        .find(|line| !line.is_empty() && !line.starts_with('#'))
        .unwrap();
    let (name, hex) = line.split_once(' ').unwrap();
    assert_eq!(name, "fetch_end");
    let bytes: Vec<u8> = hex
        .as_bytes()
        .chunks_exact(2)
        .map(|s| u8::from_str_radix(std::str::from_utf8(s).unwrap(), 16).unwrap())
        .collect();
    let frame = terminal();
    assert_eq!(encode(&frame, Limits::default()).unwrap(), bytes);
    assert_eq!(
        decode_prefix(&bytes, Limits::default()).unwrap(),
        Some((frame, bytes.len()))
    );
}

#[test]
fn fetch_end_requires_exact_terminal_fields_and_rejects_duplicates() {
    for (mutate, expected) in [
        (
            Box::new(|frame: &mut Frame| frame.fields.retain(|field| field.id != f::HAS_MORE))
                as Box<dyn Fn(&mut Frame)>,
            "missing",
        ),
        (
            Box::new(|frame: &mut Frame| {
                frame.fields.retain(|field| field.id != f::HAS_MORE);
                frame.fields.push(Field::required(f::HAS_MORE, [2]));
            }),
            "value",
        ),
        (
            Box::new(|frame: &mut Frame| {
                frame.fields.retain(|field| field.id != f::QOS);
                frame.fields.push(Field::required(f::QOS, [0]));
            }),
            "value",
        ),
        (
            Box::new(|frame: &mut Frame| {
                frame
                    .fields
                    .push(Field::required(f::GROUP_ID, 10u64.to_be_bytes()));
            }),
            "duplicate",
        ),
        (
            Box::new(|frame: &mut Frame| {
                frame.fields.retain(|field| field.id != f::GROUP_GENERATION);
                frame.fields.push(Field::required(f::GROUP_GENERATION, [1]));
            }),
            "width",
        ),
    ] {
        let mut frame = terminal();
        mutate(&mut frame);
        let encoded = encode(&frame, Limits::default());
        match expected {
            "missing" => assert!(matches!(
                encoded,
                Err(CodecError::MissingField(f::HAS_MORE))
            )),
            "value" => assert!(matches!(encoded, Err(CodecError::InvalidFieldValue(_)))),
            "duplicate" => assert!(matches!(encoded, Err(CodecError::DuplicateField(_)))),
            _ => assert!(encoded.is_err()),
        }
    }
    // Unknown critical fields are rejected rather than reinterpreted as terminal state.
    let mut frame = terminal();
    frame.fields.push(Field::required(127, [1]));
    assert!(matches!(
        encode(&frame, Limits::default()),
        Err(CodecError::UnsupportedCriticalField(127))
    ));
    // The terminal frame is distinct from the group FETCH request frame.
    assert_ne!(terminal().frame_type, FrameType::Fetch);
    let encoded = encode(&terminal(), Limits::default()).unwrap();
    assert!(
        decode_prefix(&encoded, Limits::default())
            .unwrap()
            .is_some()
    );
}
