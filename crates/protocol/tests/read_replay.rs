use rovqet_protocol::{
    CodecError, Field, Frame, FrameType, Limits, decode_prefix, encode,
    registry::field as f,
    replay::{self, ReadScope, ReadStatus},
};
fn scope() -> ReadScope {
    ReadScope {
        tenant: 1,
        topic: 3,
        partition: 0,
        incarnation: 42,
    }
}
fn fetch() -> Frame {
    let mut frame = Frame::new(FrameType::ReadFetch, 0x200);
    frame.fields = vec![
        Field::required(f::TOPIC_NAME, b"durable"),
        Field::required(f::QOS, [1]),
        Field::required(f::PARTITION_ID, 0u32.to_be_bytes()),
        Field::required(f::LOG_INCAR, 42u128.to_be_bytes()),
        Field::required(f::OFFSET, 0u64.to_be_bytes()),
        Field::required(f::MAX_MESSAGES, 2u32.to_be_bytes()),
        Field::required(f::MAX_BYTES, 1024u32.to_be_bytes()),
        Field::optional(f::WAIT_MS, 0u32.to_be_bytes()),
    ];
    frame
}
#[test]
fn independent_read_golden_vectors_and_exact_size_formula() {
    for line in include_str!("fixtures/read-v1.txt")
        .lines()
        .filter(|s| !s.is_empty() && !s.starts_with('#'))
    {
        let (name, hex) = line.split_once(' ').unwrap();
        let frame = match name {
            "read_fetch" => fetch(),
            "read_record" => replay::record(0x200, scope(), 0, 100, b"event-0".to_vec()),
            "read_end" => replay::end(0x200, scope(), 2, ReadStatus::End),
            _ => panic!("unknown vector"),
        };
        let bytes: Vec<u8> = hex
            .as_bytes()
            .chunks_exact(2)
            .map(|s| u8::from_str_radix(std::str::from_utf8(s).unwrap(), 16).unwrap())
            .collect();
        assert_eq!(encode(&frame, Limits::default()).unwrap(), bytes);
        assert_eq!(
            decode_prefix(&bytes, Limits::default()).unwrap(),
            Some((frame, bytes.len()))
        );
    }
    for payload in [0, 1, 127, 128, 16383, 16384, 262144] {
        assert_eq!(
            encode(
                &replay::record(1, scope(), 0, 100, vec![0; payload]),
                Limits::default()
            )
            .unwrap()
            .len(),
            replay::record_bytes(payload).unwrap()
        );
    }
    for status in [
        ReadStatus::Empty,
        ReadStatus::End,
        ReadStatus::CountLimit,
        ReadStatus::ByteLimit,
        ReadStatus::WaitExpired,
    ] {
        assert_eq!(
            encode(
                &replay::end(
                    1,
                    scope(),
                    if status == ReadStatus::Empty { 0 } else { 2 },
                    status
                ),
                Limits::default()
            )
            .unwrap()
            .len(),
            replay::END_BYTES
        );
    }
}
#[test]
fn strict_read_fields_limits_widths_payload_and_group_exclusion() {
    for (id, value) in [
        (f::MAX_MESSAGES, 0u32.to_be_bytes().to_vec()),
        (f::MAX_MESSAGES, 1025u32.to_be_bytes().to_vec()),
        (f::MAX_BYTES, 77u32.to_be_bytes().to_vec()),
        (f::MAX_BYTES, 4194305u32.to_be_bytes().to_vec()),
        (f::WAIT_MS, 30001u32.to_be_bytes().to_vec()),
        (f::PARTITION_ID, vec![0; 8]),
        (f::LOG_INCAR, vec![0; 8]),
        (f::QOS, vec![2]),
    ] {
        let mut frame = fetch();
        frame.fields.retain(|v| v.id != id);
        frame.fields.push(Field::required(id, value));
        assert!(encode(&frame, Limits::default()).is_err(), "field{id}");
    }
    for kind in [
        FrameType::ReadFetch,
        FrameType::ReadRecord,
        FrameType::ReadEnd,
    ] {
        let frame = match kind {
            FrameType::ReadFetch => fetch(),
            FrameType::ReadRecord => replay::record(1, scope(), 0, 1, vec![]),
            _ => replay::end(1, scope(), 0, ReadStatus::Empty),
        };
        for field in frame.fields.clone() {
            let mut duplicate = frame.clone();
            duplicate.fields.push(field);
            assert!(matches!(
                encode(&duplicate, Limits::default()),
                Err(CodecError::DuplicateField(_))
            ));
        }
        for id in [
            f::GROUP_ID,
            f::GROUP_GENERATION,
            f::LEASE_ID,
            f::ACK_OUTCOME,
        ] {
            let mut bad = frame.clone();
            bad.fields.push(Field::optional(
                id,
                if id == f::LEASE_ID {
                    vec![0; 16]
                } else if id == f::ACK_OUTCOME {
                    vec![1]
                } else {
                    vec![0; 8]
                },
            ));
            assert!(encode(&bad, Limits::default()).is_err());
        }
        let mut bad = frame;
        bad.checksum = true;
        assert!(encode(&bad, Limits::default()).is_err());
    }
    let mut end = replay::end(1, scope(), 0, ReadStatus::Empty);
    end.payload = vec![1];
    assert!(encode(&end, Limits::default()).is_err());
    let mut end = replay::end(1, scope(), 2, ReadStatus::ByteLimit);
    end.fields
        .iter_mut()
        .find(|v| v.id == f::HAS_MORE)
        .unwrap()
        .value = vec![0];
    assert!(encode(&end, Limits::default()).is_err());
    let mut end = replay::end(1, scope(), 0, ReadStatus::Empty);
    end.fields
        .iter_mut()
        .find(|v| v.id == f::READ_STATUS)
        .unwrap()
        .value = vec![6];
    assert!(encode(&end, Limits::default()).is_err());
    let mut bad = fetch();
    bad.payload = vec![1];
    assert!(encode(&bad, Limits::default()).is_err());
}
#[test]
fn old_group_fetch_and_message_requirements_and_unknown_capability_stay_frozen() {
    let mut frame = fetch();
    frame.frame_type = FrameType::Fetch;
    assert!(matches!(
        encode(&frame, Limits::default()),
        Err(CodecError::MissingField(f::GROUP_ID))
    ));
    let mut frame = replay::record(1, scope(), 0, 100, vec![]);
    frame.frame_type = FrameType::Message;
    assert!(matches!(
        encode(&frame, Limits::default()),
        Err(CodecError::MissingField(f::GROUP_ID))
    ));
    let mut frame = fetch();
    frame.fields.push(Field::required(127, vec![1]));
    assert!(matches!(
        encode(&frame, Limits::default()),
        Err(CodecError::UnsupportedCriticalField(127))
    ));
    let mut bytes = encode(&fetch(), Limits::default()).unwrap();
    bytes[6] = 126;
    assert!(matches!(
        decode_prefix(&bytes, Limits::default()),
        Err(CodecError::UnknownFrameType(126))
    ));
}
