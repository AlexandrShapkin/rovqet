//! Leased durable consumer delivery. Success requires a validated terminal FETCH_END and an
//! explicitly acknowledged delivery token; tokens are never regenerated automatically.
use super::*;
use rovqet_protocol::registry::field as f;
use rovqet_transport::read_frame_sized;

/// Wire ceilings mirrored from the broker contract; the server enforces the authoritative copy.
const MAX_GROUP_MESSAGES: usize = 1024;
const MAX_GROUP_BYTES: usize = 4 * 1024 * 1024;
const MAX_GROUP_WAIT: Duration = Duration::from_secs(30);
const GROUP_BATCH_BYTES: usize = 128;

#[derive(Debug, Clone)]
pub struct GroupOptions {
    pub partition: PartitionRef,
    pub incarnation: u128,
    pub group_id: u64,
    /// `None` claims (or replaces) the group; `Some` continues that generation.
    pub generation: Option<u64>,
    pub max_messages: u32,
    pub max_bytes: u32,
    pub wait: Duration,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeliveryToken {
    pub partition: PartitionRef,
    pub group_id: u64,
    pub generation: u64,
    pub lease_id: u128,
    pub ownership_epoch: u64,
    pub offset: DurableOffset,
    pub attempt: u32,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupMessage {
    pub token: DeliveryToken,
    pub message_id: u128,
    pub payload: Vec<u8>,
    pub ack_deadline_ms: u32,
}
#[derive(Debug)]
pub struct GroupBatch {
    pub partition: PartitionRef,
    pub group_id: u64,
    pub generation: u64,
    pub messages: Vec<GroupMessage>,
    /// Continuation position: the first unacknowledged application offset, or append end.
    pub next: DurableOffset,
    pub has_more: bool,
    pub end_of_current_history: bool,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AckOutcome {
    Committed {
        offset: DurableOffset,
        generation: u64,
    },
}
#[derive(Debug)]
pub enum GroupError {
    InvalidRequest,
    Closed,
    /// Transport loss, timeout or cancellation before the terminal frame. No partial success.
    Incomplete,
    InvalidResponse(&'static str),
    StaleGeneration,
    LeaseExpired,
    AckNotCommitted,
    PermissionDenied,
    ResourceExhausted,
    ReplayNotAvailable,
    StorageUnavailable,
    Unsupported,
    Remote(ClientError),
}
impl fmt::Display for GroupError {
    fn fmt(&self, out: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(out, "{self:?}")
    }
}
impl std::error::Error for GroupError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Remote(error) => Some(error),
            _ => None,
        }
    }
}
fn map_client(error: ClientError) -> GroupError {
    match &error {
        ClientError::Remote { code, .. } => match code {
            Some(ErrorCode::StaleGeneration) => GroupError::StaleGeneration,
            Some(ErrorCode::LeaseExpired) => GroupError::LeaseExpired,
            Some(ErrorCode::AckNotCommitted) => GroupError::AckNotCommitted,
            Some(ErrorCode::PermissionDenied) | Some(ErrorCode::Unauthenticated) => {
                GroupError::PermissionDenied
            }
            Some(ErrorCode::ResourceExhausted) => GroupError::ResourceExhausted,
            Some(ErrorCode::ReplayNotAvailable) => GroupError::ReplayNotAvailable,
            Some(ErrorCode::StorageUnavailable) | Some(ErrorCode::OutcomeUnknown) => {
                GroupError::StorageUnavailable
            }
            Some(ErrorCode::UnsupportedGuarantee) | Some(ErrorCode::UnsupportedCapability) => {
                GroupError::Unsupported
            }
            _ => GroupError::Remote(error),
        },
        ClientError::Timeout => GroupError::Incomplete,
        _ => GroupError::Remote(error),
    }
}
fn scope_matches(partition: &PartitionRef, incarnation: u128, frame: &Frame) -> bool {
    int_u32(frame, f::PARTITION_ID).ok() == Some(partition.partition_id)
        && int_u128(frame, f::LOG_INCAR).ok() == Some(incarnation)
        && frame
            .field(f::TENANT_ID)
            .is_none_or(|value| value == partition.tenant_id.to_be_bytes())
        && frame
            .field(f::TOPIC_ID)
            .is_none_or(|value| value == partition.topic_id.to_be_bytes())
}
fn int_u128(frame: &Frame, id: u64) -> Result<u128, GroupError> {
    let bytes: [u8; 16] = required(frame, id)
        .map_err(|_| GroupError::InvalidResponse("missing field"))?
        .try_into()
        .map_err(|_| GroupError::InvalidResponse("invalid field width"))?;
    Ok(u128::from_be_bytes(bytes))
}
fn int_u64(frame: &Frame, id: u64) -> Result<u64, GroupError> {
    let bytes: [u8; 8] = required(frame, id)
        .map_err(|_| GroupError::InvalidResponse("missing field"))?
        .try_into()
        .map_err(|_| GroupError::InvalidResponse("invalid field width"))?;
    Ok(u64::from_be_bytes(bytes))
}
fn int_u32(frame: &Frame, id: u64) -> Result<u32, GroupError> {
    let bytes: [u8; 4] = required(frame, id)
        .map_err(|_| GroupError::InvalidResponse("missing field"))?
        .try_into()
        .map_err(|_| GroupError::InvalidResponse("invalid field width"))?;
    Ok(u32::from_be_bytes(bytes))
}
impl Client {
    /// Claims a group generation (`generation: None`) or continues an existing one.
    pub async fn fetch_q1_group(
        &mut self,
        topic: &str,
        options: GroupOptions,
    ) -> Result<GroupBatch, GroupError> {
        if self.stream.is_none() {
            return Err(GroupError::Closed);
        }
        let wait =
            u32::try_from(options.wait.as_millis()).map_err(|_| GroupError::InvalidRequest)?;
        if options.incarnation == 0
            || topic.is_empty()
            || topic.len() > 255
            || options.group_id == 0
            || options.max_messages == 0
            || options.max_messages as usize > MAX_GROUP_MESSAGES
            || options.max_bytes < GROUP_BATCH_BYTES as u32
            || options.max_bytes as usize > MAX_GROUP_BYTES
            || options.wait > MAX_GROUP_WAIT
        {
            return Err(GroupError::InvalidRequest);
        }
        let incarnation = options.incarnation;
        let mut request = self
            .request(FrameType::Fetch)
            .map_err(|_| GroupError::InvalidRequest)?;
        let mut fields = vec![
            Field::required(f::TOPIC_NAME, topic.as_bytes()),
            Field::required(f::TENANT_ID, options.partition.tenant_id.to_be_bytes()),
            Field::required(f::TOPIC_ID, options.partition.topic_id.to_be_bytes()),
            Field::required(f::QOS, [1]),
            Field::required(
                f::PARTITION_ID,
                options.partition.partition_id.to_be_bytes(),
            ),
            Field::required(f::GROUP_ID, options.group_id.to_be_bytes()),
            Field::required(f::LOG_INCAR, incarnation.to_be_bytes()),
            Field::required(f::OFFSET, 0u64.to_be_bytes()),
            Field::required(f::MAX_MESSAGES, options.max_messages.to_be_bytes()),
            Field::required(f::MAX_BYTES, options.max_bytes.to_be_bytes()),
            Field::required(f::WAIT_MS, wait.to_be_bytes()),
        ];
        if let Some(generation) = options.generation {
            fields.push(Field::required(
                f::GROUP_GENERATION,
                generation.to_be_bytes(),
            ));
        }
        request.fields = fields;
        rovqet_protocol::encode(&request, self.limits).map_err(|_| GroupError::InvalidRequest)?;
        let duration = self
            .config
            .request_timeout
            .checked_add(options.wait)
            .ok_or(GroupError::InvalidRequest)?;
        let deadline = Instant::now()
            .checked_add(duration)
            .ok_or(GroupError::InvalidRequest)?;
        let id = request.request_id;
        let mut stream = self.stream.take().ok_or(GroupError::Closed)?;
        let operation = async {
            write_frame(&mut stream, &request, self.limits)
                .await
                .map_err(|_| GroupError::Incomplete)?;
            let mut messages: Vec<GroupMessage> = Vec::new();
            let mut last_offset: Option<u64> = None;
            let mut generation: Option<u64> = None;
            let mut payload_bytes = 0usize;
            loop {
                let (frame, _bytes) = read_frame_sized(&mut stream, self.limits)
                    .await
                    .map_err(|error| {
                        if error.kind() == io::ErrorKind::InvalidData {
                            GroupError::InvalidResponse("invalid wire frame")
                        } else {
                            GroupError::Incomplete
                        }
                    })?
                    .ok_or(GroupError::Incomplete)?;
                if frame.frame_type == FrameType::Ping {
                    let mut pong = frame;
                    pong.frame_type = FrameType::Pong;
                    write_frame(&mut stream, &pong, self.limits)
                        .await
                        .map_err(|_| GroupError::Incomplete)?;
                    continue;
                }
                if frame.frame_type == FrameType::Pong {
                    continue;
                }
                if matches!(frame.frame_type, FrameType::Drain | FrameType::Close) {
                    return Err(GroupError::Incomplete);
                }
                if frame.request_id != id {
                    return Err(GroupError::InvalidResponse("request correlation"));
                }
                if frame.frame_type == FrameType::Error {
                    return Err(map_client(
                        remote(&frame).unwrap_or(ClientError::Protocol("invalid error frame")),
                    ));
                }
                match frame.frame_type {
                    FrameType::Message => {
                        if !scope_matches(&options.partition, incarnation, &frame) {
                            return Err(GroupError::InvalidResponse("scope mismatch"));
                        }
                        if frame.field(f::QOS) != Some(&[1][..]) {
                            return Err(GroupError::InvalidResponse("not a durable delivery"));
                        }
                        if int_u64(&frame, f::GROUP_ID)? != options.group_id {
                            return Err(GroupError::InvalidResponse("group mismatch"));
                        }
                        let message_generation = int_u64(&frame, f::GROUP_GENERATION)? as u64;
                        if *generation.get_or_insert(message_generation) != message_generation {
                            return Err(GroupError::InvalidResponse(
                                "generation changed mid-batch",
                            ));
                        }
                        if options
                            .generation
                            .is_some_and(|expected| expected != message_generation)
                        {
                            return Err(GroupError::InvalidResponse("unexpected generation"));
                        }
                        let offset = int_u64(&frame, f::OFFSET)? as u64;
                        if last_offset.is_some_and(|last| offset <= last) {
                            return Err(GroupError::InvalidResponse("non-monotonic offset"));
                        }
                        last_offset = Some(offset);
                        if messages.len() >= options.max_messages as usize {
                            return Err(GroupError::InvalidResponse("delivery count limit"));
                        }
                        payload_bytes = payload_bytes
                            .checked_add(frame.payload.len())
                            .filter(|&n| n <= options.max_bytes as usize)
                            .ok_or(GroupError::InvalidResponse("delivery byte limit"))?;
                        let lease_id = int_u128(&frame, f::LEASE_ID)?;
                        if lease_id == 0 {
                            return Err(GroupError::InvalidResponse("missing lease"));
                        }
                        let attempt = int_u32(&frame, f::DELIVERY_ATTEMPT)?;
                        if attempt == 0 {
                            return Err(GroupError::InvalidResponse("missing delivery attempt"));
                        }
                        let message_id = int_u128(&frame, f::MESSAGE_ID)?;
                        let ownership_epoch = int_u64(&frame, f::OWNERSHIP_EPOCH)?;
                        let ack_deadline_ms = int_u32(&frame, f::ACK_DEADLINE_MS)?;
                        messages.push(GroupMessage {
                            token: DeliveryToken {
                                partition: PartitionRef {
                                    tenant_id: options.partition.tenant_id,
                                    topic_id: options.partition.topic_id,
                                    partition_id: options.partition.partition_id,
                                },
                                group_id: options.group_id,
                                generation: message_generation,
                                lease_id,
                                ownership_epoch,
                                offset: DurableOffset {
                                    log_incarnation: incarnation,
                                    sequence: offset,
                                },
                                attempt,
                            },
                            message_id,
                            payload: frame.payload,
                            ack_deadline_ms,
                        });
                    }
                    FrameType::FetchEnd => {
                        if !scope_matches(&options.partition, incarnation, &frame) {
                            return Err(GroupError::InvalidResponse("end scope mismatch"));
                        }
                        if int_u64(&frame, f::GROUP_ID)? != options.group_id {
                            return Err(GroupError::InvalidResponse("end group mismatch"));
                        }
                        let end_generation = int_u64(&frame, f::GROUP_GENERATION)? as u64;
                        if generation.is_some_and(|value| value != end_generation) {
                            return Err(GroupError::InvalidResponse("end generation mismatch"));
                        }
                        if options
                            .generation
                            .is_some_and(|expected| expected != end_generation)
                        {
                            return Err(GroupError::InvalidResponse("unexpected generation"));
                        }
                        // The continuation is the durable group cursor. With out-of-order
                        // ACKs it may legitimately sit before the last delivered offset.
                        let continuation = int_u64(&frame, f::OFFSET)? as u64;
                        let has_more = required(&frame, f::HAS_MORE)
                            .map_err(|_| GroupError::InvalidResponse("missing has_more"))?;
                        if has_more.len() != 1 || has_more[0] > 1 {
                            return Err(GroupError::InvalidResponse("invalid has_more"));
                        }
                        return Ok(GroupBatch {
                            partition: options.partition,
                            group_id: options.group_id,
                            generation: end_generation,
                            messages,
                            next: DurableOffset {
                                log_incarnation: incarnation,
                                sequence: continuation,
                            },
                            has_more: has_more[0] == 1,
                            end_of_current_history: has_more[0] == 0,
                        });
                    }
                    _ => return Err(GroupError::InvalidResponse("unexpected frame")),
                }
            }
        };
        let result = tokio::time::timeout_at(deadline, operation)
            .await
            .unwrap_or(Err(GroupError::Incomplete));
        match result {
            Ok(batch) => {
                self.stream = Some(stream);
                Ok(batch)
            }
            Err(error) => {
                // The response stream is framing-uncertain after any failure; never reuse it
                // and never reinterpret a partial batch as success.
                let _ = error;
                Err(error)
            }
        }
    }
    /// Acknowledges exactly one leased delivery. No lease token is ever regenerated here.
    pub async fn ack_q1_group(
        &mut self,
        topic: &str,
        token: &DeliveryToken,
    ) -> Result<AckOutcome, GroupError> {
        if token.group_id == 0
            || token.lease_id == 0
            || token.attempt == 0
            || token.ownership_epoch == 0
            || token.offset.log_incarnation == 0
        {
            return Err(GroupError::InvalidRequest);
        }
        let incarnation = token.offset.log_incarnation;
        let mut frame = self
            .request(FrameType::ConsumerAck)
            .map_err(|_| GroupError::InvalidRequest)?;
        frame.fields = vec![
            Field::required(f::TOPIC_NAME, topic.as_bytes()),
            Field::required(f::TENANT_ID, token.partition.tenant_id.to_be_bytes()),
            Field::required(f::TOPIC_ID, token.partition.topic_id.to_be_bytes()),
            Field::required(f::PARTITION_ID, token.partition.partition_id.to_be_bytes()),
            Field::required(f::GROUP_ID, token.group_id.to_be_bytes()),
            Field::required(f::GROUP_GENERATION, token.generation.to_be_bytes()),
            Field::required(f::LEASE_ID, token.lease_id.to_be_bytes()),
            Field::required(f::OWNERSHIP_EPOCH, token.ownership_epoch.to_be_bytes()),
            Field::required(f::LOG_INCAR, incarnation.to_be_bytes()),
            Field::required(f::OFFSET, token.offset.sequence.to_be_bytes()),
            Field::required(f::DELIVERY_ATTEMPT, token.attempt.to_be_bytes()),
        ];
        rovqet_protocol::encode(&frame, self.limits).map_err(|_| GroupError::InvalidRequest)?;
        let request_id = frame.request_id;
        let response = self
            .exchange(frame, FrameType::ConsumerAckResult)
            .await
            .map_err(|error| match error {
                ClientError::Timeout | ClientError::Closed | ClientError::Io(_) => {
                    GroupError::Incomplete
                }
                other => map_client(other),
            })?;
        if response.request_id != request_id {
            return Err(GroupError::InvalidResponse("ack correlation"));
        }
        let outcome = required(&response, f::ACK_OUTCOME)
            .map_err(|_| GroupError::InvalidResponse("missing ack outcome"))?;
        if outcome != [1] {
            return Err(GroupError::InvalidResponse("unsupported ack outcome"));
        }
        if int_u64(&response, f::GROUP_ID)? != token.group_id
            || int_u64(&response, f::GROUP_GENERATION)? != token.generation
            || int_u64(&response, f::OFFSET)? != token.offset.sequence
        {
            return Err(GroupError::InvalidResponse("ack identity mismatch"));
        }
        Ok(AckOutcome::Committed {
            offset: token.offset,
            generation: token.generation,
        })
    }
}
