//! Standalone history inspection. Success requires a validated terminal READ_END.
use super::*;
pub use rovqet_protocol::replay::ReadStatus;
use rovqet_protocol::replay::{self, ReadScope};
use rovqet_transport::read_frame_sized;

#[derive(Debug, Clone)]
pub struct ReadOptions {
    pub partition: PartitionRef,
    pub from: DurableOffset,
    pub max_messages: u32,
    /// Total encoded READ_RECORD + READ_END bytes, not only payloads.
    pub max_bytes: u32,
    pub wait: Duration,
}
#[derive(Debug, PartialEq, Eq)]
pub struct ReadRecord {
    pub offset: DurableOffset,
    pub message_id: u128,
    pub payload: Vec<u8>,
}
#[derive(Debug)]
pub struct ReadBatch {
    pub partition: PartitionRef,
    pub records: Vec<ReadRecord>,
    pub next: DurableOffset,
    pub has_more: bool,
    pub end_of_current_history: bool,
    pub status: ReadStatus,
    pub encoded_bytes: usize,
}
#[derive(Debug)]
pub enum ReadError {
    InvalidRequest,
    Closed,
    /// Transport loss, timeout or cancellation before READ_END never yields a partial batch.
    Incomplete,
    InvalidResponse(&'static str),
    ReplayNotAvailable,
    PermissionDenied,
    ResourceExhausted,
    StorageUnavailable,
    Unsupported,
    Remote(ClientError),
}
impl fmt::Display for ReadError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for ReadError {}
fn remote_read(frame: &Frame) -> ReadError {
    match remote(frame) {
        Ok(ClientError::Remote {
            code: Some(ErrorCode::ReplayNotAvailable),
            ..
        }) => ReadError::ReplayNotAvailable,
        Ok(ClientError::Remote {
            code: Some(ErrorCode::PermissionDenied | ErrorCode::Unauthenticated),
            ..
        }) => ReadError::PermissionDenied,
        Ok(ClientError::Remote {
            code: Some(ErrorCode::ResourceExhausted),
            ..
        }) => ReadError::ResourceExhausted,
        Ok(ClientError::Remote {
            code: Some(ErrorCode::StorageUnavailable),
            ..
        }) => ReadError::StorageUnavailable,
        Ok(ClientError::Remote {
            code: Some(ErrorCode::UnsupportedCapability | ErrorCode::UnsupportedGuarantee),
            ..
        }) => ReadError::Unsupported,
        Ok(other) => ReadError::Remote(other),
        Err(_) => ReadError::InvalidResponse("invalid remote error"),
    }
}
fn scope(frame: &Frame) -> Result<ReadScope, ReadError> {
    let integer =
        |id| u64_field(frame, id).map_err(|_| ReadError::InvalidResponse("invalid scope"));
    Ok(ReadScope {
        tenant: integer(f::TENANT_ID)?,
        topic: integer(f::TOPIC_ID)?,
        partition: u32_field(frame, f::PARTITION_ID)
            .map_err(|_| ReadError::InvalidResponse("invalid partition"))?,
        incarnation: u128::from_be_bytes(
            required(frame, f::LOG_INCAR)
                .map_err(|_| ReadError::InvalidResponse("missing incarnation"))?
                .try_into()
                .map_err(|_| ReadError::InvalidResponse("invalid incarnation"))?,
        ),
    })
}
impl Client {
    /// Read-only Q1 inspection: no delivery leases, cursor mutations or automatic retries.
    /// Cancellation owns and closes the stream even between complete READ_RECORD frames.
    pub async fn read_q1(
        &mut self,
        topic: &str,
        options: ReadOptions,
    ) -> Result<ReadBatch, ReadError> {
        if self.stream.is_none() {
            return Err(ReadError::Closed);
        }
        let wait =
            u32::try_from(options.wait.as_millis()).map_err(|_| ReadError::InvalidRequest)?;
        if options.from.log_incarnation == 0 || topic.is_empty() || topic.len() > 255 {
            return Err(ReadError::InvalidRequest);
        }
        let mut request = self
            .request(FrameType::ReadFetch)
            .map_err(|_| ReadError::InvalidRequest)?;
        request.fields = vec![
            Field::required(f::TOPIC_NAME, topic.as_bytes()),
            Field::required(f::TENANT_ID, options.partition.tenant_id.to_be_bytes()),
            Field::required(f::TOPIC_ID, options.partition.topic_id.to_be_bytes()),
            Field::required(
                f::PARTITION_ID,
                options.partition.partition_id.to_be_bytes(),
            ),
            Field::required(f::LOG_INCAR, options.from.log_incarnation.to_be_bytes()),
            Field::required(f::OFFSET, options.from.sequence.to_be_bytes()),
            Field::required(f::QOS, [1]),
            Field::required(f::MAX_MESSAGES, options.max_messages.to_be_bytes()),
            Field::required(f::MAX_BYTES, options.max_bytes.to_be_bytes()),
            Field::required(f::WAIT_MS, wait.to_be_bytes()),
        ];
        rovqet_protocol::encode(&request, self.limits).map_err(|_| ReadError::InvalidRequest)?;
        let expected_scope = ReadScope {
            tenant: options.partition.tenant_id,
            topic: options.partition.topic_id,
            partition: options.partition.partition_id,
            incarnation: options.from.log_incarnation,
        };
        let duration = self
            .config
            .request_timeout
            .checked_add(options.wait)
            .ok_or(ReadError::InvalidRequest)?;
        let deadline = Instant::now()
            .checked_add(duration)
            .ok_or(ReadError::InvalidRequest)?;
        let mut stream = self.stream.take().ok_or(ReadError::Closed)?;
        let mut records = Vec::with_capacity(options.max_messages as usize);
        let mut encoded_bytes = 0usize;
        let mut last_offset: Option<u64> = None;
        let mut remote_terminal = false;
        let operation = async {
            write_frame(&mut stream, &request, self.limits)
                .await
                .map_err(|_| ReadError::Incomplete)?;
            loop {
                let (frame, bytes) = read_frame_sized(&mut stream, self.limits)
                    .await
                    .map_err(|e| {
                        if e.kind() == io::ErrorKind::InvalidData {
                            ReadError::InvalidResponse("invalid wire frame")
                        } else {
                            ReadError::Incomplete
                        }
                    })?
                    .ok_or(ReadError::Incomplete)?;
                if frame.frame_type == FrameType::Ping {
                    let mut pong = frame;
                    pong.frame_type = FrameType::Pong;
                    write_frame(&mut stream, &pong, self.limits)
                        .await
                        .map_err(|_| ReadError::Incomplete)?;
                    continue;
                }
                if frame.frame_type == FrameType::Pong {
                    continue;
                }
                if matches!(frame.frame_type, FrameType::Drain | FrameType::Close) {
                    return Err(ReadError::Incomplete);
                }
                // Existing volatile subscriptions can interleave; preserve their independent bounds.
                if frame.frame_type == FrameType::Message {
                    let item = delivery(frame)
                        .map_err(|_| ReadError::InvalidResponse("invalid volatile delivery"))?;
                    let charge = item.payload.len() + item.topic.len();
                    if self.pending.len() >= self.config.max_pending_messages
                        || charge
                            > self
                                .config
                                .max_pending_bytes
                                .saturating_sub(self.pending_bytes)
                    {
                        return Err(ReadError::ResourceExhausted);
                    }
                    self.pending_bytes += charge;
                    self.pending.push_back(item);
                    continue;
                }
                if frame.request_id != request.request_id {
                    return Err(ReadError::InvalidResponse("request correlation"));
                }
                if frame.frame_type == FrameType::Error {
                    remote_terminal = records.is_empty();
                    return Err(remote_read(&frame));
                }
                if !matches!(frame.frame_type, FrameType::ReadRecord | FrameType::ReadEnd) {
                    return Err(ReadError::InvalidResponse("unexpected frame"));
                }
                if scope(&frame)? != expected_scope {
                    return Err(ReadError::InvalidResponse("scope or incarnation mismatch"));
                }
                encoded_bytes = encoded_bytes
                    .checked_add(bytes)
                    .filter(|&n| n <= options.max_bytes as usize)
                    .ok_or(ReadError::InvalidResponse("response byte limit"))?;
                let offset = u64_field(&frame, f::OFFSET)
                    .map_err(|_| ReadError::InvalidResponse("invalid offset"))?;
                if frame.frame_type == FrameType::ReadRecord {
                    // Application offsets are sparse and stable; metadata records between
                    // messages consume WAL positions without consuming message offsets.
                    let order = last_offset.map_or(options.from.sequence, |last| last + 1);
                    if offset < order {
                        return Err(ReadError::InvalidResponse("non-monotonic MESSAGE offset"));
                    }
                    if records.len() >= options.max_messages as usize
                        || encoded_bytes + replay::END_BYTES > options.max_bytes as usize
                    {
                        return Err(ReadError::InvalidResponse(
                            "response count or terminal byte limit",
                        ));
                    }
                    last_offset = Some(offset);
                    let message_id = u128::from_be_bytes(
                        required(&frame, f::MESSAGE_ID)
                            .map_err(|_| ReadError::InvalidResponse("missing message ID"))?
                            .try_into()
                            .map_err(|_| ReadError::InvalidResponse("invalid message ID"))?,
                    );
                    records.push(ReadRecord {
                        offset: DurableOffset {
                            log_incarnation: expected_scope.incarnation,
                            sequence: offset,
                        },
                        message_id,
                        payload: frame.payload,
                    });
                    continue;
                }
                let status = ReadStatus::try_from(
                    required(&frame, f::READ_STATUS)
                        .map_err(|_| ReadError::InvalidResponse("missing end status"))?[0],
                )
                .map_err(|_| ReadError::InvalidResponse("invalid end status"))?;
                let continuation = offset;
                let after_last = last_offset.is_none_or(|last| continuation > last);
                let valid = match status {
                    ReadStatus::Empty => {
                        records.is_empty() && continuation == 0 && options.from.sequence == 0
                    }
                    ReadStatus::End => after_last,
                    ReadStatus::CountLimit => {
                        after_last
                            && !records.is_empty()
                            && records.len() == options.max_messages as usize
                    }
                    ReadStatus::ByteLimit => {
                        after_last
                            && !records.is_empty()
                            && records.len() < options.max_messages as usize
                    }
                    ReadStatus::WaitExpired => {
                        records.is_empty()
                            && !options.wait.is_zero()
                            && continuation == options.from.sequence
                    }
                };
                if !valid {
                    return Err(ReadError::InvalidResponse("inconsistent end reason"));
                }
                let has_more = frame.field(f::HAS_MORE) == Some(&[1][..]);
                return Ok((status, has_more, continuation));
            }
        };
        let result = tokio::time::timeout_at(deadline, operation)
            .await
            .unwrap_or(Err(ReadError::Incomplete));
        match result {
            Ok((status, has_more, continuation)) => {
                self.stream = Some(stream);
                Ok(ReadBatch {
                    partition: options.partition,
                    records,
                    next: DurableOffset {
                        log_incarnation: expected_scope.incarnation,
                        sequence: continuation,
                    },
                    has_more,
                    end_of_current_history: !has_more,
                    status,
                    encoded_bytes,
                })
            }
            Err(error) => {
                if remote_terminal {
                    self.stream = Some(stream);
                }
                Err(error)
            }
        }
    }
}
