//! Bounded, serial asynchronous client for the loopback research profile.
//! Q0 admission never promises delivery. A lost publish response is unresolved;
//! the SDK never automatically republishes it and risks duplicate delivery.
#![forbid(unsafe_code)]

pub use rovqet_core::{DurableOffset, PartitionRef, Priority, PublishOutcome, QoS};
mod group;
pub use group::{
    AckOutcome, DeliveryToken, GroupBatch, GroupError, GroupMessage, GroupOptions, NackOutcome,
};
mod replay;
pub use replay::{ReadBatch, ReadError, ReadOptions, ReadRecord, ReadStatus};
use rovqet_protocol::{ErrorCode, Field, Frame, FrameType, Limits, registry::field as f};
use rovqet_transport::{read_frame, write_frame};
use std::{
    collections::{BTreeMap, VecDeque},
    fmt, io,
    net::SocketAddr,
    time::Duration,
};
use tokio::{
    net::TcpStream,
    time::{Instant, timeout},
};

#[derive(Debug, Clone)]
pub struct ClientConfig {
    pub endpoint: SocketAddr,
    pub request_timeout: Duration,
    pub max_pending_messages: usize,
    pub max_pending_bytes: usize,
    /// Initial per-subscription credits; zero permits explicit manual flow control.
    pub delivery_credit_messages: u32,
    pub delivery_credit_bytes: u32,
    /// Return consumed credit before the next receive operation.
    pub auto_replenish: bool,
}
impl Default for ClientConfig {
    fn default() -> Self {
        Self {
            endpoint: ([127, 0, 0, 1], 7443).into(),
            request_timeout: Duration::from_secs(5),
            max_pending_messages: 128,
            max_pending_bytes: 1024 * 1024,
            delivery_credit_messages: 64,
            delivery_credit_bytes: 256 * 1024,
            auto_replenish: true,
        }
    }
}
#[derive(Debug)]
pub enum ClientError {
    Io(io::Error),
    Timeout,
    Closed,
    Protocol(&'static str),
    Remote {
        code: Option<ErrorCode>,
        raw_code: u16,
        retryable: bool,
        retry_after: Option<Duration>,
    },
    ResourceExhausted,
    UnsupportedGuarantee,
    InvalidFlowControl,
    /// Socket loss or timeout after a publish might have reached the broker.
    OutcomeUnknown,
}
impl fmt::Display for ClientError {
    fn fmt(&self, out: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(out, "{self:?}")
    }
}
impl std::error::Error for ClientError {}
impl From<io::Error> for ClientError {
    fn from(value: io::Error) -> Self {
        Self::Io(value)
    }
}
#[derive(Debug, Clone)]
pub struct PublishOptions {
    pub qos: QoS,
    pub priority: Priority,
    pub ttl: Duration,
    pub key: Vec<u8>,
}
impl Default for PublishOptions {
    fn default() -> Self {
        Self {
            qos: QoS::Q0,
            priority: Priority::Telemetry,
            ttl: Duration::from_secs(2),
            key: Vec::new(),
        }
    }
}
/// Explicit Q1 identity. Keep these values and the original payload for an unresolved retry.
/// Reconnecting never changes this caller-owned producer token or retries automatically.
/// The server advertises its configured window at startup (default 24h). Downtime and
/// uncertain clock intervals extend retention. After expiration the original outcome may
/// be unavailable (OUTCOME_UNKNOWN); retained sequence high-water marks prevent reinsertion.
/// TTL is an admission deadline; direct-route key/priority hints have no durable effect.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Q1PublishOptions {
    pub producer_id: u128,
    pub producer_epoch: u64,
    pub sequence: u64,
    pub message_id: u128,
    pub partition_id: u32,
    pub log_incarnation: u128,
    pub ownership_epoch: u64,
    pub ttl: Duration,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Delivery {
    pub topic: String,
    pub subscription_id: Option<u64>,
    pub message_id: u128,
    pub payload: Vec<u8>,
    /// Encoded MESSAGE bytes charged against delivery credit.
    pub wire_bytes: u32,
}

#[derive(Clone)]
struct SubscriptionState {
    topic: String,
    initial_messages: u32,
    initial_bytes: u32,
}

/// Operations are serialized through `&mut self`; use separate clients for
/// concurrent publishers and subscribers. Buffered deliveries have hard bounds.
pub struct Client {
    config: ClientConfig,
    stream: Option<TcpStream>,
    limits: Limits,
    request_id: u64,
    message_id: u128,
    heartbeat: Duration,
    pending: VecDeque<Delivery>,
    pending_bytes: usize,
    subscriptions: BTreeMap<u64, SubscriptionState>,
    owed_credit: BTreeMap<u64, (u32, u32)>,
}
impl Client {
    pub async fn connect(config: ClientConfig) -> Result<Self, ClientError> {
        if !config.endpoint.ip().is_loopback() {
            return Err(ClientError::Protocol(
                "development endpoint must be loopback",
            ));
        }
        if config.max_pending_messages == 0
            || config.max_pending_bytes == 0
            || config.request_timeout.is_zero()
            || config.delivery_credit_messages > 128
            || config.delivery_credit_bytes > 1024 * 1024
        {
            return Err(ClientError::Protocol("invalid client limits"));
        }
        let stream = timeout(config.request_timeout, TcpStream::connect(config.endpoint))
            .await
            .map_err(|_| ClientError::Timeout)??;
        stream.set_nodelay(true)?;
        let mut client = Self {
            config,
            stream: Some(stream),
            limits: Limits::default(),
            request_id: 0,
            message_id: 0,
            heartbeat: Duration::from_secs(15),
            pending: VecDeque::new(),
            pending_bytes: 0,
            subscriptions: BTreeMap::new(),
            owed_credit: BTreeMap::new(),
        };
        let mut hello = client.request(FrameType::Hello)?;
        hello
            .fields
            .push(Field::required(f::PROTOCOL_MINOR, 0u16.to_be_bytes()));
        hello.fields.push(Field::required(
            f::MAX_FRAME_BYTES,
            (client.limits.max_frame_bytes as u32).to_be_bytes(),
        ));
        let welcome = client.exchange(hello, FrameType::Welcome).await?;
        let max = u32::from_be_bytes(
            required(&welcome, f::MAX_FRAME_BYTES)?
                .try_into()
                .map_err(|_| ClientError::Protocol("invalid frame limit"))?,
        ) as usize;
        if max == 0 || max > client.limits.max_frame_bytes {
            return Err(ClientError::Protocol("invalid negotiated limit"));
        }
        client.limits.max_frame_bytes = max;
        if let Some(bytes) = welcome.field(f::HEARTBEAT_MS) {
            let ms = u32::from_be_bytes(
                bytes
                    .try_into()
                    .map_err(|_| ClientError::Protocol("invalid heartbeat"))?,
            );
            if ms == 0 {
                return Err(ClientError::Protocol("zero heartbeat"));
            }
            client.heartbeat = Duration::from_millis(u64::from(ms).max(2) / 2);
        }
        let mut auth = client.request(FrameType::Auth)?;
        auth.fields.push(Field::required(f::AUTH_METHOD, [0]));
        auth.fields
            .push(Field::required(f::AUTH_DATA, b"dev".to_vec()));
        client.exchange(auth, FrameType::AuthOk).await?;
        Ok(client)
    }
    fn request(&mut self, kind: FrameType) -> Result<Frame, ClientError> {
        self.request_id = self
            .request_id
            .checked_add(1)
            .ok_or(ClientError::Protocol("request id exhausted"))?;
        Ok(Frame::new(kind, self.request_id))
    }
    async fn send(&mut self, frame: &Frame) -> Result<(), ClientError> {
        // Take ownership so external cancellation also closes a partially written stream.
        let mut stream = self.stream.take().ok_or(ClientError::Closed)?;
        match timeout(
            self.config.request_timeout,
            write_frame(&mut stream, frame, self.limits),
        )
        .await
        {
            Ok(Ok(())) => {
                self.stream = Some(stream);
                Ok(())
            }
            Ok(Err(e)) => Err(ClientError::Io(e)),
            Err(_) => Err(ClientError::Timeout),
        }
    }
    async fn incoming(&mut self, deadline: Option<Instant>) -> Result<Frame, ClientError> {
        loop {
            // Dropping this future closes the stream rather than reusing a partial frame.
            let mut stream = self.stream.take().ok_or(ClientError::Closed)?;
            let limits = self.limits;
            let heartbeat = self.heartbeat;
            let reading = async {
                let (mut reader, mut writer) = stream.split();
                let read = read_frame(&mut reader, limits);
                tokio::pin!(read);
                loop {
                    tokio::select! {
                        result=&mut read=>return result,
                        _=tokio::time::sleep(heartbeat)=> {
                            // Control traffic uses request id zero, outside outstanding requests.
                            write_frame(&mut writer,&Frame::new(FrameType::Ping,0),limits).await?;
                        }
                    }
                }
            };
            let result = if let Some(end) = deadline {
                match tokio::time::timeout_at(end, reading).await {
                    Ok(result) => result,
                    Err(_) => return Err(ClientError::Timeout),
                }
            } else {
                reading.await
            };
            let frame = match result {
                Ok(Some(frame)) => frame,
                Ok(None) => return Err(ClientError::Closed),
                Err(e) => return Err(ClientError::Io(e)),
            };
            self.stream = Some(stream);
            match frame.frame_type {
                FrameType::Ping => {
                    let mut pong = frame;
                    pong.frame_type = FrameType::Pong;
                    self.send(&pong).await?;
                }
                FrameType::Pong => {}
                FrameType::Close | FrameType::Drain => {
                    self.stream = None;
                    return Err(ClientError::Closed);
                }
                _ => return Ok(frame),
            }
        }
    }
    async fn exchange(&mut self, frame: Frame, expected: FrameType) -> Result<Frame, ClientError> {
        // Local encoding failures must not be confused with an uncertain send.
        rovqet_protocol::encode(&frame, self.limits)
            .map_err(|_| ClientError::Protocol("invalid request"))?;
        let id = frame.request_id;
        self.send(&frame).await?;
        let deadline = Instant::now() + self.config.request_timeout;
        loop {
            let result = self.incoming(Some(deadline)).await?;
            if result.frame_type == FrameType::Message {
                let delivery = delivery(result)?;
                let bytes = delivery.payload.len() + delivery.topic.len();
                if self.pending.len() >= self.config.max_pending_messages
                    || bytes
                        > self
                            .config
                            .max_pending_bytes
                            .saturating_sub(self.pending_bytes)
                {
                    self.stream = None;
                    return Err(ClientError::ResourceExhausted);
                }
                self.pending_bytes += bytes;
                self.pending.push_back(delivery);
                continue;
            }
            if result.request_id != id {
                self.stream = None;
                return Err(ClientError::Protocol("response request id mismatch"));
            }
            if result.frame_type == FrameType::Error {
                return Err(remote(&result)?);
            }
            if result.frame_type != expected {
                self.stream = None;
                return Err(ClientError::Protocol("unexpected response"));
            }
            return Ok(result);
        }
    }
    pub async fn publish(
        &mut self,
        topic: &str,
        payload: &[u8],
        options: PublishOptions,
    ) -> Result<PublishOutcome, ClientError> {
        if payload.len() > self.limits.max_payload_bytes
            || options.key.len() > self.limits.max_header_bytes
            || topic.is_empty()
            || topic.len() > 255
        {
            return Err(ClientError::Protocol("publish exceeds wire bounds"));
        }
        if options.qos != QoS::Q0 {
            return Err(ClientError::UnsupportedGuarantee);
        }
        let ttl = u32::try_from(options.ttl.as_millis())
            .map_err(|_| ClientError::Protocol("TTL exceeds wire bound"))?;
        self.message_id = self
            .message_id
            .checked_add(1)
            .ok_or(ClientError::Protocol("message id exhausted"))?;
        let mut frame = self.request(FrameType::Publish)?;
        frame.fields = vec![
            Field::required(f::TOPIC_NAME, topic.as_bytes().to_vec()),
            Field::required(f::PARTITION_KEY, options.key),
            Field::required(f::QOS, [0]),
            Field::optional(f::PRIORITY, [options.priority as u8]),
            Field::required(f::TTL_MS, ttl.to_be_bytes()),
            Field::required(f::MESSAGE_ID, self.message_id.to_be_bytes()),
        ];
        frame.payload = payload.to_vec();
        rovqet_protocol::encode(&frame, self.limits)
            .map_err(|_| ClientError::Protocol("invalid publish"))?;
        let result = self
            .exchange(frame, FrameType::PubAck)
            .await
            .map_err(|e| match e {
                remote @ ClientError::Remote { .. } => remote,
                _ => ClientError::OutcomeUnknown,
            })?;
        if result.field(f::QOS) != Some(&[0][..]) || result.field(f::PUB_OUTCOME) != Some(&[1][..])
        {
            return Err(ClientError::OutcomeUnknown);
        }
        Ok(PublishOutcome::Admitted)
    }
    /// Publish to an explicitly configured local-durable Q1 partition.
    /// Only a validated COMMITTED_LOCAL response yields success. A timeout, malformed
    /// response or disconnect after sending is unresolved and closes this connection.
    /// Reconnect and retry with identical options and payload to settle that outcome.
    pub async fn publish_q1(
        &mut self,
        topic: &str,
        payload: &[u8],
        options: Q1PublishOptions,
    ) -> Result<PublishOutcome, ClientError> {
        if self.stream.is_none() {
            return Err(ClientError::Closed);
        }
        if payload.len() > self.limits.max_payload_bytes || topic.is_empty() || topic.len() > 255 {
            return Err(ClientError::Protocol("publish exceeds wire bounds"));
        }
        let ttl = u32::try_from(options.ttl.as_millis())
            .map_err(|_| ClientError::Protocol("TTL exceeds wire bound"))?;
        if ttl == 0 || options.producer_id == 0 || options.log_incarnation == 0 {
            return Err(ClientError::Protocol("Q1 identity and TTL must be nonzero"));
        }
        let mut frame = self.request(FrameType::Publish)?;
        frame.fields = vec![
            Field::required(f::TOPIC_NAME, topic.as_bytes().to_vec()),
            Field::required(f::PARTITION_KEY, []),
            Field::required(f::QOS, [1]),
            Field::required(f::TTL_MS, ttl.to_be_bytes()),
            Field::required(f::MESSAGE_ID, options.message_id.to_be_bytes()),
            Field::required(f::PRODUCER_ID, options.producer_id.to_be_bytes()),
            Field::required(f::PRODUCER_EPOCH, options.producer_epoch.to_be_bytes()),
            Field::required(f::PRODUCER_SEQUENCE, options.sequence.to_be_bytes()),
            Field::required(f::PARTITION_ID, options.partition_id.to_be_bytes()),
            Field::required(f::LOG_INCAR, options.log_incarnation.to_be_bytes()),
            Field::required(f::OWNERSHIP_EPOCH, options.ownership_epoch.to_be_bytes()),
        ];
        frame.payload = payload.to_vec();
        rovqet_protocol::encode(&frame, self.limits)
            .map_err(|_| ClientError::Protocol("invalid Q1 publish"))?;
        let result = match self.exchange(frame, FrameType::PubAck).await {
            Ok(result) => result,
            Err(ClientError::Remote {
                code: Some(ErrorCode::OutcomeUnknown),
                ..
            }) => {
                self.stream = None;
                return Err(ClientError::OutcomeUnknown);
            }
            Err(remote @ ClientError::Remote { .. }) => return Err(remote),
            Err(_) => {
                self.stream = None;
                return Err(ClientError::OutcomeUnknown);
            }
        };
        let decoded = (|| {
            if result.field(f::QOS) != Some(&[1][..])
                || result.field(f::PUB_OUTCOME) != Some(&[2][..])
                || u32_field(&result, f::PARTITION_ID)? != options.partition_id
            {
                return Err(ClientError::OutcomeUnknown);
            }
            let incarnation = u128::from_be_bytes(
                required(&result, f::LOG_INCAR)?
                    .try_into()
                    .map_err(|_| ClientError::OutcomeUnknown)?,
            );
            if incarnation != options.log_incarnation {
                return Err(ClientError::OutcomeUnknown);
            }
            Ok(PublishOutcome::CommittedLocal(DurableOffset {
                log_incarnation: incarnation,
                sequence: u64_field(&result, f::OFFSET)?,
            }))
        })();
        match decoded {
            Ok(outcome) => Ok(outcome),
            Err(_) => {
                self.stream = None;
                Err(ClientError::OutcomeUnknown)
            }
        }
    }

    pub async fn subscribe(&mut self, topic: &str) -> Result<u64, ClientError> {
        self.subscribe_with_credit(
            topic,
            self.config.delivery_credit_messages,
            self.config.delivery_credit_bytes,
        )
        .await
    }
    /// Register an exact-topic subscription with an explicit bounded credit window.
    /// Both initial values may be zero; add_credit then enables delivery.
    pub async fn subscribe_with_credit(
        &mut self,
        topic: &str,
        messages: u32,
        bytes: u32,
    ) -> Result<u64, ClientError> {
        if topic.is_empty() || topic.len() > 255 {
            return Err(ClientError::Protocol("invalid topic length"));
        }
        if messages > 128 || bytes > 1024 * 1024 {
            return Err(ClientError::InvalidFlowControl);
        }
        if self.subscriptions.len() >= self.config.max_pending_messages {
            return Err(ClientError::ResourceExhausted);
        }
        let mut frame = self.request(FrameType::Subscribe)?;
        frame.fields = vec![
            Field::required(f::TOPIC_NAME, topic.as_bytes().to_vec()),
            Field::optional(f::FLOW_CREDIT_MESSAGES, messages.to_be_bytes()),
            Field::optional(f::FLOW_CREDIT_BYTES, bytes.to_be_bytes()),
        ];
        let result = self.exchange(frame, FrameType::SubAck).await?;
        let id = match credit_ack(&result, messages, bytes) {
            Ok(id) => id,
            Err(error) => {
                self.stream = None;
                return Err(error);
            }
        };
        if self.subscriptions.contains_key(&id) {
            self.stream = None;
            return Err(ClientError::Protocol("duplicate subscription id"));
        }
        self.subscriptions.insert(
            id,
            SubscriptionState {
                topic: topic.into(),
                initial_messages: messages,
                initial_bytes: bytes,
            },
        );
        Ok(id)
    }
    /// Add a delta to an existing subscription's remaining delivery credits.
    /// The server rejects increments that would exceed its bounded window.
    pub async fn add_credit(
        &mut self,
        id: u64,
        messages: u32,
        bytes: u32,
    ) -> Result<(), ClientError> {
        if !self.subscriptions.contains_key(&id)
            || (messages == 0 && bytes == 0)
            || messages > 128
            || bytes > 1024 * 1024
        {
            return Err(ClientError::InvalidFlowControl);
        }
        let mut frame = self.request(FrameType::FlowCredit)?;
        frame.fields = vec![
            Field::required(f::SUBSCRIPTION_ID, id.to_be_bytes()),
            Field::required(f::FLOW_CREDIT_MESSAGES, messages.to_be_bytes()),
            Field::required(f::FLOW_CREDIT_BYTES, bytes.to_be_bytes()),
        ];
        let result = self.exchange(frame, FrameType::FlowCredit).await?;
        match credit_ack(&result, messages, bytes) {
            Ok(ack_id) if ack_id == id => {}
            _ => {
                self.stream = None;
                return Err(ClientError::Protocol("credit acknowledgment mismatch"));
            }
        }
        Ok(())
    }
    async fn replenish(&mut self) -> Result<(), ClientError> {
        while let Some((&id, &(messages, bytes))) = self.owed_credit.first_key_value() {
            // A failed/uncertain increment closes the session rather than being replayed.
            if let Err(error) = self.add_credit(id, messages, bytes).await {
                self.stream = None;
                return Err(error);
            }
            self.owed_credit.remove(&id);
        }
        Ok(())
    }
    fn consumed(&mut self, delivery: &Delivery) -> Result<(), ClientError> {
        if !self.config.auto_replenish {
            return Ok(());
        }
        let id = delivery
            .subscription_id
            .ok_or(ClientError::Protocol("missing subscription identity"))?;
        if self.subscriptions.contains_key(&id) {
            let owed = self.owed_credit.entry(id).or_default();
            owed.0 = owed
                .0
                .checked_add(1)
                .ok_or(ClientError::InvalidFlowControl)?;
            owed.1 = owed
                .1
                .checked_add(delivery.wire_bytes)
                .ok_or(ClientError::InvalidFlowControl)?;
        }
        Ok(())
    }
    pub async fn unsubscribe(&mut self, id: u64) -> Result<(), ClientError> {
        let mut frame = self.request(FrameType::Unsubscribe)?;
        frame
            .fields
            .push(Field::required(f::SUBSCRIPTION_ID, id.to_be_bytes()));
        let result = self.exchange(frame, FrameType::SubAck).await?;
        if u64_field(&result, f::SUBSCRIPTION_ID)? != id {
            return Err(ClientError::Protocol("unsubscribe id mismatch"));
        }
        self.subscriptions.remove(&id);
        self.owed_credit.remove(&id);
        Ok(())
    }
    pub async fn receive(&mut self) -> Result<Delivery, ClientError> {
        self.replenish().await?;
        if let Some(delivery) = self.pending.pop_front() {
            self.pending_bytes -= delivery.payload.len() + delivery.topic.len();
            self.consumed(&delivery)?;
            return Ok(delivery);
        }
        let frame = self.incoming(None).await?;
        if frame.frame_type == FrameType::Error {
            return Err(remote(&frame)?);
        }
        if frame.frame_type != FrameType::Message {
            return Err(ClientError::Protocol("expected delivery"));
        }
        let delivery = delivery(frame)?;
        self.consumed(&delivery)?;
        Ok(delivery)
    }
    /// Reconnects and recreates ephemeral subscriptions. Returns old/new IDs.
    /// No in-flight publication is replayed. Connection attempts alone are retried.
    pub async fn reconnect(&mut self) -> Result<Vec<(u64, u64)>, ClientError> {
        // Release the old session before competing for a bounded connection slot.
        self.stream = None;
        self.pending.clear();
        self.pending_bytes = 0;
        self.owed_credit.clear();
        let mut replacement = None;
        let mut error = ClientError::Closed;
        for attempt in 0..3 {
            match Self::connect(self.config.clone()).await {
                Ok(c) => {
                    replacement = Some(c);
                    break;
                }
                Err(e) => error = e,
            }
            if attempt < 2 {
                let advised = match &error {
                    ClientError::Remote { retry_after, .. } => *retry_after,
                    _ => None,
                };
                let jitter = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .subsec_nanos() as u64
                    % 50;
                let backoff = Duration::from_millis((50u64 << attempt) + jitter);
                tokio::time::sleep(advised.unwrap_or_default().max(backoff)).await;
            }
        }
        let mut client = replacement.ok_or(error)?;
        let mut mappings = Vec::new();
        for (&old, subscription) in &self.subscriptions {
            mappings.push((
                old,
                client
                    .subscribe_with_credit(
                        &subscription.topic,
                        subscription.initial_messages,
                        subscription.initial_bytes,
                    )
                    .await?,
            ));
        }
        *self = client;
        Ok(mappings)
    }
    pub async fn close(&mut self) -> Result<(), ClientError> {
        let frame = self.request(FrameType::Close)?;
        let result = self.send(&frame).await;
        self.stream = None;
        self.pending.clear();
        self.pending_bytes = 0;
        self.subscriptions.clear();
        self.owed_credit.clear();
        result
    }
}
fn required(frame: &Frame, id: u64) -> Result<&[u8], ClientError> {
    frame
        .field(id)
        .ok_or(ClientError::Protocol("missing response field"))
}
fn u64_field(frame: &Frame, id: u64) -> Result<u64, ClientError> {
    Ok(u64::from_be_bytes(
        required(frame, id)?
            .try_into()
            .map_err(|_| ClientError::Protocol("invalid integer field"))?,
    ))
}
fn u32_field(frame: &Frame, id: u64) -> Result<u32, ClientError> {
    Ok(u32::from_be_bytes(
        required(frame, id)?
            .try_into()
            .map_err(|_| ClientError::Protocol("invalid credit field"))?,
    ))
}
fn credit_ack(frame: &Frame, messages: u32, bytes: u32) -> Result<u64, ClientError> {
    let id = u64_field(frame, f::SUBSCRIPTION_ID)?;
    if u32_field(frame, f::FLOW_CREDIT_MESSAGES)? != messages
        || u32_field(frame, f::FLOW_CREDIT_BYTES)? != bytes
    {
        return Err(ClientError::Protocol("credit acknowledgment mismatch"));
    }
    Ok(id)
}
fn remote(frame: &Frame) -> Result<ClientError, ClientError> {
    let code = u16::from_be_bytes(
        required(frame, f::ERROR_CODE)?
            .try_into()
            .map_err(|_| ClientError::Protocol("invalid error code"))?,
    );
    Ok(ClientError::Remote {
        code: ErrorCode::try_from(code).ok(),
        raw_code: code,
        retryable: required(frame, f::ERROR_RETRYABLE)? == [1],
        retry_after: frame
            .field(f::RETRY_AFTER_MS)
            .map(|b| {
                b.try_into()
                    .map(|b| Duration::from_millis(u32::from_be_bytes(b) as u64))
                    .map_err(|_| ClientError::Protocol("invalid retry delay"))
            })
            .transpose()?,
    })
}
fn delivery(frame: Frame) -> Result<Delivery, ClientError> {
    let wire_bytes = u32::try_from(
        rovqet_protocol::encode(&frame, Limits::default())
            .map_err(|_| ClientError::Protocol("invalid delivery"))?
            .len(),
    )
    .map_err(|_| ClientError::Protocol("oversize delivery"))?;
    if frame.field(f::QOS) != Some(&[0][..]) {
        return Err(ClientError::UnsupportedGuarantee);
    }
    let topic = std::str::from_utf8(required(&frame, f::TOPIC_NAME)?)
        .map_err(|_| ClientError::Protocol("invalid topic"))?
        .to_string();
    let message_id = u128::from_be_bytes(
        required(&frame, f::MESSAGE_ID)?
            .try_into()
            .map_err(|_| ClientError::Protocol("invalid message id"))?,
    );
    let subscription_id = frame
        .field(f::SUBSCRIPTION_ID)
        .map(|_| u64_field(&frame, f::SUBSCRIPTION_ID))
        .transpose()?;
    Ok(Delivery {
        topic,
        subscription_id,
        message_id,
        payload: frame.payload,
        wire_bytes,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::net::TcpListener;

    async fn handshake(stream: &mut TcpStream) {
        handshake_settings(stream, None).await;
    }
    async fn handshake_settings(stream: &mut TcpStream, heartbeat_ms: Option<u32>) {
        let hello = read_frame(stream, Limits::default())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(hello.frame_type, FrameType::Hello);
        let mut welcome = Frame::new(FrameType::Welcome, hello.request_id);
        welcome.fields = vec![
            Field::required(f::PROTOCOL_MINOR, 0u16.to_be_bytes()),
            Field::required(f::MAX_FRAME_BYTES, 1_048_576u32.to_be_bytes()),
            Field::required(f::SESSION_ID, 1u128.to_be_bytes()),
        ];
        if let Some(ms) = heartbeat_ms {
            welcome
                .fields
                .push(Field::optional(f::HEARTBEAT_MS, ms.to_be_bytes()));
        }
        write_frame(stream, &welcome, Limits::default())
            .await
            .unwrap();
        let auth = read_frame(stream, Limits::default())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(auth.field(f::AUTH_DATA), Some(&b"dev"[..]));
        let mut ok = Frame::new(FrameType::AuthOk, auth.request_id);
        ok.fields
            .push(Field::required(f::PRINCIPAL_ID, 1u64.to_be_bytes()));
        write_frame(stream, &ok, Limits::default()).await.unwrap();
    }
    #[tokio::test]
    async fn socket_loss_after_publish_is_unknown() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let config = ClientConfig {
            endpoint: listener.local_addr().unwrap(),
            ..Default::default()
        };
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            handshake(&mut stream).await;
            let publish = read_frame(&mut stream, Limits::default())
                .await
                .unwrap()
                .unwrap();
            assert_eq!(publish.frame_type, FrameType::Publish);
        });
        let mut client = Client::connect(config).await.unwrap();
        assert!(matches!(
            client
                .publish("demo", b"x", PublishOptions::default())
                .await,
            Err(ClientError::OutcomeUnknown)
        ));
        server.await.unwrap();
    }
    #[tokio::test]
    async fn typed_remote_error_and_no_qos_downgrade() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let config = ClientConfig {
            endpoint: listener.local_addr().unwrap(),
            ..Default::default()
        };
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            handshake(&mut stream).await;
            let request = read_frame(&mut stream, Limits::default())
                .await
                .unwrap()
                .unwrap();
            let mut error = Frame::new(FrameType::Error, request.request_id);
            error.fields = vec![
                Field::required(
                    f::ERROR_CODE,
                    (ErrorCode::ResourceExhausted as u16).to_be_bytes(),
                ),
                Field::required(f::ERROR_RETRYABLE, [1]),
            ];
            write_frame(&mut stream, &error, Limits::default())
                .await
                .unwrap();
        });
        let mut client = Client::connect(config).await.unwrap();
        let options = PublishOptions {
            qos: QoS::Q1,
            ..Default::default()
        };
        assert!(matches!(
            client.publish("demo", b"x", options).await,
            Err(ClientError::UnsupportedGuarantee)
        ));
        assert!(matches!(
            client
                .publish("demo", b"x", PublishOptions::default())
                .await,
            Err(ClientError::Remote {
                code: Some(ErrorCode::ResourceExhausted),
                retryable: true,
                ..
            })
        ));
        server.await.unwrap();
    }
    #[tokio::test]
    async fn pending_delivery_overflow_closes_connection() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let config = ClientConfig {
            endpoint: listener.local_addr().unwrap(),
            max_pending_messages: 1,
            ..Default::default()
        };
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            handshake(&mut stream).await;
            let request = read_frame(&mut stream, Limits::default())
                .await
                .unwrap()
                .unwrap();
            assert_eq!(request.frame_type, FrameType::Publish);
            for id in 1u128..=2 {
                let mut message = Frame::new(FrameType::Message, 0);
                message.fields = vec![
                    Field::required(f::TOPIC_NAME, b"demo".to_vec()),
                    Field::required(f::QOS, [0]),
                    Field::required(f::MESSAGE_ID, id.to_be_bytes()),
                ];
                message.payload = vec![7];
                write_frame(&mut stream, &message, Limits::default())
                    .await
                    .unwrap();
            }
        });
        let mut client = Client::connect(config).await.unwrap();
        assert!(matches!(
            client
                .publish("demo", b"x", PublishOptions::default())
                .await,
            Err(ClientError::OutcomeUnknown)
        ));
        assert!(client.stream.is_none());
        assert_eq!(client.pending.len(), 1);
        server.await.unwrap();
    }
    #[test]
    fn unknown_remote_code_is_preserved() {
        let mut error = Frame::new(FrameType::Error, 1);
        error.fields = vec![
            Field::required(f::ERROR_CODE, 999u16.to_be_bytes()),
            Field::required(f::ERROR_RETRYABLE, [0]),
        ];
        assert!(matches!(
            remote(&error).unwrap(),
            ClientError::Remote {
                code: None,
                raw_code: 999,
                retryable: false,
                retry_after: None
            }
        ));
        assert!(ClientConfig::default().endpoint.ip().is_loopback());
    }
    #[test]
    fn remote_retry_delay_is_preserved() {
        let mut frame = Frame::new(FrameType::Error, 1);
        frame.fields = vec![
            Field::required(f::ERROR_CODE, (ErrorCode::RetryAfter as u16).to_be_bytes()),
            Field::required(f::ERROR_RETRYABLE, [1]),
            Field::optional(f::RETRY_AFTER_MS, 250u32.to_be_bytes()),
        ];
        assert!(matches!(remote(&frame).unwrap(),ClientError::Remote {
            retry_after:Some(delay), .. } if delay==Duration::from_millis(250)));
    }
    async fn accept_subscription(stream: &mut TcpStream, id: u64) -> Frame {
        let request = read_frame(stream, Limits::default())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(request.frame_type, FrameType::Subscribe);
        let mut ack = Frame::new(FrameType::SubAck, request.request_id);
        ack.fields = vec![
            Field::required(f::SUBSCRIPTION_ID, id.to_be_bytes()),
            Field::optional(
                f::FLOW_CREDIT_MESSAGES,
                required(&request, f::FLOW_CREDIT_MESSAGES).unwrap(),
            ),
            Field::optional(
                f::FLOW_CREDIT_BYTES,
                required(&request, f::FLOW_CREDIT_BYTES).unwrap(),
            ),
        ];
        write_frame(stream, &ack, Limits::default()).await.unwrap();
        request
    }
    fn message(id: u128) -> Frame {
        let mut frame = Frame::new(FrameType::Message, 0);
        frame.fields = vec![
            Field::required(f::TOPIC_NAME, b"demo".to_vec()),
            Field::required(f::QOS, [0]),
            Field::required(f::MESSAGE_ID, id.to_be_bytes()),
            Field::optional(f::SUBSCRIPTION_ID, 7u64.to_be_bytes()),
        ];
        frame.payload = vec![id as u8];
        frame
    }
    #[tokio::test]
    async fn consumed_delivery_credit_is_replenished_before_next_receive() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let config = ClientConfig {
            endpoint: listener.local_addr().unwrap(),
            delivery_credit_messages: 1,
            delivery_credit_bytes: 1024,
            ..Default::default()
        };
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            handshake(&mut stream).await;
            let sub = accept_subscription(&mut stream, 7).await;
            assert_eq!(u32_field(&sub, f::FLOW_CREDIT_MESSAGES).unwrap(), 1);
            let first = message(1);
            let expected = encode_len(&first);
            write_frame(&mut stream, &first, Limits::default())
                .await
                .unwrap();
            let credit = read_frame(&mut stream, Limits::default())
                .await
                .unwrap()
                .unwrap();
            assert_eq!(credit.frame_type, FrameType::FlowCredit);
            assert_eq!(u64_field(&credit, f::SUBSCRIPTION_ID).unwrap(), 7);
            assert_eq!(u32_field(&credit, f::FLOW_CREDIT_MESSAGES).unwrap(), 1);
            assert_eq!(u32_field(&credit, f::FLOW_CREDIT_BYTES).unwrap(), expected);
            write_frame(&mut stream, &credit, Limits::default())
                .await
                .unwrap();
            write_frame(&mut stream, &message(2), Limits::default())
                .await
                .unwrap();
        });
        let mut client = Client::connect(config).await.unwrap();
        assert_eq!(client.subscribe("demo").await.unwrap(), 7);
        let first = client.receive().await.unwrap();
        assert_eq!(first.payload, [1]);
        assert_eq!(client.owed_credit.get(&7), Some(&(1, first.wire_bytes)));
        assert_eq!(client.receive().await.unwrap().payload, [2]);
        server.await.unwrap();
    }
    fn encode_len(frame: &Frame) -> u32 {
        rovqet_protocol::encode(frame, Limits::default())
            .unwrap()
            .len() as u32
    }
    #[tokio::test]
    async fn manual_credit_rejects_invalid_local_operations_and_surfaces_quota_error() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let config = ClientConfig {
            endpoint: listener.local_addr().unwrap(),
            auto_replenish: false,
            ..Default::default()
        };
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            handshake(&mut stream).await;
            let sub = accept_subscription(&mut stream, 7).await;
            assert_eq!(u32_field(&sub, f::FLOW_CREDIT_MESSAGES).unwrap(), 0);
            let credit = read_frame(&mut stream, Limits::default())
                .await
                .unwrap()
                .unwrap();
            assert_eq!(credit.frame_type, FrameType::FlowCredit);
            let mut error = Frame::new(FrameType::Error, credit.request_id);
            error.fields = vec![
                Field::required(
                    f::ERROR_CODE,
                    (ErrorCode::QuotaExceeded as u16).to_be_bytes(),
                ),
                Field::required(f::ERROR_RETRYABLE, [0]),
            ];
            write_frame(&mut stream, &error, Limits::default())
                .await
                .unwrap();
        });
        let mut client = Client::connect(config).await.unwrap();
        client.subscribe_with_credit("demo", 0, 0).await.unwrap();
        assert!(matches!(
            client.add_credit(7, 0, 0).await,
            Err(ClientError::InvalidFlowControl)
        ));
        assert!(matches!(
            client.add_credit(99, 1, 100).await,
            Err(ClientError::InvalidFlowControl)
        ));
        assert!(matches!(
            client.add_credit(7, 1, 100).await,
            Err(ClientError::Remote {
                code: Some(ErrorCode::QuotaExceeded),
                ..
            })
        ));
        server.await.unwrap();
    }
    #[tokio::test]
    async fn reconnect_closes_old_socket_and_recreates_each_window_once() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let config = ClientConfig {
            endpoint: listener.local_addr().unwrap(),
            ..Default::default()
        };
        let server = tokio::spawn(async move {
            let (mut old, _) = listener.accept().await.unwrap();
            handshake(&mut old).await;
            accept_subscription(&mut old, 7).await;
            assert!(
                read_frame(&mut old, Limits::default())
                    .await
                    .unwrap()
                    .is_none()
            );
            let (mut stream, _) = listener.accept().await.unwrap();
            handshake(&mut stream).await;
            let request = accept_subscription(&mut stream, 8).await;
            assert_eq!(u32_field(&request, f::FLOW_CREDIT_MESSAGES).unwrap(), 0);
            assert_eq!(u32_field(&request, f::FLOW_CREDIT_BYTES).unwrap(), 0);
            let close = read_frame(&mut stream, Limits::default())
                .await
                .unwrap()
                .unwrap();
            assert_eq!(close.frame_type, FrameType::Close);
        });
        let mut client = Client::connect(config).await.unwrap();
        client.subscribe_with_credit("demo", 0, 0).await.unwrap();
        assert_eq!(client.reconnect().await.unwrap(), [(7, 8)]);
        assert_eq!(client.subscriptions.len(), 1);
        client.close().await.unwrap();
        server.await.unwrap();
    }
    #[tokio::test]
    async fn malformed_credit_ack_closes_the_session_without_tracking_an_orphan() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let config = ClientConfig {
            endpoint: listener.local_addr().unwrap(),
            ..Default::default()
        };
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            handshake(&mut stream).await;
            let sub = read_frame(&mut stream, Limits::default())
                .await
                .unwrap()
                .unwrap();
            let mut ack = Frame::new(FrameType::SubAck, sub.request_id);
            ack.fields
                .push(Field::required(f::SUBSCRIPTION_ID, 7u64.to_be_bytes()));
            write_frame(&mut stream, &ack, Limits::default())
                .await
                .unwrap();
            assert!(
                read_frame(&mut stream, Limits::default())
                    .await
                    .unwrap()
                    .is_none()
            );
        });
        let mut client = Client::connect(config).await.unwrap();
        assert!(matches!(
            client.subscribe("demo").await,
            Err(ClientError::Protocol(_))
        ));
        assert!(client.stream.is_none());
        assert!(client.subscriptions.is_empty());
        server.await.unwrap();
    }
    #[tokio::test]
    async fn receive_sends_negotiated_heartbeat_while_waiting_for_credit_limited_delivery() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let config = ClientConfig {
            endpoint: listener.local_addr().unwrap(),
            ..Default::default()
        };
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            handshake_settings(&mut stream, Some(40)).await;
            accept_subscription(&mut stream, 7).await;
            let ping = timeout(
                Duration::from_secs(1),
                read_frame(&mut stream, Limits::default()),
            )
            .await
            .unwrap()
            .unwrap()
            .unwrap();
            assert_eq!(ping.frame_type, FrameType::Ping);
            let mut pong = ping;
            pong.frame_type = FrameType::Pong;
            write_frame(&mut stream, &pong, Limits::default())
                .await
                .unwrap();
            write_frame(&mut stream, &message(1), Limits::default())
                .await
                .unwrap();
        });
        let mut client = Client::connect(config).await.unwrap();
        client.subscribe("demo").await.unwrap();
        assert_eq!(client.receive().await.unwrap().payload, [1]);
        server.await.unwrap();
    }
    fn q1_options() -> Q1PublishOptions {
        Q1PublishOptions {
            producer_id: 77,
            producer_epoch: 3,
            sequence: 4,
            message_id: 88,
            partition_id: 2,
            log_incarnation: 99,
            ownership_epoch: 7,
            ttl: Duration::from_secs(1),
        }
    }
    fn q1_ack(request: u64) -> Frame {
        let mut ack = Frame::new(FrameType::PubAck, request);
        ack.fields = vec![
            Field::required(f::QOS, [1]),
            Field::required(f::PUB_OUTCOME, [2]),
            Field::required(f::PARTITION_ID, 2_u32.to_be_bytes()),
            Field::required(f::LOG_INCAR, 99_u128.to_be_bytes()),
            Field::required(f::OFFSET, 10_u64.to_be_bytes()),
        ];
        ack
    }
    #[tokio::test]
    async fn q1_lost_reply_reconnect_reuses_original_token_and_payload() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let config = ClientConfig {
            endpoint: listener.local_addr().unwrap(),
            ..Default::default()
        };
        let server = tokio::spawn(async move {
            let (mut first, _) = listener.accept().await.unwrap();
            handshake(&mut first).await;
            let original = read_frame(&mut first, Limits::default())
                .await
                .unwrap()
                .unwrap();
            assert_eq!(original.field(f::QOS), Some(&[1][..]));
            assert_eq!(
                original.field(f::PRODUCER_ID),
                Some(&77_u128.to_be_bytes()[..])
            );
            assert_eq!(
                original.field(f::PRODUCER_EPOCH),
                Some(&3_u64.to_be_bytes()[..])
            );
            assert_eq!(
                original.field(f::PRODUCER_SEQUENCE),
                Some(&4_u64.to_be_bytes()[..])
            );
            assert_eq!(
                original.field(f::MESSAGE_ID),
                Some(&88_u128.to_be_bytes()[..])
            );
            assert_eq!(
                original.field(f::PARTITION_ID),
                Some(&2_u32.to_be_bytes()[..])
            );
            assert_eq!(
                original.field(f::LOG_INCAR),
                Some(&99_u128.to_be_bytes()[..])
            );
            assert_eq!(
                original.field(f::OWNERSHIP_EPOCH),
                Some(&7_u64.to_be_bytes()[..])
            );
            drop(first);
            let (mut second, _) = listener.accept().await.unwrap();
            handshake(&mut second).await;
            let retry = read_frame(&mut second, Limits::default())
                .await
                .unwrap()
                .unwrap();
            assert_eq!(retry.fields, original.fields);
            assert_eq!(retry.payload, original.payload);
            write_frame(&mut second, &q1_ack(retry.request_id), Limits::default())
                .await
                .unwrap();
        });
        let mut client = Client::connect(config).await.unwrap();
        let options = q1_options();
        assert!(matches!(
            client
                .publish_q1("durable", b"same-payload", options.clone())
                .await,
            Err(ClientError::OutcomeUnknown)
        ));
        assert!(client.stream.is_none());
        client.reconnect().await.unwrap();
        assert_eq!(
            client
                .publish_q1("durable", b"same-payload", options)
                .await
                .unwrap(),
            PublishOutcome::CommittedLocal(DurableOffset {
                log_incarnation: 99,
                sequence: 10
            })
        );
        server.await.unwrap();
    }
    #[tokio::test]
    async fn q1_invalid_ack_identity_class_correlation_or_encoding_is_unknown() {
        for fault in 0..5 {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let config = ClientConfig {
                endpoint: listener.local_addr().unwrap(),
                ..Default::default()
            };
            let server = tokio::spawn(async move {
                let (mut stream, _) = listener.accept().await.unwrap();
                handshake(&mut stream).await;
                let request = read_frame(&mut stream, Limits::default())
                    .await
                    .unwrap()
                    .unwrap();
                let mut ack = q1_ack(request.request_id);
                match fault {
                    0 => {
                        ack.fields[2] = Field::required(f::PARTITION_ID, 3_u32.to_be_bytes());
                    }
                    1 => {
                        ack.fields[3] = Field::required(f::LOG_INCAR, 100_u128.to_be_bytes());
                    }
                    2 => {
                        ack.fields = vec![
                            Field::required(f::QOS, [0]),
                            Field::required(f::PUB_OUTCOME, [1]),
                        ];
                    }
                    3 => {
                        ack.request_id += 1;
                    }
                    4 => {}
                    _ => unreachable!(),
                }
                let mut bytes = rovqet_protocol::encode(&ack, Limits::default()).unwrap();
                if fault == 4 {
                    bytes[2] = 2;
                }
                tokio::io::AsyncWriteExt::write_all(&mut stream, &bytes)
                    .await
                    .unwrap();
                // The client closes the ambiguous connection instead of issuing Q0 or a new token.
                match read_frame(&mut stream, Limits::default()).await {
                    Ok(None) => {}
                    Err(error) if error.kind() == io::ErrorKind::ConnectionReset => {}
                    other => panic!("uncertain client must close without another frame: {other:?}"),
                }
            });
            let mut client = Client::connect(config).await.unwrap();
            assert!(
                matches!(
                    client.publish_q1("durable", b"body", q1_options()).await,
                    Err(ClientError::OutcomeUnknown)
                ),
                "fault {fault}"
            );
            assert!(client.stream.is_none());
            assert!(matches!(
                client.publish_q1("durable", b"body", q1_options()).await,
                Err(ClientError::Closed)
            ));
            server.await.unwrap();
        }
    }
    #[tokio::test]
    async fn q1_server_unknown_is_unresolved_and_other_errors_remain_typed() {
        for code in [
            ErrorCode::OutcomeUnknown,
            ErrorCode::SequenceConflict,
            ErrorCode::StorageUnavailable,
        ] {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let config = ClientConfig {
                endpoint: listener.local_addr().unwrap(),
                ..Default::default()
            };
            let server = tokio::spawn(async move {
                let (mut stream, _) = listener.accept().await.unwrap();
                handshake(&mut stream).await;
                let request = read_frame(&mut stream, Limits::default())
                    .await
                    .unwrap()
                    .unwrap();
                let error = rovqet_transport::error_frame(request.request_id, code);
                write_frame(&mut stream, &error, Limits::default())
                    .await
                    .unwrap();
            });
            let mut client = Client::connect(config).await.unwrap();
            let result = client.publish_q1("durable", b"body", q1_options()).await;
            if code == ErrorCode::OutcomeUnknown {
                assert!(matches!(result, Err(ClientError::OutcomeUnknown)));
                assert!(client.stream.is_none());
            } else {
                assert!(
                    matches!(result, Err(ClientError::Remote { code: Some(actual), .. }) if actual == code)
                );
            }
            server.await.unwrap();
        }
    }
    #[tokio::test]
    async fn q1_reply_timeout_is_unknown_and_does_not_resubmit() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let config = ClientConfig {
            endpoint: listener.local_addr().unwrap(),
            request_timeout: Duration::from_millis(50),
            ..Default::default()
        };
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            handshake(&mut stream).await;
            let request = read_frame(&mut stream, Limits::default())
                .await
                .unwrap()
                .unwrap();
            assert_eq!(request.field(f::QOS), Some(&[1][..]));
            // Hold the reply until the client's own deadline closes its socket.
            assert!(
                read_frame(&mut stream, Limits::default())
                    .await
                    .unwrap()
                    .is_none()
            );
        });
        let mut client = Client::connect(config).await.unwrap();
        assert!(matches!(
            client.publish_q1("durable", b"body", q1_options()).await,
            Err(ClientError::OutcomeUnknown)
        ));
        assert!(client.stream.is_none());
        server.await.unwrap();
    }
    #[tokio::test]
    async fn q1_invalid_local_options_are_not_sent() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let config = ClientConfig {
            endpoint: listener.local_addr().unwrap(),
            ..Default::default()
        };
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            handshake(&mut stream).await;
            assert_eq!(
                read_frame(&mut stream, Limits::default())
                    .await
                    .unwrap()
                    .unwrap()
                    .frame_type,
                FrameType::Close
            );
        });
        let mut client = Client::connect(config).await.unwrap();
        for fault in 0..3 {
            let mut options = q1_options();
            match fault {
                0 => options.producer_id = 0,
                1 => options.log_incarnation = 0,
                _ => options.ttl = Duration::ZERO,
            }
            assert!(matches!(
                client.publish_q1("durable", b"body", options).await,
                Err(ClientError::Protocol(_))
            ));
        }
        client.close().await.unwrap();
        server.await.unwrap();
    }
    fn read_options() -> ReadOptions {
        ReadOptions {
            partition: PartitionRef {
                tenant_id: 1,
                topic_id: 3,
                partition_id: 0,
            },
            from: DurableOffset {
                log_incarnation: 42,
                sequence: 0,
            },
            max_messages: 2,
            max_bytes: 2048,
            wait: Duration::ZERO,
        }
    }
    fn read_scope() -> rovqet_protocol::replay::ReadScope {
        rovqet_protocol::replay::ReadScope {
            tenant: 1,
            topic: 3,
            partition: 0,
            incarnation: 42,
        }
    }
    #[tokio::test]
    async fn standalone_read_rejects_incomplete_malformed_uncorrelated_and_overlimit_batches() {
        use rovqet_protocol::replay::{self, ReadStatus};
        for case in 0..13 {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let config = ClientConfig {
                endpoint: listener.local_addr().unwrap(),
                ..Default::default()
            };
            let server = tokio::spawn(async move {
                let (mut stream, _) = listener.accept().await.unwrap();
                handshake(&mut stream).await;
                let request = read_frame(&mut stream, Limits::default())
                    .await
                    .unwrap()
                    .unwrap();
                assert_eq!(request.frame_type, FrameType::ReadFetch);
                assert!(request.field(f::GROUP_ID).is_none());
                let mut record = replay::record(
                    request.request_id,
                    read_scope(),
                    0,
                    100,
                    b"event-0".to_vec(),
                );
                if case == 3 {
                    record.request_id += 1;
                }
                if case == 4 {
                    record
                        .fields
                        .iter_mut()
                        .find(|v| v.id == f::LOG_INCAR)
                        .unwrap()
                        .value = 43u128.to_be_bytes().to_vec();
                }
                if case == 5 {
                    record
                        .fields
                        .iter_mut()
                        .find(|v| v.id == f::OFFSET)
                        .unwrap()
                        .value = 2u64.to_be_bytes().to_vec();
                }
                if case == 8 {
                    record.fields.push(Field::optional(127, vec![1; 4]));
                }
                if case == 10 {
                    record
                        .fields
                        .iter_mut()
                        .find(|v| v.id == f::TENANT_ID)
                        .unwrap()
                        .value = 2u64.to_be_bytes().to_vec();
                }
                let _ = write_frame(&mut stream, &record, Limits::default()).await;
                if case == 12 {
                    let second = replay::record(
                        request.request_id,
                        read_scope(),
                        2,
                        101,
                        b"event-1".to_vec(),
                    );
                    let _ = write_frame(&mut stream, &second, Limits::default()).await;
                }
                if case == 1 {
                    return;
                } // complete record followed by missing terminal frame
                let mut end = replay::end(request.request_id, read_scope(), 2, ReadStatus::End);
                if case == 2 {
                    end.request_id += 1;
                }
                if case == 6 {
                    // A terminal continuation must not point at or before the last record.
                    end.fields
                        .iter_mut()
                        .find(|v| v.id == f::OFFSET)
                        .unwrap()
                        .value = 0u64.to_be_bytes().to_vec();
                }
                if case == 7 {
                    end = replay::end(request.request_id, read_scope(), 2, ReadStatus::CountLimit);
                }
                if case == 9 {
                    let bytes = rovqet_protocol::encode(&end, Limits::default()).unwrap();
                    let _ = tokio::io::AsyncWriteExt::write_all(&mut stream, &bytes[..20]).await;
                    return;
                }
                if case == 11 {
                    end = replay::end(request.request_id, read_scope(), 2, ReadStatus::WaitExpired);
                }
                let _ = write_frame(&mut stream, &end, Limits::default()).await;
            });
            let mut client = Client::connect(config).await.unwrap();
            let mut options = read_options();
            if case == 8 {
                options.max_bytes = 175;
            }
            if case == 12 {
                options.max_messages = 1;
            }
            let result = client.read_q1("durable", options).await;
            if case == 0 {
                let batch = result.unwrap();
                assert_eq!(batch.records.len(), 1);
                assert_eq!(batch.next.sequence, 2);
                assert_eq!(batch.encoded_bytes, 175);
                assert!(!batch.has_more);
            } else {
                if matches!(case, 1 | 9) {
                    assert!(
                        matches!(result, Err(ReadError::Incomplete)),
                        "case{case} must be incomplete"
                    );
                } else {
                    assert!(result.is_err(), "case{case}");
                }
                assert!(
                    client.stream.is_none(),
                    "case{case} must close uncertain framing"
                );
            }
            server.await.unwrap();
        }
    }
    #[tokio::test]
    async fn cancellation_between_read_records_and_end_closes_the_entire_stream() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let config = ClientConfig {
            endpoint: listener.local_addr().unwrap(),
            ..Default::default()
        };
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            handshake(&mut stream).await;
            let request = read_frame(&mut stream, Limits::default())
                .await
                .unwrap()
                .unwrap();
            let record = rovqet_protocol::replay::record(
                request.request_id,
                read_scope(),
                0,
                100,
                b"event-0".to_vec(),
            );
            write_frame(&mut stream, &record, Limits::default())
                .await
                .unwrap();
            // No READ_END. External cancellation must close even though one full record arrived.
            assert!(
                read_frame(&mut stream, Limits::default())
                    .await
                    .unwrap()
                    .is_none()
            );
        });
        let mut client = Client::connect(config).await.unwrap();
        assert!(
            tokio::time::timeout(
                Duration::from_millis(20),
                client.read_q1("durable", read_options())
            )
            .await
            .is_err()
        );
        assert!(client.stream.is_none());
        assert!(matches!(
            client.read_q1("durable", read_options()).await,
            Err(ReadError::Closed)
        ));
        server.await.unwrap();
    }
    #[tokio::test]
    async fn standalone_read_unsupported_peer_error_is_typed_without_group_fallback() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let config = ClientConfig {
            endpoint: listener.local_addr().unwrap(),
            ..Default::default()
        };
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            handshake(&mut stream).await;
            let request = read_frame(&mut stream, Limits::default())
                .await
                .unwrap()
                .unwrap();
            assert_eq!(request.frame_type, FrameType::ReadFetch);
            let mut error = Frame::new(FrameType::Error, request.request_id);
            error.fields = vec![
                Field::required(f::ERROR_CODE, 3u16.to_be_bytes()),
                Field::required(f::ERROR_RETRYABLE, [0]),
            ];
            write_frame(&mut stream, &error, Limits::default())
                .await
                .unwrap();
        });
        let mut client = Client::connect(config).await.unwrap();
        assert!(matches!(
            client.read_q1("durable", read_options()).await,
            Err(ReadError::Unsupported)
        ));
        server.await.unwrap();
    }

    fn group_options(generation: Option<u64>) -> GroupOptions {
        GroupOptions {
            partition: PartitionRef {
                tenant_id: 1,
                topic_id: 3,
                partition_id: 0,
            },
            incarnation: 42,
            group_id: 9,
            generation,
            max_messages: 8,
            max_bytes: 65536,
            wait: Duration::ZERO,
        }
    }
    fn delivery_frame(request: u64, offset: u64, lease: u128, attempt: u32) -> Frame {
        let mut frame = Frame::new(FrameType::Message, request);
        frame.fields = vec![
            Field::required(f::TENANT_ID, 1u64.to_be_bytes()),
            Field::required(f::TOPIC_ID, 3u64.to_be_bytes()),
            Field::required(f::QOS, [1]),
            Field::required(f::PARTITION_ID, 0u32.to_be_bytes()),
            Field::required(f::LOG_INCAR, 42u128.to_be_bytes()),
            Field::required(f::OFFSET, offset.to_be_bytes()),
            Field::required(f::MESSAGE_ID, (100 + u128::from(offset)).to_be_bytes()),
            Field::required(f::GROUP_ID, 9u64.to_be_bytes()),
            Field::required(f::GROUP_GENERATION, 1u64.to_be_bytes()),
            Field::required(f::LEASE_ID, lease.to_be_bytes()),
            Field::required(f::OWNERSHIP_EPOCH, 7u64.to_be_bytes()),
            Field::required(f::DELIVERY_ATTEMPT, attempt.to_be_bytes()),
            Field::required(f::ACK_DEADLINE_MS, 1000u32.to_be_bytes()),
        ];
        frame.payload = format!("event-{offset}").into_bytes();
        frame
    }
    fn fetch_end_frame(request: u64, offset: u64, group: u64, has_more: u8) -> Frame {
        let mut frame = Frame::new(FrameType::FetchEnd, request);
        frame.fields = vec![
            Field::required(f::TENANT_ID, 1u64.to_be_bytes()),
            Field::required(f::TOPIC_ID, 3u64.to_be_bytes()),
            Field::required(f::QOS, [1]),
            Field::required(f::PARTITION_ID, 0u32.to_be_bytes()),
            Field::required(f::LOG_INCAR, 42u128.to_be_bytes()),
            Field::required(f::GROUP_ID, group.to_be_bytes()),
            Field::required(f::GROUP_GENERATION, 1u64.to_be_bytes()),
            Field::required(f::OFFSET, offset.to_be_bytes()),
            Field::required(f::HAS_MORE, [has_more]),
        ];
        frame
    }
    #[tokio::test]
    async fn group_fetch_requires_a_validated_terminal_and_complete_tokens() {
        for case in 0..6 {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let config = ClientConfig {
                endpoint: listener.local_addr().unwrap(),
                ..Default::default()
            };
            let server = tokio::spawn(async move {
                let (mut stream, _) = listener.accept().await.unwrap();
                handshake(&mut stream).await;
                let request = read_frame(&mut stream, Limits::default())
                    .await
                    .unwrap()
                    .unwrap();
                assert_eq!(request.frame_type, FrameType::Fetch);
                assert_eq!(request.field(f::GROUP_ID), Some(&9u64.to_be_bytes()[..]));
                if case != 4 {
                    let mut delivery = delivery_frame(request.request_id, 0, 77, 1);
                    if case == 3 {
                        // A delivery from another group must never be accepted as ours.
                        delivery
                            .fields
                            .iter_mut()
                            .find(|v| v.id == f::GROUP_ID)
                            .unwrap()
                            .value = 10u64.to_be_bytes().to_vec();
                    }
                    write_frame(&mut stream, &delivery, Limits::default())
                        .await
                        .unwrap();
                }
                if case == 0 {
                    return;
                }
                if case != 0 {
                    let frame = match case {
                        1 => fetch_end_frame(request.request_id, 2, 10, 0),
                        2 => fetch_end_frame(request.request_id + 1, 2, 9, 0),
                        3 => fetch_end_frame(request.request_id, 4, 9, 0),
                        4 => fetch_end_frame(request.request_id, 0, 9, 0),
                        _ => {
                            let mut error = Frame::new(FrameType::Error, request.request_id);
                            error.fields = vec![
                                Field::required(f::ERROR_CODE, 18u16.to_be_bytes()),
                                Field::required(f::ERROR_RETRYABLE, [0]),
                            ];
                            error
                        }
                    };
                    write_frame(&mut stream, &frame, Limits::default())
                        .await
                        .unwrap();
                }
            });
            let mut client = Client::connect(config).await.unwrap();
            let result = client.fetch_q1_group("durable", group_options(None)).await;
            match case {
                0 => assert!(matches!(result, Err(GroupError::Incomplete)), "case{case}"),
                4 => {
                    let batch = result.unwrap();
                    assert!(batch.messages.is_empty());
                    assert_eq!(batch.generation, 1);
                    assert_eq!(batch.next.sequence, 0);
                    assert!(!batch.has_more);
                }
                5 => assert!(
                    matches!(result, Err(GroupError::StaleGeneration)),
                    "case{case}"
                ),
                _ => assert!(result.is_err(), "case{case}"),
            }
            if case != 4 {
                assert!(
                    client.stream.is_none(),
                    "case{case} must close uncertain framing"
                );
            }
            server.await.unwrap();
        }
    }
}
