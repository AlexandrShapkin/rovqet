//! Bounded, serial asynchronous client for the loopback research profile.
//! Q0 admission never promises delivery. A lost publish response is unresolved;
//! the SDK never automatically republishes it and risks duplicate delivery.
#![forbid(unsafe_code)]

pub use rovqet_core::{DurableOffset, Priority, PublishOutcome, QoS};
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
}
impl Default for ClientConfig {
    fn default() -> Self {
        Self {
            endpoint: ([127, 0, 0, 1], 7443).into(),
            request_timeout: Duration::from_secs(5),
            max_pending_messages: 128,
            max_pending_bytes: 1024 * 1024,
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
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Delivery {
    pub topic: String,
    pub subscription_id: Option<u64>,
    pub message_id: u128,
    pub payload: Vec<u8>,
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
    subscriptions: BTreeMap<u64, String>,
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
    pub async fn subscribe(&mut self, topic: &str) -> Result<u64, ClientError> {
        if topic.is_empty() || topic.len() > 255 {
            return Err(ClientError::Protocol("invalid topic length"));
        }
        if self.subscriptions.len() >= self.config.max_pending_messages {
            return Err(ClientError::ResourceExhausted);
        }
        let mut frame = self.request(FrameType::Subscribe)?;
        frame
            .fields
            .push(Field::required(f::TOPIC_NAME, topic.as_bytes().to_vec()));
        let result = self.exchange(frame, FrameType::SubAck).await?;
        let id = u64_field(&result, f::SUBSCRIPTION_ID)?;
        self.subscriptions.insert(id, topic.to_string());
        Ok(id)
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
        Ok(())
    }
    pub async fn receive(&mut self) -> Result<Delivery, ClientError> {
        if let Some(delivery) = self.pending.pop_front() {
            self.pending_bytes -= delivery.payload.len() + delivery.topic.len();
            return Ok(delivery);
        }
        let frame = self.incoming(None).await?;
        if frame.frame_type == FrameType::Error {
            return Err(remote(&frame)?);
        }
        if frame.frame_type != FrameType::Message {
            return Err(ClientError::Protocol("expected delivery"));
        }
        delivery(frame)
    }
    /// Reconnects and recreates ephemeral subscriptions. Returns old/new IDs.
    /// No in-flight publication is replayed. Connection attempts alone are retried.
    pub async fn reconnect(&mut self) -> Result<Vec<(u64, u64)>, ClientError> {
        // Release the old session before competing for a bounded connection slot.
        self.stream = None;
        self.pending.clear();
        self.pending_bytes = 0;
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
        for (&old, topic) in &self.subscriptions {
            mappings.push((old, client.subscribe(topic).await?));
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
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::net::TcpListener;

    async fn handshake(stream: &mut TcpStream) {
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
}
