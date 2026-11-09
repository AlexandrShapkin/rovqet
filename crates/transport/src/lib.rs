//! Bounded TCP sessions for the isolated, loopback-only development profile.
#![forbid(unsafe_code)]
use rovqet_protocol::{
    CodecError, ErrorCode, Field, Frame, FrameType, Limits, decode_prefix, encode, negotiate_hello,
    registry::field,
};
use std::{
    future::Future,
    io,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::{OwnedSemaphorePermit, Semaphore, mpsc},
    task::JoinSet,
    time::{Instant, timeout},
};

fn invalid(e: impl std::fmt::Debug) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, format!("{e:?}"))
}

#[derive(Debug)]
struct WireError(CodecError);
impl std::fmt::Display for WireError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.0)
    }
}
impl std::error::Error for WireError {}
fn wire_invalid(code: CodecError) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, WireError(code))
}
/// Reads exactly one frame, checking declared bounds before allocating its body.
pub async fn read_frame<R: AsyncRead + Unpin>(
    reader: &mut R,
    limits: Limits,
) -> io::Result<Option<Frame>> {
    let mut prefix = Vec::with_capacity(24);
    let mut byte = [0];
    if reader.read(&mut byte).await? == 0 {
        return Ok(None);
    }
    prefix.push(byte[0]);
    while prefix.len() < 4 {
        reader.read_exact(&mut byte).await?;
        prefix.push(byte[0]);
    }
    if prefix[..2] != *b"RQ" {
        return Err(wire_invalid(CodecError::InvalidMagic));
    }
    if prefix[2] != 1 {
        return Err(wire_invalid(CodecError::UnsupportedVersion(prefix[2])));
    }
    if prefix[3] & !1 != 0 {
        return Err(wire_invalid(CodecError::InvalidFlags(prefix[3])));
    }
    let mut lengths = [0usize; 2];
    for value in &mut lengths {
        let mut n = 0u64;
        for i in 0..10 {
            reader.read_exact(&mut byte).await?;
            prefix.push(byte[0]);
            let b = byte[0];
            if i == 9 && b > 1 {
                return Err(wire_invalid(CodecError::VarintOverflow));
            }
            n |= u64::from(b & 127) << (i * 7);
            if b & 128 == 0 {
                if i > 0 && b == 0 {
                    return Err(wire_invalid(CodecError::NonCanonicalVarint));
                }
                *value = usize::try_from(n).map_err(invalid)?;
                break;
            }
            if i == 9 {
                return Err(wire_invalid(CodecError::VarintOverflow));
            }
        }
    }
    let total = prefix
        .len()
        .checked_add(lengths[0])
        .and_then(|n| n.checked_add(lengths[1]))
        .and_then(|n| n.checked_add(if prefix[3] & 1 != 0 { 4 } else { 0 }))
        .ok_or_else(|| wire_invalid(CodecError::InvalidLength))?;
    if total > limits.max_frame_bytes
        || lengths[0] > limits.max_header_bytes
        || lengths[1] > limits.max_payload_bytes
    {
        return Err(wire_invalid(CodecError::FrameTooLarge));
    }
    let start = prefix.len();
    prefix.resize(total, 0);
    reader.read_exact(&mut prefix[start..]).await?;
    decode_prefix(&prefix, limits)
        .map_err(wire_invalid)?
        .map(|(f, _)| f)
        .ok_or_else(|| wire_invalid(CodecError::InvalidLength))
        .map(Some)
}

pub async fn write_frame<W: AsyncWrite + Unpin>(
    writer: &mut W,
    frame: &Frame,
    limits: Limits,
) -> io::Result<()> {
    writer
        .write_all(&encode(frame, limits).map_err(invalid)?)
        .await
}

#[derive(Debug, Default)]
pub struct TransportStats {
    /// Q0 MESSAGE frames that crossed the successful socket write boundary.
    pub delivered: AtomicU64,
    pub writer_expired: AtomicU64,
    pub write_failed: AtomicU64,
}
#[derive(Clone, Debug)]
pub struct Config {
    pub limits: Limits,
    pub stats: Arc<TransportStats>,
    pub max_connections: usize,
    pub outbound_messages: usize,
    pub outbound_bytes: usize,
    pub heartbeat: Duration,
    pub write_timeout: Duration,
    pub drain_timeout: Duration,
}
impl Default for Config {
    fn default() -> Self {
        Self {
            stats: Arc::new(TransportStats::default()),
            limits: Limits::default(),
            max_connections: 1024,
            outbound_messages: 64,
            outbound_bytes: 262144,
            heartbeat: Duration::from_secs(30),
            write_timeout: Duration::from_secs(5),
            drain_timeout: Duration::from_secs(5),
        }
    }
}
fn is_message(bytes: &[u8]) -> bool {
    let mut cursor = 4;
    for _ in 0..2 {
        loop {
            let Some(b) = bytes.get(cursor) else {
                return false;
            };
            cursor += 1;
            if b & 128 == 0 {
                break;
            }
        }
    }
    bytes.get(cursor) == Some(&(FrameType::Message as u8))
}
struct Queued {
    bytes: Arc<[u8]>,
    _bytes: OwnedSemaphorePermit,
    deadline: Option<Instant>,
}
/// A message-count and byte reservation; dropping it rolls back both budgets.
pub struct PendingSend {
    slot: mpsc::OwnedPermit<Queued>,
    item: Queued,
}
impl PendingSend {
    pub fn send(self) {
        self.slot.send(self.item);
    }
}
#[derive(Clone)]
pub struct Outbound {
    tx: mpsc::Sender<Queued>,
    control_tx: mpsc::Sender<Queued>,
    control_bytes: Arc<Semaphore>,
    bytes: Arc<Semaphore>,
    limits: Limits,
}
impl Outbound {
    pub fn try_send(&self, frame: Frame) -> Result<(), ErrorCode> {
        self.try_send_shared(Arc::new(frame))
    }
    pub fn try_send_shared(&self, frame: Arc<Frame>) -> Result<(), ErrorCode> {
        self.try_send_until(frame, None)
    }
    pub fn try_send_until(
        &self,
        frame: Arc<Frame>,
        deadline: Option<Instant>,
    ) -> Result<(), ErrorCode> {
        let bytes = Arc::from(encode(&frame, self.limits).map_err(|_| ErrorCode::BadFrame)?);
        self.try_send_encoded(bytes, deadline)
    }
    /// Trusted internally encoded frame, shared by every fan-out destination.
    pub fn try_send_encoded(
        &self,
        bytes: Arc<[u8]>,
        deadline: Option<Instant>,
    ) -> Result<(), ErrorCode> {
        self.reserve_encoded(bytes, deadline)?.send();
        Ok(())
    }
    pub fn reserve_encoded(
        &self,
        bytes: Arc<[u8]>,
        deadline: Option<Instant>,
    ) -> Result<PendingSend, ErrorCode> {
        let size = bytes.len();
        if size > self.limits.max_frame_bytes {
            return Err(ErrorCode::ResourceExhausted);
        }
        let (sender, budget) = if is_message(&bytes) {
            (&self.tx, &self.bytes)
        } else {
            (&self.control_tx, &self.control_bytes)
        };
        let permit = budget
            .clone()
            .try_acquire_many_owned(u32::try_from(size).map_err(|_| ErrorCode::ResourceExhausted)?)
            .map_err(|_| ErrorCode::ResourceExhausted)?;
        let slot = sender
            .clone()
            .try_reserve_owned()
            .map_err(|_| ErrorCode::ResourceExhausted)?;
        Ok(PendingSend {
            slot,
            item: Queued {
                bytes,
                _bytes: permit,
                deadline,
            },
        })
    }
}
pub trait SessionHandler: Send + Sync + 'static {
    fn handle(
        &self,
        session: u128,
        frame: Frame,
        outbound: Outbound,
    ) -> Result<Option<Frame>, ErrorCode>;
    fn disconnected(&self, _session: u128) {}
}
pub struct NoopHandler;
impl SessionHandler for NoopHandler {
    fn handle(&self, _: u128, _: Frame, _: Outbound) -> Result<Option<Frame>, ErrorCode> {
        Err(ErrorCode::UnsupportedCapability)
    }
}
pub fn error_frame(request: u64, code: ErrorCode) -> Frame {
    let mut f = Frame::new(FrameType::Error, request);
    f.fields.push(Field::required(
        field::ERROR_CODE,
        (code as u16).to_be_bytes(),
    ));
    f.fields.push(Field::required(
        field::ERROR_RETRYABLE,
        [u8::from(matches!(
            code,
            ErrorCode::ResourceExhausted | ErrorCode::RetryAfter
        ))],
    ));
    f
}
static NEXT_SESSION: AtomicU64 = AtomicU64::new(1);
/// Serve trusted loopback clients until shutdown, then boundedly drain all sessions.
pub async fn serve(
    listener: TcpListener,
    config: Config,
    handler: Arc<dyn SessionHandler>,
    shutdown: impl Future<Output = ()> + Send,
) -> io::Result<()> {
    if !listener.local_addr()?.ip().is_loopback() {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "development profile requires loopback",
        ));
    }
    if config.max_connections == 0
        || config.outbound_messages == 0
        || config.outbound_bytes == 0
        || config.outbound_bytes > u32::MAX as usize
        || config.heartbeat.is_zero()
    {
        return Err(invalid("invalid session bounds"));
    }
    let slots = Arc::new(Semaphore::new(config.max_connections));
    let (stop, _) = tokio::sync::watch::channel(false);
    let mut sessions = JoinSet::new();
    tokio::pin!(shutdown);
    loop {
        tokio::select! {
            _ = &mut shutdown => break,
            done = sessions.join_next(), if !sessions.is_empty() => { let _ = done; },
            accepted = listener.accept() => {
                let (mut stream, _) = accepted?;
                let Ok(slot) = slots.clone().try_acquire_owned() else {
                    let f = error_frame(0, ErrorCode::ResourceExhausted);
                    let _ = timeout(config.write_timeout, write_frame(&mut stream, &f, config.limits)).await; continue;
                };
                let cfg=config.clone(); let h=handler.clone(); let stop=stop.subscribe();
                sessions.spawn(async move { let _slot=slot; let id=u128::from(NEXT_SESSION.fetch_add(1, Ordering::Relaxed)); let _=session(stream,id,cfg,h,stop).await; });
            }
        }
    }
    let _ = stop.send(true);
    if timeout(config.drain_timeout, async {
        while sessions.join_next().await.is_some() {}
    })
    .await
    .is_err()
    {
        sessions.abort_all();
        while sessions.join_next().await.is_some() {}
    }
    Ok(())
}
#[derive(Clone, Copy, PartialEq, Eq)]
enum SessionState {
    AwaitHello,
    AwaitAuth,
    Active,
}

struct Cleanup {
    handler: Arc<dyn SessionHandler>,
    id: u128,
    done: bool,
}
impl Cleanup {
    fn clean(&mut self) {
        if !self.done {
            self.handler.disconnected(self.id);
            self.done = true;
        }
    }
}
impl Drop for Cleanup {
    fn drop(&mut self) {
        self.clean();
    }
}
struct AbortWriter(tokio::task::AbortHandle);
impl Drop for AbortWriter {
    fn drop(&mut self) {
        self.0.abort();
    }
}
async fn session(
    stream: TcpStream,
    id: u128,
    config: Config,
    handler: Arc<dyn SessionHandler>,
    mut stop: tokio::sync::watch::Receiver<bool>,
) -> io::Result<()> {
    let mut cleanup = Cleanup {
        handler: handler.clone(),
        id,
        done: false,
    };
    stream.set_nodelay(true)?;
    let (mut reader, mut writer) = stream.into_split();
    let mut limits = config.limits;
    let (tx, mut rx) = mpsc::channel::<Queued>(config.outbound_messages);
    let (control_tx, mut control_rx) = mpsc::channel::<Queued>(8);
    let mut outbound = Outbound {
        tx,
        control_tx,
        control_bytes: Arc::new(Semaphore::new(4096)),
        bytes: Arc::new(Semaphore::new(config.outbound_bytes)),
        limits,
    };
    let write_timeout = config.write_timeout;
    let stats = config.stats.clone();
    let mut writer_task = tokio::spawn(async move {
        loop {
            let item = tokio::select! {biased; item=control_rx.recv(), if !control_rx.is_closed() || !control_rx.is_empty()=>item,
            item=rx.recv(), if !rx.is_closed() || !rx.is_empty()=>item, else=>break};
            let Some(item) = item else {
                if control_rx.is_closed() && control_rx.is_empty() {
                    break;
                }
                continue;
            };
            if item.deadline.is_some_and(|d| Instant::now() >= d) {
                stats.writer_expired.fetch_add(1, Ordering::Relaxed);
                continue;
            }
            match timeout(write_timeout, writer.write_all(&item.bytes)).await {
                Ok(Ok(())) => {
                    if is_message(&item.bytes) {
                        stats.delivered.fetch_add(1, Ordering::Relaxed);
                    }
                }
                _ => {
                    stats.write_failed.fetch_add(1, Ordering::Relaxed);
                    return Err(io::Error::new(
                        io::ErrorKind::TimedOut,
                        "socket write failed or timed out",
                    ));
                }
            }
        }
        writer.shutdown().await
    });
    let _abort_writer = AbortWriter(writer_task.abort_handle());
    let mut state = SessionState::AwaitHello;
    loop {
        let next = tokio::select! { _=stop.changed()=>{let _=outbound.try_send(Frame::new(FrameType::Drain,0)); break;}, _=&mut writer_task=>{return Ok(());}, f=timeout(config.heartbeat,read_frame(&mut reader,limits))=>f };
        let frame = match next {
            Ok(Ok(Some(f))) => f,
            Ok(Ok(None)) => break,
            failure => {
                let code = match failure {
                    Ok(Err(ref error)) => match error
                        .get_ref()
                        .and_then(|e| e.downcast_ref::<WireError>())
                        .map(|e| e.0)
                    {
                        Some(CodecError::UnsupportedVersion(_)) => ErrorCode::UnsupportedVersion,
                        Some(CodecError::UnsupportedCriticalField(_)) => {
                            ErrorCode::UnsupportedCapability
                        }
                        _ => ErrorCode::BadFrame,
                    },
                    _ => ErrorCode::LeaseExpired,
                };
                let _ = outbound.try_send(error_frame(0, code));
                break;
            }
        };
        let request = frame.request_id;
        if frame.frame_type == FrameType::Close {
            let _ = outbound.try_send(Frame::new(FrameType::Close, request));
            break;
        }
        let result: Result<Option<Frame>, ErrorCode> = match state {
            SessionState::AwaitHello if frame.frame_type == FrameType::Hello => {
                match negotiate_hello(&frame, 0, limits.max_frame_bytes as u32, &[]) {
                    Ok(n) => {
                        let mut selected = limits;
                        selected.max_frame_bytes = n.max_frame_bytes as usize;
                        let mut f = Frame::new(FrameType::Welcome, request);
                        f.fields = vec![
                            Field::required(field::PROTOCOL_MINOR, n.minor.to_be_bytes()),
                            Field::required(
                                field::MAX_FRAME_BYTES,
                                n.max_frame_bytes.to_be_bytes(),
                            ),
                            Field::required(field::SESSION_ID, id.to_be_bytes()),
                            Field::optional(
                                field::HEARTBEAT_MS,
                                (config.heartbeat.as_millis().min(u32::MAX as u128) as u32)
                                    .to_be_bytes(),
                            ),
                        ];
                        if encode(&f, selected).is_err() {
                            Err(ErrorCode::BadFrame)
                        } else {
                            limits = selected;
                            outbound.limits = selected;
                            state = SessionState::AwaitAuth;
                            Ok(Some(f))
                        }
                    }
                    Err(_) => Err(ErrorCode::UnsupportedCapability),
                }
            }
            SessionState::AwaitAuth if frame.frame_type == FrameType::Auth => {
                if frame.field(field::AUTH_METHOD) == Some(&[0][..])
                    && frame.field(field::AUTH_DATA) == Some(b"dev")
                {
                    state = SessionState::Active;
                    let mut f = Frame::new(FrameType::AuthOk, request);
                    f.fields
                        .push(Field::required(field::PRINCIPAL_ID, 1u64.to_be_bytes()));
                    Ok(Some(f))
                } else {
                    Err(ErrorCode::Unauthenticated)
                }
            }
            SessionState::AwaitHello | SessionState::AwaitAuth => Err(ErrorCode::Unauthenticated),
            _ if frame.frame_type == FrameType::Ping => {
                let mut f = Frame::new(FrameType::Pong, request);
                if let Some(n) = frame.field(field::NONCE) {
                    f.fields.push(Field::optional(field::NONCE, n.to_vec()));
                }
                Ok(Some(f))
            }
            _ if frame.frame_type == FrameType::Pong => Ok(None),
            _ => handler.handle(id, frame, outbound.clone()),
        };
        match result {
            Ok(Some(f)) => {
                if outbound.try_send(f).is_err() {
                    break;
                }
            }
            Ok(None) => {}
            Err(e) => {
                if outbound.try_send(error_frame(request, e)).is_err() {
                    break;
                }
                if state != SessionState::Active {
                    break;
                }
            }
        }
    }
    cleanup.clean();
    drop(outbound);
    if timeout(config.drain_timeout, &mut writer_task)
        .await
        .is_err()
    {
        writer_task.abort();
        let _ = writer_task.await;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    async fn broker(
        config: Config,
    ) -> (
        std::net::SocketAddr,
        tokio::sync::oneshot::Sender<()>,
        tokio::task::JoinHandle<io::Result<()>>,
    ) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (tx, rx) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(serve(listener, config, Arc::new(NoopHandler), async {
            let _ = rx.await;
        }));
        (addr, tx, task)
    }
    fn hello() -> Frame {
        let mut f = Frame::new(FrameType::Hello, 1);
        f.fields = vec![
            Field::required(field::PROTOCOL_MINOR, 0u16.to_be_bytes()),
            Field::required(field::MAX_FRAME_BYTES, 1048576u32.to_be_bytes()),
        ];
        f
    }
    async fn exchange(stream: &mut TcpStream, f: Frame) -> Frame {
        write_frame(stream, &f, Limits::default()).await.unwrap();
        read_frame(stream, Limits::default())
            .await
            .unwrap()
            .unwrap()
    }
    #[tokio::test]
    async fn lifecycle_and_unsupported_operation() {
        let (addr, stop, task) = broker(Config::default()).await;
        let mut stream = TcpStream::connect(addr).await.unwrap();
        assert_eq!(
            exchange(&mut stream, hello()).await.frame_type,
            FrameType::Welcome
        );
        let mut auth = Frame::new(FrameType::Auth, 2);
        auth.fields = vec![
            Field::required(field::AUTH_METHOD, [0]),
            Field::required(field::AUTH_DATA, b"dev".to_vec()),
        ];
        assert_eq!(
            exchange(&mut stream, auth).await.frame_type,
            FrameType::AuthOk
        );
        assert_eq!(
            exchange(&mut stream, Frame::new(FrameType::Ping, 3))
                .await
                .frame_type,
            FrameType::Pong
        );
        assert_eq!(
            exchange(&mut stream, Frame::new(FrameType::Drain, 4))
                .await
                .field(field::ERROR_CODE),
            Some(&(ErrorCode::UnsupportedCapability as u16).to_be_bytes()[..])
        );
        assert_eq!(
            exchange(&mut stream, Frame::new(FrameType::Close, 5))
                .await
                .frame_type,
            FrameType::Close
        );
        stop.send(()).unwrap();
        task.await.unwrap().unwrap();
    }
    #[tokio::test]
    async fn unauthenticated_and_bad_frames_are_explicit() {
        let (addr, stop, task) = broker(Config::default()).await;
        let mut stream = TcpStream::connect(addr).await.unwrap();
        assert_eq!(
            exchange(&mut stream, Frame::new(FrameType::Ping, 1))
                .await
                .field(field::ERROR_CODE),
            Some(&(ErrorCode::Unauthenticated as u16).to_be_bytes()[..])
        );
        let mut stream = TcpStream::connect(addr).await.unwrap();
        stream.write_all(b"NOPE").await.unwrap();
        assert_eq!(
            read_frame(&mut stream, Limits::default())
                .await
                .unwrap()
                .unwrap()
                .field(field::ERROR_CODE),
            Some(&(ErrorCode::BadFrame as u16).to_be_bytes()[..])
        );
        stop.send(()).unwrap();
        task.await.unwrap().unwrap();
    }
    #[tokio::test]
    async fn connection_limit_and_idle_cleanup() {
        let cfg = Config {
            max_connections: 1,
            heartbeat: Duration::from_millis(80),
            ..Config::default()
        };
        let (addr, stop, task) = broker(cfg).await;
        let mut first = TcpStream::connect(addr).await.unwrap();
        assert_eq!(
            exchange(&mut first, hello()).await.frame_type,
            FrameType::Welcome
        );
        let mut second = TcpStream::connect(addr).await.unwrap();
        assert_eq!(
            read_frame(&mut second, Limits::default())
                .await
                .unwrap()
                .unwrap()
                .field(field::ERROR_CODE),
            Some(&(ErrorCode::ResourceExhausted as u16).to_be_bytes()[..])
        );
        assert_eq!(
            timeout(
                Duration::from_secs(1),
                read_frame(&mut first, Limits::default())
            )
            .await
            .unwrap()
            .unwrap()
            .unwrap()
            .frame_type,
            FrameType::Error
        );
        stop.send(()).unwrap();
        task.await.unwrap().unwrap();
    }
    #[tokio::test]
    async fn declared_oversize_rejected_before_body_read() {
        let bytes = &b"RQ\x01\x00\x09\x80\x80\x40"[..];
        let mut input = bytes;
        assert_eq!(
            read_frame(&mut input, Limits::default())
                .await
                .unwrap_err()
                .kind(),
            io::ErrorKind::InvalidData
        );
    }
    #[tokio::test]
    async fn outbound_count_and_byte_bounds_release_on_drop() {
        let (tx, mut rx) = mpsc::channel(1);
        let out = Outbound {
            control_tx: tx.clone(),
            control_bytes: Arc::new(Semaphore::new(128)),
            tx,
            bytes: Arc::new(Semaphore::new(128)),
            limits: Limits::default(),
        };
        out.try_send(Frame::new(FrameType::Ping, 1)).unwrap();
        assert_eq!(
            out.try_send(Frame::new(FrameType::Ping, 2)),
            Err(ErrorCode::ResourceExhausted)
        );
        drop(rx.recv().await.unwrap());
        assert_eq!(out.control_bytes.available_permits(), 128);
        assert_eq!(
            out.try_send_encoded(Arc::from(vec![0; 129]), None),
            Err(ErrorCode::ResourceExhausted)
        );
    }
    #[tokio::test]
    async fn reserved_data_rolls_back_and_cannot_consume_control_capacity() {
        let (tx, mut rx) = mpsc::channel(1);
        let (control_tx, mut control_rx) = mpsc::channel(1);
        let out = Outbound {
            tx,
            control_tx,
            bytes: Arc::new(Semaphore::new(512)),
            control_bytes: Arc::new(Semaphore::new(128)),
            limits: Limits::default(),
        };
        let mut message = Frame::new(FrameType::Message, 0);
        message.fields = vec![
            Field::required(field::TOPIC_NAME, b"demo".to_vec()),
            Field::required(field::QOS, [0]),
            Field::required(field::MESSAGE_ID, 1u128.to_be_bytes()),
        ];
        let encoded: Arc<[u8]> = encode(&message, Limits::default()).unwrap().into();
        let reservation = out.reserve_encoded(encoded.clone(), None).unwrap();
        assert!(out.reserve_encoded(encoded.clone(), None).is_err());
        out.try_send(Frame::new(FrameType::Ping, 1)).unwrap();
        assert!(control_rx.recv().await.is_some());
        drop(reservation);
        assert_eq!(out.bytes.available_permits(), 512);
        out.reserve_encoded(encoded, None).unwrap().send();
        assert!(rx.recv().await.is_some());
        assert_eq!(out.bytes.available_permits(), 512);
    }
}
