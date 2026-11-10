//! Bounded TCP sessions for the isolated, loopback-only development profile.
#![forbid(unsafe_code)]
use rovqet_protocol::{
    CodecError, ErrorCode, Field, Frame, FrameType, Limits, decode_prefix, encode, negotiate_hello,
    registry::field,
};
use rovqet_scheduler::{Budget, RateQuota, RateReservation, Reservation};
use std::{
    collections::{HashSet, VecDeque},
    future::Future,
    io,
    pin::Pin,
    sync::{
        Arc, Mutex, Weak,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::{Notify, OwnedSemaphorePermit, Semaphore, mpsc},
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
    Ok(read_frame_sized(reader, limits)
        .await?
        .map(|(frame, _)| frame))
}
/// Like read_frame, retaining the exact received size including ignored optional TLVs.
pub async fn read_frame_sized<R: AsyncRead + Unpin>(
    reader: &mut R,
    limits: Limits,
) -> io::Result<Option<(Frame, usize)>> {
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
        .map(|(f, _)| (f, total))
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
    pub enqueued: AtomicU64,
    pub delivered_bytes: AtomicU64,
    pub dropped_newest: AtomicU64,
    pub dropped_oldest: AtomicU64,
    pub coalesced: AtomicU64,
    pub dropped_closed: AtomicU64,
    pub dropped_shutdown: AtomicU64,
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
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OverflowPolicy {
    DropNewest,
    DropOldest,
    CoalesceLatest,
    RejectPublish,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EnqueueResult {
    pub evicted: usize,
}
struct CreditState {
    messages: u32,
    bytes: u32,
    closed: bool,
    subscription_id: u64,
    watcher: Option<Weak<Notify>>,
}
/// Per-subscription byte/message credits. Only successful replenishment wakes delivery.
pub struct DeliveryGate {
    max_messages: u32,
    max_bytes: u32,
    state: Mutex<CreditState>,
    quotas: Mutex<Vec<Arc<RateQuota>>>,
    budgets: Mutex<Vec<Arc<Budget>>>,
}
impl DeliveryGate {
    pub fn new(max_messages: u32, max_bytes: u32) -> Arc<Self> {
        Arc::new(Self {
            max_messages,
            max_bytes,
            quotas: Mutex::new(Vec::new()),
            budgets: Mutex::new(Vec::new()),
            state: Mutex::new(CreditState {
                messages: 0,
                bytes: 0,
                closed: false,
                subscription_id: 0,
                watcher: None,
            }),
        })
    }
    pub fn set_budgets(&self, budgets: Vec<Arc<Budget>>) {
        *self.budgets.lock().unwrap() = budgets;
    }
    pub fn set_quotas(&self, quotas: Vec<Arc<RateQuota>>) {
        *self.quotas.lock().unwrap() = quotas;
    }
    fn can_fit(&self, bytes: usize) -> bool {
        bytes <= self.max_bytes as usize
            && self
                .quotas
                .lock()
                .unwrap()
                .iter()
                .all(|q| q.can_fit(bytes, 1))
    }
    fn ready(&self, bytes: usize) -> bool {
        let s = self.state.lock().unwrap();
        !s.closed && s.messages > 0 && bytes <= s.bytes as usize
    }
    pub fn add(&self, messages: u32, bytes: u32) -> Result<(), ErrorCode> {
        let mut state = self.state.lock().unwrap();
        if state.closed {
            return Err(ErrorCode::BadFrame);
        }
        let m = state
            .messages
            .checked_add(messages)
            .ok_or(ErrorCode::QuotaExceeded)?;
        let b = state
            .bytes
            .checked_add(bytes)
            .ok_or(ErrorCode::QuotaExceeded)?;
        if (messages == 0 && bytes == 0) || m > self.max_messages || b > self.max_bytes {
            return Err(ErrorCode::QuotaExceeded);
        }
        state.messages = m;
        state.bytes = b;
        if let Some(n) = state.watcher.as_ref().and_then(Weak::upgrade) {
            n.notify_one();
        }
        Ok(())
    }
    pub fn set_subscription_id(&self, id: u64) {
        self.state.lock().unwrap().subscription_id = id;
    }
    fn subscription_id(&self) -> u64 {
        self.state.lock().unwrap().subscription_id
    }
    pub fn available(&self) -> (u32, u32) {
        let s = self.state.lock().unwrap();
        (s.messages, s.bytes)
    }
    pub fn close(&self) {
        let mut s = self.state.lock().unwrap();
        s.closed = true;
        if let Some(n) = s.watcher.as_ref().and_then(Weak::upgrade) {
            n.notify_one();
        }
    }
    pub fn is_closed(&self) -> bool {
        self.closed()
    }
    fn closed(&self) -> bool {
        self.state.lock().unwrap().closed
    }
    fn refund(&self, messages: u32, bytes: u32) {
        let mut state = self.state.lock().unwrap();
        if state.closed {
            return;
        }
        state.messages = state
            .messages
            .saturating_add(messages)
            .min(self.max_messages);
        state.bytes = state.bytes.saturating_add(bytes).min(self.max_bytes);
        if let Some(notify) = state.watcher.as_ref().and_then(Weak::upgrade) {
            notify.notify_one();
        }
    }
    fn take(&self, bytes: usize) -> bool {
        let mut s = self.state.lock().unwrap();
        if s.closed || s.messages == 0 || bytes > s.bytes as usize {
            return false;
        }
        s.messages -= 1;
        s.bytes -= bytes as u32;
        true
    }
    fn watch(&self, notify: &Arc<Notify>) {
        self.state.lock().unwrap().watcher = Some(Arc::downgrade(notify));
    }
}
struct Queued {
    bytes: Arc<[u8]>,
    _bytes: OwnedSemaphorePermit,
    _slot: Option<OwnedSemaphorePermit>,
    deadline: Option<Instant>,
    key: Vec<u8>,
    gate: Option<Arc<DeliveryGate>>,
    stats: Option<Arc<TransportStats>>,
    armed: bool,
    rate: Option<RateReservation>,
    _accounting: Option<Reservation>,
}
impl Queued {
    fn finish(&mut self) {
        self.armed = false;
    }
}
impl Drop for Queued {
    fn drop(&mut self) {
        if self.armed {
            if let Some(stats) = &self.stats {
                stats.dropped_shutdown.fetch_add(1, Ordering::Relaxed);
            }
        }
    }
}
struct DataState {
    items: VecDeque<Queued>,
    closed: bool,
}
struct DataQueue {
    state: Mutex<DataState>,
    notify: Arc<Notify>,
    slots: Arc<Semaphore>,
    bytes: Arc<Semaphore>,
    max_bytes: usize,
    stats: Arc<TransportStats>,
}
impl DataQueue {
    fn new(messages: usize, bytes: usize, stats: Arc<TransportStats>) -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(DataState {
                items: VecDeque::new(),
                closed: false,
            }),
            notify: Arc::new(Notify::new()),
            slots: Arc::new(Semaphore::new(messages)),
            bytes: Arc::new(Semaphore::new(bytes)),
            max_bytes: bytes,
            stats,
        })
    }
    fn close(&self) {
        let mut state = self.state.lock().unwrap();
        state.closed = true;
        for mut item in state.items.drain(..) {
            item.finish();
            self.stats.dropped_closed.fetch_add(1, Ordering::Relaxed);
        }
        self.notify.notify_waiters();
    }
    fn reserve(
        self: &Arc<Self>,
        bytes: Arc<[u8]>,
        deadline: Option<Instant>,
        key: Vec<u8>,
        gate: Option<Arc<DeliveryGate>>,
    ) -> Result<PendingSend, ErrorCode> {
        let state = self.state.lock().unwrap();
        if bytes.len() > self.max_bytes
            || gate
                .as_ref()
                .is_some_and(|g| !g.can_fit(bytes.len()) || g.closed())
            || state.closed
        {
            return Err(ErrorCode::ResourceExhausted);
        }
        let slot = self
            .slots
            .clone()
            .try_acquire_owned()
            .map_err(|_| ErrorCode::ResourceExhausted)?;
        let permit = self
            .bytes
            .clone()
            .try_acquire_many_owned(bytes.len() as u32)
            .map_err(|_| ErrorCode::ResourceExhausted)?;
        let accounting =
            account(&gate, bytes.len(), key.len()).ok_or(ErrorCode::ResourceExhausted)?;
        if let Some(g) = &gate {
            g.watch(&self.notify);
        }
        Ok(PendingSend {
            slot: PendingSlot::Data(self.clone()),
            item: Queued {
                bytes,
                _bytes: permit,
                _slot: Some(slot),
                deadline,
                key,
                gate,
                stats: Some(self.stats.clone()),
                armed: false,
                rate: None,
                _accounting: accounting,
            },
        })
    }
    fn enqueue(
        self: &Arc<Self>,
        bytes: Arc<[u8]>,
        deadline: Option<Instant>,
        policy: OverflowPolicy,
        key: Vec<u8>,
        gate: Option<Arc<DeliveryGate>>,
    ) -> Result<EnqueueResult, ErrorCode> {
        if bytes.len() > self.max_bytes || gate.as_ref().is_some_and(|g| !g.can_fit(bytes.len())) {
            self.stats.dropped_newest.fetch_add(1, Ordering::Relaxed);
            return Err(ErrorCode::ResourceExhausted);
        }
        let mut state = self.state.lock().unwrap();
        if state.closed || gate.as_ref().is_some_and(|g| g.closed()) {
            return Err(ErrorCode::ResourceExhausted);
        }
        let mut evicted = 0;
        if policy == OverflowPolicy::CoalesceLatest {
            if let Some(pos) = state
                .items
                .iter()
                .position(|item| item.key == key && same_gate(&item.gate, &gate))
            {
                let mut old = state.items.remove(pos).unwrap();
                old.finish();
                drop(old);
                self.stats.coalesced.fetch_add(1, Ordering::Relaxed);
                evicted += 1;
            }
        }
        loop {
            let slot = self.slots.clone().try_acquire_owned();
            let permit = self
                .bytes
                .clone()
                .try_acquire_many_owned(bytes.len() as u32);
            match (slot, permit) {
                (Ok(slot), Ok(permit)) => {
                    let accounting = account(&gate, bytes.len(), key.len());
                    if accounting.is_none() {
                        drop(slot);
                        drop(permit);
                        let evict = state
                            .items
                            .iter()
                            .position(|item| same_gate(&item.gate, &gate));
                        if policy == OverflowPolicy::DropOldest && evict.is_some() {
                            let mut old = state.items.remove(evict.unwrap()).unwrap();
                            old.finish();
                            drop(old);
                            self.stats.dropped_oldest.fetch_add(1, Ordering::Relaxed);
                            evicted += 1;
                            continue;
                        }
                        self.stats.dropped_newest.fetch_add(1, Ordering::Relaxed);
                        return Err(ErrorCode::ResourceExhausted);
                    }
                    if let Some(g) = &gate {
                        g.watch(&self.notify);
                    }
                    state.items.push_back(Queued {
                        bytes,
                        _bytes: permit,
                        _slot: Some(slot),
                        deadline,
                        key,
                        gate,
                        stats: Some(self.stats.clone()),
                        armed: true,
                        rate: None,
                        _accounting: accounting.unwrap(),
                    });
                    self.stats.enqueued.fetch_add(1, Ordering::Relaxed);
                    self.notify.notify_one();
                    return Ok(EnqueueResult { evicted });
                }
                (slot, permit) => {
                    drop(slot);
                    drop(permit);
                }
            }
            let evict = state
                .items
                .iter()
                .position(|item| same_gate(&item.gate, &gate));
            if policy != OverflowPolicy::DropOldest || evict.is_none() {
                self.stats.dropped_newest.fetch_add(1, Ordering::Relaxed);
                return Err(ErrorCode::ResourceExhausted);
            }
            let mut old = state.items.remove(evict.unwrap()).unwrap();
            old.finish();
            drop(old);
            self.stats.dropped_oldest.fetch_add(1, Ordering::Relaxed);
            evicted += 1;
        }
    }
    async fn next(&self) -> Option<Queued> {
        loop {
            let notified = self.notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            let wake = {
                let mut state = self.state.lock().unwrap();
                let now = Instant::now();
                let mut pos = 0;
                let mut rate_blocked = false;
                let mut visited_gates = HashSet::new();
                while pos < state.items.len() {
                    let item = &state.items[pos];
                    let expired = item.deadline.is_some_and(|d| now >= d);
                    let closed = item.gate.as_ref().is_some_and(|g| g.closed());
                    if expired || closed {
                        let mut old = state.items.remove(pos).unwrap();
                        old.finish();
                        if closed {
                            self.stats.dropped_closed.fetch_add(1, Ordering::Relaxed);
                        } else {
                            self.stats.writer_expired.fetch_add(1, Ordering::Relaxed);
                        }
                        continue;
                    }
                    let item_bytes = item.bytes.len();
                    if let Some(gate) = item.gate.clone() {
                        // Credit may change concurrently during this scan. Once a
                        // subscription's head is blocked, only a new scan may retry it;
                        // a later copy must not steal newly replenished credit.
                        if !visited_gates.insert(Arc::as_ptr(&gate)) {
                            pos += 1;
                            continue;
                        }
                        if gate.ready(item_bytes) {
                            let rate =
                                RateQuota::reserve(&gate.quotas.lock().unwrap(), item_bytes, 1);
                            if let Some(rate) = rate {
                                if gate.take(item_bytes) {
                                    let mut item = state.items.remove(pos).unwrap();
                                    item.rate = Some(rate);
                                    return Some(item);
                                }
                            } else {
                                rate_blocked = true;
                            }
                        }
                    } else {
                        return state.items.remove(pos);
                    }
                    pos += 1;
                }
                if state.closed {
                    return None;
                }
                let expiry = state.items.iter().filter_map(|i| i.deadline).min();
                if rate_blocked {
                    Some(
                        expiry
                            .unwrap_or(now + Duration::from_millis(5))
                            .min(now + Duration::from_millis(5)),
                    )
                } else {
                    expiry
                }
            };
            if let Some(deadline) = wake {
                tokio::select! {_=notified.as_mut()=>{},_=tokio::time::sleep_until(deadline)=>{}}
            } else {
                notified.await;
            }
        }
    }
}
fn account(
    gate: &Option<Arc<DeliveryGate>>,
    bytes: usize,
    key: usize,
) -> Option<Option<Reservation>> {
    match gate {
        Some(gate) => {
            let cost = bytes.checked_add(key)?.checked_add(128)?;
            Budget::reserve(&gate.budgets.lock().unwrap(), cost).map(Some)
        }
        None => Some(None),
    }
}
fn same_gate(a: &Option<Arc<DeliveryGate>>, b: &Option<Arc<DeliveryGate>>) -> bool {
    match (a, b) {
        (Some(a), Some(b)) => Arc::ptr_eq(a, b),
        (None, None) => true,
        _ => false,
    }
}
fn varint(bytes: &[u8], cursor: &mut usize) -> Option<usize> {
    let mut value = 0usize;
    for shift in (0..70).step_by(7) {
        let byte = *bytes.get(*cursor)?;
        *cursor += 1;
        value |= ((byte & 127) as usize).checked_shl(shift)?;
        if byte & 128 == 0 {
            return Some(value);
        }
    }
    None
}
fn subscription_slot(bytes: &[u8]) -> Option<usize> {
    if bytes.get(3).copied()? & 1 != 0 {
        return None;
    }
    let mut cursor = 4;
    let header = varint(bytes, &mut cursor)?;
    let _payload = varint(bytes, &mut cursor)?;
    let end = cursor.checked_add(header)?;
    varint(bytes, &mut cursor)?;
    cursor = cursor.checked_add(8)?;
    while cursor < end {
        let tag = varint(bytes, &mut cursor)?;
        let len = varint(bytes, &mut cursor)?;
        if tag >> 1 == field::SUBSCRIPTION_ID as usize && len == 8 {
            return Some(cursor);
        }
        cursor = cursor.checked_add(len)?;
    }
    None
}
async fn write_queued<W: AsyncWrite + Unpin>(writer: &mut W, item: &Queued) -> io::Result<()> {
    if let Some(gate) = &item.gate {
        let id = gate.subscription_id();
        if id != 0 {
            let slot = subscription_slot(&item.bytes)
                .ok_or_else(|| invalid("missing subscription slot"))?;
            writer.write_all(&item.bytes[..slot]).await?;
            writer.write_all(&id.to_be_bytes()).await?;
            return writer.write_all(&item.bytes[slot + 8..]).await;
        }
    }
    writer.write_all(&item.bytes).await
}
enum PendingSlot {
    Control(mpsc::OwnedPermit<Queued>),
    Data(Arc<DataQueue>),
}
/// Owns queue count/bytes until commit or drop. Dropping rolls back both charges.
pub struct PendingSend {
    slot: PendingSlot,
    item: Queued,
}
impl PendingSend {
    pub fn send(mut self) {
        match self.slot {
            PendingSlot::Control(slot) => {
                slot.send(self.item);
            }
            PendingSlot::Data(queue) => {
                let mut state = queue.state.lock().unwrap();
                queue.stats.enqueued.fetch_add(1, Ordering::Relaxed);
                if state.closed || self.item.gate.as_ref().is_some_and(|g| g.closed()) {
                    queue.stats.dropped_closed.fetch_add(1, Ordering::Relaxed);
                    return;
                }
                self.item.armed = true;
                state.items.push_back(self.item);
                queue.notify.notify_one();
            }
        }
    }
}
struct ReadWrite {
    bytes: Vec<u8>,
    complete: tokio::sync::oneshot::Sender<bool>,
    _reservation: Arc<dyn Send + Sync>,
}
#[derive(Clone)]
pub struct Outbound {
    data: Arc<DataQueue>,
    control_tx: mpsc::Sender<Queued>,
    read_tx: mpsc::Sender<ReadWrite>,
    control_bytes: Arc<Semaphore>,
    limits: Limits,
}
impl Outbound {
    pub fn limits(&self) -> Limits {
        self.limits
    }
    /// One lossless replay frame in flight, with completion at the socket write boundary.
    /// The actor's reservation guard lives through a cancelled/failed queued write.
    pub async fn send_read<T: Send + Sync + 'static>(
        &self,
        frame: &Frame,
        reservation: Arc<T>,
    ) -> Result<(), ErrorCode> {
        if !matches!(frame.frame_type, FrameType::ReadRecord | FrameType::ReadEnd) {
            return Err(ErrorCode::BadFrame);
        }
        let bytes = encode(frame, self.limits).map_err(|_| ErrorCode::ResourceExhausted)?;
        let (complete, reply) = tokio::sync::oneshot::channel();
        self.read_tx
            .send(ReadWrite {
                bytes,
                complete,
                _reservation: reservation,
            })
            .await
            .map_err(|_| ErrorCode::StorageUnavailable)?;
        if reply.await.unwrap_or(false) {
            Ok(())
        } else {
            Err(ErrorCode::StorageUnavailable)
        }
    }
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
        self.try_send_encoded(
            Arc::from(encode(&frame, self.limits).map_err(|_| ErrorCode::BadFrame)?),
            deadline,
        )
    }
    pub fn try_send_encoded(
        &self,
        bytes: Arc<[u8]>,
        deadline: Option<Instant>,
    ) -> Result<(), ErrorCode> {
        self.reserve_encoded(bytes, deadline)?.send();
        Ok(())
    }
    pub fn enqueue(
        &self,
        bytes: Arc<[u8]>,
        deadline: Option<Instant>,
        policy: OverflowPolicy,
        key: Vec<u8>,
        gate: Option<Arc<DeliveryGate>>,
    ) -> Result<EnqueueResult, ErrorCode> {
        if bytes.len() > self.limits.max_frame_bytes || !is_message(&bytes) {
            return Err(ErrorCode::BadFrame);
        }
        self.data.enqueue(bytes, deadline, policy, key, gate)
    }
    pub fn reserve_delivery(
        &self,
        bytes: Arc<[u8]>,
        deadline: Option<Instant>,
        key: Vec<u8>,
        gate: Option<Arc<DeliveryGate>>,
    ) -> Result<PendingSend, ErrorCode> {
        if bytes.len() > self.limits.max_frame_bytes || !is_message(&bytes) {
            return Err(ErrorCode::BadFrame);
        }
        self.data.reserve(bytes, deadline, key, gate)
    }
    pub fn reserve_encoded(
        &self,
        bytes: Arc<[u8]>,
        deadline: Option<Instant>,
    ) -> Result<PendingSend, ErrorCode> {
        if bytes.len() > self.limits.max_frame_bytes {
            return Err(ErrorCode::ResourceExhausted);
        }
        if is_message(&bytes) {
            return self.data.reserve(bytes, deadline, Vec::new(), None);
        }
        let permit = self
            .control_bytes
            .clone()
            .try_acquire_many_owned(
                u32::try_from(bytes.len()).map_err(|_| ErrorCode::ResourceExhausted)?,
            )
            .map_err(|_| ErrorCode::ResourceExhausted)?;
        let slot = self
            .control_tx
            .clone()
            .try_reserve_owned()
            .map_err(|_| ErrorCode::ResourceExhausted)?;
        Ok(PendingSend {
            slot: PendingSlot::Control(slot),
            item: Queued {
                bytes,
                _bytes: permit,
                _slot: None,
                deadline,
                key: Vec::new(),
                gate: None,
                stats: None,
                armed: false,
                rate: None,
                _accounting: None,
            },
        })
    }
}
pub enum SessionReply {
    Immediate(Result<Option<Frame>, ErrorCode>),
    Deferred(Pin<Box<dyn Future<Output = Result<Option<Frame>, ErrorCode>> + Send>>),
    /// Serial read-only batch; detects peer EOF without consuming a pipelined frame.
    ReadOnly(Pin<Box<dyn Future<Output = Result<Option<Frame>, ErrorCode>> + Send>>),
}
pub trait SessionHandler: Send + Sync + 'static {
    fn handle(
        &self,
        session: u128,
        frame: Frame,
        outbound: Outbound,
    ) -> Result<Option<Frame>, ErrorCode>;
    /// Called only after authentication. The principal comes from AUTH, never the request.
    /// Q0 keeps its immediate, allocation-free dispatch; durable work can defer its reply.
    fn request(
        &self,
        session: u128,
        _principal: u128,
        frame: Frame,
        outbound: Outbound,
    ) -> SessionReply {
        SessionReply::Immediate(self.handle(session, frame, outbound))
    }
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
    let mut accept_error = None;
    tokio::pin!(shutdown);
    loop {
        tokio::select! {
            _ = &mut shutdown => break,
            done = sessions.join_next(), if !sessions.is_empty() => { let _ = done; },
            accepted = listener.accept() => {
                // Drain session owners even when the listener fails. The composition
                // root must regain exclusive storage ownership before joining its worker.
                let (mut stream, _) = match accepted {
                    Ok(connection) => connection,
                    Err(error) => { accept_error = Some(error); break; },
                };
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
    match accept_error {
        Some(error) => Err(error),
        None => Ok(()),
    }
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
    let data = DataQueue::new(
        config.outbound_messages,
        config.outbound_bytes,
        config.stats.clone(),
    );
    let (control_tx, mut control_rx) = mpsc::channel::<Queued>(8);
    let (read_tx, mut read_rx) = mpsc::channel::<ReadWrite>(1);
    let mut outbound = Outbound {
        data: data.clone(),
        control_tx,
        read_tx,
        control_bytes: Arc::new(Semaphore::new(4096)),
        limits,
    };
    let write_timeout = config.write_timeout;
    let stats = config.stats.clone();
    let mut writer_task = tokio::spawn(async move {
        loop {
            let item = tokio::select! {biased; item=control_rx.recv(), if !control_rx.is_closed() || !control_rx.is_empty()=>item,
            item=read_rx.recv(), if !read_rx.is_closed() || !read_rx.is_empty()=> {
                let Some(item) = item else { continue; };
                let success = matches!(timeout(write_timeout, writer.write_all(&item.bytes)).await, Ok(Ok(())));
                let _ = item.complete.send(success);
                if !success { return Err(io::Error::new(io::ErrorKind::TimedOut, "replay write failed or timed out")); }
                continue;
            },
            item=data.next()=>item};
            let Some(mut item) = item else {
                break;
            };
            if item.gate.as_ref().is_some_and(|gate| gate.closed()) {
                item.finish();
                stats.dropped_closed.fetch_add(1, Ordering::Relaxed);
                continue;
            }
            if item.deadline.is_some_and(|d| Instant::now() >= d) {
                item.finish();
                stats.writer_expired.fetch_add(1, Ordering::Relaxed);
                if let Some(gate) = &item.gate {
                    gate.refund(1, item.bytes.len() as u32);
                }
                continue;
            }
            // Charge attempts conservatively: partial failed writes cannot refund
            // their complete estimated physical egress cost.
            if let Some(rate) = item.rate.take() {
                rate.commit();
            }
            match timeout(write_timeout, write_queued(&mut writer, &item)).await {
                Ok(Ok(())) => {
                    if is_message(&item.bytes) {
                        stats.delivered.fetch_add(1, Ordering::Relaxed);
                        stats
                            .delivered_bytes
                            .fetch_add(item.bytes.len() as u64, Ordering::Relaxed);
                    }
                }
                _ => {
                    item.finish();
                    if is_message(&item.bytes) {
                        stats.write_failed.fetch_add(1, Ordering::Relaxed);
                    }
                    return Err(io::Error::new(
                        io::ErrorKind::TimedOut,
                        "socket write failed or timed out",
                    ));
                }
            }
            item.finish();
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
                        Some(
                            CodecError::UnsupportedCriticalField(_)
                            | CodecError::UnknownFrameType(_),
                        ) => ErrorCode::UnsupportedCapability,
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
            _ => match handler.request(id, 1, frame, outbound.clone()) {
                SessionReply::Immediate(result) => result,
                SessionReply::ReadOnly(reply) => {
                    let peer_closed = async {
                        let mut byte = [0];
                        match reader.peek(&mut byte).await {
                            Ok(0) | Err(_) => (),
                            // Preserve unread input; this serial profile does not pipeline batches.
                            Ok(_) => std::future::pending::<()>().await,
                        }
                    };
                    tokio::select! {
                        result = reply => result,
                        _ = peer_closed => break,
                        _ = stop.changed() => break,
                        _ = &mut writer_task => return Ok(()),
                    }
                }
                SessionReply::Deferred(reply) => tokio::select! {
                    result = reply => result,
                    _ = stop.changed() => break,
                    _ = &mut writer_task => return Ok(()),
                },
            },
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
    outbound.data.close();
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
    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn listener_failure_drains_deferred_sessions_before_returning() {
        struct Deferred {
            started: Notify,
            disconnected: std::sync::atomic::AtomicBool,
        }
        impl SessionHandler for Deferred {
            fn handle(&self, _: u128, _: Frame, _: Outbound) -> Result<Option<Frame>, ErrorCode> {
                unreachable!("this handler defers active requests")
            }
            fn request(&self, _: u128, principal: u128, _: Frame, _: Outbound) -> SessionReply {
                assert_eq!(principal, 1);
                self.started.notify_one();
                SessionReply::Deferred(Box::pin(std::future::pending()))
            }
            fn disconnected(&self, _: u128) {
                self.disconnected.store(true, Ordering::Release);
            }
        }
        let standard = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        standard.set_nonblocking(true).unwrap();
        let control = standard.try_clone().unwrap();
        let address = standard.local_addr().unwrap();
        let listener = TcpListener::from_std(standard).unwrap();
        let handler = Arc::new(Deferred {
            started: Notify::new(),
            disconnected: std::sync::atomic::AtomicBool::new(false),
        });
        let task = tokio::spawn(serve(
            listener,
            Config::default(),
            handler.clone(),
            std::future::pending(),
        ));
        let mut client = TcpStream::connect(address).await.unwrap();
        assert_eq!(
            exchange(&mut client, hello()).await.frame_type,
            FrameType::Welcome
        );
        let mut auth = Frame::new(FrameType::Auth, 2);
        auth.fields = vec![
            Field::required(field::AUTH_METHOD, [0]),
            Field::required(field::AUTH_DATA, b"dev".to_vec()),
        ];
        assert_eq!(
            exchange(&mut client, auth).await.frame_type,
            FrameType::AuthOk
        );
        write_frame(
            &mut client,
            &Frame::new(FrameType::Drain, 3),
            Limits::default(),
        )
        .await
        .unwrap();
        timeout(Duration::from_secs(5), handler.started.notified())
            .await
            .unwrap();
        // Linux shutdown of the listening socket deterministically makes accept fail.
        socket2::SockRef::from(&control)
            .shutdown(std::net::Shutdown::Both)
            .unwrap();
        assert!(
            timeout(Duration::from_secs(5), task)
                .await
                .unwrap()
                .unwrap()
                .is_err()
        );
        assert!(handler.disconnected.load(Ordering::Acquire));
        assert_eq!(
            Arc::strong_count(&handler),
            1,
            "no session may retain the storage owner"
        );
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
            data: DataQueue::new(1, 128, Arc::new(TransportStats::default())),
            limits: Limits::default(),
            read_tx: mpsc::channel(1).0,
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
        let (control_tx, mut control_rx) = mpsc::channel(1);
        let out = Outbound {
            data: DataQueue::new(1, 512, Arc::new(TransportStats::default())),
            control_tx,
            control_bytes: Arc::new(Semaphore::new(128)),
            limits: Limits::default(),
            read_tx: mpsc::channel(1).0,
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
        assert_eq!(out.data.bytes.available_permits(), 512);
        out.reserve_encoded(encoded, None).unwrap().send();
        let mut item = out.data.next().await.unwrap();
        item.finish();
        drop(item);
        assert_eq!(out.data.bytes.available_permits(), 512);
    }
    fn message_bytes(id: u128, body: &[u8]) -> Arc<[u8]> {
        let mut message = Frame::new(FrameType::Message, 0);
        message.fields = vec![
            Field::required(field::TOPIC_NAME, b"demo".to_vec()),
            Field::required(field::QOS, [0]),
            Field::required(field::MESSAGE_ID, id.to_be_bytes()),
            Field::optional(field::SUBSCRIPTION_ID, 0u64.to_be_bytes()),
        ];
        message.payload = body.to_vec();
        encode(&message, Limits::default()).unwrap().into()
    }
    #[tokio::test]
    async fn zero_credit_overflow_policies_preserve_exact_survivors() {
        for policy in [
            OverflowPolicy::DropNewest,
            OverflowPolicy::DropOldest,
            OverflowPolicy::CoalesceLatest,
        ] {
            let stats = Arc::new(TransportStats::default());
            let queue = DataQueue::new(2, 2048, stats.clone());
            let gate = DeliveryGate::new(8, 4096);
            queue
                .enqueue(
                    message_bytes(1, b"first"),
                    None,
                    policy,
                    b"a".to_vec(),
                    Some(gate.clone()),
                )
                .unwrap();
            queue
                .enqueue(
                    message_bytes(2, b"second"),
                    None,
                    policy,
                    b"b".to_vec(),
                    Some(gate.clone()),
                )
                .unwrap();
            let result = queue.enqueue(
                message_bytes(3, b"third"),
                None,
                policy,
                b"a".to_vec(),
                Some(gate.clone()),
            );
            assert_eq!(result.is_err(), policy == OverflowPolicy::DropNewest);
            assert!(
                timeout(Duration::from_millis(5), queue.next())
                    .await
                    .is_err()
            );
            gate.add(2, 2048).unwrap();
            let mut ids = Vec::new();
            for _ in 0..2 {
                let mut item = queue.next().await.unwrap();
                let frame = decode_prefix(&item.bytes, Limits::default())
                    .unwrap()
                    .unwrap()
                    .0;
                ids.push(u128::from_be_bytes(
                    frame.field(field::MESSAGE_ID).unwrap().try_into().unwrap(),
                ));
                item.finish();
            }
            assert_eq!(
                ids,
                if policy == OverflowPolicy::DropNewest {
                    vec![1, 2]
                } else {
                    vec![2, 3]
                }
            );
            assert_eq!(queue.bytes.available_permits(), 2048);
            assert_eq!(queue.slots.available_permits(), 2);
            assert_eq!(stats.dropped_shutdown.load(Ordering::Relaxed), 0);
        }
    }
    #[tokio::test]
    async fn credit_ttl_close_and_rejected_credit_leave_bounds_intact() {
        let stats = Arc::new(TransportStats::default());
        let queue = DataQueue::new(2, 2048, stats.clone());
        let gate = DeliveryGate::new(2, 2048);
        assert_eq!(gate.add(3, 0), Err(ErrorCode::QuotaExceeded));
        assert_eq!(gate.available(), (0, 0));
        queue
            .enqueue(
                message_bytes(1, b"expire"),
                Some(Instant::now() + Duration::from_millis(5)),
                OverflowPolicy::DropNewest,
                Vec::new(),
                Some(gate.clone()),
            )
            .unwrap();
        assert!(
            timeout(Duration::from_millis(15), queue.next())
                .await
                .is_err()
        );
        assert_eq!(stats.writer_expired.load(Ordering::Relaxed), 1);
        queue
            .enqueue(
                message_bytes(2, b"close"),
                None,
                OverflowPolicy::DropNewest,
                Vec::new(),
                Some(gate.clone()),
            )
            .unwrap();
        gate.close();
        assert!(
            timeout(Duration::from_millis(5), queue.next())
                .await
                .is_err()
        );
        assert_eq!(stats.dropped_closed.load(Ordering::Relaxed), 1);
        assert_eq!(queue.bytes.available_permits(), 2048);
        assert_eq!(queue.slots.available_permits(), 2);
        queue.close();
        assert!(queue.next().await.is_none());
    }
    #[tokio::test]
    async fn shared_payload_subscription_patch_and_rate_refill() {
        let stats = Arc::new(TransportStats::default());
        let queue = DataQueue::new(2, 2048, stats.clone());
        let gate = DeliveryGate::new(2, 2048);
        gate.set_subscription_id(91);
        gate.add(2, 2048).unwrap();
        gate.set_quotas(vec![RateQuota::new(1024 * 1024, 100)]);
        let bytes = message_bytes(1, b"shared");
        queue
            .enqueue(
                bytes.clone(),
                None,
                OverflowPolicy::DropNewest,
                Vec::new(),
                Some(gate.clone()),
            )
            .unwrap();
        let mut item = queue.next().await.unwrap();
        assert!(Arc::ptr_eq(&bytes, &item.bytes));
        let (mut writer, mut reader) = tokio::io::duplex(2048);
        write_queued(&mut writer, &item).await.unwrap();
        let frame = read_frame(&mut reader, Limits::default())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            frame.field(field::SUBSCRIPTION_ID),
            Some(&91u64.to_be_bytes()[..])
        );
        assert_eq!(frame.payload, b"shared");
        item.finish();
        let quota = RateQuota::new(1024 * 1024, 100);
        let reservation = RateQuota::reserve(&[quota.clone()], 0, 100).unwrap();
        reservation.commit();
        gate.set_quotas(vec![quota]);
        queue
            .enqueue(
                message_bytes(2, b"wait-rate"),
                Some(Instant::now() + Duration::from_secs(1)),
                OverflowPolicy::DropNewest,
                Vec::new(),
                Some(gate),
            )
            .unwrap();
        assert!(
            timeout(Duration::from_millis(1), queue.next())
                .await
                .is_err()
        );
        let mut next = timeout(Duration::from_millis(100), queue.next())
            .await
            .unwrap()
            .unwrap();
        next.finish();
    }
    #[tokio::test]
    async fn hierarchical_egress_accounting_survives_eviction_cancel_and_close() {
        let stats = Arc::new(TransportStats::default());
        let queue = DataQueue::new(4, 4096, stats.clone());
        let gate = DeliveryGate::new(4, 4096);
        let global = Budget::new(2, 4096);
        let topic = Budget::new(1, 4096);
        gate.set_budgets(vec![global.clone(), topic.clone()]);
        let pending = queue
            .reserve(
                message_bytes(1, b"cancel"),
                None,
                b"a".to_vec(),
                Some(gate.clone()),
            )
            .unwrap();
        assert_eq!(global.usage().messages, 1);
        drop(pending);
        assert_eq!(global.usage().messages, 0);
        queue
            .enqueue(
                message_bytes(2, b"old"),
                None,
                OverflowPolicy::DropNewest,
                b"a".to_vec(),
                Some(gate.clone()),
            )
            .unwrap();
        assert!(
            queue
                .enqueue(
                    message_bytes(3, b"reject"),
                    None,
                    OverflowPolicy::DropNewest,
                    b"b".to_vec(),
                    Some(gate.clone())
                )
                .is_err()
        );
        queue
            .enqueue(
                message_bytes(4, b"replace"),
                None,
                OverflowPolicy::DropOldest,
                b"b".to_vec(),
                Some(gate.clone()),
            )
            .unwrap();
        assert_eq!(topic.usage().messages, 1);
        assert_eq!(global.usage().messages, 1);
        queue.close();
        assert_eq!(global.usage().messages, 0);
        assert_eq!(topic.usage().messages, 0);
        assert_eq!(stats.enqueued.load(Ordering::Relaxed), 2);
        assert_eq!(stats.dropped_oldest.load(Ordering::Relaxed), 1);
        assert_eq!(stats.dropped_closed.load(Ordering::Relaxed), 1);
        assert_eq!(stats.dropped_shutdown.load(Ordering::Relaxed), 0);
    }
    #[tokio::test]
    async fn credit_blocked_head_cannot_be_overtaken_within_subscription() {
        let queue = DataQueue::new(2, 4096, Arc::new(TransportStats::default()));
        let gate = DeliveryGate::new(2, 8192);
        let large = message_bytes(1, &[1; 1024]);
        let small = message_bytes(2, b"small");
        for bytes in [large.clone(), small.clone()] {
            queue
                .enqueue(
                    bytes,
                    None,
                    OverflowPolicy::DropNewest,
                    b"same-key".to_vec(),
                    Some(gate.clone()),
                )
                .unwrap();
        }
        gate.add(1, small.len() as u32).unwrap();
        assert!(
            timeout(Duration::from_millis(5), queue.next())
                .await
                .is_err(),
            "a newer copy cannot bypass a credit-blocked head"
        );
        gate.add(0, (large.len() - small.len()) as u32).unwrap();
        let mut first = queue.next().await.unwrap();
        assert!(Arc::ptr_eq(&first.bytes, &large));
        first.finish();
        drop(first);
        gate.add(1, small.len() as u32).unwrap();
        let mut second = queue.next().await.unwrap();
        assert!(Arc::ptr_eq(&second.bytes, &small));
        second.finish();
    }
    #[tokio::test]
    async fn replay_write_completion_is_lossless_and_cancelled_write_retains_its_guard() {
        let (control_tx, _control_rx) = mpsc::channel(1);
        let (read_tx, mut read_rx) = mpsc::channel(1);
        let out = Outbound {
            data: DataQueue::new(1, 128, Arc::new(TransportStats::default())),
            control_tx,
            read_tx,
            control_bytes: Arc::new(Semaphore::new(128)),
            limits: Limits::default(),
        };
        let scope = rovqet_protocol::replay::ReadScope {
            tenant: 1,
            topic: 3,
            partition: 0,
            incarnation: 42,
        };
        let frame = rovqet_protocol::replay::record(1, scope, 0, 100, vec![1; 8192]);
        let guard = Arc::new(());
        let weak = Arc::downgrade(&guard);
        let writer = tokio::spawn({
            let out = out.clone();
            let frame = frame.clone();
            let guard = guard.clone();
            async move { out.send_read(&frame, guard).await }
        });
        let item = read_rx.recv().await.unwrap();
        assert!(!writer.is_finished());
        assert!(item.bytes.len() > 4096);
        assert_eq!(out.control_bytes.available_permits(), 128);
        let _ = item.complete.send(true);
        drop(item._reservation);
        assert_eq!(writer.await.unwrap(), Ok(()));
        let writer = tokio::spawn({
            let out = out.clone();
            let guard = guard.clone();
            async move { out.send_read(&frame, guard).await }
        });
        let item = read_rx.recv().await.unwrap();
        writer.abort();
        let _ = writer.await;
        drop(guard);
        assert!(
            weak.upgrade().is_some(),
            "queued writer must retain the actor reservation"
        );
        drop(item);
        assert!(weak.upgrade().is_none());
    }
}
