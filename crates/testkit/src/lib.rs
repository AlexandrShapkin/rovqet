//! Independent workload identities, append-only event evidence and Q0 oracle.
//!
//! This library never inspects broker state. `Admitted` is a volatile outcome;
//! only independently received, digest-verified subscriber copies close an
//! expected delivery. Unknown outcomes remain unresolved rather than definite loss.
#![forbid(unsafe_code)]

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, HashMap};
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, BufWriter, Read, Write};
use std::path::Path;
use std::sync::{Arc, Mutex, mpsc};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

const MAX_RECIPIENT_SLOTS: usize = 5_000_000;
const MAX_JSONL_LINE: u64 = 16 * 1024;
const MAGIC: &[u8; 4] = b"RQT1";

/// SplitMix64: a pinned deterministic sequence, independent of platform hash seeds.
fn next_random(state: &mut u64) -> u64 {
    *state = state.wrapping_add(0x9e3779b97f4a7c15);
    let mut value = *state;
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d049bb133111eb);
    value ^ (value >> 31)
}

/// Deterministic payload, including an independently recoverable global ID.
/// The first twenty bytes are magic, global message ID and publisher ID (BE).
/// Callers validate a minimum size before selecting their payload profile.
pub fn payload(seed: u64, id: u64, publisher: usize, size: usize) -> Vec<u8> {
    assert!(size >= 20, "test payload must contain its identity");
    let mut bytes = vec![0; size];
    bytes[..4].copy_from_slice(MAGIC);
    bytes[4..12].copy_from_slice(&id.to_be_bytes());
    bytes[12..20].copy_from_slice(&(publisher as u64).to_be_bytes());
    let mut state = seed ^ id.rotate_left(17) ^ (publisher as u64).rotate_left(41);
    for chunk in bytes[20..].chunks_mut(8) {
        let word = next_random(&mut state).to_be_bytes();
        chunk.copy_from_slice(&word[..chunk.len()]);
    }
    bytes
}

pub fn payload_identity(bytes: &[u8]) -> Result<(u64, usize), String> {
    if bytes.len() < 20 || &bytes[..4] != MAGIC {
        return Err("invalid workload payload identity".into());
    }
    let id = u64::from_be_bytes(bytes[4..12].try_into().unwrap());
    let publisher = u64::from_be_bytes(bytes[12..20].try_into().unwrap());
    Ok((
        id,
        usize::try_from(publisher).map_err(|_| "publisher identity overflow")?,
    ))
}

pub fn digest(bytes: &[u8]) -> String {
    let value = Sha256::digest(bytes);
    value.iter().map(|byte| format!("{byte:02x}")).collect()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Outcome {
    Admitted,
    Rejected,
    Unknown,
    ClientDropped,
}

/// Monotonic offsets use one generator-process origin, never wall-clock subtraction.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "event", rename_all = "snake_case", deny_unknown_fields)]
pub enum Event {
    Offered {
        id: u64,
        publisher: usize,
        at_ns: u64,
        measured: bool,
        digest: String,
    },
    Sent {
        id: u64,
        at_ns: u64,
    },
    Ack {
        id: u64,
        at_ns: u64,
        outcome: Outcome,
    },
    Delivery {
        id: u64,
        subscriber: usize,
        at_ns: u64,
        digest: String,
    },
    Connection {
        client: String,
        connected: bool,
        authenticated: bool,
        at_ns: u64,
        elapsed_ns: u64,
    },
    Error {
        message: String,
        at_ns: u64,
    },
}

/// Exact nanosecond histogram. Raw latency values are preserved as value/count bins.
/// Quantiles use nearest rank over the complete distribution, never averaged percentiles.
#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LatencyDistribution {
    pub count: u64,
    pub p50_ns: Option<u64>,
    pub p95_ns: Option<u64>,
    pub p99_ns: Option<u64>,
    pub p999_ns: Option<u64>,
    pub max_ns: Option<u64>,
    pub buckets_ns: BTreeMap<u64, u64>,
}
impl LatencyDistribution {
    pub fn record(&mut self, value: u64) {
        *self.buckets_ns.entry(value).or_default() += 1;
        self.count += 1;
    }
    pub fn percentile(&self, numerator: u64, denominator: u64) -> Option<u64> {
        if self.count == 0 || denominator == 0 || numerator > denominator {
            return None;
        }
        let rank = ((self.count as u128 * numerator as u128).div_ceil(denominator as u128)).max(1);
        let mut cumulative = 0u128;
        for (&value, &count) in &self.buckets_ns {
            cumulative += count as u128;
            if cumulative >= rank {
                return Some(value);
            }
        }
        None
    }
    pub fn finalized(mut self) -> Self {
        self.p50_ns = self.percentile(50, 100);
        self.p95_ns = self.percentile(95, 100);
        self.p99_ns = self.percentile(99, 100);
        self.p999_ns = self.percentile(999, 1000);
        self.max_ns = self.buckets_ns.keys().next_back().copied();
        self
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Summary {
    pub offered: u64,
    pub sent: u64,
    pub admitted: u64,
    pub rejected: u64,
    pub unknown: u64,
    pub client_dropped: u64,
    /// All received copies of measured IDs, including duplicate/corrupt copies.
    pub delivered: u64,
    /// Valid digest-matching unique (message, subscriber) copies only.
    pub unique_delivered: u64,
    pub delivery_attempts: u64,
    pub expected_fanout_deliveries: u64,
    pub missing_admitted: u64,
    pub unknown_expected_fanout: u64,
    pub unknown_undelivered: u64,
    pub duplicates: u64,
    pub corrupt: u64,
    pub unexpected: u64,
    pub ordering_violations: u64,
    pub incomplete_outcomes: u64,
    pub incomplete: bool,
    pub warmup_offered: u64,
    pub warmup_delivered: u64,
    pub connected: u64,
    pub authenticated: u64,
    pub connection_failures: u64,
    pub disconnected: u64,
    pub errors: u64,
    /// Unobservable without broker telemetry: Q0 missing is not assumed expired/dropped.
    pub expired_or_intentionally_dropped: Option<u64>,
    pub ack_latency: LatencyDistribution,
    pub e2e_latency: LatencyDistribution,
    pub offered_to_delivery_latency: LatencyDistribution,
    pub connection_latency: LatencyDistribution,
    pub disconnect_latency: LatencyDistribution,
    pub deliveries_per_subscriber: Vec<u64>,
}

struct Message {
    publisher: usize,
    offered: u64,
    measured: bool,
    digest: String,
    sent: Option<u64>,
    outcome: Option<Outcome>,
    // Bitsets preserve exact fan-out identity at one bit per expected recipient.
    seen: Vec<u64>,
    valid: Vec<u64>,
}
impl Message {
    fn mark(bits: &mut [u64], subscriber: usize) -> bool {
        let word = subscriber / 64;
        let mask = 1u64 << (subscriber % 64);
        let prior = bits[word] & mask != 0;
        bits[word] |= mask;
        prior
    }
    fn valid_count(&self) -> u64 {
        self.valid.iter().map(|v| u64::from(v.count_ones())).sum()
    }
}

/// Bounded independent set oracle. The bound includes warmup messages.
pub struct Oracle {
    subscribers: usize,
    max_messages: usize,
    verify_order: bool,
    messages: BTreeMap<u64, Message>,
    last: HashMap<(usize, usize), u64>,
    totals: Summary,
}
impl Oracle {
    pub fn new(
        subscribers: usize,
        max_messages: usize,
        verify_order: bool,
    ) -> Result<Self, String> {
        if subscribers == 0
            || max_messages == 0
            || max_messages > MAX_RECIPIENT_SLOTS
            || subscribers
                .checked_mul(max_messages)
                .is_none_or(|v| v > MAX_RECIPIENT_SLOTS)
        {
            return Err(
                "oracle exceeds bounded message/recipient capacity (5,000,000 slots)".into(),
            );
        }
        Ok(Self {
            subscribers,
            max_messages,
            verify_order,
            messages: BTreeMap::new(),
            last: HashMap::new(),
            totals: Summary {
                offered: 0,
                sent: 0,
                admitted: 0,
                rejected: 0,
                unknown: 0,
                client_dropped: 0,
                delivered: 0,
                unique_delivered: 0,
                delivery_attempts: 0,
                expected_fanout_deliveries: 0,
                missing_admitted: 0,
                unknown_expected_fanout: 0,
                unknown_undelivered: 0,
                duplicates: 0,
                corrupt: 0,
                unexpected: 0,
                ordering_violations: 0,
                incomplete_outcomes: 0,
                incomplete: false,
                warmup_offered: 0,
                warmup_delivered: 0,
                connected: 0,
                authenticated: 0,
                connection_failures: 0,
                disconnected: 0,
                errors: 0,
                expired_or_intentionally_dropped: None,
                ack_latency: LatencyDistribution::default(),
                e2e_latency: LatencyDistribution::default(),
                offered_to_delivery_latency: LatencyDistribution::default(),
                connection_latency: LatencyDistribution::default(),
                disconnect_latency: LatencyDistribution::default(),
                deliveries_per_subscriber: vec![0; subscribers],
            },
        })
    }
    pub fn observe(&mut self, event: &Event) -> Result<(), String> {
        match event {
            Event::Offered {
                id,
                publisher,
                at_ns,
                measured,
                digest,
            } => {
                if self.messages.len() >= self.max_messages || self.messages.contains_key(id) {
                    return Err("oracle message bound exhausted or duplicate offered ID".into());
                }
                if digest.len() != 64 || !digest.bytes().all(|v| v.is_ascii_hexdigit()) {
                    return Err("invalid SHA256 digest".into());
                }
                let words = self.subscribers.div_ceil(64);
                self.messages.insert(
                    *id,
                    Message {
                        publisher: *publisher,
                        offered: *at_ns,
                        measured: *measured,
                        digest: digest.clone(),
                        sent: None,
                        outcome: None,
                        seen: vec![0; words],
                        valid: vec![0; words],
                    },
                );
                if *measured {
                    self.totals.offered += 1;
                } else {
                    self.totals.warmup_offered += 1;
                }
            }
            Event::Sent { id, at_ns } => {
                let m = self.messages.get_mut(id).ok_or("send without offer")?;
                if m.sent.is_some() || *at_ns < m.offered || m.outcome.is_some() {
                    return Err("invalid/repeated send event".into());
                }
                m.sent = Some(*at_ns);
                if m.measured {
                    self.totals.sent += 1;
                }
            }
            Event::Ack { id, at_ns, outcome } => {
                let m = self.messages.get_mut(id).ok_or("outcome without offer")?;
                if m.outcome.is_some() || *at_ns < m.offered {
                    return Err("invalid/repeated outcome event".into());
                }
                if *outcome != Outcome::ClientDropped && m.sent.is_none() {
                    return Err("publication outcome without send".into());
                }
                if *outcome == Outcome::ClientDropped && m.sent.is_some() {
                    return Err("sent publication cannot be client-dropped".into());
                }
                m.outcome = Some(*outcome);
                if m.measured {
                    match outcome {
                        Outcome::Admitted => self.totals.admitted += 1,
                        Outcome::Rejected => self.totals.rejected += 1,
                        Outcome::Unknown => self.totals.unknown += 1,
                        Outcome::ClientDropped => self.totals.client_dropped += 1,
                    }
                    if matches!(outcome, Outcome::Admitted | Outcome::Rejected) {
                        let latency = at_ns
                            .checked_sub(m.sent.unwrap())
                            .ok_or("outcome predates send")?;
                        self.totals.ack_latency.record(latency);
                    }
                }
            }
            Event::Delivery {
                id,
                subscriber,
                at_ns,
                digest,
            } => {
                self.totals.delivery_attempts += 1;
                if *subscriber >= self.subscribers {
                    self.totals.unexpected += 1;
                    return Ok(());
                }
                let Some(m) = self.messages.get_mut(id) else {
                    self.totals.unexpected += 1;
                    return Ok(());
                };
                if *at_ns < m.offered {
                    return Err("delivery predates offer on same monotonic clock".into());
                }
                if m.measured {
                    self.totals.delivered += 1;
                    self.totals.deliveries_per_subscriber[*subscriber] += 1;
                } else {
                    self.totals.warmup_delivered += 1;
                }
                let duplicate = Message::mark(&mut m.seen, *subscriber);
                if duplicate && m.measured {
                    self.totals.duplicates += 1;
                }
                if *digest != m.digest {
                    if m.measured {
                        self.totals.corrupt += 1;
                    }
                    return Ok(());
                }
                if Message::mark(&mut m.valid, *subscriber) {
                    return Ok(());
                }
                if m.measured {
                    self.totals.unique_delivered += 1;
                    let sent = m.sent.ok_or("delivery without send")?;
                    let latency = at_ns.checked_sub(sent).ok_or("delivery predates send")?;
                    self.totals.e2e_latency.record(latency);
                    self.totals
                        .offered_to_delivery_latency
                        .record(at_ns - m.offered);
                    if self.verify_order {
                        let prior = self.last.entry((*subscriber, m.publisher)).or_insert(*id);
                        if *id < *prior {
                            self.totals.ordering_violations += 1;
                        }
                        *prior = (*prior).max(*id);
                    }
                }
            }
            Event::Connection {
                connected,
                authenticated,
                elapsed_ns,
                ..
            } => {
                if *connected {
                    self.totals.connected += 1;
                    self.totals.connection_latency.record(*elapsed_ns);
                    if *authenticated {
                        self.totals.authenticated += 1;
                    } else {
                        self.totals.connection_failures += 1;
                    }
                } else if *authenticated {
                    self.totals.disconnected += 1;
                    self.totals.disconnect_latency.record(*elapsed_ns);
                } else {
                    self.totals.connection_failures += 1;
                }
            }
            Event::Error { .. } => {
                self.totals.errors += 1;
            }
        }
        Ok(())
    }
    pub fn summary(&self) -> Summary {
        let mut result = self.totals.clone();
        for message in self.messages.values().filter(|m| m.measured) {
            let valid = message.valid_count();
            match message.outcome {
                Some(Outcome::Admitted) => {
                    result.expected_fanout_deliveries += self.subscribers as u64;
                    result.missing_admitted += self.subscribers as u64 - valid;
                }
                Some(Outcome::Unknown) => {
                    result.unknown_expected_fanout += self.subscribers as u64;
                    result.unknown_undelivered += self.subscribers as u64 - valid;
                }
                Some(Outcome::Rejected | Outcome::ClientDropped) => {
                    result.unexpected += valid;
                }
                None => {
                    result.incomplete_outcomes += 1;
                }
            }
        }
        result.incomplete = result.incomplete_outcomes > 0 || result.errors > 0;
        result.ack_latency = result.ack_latency.finalized();
        result.e2e_latency = result.e2e_latency.finalized();
        result.offered_to_delivery_latency = result.offered_to_delivery_latency.finalized();
        result.connection_latency = result.connection_latency.finalized();
        result.disconnect_latency = result.disconnect_latency.finalized();
        result
    }
}

/// Bounded producer handle; saturation is an invalid run, never silent evidence loss.
enum LogCommand {
    Event(Event),
    Finish,
}
#[derive(Clone)]
pub struct EventWriter {
    tx: mpsc::SyncSender<LogCommand>,
    failure: Arc<Mutex<Option<String>>>,
    closed: Arc<Mutex<bool>>,
}
impl EventWriter {
    pub fn record(&self, event: &Event) -> Result<(), String> {
        let closed = self.closed.lock().unwrap();
        if *closed {
            return Err("JSONL evidence log closed".into());
        }
        if let Some(error) = self.failure.lock().unwrap().as_ref() {
            return Err(error.clone());
        }
        self.tx
            .try_send(LogCommand::Event(event.clone()))
            .map_err(|error| {
                let message = format!("JSONL evidence queue saturated or disconnected: {error}");
                *self.failure.lock().unwrap() = Some(message.clone());
                message
            })
    }
}
pub struct EventLog {
    writer: Option<EventWriter>,
    thread: Option<JoinHandle<Result<(), String>>>,
}
impl EventLog {
    pub fn create(path: impl AsRef<Path>, capacity: usize) -> Result<Self, String> {
        if capacity == 0 {
            return Err("zero evidence queue capacity".into());
        }
        // Refuse accidental overwrite or append to evidence from another run.
        let file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(path)
            .map_err(|e| e.to_string())?;
        let (tx, rx) = mpsc::sync_channel::<LogCommand>(capacity);
        let failure = Arc::new(Mutex::new(None));
        let thread = thread::spawn(move || {
            let mut output = BufWriter::with_capacity(256 * 1024, file);
            let mut count = 0usize;
            let mut last = Instant::now();
            loop {
                match rx.recv_timeout(Duration::from_millis(100)) {
                    Ok(LogCommand::Event(event)) => {
                        serde_json::to_writer(&mut output, &event).map_err(|e| e.to_string())?;
                        output.write_all(b"\n").map_err(|e| e.to_string())?;
                        count += 1;
                    }
                    Ok(LogCommand::Finish) => break,
                    Err(mpsc::RecvTimeoutError::Timeout) => {}
                    Err(mpsc::RecvTimeoutError::Disconnected) => break,
                }
                if count >= 4096 || last.elapsed() >= Duration::from_millis(100) {
                    output.flush().map_err(|e| e.to_string())?;
                    count = 0;
                    last = Instant::now();
                }
            }
            output.flush().map_err(|e| e.to_string())?;
            output.get_ref().sync_data().map_err(|e| e.to_string())
        });
        Ok(Self {
            writer: Some(EventWriter {
                tx,
                failure,
                closed: Arc::new(Mutex::new(false)),
            }),
            thread: Some(thread),
        })
    }
    pub fn writer(&self) -> EventWriter {
        self.writer.as_ref().expect("finished event log").clone()
    }
    pub fn record(&self, event: &Event) -> Result<(), String> {
        self.writer
            .as_ref()
            .ok_or("finished event log")?
            .record(event)
    }
    /// Controlled final flush; producer handles reject records once closing starts.
    pub fn finish(mut self) -> Result<(), String> {
        let writer = self.writer.take().unwrap();
        {
            let mut closed = writer.closed.lock().unwrap();
            *closed = true;
            let _ = writer.tx.send(LogCommand::Finish);
        }
        let failure = writer.failure.clone();
        let result = self
            .thread
            .take()
            .unwrap()
            .join()
            .map_err(|_| "JSONL writer panicked")?;
        result?;
        let error = failure.lock().unwrap().clone();
        if let Some(error) = error {
            Err(error)
        } else {
            Ok(())
        }
    }
}

/// Replay JSONL independently, rejecting malformed/truncated/oversized lines.
pub fn replay(path: impl AsRef<Path>, oracle: &mut Oracle) -> Result<(), String> {
    let file = File::open(path).map_err(|e| e.to_string())?;
    let mut reader = BufReader::new(file);
    let mut line = Vec::new();
    let mut number = 0;
    loop {
        line.clear();
        let count = reader
            .by_ref()
            .take(MAX_JSONL_LINE + 1)
            .read_until(b'\n', &mut line)
            .map_err(|e| e.to_string())?;
        if count == 0 {
            break;
        }
        number += 1;
        if count as u64 > MAX_JSONL_LINE || line.last() != Some(&b'\n') {
            return Err(format!("truncated/oversized JSONL line {number}"));
        }
        let event: Event = serde_json::from_slice(&line)
            .map_err(|e| format!("invalid JSONL line {number}: {e}"))?;
        oracle
            .observe(&event)
            .map_err(|e| format!("invalid event at line {number}: {e}"))?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};
    static TEMP_ID: AtomicU64 = AtomicU64::new(0);
    fn path() -> std::path::PathBuf {
        std::env::temp_dir().join(format!(
            "rovqet-oracle-{}-{}.jsonl",
            std::process::id(),
            TEMP_ID.fetch_add(1, Ordering::Relaxed)
        ))
    }
    fn offered(id: u64, measured: bool) -> Event {
        Event::Offered {
            id,
            publisher: 0,
            at_ns: 10,
            measured,
            digest: digest(&payload(42, id, 0, 256)),
        }
    }
    fn delivery(id: u64, subscriber: usize, at_ns: u64) -> Event {
        Event::Delivery {
            id,
            subscriber,
            at_ns,
            digest: digest(&payload(42, id, 0, 256)),
        }
    }
    fn send(oracle: &mut Oracle, id: u64, outcome: Outcome) {
        oracle.observe(&offered(id, true)).unwrap();
        oracle.observe(&Event::Sent { id, at_ns: 20 }).unwrap();
        oracle
            .observe(&Event::Ack {
                id,
                at_ns: 40,
                outcome,
            })
            .unwrap();
    }
    #[test]
    fn deterministic_payload_has_fixed_independent_identity_and_sha_fixture() {
        let a = payload(42, 7, 3, 256);
        assert_eq!(a, payload(42, 7, 3, 256));
        assert_ne!(a, payload(43, 7, 3, 256));
        assert_eq!(
            &a[..20],
            &[
                b'R', b'Q', b'T', b'1', 0, 0, 0, 0, 0, 0, 0, 7, 0, 0, 0, 0, 0, 0, 0, 3
            ]
        );
        assert_eq!(payload_identity(&a).unwrap(), (7, 3));
        assert!(payload_identity(b"not a workload").is_err());
        // SHA-256 standard test vector is independent of the workload encoder.
        assert_eq!(
            digest(b"abc"),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
        assert_eq!(a.len(), 256);
        assert_eq!(payload(42, 7, 3, 1024).len(), 1024);
        assert_eq!(
            &a[20..28],
            &[0xae, 0xe9, 0x59, 0xb5, 0x5d, 0x88, 0x39, 0x27]
        );
    }
    #[test]
    fn artificial_transport_fixture_accounts_missing_duplicates_corruption_and_unknown() {
        let mut oracle = Oracle::new(2, 10, true).unwrap();
        send(&mut oracle, 1, Outcome::Admitted);
        send(&mut oracle, 2, Outcome::Unknown);
        send(&mut oracle, 3, Outcome::Rejected);
        oracle.observe(&offered(4, true)).unwrap();
        oracle
            .observe(&Event::Ack {
                id: 4,
                at_ns: 12,
                outcome: Outcome::ClientDropped,
            })
            .unwrap();
        oracle.observe(&delivery(1, 0, 50)).unwrap();
        oracle.observe(&delivery(1, 0, 60)).unwrap();
        let mut corrupt = delivery(1, 1, 60);
        if let Event::Delivery { digest, .. } = &mut corrupt {
            *digest = "0".repeat(64);
        }
        oracle.observe(&corrupt).unwrap();
        oracle.observe(&delivery(2, 1, 70)).unwrap();
        oracle.observe(&delivery(99, 0, 80)).unwrap();
        let result = oracle.summary();
        assert_eq!(
            (
                result.offered,
                result.sent,
                result.admitted,
                result.rejected,
                result.unknown,
                result.client_dropped
            ),
            (4, 3, 1, 1, 1, 1)
        );
        assert_eq!(
            (
                result.delivered,
                result.unique_delivered,
                result.expected_fanout_deliveries,
                result.missing_admitted
            ),
            (4, 2, 2, 1)
        );
        assert_eq!(
            (result.duplicates, result.corrupt, result.unexpected),
            (1, 1, 1)
        );
        assert_eq!(
            (result.unknown_expected_fanout, result.unknown_undelivered),
            (2, 1)
        );
        assert_eq!(result.expired_or_intentionally_dropped, None);
        assert_eq!(result.ack_latency.count, 2);
        assert_eq!(result.e2e_latency.count, 2);
        assert!(!result.incomplete);
    }
    #[test]
    fn ordering_checks_inversions_not_q0_loss_gaps_and_warmup_is_separate() {
        let mut oracle = Oracle::new(1, 8, true).unwrap();
        oracle.observe(&offered(0, false)).unwrap();
        oracle.observe(&Event::Sent { id: 0, at_ns: 20 }).unwrap();
        oracle
            .observe(&Event::Ack {
                id: 0,
                at_ns: 40,
                outcome: Outcome::Admitted,
            })
            .unwrap();
        oracle.observe(&delivery(0, 0, 50)).unwrap();
        for id in [1, 4, 7] {
            send(&mut oracle, id, Outcome::Admitted);
        }
        for id in [1, 7, 4] {
            oracle.observe(&delivery(id, 0, 60)).unwrap();
        }
        let result = oracle.summary();
        assert_eq!(
            (
                result.offered,
                result.warmup_offered,
                result.warmup_delivered
            ),
            (3, 1, 1)
        );
        assert_eq!(result.ordering_violations, 1);
        assert_eq!(result.missing_admitted, 0);
    }
    #[test]
    fn incomplete_outcomes_and_invalid_events_are_explicit() {
        let mut oracle = Oracle::new(1, 1, false).unwrap();
        oracle.observe(&offered(1, true)).unwrap();
        assert!(oracle.summary().incomplete);
        assert_eq!(oracle.summary().incomplete_outcomes, 1);
        assert!(
            oracle
                .observe(&Event::Ack {
                    id: 1,
                    at_ns: 20,
                    outcome: Outcome::Admitted
                })
                .is_err()
        );
        assert!(oracle.observe(&offered(2, true)).is_err());
        assert!(Oracle::new(0, 1, false).is_err());
        assert!(Oracle::new(1000, 5001, false).is_err());
    }
    #[test]
    fn exact_histogram_percentiles_preserve_every_sample() {
        let mut distribution = LatencyDistribution::default();
        for v in 1..=1000 {
            distribution.record(v);
        }
        distribution.record(1000);
        let distribution = distribution.finalized();
        assert_eq!(distribution.count, 1001);
        assert_eq!(distribution.p50_ns, Some(501));
        assert_eq!(distribution.p95_ns, Some(951));
        assert_eq!(distribution.p99_ns, Some(991));
        assert_eq!(distribution.p999_ns, Some(1000));
        assert_eq!(distribution.buckets_ns.get(&1000), Some(&2));
        assert_eq!(LatencyDistribution::default().finalized().p99_ns, None);
    }
    #[test]
    fn bounded_jsonl_roundtrip_and_replay_matches_live_oracle() {
        let path = path();
        let log = EventLog::create(&path, 32).unwrap();
        let mut live = Oracle::new(1, 5, false).unwrap();
        let events = [
            offered(1, true),
            Event::Sent { id: 1, at_ns: 20 },
            Event::Ack {
                id: 1,
                at_ns: 40,
                outcome: Outcome::Admitted,
            },
            delivery(1, 0, 70),
        ];
        for event in &events {
            log.record(event).unwrap();
            live.observe(event).unwrap();
        }
        log.finish().unwrap();
        let mut replayed = Oracle::new(1, 5, false).unwrap();
        replay(&path, &mut replayed).unwrap();
        assert_eq!(live.summary(), replayed.summary());
        assert_eq!(std::fs::read_to_string(&path).unwrap().lines().count(), 4);
        assert!(EventLog::create(&path, 32).is_err());
        std::fs::remove_file(path).unwrap();
    }
    #[test]
    fn malformed_and_truncated_jsonl_never_look_like_a_complete_run() {
        for bytes in [
            b"not-json\n".to_vec(),
            serde_json::to_vec(&offered(1, true)).unwrap(),
            vec![b'x'; MAX_JSONL_LINE as usize + 1],
        ] {
            let path = path();
            std::fs::write(&path, bytes).unwrap();
            let mut oracle = Oracle::new(1, 5, false).unwrap();
            assert!(replay(&path, &mut oracle).is_err());
            std::fs::remove_file(path).unwrap();
        }
    }
    #[test]
    fn evidence_queue_saturation_is_reported_and_does_not_block_producers() {
        let (tx, _rx) = mpsc::sync_channel(1);
        let writer = EventWriter {
            tx,
            failure: Arc::new(Mutex::new(None)),
            closed: Arc::new(Mutex::new(false)),
        };
        writer.record(&offered(1, true)).unwrap();
        assert!(writer.record(&offered(2, true)).is_err());
        assert!(writer.record(&offered(3, true)).is_err());
        let path = path();
        assert!(EventLog::create(&path, 0).is_err());
        assert!(!path.exists());
    }
    #[test]
    fn cloned_producers_cannot_prevent_clean_log_finish() {
        let path = path();
        let log = EventLog::create(&path, 4).unwrap();
        let writer = log.writer();
        writer.record(&offered(1, true)).unwrap();
        log.finish().unwrap();
        assert!(writer.record(&offered(2, true)).is_err());
        assert_eq!(std::fs::read_to_string(&path).unwrap().lines().count(), 1);
        std::fs::remove_file(path).unwrap();
    }
}
