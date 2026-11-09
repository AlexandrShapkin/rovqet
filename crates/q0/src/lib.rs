//! Volatile Q0 dispatch: ADMITTED only promises entry into bounded work.
#![forbid(unsafe_code)]
use rovqet_core::{Priority, QoS};
use rovqet_protocol::{ErrorCode, Field, Frame, FrameType, Limits, encode, registry::field as f};
use rovqet_router::{OverflowPolicy, Router, RouterError, SubscriptionIndex, TopicConfig};
use rovqet_scheduler::{Budget, Reservation, Scheduler};
use rovqet_transport::{Outbound, PendingSend, SessionHandler};
use std::{
    collections::{HashMap, HashSet},
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
};
use tokio::{
    sync::Notify,
    time::{Duration, Instant},
};

#[derive(Default)]
pub struct Counters {
    pub offered: AtomicU64,
    pub admitted: AtomicU64,
    pub rejected: AtomicU64,
    pub enqueued: AtomicU64,
    pub dropped_overflow: AtomicU64,
    pub dropped_expired: AtomicU64,
    pub dropped_unsubscribed: AtomicU64,
}
struct Work {
    topic: String,
    bytes: Arc<[u8]>,
    deadline: Instant,
    _reservation: Reservation,
    reserved: Option<Vec<(u64, PendingSend)>>,
}
struct State {
    queue: Scheduler<Work>,
    sessions: HashMap<u128, Arc<Budget>>,
}
pub struct Broker {
    router: Router,
    subscriptions: SubscriptionIndex<Outbound>,
    state: Mutex<State>,
    global: Arc<Budget>,
    topics: HashMap<String, Arc<Budget>>,
    notify: Notify,
    pub counters: Counters,
    max_sessions: usize,
}
impl Broker {
    pub fn new(
        topics: Vec<TopicConfig>,
        max_sessions: usize,
        messages: usize,
        bytes: usize,
    ) -> Result<Arc<Self>, RouterError> {
        if max_sessions == 0
            || messages < 2
            || bytes == 0
            || topics.iter().any(|t| {
                !matches!(
                    t.overflow,
                    OverflowPolicy::DropNewest | OverflowPolicy::RejectPublish
                )
            })
        {
            return Err(RouterError::InvalidConfig);
        }
        let budgets = topics
            .iter()
            .map(|t| (t.name.clone(), Budget::new(messages, t.max_ingress_bytes)))
            .collect();
        let router = Router::new(topics, 16)?;
        Ok(Arc::new(Self {
            subscriptions: SubscriptionIndex::new(&router),
            router,
            state: Mutex::new(State {
                queue: Scheduler::new(messages, 1),
                sessions: HashMap::new(),
            }),
            global: Budget::new(messages, bytes),
            topics: budgets,
            notify: Notify::new(),
            counters: Counters::default(),
            max_sessions,
        }))
    }
    pub fn development() -> Arc<Self> {
        let topics = ["demo", "telemetry"]
            .into_iter()
            .enumerate()
            .map(|(i, name)| TopicConfig {
                tenant_id: 1,
                topic_id: i as u64 + 1,
                name: name.into(),
                partitions: 4,
                allowed_qos: vec![QoS::Q0],
                priority: Priority::Telemetry,
                max_ttl_ms: 30_000,
                max_subscriptions: 4096,
                overflow: OverflowPolicy::DropNewest,
                max_ingress_bytes: 8 * 1024 * 1024,
            })
            .collect();
        Self::new(topics, 4096, 1024, 16 * 1024 * 1024).unwrap()
    }
    pub async fn run(self: Arc<Self>, shutdown: impl std::future::Future<Output = ()>) {
        tokio::pin!(shutdown);
        loop {
            let work = self.state.lock().unwrap().queue.pop();
            if let Some(work) = work {
                if let Some(reserved) = work.reserved {
                    let active: HashSet<_> = self
                        .subscriptions
                        .matches(&work.topic)
                        .iter()
                        .map(|s| s.id)
                        .collect();
                    for (id, pending) in reserved {
                        if !active.contains(&id) {
                            self.counters
                                .dropped_unsubscribed
                                .fetch_add(1, Ordering::Relaxed);
                        } else if Instant::now() >= work.deadline {
                            self.counters
                                .dropped_expired
                                .fetch_add(1, Ordering::Relaxed);
                        } else {
                            pending.send();
                            self.counters.enqueued.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                } else {
                    let subscribers = self.subscriptions.matches(&work.topic);
                    for subscriber in subscribers.iter() {
                        if Instant::now() >= work.deadline {
                            self.counters
                                .dropped_expired
                                .fetch_add(1, Ordering::Relaxed);
                            continue;
                        }
                        if subscriber
                            .handle
                            .try_send_encoded(work.bytes.clone(), Some(work.deadline))
                            .is_ok()
                        {
                            self.counters.enqueued.fetch_add(1, Ordering::Relaxed);
                        } else {
                            self.counters
                                .dropped_overflow
                                .fetch_add(1, Ordering::Relaxed);
                        }
                    }
                }
                // Bound cooperative task occupancy under a sustained publication stream.
                tokio::task::yield_now().await;
            } else {
                tokio::select! { _ = &mut shutdown => break, _ = self.notify.notified() => {} }
            }
            // Always observe shutdown even under a continuously nonempty queue.
            tokio::select! { biased; _ = &mut shutdown => break, _ = std::future::ready(()) => {} }
        }
        // Dropping queued reservations releases all charges at shutdown.
        let mut state = self.state.lock().unwrap();
        while state.queue.pop().is_some() {}
    }
    fn publish(&self, session: u128, frame: Frame) -> Result<Frame, ErrorCode> {
        self.counters.offered.fetch_add(1, Ordering::Relaxed);
        let result = self.admit(session, frame);
        if result.is_err() {
            self.counters.rejected.fetch_add(1, Ordering::Relaxed);
        }
        result
    }
    fn admit(&self, session: u128, frame: Frame) -> Result<Frame, ErrorCode> {
        if frame.field(f::QOS) != Some(&[0]) {
            return Err(ErrorCode::UnsupportedGuarantee);
        }
        let name = std::str::from_utf8(frame.field(f::TOPIC_NAME).ok_or(ErrorCode::BadFrame)?)
            .map_err(|_| ErrorCode::BadFrame)?
            .to_owned();
        let ttl = u32::from_be_bytes(
            frame
                .field(f::TTL_MS)
                .ok_or(ErrorCode::BadFrame)?
                .try_into()
                .map_err(|_| ErrorCode::BadFrame)?,
        );
        if ttl == 0 {
            return Err(ErrorCode::Expired);
        }
        let topic = self
            .router
            .validate_publish(&name, QoS::Q0, ttl)
            .map_err(map_error)?;
        let key = frame
            .field(f::PARTITION_KEY)
            .ok_or(ErrorCode::BadFrame)?
            .to_vec();
        let partition = Router::partition(&topic, &key);
        let deadline = Instant::now() + Duration::from_millis(ttl as u64);
        let mut message = Frame::new(FrameType::Message, 0);
        message.fields = vec![
            Field::required(f::TOPIC_NAME, name.as_bytes()),
            Field::required(f::QOS, [0]),
            Field::required(
                f::MESSAGE_ID,
                frame.field(f::MESSAGE_ID).ok_or(ErrorCode::BadFrame)?,
            ),
            Field::optional(f::PARTITION_ID, partition.partition_id.to_be_bytes()),
            Field::optional(f::PRIORITY, [topic.priority as u8]),
        ];
        message.payload = frame.payload;
        let bytes: Arc<[u8]> = encode(&message, Limits::default())
            .map_err(|_| ErrorCode::BadFrame)?
            .into();
        let mut state = self.state.lock().unwrap();
        if !state.sessions.contains_key(&session) {
            if state.sessions.len() >= self.max_sessions {
                return Err(ErrorCode::ResourceExhausted);
            }
            state
                .sessions
                .insert(session, Budget::new(128, 1024 * 1024));
        }
        // Include estimated physical egress and metadata, even with shared bytes.
        let recipients = self.subscriptions.matches(&name).len().max(1);
        let cost = bytes
            .len()
            .checked_add(key.len())
            .and_then(|v| v.checked_add(256))
            .and_then(|v| v.checked_mul(recipients))
            .ok_or(ErrorCode::ResourceExhausted)?;
        let reservation = Budget::reserve(
            &[
                self.global.clone(),
                self.topics[&name].clone(),
                state.sessions[&session].clone(),
            ],
            cost,
        )
        .ok_or(ErrorCode::ResourceExhausted)?;
        let reserved = if topic.overflow == OverflowPolicy::RejectPublish {
            Some(
                self.subscriptions
                    .matches(&name)
                    .iter()
                    .map(|s| {
                        s.handle
                            .reserve_encoded(bytes.clone(), Some(deadline))
                            .map(|pending| (s.id, pending))
                    })
                    .collect::<Result<Vec<_>, _>>()?,
            )
        } else {
            None
        };
        let work = Work {
            topic: name.into(),
            bytes,
            deadline,
            _reservation: reservation,
            reserved,
        };
        state
            .queue
            .push(topic.priority, work)
            .map_err(|_| ErrorCode::ResourceExhausted)?;
        self.counters.admitted.fetch_add(1, Ordering::Relaxed);
        self.notify.notify_one();
        let mut ack = Frame::new(FrameType::PubAck, frame.request_id);
        ack.fields = vec![
            Field::required(f::QOS, [0]),
            Field::required(f::PUB_OUTCOME, [1]),
            Field::optional(f::PARTITION_ID, partition.partition_id.to_be_bytes()),
        ];
        Ok(ack)
    }
}
fn map_error(error: RouterError) -> ErrorCode {
    match error {
        RouterError::QosNotAllowed => ErrorCode::UnsupportedGuarantee,
        RouterError::ResourceExhausted => ErrorCode::SubscriberLimit,
        RouterError::UnknownTopic => ErrorCode::PermissionDenied,
        _ => ErrorCode::BadFrame,
    }
}
impl SessionHandler for Broker {
    fn handle(
        &self,
        session: u128,
        frame: Frame,
        outbound: Outbound,
    ) -> Result<Option<Frame>, ErrorCode> {
        match frame.frame_type {
            FrameType::Publish => self.publish(session, frame).map(Some),
            FrameType::Subscribe => {
                let name = std::str::from_utf8(
                    frame
                        .field(f::FILTER)
                        .or_else(|| frame.field(f::TOPIC_NAME))
                        .ok_or(ErrorCode::BadFrame)?,
                )
                .map_err(|_| ErrorCode::BadFrame)?;
                let id = self
                    .subscriptions
                    .subscribe(name, session, outbound)
                    .map_err(map_error)?;
                let mut ack = Frame::new(FrameType::SubAck, frame.request_id);
                ack.fields
                    .push(Field::required(f::SUBSCRIPTION_ID, id.to_be_bytes()));
                Ok(Some(ack))
            }
            FrameType::Unsubscribe => {
                let id = u64::from_be_bytes(
                    frame
                        .field(f::SUBSCRIPTION_ID)
                        .ok_or(ErrorCode::BadFrame)?
                        .try_into()
                        .map_err(|_| ErrorCode::BadFrame)?,
                );
                self.subscriptions
                    .unsubscribe(session, id)
                    .map_err(map_error)?;
                let mut ack = Frame::new(FrameType::SubAck, frame.request_id);
                ack.fields
                    .push(Field::required(f::SUBSCRIPTION_ID, id.to_be_bytes()));
                Ok(Some(ack))
            }
            _ => Err(ErrorCode::UnsupportedCapability),
        }
    }
    fn disconnected(&self, session: u128) {
        self.subscriptions.remove_session(session);
        self.state.lock().unwrap().sessions.remove(&session);
    }
}
