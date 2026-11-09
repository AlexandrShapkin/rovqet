//! Volatile Q0 dispatch. ADMITTED promises bounded work, never delivery/durability.
#![forbid(unsafe_code)]
use rovqet_core::{PartitionRef, Priority, QoS};
use rovqet_protocol::{ErrorCode, Field, Frame, FrameType, Limits, encode, registry::field as f};
use rovqet_router::{
    OverflowPolicy, Router, RouterError, Subscription, SubscriptionIndex, TopicConfig,
};
use rovqet_scheduler::{
    Budget, Overload, OverloadState, RateQuota, RateReservation, Reservation, Scheduler, Usage,
};
use rovqet_transport::{DeliveryGate, Outbound, PendingSend, SessionHandler};
use std::{
    collections::HashMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};
use tokio::{sync::Notify, time::Instant};

#[derive(Clone, Copy, Debug)]
pub struct RateLimit {
    pub bytes_per_sec: usize,
    pub deliveries_per_sec: usize,
}
impl RateLimit {
    fn quota(self) -> Arc<RateQuota> {
        RateQuota::new(self.bytes_per_sec, self.deliveries_per_sec)
    }
    fn valid(self) -> bool {
        self.bytes_per_sec > 0 && self.deliveries_per_sec > 0
    }
}
/// One node is the global domain in this isolated prototype. All scopes are hard bounds.
#[derive(Clone, Debug)]
pub struct ResourceConfig {
    pub global_egress: RateLimit,
    pub tenant_egress: RateLimit,
    pub topic_egress: RateLimit,
    pub topic_overrides: HashMap<String, RateLimit>,
    pub tenant_ingress_bytes: usize,
    pub partition_ingress_bytes: usize,
    pub session_ingress_bytes: usize,
    pub session_ingress_messages: usize,
    pub overload_dwell: Duration,
    pub max_partitions: usize,
    pub global_outbound_bytes: usize,
    pub tenant_outbound_bytes: usize,
    pub topic_outbound_bytes: usize,
    pub outbound_messages: usize,
}
impl Default for ResourceConfig {
    fn default() -> Self {
        let rate = RateLimit {
            bytes_per_sec: 256 * 1024 * 1024,
            deliveries_per_sec: 1_000_000,
        };
        Self {
            global_egress: rate,
            tenant_egress: rate,
            topic_egress: rate,
            topic_overrides: HashMap::new(),
            tenant_ingress_bytes: 16 * 1024 * 1024,
            partition_ingress_bytes: 8 * 1024 * 1024,
            session_ingress_bytes: 1024 * 1024,
            session_ingress_messages: 128,
            overload_dwell: Duration::from_millis(100),
            max_partitions: 4096,
            global_outbound_bytes: 64 * 1024 * 1024,
            tenant_outbound_bytes: 32 * 1024 * 1024,
            topic_outbound_bytes: 16 * 1024 * 1024,
            outbound_messages: 16384,
        }
    }
}
struct Rates {
    admission: Arc<RateQuota>,
    delivery: Arc<RateQuota>,
}
impl Rates {
    fn new(limit: RateLimit) -> Self {
        Self {
            admission: limit.quota(),
            delivery: limit.quota(),
        }
    }
}
#[derive(Default)]
pub struct Counters {
    pub offered: AtomicU64,
    pub admitted: AtomicU64,
    pub rejected: AtomicU64,
    pub enqueued: AtomicU64,
    pub dropped_overflow: AtomicU64,
    pub dropped_expired: AtomicU64,
    pub dropped_unsubscribed: AtomicU64,
    pub dropped_shutdown: AtomicU64,
    pub egress_rejected: AtomicU64,
    pub overload_transitions: AtomicU64,
    pub flow_rejected: AtomicU64,
}
#[derive(Clone)]
struct Destination {
    outbound: Outbound,
    gate: Arc<DeliveryGate>,
}
struct Work {
    bytes: Arc<[u8]>,
    key: Vec<u8>,
    deadline: Instant,
    policy: OverflowPolicy,
    recipients: Arc<Vec<Subscription<Destination>>>,
    reserved: Option<Vec<PendingSend>>,
    _reservation: Reservation,
    _rate: RateReservation,
}
struct State {
    queue: Scheduler<Work>,
    sessions: HashMap<u128, Arc<Budget>>,
    gates: HashMap<u64, (u128, Arc<DeliveryGate>)>,
    overload: Overload,
}
pub struct Broker {
    router: Router,
    subscriptions: SubscriptionIndex<Destination>,
    state: Mutex<State>,
    global: Arc<Budget>,
    topics: HashMap<String, Arc<Budget>>,
    tenants: HashMap<u64, Arc<Budget>>,
    partitions: HashMap<PartitionRef, Arc<Budget>>,
    egress_global: Arc<Budget>,
    egress_tenants: HashMap<u64, Arc<Budget>>,
    egress_topics: HashMap<String, Arc<Budget>>,
    global_rate: Rates,
    tenant_rates: HashMap<u64, Rates>,
    topic_rates: HashMap<String, Rates>,
    resources: ResourceConfig,
    notify: Notify,
    pub counters: Counters,
    max_sessions: usize,
    messages: usize,
    bytes: usize,
}
impl Broker {
    pub fn new(
        topics: Vec<TopicConfig>,
        max_sessions: usize,
        messages: usize,
        bytes: usize,
    ) -> Result<Arc<Self>, RouterError> {
        Self::with_resources(
            topics,
            max_sessions,
            messages,
            bytes,
            ResourceConfig::default(),
        )
    }
    pub fn with_resources(
        topics: Vec<TopicConfig>,
        max_sessions: usize,
        messages: usize,
        bytes: usize,
        resources: ResourceConfig,
    ) -> Result<Arc<Self>, RouterError> {
        if max_sessions == 0
            || messages < 2
            || bytes == 0
            || !resources.global_egress.valid()
            || !resources.tenant_egress.valid()
            || !resources.topic_egress.valid()
            || resources.topic_overrides.values().any(|r| !r.valid())
            || resources.tenant_ingress_bytes == 0
            || resources.partition_ingress_bytes == 0
            || resources.session_ingress_bytes == 0
            || resources.session_ingress_messages == 0
            || resources.global_outbound_bytes == 0
            || resources.tenant_outbound_bytes == 0
            || resources.topic_outbound_bytes == 0
            || resources.outbound_messages == 0
            || topics.iter().map(|t| t.partitions as u64).sum::<u64>()
                > resources.max_partitions as u64
        {
            return Err(RouterError::InvalidConfig);
        }
        let router = Router::new(topics.clone(), 16)?;
        if resources
            .topic_overrides
            .keys()
            .any(|name| router.topic(name).is_none())
        {
            return Err(RouterError::InvalidConfig);
        }
        let mut tenants = HashMap::new();
        let mut partitions = HashMap::new();
        let mut tenant_rates = HashMap::new();
        let mut egress_tenants = HashMap::new();
        for t in &topics {
            tenants
                .entry(t.tenant_id)
                .or_insert_with(|| Budget::new(messages, resources.tenant_ingress_bytes));
            egress_tenants.entry(t.tenant_id).or_insert_with(|| {
                Budget::new(resources.outbound_messages, resources.tenant_outbound_bytes)
            });
            tenant_rates
                .entry(t.tenant_id)
                .or_insert_with(|| Rates::new(resources.tenant_egress));
            for id in 0..t.partitions {
                partitions.insert(
                    PartitionRef {
                        tenant_id: t.tenant_id,
                        topic_id: t.topic_id,
                        partition_id: id,
                    },
                    Budget::new(messages, resources.partition_ingress_bytes),
                );
            }
        }
        let egress_topics = topics
            .iter()
            .map(|t| {
                (
                    t.name.clone(),
                    Budget::new(resources.outbound_messages, resources.topic_outbound_bytes),
                )
            })
            .collect();
        let topic_budgets = topics
            .iter()
            .map(|t| (t.name.clone(), Budget::new(messages, t.max_ingress_bytes)))
            .collect();
        let topic_rates = topics
            .iter()
            .map(|t| {
                (
                    t.name.clone(),
                    Rates::new(
                        *resources
                            .topic_overrides
                            .get(&t.name)
                            .unwrap_or(&resources.topic_egress),
                    ),
                )
            })
            .collect();
        Ok(Arc::new(Self {
            subscriptions: SubscriptionIndex::new(&router),
            router,
            state: Mutex::new(State {
                queue: Scheduler::new(messages, 1),
                sessions: HashMap::new(),
                gates: HashMap::new(),
                overload: Overload::new(resources.overload_dwell),
            }),
            global: Budget::new(messages, bytes),
            topics: topic_budgets,
            tenants,
            partitions,
            egress_global: Budget::new(
                resources.outbound_messages,
                resources.global_outbound_bytes,
            ),
            egress_tenants,
            egress_topics,
            global_rate: Rates::new(resources.global_egress),
            tenant_rates,
            topic_rates,
            resources,
            notify: Notify::new(),
            counters: Counters::default(),
            max_sessions,
            messages,
            bytes,
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
    pub fn ingress_usage(&self) -> Usage {
        self.global.usage()
    }
    pub fn egress_usage(&self) -> Usage {
        self.egress_global.usage()
    }
    pub fn active_subscriptions(&self) -> usize {
        self.state.lock().unwrap().gates.len()
    }
    pub fn active_sessions(&self) -> usize {
        self.state.lock().unwrap().sessions.len()
    }
    fn observe(&self, state: &mut State) -> OverloadState {
        let used = self.global.usage();
        let percent = (used.messages.saturating_mul(100) / self.messages)
            .max(used.bytes.saturating_mul(100) / self.bytes);
        let before = state.overload.state();
        let after = state.overload.observe(percent, std::time::Instant::now());
        if before != after {
            self.counters
                .overload_transitions
                .fetch_add(1, Ordering::Relaxed);
        }
        after
    }
    pub fn overload_state(&self) -> OverloadState {
        self.observe(&mut self.state.lock().unwrap())
    }
    pub fn drain(&self) {
        self.state
            .lock()
            .unwrap()
            .overload
            .set(OverloadState::Drain);
        self.notify.notify_one();
    }
    pub fn unavailable(&self) {
        self.state
            .lock()
            .unwrap()
            .overload
            .set(OverloadState::Unavailable);
        self.notify.notify_one();
    }
    pub fn recover(&self) {
        self.state.lock().unwrap().overload.set(OverloadState::Shed);
        self.notify.notify_one();
    }
    pub async fn run(self: Arc<Self>, shutdown: impl std::future::Future<Output = ()>) {
        tokio::pin!(shutdown);
        loop {
            tokio::select! {biased; _=&mut shutdown=>break, _=std::future::ready(())=>{}}
            let work = self.state.lock().unwrap().queue.pop();
            if let Some(work) = work {
                if let Some(reserved) = work.reserved {
                    for (dest, pending) in work.recipients.iter().zip(reserved) {
                        if dest.handle.gate.is_closed() {
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
                    for dest in work.recipients.iter() {
                        if dest.handle.gate.is_closed() {
                            self.counters
                                .dropped_unsubscribed
                                .fetch_add(1, Ordering::Relaxed);
                            continue;
                        }
                        if Instant::now() >= work.deadline {
                            self.counters
                                .dropped_expired
                                .fetch_add(1, Ordering::Relaxed);
                            continue;
                        }
                        let policy = match work.policy {
                            OverflowPolicy::DropNewest => {
                                rovqet_transport::OverflowPolicy::DropNewest
                            }
                            OverflowPolicy::DropOldest => {
                                rovqet_transport::OverflowPolicy::DropOldest
                            }
                            OverflowPolicy::CoalesceLatest => {
                                rovqet_transport::OverflowPolicy::CoalesceLatest
                            }
                            OverflowPolicy::RejectPublish => unreachable!(),
                        };
                        // Scope coalescing to topic plus partition key (both length-delimited).
                        if dest
                            .handle
                            .outbound
                            .enqueue(
                                work.bytes.clone(),
                                Some(work.deadline),
                                policy,
                                work.key.clone(),
                                Some(dest.handle.gate.clone()),
                            )
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
                // Admission-rate reservation is consumed even for an admitted Q0 drop.
                work._rate.commit();
                tokio::task::yield_now().await;
            } else {
                tokio::select! {_=&mut shutdown=>break,_=self.notify.notified()=>{}}
            }
        }
        let mut state = self.state.lock().unwrap();
        state.overload.set(OverloadState::Drain);
        while let Some(work) = state.queue.pop() {
            self.counters
                .dropped_shutdown
                .fetch_add(work.recipients.len() as u64, Ordering::Relaxed);
        }
    }
    fn ensure_session(&self, state: &mut State, session: u128) -> Result<(), ErrorCode> {
        if !state.sessions.contains_key(&session) {
            if state.sessions.len() >= self.max_sessions {
                return Err(ErrorCode::ResourceExhausted);
            }
            state.sessions.insert(
                session,
                Budget::new(
                    self.resources.session_ingress_messages,
                    self.resources.session_ingress_bytes,
                ),
            );
        }
        Ok(())
    }
    fn admit(&self, session: u128, frame: Frame) -> Result<Frame, ErrorCode> {
        if frame.field(f::QOS) != Some(&[0]) {
            return Err(ErrorCode::UnsupportedGuarantee);
        }
        let name = std::str::from_utf8(frame.field(f::TOPIC_NAME).ok_or(ErrorCode::BadFrame)?)
            .map_err(|_| ErrorCode::BadFrame)?
            .to_owned();
        let ttl = u32_field(&frame, f::TTL_MS)?;
        if ttl == 0 {
            return Err(ErrorCode::Expired);
        }
        let topic = self
            .router
            .validate_publish(&name, QoS::Q0, ttl)
            .map_err(map_error)?;
        let raw_key = frame.field(f::PARTITION_KEY).ok_or(ErrorCode::BadFrame)?;
        let partition = Router::partition(&topic, raw_key);
        let mut key = Vec::with_capacity(name.len() + raw_key.len() + 4);
        key.extend_from_slice(&(name.len() as u32).to_be_bytes());
        key.extend_from_slice(name.as_bytes());
        key.extend_from_slice(raw_key);
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
            Field::optional(f::SUBSCRIPTION_ID, 0u64.to_be_bytes()),
        ];
        message.payload = frame.payload;
        let bytes: Arc<[u8]> = encode(&message, Limits::default())
            .map_err(|_| ErrorCode::BadFrame)?
            .into();
        let mut state = self.state.lock().unwrap();
        match self.observe(&mut state) {
            OverloadState::Shed => return Err(ErrorCode::ResourceExhausted),
            OverloadState::Drain => return Err(ErrorCode::ServerDraining),
            OverloadState::Unavailable => return Err(ErrorCode::ResourceExhausted),
            _ => {}
        }
        self.ensure_session(&mut state, session)?;
        let recipients = self.subscriptions.matches(&name);
        let cost = (bytes.len() + key.len() + 256)
            .checked_mul(recipients.len().max(1))
            .ok_or(ErrorCode::ResourceExhausted)?;
        let reservation = Budget::reserve(
            &[
                self.global.clone(),
                self.tenants[&topic.tenant_id].clone(),
                self.topics[&name].clone(),
                self.partitions[&partition].clone(),
                state.sessions[&session].clone(),
            ],
            cost,
        )
        .ok_or(ErrorCode::ResourceExhausted)?;
        let physical_bytes = bytes
            .len()
            .checked_mul(recipients.len())
            .ok_or(ErrorCode::EgressQuota)?;
        let rate = RateQuota::reserve(
            &[
                self.global_rate.admission.clone(),
                self.tenant_rates[&topic.tenant_id].admission.clone(),
                self.topic_rates[&name].admission.clone(),
            ],
            physical_bytes,
            recipients.len(),
        )
        .ok_or(ErrorCode::EgressQuota)?;
        let deadline = Instant::now() + Duration::from_millis(ttl as u64);
        let reserved = if topic.overflow == OverflowPolicy::RejectPublish {
            Some(
                recipients
                    .iter()
                    .map(|s| {
                        s.handle.outbound.reserve_delivery(
                            bytes.clone(),
                            Some(deadline),
                            key.clone(),
                            Some(s.handle.gate.clone()),
                        )
                    })
                    .collect::<Result<Vec<_>, _>>()?,
            )
        } else {
            None
        };
        state
            .queue
            .push(
                topic.priority,
                Work {
                    bytes,
                    key,
                    deadline,
                    policy: topic.overflow,
                    recipients,
                    reserved,
                    _reservation: reservation,
                    _rate: rate,
                },
            )
            .map_err(|_| ErrorCode::ResourceExhausted)?;
        self.observe(&mut state);
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
fn u32_field(frame: &Frame, id: u64) -> Result<u32, ErrorCode> {
    Ok(u32::from_be_bytes(
        frame
            .field(id)
            .ok_or(ErrorCode::BadFrame)?
            .try_into()
            .map_err(|_| ErrorCode::BadFrame)?,
    ))
}
fn sub_id(frame: &Frame) -> Result<u64, ErrorCode> {
    Ok(u64::from_be_bytes(
        frame
            .field(f::SUBSCRIPTION_ID)
            .ok_or(ErrorCode::BadFrame)?
            .try_into()
            .map_err(|_| ErrorCode::BadFrame)?,
    ))
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
            FrameType::Publish => {
                self.counters.offered.fetch_add(1, Ordering::Relaxed);
                let result = self.admit(session, frame);
                if let Err(code) = result.as_ref() {
                    self.counters.rejected.fetch_add(1, Ordering::Relaxed);
                    if *code == ErrorCode::EgressQuota {
                        self.counters
                            .egress_rejected
                            .fetch_add(1, Ordering::Relaxed);
                    }
                }
                result.map(Some)
            }
            FrameType::Subscribe => {
                let name = std::str::from_utf8(
                    frame
                        .field(f::FILTER)
                        .or_else(|| frame.field(f::TOPIC_NAME))
                        .ok_or(ErrorCode::BadFrame)?,
                )
                .map_err(|_| ErrorCode::BadFrame)?;
                let topic = self.router.topic(name).ok_or(ErrorCode::PermissionDenied)?;
                let messages = if frame.field(f::FLOW_CREDIT_MESSAGES).is_some() {
                    u32_field(&frame, f::FLOW_CREDIT_MESSAGES)?
                } else {
                    64
                };
                let bytes = if frame.field(f::FLOW_CREDIT_BYTES).is_some() {
                    u32_field(&frame, f::FLOW_CREDIT_BYTES)?
                } else {
                    256 * 1024
                };
                let gate = DeliveryGate::new(128, 1024 * 1024);
                if messages != 0 || bytes != 0 {
                    gate.add(messages, bytes)?;
                }
                gate.set_budgets(vec![
                    self.egress_global.clone(),
                    self.egress_tenants[&topic.tenant_id].clone(),
                    self.egress_topics[name].clone(),
                ]);
                gate.set_quotas(vec![
                    self.global_rate.delivery.clone(),
                    self.tenant_rates[&topic.tenant_id].delivery.clone(),
                    self.topic_rates[name].delivery.clone(),
                ]);
                let mut state = self.state.lock().unwrap();
                self.ensure_session(&mut state, session)?;
                let id = self
                    .subscriptions
                    .subscribe(
                        name,
                        session,
                        Destination {
                            outbound,
                            gate: gate.clone(),
                        },
                    )
                    .map_err(map_error)?;
                gate.set_subscription_id(id);
                state.gates.insert(id, (session, gate));
                let mut ack = Frame::new(FrameType::SubAck, frame.request_id);
                ack.fields = vec![
                    Field::required(f::SUBSCRIPTION_ID, id.to_be_bytes()),
                    Field::optional(f::FLOW_CREDIT_MESSAGES, messages.to_be_bytes()),
                    Field::optional(f::FLOW_CREDIT_BYTES, bytes.to_be_bytes()),
                ];
                Ok(Some(ack))
            }
            FrameType::FlowCredit => {
                let result = (|| {
                    let id = sub_id(&frame)?;
                    let messages = u32_field(&frame, f::FLOW_CREDIT_MESSAGES)?;
                    let bytes = u32_field(&frame, f::FLOW_CREDIT_BYTES)?;
                    if messages == 0 && bytes == 0 {
                        return Err(ErrorCode::BadFrame);
                    }
                    let state = self.state.lock().unwrap();
                    let (owner, gate) = state.gates.get(&id).ok_or(ErrorCode::PermissionDenied)?;
                    if *owner != session {
                        return Err(ErrorCode::PermissionDenied);
                    }
                    gate.add(messages, bytes)?;
                    let mut ack = Frame::new(FrameType::FlowCredit, frame.request_id);
                    ack.fields = vec![
                        Field::required(f::SUBSCRIPTION_ID, id.to_be_bytes()),
                        Field::required(f::FLOW_CREDIT_MESSAGES, messages.to_be_bytes()),
                        Field::required(f::FLOW_CREDIT_BYTES, bytes.to_be_bytes()),
                    ];
                    Ok(Some(ack))
                })();
                if result.is_err() {
                    self.counters.flow_rejected.fetch_add(1, Ordering::Relaxed);
                }
                result
            }
            FrameType::Unsubscribe => {
                let id = sub_id(&frame)?;
                let mut state = self.state.lock().unwrap();
                let (owner, gate) = state.gates.get(&id).ok_or(ErrorCode::PermissionDenied)?;
                if *owner != session {
                    return Err(ErrorCode::PermissionDenied);
                }
                gate.close();
                self.subscriptions
                    .unsubscribe(session, id)
                    .map_err(map_error)?;
                state.gates.remove(&id);
                let mut ack = Frame::new(FrameType::SubAck, frame.request_id);
                ack.fields
                    .push(Field::required(f::SUBSCRIPTION_ID, id.to_be_bytes()));
                Ok(Some(ack))
            }
            _ => Err(ErrorCode::UnsupportedCapability),
        }
    }
    fn disconnected(&self, session: u128) {
        let mut state = self.state.lock().unwrap();
        state.gates.retain(|_, (owner, gate)| {
            if *owner == session {
                gate.close();
                false
            } else {
                true
            }
        });
        self.subscriptions.remove_session(session);
        state.sessions.remove(&session);
    }
}
