//! Independent Q0 load/oracle process. Offers are scheduled independently of ACKs.
use futures_util::StreamExt;
use rovqet_sdk::{Client, ClientConfig, ClientError, PublishOptions};
use rovqet_testkit::{Event, EventLog, Oracle, Outcome, digest, payload, payload_identity};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};
use tokio::{
    sync::{mpsc, watch},
    task::JoinSet,
};

fn default_backend() -> String {
    "rovqet".into()
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Config {
    endpoint: std::net::SocketAddr,
    #[serde(default = "default_backend")]
    backend: String,
    publishers: usize,
    subscribers: usize,
    messages: usize,
    rate: u64,
    payload_bytes: usize,
    warmup_messages: usize,
    seed: u64,
    output: PathBuf,
    slow_subscribers: usize,
    slow_ms: u64,
    credit_starved: usize,
    drain_ms: u64,
    check_order: bool,
    queue_capacity: usize,
    idle_connections: usize,
    duration_secs: f64,
    request_timeout_ms: u64,
}
impl Default for Config {
    fn default() -> Self {
        Self {
            endpoint: ([127, 0, 0, 1], 7443).into(),
            backend: default_backend(),
            publishers: 1,
            subscribers: 1,
            messages: 1000,
            rate: 1000,
            payload_bytes: 256,
            warmup_messages: 0,
            seed: 42,
            output: PathBuf::from("/tmp/rovqet-load"),
            slow_subscribers: 0,
            slow_ms: 100,
            credit_starved: 0,
            drain_ms: 2000,
            check_order: false,
            queue_capacity: 128,
            idle_connections: 0,
            duration_secs: 1.0,
            request_timeout_ms: 3000,
        }
    }
}
impl Config {
    fn parse(args: impl IntoIterator<Item = String>) -> Result<Self, String> {
        let mut args = args.into_iter().peekable();
        if args
            .peek()
            .is_some_and(|s| s == "q0-load" || s == "q0-smoke")
        {
            args.next();
        }
        let mut values = HashMap::new();
        while let Some(key) = args.next() {
            if !key.starts_with("--") {
                return Err(format!("expected flag, got {key}"));
            }
            let value = args
                .next()
                .ok_or_else(|| format!("missing value for {key}"))?;
            if values.insert(key.clone(), value).is_some() {
                return Err(format!("duplicate flag {key}"));
            }
        }
        let mut c = Self::default();
        macro_rules! field {
            ($flag:literal,$name:ident) => {
                if let Some(v) = values.remove($flag) {
                    c.$name = v.parse().map_err(|_| format!("invalid {}", $flag))?;
                }
            };
        }
        let explicit_messages = values.contains_key("--messages");
        let explicit_duration = values.contains_key("--duration-secs");
        field!("--endpoint", endpoint);
        field!("--backend", backend);
        field!("--publishers", publishers);
        field!("--subscribers", subscribers);
        field!("--messages", messages);
        field!("--rate", rate);
        field!("--payload-bytes", payload_bytes);
        field!("--warmup-messages", warmup_messages);
        field!("--seed", seed);
        field!("--slow-subscribers", slow_subscribers);
        field!("--slow-ms", slow_ms);
        field!("--credit-starved", credit_starved);
        field!("--drain-ms", drain_ms);
        field!("--check-order", check_order);
        field!("--queue-capacity", queue_capacity);
        field!("--request-timeout-ms", request_timeout_ms);
        field!("--idle-connections", idle_connections);
        field!("--duration-secs", duration_secs);
        if let Some(v) = values.remove("--output") {
            c.output = PathBuf::from(v);
        }
        if let Some(v) = values.remove("--warmup-secs") {
            let secs: f64 = v.parse().map_err(|_| "invalid warmup seconds")?;
            if !secs.is_finite() || secs < 0.0 || secs > 60.0 || c.rate == 0 {
                return Err("warmup seconds requires finite0..60 and positive rate".into());
            }
            c.warmup_messages = (secs * c.rate as f64).ceil() as usize;
        }
        if !values.is_empty() {
            return Err(format!("unknown flags: {:?}", values.keys()));
        }
        if !c.duration_secs.is_finite() || c.duration_secs <= 0.0 || c.duration_secs > 60.0 {
            return Err("duration must be finite, positive and <=60s".into());
        }
        if explicit_duration && !explicit_messages && c.idle_connections == 0 {
            if c.rate == 0 {
                return Err("burst requires explicit messages".into());
            }
            c.messages = (c.duration_secs * c.rate as f64).ceil() as usize;
        }
        c.validate()?;
        Ok(c)
    }
    fn validate(&self) -> Result<(), String> {
        if !matches!(self.backend.as_str(), "rovqet" | "nats")
            || (self.backend == "nats" && self.credit_starved > 0)
            || !self.endpoint.ip().is_loopback()
            || self.publishers == 0
            || self.publishers > 1000
            || self.subscribers == 0
            || self.subscribers > 4096
            || !matches!(self.payload_bytes, 256 | 1024)
            || self.messages == 0
            || self.messages > 1_000_000
            || self.warmup_messages > 1_000_000
            || self
                .messages
                .checked_add(self.warmup_messages)
                .is_none_or(|n| n > 1_000_000 || n.saturating_mul(self.subscribers) > 5_000_000)
            || self.request_timeout_ms < 10
            || self.request_timeout_ms > 30_000
            || self.rate > 10_000_000
            || self.queue_capacity == 0
            || self.queue_capacity > 4096
            || self
                .publishers
                .saturating_mul(self.queue_capacity)
                .saturating_mul(self.payload_bytes)
                > 256 * 1024 * 1024
            || self.slow_subscribers + self.credit_starved > self.subscribers
            || self.drain_ms > 30_000
            || self.slow_ms > 10_000
            || self.idle_connections > 20_000
        {
            return Err("invalid or unsafe bounded benchmark configuration".into());
        }
        Ok(())
    }
}
#[derive(Clone)]
struct Events {
    tx: mpsc::Sender<Event>,
    failed: Arc<AtomicBool>,
    origin: Instant,
}
impl Events {
    fn now(&self) -> u64 {
        self.origin.elapsed().as_nanos().min(u64::MAX as u128) as u64
    }
    fn emit(&self, event: Event) {
        if self.tx.try_send(event).is_err() {
            self.failed.store(true, Ordering::Relaxed);
        }
    }
    fn error(&self, message: impl Into<String>) {
        self.emit(Event::Error {
            message: message.into(),
            at_ns: self.now(),
        });
    }
}
fn client_config(c: &Config) -> ClientConfig {
    ClientConfig {
        endpoint: c.endpoint,
        request_timeout: Duration::from_millis(c.request_timeout_ms),
        ..ClientConfig::default()
    }
}
// async-nats 0.39 flush only flushes the local socket buffer. A separate
// self-echo round trip verifies that preceding commands reached this server.
// It is setup/settlement control traffic, never a publication admission ACK.
async fn nats_barrier(client: &async_nats::Client, deadline: Duration) -> Result<(), String> {
    tokio::time::timeout(deadline, async {
        let subject = client.new_inbox();
        let mut replies = client
            .subscribe(subject.clone())
            .await
            .map_err(|e| e.to_string())?;
        client
            .publish(subject, Vec::from(b"ready").into())
            .await
            .map_err(|e| e.to_string())?;
        let reply = replies.next().await.ok_or("NATS readiness echo ended")?;
        if reply.payload.as_ref() != b"ready" {
            return Err("NATS readiness echo mismatch".into());
        }
        replies.unsubscribe().await.map_err(|e| e.to_string())?;
        Ok(())
    })
    .await
    .map_err(|_| "NATS server round-trip barrier timeout".to_string())?
}
// One enum keeps scheduling, identity generation, event logging and oracle common.
// NATS has a bounded operation channel and bounded per-subscriber client queue;
// unlike Rovqet it continuously reads TCP independently of application receive.
enum BenchmarkClient {
    Rovqet(Client),
    Nats {
        client: Option<async_nats::Client>,
        subscriber: Option<async_nats::Subscriber>,
        active: Arc<AtomicBool>,
        closed: Arc<AtomicBool>,
        timeout: Duration,
    },
}
impl BenchmarkClient {
    async fn subscribe(&mut self, starved: bool) -> Result<(), String> {
        match self {
            Self::Rovqet(client) => if starved {
                client.subscribe_with_credit("demo", 0, 0).await
            } else {
                client.subscribe("demo").await
            }
            .map(|_| ())
            .map_err(|e| e.to_string()),
            Self::Nats {
                client,
                subscriber,
                timeout,
                ..
            } => {
                if starved {
                    return Err("NATS Core has no Rovqet delivery-credit equivalent".into());
                }
                let client = client.as_ref().ok_or("NATS client closed")?;
                *subscriber = Some(client.subscribe("demo").await.map_err(|e| e.to_string())?);
                // Echo round trip proves SUB processed before publication begins.
                nats_barrier(client, *timeout).await
            }
        }
    }
    async fn receive(&mut self) -> Result<Vec<u8>, String> {
        match self {
            Self::Rovqet(client) => client
                .receive()
                .await
                .map(|m| m.payload)
                .map_err(|e| e.to_string()),
            Self::Nats { subscriber, .. } => subscriber
                .as_mut()
                .ok_or("not subscribed")?
                .next()
                .await
                .map(|m| m.payload.to_vec())
                .ok_or("NATS subscription ended".into()),
        }
    }
    async fn publish(&mut self, id: usize, body: Vec<u8>) -> Outcome {
        match self {
            Self::Rovqet(client) => match client
                .publish(
                    "demo",
                    &body,
                    PublishOptions {
                        key: id.to_be_bytes().to_vec(),
                        ttl: Duration::from_secs(2),
                        ..PublishOptions::default()
                    },
                )
                .await
            {
                Ok(_) => Outcome::Admitted,
                Err(ClientError::Remote { .. }) => Outcome::Rejected,
                Err(_) => Outcome::Unknown,
            },
            Self::Nats {
                client, timeout, ..
            } => match tokio::time::timeout(
                *timeout,
                client
                    .as_ref()
                    .expect("active NATS client")
                    .publish("demo", body.into()),
            )
            .await
            {
                Ok(Ok(())) => Outcome::PublishedUnconfirmed,
                _ => Outcome::Unknown,
            },
        }
    }
    async fn close(&mut self) -> Result<(), String> {
        match self {
            Self::Rovqet(client) => client.close().await.map_err(|e| e.to_string()),
            Self::Nats {
                client,
                subscriber,
                active,
                closed,
                timeout,
            } => {
                let client = client.take().ok_or("NATS client already closed")?;
                // Flush only at settlement, never mislabel PONG as per-message admission.
                let result = nats_barrier(&client, *timeout).await;
                active.store(false, Ordering::Relaxed);
                drop(subscriber.take());
                // Dropping the final client sender closes the official client
                // task after the flush; await its Closed callback and socket EOF.
                drop(client);
                tokio::time::timeout(*timeout, async {
                    while !closed.load(Ordering::Relaxed) {
                        tokio::time::sleep(Duration::from_millis(1)).await;
                    }
                })
                .await
                .map_err(|_| "NATS close completion timeout".to_string())?;
                result
            }
        }
    }
}
async fn connect(c: &Config, events: &Events, name: &str) -> Option<BenchmarkClient> {
    let begin = events.now();
    let result: Result<BenchmarkClient, String> = if c.backend == "nats" {
        let timeout = Duration::from_millis(c.request_timeout_ms);
        let active = Arc::new(AtomicBool::new(true));
        let callback_active = active.clone();
        let closed = Arc::new(AtomicBool::new(false));
        let callback_closed = closed.clone();
        let callback_events = events.clone();
        let callback_name = name.to_owned();
        let options = async_nats::ConnectOptions::new()
            .user_and_password("rovqet".into(), "development".into())
            .connection_timeout(timeout)
            .client_capacity(128)
            .subscription_capacity(64)
            // Library 0.39 maps zero to unlimited. Permit one attempt, but ANY
            // disconnect during measurement invalidates the run via callback.
            .max_reconnects(1)
            .event_callback(move |event| {
                let active = callback_active.clone();
                let events = callback_events.clone();
                let name = callback_name.clone();
                let closed = callback_closed.clone();
                async move {
                    if matches!(event, async_nats::Event::Closed) {
                        closed.store(true, Ordering::Relaxed);
                    }
                    if active.load(Ordering::Relaxed) {
                        match event {
                            async_nats::Event::Disconnected
                            | async_nats::Event::ServerError(_)
                            | async_nats::Event::ClientError(_) => {
                                events.error(format!("NATS {name}: {event}"))
                            }
                            async_nats::Event::SlowConsumer(_) => {
                                events.emit(Event::NatsSlowConsumer {
                                    client: name,
                                    at_ns: events.now(),
                                })
                            }
                            // Subscriber misses remain independently visible in oracle.
                            // Library callback channel is lossy: do not claim exact drops here.
                            _ => {}
                        }
                    }
                }
            });
        match tokio::time::timeout(timeout, options.connect(c.endpoint.to_string())).await {
            Ok(Ok(client)) => match nats_barrier(&client, timeout).await {
                Ok(()) => Ok(BenchmarkClient::Nats {
                    client: Some(client),
                    subscriber: None,
                    active,
                    closed,
                    timeout,
                }),
                _ => Err("NATS authentication/server echo barrier failed".into()),
            },
            Ok(Err(e)) => Err(e.to_string()),
            Err(_) => Err("NATS connect deadline exceeded".into()),
        }
    } else {
        Client::connect(client_config(c))
            .await
            .map(BenchmarkClient::Rovqet)
            .map_err(|e| e.to_string())
    };
    match result {
        Ok(client) => {
            events.emit(Event::Connection {
                client: name.into(),
                connected: true,
                authenticated: true,
                at_ns: events.now(),
                elapsed_ns: events.now() - begin,
            });
            Some(client)
        }
        Err(error) => {
            events.emit(Event::Connection {
                client: name.into(),
                connected: false,
                authenticated: false,
                at_ns: events.now(),
                elapsed_ns: events.now() - begin,
            });
            events.error(format!("connect {name}: {error}"));
            None
        }
    }
}
async fn close(mut client: BenchmarkClient, events: &Events, name: String) {
    let begin = events.now();
    let is_nats = matches!(client, BenchmarkClient::Nats { .. });
    let result = client.close().await;
    if is_nats {
        if let Err(e) = result {
            events.error(format!("close {name}: {e}"));
        }
    }
    drop(client);
    events.emit(Event::Connection {
        client: name,
        connected: false,
        authenticated: true,
        at_ns: events.now(),
        elapsed_ns: events.now() - begin,
    });
}
struct Job {
    id: u64,
    body: Vec<u8>,
}
async fn execute(c: &Config, events: Events) -> serde_json::Value {
    let setup = Instant::now();
    if c.idle_connections > 0 {
        let mut clients = Vec::with_capacity(c.idle_connections);
        let mut attempts = 0;
        let mut consecutive_failures = 0;
        for id in 0..c.idle_connections {
            if setup.elapsed() > Duration::from_secs(30) || consecutive_failures >= 16 {
                events.error("connection setup deadline/failure limit; remaining attempts NOT_RUN");
                break;
            }
            attempts += 1;
            if let Some(client) = connect(c, &events, &format!("idle-{id}")).await {
                clients.push((id, client));
                consecutive_failures = 0;
            } else {
                consecutive_failures += 1;
            }
        }
        let established = clients.len();
        let setup_secs = setup.elapsed().as_secs_f64();
        tokio::time::sleep(Duration::from_secs_f64(c.duration_secs)).await;
        let disconnect = Instant::now();
        for (id, client) in clients {
            close(client, &events, format!("idle-{id}")).await;
        }
        return json!({"mode":"idle","requested_connections":c.idle_connections,"established_connections":established,"connection_attempts":attempts,"connection_failures":attempts-established,"not_attempted":c.idle_connections-attempts,
            "connection_establishment_secs":setup_secs,"connections_per_sec":established as f64/setup_secs,"disconnect_secs":disconnect.elapsed().as_secs_f64(),"hold_secs":c.duration_secs});
    }
    let (stop, _) = watch::channel(false);
    let mut subscribers = Vec::new();
    let mut attempts = 0;
    for id in 0..c.subscribers {
        if setup.elapsed() > Duration::from_secs(30) {
            events.error("subscriber setup deadline; remaining attempts NOT_RUN");
            break;
        }
        attempts += 1;
        if let Some(mut client) = connect(c, &events, &format!("sub-{id}")).await {
            let starved = id < c.credit_starved;
            let result = client.subscribe(starved).await;
            match result {
                Ok(_) => subscribers.push((id, client, starved)),
                Err(e) => events.error(format!("subscribe{id}: {e}")),
            }
        }
    }
    let actual_subscribers = subscribers.len();
    let mut pub_clients = Vec::new();
    for id in 0..c.publishers {
        if setup.elapsed() > Duration::from_secs(30) {
            events.error("publisher setup deadline; remaining attempts NOT_RUN");
            break;
        }
        attempts += 1;
        if let Some(client) = connect(c, &events, &format!("pub-{id}")).await {
            pub_clients.push((id, client));
        }
    }
    let actual_publishers = pub_clients.len();
    let setup_secs = setup.elapsed().as_secs_f64();
    let mut receivers = JoinSet::new();
    let delivery_digest_ns = Arc::new(AtomicU64::new(0));
    for (id, mut client, starved) in subscribers {
        let events = events.clone();
        let mut stopped = stop.subscribe();
        let slow = id >= c.credit_starved && id < c.credit_starved + c.slow_subscribers;
        let slow_ms = c.slow_ms;
        let delivery_digest_ns = delivery_digest_ns.clone();
        receivers.spawn(async move{
            loop{
                if *stopped.borrow(){break;}
                if starved {let _=stopped.changed().await;break;}
                let delivery=tokio::select!{biased; _=stopped.changed()=>break, value=client.receive()=>value};
                match delivery{
                    Ok(delivery)=>{let at_ns=events.now();match payload_identity(&delivery){
                        Ok((message_id,_))=>{let started=Instant::now();let received_digest=digest(&delivery);delivery_digest_ns.fetch_add(started.elapsed().as_nanos().min(u64::MAX as u128) as u64,Ordering::Relaxed);events.emit(Event::Delivery{id:message_id,subscriber:id,at_ns,digest:received_digest});},
                        Err(e)=>events.error(format!("malformed payload sub{id}: {e}")),
                    }
                        if slow{tokio::select!{_=stopped.changed()=>break,_=tokio::time::sleep(Duration::from_millis(slow_ms))=>{}}}
                    },
                    Err(e)=>{events.error(format!("receive sub{id}: {e}"));break;}
                }
            }
            // Cancellation of an active receive closes the stream by SDK contract.
            close(client,&events,format!("sub-{id}")).await;
        });
    }
    let mut workers = JoinSet::new();
    let mut queues = HashMap::new();
    for (id, mut client) in pub_clients {
        let (tx, mut rx) = mpsc::channel::<Job>(c.queue_capacity);
        queues.insert(id, tx);
        let events = events.clone();
        workers.spawn(async move {
            while let Some(job) = rx.recv().await {
                events.emit(Event::Sent {
                    id: job.id,
                    at_ns: events.now(),
                });
                let outcome = client.publish(id, job.body).await;
                events.emit(Event::Ack {
                    id: job.id,
                    at_ns: events.now(),
                    outcome,
                });
            }
            close(client, &events, format!("pub-{id}")).await;
        });
    }
    let run_start = Instant::now();
    let mut measured_start = None;
    let mut max_lateness = 0u64;
    let mut late_offers = 0u64;
    let mut payload_preparation_ns = 0u64;
    let total = c.warmup_messages + c.messages;
    for id in 0..total {
        let scheduled = if c.rate == 0 {
            run_start
        } else {
            run_start + Duration::from_nanos(((id as u128 * 1_000_000_000) / c.rate as u128) as u64)
        };
        if c.rate > 0 {
            tokio::time::sleep_until(scheduled.into()).await;
        }
        if id == c.warmup_messages {
            measured_start = Some(Instant::now());
        }
        let lateness = scheduled.elapsed().as_nanos().min(u64::MAX as u128) as u64;
        max_lateness = max_lateness.max(lateness);
        if lateness > 1_000_000 {
            late_offers += 1;
        }
        let publisher = id % c.publishers;
        let preparation = Instant::now();
        let body = payload(c.seed, id as u64, publisher, c.payload_bytes);
        let body_digest = digest(&body);
        payload_preparation_ns += preparation.elapsed().as_nanos().min(u64::MAX as u128) as u64;
        events.emit(Event::Offered {
            id: id as u64,
            publisher,
            at_ns: events.now(),
            measured: id >= c.warmup_messages,
            digest: body_digest,
        });
        let job = Job {
            id: id as u64,
            body,
        };
        if queues
            .get(&publisher)
            .is_none_or(|q| q.try_send(job).is_err())
        {
            events.emit(Event::Ack {
                id: id as u64,
                at_ns: events.now(),
                outcome: Outcome::ClientDropped,
            });
        }
        if id % 64 == 0 {
            tokio::task::yield_now().await;
        }
    }
    let offer_secs = measured_start.unwrap_or(run_start).elapsed().as_secs_f64();
    drop(queues);
    // Every worker operation has an SDK deadline; bound the overall settlement too.
    let worker_deadline = Duration::from_secs(15);
    let publishers_finished = tokio::time::timeout(worker_deadline, async {
        while let Some(result) = workers.join_next().await {
            if let Err(e) = result {
                events.error(format!("publisher task: {e}"));
            }
        }
    })
    .await
    .is_ok();
    if !publishers_finished {
        events.error("publisher settlement deadline; incomplete outcomes remain unresolved");
        workers.abort_all();
        while workers.join_next().await.is_some() {}
    }
    let publish_settlement_secs = measured_start.unwrap_or(run_start).elapsed().as_secs_f64();
    tokio::time::sleep(Duration::from_millis(c.drain_ms)).await;
    let delivery_observation_secs = measured_start.unwrap_or(run_start).elapsed().as_secs_f64();
    let disconnect = Instant::now();
    let _ = stop.send(true);
    while let Some(result) = receivers.join_next().await {
        if let Err(e) = result {
            events.error(format!("subscriber task: {e}"));
        }
    }
    json!({"mode":"load","established_connections":actual_publishers+actual_subscribers,"authenticated_publishers":actual_publishers,"authenticated_subscribers":actual_subscribers,
        "connection_attempts":attempts,"connection_failures":attempts-actual_publishers-actual_subscribers,"not_attempted":c.publishers+c.subscribers-attempts,"connection_establishment_secs":setup_secs,
        "connections_per_sec":(actual_publishers+actual_subscribers) as f64/setup_secs,
        "offer_secs":offer_secs,"publish_settlement_secs":publish_settlement_secs,"delivery_observation_secs":delivery_observation_secs,
        "disconnect_secs":disconnect.elapsed().as_secs_f64(),"publishers_finished":publishers_finished,
        "delivery_digest_ns":delivery_digest_ns.load(Ordering::Relaxed),
        "payload_generation_and_digest_ns":payload_preparation_ns,
        "late_offers_over_1ms":late_offers,"max_offer_lateness_ns":max_lateness,
        "timing":"single monotonic origin within independent generator process; E2E from Sent to subscriber receipt; no cross-host clock subtraction"})
}
#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() {
    let args: Vec<_> = std::env::args().skip(1).collect();
    if args.first().is_some_and(|s| s == "replay") {
        let result = replay_run(&args);
        if let Err(e) = result {
            eprintln!("replay: {e}");
            std::process::exit(2);
        }
        return;
    }
    if std::env::args().any(|s| s == "--help") {
        println!(
            "rovqet-testkit q0-load --endpoint 127.0.0.1:7443 --publishers 1 --subscribers 1 --messages 1000 --rate 1000 --payload-bytes 256 --output DIR\n--backend rovqet|nats (default rovqet). NATS Core uses static loopback auth rovqet/development and local-enqueue outcomes, no per-publication ADMITTED ACK; credits unsupported. Flags and values must be separate. rate0 is a bounded burst. --duration-secs derives messages if omitted. --idle-connections N measures real authenticated idle sessions. --warmup-messages N or --warmup-secs N; --slow-subscribers N --slow-ms N --credit-starved N --drain-ms N --check-order true --seed 42 --queue-capacity 128."
        );
        return;
    }
    match run().await {
        Ok(valid) => {
            if !valid {
                std::process::exit(1)
            }
        }
        Err(error) => {
            eprintln!("testkit: {error}");
            std::process::exit(2)
        }
    }
}
async fn run() -> Result<bool, String> {
    let config = Config::parse(std::env::args().skip(1))?;
    std::fs::create_dir_all(&config.output).map_err(|e| e.to_string())?;
    let log = EventLog::create(&config.output.join("events.jsonl"), 32768)?;
    let (tx, mut rx) = mpsc::channel(32768);
    let failed = Arc::new(AtomicBool::new(false));
    let errors = Arc::new(AtomicU64::new(0));
    let consumer_errors = errors.clone();
    let mut oracle = Oracle::new(
        config.subscribers,
        config.messages + config.warmup_messages,
        config.check_order,
    )?;
    let origin = Instant::now();
    let events = Events {
        tx,
        failed: failed.clone(),
        origin,
    };
    let warmup_bound = config.warmup_messages as u64;
    let collector = tokio::spawn(async move {
        let mut first_measured = None;
        let mut last_measured = 0;
        let mut oracle_ns = 0u64;
        let mut logging_enqueue_ns = 0u64;
        let mut observed_events = 0u64;
        while let Some(event) = rx.recv().await {
            match &event {
                Event::Offered {
                    at_ns,
                    measured: true,
                    ..
                } => {
                    first_measured.get_or_insert(*at_ns);
                    last_measured = last_measured.max(*at_ns);
                }
                Event::Ack { id, at_ns, .. } | Event::Delivery { id, at_ns, .. }
                    if *id >= warmup_bound =>
                {
                    last_measured = last_measured.max(*at_ns);
                }
                _ => {}
            }
            if matches!(event, Event::Error { .. }) {
                consumer_errors.fetch_add(1, Ordering::Relaxed);
            }
            observed_events += 1;
            let aggregate_started = Instant::now();
            if let Err(e) = oracle.observe(&event) {
                consumer_errors.fetch_add(1, Ordering::Relaxed);
                if consumer_errors.load(Ordering::Relaxed) <= 8 {
                    eprintln!("oracle: {e}");
                }
            }
            oracle_ns += aggregate_started.elapsed().as_nanos().min(u64::MAX as u128) as u64;
            let logging_started = Instant::now();
            if let Err(e) = log.record(&event) {
                consumer_errors.fetch_add(1, Ordering::Relaxed);
                if consumer_errors.load(Ordering::Relaxed) <= 8 {
                    eprintln!("evidence: {e}");
                }
            }
            logging_enqueue_ns += logging_started.elapsed().as_nanos().min(u64::MAX as u128) as u64;
        }
        let summary = oracle.summary();
        let finished = log.finish();
        (
            summary,
            finished,
            first_measured.map(|first| last_measured.saturating_sub(first) as f64 / 1e9),
            json!({"oracle_observe_ns":oracle_ns,"logging_enqueue_ns":logging_enqueue_ns,"observed_events":observed_events}),
        )
    });
    let measurement = execute(&config, events.clone()).await;
    drop(events);
    let (summary, flushed, active_measured_secs, instrumentation) =
        collector.await.map_err(|e| e.to_string())?;
    flushed?;
    let serialized = serde_json::to_value(&summary).map_err(|e| e.to_string())?;
    let valid = !failed.load(Ordering::Relaxed)
        && errors.load(Ordering::Relaxed) == 0
        && serialized
            .get("duplicates")
            .and_then(|v| v.as_u64())
            .unwrap_or(0)
            == 0
        && serialized
            .get("corrupt")
            .and_then(|v| v.as_u64())
            .unwrap_or(0)
            == 0
        && serialized
            .get("unexpected")
            .and_then(|v| v.as_u64())
            .unwrap_or(0)
            == 0
        && !serialized
            .get("incomplete")
            .and_then(|v| v.as_bool())
            .unwrap_or(true)
        && (!config.check_order
            || serialized
                .get("ordering_violations")
                .and_then(|v| v.as_u64())
                .unwrap_or(0)
                == 0);
    let count = |key: &str| serialized.get(key).and_then(|v| v.as_u64()).unwrap_or(0) as f64;
    let rate = |key: &str, seconds: &str| {
        measurement
            .get(seconds)
            .and_then(|v| v.as_f64())
            .filter(|s| *s > 0.0)
            .map(|s| count(key) / s)
    };
    let throughput = json!({"offered_per_sec":rate("offered","offer_secs"),"sent_per_sec":rate("sent","publish_settlement_secs"),
        "published_unconfirmed_per_sec":rate("published_unconfirmed","publish_settlement_secs"),
        "admitted_per_sec":rate("admitted","publish_settlement_secs"),"rejected_per_sec":rate("rejected","publish_settlement_secs"),
        "physical_deliveries_per_sec":rate("delivered","delivery_observation_secs"),
        "active_measured_secs":active_measured_secs,
        "active_physical_deliveries_per_sec":active_measured_secs.filter(|s|*s>0.0).map(|s|count("delivered")/s),
        "active_admitted_per_sec":active_measured_secs.filter(|s|*s>0.0).map(|s|count("admitted")/s),
        "active_window":"first measured offer through last measured offer/ACK/delivery, excludes empty final drain; counters still include every rejected/client-dropped/unknown outcome","delivery_rate_window":"measurement start through settlement and configured final drain; includes waiting for missing work",
        "unique_deliveries_per_sec":rate("unique_delivered","delivery_observation_secs")});
    let result = json!({"instrumentation":instrumentation,"config":config,"oracle":summary,"measurement":measurement,"throughput":throughput,"valid":valid,
        "event_queue_overflow":failed.load(Ordering::Relaxed),"error_events_or_invalid_records":errors.load(Ordering::Relaxed),
        "publication_semantics":if config.backend == "nats" {"NATS Core publish returns local client enqueue, recorded published_unconfirmed. No per-publication admission ACK. Setup/settlement uses an independent temporary-subject echo barrier, not per-publication admission or recipient receipt. Control echoes are outside workload events; server aggregate counters include them. ack_latency is empty. unconfirmed_unreceived is not definite broker loss. Client subscriber queue64 drops without Rovqet flow credits; Core has no TTL."} else {"Rovqet ADMITTED is volatile bounded broker admission; no durability or recipient receipt guarantee."},
        "elapsed_secs":origin.elapsed().as_secs_f64(),"status":if valid{"COMPLETE"}else{"INVALID"},
        "notes":["Q0 missing admitted copies are observations, not durability failures; unknown outcomes remain separate","Socket/kernel/network resources supplied by external /proc observer; metrics unavailable inside this process are not guessed","Exact latency buckets retained; percentiles never averaged; recovery testing NOT_RUN for Q0"]});
    write_json(&config.output.join("summary.json"), &result)?;
    println!("{}", json!({"valid":valid,"output":config.output}));
    Ok(valid)
}
fn replay_run(args: &[String]) -> Result<(), String> {
    if args.len() != 3 || args[1] != "--run" {
        return Err("expected replay --run DIR".into());
    }
    let dir = PathBuf::from(&args[2]);
    let saved: serde_json::Value = serde_json::from_slice(
        &std::fs::read(dir.join("summary.json")).map_err(|e| e.to_string())?,
    )
    .map_err(|e| e.to_string())?;
    let c: Config = serde_json::from_value(saved["config"].clone()).map_err(|e| e.to_string())?;
    c.validate()?;
    let mut oracle = Oracle::new(c.subscribers, c.messages + c.warmup_messages, c.check_order)?;
    rovqet_testkit::replay(dir.join("events.jsonl"), &mut oracle)?;
    if serde_json::to_value(oracle.summary()).map_err(|e| e.to_string())? != saved["oracle"] {
        return Err("JSONL replay differs from aggregate evidence".into());
    }
    if saved["valid"] != true {
        return Err("JSONL replays consistently but original run was INVALID".into());
    }
    println!("PASS: independent JSONL replay matches every oracle aggregate");
    Ok(())
}

fn write_json(path: &Path, value: &serde_json::Value) -> Result<(), String> {
    std::fs::write(
        path,
        serde_json::to_vec_pretty(value).map_err(|e| e.to_string())?,
    )
    .map_err(|e| e.to_string())
}
#[cfg(test)]
mod tests {
    use super::*;
    fn args(s: &str) -> Vec<String> {
        s.split_whitespace().map(String::from).collect()
    }
    #[test]
    fn invalid_configurations_are_rejected() {
        for s in [
            "--payload-bytes512",
            "--publishers 0",
            "--subscribers 5000",
            "--rate 0 --duration-secs1",
            "--duration-secs NaN",
            "--queue-capacity 0",
            "--messages 0",
            "--unknown 1",
            "--rate 10 --rate 20",
        ] {
            assert!(Config::parse(args(s)).is_err(), "{s}");
        }
    }
    #[test]
    fn duration_and_warmup_are_reproducible() {
        let c = Config::parse(args(
            "--rate 200 --duration-secs 2 --warmup-secs 0.5 --seed 7",
        ))
        .unwrap();
        assert_eq!((c.messages, c.warmup_messages, c.seed), (400, 100, 7));
    }
    #[test]
    fn backend_parity_and_unsupported_credit_are_explicit() {
        let rovqet = Config::parse(args(
            "--seed 7 --rate 500 --messages 100 --payload-bytes 1024",
        ))
        .unwrap();
        let nats = Config::parse(args(
            "--backend nats --seed 7 --rate 500 --messages 100 --payload-bytes 1024",
        ))
        .unwrap();
        assert_eq!(
            (
                rovqet.seed,
                rovqet.rate,
                rovqet.messages,
                rovqet.payload_bytes
            ),
            (nats.seed, nats.rate, nats.messages, nats.payload_bytes)
        );
        assert!(Config::parse(args("--backend nats --credit-starved 1")).is_err());
        assert!(Config::parse(args("--backend fictional")).is_err());
        let old = serde_json::to_value(rovqet).unwrap();
        let mut old_without_backend = old.clone();
        old_without_backend
            .as_object_mut()
            .unwrap()
            .remove("backend");
        assert_eq!(
            serde_json::from_value::<Config>(old_without_backend)
                .unwrap()
                .backend,
            "rovqet"
        );
    }
    #[tokio::test]
    async fn nats_fixture_uses_real_tcp_identity_and_local_enqueue_semantics() {
        use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = listener.local_addr().unwrap();
        let fixture = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            let (reader, mut writer) = socket.into_split();
            let mut reader = BufReader::new(reader);
            writer.write_all(b"INFO {\"server_id\":\"fixture\",\"server_name\":\"fixture\",\"version\":\"2.11.10\",\"proto\":1,\"headers\":true,\"auth_required\":true,\"max_payload\":1048576}\r\n").await.unwrap();
            let mut subscriptions = HashMap::new();
            loop {
                let mut line = String::new();
                if reader.read_line(&mut line).await.unwrap() == 0 {
                    break;
                }
                let parts: Vec<_> = line.split_whitespace().collect();
                match parts.first().copied() {
                    Some("CONNECT") => {
                        assert!(line.contains("rovqet") && line.contains("development"));
                    }
                    Some("PING") => writer.write_all(b"PONG\r\n").await.unwrap(),
                    Some("PONG") => {}
                    Some("UNSUB") => {
                        let sid = parts[1];
                        subscriptions.retain(|_, value| value != sid);
                    }
                    Some("SUB") => {
                        subscriptions
                            .insert(parts[1].to_owned(), parts.last().unwrap().to_string());
                    }
                    Some("PUB") => {
                        let size: usize = parts.last().unwrap().parse().unwrap();
                        let mut body = vec![0; size + 2];
                        reader.read_exact(&mut body).await.unwrap();
                        let subject = parts[1];
                        let sid = subscriptions
                            .get(subject)
                            .expect("fixture subscriber exists");
                        writer
                            .write_all(format!("MSG {subject} {sid} {size}\r\n").as_bytes())
                            .await
                            .unwrap();
                        writer.write_all(&body).await.unwrap();
                    }
                    other => panic!("unexpected NATS fixture command {other:?}"),
                }
            }
        });
        let (tx, mut rx) = mpsc::channel(128);
        let events = Events {
            tx,
            failed: Arc::new(AtomicBool::new(false)),
            origin: Instant::now(),
        };
        let c = Config {
            backend: "nats".into(),
            endpoint,
            ..Config::default()
        };
        let mut client = connect(&c, &events, "fixture").await.unwrap();
        client.subscribe(false).await.unwrap();
        let body = payload(42, 7, 0, 256);
        assert_eq!(
            client.publish(0, body.clone()).await,
            Outcome::PublishedUnconfirmed
        );
        assert_eq!(client.receive().await.unwrap(), body);
        close(client, &events, "fixture".into()).await;
        tokio::time::timeout(Duration::from_secs(2), fixture)
            .await
            .unwrap()
            .unwrap();
        drop(events);
        while let Some(event) = rx.recv().await {
            assert!(!matches!(event, Event::Error { .. }), "{event:?}");
        }
    }
    #[tokio::test]
    async fn refused_connection_produces_invalid_evidence() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = listener.local_addr().unwrap();
        drop(listener);
        let (tx, mut rx) = mpsc::channel(4);
        let e = Events {
            tx,
            failed: Arc::new(AtomicBool::new(false)),
            origin: Instant::now(),
        };
        let c = Config {
            endpoint,
            ..Config::default()
        };
        assert!(connect(&c, &e, "refused").await.is_none());
        assert!(matches!(
            rx.recv().await,
            Some(Event::Connection {
                connected: false,
                ..
            })
        ));
        assert!(matches!(rx.recv().await, Some(Event::Error { .. })));
    }
    #[tokio::test]
    async fn stalled_handshake_timeout_closes_the_real_socket() {
        use tokio::io::AsyncReadExt;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut bytes = Vec::new();
            tokio::time::timeout(Duration::from_secs(1), socket.read_to_end(&mut bytes))
                .await
                .unwrap()
                .unwrap();
            assert!(!bytes.is_empty());
        });
        let (tx, mut rx) = mpsc::channel(4);
        let events = Events {
            tx,
            failed: Arc::new(AtomicBool::new(false)),
            origin: Instant::now(),
        };
        let c = Config {
            endpoint,
            request_timeout_ms: 20,
            ..Config::default()
        };
        assert!(connect(&c, &events, "stalled").await.is_none());
        assert!(matches!(
            rx.recv().await,
            Some(Event::Connection {
                connected: false,
                ..
            })
        ));
        assert!(matches!(rx.recv().await, Some(Event::Error { .. })));
        server.await.unwrap();
    }
}
