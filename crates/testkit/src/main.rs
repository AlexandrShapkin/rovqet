//! Independent Q0 load/oracle process. Offers are scheduled independently of ACKs.
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

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Config {
    endpoint: std::net::SocketAddr,
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
        if !self.endpoint.ip().is_loopback()
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
async fn connect(c: &Config, events: &Events, name: &str) -> Option<Client> {
    let begin = events.now();
    match Client::connect(client_config(c)).await {
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
async fn close(mut client: Client, events: &Events, name: String) {
    let begin = events.now();
    let result = client.close().await;
    // A receive timeout closes a partial read safely. Such connections are measured separately.
    let _ = result;
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
            let result = if starved {
                client.subscribe_with_credit("demo", 0, 0).await
            } else {
                client.subscribe("demo").await
            };
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
    for (id, mut client, starved) in subscribers {
        let events = events.clone();
        let mut stopped = stop.subscribe();
        let slow = id >= c.credit_starved && id < c.credit_starved + c.slow_subscribers;
        let slow_ms = c.slow_ms;
        receivers.spawn(async move{
            loop{
                if *stopped.borrow(){break;}
                if starved {let _=stopped.changed().await;break;}
                let delivery=tokio::select!{biased; _=stopped.changed()=>break, value=client.receive()=>value};
                match delivery{
                    Ok(delivery)=>{let at_ns=events.now();match payload_identity(&delivery.payload){
                        Ok((message_id,_))=>events.emit(Event::Delivery{id:message_id,subscriber:id,at_ns,digest:digest(&delivery.payload)}),
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
                let result = client
                    .publish(
                        "demo",
                        &job.body,
                        PublishOptions {
                            key: id.to_be_bytes().to_vec(),
                            ttl: Duration::from_secs(2),
                            ..PublishOptions::default()
                        },
                    )
                    .await;
                let outcome = match result {
                    Ok(_) => Outcome::Admitted,
                    Err(ClientError::Remote { .. }) => Outcome::Rejected,
                    Err(ClientError::OutcomeUnknown) => Outcome::Unknown,
                    Err(ClientError::Closed) => Outcome::Unknown,
                    Err(_) => Outcome::Unknown,
                };
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
        let body = payload(c.seed, id as u64, publisher, c.payload_bytes);
        events.emit(Event::Offered {
            id: id as u64,
            publisher,
            at_ns: events.now(),
            measured: id >= c.warmup_messages,
            digest: digest(&body),
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
            "rovqet-testkit q0-load --endpoint 127.0.0.1:7443 --publishers 1 --subscribers 1 --messages 1000 --rate 1000 --payload-bytes 256 --output DIR\nFlags and values must be separate. rate0 is a bounded burst. --duration-secs derives messages if omitted. --idle-connections N measures real authenticated idle sessions. --warmup-messages N or --warmup-secs N; --slow-subscribers N --slow-ms N --credit-starved N --drain-ms N --check-order true --seed 42 --queue-capacity 128."
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
            if let Err(e) = oracle.observe(&event) {
                consumer_errors.fetch_add(1, Ordering::Relaxed);
                if consumer_errors.load(Ordering::Relaxed) <= 8 {
                    eprintln!("oracle: {e}");
                }
            }
            if let Err(e) = log.record(&event) {
                consumer_errors.fetch_add(1, Ordering::Relaxed);
                if consumer_errors.load(Ordering::Relaxed) <= 8 {
                    eprintln!("evidence: {e}");
                }
            }
        }
        let summary = oracle.summary();
        let finished = log.finish();
        (
            summary,
            finished,
            first_measured.map(|first| last_measured.saturating_sub(first) as f64 / 1e9),
        )
    });
    let measurement = execute(&config, events.clone()).await;
    drop(events);
    let (summary, flushed, active_measured_secs) = collector.await.map_err(|e| e.to_string())?;
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
        "admitted_per_sec":rate("admitted","publish_settlement_secs"),"rejected_per_sec":rate("rejected","publish_settlement_secs"),
        "physical_deliveries_per_sec":rate("delivered","delivery_observation_secs"),
        "active_measured_secs":active_measured_secs,
        "active_physical_deliveries_per_sec":active_measured_secs.filter(|s|*s>0.0).map(|s|count("delivered")/s),
        "active_admitted_per_sec":active_measured_secs.filter(|s|*s>0.0).map(|s|count("admitted")/s),
        "active_window":"first measured offer through last measured offer/ACK/delivery, excludes empty final drain; counters still include every rejected/client-dropped/unknown outcome","delivery_rate_window":"measurement start through settlement and configured final drain; includes waiting for missing work",
        "unique_deliveries_per_sec":rate("unique_delivered","delivery_observation_secs")});
    let result = json!({"config":config,"oracle":summary,"measurement":measurement,"throughput":throughput,"valid":valid,
        "event_queue_overflow":failed.load(Ordering::Relaxed),"error_events_or_invalid_records":errors.load(Ordering::Relaxed),
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
