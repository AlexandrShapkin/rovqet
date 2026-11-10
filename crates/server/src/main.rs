//! Isolated development broker: plaintext and static identity on loopback only.
#![forbid(unsafe_code)]
use rovqet_core::{Priority, QoS};
use rovqet_q0::Broker;
use rovqet_router::{OverflowPolicy, TopicConfig};
use rovqet_transport::{Config, TransportStats, serve};
mod q1;
use std::{
    collections::{HashMap, HashSet},
    io,
    net::SocketAddr,
    sync::{Arc, atomic::Ordering},
    time::{Duration, Instant},
};

const DEFAULT_CONNECTIONS: usize = 1024;
const BENCHMARK_CONNECTION_CEILING: usize = 20_000;
// Frame + outbound + protected control bytes + metadata/allocator margin.
// This conservative user-space envelope is not a reservation or a kernel-memory estimate.
const CONNECTION_ENVELOPE_BYTES: usize = 1_048_576 + 262_144 + 4_096 + 65_536;

#[derive(Debug)]
struct Options {
    listen: SocketAddr,
    max_connections: usize,
    connection_budget_bytes: usize,
    metrics_interval: Option<Duration>,
    q1: Option<q1::Options>,
}
fn invalid(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message.into())
}
impl Options {
    fn parse(args: &[String]) -> io::Result<Self> {
        let mut result = Self {
            listen: "127.0.0.1:7443".parse().unwrap(),
            max_connections: DEFAULT_CONNECTIONS,
            connection_budget_bytes: DEFAULT_CONNECTIONS * CONNECTION_ENVELOPE_BYTES,
            metrics_interval: None,
            q1: None,
        };
        let mut q1_values = HashMap::new();
        let mut seen = HashSet::new();
        let mut explicit_budget = false;
        for pair in args.chunks(2) {
            if pair.len() != 2 || !seen.insert(pair[0].as_str()) {
                return Err(invalid("expected unique --option value pairs; see --help"));
            }
            match pair[0].as_str() {
                "--listen" => {
                    result.listen = pair[1]
                        .parse()
                        .map_err(|_| invalid("invalid listen address"))?
                }
                "--max-connections" => {
                    result.max_connections = pair[1]
                        .parse()
                        .map_err(|_| invalid("invalid connection count"))?
                }
                "--connection-budget-bytes" => {
                    result.connection_budget_bytes = pair[1]
                        .parse()
                        .map_err(|_| invalid("invalid connection budget"))?;
                    explicit_budget = true;
                }
                "--metrics-interval-ms" => {
                    let ms: u64 = pair[1]
                        .parse()
                        .map_err(|_| invalid("invalid metrics interval"))?;
                    if !(50..=60_000).contains(&ms) {
                        return Err(invalid("metrics interval must be 50..60000 ms"));
                    }
                    result.metrics_interval = Some(Duration::from_millis(ms));
                }
                "--q1-data-dir"
                | "--q1-topic"
                | "--q1-tenant-id"
                | "--q1-topic-id"
                | "--q1-partition-id"
                | "--q1-incarnation"
                | "--q1-owner-epoch"
                | "--q1-dedup-window-ms"
                | "--q1-max-producers"
                | "--q1-max-outcomes"
                | "--q1-initialize-new-log" => {
                    q1_values.insert(pair[0].clone(), pair[1].clone());
                }
                _ => return Err(invalid("unknown option; see --help")),
            }
        }
        if !result.listen.ip().is_loopback() {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "development listener must be loopback",
            ));
        }
        if result.max_connections == 0 || result.max_connections > BENCHMARK_CONNECTION_CEILING {
            return Err(invalid("max-connections must be 1..20000"));
        }
        if result.max_connections > DEFAULT_CONNECTIONS && !explicit_budget {
            return Err(invalid(
                "higher connection limits require an explicit --connection-budget-bytes",
            ));
        }
        let required = result
            .max_connections
            .checked_mul(CONNECTION_ENVELOPE_BYTES)
            .ok_or_else(|| invalid("connection envelope overflow"))?;
        if required > result.connection_budget_bytes {
            return Err(invalid(format!(
                "connection envelope requires at least {required} bytes; kernel socket memory and other broker queues are additional"
            )));
        }
        result.q1 = q1::Options::parse(&q1_values)?;
        Ok(result)
    }
}

fn development_broker(max_sessions: usize) -> Arc<Broker> {
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
    Broker::new(topics, max_sessions, 1024, 16 * 1024 * 1024)
        .expect("fixed development topic configuration is valid")
}

fn metrics(broker: &Broker, stats: &TransportStats, started: Instant, options: &Options) -> String {
    let load = |counter: &std::sync::atomic::AtomicU64| counter.load(Ordering::Relaxed);
    let c = &broker.counters;
    let ingress = broker.ingress_usage();
    let egress = broker.egress_usage();
    format!(
        concat!(
            "{{\"time_mono_ns\":{},\"max_connections\":{},\"connection_envelope_bytes\":{},",
            "\"offered\":{},\"admitted\":{},\"rejected\":{},\"enqueued\":{},",
            "\"dropped_overflow\":{},\"dropped_expired\":{},\"dropped_unsubscribed\":{},\"dropped_shutdown_ingress\":{},",
            "\"egress_rejected\":{},\"flow_rejected\":{},\"overload_transitions\":{},\"overload_state\":\"{:?}\",",
            "\"delivered\":{},\"delivered_bytes\":{},\"outbound_enqueued\":{},\"writer_expired\":{},\"write_failed\":{},",
            "\"dropped_newest\":{},\"dropped_oldest\":{},\"coalesced\":{},\"dropped_closed\":{},\"dropped_shutdown_outbound\":{},",
            "\"ingress_messages\":{},\"ingress_bytes\":{},\"egress_messages\":{},\"egress_bytes\":{},",
            "\"active_subscriptions\":{},\"broker_work_sessions\":{}}}"
        ),
        started.elapsed().as_nanos(),
        options.max_connections,
        CONNECTION_ENVELOPE_BYTES,
        load(&c.offered),
        load(&c.admitted),
        load(&c.rejected),
        load(&c.enqueued),
        load(&c.dropped_overflow),
        load(&c.dropped_expired),
        load(&c.dropped_unsubscribed),
        load(&c.dropped_shutdown),
        load(&c.egress_rejected),
        load(&c.flow_rejected),
        load(&c.overload_transitions),
        broker.overload_state(),
        load(&stats.delivered),
        load(&stats.delivered_bytes),
        load(&stats.enqueued),
        load(&stats.writer_expired),
        load(&stats.write_failed),
        load(&stats.dropped_newest),
        load(&stats.dropped_oldest),
        load(&stats.coalesced),
        load(&stats.dropped_closed),
        load(&stats.dropped_shutdown),
        ingress.messages,
        ingress.bytes,
        egress.messages,
        egress.bytes,
        broker.active_subscriptions(),
        broker.active_sessions()
    )
}

#[tokio::main]
async fn main() -> io::Result<()> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if args == ["--version"] {
        println!(
            "rovqet {} (wire v1; development profile)",
            env!("CARGO_PKG_VERSION")
        );
        return Ok(());
    }
    if args == ["--help"] || args == ["-h"] {
        println!(concat!(
            "Usage: rovqet [--listen 127.0.0.1:7443] [--max-connections N]\n",
            "  [--connection-budget-bytes N] [--metrics-interval-ms 250]\n",
            "  Q1 opt-in (all required): --q1-data-dir EXISTING_DIR --q1-topic NAME\n",
            "    --q1-tenant-id 1 --q1-topic-id ID --q1-partition-id ID\n",
            "    --q1-incarnation U128 --q1-owner-epoch U64\n",
            "    [--q1-initialize-new-log true] ONLY for new empty storage and a fresh incarnation\n",
            "    [--q1-dedup-window-ms 86400000 --q1-max-producers 100000 --q1-max-outcomes 100000]\n",
            "Development only: plaintext, static dev identity, loopback required.\n",
            "Q0 topics: demo, telemetry. Default connection limit: 1024.\n",
            "Higher limits (ceiling 20000) require an explicit user-space envelope budget\n",
            "of at least 1380352 bytes/connection. This does not reserve memory; kernel\n",
            "socket memory, shared queues and process overhead are additional.\n",
            "Periodic BROKER_METRICS JSON is opt-in; a final snapshot is always emitted."
        ));
        return Ok(());
    }
    let options = Arc::new(Options::parse(&args)?);
    q1::validate_test_environment()?;
    let listener = tokio::net::TcpListener::bind(options.listen).await?;
    let route = if let Some(q1) = options.q1.clone() {
        Some(Arc::new(
            tokio::task::spawn_blocking(move || q1::Route::open(q1))
                .await
                .map_err(io::Error::other)??,
        ))
    } else {
        None
    };
    if let Some(q1) = &options.q1 {
        eprintln!(
            "Q1 recovered: topic={} partition={:?} incarnation={} owner_epoch={} dedup_window_ms={} max_producers={} max_outcomes={}",
            q1.topic,
            q1.partition,
            q1.incarnation,
            q1.owner_epoch,
            q1.dedup_window.as_millis(),
            q1.max_producers,
            q1.max_outcomes
        );
    }
    eprintln!(
        "rovqet development profile listening on {}",
        listener.local_addr()?
    );
    let started = Instant::now();
    let broker = development_broker(options.max_connections);
    let config = Config {
        max_connections: options.max_connections,
        ..Config::default()
    };
    let stats = config.stats.clone();
    let (stop, mut rx) = tokio::sync::watch::channel(false);
    let dispatcher = tokio::spawn(broker.clone().run(async move {
        let _ = rx.changed().await;
    }));
    let metrics_task = options.metrics_interval.map(|interval| {
        let broker = broker.clone(); let stats = stats.clone(); let options = options.clone();
        let mut stop = stop.subscribe();
        tokio::spawn(async move {
            loop { tokio::select! {
                _ = stop.changed() => break,
                _ = tokio::time::sleep(interval) => eprintln!("BROKER_METRICS {}", metrics(&broker, &stats, started, &options)),
            }}
        })
    });
    let handler = Arc::new(q1::Dispatch::new(broker.clone(), route.clone()));
    let drain = handler.clone();
    let result = serve(listener, config, handler.clone(), async move {
        let _ = tokio::signal::ctrl_c().await;
        drain.drain();
    })
    .await;
    drop(handler);
    if let Some(route) = route {
        let owned = Arc::try_unwrap(route)
            .map_err(|_| io::Error::other("Q1 route retained after session shutdown"))?;
        match tokio::time::timeout(
            Duration::from_secs(5),
            tokio::task::spawn_blocking(move || owned.shutdown()),
        )
        .await
        {
            Ok(done) => done.map_err(io::Error::other)??,
            Err(_) => {
                eprintln!(
                    "Q1 shutdown deadline exceeded; pending outcomes unknown; restart recovery required"
                );
                std::process::exit(1);
            }
        }
    }
    let _ = stop.send(true);
    let _ = dispatcher.await;
    if let Some(task) = metrics_task {
        let _ = task.await;
    }
    eprintln!(
        "BROKER_METRICS {}",
        metrics(&broker, &stats, started, &options)
    );
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    fn args(items: &[&str]) -> Vec<String> {
        items.iter().map(|s| (*s).into()).collect()
    }
    #[test]
    fn defaults_and_loopback_are_preserved() {
        let o = Options::parse(&[]).unwrap();
        assert_eq!(o.max_connections, 1024);
        assert!(o.listen.ip().is_loopback());
        assert!(o.metrics_interval.is_none());
        assert!(Options::parse(&args(&["--listen", "0.0.0.0:7443"])).is_err());
    }
    #[test]
    fn high_density_requires_a_sufficient_explicit_budget() {
        assert!(Options::parse(&args(&["--max-connections", "10000"])).is_err());
        assert!(
            Options::parse(&args(&[
                "--max-connections",
                "10000",
                "--connection-budget-bytes",
                "1000000"
            ]))
            .is_err()
        );
        let budget = (10000 * CONNECTION_ENVELOPE_BYTES).to_string();
        assert_eq!(
            Options::parse(&args(&[
                "--max-connections",
                "10000",
                "--connection-budget-bytes",
                &budget
            ]))
            .unwrap()
            .max_connections,
            10000
        );
    }
    #[test]
    fn invalid_or_ambiguous_config_is_rejected() {
        for bad in [
            &["--max-connections", "0"][..],
            &["--max-connections", "20001"],
            &["--listen"],
            &["--max-connections", "4", "--max-connections", "5"],
            &["--metrics-interval-ms", "0"],
            &["--unknown", "1"],
        ] {
            assert!(Options::parse(&args(bad)).is_err(), "{bad:?}");
        }
    }
    #[test]
    fn metric_snapshot_exposes_zero_bounded_queues() {
        let broker = development_broker(32);
        let stats = TransportStats::default();
        let line = metrics(
            &broker,
            &stats,
            Instant::now(),
            &Options::parse(&[]).unwrap(),
        );
        assert!(line.starts_with("{\"time_mono_ns\":"));
        assert!(line.contains("\"ingress_messages\":0"));
        assert!(line.contains("\"egress_bytes\":0"));
        assert!(line.contains("\"overload_state\":\"Normal\""));
    }
}
