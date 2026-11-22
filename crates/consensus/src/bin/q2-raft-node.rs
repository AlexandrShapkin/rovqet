//! One Q2 shard Raft voter as an observable process.
//!
//! The process prints one JSON event per line and accepts commands on stdin, so a
//! multi-process harness can observe leadership, applied revisions, snapshots and the
//! installed ownership authority without reaching into the library's internals.
use rovqet_consensus::types::{ShardCommand, ShardNodeId, ShardPublish, ShardRequest};
use rovqet_consensus::{ShardAuthority, ShardNode, ShardNodeConfig};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::{Duration, Instant};
use tokio::io::{AsyncBufReadExt, BufReader};

fn usage() -> String {
    "usage: q2-raft-node --id N --cluster-id N --shard-id N --listen HOST:PORT \
     --data-dir PATH [--peer N=HOST:PORT]... [--bootstrap true|false] \
     [--election-timeout-ms N] [--heartbeat-interval-ms N] [--version]"
        .to_string()
}

struct Arguments {
    id: ShardNodeId,
    cluster_id: u128,
    shard_id: u64,
    listen: SocketAddr,
    data_directory: PathBuf,
    peers: Vec<(ShardNodeId, SocketAddr)>,
    bootstrap: bool,
    election_timeout_ms: u64,
    heartbeat_interval_ms: u64,
}

fn parse_arguments(raw: &[String]) -> Result<Arguments, String> {
    let mut id: Option<ShardNodeId> = None;
    let mut cluster_id: Option<u128> = None;
    let mut shard_id: Option<u64> = None;
    let mut listen: Option<SocketAddr> = None;
    let mut data_directory: Option<PathBuf> = None;
    let mut peers: Vec<(ShardNodeId, SocketAddr)> = Vec::new();
    let mut bootstrap = false;
    let mut election_timeout_ms = 300u64;
    let mut heartbeat_interval_ms = 80u64;

    let mut index = 0;
    while index < raw.len() {
        let flag = raw[index].as_str();
        let value = raw
            .get(index + 1)
            .ok_or_else(|| format!("{flag} needs a value\n{}", usage()))?;
        let parse_error = |name: &str, error: &dyn std::fmt::Display| {
            format!("{name} {value:?} is invalid: {error}\n{}", usage())
        };
        match flag {
            "--id" => id = Some(value.parse().map_err(|error| parse_error("--id", &error))?),
            "--cluster-id" => {
                cluster_id = Some(
                    value
                        .parse()
                        .map_err(|error| parse_error("--cluster-id", &error))?,
                )
            }
            "--shard-id" => {
                shard_id = Some(
                    value
                        .parse()
                        .map_err(|error| parse_error("--shard-id", &error))?,
                )
            }
            "--listen" => {
                listen = Some(
                    value
                        .parse()
                        .map_err(|error| parse_error("--listen", &error))?,
                )
            }
            "--data-dir" => data_directory = Some(PathBuf::from(value)),
            "--peer" => {
                let (peer, address) = value
                    .split_once('=')
                    .ok_or_else(|| format!("--peer needs N=HOST:PORT, got {value}"))?;
                peers.push((
                    peer.parse()
                        .map_err(|error| parse_error("--peer id", &error))?,
                    address
                        .parse()
                        .map_err(|error| parse_error("--peer address", &error))?,
                ));
            }
            "--bootstrap" => {
                bootstrap = match value.as_str() {
                    "true" => true,
                    "false" => false,
                    other => return Err(format!("--bootstrap needs true|false, got {other}")),
                }
            }
            "--election-timeout-ms" => {
                election_timeout_ms = value
                    .parse()
                    .map_err(|error| parse_error("--election-timeout-ms", &error))?
            }
            "--heartbeat-interval-ms" => {
                heartbeat_interval_ms = value
                    .parse()
                    .map_err(|error| parse_error("--heartbeat-interval-ms", &error))?
            }
            other => return Err(format!("unknown argument {other}\n{}", usage())),
        }
        index += 2;
    }

    Ok(Arguments {
        id: id.ok_or_else(usage)?,
        cluster_id: cluster_id.ok_or_else(usage)?,
        shard_id: shard_id.ok_or_else(usage)?,
        listen: listen.ok_or_else(usage)?,
        data_directory: data_directory.ok_or_else(usage)?,
        peers,
        bootstrap,
        election_timeout_ms,
        heartbeat_interval_ms,
    })
}

/// Renders a digest chain as lowercase hex, the form the harness compares.
fn hex(bytes: &[u8]) -> String {
    let mut rendered = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        rendered.push_str(&format!("{byte:02x}"));
    }
    rendered
}

/// Decodes one command payload from stdin.
///
/// Three shapes are accepted: the externally-tagged command itself
/// (`{"Request": …}` / `{"Publish": …}`), a wrapper (`{"request": …}` /
/// `{"publish": …}`) and the legacy bare request (`{"request_id": …, …}`), so every
/// harness input that worked before the Q2 publish command existed keeps working.
fn parse_shard_command(value: &serde_json::Value) -> Result<ShardCommand, String> {
    if let Some(request) = value.get("request") {
        let request: ShardRequest = serde_json::from_value(request.clone())
            .map_err(|error| format!("invalid shard request: {error}"))?;
        return Ok(ShardCommand::Request(request));
    }
    if let Some(publish) = value.get("publish") {
        let publish: ShardPublish = serde_json::from_value(publish.clone())
            .map_err(|error| format!("invalid shard publish: {error}"))?;
        return Ok(ShardCommand::Publish(publish));
    }
    if let Ok(command) = serde_json::from_value::<ShardCommand>(value.clone()) {
        return Ok(command);
    }
    let request: ShardRequest = serde_json::from_value(value.clone())
        .map_err(|error| format!("invalid shard command: {error}"))?;
    Ok(ShardCommand::Request(request))
}

/// The applied-state projection printed by the `applied` event.
fn applied_event(id: ShardNodeId, state: &rovqet_consensus::sm::ShardState) -> serde_json::Value {
    let partitions: Vec<serde_json::Value> = state
        .partitions
        .iter()
        .map(|(partition, partition_state)| {
            serde_json::json!({
                "partition": partition.to_string(),
                "count": partition_state.applied_count,
                "request_id": partition_state.last_request_id.to_string(),
                "chain": hex(&partition_state.chain_digest),
            })
        })
        .collect();
    serde_json::json!({
        "event": "applied",
        "node": id,
        "applied_index": state.last_applied.map(|log_id| log_id.index).unwrap_or(0),
        "revision": state.revision,
        "partitions": partitions,
    })
}

fn emit(value: serde_json::Value) {
    println!("{value}");
}

#[tokio::main]
async fn main() {
    let raw: Vec<String> = std::env::args().skip(1).collect();
    if raw.iter().any(|argument| argument == "--version") {
        println!(
            "q2-raft-node {} (openraft 0.9.26)",
            env!("CARGO_PKG_VERSION")
        );
        return;
    }
    let arguments = match parse_arguments(&raw) {
        Ok(arguments) => arguments,
        Err(error) => {
            emit(serde_json::json!({ "event": "error", "error": error }));
            eprintln!("{error}");
            std::process::exit(2);
        }
    };
    match run(arguments).await {
        Ok(()) => {}
        Err(error) => {
            emit(serde_json::json!({ "event": "error", "error": error }));
            std::process::exit(1);
        }
    }
}

async fn run(arguments: Arguments) -> Result<(), String> {
    let id = arguments.id;
    let node = ShardNode::start(ShardNodeConfig {
        id,
        cluster_id: arguments.cluster_id,
        shard_id: arguments.shard_id,
        listen: arguments.listen,
        peers: arguments.peers,
        data_directory: arguments.data_directory,
        bootstrap: arguments.bootstrap,
        election_timeout_ms: arguments.election_timeout_ms,
        heartbeat_interval_ms: arguments.heartbeat_interval_ms,
        max_payload_entries: 64,
        snapshot_logs_since_last: 8,
    })
    .await
    .map_err(|error| format!("{error}"))?;

    let mut last_leader: Option<(ShardNodeId, u64)> = None;
    let mut last_revision: Option<u64> = None;
    let mut last_snapshot: Option<(u64, u64)> = None;
    let mut commands = BufReader::new(tokio::io::stdin()).lines();
    let mut ticker = tokio::time::interval(Duration::from_millis(100));

    loop {
        tokio::select! {
            line = commands.next_line() => {
                let Some(line) = line.map_err(|error| format!("{error}"))? else { break };
                let line = line.trim();
                if line.is_empty() {
                    continue;
                }
                let value: serde_json::Value = match serde_json::from_str(line) {
                    Ok(value) => value,
                    Err(error) => {
                        emit(serde_json::json!({
                            "event": "proposed",
                            "node": id,
                            "ok": false,
                            "error": format!("invalid command: {error}"),
                        }));
                        continue;
                    }
                };
                let proposed = value
                    .get("propose")
                    .or_else(|| value.get("request"))
                    .or_else(|| value.get("publish"));
                if let Some(command) = proposed {
                    let command: ShardCommand = match parse_shard_command(command) {
                        Ok(command) => command,
                        Err(error) => {
                            emit(serde_json::json!({
                                "event": "proposed",
                                "node": id,
                                "ok": false,
                                "error": error,
                            }));
                            continue;
                        }
                    };
                    // A node that does not know the leader yet retries within a bound;
                    // the state machine applies a repeated request id idempotently.
                    let deadline = Instant::now() + Duration::from_secs(10);
                    loop {
                        match node.propose(command.clone()).await {
                            Ok(response) => {
                                emit(serde_json::json!({
                                    "event": "proposed",
                                    "node": id,
                                    "ok": true,
                                    "response": response,
                                }));
                                break;
                            }
                            Err(error) => {
                                if Instant::now() + Duration::from_millis(200) >= deadline {
                                    emit(serde_json::json!({
                                        "event": "proposed",
                                        "node": id,
                                        "ok": false,
                                        "error": format!("{error}"),
                                    }));
                                    break;
                                }
                                tokio::time::sleep(Duration::from_millis(100)).await;
                            }
                        }
                    }
                } else if let Some(authority) = value.get("authority") {
                    let authority: ShardAuthority =
                        match serde_json::from_value(authority.clone()) {
                            Ok(authority) => authority,
                            Err(error) => {
                                emit(serde_json::json!({
                                    "event": "error",
                                    "error": format!("invalid authority: {error}"),
                                }));
                                continue;
                            }
                        };
                    node.install_authority(Some(authority));
                    emit(serde_json::json!({
                        "event": "authority",
                        "node": id,
                        "owner": authority.owner,
                        "epoch": authority.epoch,
                    }));
                } else if value.get("snapshot").and_then(|value| value.as_bool()) == Some(true) {
                    if let Err(error) = node.trigger_snapshot().await {
                        emit(serde_json::json!({
                            "event": "error",
                            "error": format!("{error}"),
                        }));
                    }
                } else if value.get("shutdown").and_then(|value| value.as_bool()) == Some(true) {
                    break;
                } else {
                    emit(serde_json::json!({
                        "event": "proposed",
                        "node": id,
                        "ok": false,
                        "error": "unsupported stdin command",
                    }));
                }
            }
            _ = ticker.tick() => {
                if let Some((leader_id, term)) = node.leader_with_term().await {
                    if last_leader != Some((leader_id, term)) {
                        last_leader = Some((leader_id, term));
                        // Only the node that won the election prints `leader`, so the
                        // evidence names the leader unambiguously.
                        let event = if leader_id == id { "leader" } else { "follower" };
                        emit(serde_json::json!({
                            "event": event,
                            "node": id,
                            "leader": leader_id,
                            "term": term,
                        }));
                    }
                }
                let state = node.applied_state();
                if last_revision != Some(state.revision) {
                    last_revision = Some(state.revision);
                    emit(applied_event(id, &state));
                }
                if let Some(snapshot) = node.last_snapshot().await {
                    if last_snapshot != Some(snapshot) {
                        last_snapshot = Some(snapshot);
                        emit(serde_json::json!({
                            "event": "snapshot",
                            "node": id,
                            "included_index": snapshot.0,
                            "included_term": snapshot.1,
                        }));
                    }
                }
            }
        }
    }

    node.shutdown().await.map_err(|error| format!("{error}"))?;
    Ok(())
}
