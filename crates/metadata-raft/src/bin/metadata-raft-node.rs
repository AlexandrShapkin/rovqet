//! One metadata Raft node as an observable process.
//!
//! The process prints one JSON event per line and accepts metadata commands on stdin, so an
//! acceptance test can observe elections, terms and committed revisions without reaching into
//! the library's internals.
use rovqet_metadata_raft::types::{MetaNodeId, MetadataCommand};
use rovqet_metadata_raft::{MetadataNode, MetadataNodeConfig};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, BufReader};

fn usage() -> String {
    "usage: metadata-raft-node --id N --cluster-id N --listen HOST:PORT --data-dir PATH \
     [--peer N=HOST:PORT]... [--bootstrap true|false]"
        .to_string()
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let arguments: Vec<String> = std::env::args().skip(1).collect();
    let mut id: Option<MetaNodeId> = None;
    let mut cluster_id: Option<u128> = None;
    let mut listen: Option<SocketAddr> = None;
    let mut data_directory: Option<PathBuf> = None;
    let mut peers: Vec<(MetaNodeId, SocketAddr)> = Vec::new();
    let mut bootstrap = false;

    let mut index = 0;
    while index < arguments.len() {
        let flag = arguments[index].as_str();
        let value = arguments
            .get(index + 1)
            .ok_or_else(|| format!("{flag} needs a value\n{}", usage()))?;
        match flag {
            "--id" => id = Some(value.parse()?),
            "--cluster-id" => cluster_id = Some(value.parse()?),
            "--listen" => listen = Some(value.parse()?),
            "--data-dir" => data_directory = Some(PathBuf::from(value)),
            "--peer" => {
                let (peer, address) = value
                    .split_once('=')
                    .ok_or_else(|| format!("--peer needs N=HOST:PORT, got {value}"))?;
                peers.push((peer.parse()?, address.parse()?));
            }
            "--bootstrap" => bootstrap = value == "true",
            other => return Err(format!("unknown argument {other}\n{}", usage()).into()),
        }
        index += 2;
    }

    let id = id.ok_or_else(|| usage())?;
    let cluster_id = cluster_id.ok_or_else(|| usage())?;
    let listen = listen.ok_or_else(|| usage())?;
    let data_directory = data_directory.ok_or_else(|| usage())?;

    let node = MetadataNode::start(MetadataNodeConfig {
        id,
        cluster_id,
        listen,
        peers,
        data_directory,
        bootstrap,
        election_timeout_ms: 300,
        heartbeat_interval_ms: 80,
        max_payload_entries: 64,
        snapshot_logs_since_last: 4,
    })
    .await?;

    let mut last_leader: Option<(MetaNodeId, u64)> = None;
    let mut last_revision: Option<u64> = None;
    let mut commands = BufReader::new(tokio::io::stdin()).lines();
    let mut ticker = tokio::time::interval(Duration::from_millis(100));

    loop {
        tokio::select! {
            line = commands.next_line() => {
                let Some(line) = line? else { break };
                let line = line.trim();
                if line.is_empty() {
                    continue;
                }
                let command: MetadataCommand = match serde_json::from_str(line) {
                    Ok(command) => command,
                    Err(error) => {
                        println!(
                            "{}",
                            serde_json::json!({
                                "event": "proposed",
                                "node": id,
                                "ok": false,
                                "error": format!("invalid command: {error}"),
                            })
                        );
                        continue;
                    }
                };
                // A node that is not the leader retries until it becomes one.
                let deadline = std::time::Instant::now() + Duration::from_secs(30);
                let mut outcome = None;
                while std::time::Instant::now() < deadline {
                    match node.propose(command.clone()).await {
                        Ok(response) => {
                            outcome = Some(serde_json::json!({
                                "event": "proposed",
                                "node": id,
                                "ok": true,
                                "response": response,
                            }));
                            break;
                        }
                        Err(error) => {
                            if std::time::Instant::now() + Duration::from_millis(200) >= deadline {
                                outcome = Some(serde_json::json!({
                                    "event": "proposed",
                                    "node": id,
                                    "ok": false,
                                    "error": format!("{error}"),
                                }));
                                break;
                            }
                            tokio::time::sleep(Duration::from_millis(200)).await;
                        }
                    }
                }
                if let Some(outcome) = outcome {
                    println!("{outcome}");
                }
            }
            _ = ticker.tick() => {
                if let Some((leader_id, term)) = node.leader_with_term().await {
                    if last_leader != Some((leader_id, term)) {
                        last_leader = Some((leader_id, term));
                        // A "leader" event is only emitted by the node that won the election,
                        // so the evidence identifies the leader unambiguously.
                        let event = if leader_id == id { "leader" } else { "follower" };
                        println!(
                            "{}",
                            serde_json::json!({
                                "event": event,
                                "node": id,
                                "leader": leader_id,
                                "term": term,
                            })
                        );
                    }
                }
                let state = node.applied_state();
                if last_revision != Some(state.revision) {
                    last_revision = Some(state.revision);
                    println!(
                        "{}",
                        serde_json::json!({
                            "event": "applied",
                            "node": id,
                            "revision": state.revision,
                            "nodes": state.nodes,
                            "placements": state.placements,
                        })
                    );
                }
            }
        }
    }

    node.shutdown().await?;
    Ok(())
}
