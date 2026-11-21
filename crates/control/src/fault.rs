//! A deterministic directional transport fault for isolated test processes only.
//!
//! The fault blackholes *outgoing* connections to a listed set of peer nodes. It is a
//! network-level partition in one direction only: the isolated process keeps its own
//! listener reachable and keeps answering its own clients, while every new connection it
//! would open towards the listed peers fails as if the peer were unreachable.
//!
//! The blocked set is read from a marker file on every call, so the parent can isolate a
//! process and restore the connectivity without restarting it. A normal build has no fault
//! code at all: the feature gate compiles the module away and the check is a constant
//! `false`.
use std::collections::BTreeSet;

/// The environment variable that names the marker file of the blocked peers.
pub const BLOCK_FILE_ENV: &str = "ROVQET_FAULT_BLOCK_FILE";

/// The peers whose incoming direction is blackholed by this process.
///
/// The marker file holds a comma separated list of node identifiers. An empty or missing
/// file blocks nothing.
pub fn blocked_peers() -> BTreeSet<u64> {
    let mut peers = BTreeSet::new();
    let Ok(path) = std::env::var(BLOCK_FILE_ENV) else {
        return peers;
    };
    let Ok(text) = std::fs::read_to_string(path) else {
        return peers;
    };
    for entry in text.split(',') {
        let entry = entry.trim();
        if entry.is_empty() {
            continue;
        }
        if let Ok(node) = entry.parse::<u64>() {
            peers.insert(node);
        }
    }
    peers
}

/// True when a connection towards this peer must not be opened.
///
/// Without the fault-injection feature nothing is ever blocked.
#[cfg(feature = "fault-injection")]
pub fn is_blocked(node: u64) -> bool {
    blocked_peers().contains(&node)
}

/// True when a connection towards this peer must not be opened.
///
/// Without the fault-injection feature nothing is ever blocked.
#[cfg(not(feature = "fault-injection"))]
pub fn is_blocked(_node: u64) -> bool {
    false
}

/// The error a blackholed connection reports.
///
/// The request is never delivered, so the outcome of the remote work is unknown rather
/// than a definite failure.
pub fn blocked_error(node: u64) -> std::io::Error {
    std::io::Error::new(
        std::io::ErrorKind::ConnectionRefused,
        format!("directional fault: the path towards peer {node} is blocked"),
    )
}
