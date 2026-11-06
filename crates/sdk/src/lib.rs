//! Public Rust client contracts before TCP and wire implementation (A-01/A-06).
//!
//! This bootstrap provides only typed configuration and outcomes. There is no
//! working network client yet, and no function pretends to connect or publish.

#![forbid(unsafe_code)]

use std::net::SocketAddr;

pub use rovqet_core::{DurableOffset, Priority, PublishOutcome, QoS};

/// Local research profile; production defaults and TLS are implemented later.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClientConfig {
    pub endpoint: SocketAddr,
}

impl Default for ClientConfig {
    fn default() -> Self {
        Self {
            endpoint: SocketAddr::from(([127, 0, 0, 1], 7443)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prototype_endpoint_is_loopback_only() {
        assert!(ClientConfig::default().endpoint.ip().is_loopback());
    }
}
