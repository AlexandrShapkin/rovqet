//! Ownership authority and fencing for durable Q1 mutations.
//!
//! A partition without authority runs in standalone mode: existing single-node behaviour is
//! unchanged. A partition with an installed authority is a *fenced* owner: every durable
//! mutation (publication, consumer ACK, NACK, terminal disposition) is only allowed while
//! the local node is the committed owner and the ownership lease is still valid.
//!
//! The authority is installed from committed control-plane metadata. A node that never
//! learns about a revocation keeps its lease until it expires, which is why a replacement
//! owner must wait for confirmed revocation or lease expiry before it activates.
use std::fmt;
use std::time::{Duration, Instant};

/// Authoritative ownership of one durable partition.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Authority {
    /// Node that currently owns the partition.
    pub owner: u64,
    /// Monotonic ownership epoch; a higher epoch fences every older authority.
    pub epoch: u64,
    /// Ownership lease validity counted from the moment the authority was installed.
    pub lease: Duration,
}

impl Authority {
    pub fn new(owner: u64, epoch: u64, lease: Duration) -> Option<Self> {
        (owner != 0 && epoch != 0 && !lease.is_zero()).then_some(Self {
            owner,
            epoch,
            lease,
        })
    }
    pub fn is_expired_at(&self, installed_at: Instant, now: Instant) -> bool {
        now.duration_since(installed_at) >= self.lease
    }
}

/// Why a durable mutation was refused by fencing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FenceError {
    /// This node is not the committed owner.
    NotOwner,
    /// The ownership epoch is older than the installed authority.
    StaleEpoch,
    /// The ownership lease expired; the node must fail closed.
    ExpiredLease,
}

impl fmt::Display for FenceError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for FenceError {}

/// Fencing state of one partition.
#[derive(Debug, Clone, Copy)]
pub struct Fence {
    authority: Option<Authority>,
    installed_at: Instant,
}

impl Fence {
    pub fn standalone() -> Self {
        Self {
            authority: None,
            installed_at: Instant::now(),
        }
    }
    pub fn install(&mut self, authority: Option<Authority>, now: Instant) {
        self.authority = authority;
        self.installed_at = now;
    }
    pub fn authority(&self) -> Option<Authority> {
        self.authority
    }
    pub fn epoch(&self) -> Option<u64> {
        self.authority.map(|authority| authority.epoch)
    }
    /// Validates a durable mutation of the local node.
    pub fn check(
        &self,
        node: u64,
        expected_epoch: Option<u64>,
        now: Instant,
    ) -> Result<(), FenceError> {
        let Some(authority) = self.authority else {
            // Standalone mode: no control-plane authority is installed.
            return Ok(());
        };
        if authority.owner != node {
            return Err(FenceError::NotOwner);
        }
        if let Some(expected) = expected_epoch {
            if expected != authority.epoch {
                return Err(FenceError::StaleEpoch);
            }
        }
        if authority.is_expired_at(self.installed_at, now) {
            return Err(FenceError::ExpiredLease);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn standalone_mode_allows_every_mutation() {
        let fence = Fence::standalone();
        assert!(fence.check(7, Some(3), Instant::now()).is_ok());
    }

    #[test]
    fn a_fenced_partition_refuses_foreign_stale_and_expired_authority() {
        let mut fence = Fence::standalone();
        let now = Instant::now();
        fence.install(Authority::new(2, 5, Duration::from_secs(60)), now);
        assert_eq!(fence.check(1, Some(5), now), Err(FenceError::NotOwner));
        assert_eq!(fence.check(2, Some(4), now), Err(FenceError::StaleEpoch));
        assert!(fence.check(2, Some(5), now).is_ok());
        assert_eq!(
            fence.check(2, Some(5), now + Duration::from_secs(61)),
            Err(FenceError::ExpiredLease)
        );
    }

    #[test]
    fn a_new_authority_fences_the_previous_one() {
        let mut fence = Fence::standalone();
        let now = Instant::now();
        fence.install(Authority::new(1, 5, Duration::from_secs(60)), now);
        assert!(fence.check(1, Some(5), now).is_ok());
        fence.install(Authority::new(2, 6, Duration::from_secs(60)), now);
        assert_eq!(fence.check(1, Some(5), now), Err(FenceError::NotOwner));
    }
}
