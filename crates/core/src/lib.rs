//! Shared broker-level contracts without transport or storage dependencies.
//! No durability is implemented in this bootstrap package.

#![forbid(unsafe_code)]

use std::fmt;

/// Q0 admission is volatile; Q1 and Q2 require distinct durability barriers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum QoS {
    Q0 = 0,
    Q1 = 1,
    Q2 = 2,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UnsupportedQoS(pub u8);

impl fmt::Display for UnsupportedQoS {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "unsupported QoS value {}", self.0)
    }
}

impl std::error::Error for UnsupportedQoS {}

impl TryFrom<u8> for QoS {
    type Error = UnsupportedQoS;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::Q0),
            1 => Ok(Self::Q1),
            2 => Ok(Self::Q2),
            _ => Err(UnsupportedQoS(value)),
        }
    }
}

/// Priority is assigned by server policy, never trusted from a client field.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum Priority {
    Control = 0,
    Interactive = 1,
    Telemetry = 2,
    Bulk = 3,
}

/// An offset has no durable identity without its log incarnation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct DurableOffset {
    pub log_incarnation: u128,
    pub sequence: u64,
}

/// Admission is not durability; local and quorum commits must be distinguished.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PublishOutcome {
    Admitted,
    CommittedLocal(DurableOffset),
    CommittedQuorum(DurableOffset),
}

/// Identifier scopes are intentionally explicit to avoid accidental mixing.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PartitionRef {
    pub tenant_id: u64,
    pub topic_id: u64,
    pub partition_id: u32,
}

pub use rovqet_protocol::WIRE_MAJOR_VERSION;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn qos_values_match_reference_registry() {
        assert_eq!(QoS::try_from(0), Ok(QoS::Q0));
        assert_eq!(QoS::try_from(1), Ok(QoS::Q1));
        assert_eq!(QoS::try_from(2), Ok(QoS::Q2));
        assert_eq!(QoS::try_from(3), Err(UnsupportedQoS(3)));
        assert_eq!(QoS::Q2 as u8, 2);
    }

    #[test]
    fn distinct_commit_outcomes_are_not_interchangeable() {
        let offset = DurableOffset {
            log_incarnation: 42,
            sequence: 7,
        };
        assert_ne!(
            PublishOutcome::Admitted,
            PublishOutcome::CommittedLocal(offset)
        );
        assert_ne!(
            PublishOutcome::CommittedLocal(offset),
            PublishOutcome::CommittedQuorum(offset)
        );
    }
}
