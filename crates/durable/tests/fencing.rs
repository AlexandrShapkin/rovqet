//! Ownership fencing at the durable commit boundary.
use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use rovqet_core::{PartitionRef, QoS};
use rovqet_durable::{
    AckRequest, Authority, Error, GroupFetchRequest, NackRequest, Partition, PartitionConfig,
    PublishRequest,
};

static NEXT: AtomicU64 = AtomicU64::new(0);
struct Temp(PathBuf);
impl Temp {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "rovqet-fencing-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&path).unwrap();
        Self(path)
    }
}
impl Drop for Temp {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn partition() -> PartitionRef {
    PartitionRef {
        tenant_id: 1,
        topic_id: 3,
        partition_id: 0,
    }
}
fn config(node_id: u64) -> PartitionConfig {
    PartitionConfig {
        partition: partition(),
        ownership_epoch: 7,
        node_id,
        queue_messages: 16,
        queue_bytes: 4 * 1024 * 1024,
        max_outcomes: 64,
        max_producers: 64,
        dedup_window: Duration::from_secs(86400),
        group: rovqet_durable::GroupConfig::default(),
    }
}
fn open(t: &Temp, node_id: u64) -> Partition {
    let dir = t.0.join(format!("wal{node_id}"));
    fs::create_dir_all(&dir).unwrap();
    Partition::open(
        &dir,
        rovqet_wal::Config {
            partition: partition(),
            incarnation: 42,
            ..Default::default()
        },
        config(node_id),
    )
    .unwrap()
}
fn publish(p: &Partition, sequence: u64) -> Result<(), Error> {
    p.try_publish(PublishRequest {
        partition: partition(),
        qos: QoS::Q1,
        ownership_epoch: 7,
        principal: 1,
        producer_id: 11,
        producer_epoch: 0,
        sequence,
        message_id: 100 + u128::from(sequence),
        payload: format!("event-{sequence}").into_bytes(),
    })
    .unwrap()
    .wait()
    .map(|_| ())
}

#[test]
fn standalone_partitions_are_not_fenced() {
    let t = Temp::new();
    let p = open(&t, 0);
    assert!(publish(&p, 0).is_ok());
}

#[test]
fn a_foreign_owner_is_fenced_at_the_commit_boundary() {
    let t = Temp::new();
    let p = open(&t, 5);
    p.set_authority(Authority::new(5, 3, Duration::from_secs(60)))
        .unwrap();
    assert!(publish(&p, 0).is_ok());
    let fetch = p
        .try_fetch_group(GroupFetchRequest {
            partition: partition(),
            group_id: 9,
            generation: 0,
            from: rovqet_core::DurableOffset {
                log_incarnation: 42,
                sequence: 0,
            },
            max_messages: 4,
            max_bytes: 65536,
            wait: Duration::ZERO,
        })
        .unwrap()
        .wait()
        .unwrap();
    let token = fetch.deliveries[0].clone();
    // Ownership moves away: the old owner may not commit or acknowledge any more.
    p.set_authority(Authority::new(9, 4, Duration::from_secs(60)))
        .unwrap();
    assert!(matches!(publish(&p, 1), Err(Error::Fenced)));
    let ack = p
        .try_ack(AckRequest {
            partition: partition(),
            group_id: 9,
            generation: token.generation,
            lease_id: token.lease_id,
            ownership_epoch: 7,
            offset: token.offset,
            attempt: token.attempt,
            principal: 1,
        })
        .unwrap()
        .wait();
    assert!(ack.is_err(), "a fenced partition must not acknowledge");
    let nack = p
        .try_nack(NackRequest {
            partition: partition(),
            group_id: 9,
            generation: token.generation,
            lease_id: token.lease_id,
            ownership_epoch: 7,
            offset: token.offset,
            attempt: token.attempt,
            principal: 1,
            reason: 1,
        })
        .unwrap()
        .wait();
    assert!(
        nack.is_err(),
        "a fenced partition must not schedule a retry"
    );
}

#[test]
fn an_expired_lease_fails_closed() {
    let t = Temp::new();
    let p = open(&t, 5);
    p.set_authority(Authority::new(5, 3, Duration::from_millis(1)))
        .unwrap();
    assert!(publish(&p, 0).is_ok());
    std::thread::sleep(Duration::from_millis(10));
    assert!(matches!(publish(&p, 1), Err(Error::FenceExpired)));
}

#[test]
fn a_newer_authority_fences_the_previous_owner() {
    let t = Temp::new();
    let p = open(&t, 5);
    p.set_authority(Authority::new(5, 3, Duration::from_secs(60)))
        .unwrap();
    assert!(publish(&p, 0).is_ok());
    // Ownership moves to another node: the old owner may not commit any more.
    p.set_authority(Authority::new(6, 4, Duration::from_secs(60)))
        .unwrap();
    assert!(matches!(publish(&p, 1), Err(Error::Fenced)));
    // An older epoch can never replace a newer authority.
    p.set_authority(Authority::new(5, 3, Duration::from_secs(60)))
        .unwrap();
    assert!(matches!(publish(&p, 2), Err(Error::Fenced)));
}
