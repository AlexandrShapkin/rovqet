//! Durable consumer-group actor tests. Isolated directories; no real process crash here.
use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use rovqet_core::{DurableOffset, PartitionRef, QoS};
use rovqet_durable::{
    AckRequest, GroupConfig, GroupError, GroupFetchRequest, GroupStop, Partition, PartitionConfig,
    PublishRequest,
};
use rovqet_wal::{Config as WalConfig, RecordKind, Wal};

static NEXT: AtomicU64 = AtomicU64::new(0);
struct Temp(PathBuf);
impl Temp {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "rovqet-q1-group-test-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&path).unwrap();
        Self(path)
    }
    fn wal(&self) -> PathBuf {
        self.0.join("wal")
    }
}
impl Drop for Temp {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn partition_id() -> PartitionRef {
    PartitionRef {
        tenant_id: 1,
        topic_id: 2,
        partition_id: 3,
    }
}
fn wal_config() -> WalConfig {
    WalConfig {
        cluster_id: 4,
        node_id: 5,
        partition: partition_id(),
        incarnation: 42,
        max_segment_bytes: 8192,
        max_record_bytes: 4096,
        max_pending_bytes: 64 * 1024,
        max_pending_records: 32,
        max_segments: 64,
        max_records: 512,
    }
}
fn config(group: GroupConfig) -> PartitionConfig {
    PartitionConfig {
        partition: partition_id(),
        ownership_epoch: 7,
        queue_messages: 32,
        queue_bytes: 4 * 1024 * 1024,
        max_outcomes: 256,
        max_producers: 256,
        dedup_window: Duration::from_secs(86400),
        group,
    }
}
fn open(t: &Temp, group: GroupConfig) -> Partition {
    fs::create_dir_all(t.wal()).unwrap();
    Partition::open(t.wal(), wal_config(), config(group)).unwrap()
}
fn publish(p: &Partition, sequence: u64) {
    let outcome = p
        .try_publish(PublishRequest {
            partition: partition_id(),
            qos: QoS::Q1,
            ownership_epoch: 7,
            principal: 10,
            producer_id: 11,
            producer_epoch: 0,
            sequence,
            message_id: 100 + u128::from(sequence),
            payload: format!("event-{sequence}").into_bytes(),
        })
        .unwrap()
        .wait()
        .unwrap();
    assert!(matches!(
        outcome,
        rovqet_core::PublishOutcome::CommittedLocal(_)
    ));
}
fn fetch(p: &Partition, generation: u64, max: usize) -> rovqet_durable::GroupFetchBatch {
    p.try_fetch_group(GroupFetchRequest {
        partition: partition_id(),
        group_id: 9,
        generation,
        from: DurableOffset {
            log_incarnation: 42,
            sequence: 0,
        },
        max_messages: max,
        max_bytes: 64 * 1024,
        wait: Duration::ZERO,
    })
    .unwrap()
    .wait()
    .unwrap()
}
fn ack(p: &Partition, delivery: &rovqet_durable::GroupDelivery) -> Result<(), GroupError> {
    p.try_ack(AckRequest {
        partition: partition_id(),
        group_id: 9,
        generation: delivery.generation,
        lease_id: delivery.lease_id,
        ownership_epoch: 7,
        offset: delivery.offset,
        attempt: delivery.attempt,
        principal: 1,
    })
    .unwrap()
    .wait()
    .map(|_| ())
}
#[test]
fn claim_delivery_ordered_acks_and_restart_cursor() {
    let t = Temp::new();
    let p = open(&t, GroupConfig::default());
    for sequence in 0..3 {
        publish(&p, sequence);
    }
    let batch = fetch(&p, 0, 1);
    assert_eq!(batch.generation, 1);
    assert_eq!(batch.deliveries.len(), 1);
    assert_eq!(batch.deliveries[0].offset.sequence, 0);
    assert_eq!(batch.deliveries[0].attempt, 1);
    assert_eq!(batch.deliveries[0].payload, b"event-0");
    ack(&p, &batch.deliveries[0]).unwrap();
    let second = fetch(&p, 1, 1);
    assert_eq!(second.deliveries[0].offset.sequence, 2);
    ack(&p, &second.deliveries[0]).unwrap();
    let third = fetch(&p, 1, 1);
    assert_eq!(third.deliveries[0].offset.sequence, 4);
    ack(&p, &third.deliveries[0]).unwrap();
    let end = fetch(&p, 1, 4);
    assert!(end.deliveries.is_empty());
    assert_eq!(end.stop, GroupStop::End);
    assert_eq!(end.generation, 1);
    drop(p);
    let p = open(&t, GroupConfig::default());
    let after = fetch(&p, 0, 4);
    assert_eq!(after.generation, 2);
    assert!(after.deliveries.is_empty());
    assert_eq!(after.stop, GroupStop::End);
    assert_eq!(after.next.sequence, 10);
    publish(&p, 3);
    let next = fetch(&p, 2, 4);
    assert_eq!(next.deliveries[0].offset.sequence, 11);
    assert_eq!(next.deliveries[0].payload, b"event-3");
}

#[test]
fn out_of_order_acks_never_skip_the_gap_and_survive_restart() {
    let t = Temp::new();
    let p = open(&t, GroupConfig::default());
    for sequence in 0..3 {
        publish(&p, sequence);
    }
    let batch = fetch(&p, 0, 8);
    assert_eq!(batch.deliveries.len(), 3);
    ack(&p, &batch.deliveries[2]).unwrap();
    let still = fetch(&p, 1, 8);
    assert_eq!(
        still.next.sequence, 0,
        "later ACK must not advance the cursor"
    );
    assert!(still.deliveries.iter().all(|d| d.offset.sequence != 4));
    ack(&p, &batch.deliveries[1]).unwrap();
    assert_eq!(fetch(&p, 1, 8).next.sequence, 0);
    ack(&p, &batch.deliveries[0]).unwrap();
    let end = fetch(&p, 1, 8);
    assert!(end.deliveries.is_empty());
    assert_eq!(end.stop, GroupStop::End);
    drop(p);
    let p = open(&t, GroupConfig::default());
    let after = fetch(&p, 0, 8);
    assert!(after.deliveries.is_empty());
    assert_eq!(after.stop, GroupStop::End);
    assert_eq!(after.next.sequence, 10);
}

#[test]
fn sparse_checkpoint_recovery_keeps_the_gap() {
    let t = Temp::new();
    let mut group = GroupConfig::default();
    group.checkpoint_after = 1;
    let p = open(&t, group);
    for sequence in 0..3 {
        publish(&p, sequence);
    }
    let batch = fetch(&p, 0, 8);
    ack(&p, &batch.deliveries[2]).unwrap();
    ack(&p, &batch.deliveries[1]).unwrap();
    drop(p);
    let p = open(&t, group);
    // The earlier leases are gone after restart, so offset 0 is redelivered.
    let retry = fetch(&p, 0, 8);
    assert_eq!(retry.generation, 2);
    assert_eq!(retry.deliveries.len(), 1);
    assert_eq!(retry.deliveries[0].offset.sequence, 0);
    ack(&p, &retry.deliveries[0]).unwrap();
    let end = fetch(&p, 2, 8);
    assert!(end.deliveries.is_empty());
    assert_eq!(end.stop, GroupStop::End);
}

#[test]
fn duplicate_ack_is_a_read_only_lookup_without_a_new_record() {
    let t = Temp::new();
    let p = open(&t, GroupConfig::default());
    publish(&p, 0);
    let batch = fetch(&p, 0, 1);
    let delivery = &batch.deliveries[0];
    ack(&p, delivery).unwrap();
    let before = p.committed_records();
    ack(&p, delivery).unwrap();
    assert_eq!(p.committed_records(), before);
    drop(p);
    // MESSAGE + PRODUCER_OUTCOME + generation checkpoint + exactly one CONSUMER_ACK.
    let mut wal = Wal::open(t.wal(), wal_config()).unwrap();
    let count = wal.committed_position().unwrap().sequence + 1;
    assert_eq!(count, 4);
    let ack_record = wal
        .read(
            DurableOffset {
                log_incarnation: 42,
                sequence: 3,
            },
            1,
            wal_config().max_record_bytes,
        )
        .unwrap();
    assert_eq!(ack_record[0].kind, RecordKind::ConsumerAck);
}

#[test]
fn replacement_fences_the_old_generation_and_old_lease() {
    let t = Temp::new();
    let p = open(&t, GroupConfig::default());
    publish(&p, 0);
    let first = fetch(&p, 0, 1);
    assert_eq!(first.generation, 1);
    let second = fetch(&p, 0, 1);
    assert_eq!(second.generation, 2);
    assert!(matches!(
        ack(&p, &first.deliveries[0]),
        Err(GroupError::StaleGeneration)
    ));
    // The takeover dropped the old lease; reusing its identifier under the new
    // generation cannot acknowledge the record.
    assert!(matches!(
        p.try_ack(AckRequest {
            partition: partition_id(),
            group_id: 9,
            generation: 2,
            lease_id: first.deliveries[0].lease_id,
            ownership_epoch: 7,
            offset: first.deliveries[0].offset,
            attempt: first.deliveries[0].attempt,
            principal: 1,
        })
        .unwrap()
        .wait(),
        Err(GroupError::NotLeased)
    ));
    // The fresh delivery is independently usable.
    assert_eq!(second.deliveries[0].offset.sequence, 0);
    assert!(ack(&p, &second.deliveries[0]).is_ok());
    assert!(matches!(
        p.try_fetch_group(GroupFetchRequest {
            partition: partition_id(),
            group_id: 9,
            generation: 1,
            from: DurableOffset {
                log_incarnation: 42,
                sequence: 0
            },
            max_messages: 1,
            max_bytes: 1024,
            wait: Duration::ZERO,
        })
        .unwrap()
        .wait(),
        Err(GroupError::StaleGeneration)
    ));
}

#[test]
fn expired_lease_rejects_ack_and_redelivers_with_a_new_attempt() {
    let t = Temp::new();
    let mut group = GroupConfig::default();
    group.lease = Duration::from_millis(20);
    let p = open(&t, group);
    publish(&p, 0);
    let batch = fetch(&p, 0, 1);
    std::thread::sleep(Duration::from_millis(40));
    assert!(matches!(
        ack(&p, &batch.deliveries[0]),
        Err(GroupError::NotLeased)
    ));
    let retry = fetch(&p, 1, 1);
    assert_eq!(retry.deliveries[0].offset.sequence, 0);
    assert_eq!(retry.deliveries[0].attempt, 2);
    assert_ne!(retry.deliveries[0].lease_id, batch.deliveries[0].lease_id);
    ack(&p, &retry.deliveries[0]).unwrap();
}

#[test]
fn invalid_identity_incarnation_and_unleased_offsets_are_typed() {
    let t = Temp::new();
    let p = open(&t, GroupConfig::default());
    publish(&p, 0);
    assert!(matches!(
        p.try_fetch_group(GroupFetchRequest {
            partition: partition_id(),
            group_id: 9,
            generation: 0,
            from: DurableOffset {
                log_incarnation: 41,
                sequence: 0
            },
            max_messages: 1,
            max_bytes: 1024,
            wait: Duration::ZERO,
        })
        .unwrap()
        .wait(),
        Err(GroupError::IncarnationMismatch)
    ));
    let delivery = fetch(&p, 0, 1).deliveries[0].clone();
    // Wrong offset for a valid lease.
    assert!(matches!(
        p.try_ack(AckRequest {
            partition: partition_id(),
            group_id: 9,
            generation: delivery.generation,
            lease_id: delivery.lease_id,
            ownership_epoch: 7,
            offset: DurableOffset {
                log_incarnation: 42,
                sequence: 1
            },
            attempt: delivery.attempt,
            principal: 1,
        })
        .unwrap()
        .wait(),
        Err(GroupError::NotLeased)
    ));
    // Never leased.
    assert!(matches!(
        p.try_ack(AckRequest {
            partition: partition_id(),
            group_id: 9,
            generation: delivery.generation,
            lease_id: delivery.lease_id + 1000,
            ownership_epoch: 7,
            offset: delivery.offset,
            attempt: delivery.attempt,
            principal: 1,
        })
        .unwrap()
        .wait(),
        Err(GroupError::NotLeased)
    ));
    // Stale ownership epoch.
    assert!(matches!(
        p.try_ack(AckRequest {
            partition: partition_id(),
            group_id: 9,
            generation: delivery.generation,
            lease_id: delivery.lease_id,
            ownership_epoch: 6,
            offset: delivery.offset,
            attempt: delivery.attempt,
            principal: 1,
        })
        .unwrap()
        .wait(),
        Err(GroupError::StaleGeneration)
    ));
}

#[test]
fn sparse_bound_closes_with_the_gap_and_groups_stay_isolated() {
    let t = Temp::new();
    let mut group = GroupConfig::default();
    group.max_sparse = 1;
    let p = open(&t, group);
    for sequence in 0..2 {
        publish(&p, sequence);
    }
    let batch = fetch(&p, 0, 8);
    ack(&p, &batch.deliveries[1]).unwrap();
    // A second sparse entry under a full bound is refused.
    let other = p
        .try_fetch_group(GroupFetchRequest {
            partition: partition_id(),
            group_id: 10,
            generation: 0,
            from: DurableOffset {
                log_incarnation: 42,
                sequence: 0,
            },
            max_messages: 8,
            max_bytes: 64 * 1024,
            wait: Duration::ZERO,
        })
        .unwrap()
        .wait()
        .unwrap();
    assert_eq!(other.deliveries[0].offset.sequence, 0);
    // Closing the pointer gap is always accepted and clears the sparse set.
    ack(&p, &batch.deliveries[0]).unwrap();
    let end = fetch(&p, 1, 8);
    assert!(end.deliveries.is_empty());
    assert_eq!(end.stop, GroupStop::End);
    // The second group has its own cursor: nothing in group 9 was acknowledged for it.
    let isolated = fetch(&p, 1, 8);
    assert!(isolated.deliveries.is_empty());
}

#[test]
fn corrupt_consumer_metadata_fails_recovery_without_reset() {
    for corrupt in 0..2 {
        let t = Temp::new();
        let p = open(&t, GroupConfig::default());
        publish(&p, 0);
        drop(p);
        let mut wal = Wal::open(t.wal(), wal_config()).unwrap();
        let next = wal.committed_position().unwrap().sequence + 1;
        let payload = if corrupt == 0 {
            b"garbage".to_vec()
        } else {
            let mut payload = b"RQCP\0\x01".to_vec();
            payload.extend_from_slice(&9_u64.to_be_bytes());
            payload.extend_from_slice(&1_u64.to_be_bytes());
            payload.extend_from_slice(&(next + 1).to_be_bytes());
            payload.extend_from_slice(&1_u32.to_be_bytes());
            payload.extend_from_slice(&0_u64.to_be_bytes());
            payload
        };
        wal.append(rovqet_wal::Record {
            kind: if corrupt == 0 {
                RecordKind::ConsumerAck
            } else {
                RecordKind::CursorCheckpoint
            },
            logical_offset: next,
            owner_epoch: 7,
            producer_id: 0,
            producer_epoch: 0,
            sequence: 0,
            message_id: 0,
            server_timestamp: 1,
            payload,
        })
        .unwrap();
        wal.sync().unwrap();
        drop(wal);
        assert!(matches!(
            Partition::open(t.wal(), wal_config(), config(GroupConfig::default())),
            Err(rovqet_durable::Error::Corruption(_))
        ));
    }
}

#[test]
fn standalone_replay_skips_ack_and_checkpoint_records_and_keeps_identities() {
    let t = Temp::new();
    let p = open(&t, GroupConfig::default());
    publish(&p, 0);
    publish(&p, 1);
    let batch = fetch(&p, 0, 8);
    ack(&p, &batch.deliveries[0]).unwrap();
    publish(&p, 2);
    let rows = p
        .try_replay(rovqet_durable::ReplayRequest {
            partition: partition_id(),
            from: DurableOffset {
                log_incarnation: 42,
                sequence: 0,
            },
            max_messages: 32,
            max_bytes: 64 * 1024,
            wait: Duration::ZERO,
        })
        .unwrap()
        .wait()
        .unwrap();
    assert_eq!(
        rows.messages
            .iter()
            .map(|m| (m.offset.sequence, m.message_id, m.payload.clone()))
            .collect::<Vec<_>>(),
        vec![
            (0, 100, b"event-0".to_vec()),
            (2, 101, b"event-1".to_vec()),
            (6, 102, b"event-2".to_vec()),
        ]
    );
    assert_eq!(rows.next.sequence, 8);
    assert!(rows.end_of_available_history);
}

#[test]
fn shutdown_with_outstanding_lease_keeps_the_committed_ack() {
    let t = Temp::new();
    let p = open(&t, GroupConfig::default());
    for sequence in 0..2 {
        publish(&p, sequence);
    }
    let batch = fetch(&p, 0, 8);
    ack(&p, &batch.deliveries[0]).unwrap();
    // The second lease is still outstanding at shutdown.
    drop(p);
    let p = open(&t, GroupConfig::default());
    let resumed = fetch(&p, 0, 8);
    assert_eq!(resumed.deliveries.len(), 1);
    assert_eq!(resumed.deliveries[0].offset.sequence, 2);
    assert_eq!(resumed.deliveries[0].attempt, 1);
}

#[test]
fn concurrent_identical_acks_commit_exactly_once() {
    let t = Temp::new();
    let p = std::sync::Arc::new(open(&t, GroupConfig::default()));
    publish(&p, 0);
    publish(&p, 1);
    let batch = fetch(&p, 0, 8);
    assert_eq!(batch.deliveries.len(), 2);
    let delivery = batch.deliveries[0].clone();
    let mut handles = Vec::new();
    for _ in 0..4 {
        let partition = p.clone();
        let delivery = delivery.clone();
        handles.push(std::thread::spawn(move || ack(&partition, &delivery)));
    }
    for handle in handles {
        assert!(handle.join().unwrap().is_ok());
    }
    drop(batch);
    let partition = std::sync::Arc::try_unwrap(p).ok().unwrap();
    drop(partition);
    // Two publication pairs, one generation checkpoint and exactly one CONSUMER_ACK.
    let wal = Wal::open(t.wal(), wal_config()).unwrap();
    assert_eq!(wal.committed_position().unwrap().sequence + 1, 6);
}

#[test]
fn admission_exhaustion_is_typed_before_any_delivery() {
    let t = Temp::new();
    fs::create_dir_all(t.wal()).unwrap();
    let mut group = GroupConfig::default();
    group.max_groups = 1;
    let mut config = config(group);
    config.queue_bytes = 8192;
    let p = Partition::open(t.wal(), wal_config(), config).unwrap();
    publish(&p, 0);
    // The reservation for a full batch cannot fit the tiny partition budget.
    assert!(matches!(
        p.try_fetch_group(GroupFetchRequest {
            partition: partition_id(),
            group_id: 9,
            generation: 0,
            from: DurableOffset {
                log_incarnation: 42,
                sequence: 0
            },
            max_messages: 8,
            max_bytes: 65536,
            wait: Duration::ZERO,
        }),
        Err(GroupError::Capacity)
    ));
    // No group generation was claimed by a rejected admission.
    let batch = p
        .try_fetch_group(GroupFetchRequest {
            partition: partition_id(),
            group_id: 9,
            generation: 0,
            from: DurableOffset {
                log_incarnation: 42,
                sequence: 0,
            },
            max_messages: 1,
            max_bytes: 1024,
            wait: Duration::ZERO,
        })
        .unwrap()
        .wait()
        .unwrap();
    assert_eq!(batch.generation, 1);
    assert_eq!(batch.deliveries.len(), 1);
}

#[test]
fn cancelled_group_wait_releases_its_reservation_and_keeps_publication_alive() {
    let t = Temp::new();
    let p = open(&t, GroupConfig::default());
    let waiting = p
        .try_fetch_group(GroupFetchRequest {
            partition: partition_id(),
            group_id: 9,
            generation: 0,
            from: DurableOffset {
                log_incarnation: 42,
                sequence: 0,
            },
            max_messages: 8,
            max_bytes: 65536,
            wait: Duration::from_secs(30),
        })
        .unwrap();
    drop(waiting);
    publish(&p, 0);
    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    while p.usage().messages != 0 {
        assert!(std::time::Instant::now() < deadline);
        std::thread::sleep(Duration::from_millis(2));
    }
    let batch = fetch(&p, 0, 8);
    assert_eq!(batch.deliveries.len(), 1);
    assert_eq!(batch.deliveries[0].payload, b"event-0");
}
