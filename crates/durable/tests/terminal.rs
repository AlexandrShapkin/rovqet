//! B-06 retry, quarantine and real Q1 DLQ tests. Disposable storage, controlled clock.
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use rovqet_core::{DurableOffset, PartitionRef, PublishOutcome, QoS};
use rovqet_durable::{
    NackOutcome, NackRequest, Partition, PartitionConfig, PublishRequest, TerminalPolicy,
};
use rovqet_wal::Config as WalConfig;

static NEXT: AtomicU64 = AtomicU64::new(0);
struct Temp(PathBuf);
impl Temp {
    fn new(label: &str) -> Self {
        let path = std::env::temp_dir().join(format!(
            "rovqet-b06-{label}-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&path).unwrap();
        Self(path)
    }
    fn dir(&self, name: &str) -> PathBuf {
        self.0.join(name)
    }
}
impl Drop for Temp {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

#[derive(Clone, Default)]
struct ManualClock(Arc<AtomicU64>);
impl ManualClock {
    fn advance(&self, ms: u64) {
        self.0.fetch_add(ms, Ordering::SeqCst);
    }
}
impl rovqet_durable::Clock for ManualClock {
    fn now_millis(&self) -> Option<u64> {
        Some(self.0.load(Ordering::SeqCst))
    }
}

fn partition(tenant: u64, topic: u64, id: u32) -> PartitionRef {
    PartitionRef {
        tenant_id: tenant,
        topic_id: topic,
        partition_id: id,
    }
}
fn wal_config(partition: PartitionRef, incarnation: u128) -> WalConfig {
    WalConfig {
        partition,
        incarnation,
        max_segment_bytes: 8192,
        max_record_bytes: 4096,
        ..WalConfig::default()
    }
}
fn config(partition: PartitionRef, group: rovqet_durable::GroupConfig) -> PartitionConfig {
    PartitionConfig {
        partition,
        ownership_epoch: 7,
        node_id: 0,
        queue_messages: 64,
        queue_bytes: 4 * 1024 * 1024,
        max_outcomes: 512,
        max_producers: 512,
        dedup_window: Duration::from_secs(86400),
        group,
    }
}
fn group_config(policy: TerminalPolicy, attempts: u32) -> rovqet_durable::GroupConfig {
    rovqet_durable::GroupConfig {
        max_attempts: attempts,
        retry_initial: Duration::from_millis(100),
        retry_max: Duration::from_millis(250),
        terminal_policy: policy,
        ..rovqet_durable::GroupConfig::default()
    }
}
fn publish(p: &Partition, sequence: u64, payload: &[u8]) {
    let outcome = p
        .try_publish(PublishRequest {
            partition: p.partition_ref(),
            qos: QoS::Q1,
            ownership_epoch: 7,
            principal: 11,
            producer_id: 11,
            producer_epoch: 0,
            sequence,
            message_id: 1000 + u128::from(sequence),
            payload: payload.to_vec(),
        })
        .unwrap()
        .wait()
        .unwrap();
    assert!(matches!(outcome, PublishOutcome::CommittedLocal(_)));
}
fn fetch(p: &Partition, generation: u64, max: u32) -> rovqet_durable::GroupFetchBatch {
    p.try_fetch_group(rovqet_durable::GroupFetchRequest {
        partition: p.partition_ref(),
        group_id: 9,
        generation,
        from: DurableOffset {
            log_incarnation: p.incarnation(),
            sequence: 0,
        },
        max_messages: max as usize,
        max_bytes: 64 * 1024,
        wait: Duration::ZERO,
    })
    .unwrap()
    .wait()
    .unwrap()
}
fn nack(
    p: &Partition,
    delivery: &rovqet_durable::GroupDelivery,
) -> Result<NackOutcome, rovqet_durable::GroupError> {
    p.try_nack(NackRequest {
        partition: p.partition_ref(),
        group_id: 9,
        generation: delivery.generation,
        lease_id: delivery.lease_id,
        ownership_epoch: 7,
        offset: delivery.offset,
        attempt: delivery.attempt,
        principal: 1,
        reason: 1,
    })
    .unwrap()
    .wait()
}
fn ack(p: &Partition, delivery: &rovqet_durable::GroupDelivery) {
    p.try_ack(rovqet_durable::AckRequest {
        partition: p.partition_ref(),
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
    .unwrap();
}

struct TestDlq {
    partition: Arc<Partition>,
    partition_ref: PartitionRef,
    incarnation: u128,
}
impl rovqet_durable::DlqPublisher for TestDlq {
    fn publish(
        &self,
        record: &rovqet_durable::DlqRecord<'_>,
    ) -> Result<DurableOffset, rovqet_durable::DlqError> {
        let payload = rovqet_durable::dlq_payload(record);
        self.partition
            .try_publish(PublishRequest {
                partition: self.partition_ref,
                qos: QoS::Q1,
                ownership_epoch: 7,
                principal: 0,
                producer_id: record.disposition_id,
                producer_epoch: 0,
                sequence: 0,
                message_id: record.disposition_id,
                payload,
            })
            .and_then(|pending| pending.wait())
            .map_err(|_| rovqet_durable::DlqError::Unavailable)
            .and_then(|outcome| match outcome {
                PublishOutcome::CommittedLocal(offset) => Ok(offset),
                _ => Err(rovqet_durable::DlqError::Unavailable),
            })
    }
    fn verify(
        &self,
        offset: DurableOffset,
        disposition_id: u128,
        original_message_id: u128,
    ) -> Result<bool, rovqet_durable::DlqError> {
        let batch = self
            .partition
            .try_replay(rovqet_durable::ReplayRequest {
                partition: self.partition_ref,
                from: offset,
                max_messages: 1,
                max_bytes: 64 * 1024,
                wait: Duration::ZERO,
            })
            .and_then(|pending| pending.wait())
            .map_err(|_| rovqet_durable::DlqError::Unavailable)?;
        let Some(message) = batch.messages.first() else {
            return Ok(false);
        };
        if message.offset != offset
            || message.message_id != disposition_id
            || message.offset.log_incarnation != self.incarnation
        {
            return Ok(false);
        }
        let metadata = rovqet_durable::parse_dlq_payload(&message.payload)
            .map_err(|_| rovqet_durable::DlqError::Corruption("DLQ payload"))?;
        Ok(metadata.disposition_id == disposition_id
            && metadata.original_message_id == original_message_id)
    }
}
fn open_dlq(t: &Temp) -> Arc<TestDlq> {
    let dir = t.dir("dlq");
    fs::create_dir_all(&dir).unwrap();
    let partition_ref = partition(1, 4, 0);
    let partition = Partition::open(
        &dir,
        wal_config(partition_ref, 77),
        config(partition_ref, rovqet_durable::GroupConfig::default()),
    )
    .unwrap();
    Arc::new(TestDlq {
        partition: Arc::new(partition),
        partition_ref,
        incarnation: 77,
    })
}
fn dlq_history(dlq: &TestDlq) -> Vec<rovqet_durable::ReplayMessage> {
    dlq.partition
        .try_replay(rovqet_durable::ReplayRequest {
            partition: dlq.partition_ref,
            from: DurableOffset {
                log_incarnation: dlq.incarnation,
                sequence: 0,
            },
            max_messages: 64,
            max_bytes: 64 * 1024,
            wait: Duration::ZERO,
        })
        .unwrap()
        .wait()
        .unwrap()
        .messages
}

#[test]
fn nack_schedules_bounded_backoff_and_exhaustion_never_closes() {
    let t = Temp::new("retry");
    let group = group_config(TerminalPolicy::None, 3);
    let clock = ManualClock::default();
    let main = t.dir("wal");
    fs::create_dir_all(&main).unwrap();
    let mut p = Partition::open_with_clock(
        &main,
        wal_config(partition(1, 3, 0), 42),
        config(partition(1, 3, 0), group),
        Arc::new(clock.clone()),
    )
    .unwrap();
    publish(&p, 0, b"event-0");
    let first = fetch(&p, 0, 1);
    assert_eq!(first.deliveries[0].attempt, 1);
    assert!(matches!(
        nack(&p, &first.deliveries[0]),
        Ok(NackOutcome::RetryScheduled {
            retry_after_ms: 100
        })
    ));
    // No cursor movement and no immediate redelivery before the backoff deadline.
    let early = fetch(&p, 1, 1);
    assert!(early.deliveries.is_empty());
    assert_eq!(early.next.sequence, 0);
    clock.advance(100);
    let second = fetch(&p, 1, 1);
    assert_eq!(second.deliveries[0].attempt, 2);
    clock.advance(10);
    assert!(matches!(
        nack(&p, &second.deliveries[0]),
        Ok(NackOutcome::RetryScheduled {
            retry_after_ms: 200
        })
    ));
    clock.advance(200);
    let third = fetch(&p, 1, 1);
    assert_eq!(third.deliveries[0].attempt, 3);
    // Exhaustion is recorded durably but leaves the source incomplete.
    assert!(matches!(
        nack(&p, &third.deliveries[0]),
        Ok(NackOutcome::RetryExhausted)
    ));
    let stalled = fetch(&p, 1, 1);
    assert!(stalled.deliveries.is_empty());
    assert_eq!(stalled.next.sequence, 0);
    // A repeated exhausted NACK is idempotent: no new retry record.
    let before = p.committed_records();
    assert!(matches!(
        nack(&p, &third.deliveries[0]),
        Ok(NackOutcome::RetryExhausted)
    ));
    assert_eq!(p.committed_records(), before);
    drop(p);
    let p = Partition::open_with_clock(
        &main,
        wal_config(partition(1, 3, 0), 42),
        config(partition(1, 3, 0), group),
        Arc::new(clock),
    )
    .unwrap();
    let after = fetch(&p, 0, 1);
    assert!(after.deliveries.is_empty(), "exhaustion persisted");
    assert_eq!(after.next.sequence, 0, "exhaustion never closes the cursor");
}

#[test]
fn quarantine_terminal_requires_a_barrier_and_closes_only_its_offset() {
    let t = Temp::new("quarantine");
    let clock = ManualClock::default();
    let dir = t.dir("wal");
    fs::create_dir_all(&dir).unwrap();
    let p = Partition::open_with_clock(
        &dir,
        wal_config(partition(1, 3, 0), 42),
        config(
            partition(1, 3, 0),
            group_config(TerminalPolicy::Quarantine, 1),
        ),
        Arc::new(clock),
    )
    .unwrap();
    for sequence in 0..3 {
        publish(&p, sequence, format!("event-{sequence}").as_bytes());
    }
    let batch = fetch(&p, 0, 8);
    assert_eq!(batch.deliveries.len(), 3);
    // Quarantine the middle record first: the cursor must not skip the first one.
    let outcome = nack(&p, &batch.deliveries[1]).unwrap();
    let NackOutcome::TerminalCommitted { disposition_id } = outcome else {
        panic!("expected committed quarantine: {outcome:?}");
    };
    assert_ne!(disposition_id, 0);
    let gap = fetch(&p, 1, 8);
    assert_eq!(gap.next.sequence, 0);
    assert!(gap.deliveries.iter().all(|d| d.offset.sequence != 2));
    // Acknowledge the first record: the cursor then advances across the closed gap.
    ack(&p, &batch.deliveries[0]);
    let advanced = fetch(&p, 1, 8);
    assert!(advanced.deliveries.iter().all(|d| d.offset.sequence != 2));
    drop(p);
    let p = Partition::open_with_clock(
        &dir,
        wal_config(partition(1, 3, 0), 42),
        config(
            partition(1, 3, 0),
            group_config(TerminalPolicy::Quarantine, 1),
        ),
        Arc::new(ManualClock::default()),
    )
    .unwrap();
    let recovered = fetch(&p, 0, 8);
    assert!(recovered.deliveries.iter().all(|d| d.offset.sequence != 2));
    // Repeated NACK of the closed token is a read-only saved outcome.
    assert!(matches!(
        nack(&p, &batch.deliveries[1]),
        Ok(NackOutcome::AlreadyClosed { disposition_id: id }) if id == disposition_id
    ));
}

#[test]
fn dlq_is_a_real_replayable_committed_partition_and_ordering_is_idempotent() {
    let t = Temp::new("dlq");
    let dlq = open_dlq(&t);
    let dir = t.dir("wal");
    fs::create_dir_all(&dir).unwrap();
    let p = Partition::open_with_dlq_and_clock(
        &dir,
        wal_config(partition(1, 3, 0), 42),
        config(partition(1, 3, 0), group_config(TerminalPolicy::Dlq, 1)),
        dlq.clone(),
        Arc::new(ManualClock::default()),
    )
    .unwrap();
    publish(&p, 0, b"event-0");
    let batch = fetch(&p, 0, 1);
    let outcome = nack(&p, &batch.deliveries[0]).unwrap();
    let NackOutcome::TerminalCommitted { disposition_id } = outcome else {
        panic!("expected committed DLQ terminal: {outcome:?}");
    };
    let history = dlq_history(&dlq);
    assert_eq!(history.len(), 1);
    assert_eq!(history[0].message_id, disposition_id);
    let metadata = rovqet_durable::parse_dlq_payload(&history[0].payload).unwrap();
    assert_eq!(metadata.disposition_id, disposition_id);
    assert_eq!(metadata.original_message_id, 1000);
    assert_eq!(metadata.source_offset, 0);
    assert_eq!(metadata.source_incarnation, 42);
    assert_eq!(metadata.payload, b"event-0");
    // Source cursor advanced only after the committed terminal record.
    let end = fetch(&p, 1, 1);
    assert!(end.deliveries.is_empty());
    // The cursor is the append watermark, not an arithmetic message offset.
    assert_eq!(end.next.sequence, p.committed_records());
    // Repeated NACK must not create another DLQ message.
    assert!(matches!(
        nack(&p, &batch.deliveries[0]),
        Ok(NackOutcome::AlreadyClosed { .. })
    ));
    assert_eq!(dlq_history(&dlq).len(), 1);
    drop(p);
    // Recovery verifies the committed DLQ proof and keeps the cursor closed.
    let p = Partition::open_with_dlq_and_clock(
        &dir,
        wal_config(partition(1, 3, 0), 42),
        config(partition(1, 3, 0), group_config(TerminalPolicy::Dlq, 1)),
        dlq.clone(),
        Arc::new(ManualClock::default()),
    )
    .unwrap();
    let recovered = fetch(&p, 0, 1);
    assert!(recovered.deliveries.is_empty());
    assert!(recovered.next.sequence > 0);
    assert_eq!(dlq_history(&dlq).len(), 1);
}

#[test]
fn unavailable_dlq_leaves_the_source_incomplete_then_recovery_completes_it() {
    let t = Temp::new("dlq-down");
    let dir = t.dir("wal");
    fs::create_dir_all(&dir).unwrap();
    let clock = ManualClock::default();
    let p = Partition::open_with_clock(
        &dir,
        wal_config(partition(1, 3, 0), 42),
        config(partition(1, 3, 0), group_config(TerminalPolicy::Dlq, 1)),
        Arc::new(clock.clone()),
    )
    .unwrap();
    publish(&p, 0, b"event-0");
    let batch = fetch(&p, 0, 1);
    assert!(matches!(
        nack(&p, &batch.deliveries[0]),
        Err(rovqet_durable::GroupError::DlqUnavailable)
    ));
    // The pending disposition is durable but the source is still incomplete.
    assert_eq!(fetch(&p, 1, 1).next.sequence, 0);
    drop(p);
    // Restart with the DLQ available: recovery reconciles and commits the terminal record.
    let dlq = open_dlq(&t);
    let p = Partition::open_with_dlq_and_clock(
        &dir,
        wal_config(partition(1, 3, 0), 42),
        config(partition(1, 3, 0), group_config(TerminalPolicy::Dlq, 1)),
        dlq.clone(),
        Arc::new(clock),
    )
    .unwrap();
    let recovered = fetch(&p, 0, 1);
    assert!(recovered.deliveries.is_empty());
    assert!(recovered.next.sequence > 0);
    assert_eq!(dlq_history(&dlq).len(), 1);
}

#[test]
fn sparse_terminal_closure_preserves_the_contiguous_cursor() {
    let t = Temp::new("sparse-terminal");
    let dir = t.dir("wal");
    fs::create_dir_all(&dir).unwrap();
    let p = Partition::open_with_clock(
        &dir,
        wal_config(partition(1, 3, 0), 42),
        config(
            partition(1, 3, 0),
            group_config(TerminalPolicy::Quarantine, 1),
        ),
        Arc::new(ManualClock::default()),
    )
    .unwrap();
    for sequence in 0..3 {
        publish(&p, sequence, format!("event-{sequence}").as_bytes());
    }
    let batch = fetch(&p, 0, 8);
    // Close the last record; the cursor stays at the first incomplete offset.
    assert!(matches!(
        nack(&p, &batch.deliveries[2]),
        Ok(NackOutcome::TerminalCommitted { .. })
    ));
    assert_eq!(fetch(&p, 1, 8).next.sequence, 0);
    ack(&p, &batch.deliveries[0]);
    assert_eq!(fetch(&p, 1, 8).next.sequence, 2);
    // Closing the middle record lets the cursor advance through the sparse closures.
    assert!(matches!(
        nack(&p, &batch.deliveries[1]),
        Ok(NackOutcome::TerminalCommitted { .. })
    ));
    let end = fetch(&p, 1, 8);
    assert!(end.deliveries.is_empty());
    assert!(end.next.sequence >= 6);
}

#[test]
fn corrupt_terminal_metadata_fails_closed() {
    let t = Temp::new("corrupt-terminal");
    let dir = t.dir("wal");
    fs::create_dir_all(&dir).unwrap();
    let p = Partition::open_with_clock(
        &dir,
        wal_config(partition(1, 3, 0), 42),
        config(
            partition(1, 3, 0),
            group_config(TerminalPolicy::Quarantine, 1),
        ),
        Arc::new(ManualClock::default()),
    )
    .unwrap();
    publish(&p, 0, b"event-0");
    drop(p);
    let mut wal = rovqet_wal::Wal::open(dir.clone(), wal_config(partition(1, 3, 0), 42)).unwrap();
    let next = wal.committed_position().unwrap().sequence + 1;
    wal.append(rovqet_wal::Record {
        kind: rovqet_wal::RecordKind::TerminalDisposition,
        logical_offset: next,
        owner_epoch: 7,
        producer_id: 0,
        producer_epoch: 0,
        sequence: 0,
        message_id: 1000,
        server_timestamp: 1,
        payload: b"corrupt-terminal-record".to_vec(),
    })
    .unwrap();
    wal.sync().unwrap();
    drop(wal);
    assert!(matches!(
        Partition::open_with_clock(
            &dir,
            wal_config(partition(1, 3, 0), 42),
            config(
                partition(1, 3, 0),
                group_config(TerminalPolicy::Quarantine, 1)
            ),
            Arc::new(ManualClock::default()),
        ),
        Err(rovqet_durable::Error::Corruption(_))
    ));
}

#[test]
fn retry_and_terminal_state_bounds_are_enforced() {
    // Bounded retry entries.
    let retry_temp = Temp::new("bounds-retry");
    let retry_dir = retry_temp.dir("wal");
    fs::create_dir_all(&retry_dir).unwrap();
    let mut group = group_config(TerminalPolicy::None, 8);
    group.max_retries = 1;
    let p = Partition::open_with_clock(
        &retry_dir,
        wal_config(partition(1, 3, 0), 42),
        config(partition(1, 3, 0), group),
        Arc::new(ManualClock::default()),
    )
    .unwrap();
    publish(&p, 0, b"event-0");
    publish(&p, 1, b"event-1");
    let batch = fetch(&p, 0, 8);
    assert!(matches!(
        nack(&p, &batch.deliveries[0]),
        Ok(NackOutcome::RetryScheduled { .. })
    ));
    assert!(matches!(
        nack(&p, &batch.deliveries[1]),
        Err(rovqet_durable::GroupError::Capacity)
    ));
    drop(p);

    // Bounded terminal-pending state.
    let terminal_temp = Temp::new("bounds-terminal");
    let terminal_dir = terminal_temp.dir("wal");
    fs::create_dir_all(&terminal_dir).unwrap();
    let mut group = group_config(TerminalPolicy::Dlq, 1);
    group.max_terminal_pending = 0;
    let p = Partition::open_with_clock(
        &terminal_dir,
        wal_config(partition(1, 3, 0), 42),
        config(partition(1, 3, 0), group),
        Arc::new(ManualClock::default()),
    )
    .unwrap();
    publish(&p, 0, b"event-0");
    let batch = fetch(&p, 0, 1);
    assert!(matches!(
        nack(&p, &batch.deliveries[0]),
        Err(rovqet_durable::GroupError::Capacity)
    ));
    assert_eq!(fetch(&p, 1, 1).next.sequence, 0);
}
