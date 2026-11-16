//! The bundle quiescence barrier of a coordinated source-plus-DLQ handover.
//!
//! A terminal disposition crosses two durability domains: the source publishes a real Q1
//! DLQ message and only then commits the source terminal record. The barrier closes the
//! terminal path before the final fences, so a DLQ publication can never happen after the
//! dependent log was handed over, and an admitted-but-unfinished disposition keeps its
//! durable PENDING record instead of being completed by the old owner.
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use rovqet_core::{DurableOffset, PartitionRef, PublishOutcome, QoS};
use rovqet_durable::{
    Authority, DlqError, DlqPublisher, DlqRecord, Error, GroupError, NackOutcome, NackRequest,
    Partition, PartitionConfig, PublishRequest, TerminalPolicy,
};
use rovqet_wal::Config as WalConfig;

static NEXT: AtomicU64 = AtomicU64::new(0);

struct Temp(PathBuf);
impl Temp {
    fn new(label: &str) -> Self {
        let path = std::env::temp_dir().join(format!(
            "rovqet-bundle-{label}-{}-{}",
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

fn source_partition() -> PartitionRef {
    PartitionRef {
        tenant_id: 1,
        topic_id: 3,
        partition_id: 0,
    }
}

fn dlq_partition() -> PartitionRef {
    PartitionRef {
        tenant_id: 1,
        topic_id: 4,
        partition_id: 0,
    }
}

const SOURCE_INCARNATION: u128 = 42;
const DLQ_INCARNATION: u128 = 77;
const MIGRATION: u128 = 0x9999_8888_7777_6666;

fn wal_config(partition: PartitionRef, incarnation: u128) -> WalConfig {
    WalConfig {
        partition,
        incarnation,
        ..WalConfig::default()
    }
}

fn partition_config(
    partition: PartitionRef,
    node_id: u64,
    epoch: u64,
    policy: TerminalPolicy,
    attempts: u32,
) -> PartitionConfig {
    PartitionConfig {
        partition,
        ownership_epoch: epoch,
        node_id,
        queue_messages: 64,
        queue_bytes: 4 * 1024 * 1024,
        max_outcomes: 512,
        max_producers: 512,
        dedup_window: Duration::from_secs(86400),
        group: rovqet_durable::GroupConfig {
            max_attempts: attempts,
            retry_initial: Duration::from_millis(10),
            retry_max: Duration::from_millis(50),
            terminal_policy: policy,
            ..rovqet_durable::GroupConfig::default()
        },
    }
}

/// A destination that records every publication attempt and can be made unavailable.
struct Destination {
    partition: Partition,
    available: std::sync::atomic::AtomicBool,
    attempts: AtomicU64,
}

impl DlqPublisher for Destination {
    fn publish(&self, record: &DlqRecord<'_>) -> Result<DurableOffset, DlqError> {
        self.attempts.fetch_add(1, Ordering::SeqCst);
        if !self.available.load(Ordering::SeqCst) {
            return Err(DlqError::Unavailable);
        }
        let payload = rovqet_durable::dlq_payload(record);
        self.partition
            .try_publish(PublishRequest {
                partition: dlq_partition(),
                qos: QoS::Q1,
                ownership_epoch: 8,
                principal: 0,
                producer_id: record.disposition_id,
                producer_epoch: 0,
                sequence: 0,
                message_id: record.disposition_id,
                payload,
            })
            .and_then(|pending| pending.wait())
            .map_err(|_| DlqError::Unavailable)
            .and_then(|outcome| match outcome {
                PublishOutcome::CommittedLocal(offset) => Ok(offset),
                _ => Err(DlqError::Unavailable),
            })
    }

    fn verify(
        &self,
        offset: DurableOffset,
        disposition_id: u128,
        original_message_id: u128,
    ) -> Result<bool, DlqError> {
        if offset.log_incarnation != DLQ_INCARNATION {
            return Ok(false);
        }
        let batch = self
            .partition
            .try_replay(rovqet_durable::ReplayRequest {
                partition: dlq_partition(),
                from: offset,
                max_messages: 1,
                max_bytes: 64 * 1024,
                wait: Duration::ZERO,
            })
            .and_then(|pending| pending.wait())
            .map_err(|_| DlqError::Unavailable)?;
        let Some(message) = batch.messages.first() else {
            return Ok(false);
        };
        if message.offset != offset || message.message_id != disposition_id {
            return Ok(false);
        }
        let metadata = rovqet_durable::parse_dlq_payload(&message.payload)
            .map_err(|_| DlqError::Corruption("DLQ payload"))?;
        Ok(metadata.disposition_id == disposition_id
            && metadata.original_message_id == original_message_id)
    }
}

fn open_destination(t: &Temp, epoch: u64) -> Arc<Destination> {
    let dir = t.dir("dlq");
    fs::create_dir_all(&dir).unwrap();
    let partition = Partition::open(
        &dir,
        wal_config(dlq_partition(), DLQ_INCARNATION),
        partition_config(dlq_partition(), 1, epoch, TerminalPolicy::None, 1),
    )
    .expect("the destination opens");
    Arc::new(Destination {
        partition,
        available: std::sync::atomic::AtomicBool::new(true),
        attempts: AtomicU64::new(0),
    })
}

fn open_source(t: &Temp, node_id: u64, epoch: u64, destination: Arc<Destination>) -> Partition {
    let dir = t.dir("source");
    fs::create_dir_all(&dir).unwrap();
    Partition::open_with_dlq(
        &dir,
        wal_config(source_partition(), SOURCE_INCARNATION),
        partition_config(source_partition(), node_id, epoch, TerminalPolicy::Dlq, 1),
        destination,
    )
    .expect("the source opens with its destination")
}

fn reopen_source(t: &Temp, node_id: u64, epoch: u64, destination: Arc<Destination>) -> Partition {
    let dir = t.dir("source");
    Partition::open_existing_with_dlq(
        &dir,
        wal_config(source_partition(), SOURCE_INCARNATION),
        partition_config(source_partition(), node_id, epoch, TerminalPolicy::Dlq, 1),
        destination,
    )
    .expect("the source reopens with its destination")
}

fn publish(p: &Partition, sequence: u64) {
    let outcome = p
        .try_publish(PublishRequest {
            partition: source_partition(),
            qos: QoS::Q1,
            ownership_epoch: if sequence == 0 { 7 } else { 7 },
            principal: 11,
            producer_id: 11,
            producer_epoch: 0,
            sequence,
            message_id: 1000 + u128::from(sequence),
            payload: format!("event-{sequence}").into_bytes(),
        })
        .unwrap()
        .wait()
        .unwrap();
    assert!(matches!(outcome, PublishOutcome::CommittedLocal(_)));
}

fn nack_first(p: &Partition, attempt: u32) -> Result<NackOutcome, GroupError> {
    let batch = p
        .try_fetch_group(rovqet_durable::GroupFetchRequest {
            partition: source_partition(),
            group_id: 9,
            generation: 0,
            from: DurableOffset {
                log_incarnation: SOURCE_INCARNATION,
                sequence: 0,
            },
            max_messages: 4,
            max_bytes: 65536,
            wait: Duration::ZERO,
        })
        .map_err(|_| GroupError::Unavailable)?
        .wait()?;
    let Some(delivery) = batch.deliveries.first().cloned() else {
        return Err(GroupError::NotLeased);
    };
    p.try_nack(NackRequest {
        partition: source_partition(),
        group_id: 9,
        generation: delivery.generation,
        lease_id: delivery.lease_id,
        ownership_epoch: 7,
        offset: delivery.offset,
        attempt,
        principal: 1,
        reason: 1,
    })
    .map_err(|_| GroupError::Unavailable)?
    .wait()
}

#[test]
fn the_barrier_refuses_new_terminal_operations() {
    let temp = Temp::new("barrier");
    let destination = open_destination(&temp, 8);
    let source = open_source(&temp, 1, 7, destination.clone());
    source
        .set_authority(Authority::new(1, 7, Duration::from_secs(600)))
        .unwrap();
    publish(&source, 0);
    let barrier = block(source.quiesce_terminal()).expect("the barrier is established");
    assert_eq!(barrier.pending, 0, "nothing was pending at the barrier");
    assert!(
        barrier.watermark > 0,
        "the barrier reports the committed position"
    );
    // A terminal operation after the barrier is refused and publishes nothing.
    assert!(
        matches!(nack_first(&source, 1), Err(GroupError::Quiesced)),
        "the quiesced terminal path must refuse a new disposition"
    );
    assert_eq!(
        destination.attempts.load(Ordering::SeqCst),
        0,
        "no dependent publication may happen after the barrier"
    );
}

#[test]
fn the_barrier_preserves_an_unfinished_disposition_and_the_recovery_completes_it() {
    let temp = Temp::new("pending");
    let destination = open_destination(&temp, 8);
    let source = open_source(&temp, 1, 7, destination.clone());
    source
        .set_authority(Authority::new(1, 7, Duration::from_secs(600)))
        .unwrap();
    publish(&source, 0);
    // The destination is unavailable: the source commits its durable PENDING record and the
    // disposition stays pending. It is never silently closed.
    destination.available.store(false, Ordering::SeqCst);
    assert!(matches!(
        nack_first(&source, 1),
        Err(GroupError::DlqUnavailable) | Ok(NackOutcome::TerminalPending { .. })
    ));
    let barrier = block(source.quiesce_terminal()).expect("the barrier is established");
    assert_eq!(
        barrier.pending, 1,
        "the barrier accounts for the unfinished terminal transaction"
    );
    let attempts = destination.attempts.load(Ordering::SeqCst);
    assert!(
        attempts >= 1,
        "the admitted transaction tried its dependent publication"
    );
    drop(source);
    assert_eq!(
        destination.attempts.load(Ordering::SeqCst),
        attempts,
        "the barrier never publishes again"
    );
    // The successor resumes the preserved disposition against its own destination: exactly
    // one dependent message is published for it.
    destination.available.store(true, Ordering::SeqCst);
    let successor = reopen_source(&temp, 2, 8, destination.clone());
    let references = block(successor.terminal_references()).expect("the audit answers");
    assert_eq!(
        references.len(),
        1,
        "the recovered disposition is committed with one destination reference"
    );
    let verified = destination
        .verify(
            DurableOffset {
                log_incarnation: references[0].destination_incarnation,
                sequence: references[0].destination_offset,
            },
            references[0].disposition_id,
            references[0].original_message_id,
        )
        .expect("the destination verifies");
    assert!(
        verified,
        "the destination holds exactly the referenced record"
    );
}

#[test]
fn a_fenced_owner_never_completes_a_pending_disposition_after_the_handover() {
    let temp = Temp::new("fenced");
    let destination = open_destination(&temp, 8);
    let source = open_source(&temp, 1, 7, destination.clone());
    source
        .set_authority(Authority::new(1, 7, Duration::from_secs(600)))
        .unwrap();
    publish(&source, 0);
    destination.available.store(false, Ordering::SeqCst);
    let _ = nack_first(&source, 1);
    let barrier = block(source.quiesce_terminal()).expect("the barrier is established");
    assert_eq!(barrier.pending, 1);
    source
        .fence_old_owner(MIGRATION, 7, 8)
        .expect("the source fences durably");
    drop(source);
    let attempts = destination.attempts.load(Ordering::SeqCst);
    // The restarted old owner is fenced by its own durable record: it must not publish the
    // preserved disposition into a destination that already belongs to the successor.
    destination.available.store(true, Ordering::SeqCst);
    let restarted = reopen_source(&temp, 1, 7, destination.clone());
    assert_eq!(
        destination.attempts.load(Ordering::SeqCst),
        attempts,
        "a fenced owner must never publish again"
    );
    // The disposition is still pending: it is preserved, not fabricated.
    assert!(
        matches!(nack_first(&restarted, 1), Err(_)),
        "a fenced owner refuses every terminal operation"
    );
    drop(restarted);
    // The successor resumes it and publishes exactly once.
    let successor = reopen_source(&temp, 2, 8, destination.clone());
    let references = block(successor.terminal_references()).expect("the audit answers");
    assert_eq!(references.len(), 1);
    let verified = destination
        .verify(
            DurableOffset {
                log_incarnation: references[0].destination_incarnation,
                sequence: references[0].destination_offset,
            },
            references[0].disposition_id,
            references[0].original_message_id,
        )
        .expect("the destination verifies");
    assert!(verified);
}

#[test]
fn a_committed_terminal_reference_is_audited_from_the_durable_history() {
    let temp = Temp::new("audit");
    let destination = open_destination(&temp, 8);
    let source = open_source(&temp, 1, 7, destination.clone());
    source
        .set_authority(Authority::new(1, 7, Duration::from_secs(600)))
        .unwrap();
    publish(&source, 0);
    let outcome = nack_first(&source, 1).expect("the terminal disposition commits");
    let disposition = match outcome {
        NackOutcome::TerminalCommitted { disposition_id } => disposition_id,
        other => panic!("expected a committed disposition: {other:?}"),
    };
    let references = block(source.terminal_references()).expect("the audit answers");
    assert_eq!(references.len(), 1);
    assert_eq!(references[0].disposition_id, disposition);
    assert_eq!(references[0].destination_incarnation, DLQ_INCARNATION);
    drop(source);
    // The reference is durable: a restart reports exactly the same identity.
    let reopened = reopen_source(&temp, 2, 8, destination.clone());
    let recovered = block(reopened.terminal_references()).expect("the audit answers");
    assert_eq!(recovered, references);
    // A destination record of another identity is never accepted as proof.
    let forged = rovqet_durable::TerminalReference {
        disposition_id: disposition,
        destination_incarnation: DLQ_INCARNATION + 1,
        destination_offset: references[0].destination_offset,
        original_message_id: references[0].original_message_id,
        source_offset: references[0].source_offset,
    };
    assert!(
        !destination
            .verify(
                DurableOffset {
                    log_incarnation: forged.destination_incarnation,
                    sequence: forged.destination_offset,
                },
                forged.disposition_id,
                forged.original_message_id,
            )
            .expect("the destination answers"),
        "a record of another incarnation is not the referenced disposition"
    );
}

#[test]
fn a_repeated_fence_of_the_same_handover_returns_the_same_durable_proof() {
    let temp = Temp::new("refence");
    let destination = open_destination(&temp, 8);
    let source = open_source(&temp, 1, 7, destination.clone());
    source
        .set_authority(Authority::new(1, 7, Duration::from_secs(600)))
        .unwrap();
    let first = source
        .fence_old_owner(MIGRATION, 7, 8)
        .expect("the first fence succeeds");
    let second = source
        .fence_old_owner(MIGRATION, 7, 8)
        .expect("the repeated fence of the same handover is idempotent");
    assert_eq!(first.fence_offset, second.fence_offset);
    assert_eq!(first.fence_digest, second.fence_digest);
    assert_eq!(first.migration_id, second.migration_id);
    assert_eq!(first.old_epoch, second.old_epoch);
    // The same owner may not be fenced for another handover.
    assert!(matches!(
        source.fence_old_owner(MIGRATION + 1, 7, 8),
        Err(Error::Fenced)
    ));
}

/// Runs one small future to completion on a dedicated runtime.
fn block<T>(future: impl std::future::Future<Output = T>) -> T {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime")
        .block_on(future)
}
