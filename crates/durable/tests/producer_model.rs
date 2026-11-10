//! Bounded B-03 model proofs. These do not qualify a timed deduplication window.
use rovqet_core::{DurableOffset, PartitionRef, PublishOutcome, QoS};
use rovqet_durable::{Error, Partition, PartitionConfig, PublishRequest};
use rovqet_wal::{Config as WalConfig, RecordKind, Wal};
use std::{
    collections::BTreeMap,
    fs,
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
};

static NEXT: AtomicU64 = AtomicU64::new(0);
struct Directory(PathBuf);
impl Directory {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "rovqet-producer-model-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&path).unwrap();
        Self(path)
    }
}
impl Drop for Directory {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.0).unwrap();
    }
}

struct Harness {
    directory: Directory,
    wal: WalConfig,
    config: PartitionConfig,
    actor: Option<Partition>,
}
impl Harness {
    fn new(partition_id: u32, max_outcomes: usize) -> Self {
        let directory = Directory::new();
        let partition = PartitionRef {
            tenant_id: 1,
            topic_id: 7,
            partition_id,
        };
        let wal = WalConfig {
            partition,
            incarnation: 77,
            max_segment_bytes: 1024,
            max_record_bytes: 256,
            max_pending_bytes: 4096,
            max_pending_records: 8,
            max_segments: 64,
            max_records: 256,
            ..WalConfig::default()
        };
        let config = PartitionConfig {
            partition,
            ownership_epoch: 3,
            queue_messages: 4,
            queue_bytes: 4096,
            max_outcomes,
        };
        let actor = Partition::open(&directory.0, wal.clone(), config.clone()).unwrap();
        Self {
            directory,
            wal,
            config,
            actor: Some(actor),
        }
    }
    fn publish(&self, request: PublishRequest) -> Result<PublishOutcome, Error> {
        self.actor.as_ref().unwrap().try_publish(request)?.wait()
    }
    fn restart(&mut self) {
        self.actor.take().unwrap().shutdown().unwrap();
        self.actor = Some(
            Partition::open(&self.directory.0, self.wal.clone(), self.config.clone()).unwrap(),
        );
    }
    fn request(
        &self,
        principal: u128,
        producer: u128,
        epoch: u64,
        sequence: u64,
    ) -> PublishRequest {
        PublishRequest {
            partition: self.config.partition,
            qos: QoS::Q1,
            ownership_epoch: self.config.ownership_epoch,
            principal,
            producer_id: producer,
            producer_epoch: epoch,
            sequence,
            message_id: principal * 100_000 + producer * 1000 + u128::from(epoch * 10 + sequence),
            payload: format!("{principal}:{producer}:{epoch}:{sequence}").into_bytes(),
        }
    }
    fn close_and_read(&mut self) -> Vec<rovqet_wal::Record> {
        self.actor.take().unwrap().shutdown().unwrap();
        let wal = Wal::open(&self.directory.0, self.wal.clone()).unwrap();
        wal.read(offset(0), 256, 64 * 1024).unwrap()
    }
}
impl Drop for Harness {
    fn drop(&mut self) {
        // Join before Directory's destructor removes the disposable history.
        if let Some(actor) = self.actor.take() {
            actor.shutdown().unwrap();
        }
    }
}
fn offset(sequence: u64) -> DurableOffset {
    DurableOffset {
        log_incarnation: 77,
        sequence,
    }
}
fn committed(sequence: u64) -> PublishOutcome {
    PublishOutcome::CommittedLocal(offset(sequence))
}

#[test]
fn scoped_producer_model_preserves_outcomes_across_restarts_and_capacity() {
    const SCOPES: [(u128, u128); 4] = [(11, 21), (11, 22), (12, 21), (12, 22)];
    const FRESH: usize = 24;
    let mut harness = Harness::new(0, FRESH);
    // An independent table predicts offsets and retained message identities.
    let mut model = BTreeMap::<(u128, u128, u64, u64), (u64, PublishRequest)>::new();
    for epoch in 1..=2 {
        for sequence in 0..3 {
            for (principal, producer) in SCOPES {
                let request = harness.request(principal, producer, epoch, sequence);
                let gap = harness.request(principal, producer, epoch, sequence + 1);
                assert!(matches!(harness.publish(gap), Err(Error::SequenceGap)));
                let expected = (model.len() * 2) as u64;
                assert_eq!(
                    harness.publish(request.clone()).unwrap(),
                    committed(expected)
                );
                assert_eq!(
                    harness.publish(request.clone()).unwrap(),
                    committed(expected)
                );
                let mut conflict = request.clone();
                conflict.payload.push(0xff);
                assert!(matches!(
                    harness.publish(conflict),
                    Err(Error::SequenceConflict)
                ));
                let mut changed_identity = request.clone();
                changed_identity.message_id += 1;
                assert!(matches!(
                    harness.publish(changed_identity),
                    Err(Error::SequenceConflict)
                ));
                let fenced = harness.request(principal, producer, epoch - 1, 0);
                assert!(matches!(
                    harness.publish(fenced),
                    Err(Error::ProducerFenced)
                ));
                model.insert(
                    (principal, producer, epoch, sequence),
                    (expected, request.clone()),
                );
                if model.len() % 4 == 0 {
                    harness.restart();
                    assert_eq!(harness.publish(request).unwrap(), committed(expected));
                }
            }
        }
    }
    assert_eq!(model.len(), FRESH);
    let unrelated = harness.request(99, 99, 1, 0);
    assert!(matches!(
        harness.publish(unrelated.clone()),
        Err(Error::Capacity)
    ));
    harness.restart();
    assert!(matches!(harness.publish(unrelated), Err(Error::Capacity)));
    for ((_, _, epoch, _), (expected, request)) in &model {
        if *epoch == 2 {
            assert_eq!(
                harness.publish(request.clone()).unwrap(),
                committed(*expected)
            );
        } else {
            assert!(matches!(
                harness.publish(request.clone()),
                Err(Error::ProducerFenced)
            ));
        }
    }
    let rows = harness.close_and_read();
    assert_eq!(
        rows.len(),
        2 * FRESH,
        "rejects/retries must never append another pair"
    );
    for ((principal, producer, epoch, sequence), (expected, request)) in &model {
        let message = &rows[*expected as usize];
        let outcome = &rows[*expected as usize + 1];
        assert_eq!(message.kind, RecordKind::Message);
        assert_eq!(message.logical_offset, *expected);
        assert_eq!(message.producer_id, *producer);
        assert_eq!(message.producer_epoch, *epoch);
        assert_eq!(message.sequence, *sequence);
        assert_eq!(message.message_id, request.message_id);
        assert_eq!(message.payload, request.payload);
        assert_eq!(outcome.kind, RecordKind::ProducerOutcome);
        assert_eq!(outcome.payload[..6], *b"RQPO\0\x01");
        assert_eq!(&outcome.payload[6..22], &principal.to_be_bytes());
        assert_eq!(&outcome.payload[22..30], &expected.to_be_bytes());
    }
}

#[test]
fn same_tokens_in_independent_partitions_and_principals_recover_independently() {
    let mut left = Harness::new(0, 8);
    let mut right = Harness::new(1, 8);
    let mut left_first = left.request(1, 5, 0, 0);
    let mut right_first = right.request(1, 5, 0, 0);
    left_first.payload = b"left partition".to_vec();
    right_first.payload = b"right partition".to_vec();
    assert_eq!(left.publish(left_first.clone()).unwrap(), committed(0));
    assert_eq!(right.publish(right_first.clone()).unwrap(), committed(0));
    let other_left = left.request(2, 5, 0, 0);
    let other_right = right.request(2, 5, 0, 0);
    assert_eq!(left.publish(other_left.clone()).unwrap(), committed(2));
    assert_eq!(right.publish(other_right.clone()).unwrap(), committed(2));
    let left_second = left.request(1, 5, 0, 1);
    let right_second = right.request(1, 5, 0, 1);
    assert_eq!(left.publish(left_second.clone()).unwrap(), committed(4));
    assert_eq!(right.publish(right_second.clone()).unwrap(), committed(4));
    left.restart();
    right.restart();
    for (harness, first, other, second) in [
        (
            &left,
            left_first.clone(),
            other_left.clone(),
            left_second.clone(),
        ),
        (
            &right,
            right_first.clone(),
            other_right.clone(),
            right_second.clone(),
        ),
    ] {
        assert_eq!(harness.publish(first).unwrap(), committed(0));
        assert_eq!(harness.publish(other).unwrap(), committed(2));
        assert_eq!(harness.publish(second).unwrap(), committed(4));
    }
    let new_left_epoch = left.request(1, 5, 1, 0);
    assert_eq!(left.publish(new_left_epoch.clone()).unwrap(), committed(6));
    assert!(matches!(
        left.publish(left_second),
        Err(Error::ProducerFenced)
    ));
    // Fencing one principal/partition cannot fence another scope's identical producer ID.
    assert_eq!(left.publish(other_left).unwrap(), committed(2));
    assert_eq!(right.publish(right_second).unwrap(), committed(4));
    left.restart();
    right.restart();
    assert_eq!(left.publish(new_left_epoch).unwrap(), committed(6));
    assert_eq!(right.publish(right_first.clone()).unwrap(), committed(0));
    let left_rows = left.close_and_read();
    let right_rows = right.close_and_read();
    assert_eq!(left_rows.len(), 8);
    assert_eq!(right_rows.len(), 6);
    assert_eq!(left_rows[0].payload, left_first.payload);
    assert_eq!(right_rows[0].payload, right_first.payload);
}
