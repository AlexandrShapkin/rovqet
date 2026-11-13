//! Deterministic window, quota, clock and concurrent retry proofs. No wall-clock sleeps.
use rovqet_core::{DurableOffset, PublishOutcome, QoS};
use rovqet_durable::{Clock, Error, Partition, PartitionConfig, PublishRequest};
use rovqet_wal::{Config, Wal};
use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};
static NEXT: AtomicU64 = AtomicU64::new(0);
#[derive(Default)]
struct Time(AtomicU64);
impl Time {
    fn set(&self, n: u64) {
        self.0.store(n, Ordering::SeqCst);
    }
}
impl Clock for Time {
    fn now_millis(&self) -> Option<u64> {
        match self.0.load(Ordering::SeqCst) {
            u64::MAX => None,
            n => Some(n),
        }
    }
}
struct Fixture {
    path: std::path::PathBuf,
    clock: Arc<Time>,
    wal: Config,
    config: PartitionConfig,
    actor: Option<Partition>,
}
impl Fixture {
    fn new(outcomes: usize, producers: usize) -> Self {
        let path = std::env::temp_dir().join(format!(
            "rovqet-window-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir(&path).unwrap();
        let wal = Config::default();
        let config = PartitionConfig {
            partition: wal.partition,
            ownership_epoch: 1,
            node_id: 0,
            queue_messages: 32,
            queue_bytes: 65536,
            max_outcomes: outcomes,
            max_producers: producers,
            dedup_window: Duration::from_millis(100),
            group: rovqet_durable::GroupConfig::default(),
        };
        let clock = Arc::new(Time::default());
        let actor = Some(
            Partition::open_with_clock(&path, wal.clone(), config.clone(), clock.clone()).unwrap(),
        );
        Self {
            path,
            clock,
            wal,
            config,
            actor,
        }
    }
    fn request(&self, sequence: u64) -> PublishRequest {
        PublishRequest {
            partition: self.wal.partition,
            qos: QoS::Q1,
            ownership_epoch: 1,
            principal: 7,
            producer_id: 9,
            producer_epoch: 0,
            sequence,
            message_id: sequence as u128 + 1,
            payload: vec![sequence as u8; 12],
        }
    }
    fn publish(&self, r: PublishRequest) -> Result<PublishOutcome, Error> {
        self.actor.as_ref().unwrap().try_publish(r)?.wait()
    }
    fn restart(&mut self) {
        self.actor.take().unwrap().shutdown().unwrap();
        self.actor = Some(
            Partition::open_with_clock(
                &self.path,
                self.wal.clone(),
                self.config.clone(),
                self.clock.clone(),
            )
            .unwrap(),
        );
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        if let Some(p) = self.actor.take() {
            p.shutdown().unwrap();
        }
        std::fs::remove_dir_all(&self.path).unwrap();
    }
}
#[test]
fn exact_window_boundary_expiration_releases_only_outcomes_and_recovers_watermarks() {
    let mut f = Fixture::new(1, 1);
    let first = f.publish(f.request(0)).unwrap();
    f.clock.set(99);
    assert_eq!(f.publish(f.request(0)).unwrap(), first);
    assert!(matches!(f.publish(f.request(1)), Err(Error::Capacity)));
    f.clock.set(100);
    assert!(matches!(
        f.publish(f.request(0)),
        Err(Error::OutcomeUnavailable)
    ));
    assert!(matches!(f.publish(f.request(2)), Err(Error::SequenceGap)));
    let second = f.publish(f.request(1)).unwrap();
    f.restart(); // recovery must stream past expired history even with only one outcome slot
    assert!(matches!(
        f.publish(f.request(0)),
        Err(Error::OutcomeUnavailable)
    ));
    assert_eq!(f.publish(f.request(1)).unwrap(), second);
    f.clock.set(200);
    let mut fresh_producer = f.request(0);
    fresh_producer.producer_id += 1;
    assert!(matches!(f.publish(fresh_producer), Err(Error::Capacity)));
    let mut epoch = f.request(0);
    epoch.producer_epoch = 1;
    f.publish(epoch.clone()).unwrap();
    f.restart();
    assert!(matches!(
        f.publish(f.request(0)),
        Err(Error::ProducerFenced)
    ));
    assert_eq!(
        f.publish(epoch).unwrap(),
        PublishOutcome::CommittedLocal(DurableOffset {
            log_incarnation: 1,
            sequence: 4
        })
    );
}
#[test]
fn restart_downtime_clock_rollback_and_unavailable_evidence_extend_retention() {
    let mut f = Fixture::new(1, 1);
    f.clock.set(1000);
    // Initial clock sample was zero; no committed history existed before the first advance.
    let first = f.publish(f.request(0)).unwrap();
    f.clock.set(1050);
    assert_eq!(f.publish(f.request(0)).unwrap(), first);
    f.clock.set(1); // rollback cannot expire
    assert_eq!(f.publish(f.request(0)).unwrap(), first);
    f.clock.set(u64::MAX); // missing timestamp evidence cannot expire
    assert_eq!(f.publish(f.request(0)).unwrap(), first);
    f.clock.set(1_000_000);
    assert_eq!(f.publish(f.request(0)).unwrap(), first); // re-anchor only
    f.clock.set(2_000_000);
    f.restart(); // downtime is excluded, including arbitrary wall-clock shifts
    assert_eq!(f.publish(f.request(0)).unwrap(), first);
    f.clock.set(2_000_099);
    assert_eq!(f.publish(f.request(0)).unwrap(), first);
    f.clock.set(2_000_100);
    assert!(matches!(
        f.publish(f.request(0)),
        Err(Error::OutcomeUnavailable)
    ));
}
#[test]
fn retry_storm_has_one_durable_append_and_content_conflicts_stay_distinct() {
    let mut f = Fixture::new(1, 1);
    let request = f.request(0);
    let actor = f.actor.as_ref().unwrap();
    std::thread::scope(|s| {
        for _ in 0..16 {
            let request = request.clone();
            s.spawn(move || {
                for _ in 0..32 {
                    loop {
                        match actor.try_publish(request.clone()) {
                            Ok(pending) => {
                                assert_eq!(
                                    pending.wait().unwrap(),
                                    PublishOutcome::CommittedLocal(DurableOffset {
                                        log_incarnation: 1,
                                        sequence: 0
                                    })
                                );
                                break;
                            }
                            Err(Error::Capacity) => std::thread::yield_now(),
                            _ => panic!("unexpected admission failure"),
                        }
                    }
                }
            });
        }
    });
    let mut changed = request.clone();
    changed.message_id += 1;
    assert!(matches!(f.publish(changed), Err(Error::SequenceConflict)));
    let mut changed = request;
    changed.payload[0] ^= 1;
    assert!(matches!(f.publish(changed), Err(Error::SequenceConflict)));
    f.actor.take().unwrap().shutdown().unwrap();
    let wal = Wal::open(&f.path, f.wal.clone()).unwrap();
    assert_eq!(wal.committed_position().unwrap().sequence, 1);
}

#[test]
fn zero_legacy_timestamp_never_expires_without_trustworthy_evidence() {
    let mut f = Fixture::new(1, 1);
    f.actor.take().unwrap().shutdown().unwrap();
    let r = f.request(0);
    let mut wal = Wal::open(&f.path, f.wal.clone()).unwrap();
    let message = rovqet_wal::Record {
        kind: rovqet_wal::RecordKind::Message,
        logical_offset: 0,
        owner_epoch: 1,
        producer_id: r.producer_id,
        producer_epoch: 0,
        sequence: 0,
        message_id: r.message_id,
        server_timestamp: 0,
        payload: r.payload.clone(),
    };
    let mut payload = b"RQPO\0\x01".to_vec();
    payload.extend(r.principal.to_be_bytes());
    payload.extend(0u64.to_be_bytes());
    wal.append(message.clone()).unwrap();
    wal.append(rovqet_wal::Record {
        kind: rovqet_wal::RecordKind::ProducerOutcome,
        logical_offset: 1,
        payload,
        ..message
    })
    .unwrap();
    wal.sync().unwrap();
    drop(wal);
    f.actor = Some(
        Partition::open_with_clock(&f.path, f.wal.clone(), f.config.clone(), f.clock.clone())
            .unwrap(),
    );
    f.clock.set(1_000_000);
    assert!(f.publish(r).is_ok());
    assert!(matches!(f.publish(f.request(1)), Err(Error::Capacity)));
    f.restart();
    assert!(f.publish(f.request(0)).is_ok());
}
