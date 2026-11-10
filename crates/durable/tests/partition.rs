//! Q1 actor acceptance tests use isolated directories and intact local storage.
//! SIGKILL below is a process crash, not a physical power-loss experiment.
use std::fs::{self, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

static NEXT: AtomicU64 = AtomicU64::new(0);
struct Temp(PathBuf);
impl Temp {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "rovqet-q1-actor-test-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&path).unwrap();
        Self(path)
    }
}
impl Drop for Temp {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.0).unwrap();
    }
}

#[cfg(unix)]
struct CrashChild(std::process::Child);
#[cfg(unix)]
impl Drop for CrashChild {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

#[cfg(unix)]
fn receive_commit_then_kill(path: &std::path::Path) -> String {
    let mut child = CrashChild(
        std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "crash_actor_helper",
                "--nocapture",
                "--test-threads=1",
            ])
            .env("ROVQET_Q1_CRASH_PATH", path)
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::inherit())
            .spawn()
            .unwrap(),
    );
    let stdout = child.0.stdout.take().unwrap();
    let (send, receive) = std::sync::mpsc::sync_channel(1);
    let reader = std::thread::spawn(move || {
        for line in BufReader::new(stdout).lines() {
            let line = line.unwrap();
            if let Some((_, ack)) = line.split_once("Q1_ACK ") {
                let _ = send.send(ack.to_string());
                break;
            }
        }
    });
    let ack = receive
        .recv_timeout(std::time::Duration::from_secs(15))
        .expect("child must report actual durable result");
    let mut oracle = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(path.join("oracle.journal"))
        .unwrap();
    writeln!(oracle, "{ack}").unwrap();
    oracle.sync_all().unwrap();
    child.0.kill().unwrap();
    let status = child.0.wait().unwrap();
    use std::os::unix::process::ExitStatusExt;
    assert_eq!(status.signal(), Some(9));
    reader.join().unwrap();
    ack
}

use rovqet_core::{DurableOffset, PartitionRef, PublishOutcome, QoS};
use rovqet_durable::{Error, Partition, PartitionConfig, PublishRequest};
use rovqet_wal::{Config as WalConfig, RecordKind, Wal};

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
        max_segment_bytes: 512,
        max_record_bytes: 256,
        max_pending_bytes: 1024,
        max_pending_records: 8,
        max_segments: 32,
        max_records: 128,
    }
}
fn actor_config() -> PartitionConfig {
    PartitionConfig {
        partition: partition_id(),
        ownership_epoch: 7,
        queue_messages: 8,
        queue_bytes: 8192,
        max_outcomes: 64,
        max_producers: 100_000,
        dedup_window: std::time::Duration::from_secs(86400),
    }
}
fn request(sequence: u64) -> PublishRequest {
    PublishRequest {
        partition: partition_id(),
        qos: QoS::Q1,
        ownership_epoch: 7,
        principal: 10,
        producer_id: 11,
        producer_epoch: 0,
        sequence,
        message_id: 100 + u128::from(sequence),
        payload: format!("event-{sequence}").into_bytes(),
    }
}
fn outcome(sequence: u64) -> PublishOutcome {
    PublishOutcome::CommittedLocal(DurableOffset {
        log_incarnation: 42,
        sequence,
    })
}
fn open(t: &Temp) -> Partition {
    fs::create_dir_all(t.0.join("wal")).unwrap();
    Partition::open(t.0.join("wal"), wal_config(), actor_config()).unwrap()
}
fn publish(p: &Partition, r: PublishRequest) -> Result<PublishOutcome, Error> {
    p.try_publish(r)?.wait()
}

#[test]
fn commit_recovers_message_and_bound_producer_outcome_after_shutdown() {
    let t = Temp::new();
    let p = open(&t);
    for i in 0..5 {
        assert_eq!(publish(&p, request(i)).unwrap(), outcome(i * 2));
    }
    p.shutdown().unwrap();
    let w = Wal::open(t.0.join("wal"), wal_config()).unwrap();
    let records = w
        .read(
            DurableOffset {
                log_incarnation: 42,
                sequence: 0,
            },
            16,
            4096,
        )
        .unwrap();
    assert_eq!(records.len(), 10);
    for (i, pair) in records.chunks_exact(2).enumerate() {
        assert_eq!(pair[0].kind, RecordKind::Message);
        assert_eq!(pair[1].kind, RecordKind::ProducerOutcome);
        assert_eq!(pair[0].payload, request(i as u64).payload);
        assert_eq!(pair[0].producer_id, pair[1].producer_id);
        assert_eq!(&pair[1].payload[..6], b"RQPO\0\x01");
        assert_eq!(
            u128::from_be_bytes(pair[1].payload[6..22].try_into().unwrap()),
            10
        );
        assert_eq!(
            u64::from_be_bytes(pair[1].payload[22..30].try_into().unwrap()),
            (i * 2) as u64
        );
    }
    drop(w);
    let p = open(&t);
    assert_eq!(publish(&p, request(0)).unwrap(), outcome(0));
    assert_eq!(publish(&p, request(4)).unwrap(), outcome(8));
    assert_eq!(publish(&p, request(5)).unwrap(), outcome(10));
    p.shutdown().unwrap();
}

#[test]
fn lost_waiter_preserves_accepted_operation_and_retry_settles_same_outcome() {
    let t = Temp::new();
    let p = open(&t);
    let pending = p.try_publish(request(0)).unwrap();
    drop(pending);
    p.shutdown().unwrap();
    let p = open(&t);
    assert_eq!(publish(&p, request(0)).unwrap(), outcome(0));
    p.shutdown().unwrap();
    let w = Wal::open(t.0.join("wal"), wal_config()).unwrap();
    assert_eq!(
        w.committed_position(),
        Some(DurableOffset {
            log_incarnation: 42,
            sequence: 1
        })
    );
}

#[test]
fn incompatible_class_partition_and_owner_are_rejected_before_storage() {
    let t = Temp::new();
    let p = open(&t);
    for qos in [QoS::Q0, QoS::Q2] {
        let mut r = request(0);
        r.qos = qos;
        assert!(matches!(p.try_publish(r), Err(Error::WrongClass)));
    }
    let mut r = request(0);
    r.partition.topic_id += 1;
    assert!(matches!(p.try_publish(r), Err(Error::WrongPartition)));
    for epoch in [6, 8] {
        let mut r = request(0);
        r.ownership_epoch = epoch;
        assert!(matches!(p.try_publish(r), Err(Error::StaleOwner)));
    }
    p.shutdown().unwrap();
    let w = Wal::open(t.0.join("wal"), wal_config()).unwrap();
    assert_eq!(w.committed_position(), None);
}

#[test]
fn retry_identity_conflicts_gaps_and_producer_fencing_remain_scoped() {
    let t = Temp::new();
    let p = open(&t);
    assert!(matches!(publish(&p, request(1)), Err(Error::SequenceGap)));
    assert_eq!(publish(&p, request(0)).unwrap(), outcome(0));
    assert_eq!(publish(&p, request(0)).unwrap(), outcome(0));
    let mut changed = request(0);
    changed.payload.push(1);
    assert!(matches!(publish(&p, changed), Err(Error::SequenceConflict)));
    let mut changed = request(0);
    changed.message_id += 1;
    assert!(matches!(publish(&p, changed), Err(Error::SequenceConflict)));
    assert!(matches!(publish(&p, request(2)), Err(Error::SequenceGap)));
    let mut renewed = request(0);
    renewed.producer_epoch = 1;
    assert_eq!(publish(&p, renewed.clone()).unwrap(), outcome(2));
    assert!(matches!(
        publish(&p, request(0)),
        Err(Error::ProducerFenced)
    ));
    let mut independent = request(0);
    independent.principal += 1;
    assert_eq!(publish(&p, independent.clone()).unwrap(), outcome(4));
    p.shutdown().unwrap();
    let p = open(&t);
    assert_eq!(publish(&p, renewed).unwrap(), outcome(2));
    assert_eq!(publish(&p, independent).unwrap(), outcome(4));
    assert!(matches!(
        publish(&p, request(0)),
        Err(Error::ProducerFenced)
    ));
    p.shutdown().unwrap();
}

#[test]
fn outcome_quota_fails_closed_without_eviction_and_retry_still_succeeds() {
    let t = Temp::new();
    fs::create_dir(t.0.join("wal")).unwrap();
    let mut cfg = actor_config();
    cfg.max_outcomes = 1;
    let p = Partition::open(t.0.join("wal"), wal_config(), cfg.clone()).unwrap();
    assert_eq!(publish(&p, request(0)).unwrap(), outcome(0));
    assert!(matches!(publish(&p, request(1)), Err(Error::Capacity)));
    assert_eq!(publish(&p, request(0)).unwrap(), outcome(0));
    p.shutdown().unwrap();
    let p = Partition::open(t.0.join("wal"), wal_config(), cfg).unwrap();
    assert_eq!(publish(&p, request(0)).unwrap(), outcome(0));
    p.shutdown().unwrap();
}

#[test]
fn queue_byte_budget_rejects_before_append_and_releases_after_result() {
    let t = Temp::new();
    fs::create_dir(t.0.join("wal")).unwrap();
    let mut cfg = actor_config();
    cfg.queue_bytes = 1;
    let p = Partition::open(t.0.join("wal"), wal_config(), cfg).unwrap();
    assert!(matches!(p.try_publish(request(0)), Err(Error::Capacity)));
    assert_eq!(p.usage().messages, 0);
    assert_eq!(p.usage().bytes, 0);
    p.shutdown().unwrap();
    let w = Wal::open(t.0.join("wal"), wal_config()).unwrap();
    assert_eq!(w.committed_position(), None);
}

#[test]
fn old_owner_cannot_reopen_newer_committed_epoch() {
    let t = Temp::new();
    let p = open(&t);
    assert_eq!(publish(&p, request(0)).unwrap(), outcome(0));
    p.shutdown().unwrap();
    let mut cfg = actor_config();
    cfg.ownership_epoch = 8;
    let p = Partition::open(t.0.join("wal"), wal_config(), cfg).unwrap();
    assert!(matches!(p.try_publish(request(1)), Err(Error::StaleOwner)));
    let mut r = request(1);
    r.ownership_epoch = 8;
    assert_eq!(publish(&p, r).unwrap(), outcome(2));
    p.shutdown().unwrap();
    assert!(matches!(
        Partition::open(t.0.join("wal"), wal_config(), actor_config()),
        Err(Error::StaleOwner)
    ));
}

#[test]
fn invalid_actor_configuration_does_not_create_history() {
    let t = Temp::new();
    fs::create_dir(t.0.join("wal")).unwrap();
    for case in 0..4 {
        let mut cfg = actor_config();
        match case {
            0 => cfg.partition.tenant_id += 1,
            1 => cfg.queue_messages = 0,
            2 => cfg.queue_bytes = 0,
            _ => cfg.max_outcomes = 0,
        }
        assert!(matches!(
            Partition::open(t.0.join("wal"), wal_config(), cfg),
            Err(Error::InvalidConfig)
        ));
    }
    assert!(!t.0.join("wal/MANIFEST").exists());
}

#[test]
fn crash_actor_helper() {
    let Some(path) = std::env::var_os("ROVQET_Q1_CRASH_PATH") else {
        return;
    };
    let path = PathBuf::from(path);
    fs::create_dir(path.join("wal")).unwrap();
    let p = Partition::open(path.join("wal"), wal_config(), actor_config()).unwrap();
    let actual = publish(&p, request(0)).unwrap();
    assert_eq!(actual, outcome(0));
    println!("Q1_ACK 42 0");
    std::io::stdout().flush().unwrap();
    // Parent has an independent ACK journal and sends SIGKILL while the actor remains alive.
    loop {
        std::thread::park();
    }
}

#[test]
#[cfg(unix)]
fn acknowledged_publication_recovers_after_sigkill_against_independent_journal() {
    let t = Temp::new();
    assert_eq!(receive_commit_then_kill(&t.0), "42 0");
    assert_eq!(
        fs::read_to_string(t.0.join("oracle.journal")).unwrap(),
        "42 0\n"
    );
    let p = Partition::open(t.0.join("wal"), wal_config(), actor_config()).unwrap();
    assert_eq!(publish(&p, request(0)).unwrap(), outcome(0));
    p.shutdown().unwrap();
    let w = Wal::open(t.0.join("wal"), wal_config()).unwrap();
    let rows = w
        .read(
            DurableOffset {
                log_incarnation: 42,
                sequence: 0,
            },
            8,
            4096,
        )
        .unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].payload, request(0).payload);
    assert_eq!(rows[0].kind, RecordKind::Message);
    assert_eq!(rows[1].kind, RecordKind::ProducerOutcome);
}

#[test]
fn malformed_committed_publication_pairs_fail_recovery_without_discarding_history() {
    for malformed in 0..4 {
        let t = Temp::new();
        fs::create_dir(t.0.join("wal")).unwrap();
        let mut w = Wal::open(t.0.join("wal"), wal_config()).unwrap();
        let message = rovqet_wal::Record {
            kind: RecordKind::Message,
            logical_offset: 0,
            owner_epoch: 7,
            producer_id: 11,
            producer_epoch: 0,
            sequence: 0,
            message_id: 100,
            server_timestamp: 1,
            payload: b"event-0".to_vec(),
        };
        w.append(message.clone()).unwrap();
        if malformed != 0 {
            let mut outcome = message.clone();
            outcome.kind = RecordKind::ProducerOutcome;
            outcome.logical_offset = 1;
            outcome.payload = b"RQPO\0\x01".to_vec();
            outcome.payload.extend(10_u128.to_be_bytes());
            outcome.payload.extend(0_u64.to_be_bytes());
            match malformed {
                1 => outcome.message_id += 1,
                2 => outcome.payload[0] = b'X',
                3 => outcome.payload[29] = 99,
                _ => unreachable!(),
            }
            w.append(outcome).unwrap();
        }
        let committed = w.sync().unwrap();
        drop(w);
        assert!(matches!(
            Partition::open(t.0.join("wal"), wal_config(), actor_config()),
            Err(Error::Corruption(_))
        ));
        let w = Wal::open(t.0.join("wal"), wal_config()).unwrap();
        assert_eq!(w.committed_position(), committed);
    }
}
