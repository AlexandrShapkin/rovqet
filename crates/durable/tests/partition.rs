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
    let batch = p.try_replay(replay_request(0)).unwrap().wait().unwrap();
    assert_eq!(batch.messages.len(), 1);
    assert_eq!(
        batch.messages[0].offset,
        DurableOffset {
            log_incarnation: 42,
            sequence: 0
        }
    );
    assert_eq!(batch.messages[0].message_id, request(0).message_id);
    assert_eq!(batch.messages[0].payload, request(0).payload);
    assert!(batch.end_of_available_history);
    drop(batch);
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

fn replay_request(offset: u64) -> rovqet_durable::ReplayRequest {
    rovqet_durable::ReplayRequest {
        partition: partition_id(),
        from: DurableOffset {
            log_incarnation: 42,
            sequence: offset,
        },
        max_messages: 8,
        max_bytes: 2048,
        wait: std::time::Duration::ZERO,
    }
}
#[test]
fn replay_clean_restart_crosses_segments_counts_only_messages_and_keeps_sparse_offsets() {
    let t = Temp::new();
    let p = open(&t);
    for i in 0..7 {
        publish(&p, request(i)).unwrap();
    }
    p.shutdown().unwrap();
    assert!(
        fs::read_dir(t.0.join("wal"))
            .unwrap()
            .filter_map(Result::ok)
            .filter(|e| e.file_name().to_string_lossy().starts_with("segment-"))
            .count()
            > 1
    );
    let p = Partition::open_existing(t.0.join("wal"), wal_config(), actor_config()).unwrap();
    let mut cursor = 0;
    let mut seen = Vec::new();
    loop {
        let mut r = replay_request(cursor);
        r.max_messages = 2;
        let batch = p.try_replay(r).unwrap().wait().unwrap();
        assert!(batch.messages.len() <= 2);
        assert!(batch.response_bytes <= 2048);
        for message in &batch.messages {
            assert_eq!(message.offset.log_incarnation, 42);
            assert_eq!(message.offset.sequence, message.sequence * 2);
            assert_eq!(message.payload, request(message.sequence).payload);
            seen.push(message.message_id);
        }
        cursor = batch.next.sequence;
        if batch.end_of_available_history {
            break;
        }
    }
    assert_eq!(cursor, 14);
    assert_eq!(
        seen,
        (0..7).map(|i| request(i).message_id).collect::<Vec<_>>()
    );
    let end = p
        .try_replay(replay_request(cursor))
        .unwrap()
        .wait()
        .unwrap();
    assert!(end.messages.is_empty() && end.end_of_available_history);
    assert_eq!(end.next.sequence, 14);
}
#[test]
fn replay_empty_history_invalid_offsets_incarnation_partition_and_limits_are_typed() {
    use rovqet_durable::ReplayError;
    let t = Temp::new();
    let p = open(&t);
    let batch = p.try_replay(replay_request(0)).unwrap().wait().unwrap();
    assert!(batch.messages.is_empty() && batch.end_of_available_history);
    assert_eq!(batch.next.sequence, 0);
    drop(batch);
    for offset in [1, 2, u64::MAX] {
        assert!(matches!(
            p.try_replay(replay_request(offset)).unwrap().wait(),
            Err(ReplayError::ReplayNotAvailable)
        ));
    }
    let mut r = replay_request(0);
    r.from.log_incarnation = 41;
    assert!(matches!(
        p.try_replay(r).unwrap().wait(),
        Err(ReplayError::IncarnationMismatch)
    ));
    let mut r = replay_request(0);
    r.partition.partition_id += 1;
    assert!(matches!(p.try_replay(r), Err(ReplayError::WrongPartition)));
    for (messages, bytes, wait) in [
        (0, 2048, 0),
        (1025, 2048, 0),
        (8, 0, 0),
        (8, 4 * 1024 * 1024 + 1, 0),
        (8, 2048, 30001),
    ] {
        let mut r = replay_request(0);
        r.max_messages = messages;
        r.max_bytes = bytes;
        r.wait = std::time::Duration::from_millis(wait);
        assert!(matches!(p.try_replay(r), Err(ReplayError::InvalidLimits)));
    }
    publish(&p, request(0)).unwrap();
    assert!(matches!(
        p.try_replay(replay_request(1)).unwrap().wait(),
        Err(ReplayError::ReplayNotAvailable)
    ));
}
#[test]
fn replay_byte_limits_are_exact_and_oversize_first_message_is_not_a_false_empty_end() {
    use rovqet_durable::{REPLAY_BATCH_BYTES, REPLAY_MESSAGE_BYTES, ReplayError};
    let t = Temp::new();
    let p = open(&t);
    for i in 0..3 {
        publish(&p, request(i)).unwrap();
    }
    let bytes = REPLAY_BATCH_BYTES + REPLAY_MESSAGE_BYTES + request(0).payload.len();
    let mut r = replay_request(0);
    r.max_bytes = bytes;
    let batch = p.try_replay(r.clone()).unwrap().wait().unwrap();
    assert_eq!(batch.messages.len(), 1);
    assert_eq!(batch.response_bytes, bytes);
    assert_eq!(batch.next.sequence, 2);
    assert!(!batch.end_of_available_history);
    drop(batch);
    r.max_bytes -= 1;
    assert!(
        matches!(p.try_replay(r).unwrap().wait(), Err(ReplayError::ResponseTooSmall { required_bytes }) if required_bytes == bytes)
    );
}
#[test]
fn waiting_replay_yields_to_publication_shutdown_and_cancellation_without_leases() {
    let t = Temp::new();
    let p = open(&t);
    let mut r = replay_request(0);
    r.wait = std::time::Duration::from_secs(30);
    let pending = p.try_replay(r.clone()).unwrap();
    publish(&p, request(0)).unwrap(); // a waiting read never blocks the publisher
    let batch = pending.wait().unwrap();
    assert_eq!(batch.messages[0].message_id, request(0).message_id);
    assert!(batch.end_of_available_history);
    drop(batch);
    r.from.sequence = 2;
    let cancelled = p.try_replay(r.clone()).unwrap();
    drop(cancelled);
    // A subsequent actor reply fences the cancelled request's queue traversal.
    drop(p.try_replay(replay_request(2)).unwrap().wait().unwrap());
    let waiting = p.try_replay(r).unwrap();
    p.shutdown().unwrap(); // no 30-second wait on shutdown
    let batch = waiting.wait().unwrap();
    assert!(batch.messages.is_empty() && batch.end_of_available_history);
}
#[test]
fn replay_wait_deadline_returns_explicit_empty_available_end() {
    let t = Temp::new();
    let p = open(&t);
    let mut r = replay_request(0);
    r.wait = std::time::Duration::from_millis(1);
    let batch = p.try_replay(r).unwrap().wait().unwrap();
    assert!(batch.messages.is_empty() && batch.end_of_available_history);
}
#[test]
fn replay_batch_and_cancelled_waiter_retain_and_release_explicit_memory_quota() {
    use rovqet_durable::ReplayError;
    let t = Temp::new();
    fs::create_dir(t.0.join("wal")).unwrap();
    let mut config = actor_config();
    config.queue_messages = 1;
    let p = Partition::open(t.0.join("wal"), wal_config(), config).unwrap();
    let batch = p.try_replay(replay_request(0)).unwrap().wait().unwrap();
    assert_eq!(p.usage().messages, 1);
    assert!(p.usage().bytes >= 2048);
    assert!(matches!(
        p.try_replay(replay_request(0)),
        Err(ReplayError::Capacity)
    ));
    assert!(matches!(p.try_publish(request(0)), Err(Error::Capacity)));
    drop(batch);
    assert_eq!(p.usage().messages, 0);
    let mut too_big = replay_request(0);
    too_big.max_bytes = 8192;
    assert!(matches!(p.try_replay(too_big), Err(ReplayError::Capacity)));
    publish(&p, request(0)).unwrap();
}
#[test]
fn replay_live_committed_corruption_poisoning_never_returns_partial_history() {
    use rovqet_durable::ReplayError;
    for internal in [false, true] {
        let t = Temp::new();
        let p = open(&t);
        publish(&p, request(0)).unwrap();
        let path = fs::read_dir(t.0.join("wal"))
            .unwrap()
            .filter_map(Result::ok)
            .find(|e| e.file_name().to_string_lossy().starts_with("segment-"))
            .unwrap()
            .path();
        let mut bytes = fs::read(&path).unwrap();
        let at = rovqet_wal::HEADER_BYTES
            + if internal {
                rovqet_wal::RECORD_OVERHEAD + request(0).payload.len()
            } else {
                0
            };
        bytes[at + 8] ^= 1;
        fs::write(&path, &bytes).unwrap();
        assert!(matches!(
            p.try_replay(replay_request(0)).unwrap().wait(),
            Err(ReplayError::Storage(_))
        ));
        assert!(matches!(
            p.try_replay(replay_request(0)).unwrap().wait(),
            Err(ReplayError::Unavailable)
        ));
        assert!(matches!(publish(&p, request(1)), Err(Error::Unavailable)));
        p.shutdown().unwrap();
        assert!(Partition::open_existing(t.0.join("wal"), wal_config(), actor_config()).is_err());
        assert_eq!(fs::read(path).unwrap(), bytes);
    }
}
#[test]
fn replay_discards_uncommitted_incomplete_tail_and_missing_history_is_never_initialized() {
    let t = Temp::new();
    let p = open(&t);
    publish(&p, request(0)).unwrap();
    p.shutdown().unwrap();
    let mut wal = Wal::open(t.0.join("wal"), wal_config()).unwrap();
    wal.append(rovqet_wal::Record {
        kind: RecordKind::Message,
        logical_offset: 2,
        owner_epoch: 7,
        producer_id: 11,
        producer_epoch: 0,
        sequence: 1,
        message_id: 101,
        server_timestamp: 1,
        payload: b"uncommitted".to_vec(),
    })
    .unwrap();
    drop(wal);
    let p = Partition::open_existing(t.0.join("wal"), wal_config(), actor_config()).unwrap();
    let batch = p.try_replay(replay_request(0)).unwrap().wait().unwrap();
    assert_eq!(batch.messages.len(), 1);
    assert_eq!(batch.next.sequence, 2);
    drop(batch);
    p.shutdown().unwrap();
    // Disposable loss fixture: move history aside; no broker reset/delete operation exists.
    fs::rename(t.0.join("wal"), t.0.join("lost-wal")).unwrap();
    assert!(Partition::open_existing(t.0.join("wal"), wal_config(), actor_config()).is_err());
    fs::create_dir(t.0.join("wal")).unwrap();
    assert!(Partition::open_existing(t.0.join("wal"), wal_config(), actor_config()).is_err());
    assert_eq!(fs::read_dir(t.0.join("wal")).unwrap().count(), 0);
    fs::rename(t.0.join("wal"), t.0.join("empty")).unwrap();
    fs::rename(t.0.join("lost-wal"), t.0.join("wal")).unwrap();
    let mut wrong = wal_config();
    wrong.incarnation += 1;
    assert!(Partition::open_existing(t.0.join("wal"), wrong, actor_config()).is_err());
}

#[test]
fn fresh_disposable_incarnation_never_accepts_an_old_replay_reference() {
    use rovqet_durable::ReplayError;
    let t = Temp::new();
    let p = open(&t);
    publish(&p, request(0)).unwrap();
    p.shutdown().unwrap();
    // Explicit test authorization: independent disposable directory and fresh incarnation.
    let fresh = t.0.join("independent-new-incarnation");
    fs::create_dir(&fresh).unwrap();
    let mut wal = wal_config();
    wal.incarnation = 43;
    let p = Partition::open(fresh, wal, actor_config()).unwrap();
    assert!(matches!(
        p.try_replay(replay_request(0)).unwrap().wait(),
        Err(ReplayError::IncarnationMismatch)
    ));
    let mut r = request(0);
    r.producer_id = 99;
    publish(&p, r).unwrap();
    let mut r = replay_request(0);
    r.from.log_incarnation = 43;
    let batch = p.try_replay(r).unwrap().wait().unwrap();
    assert_eq!(batch.messages[0].offset.log_incarnation, 43);
    assert_eq!(batch.messages[0].offset.sequence, 0);
    assert!(
        std::mem::size_of::<rovqet_durable::ReplayMessage>()
            <= rovqet_durable::REPLAY_MESSAGE_BYTES
    );
    assert!(
        std::mem::size_of::<rovqet_durable::ReplayBatch>() <= rovqet_durable::REPLAY_BATCH_BYTES
    );
}

#[tokio::test(flavor = "current_thread")]
async fn asynchronous_replay_wait_yields_and_completes_after_concurrent_commit() {
    let t = Temp::new();
    let p = open(&t);
    let mut r = replay_request(0);
    r.wait = std::time::Duration::from_secs(30);
    let read = p.try_replay(r).unwrap();
    let publication = p.try_publish(request(0)).unwrap();
    assert_eq!(publication.wait_async().await.unwrap(), outcome(0));
    let batch = read.wait_async().await.unwrap();
    assert_eq!(batch.messages.len(), 1);
    drop(batch);
    // Blocking shutdown is dispatched away from this single runtime worker.
    tokio::task::spawn_blocking(move || p.shutdown().unwrap())
        .await
        .unwrap();
}

#[cfg(feature = "fault-injection")]
#[test]
fn replay_queued_during_unsynced_append_cannot_expose_it_and_cancellation_releases_budget() {
    use rovqet_durable::{TestControl, TestStage};
    use std::time::{Duration, Instant};
    let t = Temp::new();
    fs::create_dir(t.0.join("wal")).unwrap();
    let controls = t.0.join("controls");
    fs::create_dir(&controls).unwrap();
    let p = Partition::open_with_test_control(
        t.0.join("wal"),
        wal_config(),
        actor_config(),
        TestControl {
            directory: controls.clone(),
            stage: Some(TestStage::AfterAppendBeforeSync),
            sequence: 0,
            wal_fault: None,
        },
    )
    .unwrap();
    let publication = p.try_publish(request(0)).unwrap();
    let deadline = Instant::now() + Duration::from_secs(5);
    while !controls.join("reached-after-append-before-sync").exists() {
        assert!(Instant::now() < deadline);
        std::thread::yield_now();
    }
    let read = p.try_replay(replay_request(0)).unwrap();
    let cancelled = p.try_replay(replay_request(0)).unwrap();
    drop(cancelled);
    std::thread::scope(|scope| {
        let (send, receive) = std::sync::mpsc::sync_channel(1);
        let reader = scope.spawn(move || {
            send.send(read.wait()).unwrap();
        });
        assert!(matches!(
            receive.recv_timeout(Duration::from_millis(20)),
            Err(std::sync::mpsc::RecvTimeoutError::Timeout)
        ));
        fs::write(
            controls.join("release-after-append-before-sync"),
            b"release",
        )
        .unwrap();
        assert_eq!(publication.wait().unwrap(), outcome(0));
        let batch = receive
            .recv_timeout(Duration::from_secs(5))
            .unwrap()
            .unwrap();
        assert_eq!(batch.messages.len(), 1);
        assert_eq!(batch.messages[0].payload, request(0).payload);
        drop(batch);
        reader.join().unwrap();
    });
    // Later command completion fences cancellation cleanup as well as the successful read.
    drop(p.try_replay(replay_request(2)).unwrap().wait().unwrap());
    assert_eq!(p.usage().messages, 0);
    p.shutdown().unwrap();
}
