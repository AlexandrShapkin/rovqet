//! Filesystem/process-crash tests. These do not simulate physical power loss.
use std::fs::{self, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use rovqet_core::{DurableOffset, PartitionRef};
use rovqet_wal::{Config, Error, FaultPoint, IoFailureKind, Record, RecordKind, Wal};

static NEXT: AtomicU64 = AtomicU64::new(0);
struct Temp(PathBuf);
impl Temp {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "rovqet-wal-test-{}-{}",
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
fn config() -> Config {
    Config {
        cluster_id: 1,
        node_id: 2,
        partition: PartitionRef {
            tenant_id: 3,
            topic_id: 4,
            partition_id: 5,
        },
        incarnation: 42,
        max_segment_bytes: 512,
        max_record_bytes: 256,
        max_pending_bytes: 1024,
        max_pending_records: 8,
        max_segments: 64,
        max_records: 1024,
    }
}
fn record(offset: u64) -> Record {
    Record {
        kind: RecordKind::Message,
        logical_offset: offset,
        owner_epoch: 7,
        producer_id: 8,
        producer_epoch: 9,
        sequence: offset,
        message_id: 10 + u128::from(offset),
        server_timestamp: 11,
        payload: vec![u8::try_from(offset).unwrap(); 32],
    }
}
fn offset(sequence: u64) -> DurableOffset {
    DurableOffset {
        log_incarnation: 42,
        sequence,
    }
}
fn segments(path: &Path) -> Vec<PathBuf> {
    let mut paths: Vec<_> = fs::read_dir(path)
        .unwrap()
        .map(|e| e.unwrap().path())
        .filter(|p| p.extension().is_some_and(|e| e == "wal"))
        .collect();
    paths.sort();
    paths
}
fn flip(path: &Path, at: u64) {
    let mut f = OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .unwrap();
    f.seek(SeekFrom::Start(at)).unwrap();
    let mut b = [0];
    f.read_exact(&mut b).unwrap();
    f.seek(SeekFrom::Start(at)).unwrap();
    b[0] ^= 0x80;
    f.write_all(&b).unwrap();
    f.sync_all().unwrap();
}
fn tail(path: &Path, bytes: &[u8]) {
    let mut f = OpenOptions::new().append(true).open(path).unwrap();
    f.write_all(bytes).unwrap();
    f.sync_all().unwrap();
}
fn read_all(wal: &mut Wal) -> Vec<Record> {
    wal.read(offset(0), 1024, 1024 * 1024).unwrap()
}

#[test]
fn append_is_not_commit_and_reopen_recovers_commit() {
    let t = Temp::new();
    let mut w = Wal::open(&t.0, config()).unwrap();
    w.append(record(0)).unwrap();
    assert_eq!(w.committed_position(), None);
    assert!(w.read(offset(0), 1, 1024).is_err());
    assert_eq!(w.sync().unwrap(), Some(offset(0)));
    drop(w);
    let mut w = Wal::open(&t.0, config()).unwrap();
    assert_eq!(w.committed_position(), Some(offset(0)));
    assert_eq!(read_all(&mut w), vec![record(0)]);
}

#[test]
fn ordered_group_commit_rotation_and_reopen() {
    let t = Temp::new();
    let mut w = Wal::open(&t.0, config()).unwrap();
    for n in 0..8 {
        w.append(record(n)).unwrap();
    }
    assert_eq!(w.sync().unwrap(), Some(offset(7)));
    assert!(segments(&t.0).len() > 1);
    drop(w);
    let mut w = Wal::open(&t.0, config()).unwrap();
    assert_eq!(read_all(&mut w), (0..8).map(record).collect::<Vec<_>>());
    w.append(record(8)).unwrap();
    w.sync().unwrap();
    drop(w);
    let mut w = Wal::open(&t.0, config()).unwrap();
    assert_eq!(read_all(&mut w), (0..9).map(record).collect::<Vec<_>>());
}

#[test]
fn read_crosses_segments_and_obeys_limits() {
    let t = Temp::new();
    let mut w = Wal::open(&t.0, config()).unwrap();
    for n in 0..8 {
        w.append(record(n)).unwrap();
    }
    w.sync().unwrap();
    assert_eq!(
        w.read(offset(1), 5, 10000).unwrap(),
        (1..6).map(record).collect::<Vec<_>>()
    );
    assert_eq!(w.read(offset(1), 1, 10000).unwrap(), vec![record(1)]);
    assert!(w.read(offset(100), 1, 10000).is_err());
    assert!(
        w.read(
            DurableOffset {
                log_incarnation: 99,
                sequence: 0
            },
            1,
            10000
        )
        .is_err()
    );
}

#[test]
fn wrong_incarnation_and_partition_fail_without_mutation() {
    let t = Temp::new();
    let mut w = Wal::open(&t.0, config()).unwrap();
    w.append(record(0)).unwrap();
    w.sync().unwrap();
    drop(w);
    let before = fs::read(t.0.join("MANIFEST")).unwrap();
    let mut wrong = config();
    wrong.incarnation += 1;
    assert!(matches!(
        Wal::open(&t.0, wrong),
        Err(Error::IncarnationMismatch)
    ));
    let mut wrong = config();
    wrong.partition.topic_id += 1;
    assert!(matches!(
        Wal::open(&t.0, wrong),
        Err(Error::IdentityMismatch)
    ));
    assert_eq!(fs::read(t.0.join("MANIFEST")).unwrap(), before);
}

#[test]
fn partial_header_and_partial_payload_after_boundary_are_truncated() {
    for torn in [vec![0, 0], vec![0, 0, 0, 120, 1, 1, 0, 0, 9, 8, 7, 6]] {
        let t = Temp::new();
        let mut w = Wal::open(&t.0, config()).unwrap();
        w.append(record(0)).unwrap();
        w.sync().unwrap();
        drop(w);
        let path = segments(&t.0).pop().unwrap();
        let len = fs::metadata(&path).unwrap().len();
        tail(&path, &torn);
        let mut w = Wal::open(&t.0, config()).unwrap();
        assert_eq!(read_all(&mut w), vec![record(0)]);
        assert_eq!(fs::metadata(path).unwrap().len(), len);
        w.append(record(1)).unwrap();
        w.sync().unwrap();
    }
}

#[test]
fn invalid_checksum_in_committed_record_is_fatal() {
    let t = Temp::new();
    let mut w = Wal::open(&t.0, config()).unwrap();
    w.append(record(0)).unwrap();
    w.sync().unwrap();
    drop(w);
    let path = segments(&t.0).pop().unwrap();
    let len = fs::metadata(&path).unwrap().len();
    flip(&path, len - 1);
    assert!(matches!(
        Wal::open(&t.0, config()),
        Err(Error::Corruption(_))
    ));
    assert_eq!(fs::metadata(path).unwrap().len(), len);
}

#[test]
fn missing_and_truncated_committed_history_are_fatal() {
    for remove in [false, true] {
        let t = Temp::new();
        let mut w = Wal::open(&t.0, config()).unwrap();
        w.append(record(0)).unwrap();
        w.sync().unwrap();
        drop(w);
        let path = segments(&t.0).pop().unwrap();
        if remove {
            fs::remove_file(path).unwrap();
        } else {
            let f = OpenOptions::new().write(true).open(path).unwrap();
            f.set_len(20).unwrap();
        }
        assert!(Wal::open(&t.0, config()).is_err());
    }
}

#[test]
fn checksum_corruption_in_uncommitted_tail_does_not_discard_committed_prefix() {
    let t = Temp::new();
    let mut c = config();
    c.max_segment_bytes = 4096;
    let mut w = Wal::open(&t.0, c.clone()).unwrap();
    w.append(record(0)).unwrap();
    w.sync().unwrap();
    w.append(record(1)).unwrap();
    drop(w);
    let path = segments(&t.0).pop().unwrap();
    let len = fs::metadata(&path).unwrap().len();
    flip(&path, len - 1);
    let mut w = Wal::open(&t.0, c).unwrap();
    assert_eq!(read_all(&mut w), vec![record(0)]);
    w.append(record(1)).unwrap();
    w.sync().unwrap();
}

#[test]
fn manifest_checksum_and_segment_header_corruption_fail_closed() {
    for manifest in [true, false] {
        let t = Temp::new();
        let mut w = Wal::open(&t.0, config()).unwrap();
        w.append(record(0)).unwrap();
        w.sync().unwrap();
        drop(w);
        let path = if manifest {
            t.0.join("MANIFEST")
        } else {
            segments(&t.0).remove(0)
        };
        flip(&path, 0);
        assert!(Wal::open(&t.0, config()).is_err());
    }
}

#[test]
fn interrupted_manifest_temporary_file_is_not_promoted() {
    let t = Temp::new();
    let mut w = Wal::open(&t.0, config()).unwrap();
    w.append(record(0)).unwrap();
    w.sync().unwrap();
    drop(w);
    fs::write(t.0.join("MANIFEST.tmp"), b"partial replacement").unwrap();
    let mut w = Wal::open(&t.0, config()).unwrap();
    assert_eq!(read_all(&mut w), vec![record(0)]);
    w.append(record(1)).unwrap();
    w.sync().unwrap();
    drop(w);
    let mut w = Wal::open(&t.0, config()).unwrap();
    assert_eq!(read_all(&mut w), vec![record(0), record(1)]);
}

#[test]
fn append_io_failure_poisoned_writer_never_reports_commit() {
    let t = Temp::new();
    let mut w = Wal::open(&t.0, config()).unwrap();
    w.inject_fault(FaultPoint::Append);
    assert!(matches!(
        w.append(record(0)),
        Err(Error::Io {
            kind: IoFailureKind::Write,
            ..
        })
    ));
    assert!(matches!(w.sync(), Err(Error::Poisoned)));
    assert!(matches!(w.append(record(0)), Err(Error::Poisoned)));
    assert_eq!(w.committed_position(), None);
    drop(w);
    let w = Wal::open(&t.0, config()).unwrap();
    assert_eq!(w.committed_position(), None);
}

#[test]
fn failed_or_uncertain_sync_has_no_success_outcome() {
    for point in [
        FaultPoint::SegmentSync,
        FaultPoint::ManifestWrite,
        FaultPoint::ManifestSync,
        FaultPoint::ManifestRename,
        FaultPoint::DirectorySync,
    ] {
        let t = Temp::new();
        let mut w = Wal::open(&t.0, config()).unwrap();
        w.append(record(0)).unwrap();
        w.sync().unwrap();
        w.append(record(1)).unwrap();
        w.inject_fault(point);
        let expected = if point == FaultPoint::ManifestWrite {
            IoFailureKind::Write
        } else {
            IoFailureKind::Sync
        };
        assert!(
            matches!(w.sync(), Err(Error::Io {kind, ..}) if kind == expected),
            "{point:?}"
        );
        assert_eq!(w.committed_position(), Some(offset(0)), "{point:?}");
        assert!(matches!(w.sync(), Err(Error::Poisoned)));
        assert!(matches!(w.append(record(2)), Err(Error::Poisoned)));
        drop(w);
        let mut w = Wal::open(&t.0, config()).unwrap();
        let recovered = read_all(&mut w);
        assert!(recovered == vec![record(0)] || recovered == vec![record(0), record(1)]);
        // An uncertain barrier may have published; it cannot lose the earlier successful barrier.
    }
}

#[test]
fn queue_count_and_byte_exhaustion_are_bounded() {
    for by_bytes in [false, true] {
        let t = Temp::new();
        let mut c = config();
        if by_bytes {
            c.max_pending_bytes = 160;
            c.max_record_bytes = 160;
        } else {
            c.max_pending_records = 1;
        }
        let mut w = Wal::open(&t.0, c).unwrap();
        w.enqueue(record(0)).unwrap();
        let used = w.pending_bytes();
        assert!(matches!(w.enqueue(record(1)), Err(Error::Capacity)));
        assert_eq!(w.pending_bytes(), used);
        w.flush().unwrap();
        w.sync().unwrap();
        assert_eq!(read_all(&mut w), vec![record(0)]);
    }
}

#[test]
fn cancellation_only_cancels_pending_memory() {
    let t = Temp::new();
    let mut w = Wal::open(&t.0, config()).unwrap();
    w.enqueue(record(0)).unwrap();
    assert!(w.pending_bytes() > 0);
    w.cancel_pending();
    assert_eq!(w.pending_bytes(), 0);
    assert_eq!(w.sync().unwrap(), None);
    w.enqueue(record(0)).unwrap();
    w.flush().unwrap();
    w.enqueue(record(1)).unwrap();
    w.cancel_pending();
    assert_eq!(w.sync().unwrap(), Some(offset(0)));
    assert_eq!(read_all(&mut w), vec![record(0)]);
}

#[test]
fn segment_and_index_capacity_reject_without_losing_commits() {
    for segments_only in [false, true] {
        let t = Temp::new();
        let mut c = config();
        if segments_only {
            c.max_segments = 1;
        } else {
            c.max_records = 1;
        }
        let mut w = Wal::open(&t.0, c).unwrap();
        w.append(record(0)).unwrap();
        w.sync().unwrap();
        let mut rejected = false;
        for n in 1..8 {
            if w.append(record(n)).is_err() {
                rejected = true;
                break;
            }
        }
        assert!(rejected);
        drop(w);
        let mut w = Wal::open(&t.0, config()).unwrap();
        assert_eq!(read_all(&mut w), vec![record(0)]);
    }
}

#[test]
fn drop_closes_descriptors_for_wal_files() {
    let t = Temp::new();
    let mut w = Wal::open(&t.0, config()).unwrap();
    for n in 0..8 {
        w.append(record(n)).unwrap();
    }
    w.sync().unwrap();
    drop(w);
    #[cfg(target_os = "linux")]
    for entry in fs::read_dir("/proc/self/fd").unwrap() {
        if let Ok(target) = fs::read_link(entry.unwrap().path()) {
            assert!(!target.starts_with(&t.0), "leaked {target:?}");
        }
    }
    let mut w = Wal::open(&t.0, config()).unwrap();
    assert_eq!(read_all(&mut w).len(), 8);
}

#[test]
fn subprocess_crash_helper() {
    let Some(path) = std::env::var_os("ROVQET_WAL_CRASH_PATH") else {
        return;
    };
    let mut w = Wal::open(PathBuf::from(path), config()).unwrap();
    for n in 0..8 {
        w.append(record(n)).unwrap();
    }
    w.sync().unwrap();
    w.append(record(8)).unwrap();
    // Exit skips Rust destructors: process-crash recovery, not a physical power cut.
    std::process::exit(0);
}

#[test]
fn completed_barrier_survives_process_exit_without_destructors() {
    let t = Temp::new();
    let result = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "subprocess_crash_helper", "--nocapture"])
        .env("ROVQET_WAL_CRASH_PATH", &t.0)
        .status()
        .unwrap();
    assert!(result.success());
    let mut w = Wal::open(&t.0, config()).unwrap();
    assert_eq!(w.committed_position(), Some(offset(7)));
    assert_eq!(read_all(&mut w), (0..8).map(record).collect::<Vec<_>>());
}

// Independent table-based checksum oracle for constructing structurally invalid,
// checksum-valid fixtures. The encoder fixture below is a literal byte vector.
fn fixture_crc(bytes: &[u8]) -> u32 {
    let table: [u32; 256] = std::array::from_fn(|i| {
        (0..8).fold(i as u32, |v, _| {
            if v & 1 != 0 {
                (v >> 1) ^ 0x82f63b78
            } else {
                v >> 1
            }
        })
    });
    !bytes.iter().fold(!0_u32, |crc, byte| {
        (crc >> 8) ^ table[((crc ^ u32::from(*byte)) & 255) as usize]
    })
}
fn put_crc(bytes: &mut [u8]) {
    let last = bytes.len() - 4;
    let crc = fixture_crc(&bytes[..last]);
    bytes[last..].copy_from_slice(&crc.to_be_bytes());
}

#[test]
fn literal_record_golden_vector_and_crc32c_standard_check() {
    assert_eq!(fixture_crc(b"123456789"), 0xe3069283);
    let hex = "00000078010100000000000000000000000000000000000700000000000000000000000000000008000000000000000900000000000000000000000000000000000000000000000a000000000000000b0000002000000000000000000000000000000000000000000000000000000000000000002ec41451";
    let expected: Vec<_> = hex
        .as_bytes()
        .chunks_exact(2)
        .map(|v| u8::from_str_radix(std::str::from_utf8(v).unwrap(), 16).unwrap())
        .collect();
    let t = Temp::new();
    let mut w = Wal::open(&t.0, config()).unwrap();
    w.append(record(0)).unwrap();
    w.sync().unwrap();
    drop(w);
    let bytes = fs::read(segments(&t.0).remove(0)).unwrap();
    assert_eq!(&bytes[rovqet_wal::HEADER_BYTES..], expected);
    assert_eq!(&bytes[..8], &[b'R', b'Q', b'W', b'L', 0, 1, 0, 0]);
}

#[test]
fn checksum_valid_invalid_record_fields_and_absurd_lengths_fail_closed() {
    for field in [0_usize, 4, 5, 6, 80] {
        let t = Temp::new();
        let mut w = Wal::open(&t.0, config()).unwrap();
        w.append(record(0)).unwrap();
        w.sync().unwrap();
        drop(w);
        let path = segments(&t.0).remove(0);
        let mut bytes = fs::read(&path).unwrap();
        let r = &mut bytes[rovqet_wal::HEADER_BYTES..];
        match field {
            0 => r[..4].copy_from_slice(&u32::MAX.to_be_bytes()),
            4 => r[4] = 99,
            5 => r[5] = 99,
            6 => r[6] = 1,
            80 => r[80..84].copy_from_slice(&u32::MAX.to_be_bytes()),
            _ => unreachable!(),
        }
        put_crc(r);
        fs::write(&path, bytes).unwrap();
        assert!(Wal::open(&t.0, config()).is_err());
    }
}

#[test]
fn sealed_footer_corruption_and_missing_manifest_are_fatal() {
    for manifest in [false, true] {
        let t = Temp::new();
        let mut w = Wal::open(&t.0, config()).unwrap();
        for n in 0..8 {
            w.append(record(n)).unwrap();
        }
        w.sync().unwrap();
        drop(w);
        if manifest {
            fs::remove_file(t.0.join("MANIFEST")).unwrap();
        } else {
            let first = segments(&t.0).remove(0);
            let len = fs::metadata(&first).unwrap().len();
            flip(&first, len - 1);
        }
        assert!(Wal::open(&t.0, config()).is_err());
    }
}

#[test]
fn checksum_valid_wrong_segment_version_is_rejected() {
    let t = Temp::new();
    let mut w = Wal::open(&t.0, config()).unwrap();
    w.append(record(0)).unwrap();
    w.sync().unwrap();
    drop(w);
    let path = segments(&t.0).remove(0);
    let mut bytes = fs::read(&path).unwrap();
    bytes[5] = 99;
    put_crc(&mut bytes[..rovqet_wal::HEADER_BYTES]);
    fs::write(path, bytes).unwrap();
    assert!(Wal::open(&t.0, config()).is_err());
}

#[test]
fn partial_append_failure_is_uncertain_and_recovery_preserves_prior_commit() {
    let t = Temp::new();
    let mut w = Wal::open(&t.0, config()).unwrap();
    w.append(record(0)).unwrap();
    w.sync().unwrap();
    w.inject_fault(FaultPoint::PartialAppend);
    assert!(w.append(record(1)).is_err());
    assert!(w.sync().is_err());
    assert_eq!(w.committed_position(), Some(offset(0)));
    drop(w);
    let mut w = Wal::open(&t.0, config()).unwrap();
    assert_eq!(read_all(&mut w), vec![record(0)]);
    w.append(record(1)).unwrap();
    w.sync().unwrap();
    drop(w);
    let mut w = Wal::open(&t.0, config()).unwrap();
    assert_eq!(read_all(&mut w), vec![record(0), record(1)]);
}

#[test]
fn interrupted_initial_manifest_is_never_reinitialized() {
    let t = Temp::new();
    fs::write(t.0.join("MANIFEST.tmp"), b"interrupted first publication").unwrap();
    assert!(Wal::open(&t.0, config()).is_err());
    assert!(!t.0.join("MANIFEST").exists());
}

#[test]
fn explicit_record_kinds_preserve_metadata_on_recovery() {
    let t = Temp::new();
    let mut w = Wal::open(&t.0, config()).unwrap();
    let records: Vec<_> = [
        RecordKind::Message,
        RecordKind::ProducerOutcome,
        RecordKind::ConsumerAck,
        RecordKind::TerminalDisposition,
        RecordKind::CursorCheckpoint,
        RecordKind::Tombstone,
    ]
    .into_iter()
    .enumerate()
    .map(|(i, kind)| {
        let mut r = record(i as u64);
        r.kind = kind;
        r
    })
    .collect();
    for r in &records {
        w.enqueue(r.clone()).unwrap();
    }
    w.flush().unwrap();
    w.sync().unwrap();
    drop(w);
    let mut w = Wal::open(&t.0, config()).unwrap();
    assert_eq!(read_all(&mut w), records);
}

#[test]
fn oversized_and_out_of_order_records_are_rejected_before_writing() {
    let t = Temp::new();
    let mut w = Wal::open(&t.0, config()).unwrap();
    assert!(matches!(
        w.enqueue(record(1)),
        Err(rovqet_wal::Error::InvalidOffset)
    ));
    let mut big = record(0);
    big.payload.resize(1024, 0);
    assert!(matches!(w.enqueue(big), Err(rovqet_wal::Error::Capacity)));
    assert_eq!(w.pending_bytes(), 0);
    w.append(record(0)).unwrap();
    w.sync().unwrap();
    assert_eq!(read_all(&mut w), vec![record(0)]);
}

fn unhex(hex: &str) -> Vec<u8> {
    hex.as_bytes()
        .chunks_exact(2)
        .map(|v| u8::from_str_radix(std::str::from_utf8(v).unwrap(), 16).unwrap())
        .collect()
}

#[test]
fn literal_header_and_manifest_generation_vectors() {
    let t = Temp::new();
    let mut w = Wal::open(&t.0, config()).unwrap();
    let initial = "5251574d00010000000000000000000100000000000000020000000000000003000000000000000400000005010000000000000000000000000000002a0000000000000001000000000000000000000001000000000000000000000000000000340000000000000000ffffffffffffffff0084ffdc9d";
    assert_eq!(fs::read(t.0.join("MANIFEST")).unwrap(), unhex(initial));
    let header = "5251574c00010000000000000000000000000000000000050000000000000000000000000000002a0000000000000000d2965b16";
    assert_eq!(fs::read(segments(&t.0).remove(0)).unwrap(), unhex(header));
    w.append(record(0)).unwrap();
    w.sync().unwrap();
    let committed = "5251574d00010000000000000000000100000000000000020000000000000003000000000000000400000005010000000000000000000000000000002a0000000000000002000000000000000100000001000000000000000000000000000000ac0000000000000001000000000000000000125ddd03";
    assert_eq!(fs::read(t.0.join("MANIFEST")).unwrap(), unhex(committed));
    assert_eq!(w.generation(), 2);
}

#[test]
fn interrupted_rotation_reverts_uncommitted_seal_and_removes_orphan_segments() {
    let t = Temp::new();
    let mut w = Wal::open(&t.0, config()).unwrap();
    w.append(record(0)).unwrap();
    w.sync().unwrap();
    let committed_manifest = fs::read(t.0.join("MANIFEST")).unwrap();
    let first = segments(&t.0).remove(0);
    let committed_segment = fs::read(&first).unwrap();
    for n in 1..8 {
        w.append(record(n)).unwrap();
    }
    assert!(segments(&t.0).len() > 1);
    w.inject_fault(FaultPoint::ManifestRename);
    assert!(matches!(
        w.sync(),
        Err(Error::Io {
            kind: IoFailureKind::Sync,
            ..
        })
    ));
    drop(w);
    let mut w = Wal::open(&t.0, config()).unwrap();
    assert_eq!(segments(&t.0), vec![first.clone()]);
    assert_eq!(fs::read(first).unwrap(), committed_segment);
    assert_eq!(fs::read(t.0.join("MANIFEST")).unwrap(), committed_manifest);
    assert_eq!(read_all(&mut w), vec![record(0)]);
    for n in 1..8 {
        w.append(record(n)).unwrap();
    }
    w.sync().unwrap();
    drop(w);
    let mut w = Wal::open(&t.0, config()).unwrap();
    assert_eq!(read_all(&mut w), (0..8).map(record).collect::<Vec<_>>());
}

#[test]
fn checksum_valid_manifest_inventory_bounds_fail_closed() {
    for mutation in 0..3 {
        let t = Temp::new();
        drop(Wal::open(&t.0, config()).unwrap());
        let path = t.0.join("MANIFEST");
        let mut manifest = fs::read(&path).unwrap();
        match mutation {
            0 => manifest[61..69].copy_from_slice(&0_u64.to_be_bytes()),
            1 => manifest[97..105].copy_from_slice(&u64::MAX.to_be_bytes()),
            2 => manifest[113] = 1, // A sealed segment needs room for its footer.
            _ => unreachable!(),
        }
        put_crc(&mut manifest);
        fs::write(path, manifest).unwrap();
        assert!(matches!(
            Wal::open(&t.0, config()),
            Err(Error::Corruption(_))
        ));
    }
}
