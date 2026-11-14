//! The durable old-owner fence of a Q1 ownership migration.
//!
//! The fence is a record of the Q1 WAL: it is serialized with the durable mutations of
//! the same actor, it exists only after the durable boundary, and a restart replays it. A
//! restarted owner therefore never regains its mutation authority.
use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use rovqet_core::{DurableOffset, PartitionRef, QoS};
use rovqet_durable::{
    AckRequest, Authority, Error, GroupFetchRequest, NackRequest, Partition, PartitionConfig,
    PublishRequest,
};

static NEXT: AtomicU64 = AtomicU64::new(0);

struct Temp(PathBuf);
impl Temp {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "rovqet-fence-{}-{}",
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
    .expect("partition")
}

fn reopen(t: &Temp, node_id: u64) -> Partition {
    let dir = t.0.join(format!("wal{node_id}"));
    Partition::open_existing(
        &dir,
        rovqet_wal::Config {
            partition: partition(),
            incarnation: 42,
            ..Default::default()
        },
        config(node_id),
    )
    .expect("reopened partition")
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

fn fetch(p: &Partition) -> rovqet_durable::GroupFetchBatch {
    p.try_fetch_group(GroupFetchRequest {
        partition: partition(),
        group_id: 9,
        generation: 0,
        from: DurableOffset {
            log_incarnation: 42,
            sequence: 0,
        },
        max_messages: 4,
        max_bytes: 65536,
        wait: Duration::ZERO,
    })
    .unwrap()
    .wait()
    .unwrap()
}

fn ack(p: &Partition, token: &rovqet_durable::GroupDelivery) -> Result<(), Error> {
    p.try_ack(AckRequest {
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
    .wait()
    .map(|_| ())
    .map_err(|_| Error::Fenced)
}

fn nack(p: &Partition, token: &rovqet_durable::GroupDelivery) -> Result<(), Error> {
    p.try_nack(NackRequest {
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
    .wait()
    .map(|_| ())
    .map_err(|_| Error::Fenced)
}

const MIGRATION: u128 = 0x1111_2222_3333_4444;

#[test]
fn a_fence_proof_exists_only_after_the_durable_boundary() {
    let temp = Temp::new();
    let partition = open(&temp, 1);
    partition
        .set_authority(Authority::new(1, 7, Duration::from_secs(600)))
        .unwrap();
    publish(&partition, 0).expect("a publication before the fence");

    let proof = partition
        .fence_old_owner(MIGRATION, 7, 8)
        .expect("the fence must succeed for the committed owner");
    assert_eq!(proof.migration_id, MIGRATION);
    assert_eq!(proof.old_owner, 1);
    assert_eq!(proof.old_epoch, 7);
    assert_eq!(proof.next_epoch, 8);
    assert!(!proof.fence_digest.is_empty());
    assert!(
        proof.manifest_generation > 0,
        "the manifest must be durable"
    );

    // The fence record is part of the durable log: the manifest exists and is complete.
    let manifest = fs::read(temp.0.join("wal1").join("MANIFEST")).expect("manifest");
    assert!(!manifest.is_empty());
}

#[test]
fn a_fenced_owner_refuses_every_durable_mutation() {
    let temp = Temp::new();
    let partition = open(&temp, 1);
    partition
        .set_authority(Authority::new(1, 7, Duration::from_secs(600)))
        .unwrap();
    publish(&partition, 0).expect("a publication before the fence");
    let fetched = fetch(&partition);
    let token = fetched.deliveries[0].clone();
    partition
        .fence_old_owner(MIGRATION, 7, 8)
        .expect("the fence must succeed");

    assert!(
        matches!(publish(&partition, 1), Err(Error::Fenced)),
        "a fenced owner must not publish"
    );
    assert!(
        ack(&partition, &token).is_err(),
        "a fenced owner must not acknowledge"
    );
    assert!(
        nack(&partition, &token).is_err(),
        "a fenced owner must not reject"
    );
}

#[test]
fn a_restarted_fenced_owner_still_refuses_publications() {
    let temp = Temp::new();
    {
        let partition = open(&temp, 1);
        partition
            .set_authority(Authority::new(1, 7, Duration::from_secs(600)))
            .unwrap();
        publish(&partition, 0).expect("a publication before the fence");
        partition
            .fence_old_owner(MIGRATION, 7, 8)
            .expect("the fence must succeed");
    }
    // The restarted process starts without an installed authority, exactly like a fresh
    // start. The durable revocation must still refuse the mutation.
    let restarted = reopen(&temp, 1);
    assert!(
        matches!(publish(&restarted, 1), Err(Error::Fenced)),
        "a restarted fenced owner must not publish"
    );
}

#[test]
fn a_fence_without_the_committed_authority_is_refused() {
    let temp = Temp::new();
    let partition = open(&temp, 1);
    // No authority was installed: the partition is standalone and may not fence itself.
    assert!(
        matches!(
            partition.fence_old_owner(MIGRATION, 7, 8),
            Err(Error::Fenced)
        ),
        "only the committed owner may revoke its authority"
    );
}

#[test]
fn a_fence_for_a_stale_epoch_is_refused() {
    let temp = Temp::new();
    let partition = open(&temp, 1);
    partition
        .set_authority(Authority::new(1, 7, Duration::from_secs(600)))
        .unwrap();
    assert!(
        matches!(
            partition.fence_old_owner(MIGRATION, 6, 8),
            Err(Error::Fenced)
        ),
        "a fence for an epoch the owner does not hold must be refused"
    );
}

#[test]
fn a_second_fence_is_refused_and_keeps_the_first_migration() {
    let temp = Temp::new();
    let partition = open(&temp, 1);
    partition
        .set_authority(Authority::new(1, 7, Duration::from_secs(600)))
        .unwrap();
    let proof = partition
        .fence_old_owner(MIGRATION, 7, 8)
        .expect("the fence must succeed");
    // A fence proof may never be replayed as an authorization for another migration.
    assert!(
        matches!(
            partition.fence_old_owner(MIGRATION + 1, 7, 8),
            Err(Error::Fenced)
        ),
        "a fenced owner may not be fenced again for another migration"
    );
    assert_eq!(proof.migration_id, MIGRATION);
}

#[test]
fn a_fence_keeps_the_committed_history_readable() {
    let temp = Temp::new();
    let source = open(&temp, 1);
    source
        .set_authority(Authority::new(1, 7, Duration::from_secs(600)))
        .unwrap();
    publish(&source, 0).expect("a publication before the fence");
    source
        .fence_old_owner(MIGRATION, 7, 8)
        .expect("the fence must succeed");

    // A fenced owner does not act as an owner any more: it does not even deliver.
    let reopened = reopen(&temp, 1);
    let refused = reopened
        .try_fetch_group(GroupFetchRequest {
            partition: partition(),
            group_id: 9,
            generation: 0,
            from: DurableOffset {
                log_incarnation: 42,
                sequence: 0,
            },
            max_messages: 4,
            max_bytes: 65536,
            wait: Duration::ZERO,
        })
        .map(|pending| pending.wait());
    assert!(
        matches!(refused, Ok(Err(_))),
        "a fenced owner must not deliver"
    );

    // The committed history itself survives the fence: a copy of the durable directory
    // recovers the publication, so the successor can take it over.
    let copy = temp.0.join("copy");
    fs::create_dir_all(&copy).unwrap();
    for entry in fs::read_dir(temp.0.join("wal1")).unwrap() {
        let entry = entry.unwrap();
        fs::copy(entry.path(), copy.join(entry.file_name())).unwrap();
    }
    let successor = Partition::open(
        &copy,
        rovqet_wal::Config {
            partition: partition(),
            incarnation: 42,
            ..Default::default()
        },
        config(2),
    )
    .expect("the copied history must open");
    let fetched = fetch(&successor);
    assert!(
        !fetched.deliveries.is_empty(),
        "the publication before the fence must survive the handover"
    );
}
