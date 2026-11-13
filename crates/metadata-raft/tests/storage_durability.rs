//! Durability and corruption handling of the metadata Raft storage.
use openraft::storage::{RaftLogReader, RaftLogStorage, RaftLogStorageExt};
use openraft::{Entry, EntryPayload, LeaderId, LogId, Vote};
use rovqet_metadata_raft::storage::FileLogStorage;
use rovqet_metadata_raft::types::MetadataRaftTypeConfig;

fn log_id(index: u64) -> LogId<u64> {
    LogId::new(LeaderId::new(2, 1), index)
}

fn entries(from: u64, until: u64) -> Vec<Entry<MetadataRaftTypeConfig>> {
    (from..until)
        .map(|index| Entry {
            log_id: log_id(index),
            payload: EntryPayload::Blank,
        })
        .collect()
}

#[tokio::test]
async fn a_persisted_vote_survives_a_reopen() {
    let directory = tempfile::tempdir().expect("temp dir");
    let vote = Vote::new(3, 2);
    {
        let mut storage = FileLogStorage::open(directory.path()).expect("open");
        storage.save_vote(&vote).await.expect("save vote");
        let state = storage.get_log_state().await.expect("state");
        assert_eq!(state.last_log_id, None, "a fresh log is empty");
    }
    let mut reopened = FileLogStorage::open(directory.path()).expect("reopen");
    assert_eq!(
        reopened.read_vote().await.expect("read vote"),
        Some(vote),
        "a persisted vote must survive a reopen"
    );
}

#[tokio::test]
async fn a_corrupted_log_fails_closed_instead_of_being_reset() {
    let directory = tempfile::tempdir().expect("temp dir");
    {
        let _ = FileLogStorage::open(directory.path()).expect("open");
    }
    let path = directory.path().join("raft-log.bin");
    let mut bytes = std::fs::read(&path).expect("read log");
    // Corrupt a byte inside the log body.
    bytes.push(0x42);
    bytes.push(0x43);
    bytes.push(0x44);
    bytes.push(0x45);
    std::fs::write(&path, &bytes).expect("write log");
    assert!(
        FileLogStorage::open(directory.path()).is_err(),
        "a corrupted metadata log must never be silently reset"
    );
}

#[tokio::test]
async fn entries_are_keyed_by_log_id_and_recover_after_truncate_and_purge() {
    let directory = tempfile::tempdir().expect("temp dir");
    let mut storage = FileLogStorage::open(directory.path()).expect("open");
    storage
        .blocking_append(entries(0, 3))
        .await
        .expect("append");

    let state = storage.get_log_state().await.expect("log state");
    assert_eq!(state.last_log_id, Some(log_id(2)));
    assert_eq!(
        state.last_purged_log_id, None,
        "a fresh log has no purge point"
    );

    // The range is expressed in Raft log indexes, not in storage positions.
    let requested = storage.try_get_log_entries(1..).await.expect("read");
    assert_eq!(
        requested.len(),
        2,
        "range 1.. must not return the entry whose log id is 0"
    );
    assert_eq!(requested[0].log_id, log_id(1));
    assert_eq!(requested[1].log_id, log_id(2));

    // Truncate drops `log_id` itself and keeps everything below it.
    storage.truncate(log_id(1)).await.expect("truncate");
    let remaining = storage.try_get_log_entries(..).await.expect("read");
    assert_eq!(remaining.len(), 1, "truncate is inclusive of log_id");
    assert_eq!(remaining[0].log_id, log_id(0));

    storage
        .blocking_append(entries(1, 3))
        .await
        .expect("re-append");
    let state = storage.get_log_state().await.expect("log state");
    assert_eq!(state.last_log_id, Some(log_id(2)));

    // Purge drops everything up to and including `log_id` and records the purge point.
    storage.purge(log_id(0)).await.expect("purge");
    let state = storage.get_log_state().await.expect("log state");
    assert_eq!(state.last_purged_log_id, Some(log_id(0)));
    assert_eq!(state.last_log_id, Some(log_id(2)));
    let remaining = storage.try_get_log_entries(..).await.expect("read");
    assert_eq!(remaining.len(), 2);
    assert_eq!(remaining[0].log_id, log_id(1));

    // Purging the whole log leaves an empty log that still reports the purge point.
    storage.purge(log_id(2)).await.expect("purge all");
    let state = storage.get_log_state().await.expect("log state");
    assert_eq!(state.last_purged_log_id, Some(log_id(2)));
    assert_eq!(
        state.last_log_id,
        Some(log_id(2)),
        "an empty log reports its last purged id"
    );
    drop(storage);

    let mut reopened = FileLogStorage::open(directory.path()).expect("reopen");
    let state = reopened.get_log_state().await.expect("log state");
    assert_eq!(state.last_purged_log_id, Some(log_id(2)));
    assert_eq!(state.last_log_id, Some(log_id(2)));

    // Appending after the purge point continues at the purged index plus one, and reads
    // are answered with the entries' own log ids.
    reopened
        .blocking_append(entries(3, 4))
        .await
        .expect("append");
    let after = reopened.try_get_log_entries(3..).await.expect("read");
    assert_eq!(after.len(), 1);
    assert_eq!(after[0].log_id, log_id(3));
    let purged_range = reopened.try_get_log_entries(..3).await.expect("read");
    assert!(
        purged_range.is_empty(),
        "the purged prefix must not come back from storage"
    );
}
