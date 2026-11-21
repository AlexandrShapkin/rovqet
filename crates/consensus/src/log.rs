//! Filesystem-backed Raft log storage for one Q2 shard node.
//!
//! The format is versioned and checksummed, and it is deliberately distinct from the
//! metadata group format: `raft-log.bin` starts with the `QRL1` magic (Q2 raft log,
//! format version 1) instead of the metadata `MRL1` magic, so a Q2 log can never be
//! mistaken for a metadata log or opened as one. After the magic the file holds records
//! `len u32 | crc32c u32 | payload`, where the payload is the JSON encoded Raft entry.
//! Records are keyed by the entry's own `log_id.index` (zero based, no sentinel index),
//! so the reader's view always matches openraft's log-id bookkeeping. Vote, committed and
//! purge positions live in their own atomically replaced JSON files. The durable
//! completion callback is only signalled after `fsync`, and any corruption fails closed
//! instead of being reset.
use crate::types::{ShardNodeId, ShardRaftTypeConfig};
use openraft::storage::{RaftLogReader, RaftLogStorage};
use openraft::{
    Entry, ErrorSubject, ErrorVerb, LogId, LogState, StorageError, StorageIOError, Vote,
};
use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::io::{ErrorKind, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};

/// Magic of the Q2 log format, format version 1. Never `MRL1`: the metadata format and
/// this format must not be interchangeable.
const LOG_MAGIC: &[u8; 4] = b"QRL1";
const RECORD_HEADER: usize = 8;
/// Largest record payload accepted while scanning or appending.
const MAX_RECORD: usize = 16 * 1024 * 1024;
/// Largest log file accepted while scanning.
const MAX_LOG_FILE: u64 = 256 * 1024 * 1024;

/// CRC32C (Castagnoli) over the record payload.
fn crc32c(data: &[u8]) -> u32 {
    let mut crc = 0xffff_ffffu32;
    for byte in data {
        crc ^= u32::from(*byte);
        for _ in 0..8 {
            let mask = (crc & 1).wrapping_neg();
            crc = (crc >> 1) ^ (0x82f6_3b78 & mask);
        }
    }
    !crc
}

fn storage_error(
    subject: ErrorSubject<ShardNodeId>,
    verb: ErrorVerb,
    error: std::io::Error,
) -> StorageError<ShardNodeId> {
    StorageError::IO {
        source: StorageIOError::new(subject, verb, openraft::AnyError::new(&error)),
    }
}

fn invalid_data(message: &'static str) -> std::io::Error {
    std::io::Error::new(ErrorKind::InvalidData, message)
}

/// A simulated durable write failure, used by the controlled storage fault tests.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Fault {
    /// The write fails before any byte reaches the file.
    BeforeWrite,
    /// The bytes are written but the durable flush fails, so durability is unknown.
    AtFlush,
}

/// One stored entry position, keyed by the entry's own `log_id.index`.
#[derive(Debug, Clone, Copy)]
struct EntryIndex {
    index: u64,
    offset: u64,
    len: usize,
}

/// Persistent Raft log storage of one Q2 shard node.
pub struct FileLogStorage {
    directory: PathBuf,
    /// The single write handle. Every read opens an independent handle instead, a cloned
    /// descriptor would share the file offset and corrupt appends.
    file: File,
    /// Entries keyed by `entry.log_id.index`.
    ///
    /// Shared with every live log reader. OpenRaft keeps a reader across appends and
    /// requires a non-empty result for a non-empty range, so a reader holding a copied
    /// map would report a just appended entry as missing.
    index: Arc<RwLock<BTreeMap<u64, EntryIndex>>>,
    /// Set when a durable write failed in an uncertain way: the log may hold bytes that
    /// were never synced, so the storage refuses to serve it and fails closed.
    poisoned: bool,
    /// A pending simulated failure, see [`Fault`].
    fault: Option<Fault>,
    vote: Option<Vote<ShardNodeId>>,
    committed: Option<LogId<ShardNodeId>>,
    purged: Option<LogId<ShardNodeId>>,
}

impl FileLogStorage {
    /// Opens (or creates) the log directory and recovers the durable state.
    ///
    /// Corruption fails closed: a bad magic, a bad checksum, an oversized or truncated
    /// record or a non-contiguous index sequence returns an error instead of resetting
    /// the log.
    pub fn open(directory: impl AsRef<Path>) -> Result<Self, StorageError<ShardNodeId>> {
        let directory = directory.as_ref().to_path_buf();
        std::fs::create_dir_all(&directory)
            .map_err(|error| storage_error(ErrorSubject::Store, ErrorVerb::Write, error))?;
        let path = directory.join("raft-log.bin");
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)
            .map_err(|error| storage_error(ErrorSubject::Store, ErrorVerb::Read, error))?;
        let length = file
            .metadata()
            .map_err(|error| storage_error(ErrorSubject::Logs, ErrorVerb::Read, error))?
            .len();
        if length > MAX_LOG_FILE {
            return Err(storage_error(
                ErrorSubject::Logs,
                ErrorVerb::Read,
                invalid_data("shard log file exceeds the size bound"),
            ));
        }
        // A fresh log starts with the magic only; an existing log is scanned and verified.
        if length == 0 {
            file.write_all(LOG_MAGIC)
                .and_then(|()| file.sync_all())
                .map_err(|error| storage_error(ErrorSubject::Logs, ErrorVerb::Write, error))?;
        }
        let mut index = scan(&mut file)
            .map_err(|error| storage_error(ErrorSubject::Logs, ErrorVerb::Read, error))?;
        let vote = read_json(&directory.join("vote.json"))
            .map_err(|error| storage_error(ErrorSubject::Vote, ErrorVerb::Read, error))?;
        let committed = read_json(&directory.join("committed.json"))
            .map_err(|error| storage_error(ErrorSubject::Store, ErrorVerb::Read, error))?;
        let purged = read_json(&directory.join("purged.json"))
            .map_err(|error| storage_error(ErrorSubject::Logs, ErrorVerb::Read, error))?;
        // The purge writes the pointer durably before it rewrites the file, so a crash
        // between the two leaves the pointer ahead of the file. The entries at or below
        // the purge point were purged by design and are covered by the published
        // snapshot, so they are dropped from the recovered index instead of failing the
        // whole node. A first entry *after* the expected one is a real gap and still
        // fails closed.
        if let Some((first, _)) = index.first_key_value() {
            let expected = purged
                .map(|log_id: LogId<ShardNodeId>| log_id.index.saturating_add(1))
                .unwrap_or(0);
            if *first > expected {
                return Err(storage_error(
                    ErrorSubject::Logs,
                    ErrorVerb::Read,
                    invalid_data("shard log does not follow the purge point"),
                ));
            }
            if *first < expected {
                // `split_off` keeps everything at or after the key in its result.
                index = index.split_off(&expected);
            }
        }
        Ok(Self {
            directory,
            file,
            index: Arc::new(RwLock::new(index)),
            poisoned: false,
            fault: None,
            vote,
            committed,
            purged,
        })
    }

    /// Refuses every operation once a durable write failed in an uncertain way.
    fn check_healthy(&self) -> Result<(), StorageError<ShardNodeId>> {
        if self.poisoned {
            return Err(storage_error(
                ErrorSubject::Logs,
                ErrorVerb::Read,
                invalid_data("shard log storage is poisoned by a failed durable write"),
            ));
        }
        Ok(())
    }

    fn log_path(&self) -> PathBuf {
        self.directory.join("raft-log.bin")
    }

    fn index_read(&self) -> std::sync::RwLockReadGuard<'_, BTreeMap<u64, EntryIndex>> {
        self.index
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn index_write(&self) -> std::sync::RwLockWriteGuard<'_, BTreeMap<u64, EntryIndex>> {
        self.index
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// The index the next appended entry must have: one past the last stored entry, or
    /// one past the purge point when the log itself is empty.
    fn next_index(&self) -> u64 {
        self.index_read()
            .keys()
            .next_back()
            .map(|log_index| log_index.saturating_add(1))
            .or_else(|| self.purged.map(|log_id| log_id.index.saturating_add(1)))
            .unwrap_or(0)
    }

    fn read_payload(&self, position: &EntryIndex) -> Result<Vec<u8>, StorageError<ShardNodeId>> {
        // An independent read handle: a cloned descriptor would share the write offset.
        let mut file = File::open(self.log_path())
            .map_err(|error| storage_error(ErrorSubject::Logs, ErrorVerb::Read, error))?;
        read_payload_from(&mut file, position)
            .map_err(|error| storage_error(ErrorSubject::Logs, ErrorVerb::Read, error))
    }

    fn read_entry(
        &self,
        position: &EntryIndex,
    ) -> Result<Entry<ShardRaftTypeConfig>, StorageError<ShardNodeId>> {
        let payload = self.read_payload(position)?;
        decode_entry(position, &payload)
            .map_err(|error| storage_error(ErrorSubject::Logs, ErrorVerb::Read, error))
    }

    /// Replaces the log file with the given entries and fsyncs the directory.
    ///
    /// The entries are `(log_id.index, payload)` pairs and must form one contiguous run
    /// that starts directly after the purge point; anything else would leave a hole.
    fn rewrite(&mut self, entries: Vec<(u64, Vec<u8>)>) -> Result<(), StorageError<ShardNodeId>> {
        // The write lock is held for the whole rewrite: a reader that holds the read lock
        // must never observe the replaced file with the previous offsets.
        let mut guard = self.index_write();
        let expected_start = self
            .purged
            .map(|log_id| log_id.index.saturating_add(1))
            .unwrap_or(0);
        let temporary = self.directory.join("raft-log.bin.tmp");
        let mut index: BTreeMap<u64, EntryIndex> = BTreeMap::new();
        {
            let mut file = File::create(&temporary)
                .map_err(|error| storage_error(ErrorSubject::Logs, ErrorVerb::Write, error))?;
            file.write_all(LOG_MAGIC)
                .map_err(|error| storage_error(ErrorSubject::Logs, ErrorVerb::Write, error))?;
            let mut offset = LOG_MAGIC.len() as u64;
            let mut expected: Option<u64> = None;
            for (log_index, payload) in entries {
                let follows = match expected {
                    None => log_index == expected_start,
                    Some(value) => log_index == value,
                };
                if !follows {
                    return Err(storage_error(
                        ErrorSubject::Logs,
                        ErrorVerb::Write,
                        invalid_data("shard log rewrite would leave a hole"),
                    ));
                }
                if payload.len() > MAX_RECORD {
                    return Err(storage_error(
                        ErrorSubject::Logs,
                        ErrorVerb::Write,
                        invalid_data("shard log record too large"),
                    ));
                }
                let checksum = crc32c(&payload);
                file.write_all(&(payload.len() as u32).to_be_bytes())
                    .and_then(|()| file.write_all(&checksum.to_be_bytes()))
                    .and_then(|()| file.write_all(&payload))
                    .map_err(|error| storage_error(ErrorSubject::Logs, ErrorVerb::Write, error))?;
                index.insert(
                    log_index,
                    EntryIndex {
                        index: log_index,
                        offset,
                        len: payload.len(),
                    },
                );
                offset += (RECORD_HEADER + payload.len()) as u64;
                expected = Some(log_index.saturating_add(1));
            }
            file.sync_all()
                .map_err(|error| storage_error(ErrorSubject::Logs, ErrorVerb::Write, error))?;
        }
        std::fs::rename(&temporary, self.log_path())
            .map_err(|error| storage_error(ErrorSubject::Logs, ErrorVerb::Write, error))?;
        sync_directory(&self.directory)
            .map_err(|error| storage_error(ErrorSubject::Logs, ErrorVerb::Write, error))?;
        *guard = index;
        drop(guard);
        self.file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(self.log_path())
            .map_err(|error| storage_error(ErrorSubject::Logs, ErrorVerb::Read, error))?;
        Ok(())
    }

    /// Merges an append that overlaps the stored suffix into the log and rewrites the
    /// file.
    fn merge_rewrite(
        &mut self,
        payloads: Vec<(u64, Vec<u8>)>,
    ) -> Result<(), StorageError<ShardNodeId>> {
        let mut merged: BTreeMap<u64, Vec<u8>> = BTreeMap::new();
        let existing: Vec<(u64, EntryIndex)> = self
            .index_read()
            .iter()
            .map(|(log_index, position)| (*log_index, *position))
            .collect();
        for (log_index, position) in existing {
            let payload = self.read_payload(&position)?;
            merged.insert(log_index, payload);
        }
        for (log_index, payload) in payloads {
            merged.insert(log_index, payload);
        }
        self.rewrite(merged.into_iter().collect())
    }

    /// Writes the given entries at the end of the durable log and syncs them.
    fn append_sync(
        &mut self,
        entries: &[Entry<ShardRaftTypeConfig>],
    ) -> Result<(), StorageError<ShardNodeId>> {
        if entries.is_empty() {
            return Ok(());
        }
        let mut payloads = Vec::with_capacity(entries.len());
        for entry in entries {
            let payload = encode_entry(entry)
                .map_err(|error| storage_error(ErrorSubject::Logs, ErrorVerb::Write, error))?;
            if payload.len() > MAX_RECORD {
                return Err(storage_error(
                    ErrorSubject::Logs,
                    ErrorVerb::Write,
                    invalid_data("shard log record too large"),
                ));
            }
            payloads.push((entry.log_id.index, payload));
        }

        let expected = self.next_index();
        let first = payloads[0].0;
        if first < expected {
            // The batch overlaps what is already stored: merge it in, the same overwrite
            // semantics an in-memory map of log id to entry has.
            return self.merge_rewrite(payloads);
        }
        for (offset_in_batch, (log_index, _)) in payloads.iter().enumerate() {
            if *log_index != expected.saturating_add(offset_in_batch as u64) {
                return Err(storage_error(
                    ErrorSubject::Logs,
                    ErrorVerb::Write,
                    invalid_data("shard log append would leave a hole"),
                ));
            }
        }

        // The injected fault is only ever set through the gated injectors below; the
        // check itself stays compiled so the storage behaves identically with and
        // without the `fault-injection` feature.
        if self.fault == Some(Fault::BeforeWrite) {
            self.fault = None;
            return Err(storage_error(
                ErrorSubject::Logs,
                ErrorVerb::Write,
                invalid_data("injected shard log append failure"),
            ));
        }
        let mut offset = self
            .file
            .seek(SeekFrom::End(0))
            .map_err(|error| storage_error(ErrorSubject::Logs, ErrorVerb::Seek, error))?;
        if offset == 0 {
            self.file
                .write_all(LOG_MAGIC)
                .map_err(|error| storage_error(ErrorSubject::Logs, ErrorVerb::Write, error))?;
            offset = LOG_MAGIC.len() as u64;
        }
        let start_length = offset;
        let mut position = offset;
        let mut written: Vec<(u64, EntryIndex)> = Vec::with_capacity(payloads.len());
        let result = (|| -> Result<(), std::io::Error> {
            for (log_index, payload) in &payloads {
                let checksum = crc32c(payload);
                self.file.write_all(&(payload.len() as u32).to_be_bytes())?;
                self.file.write_all(&checksum.to_be_bytes())?;
                self.file.write_all(payload)?;
                written.push((
                    *log_index,
                    EntryIndex {
                        index: *log_index,
                        offset: position,
                        len: payload.len(),
                    },
                ));
                position += (RECORD_HEADER + payload.len()) as u64;
            }
            if self.fault == Some(Fault::AtFlush) {
                self.fault = None;
                // The bytes reached the page cache but were never synced: durability is
                // unknown, so the storage must fail closed afterwards.
                self.poisoned = true;
                return Err(std::io::Error::new(
                    ErrorKind::Other,
                    "injected shard log flush failure",
                ));
            }
            self.file.flush()?;
            // The completion callback must only be signalled after the data is durable.
            self.file.sync_all()?;
            Ok(())
        })();
        match result {
            Ok(()) => {
                // Only now are the entries readable, so only now they enter the index.
                let mut index = self.index_write();
                for (log_index, entry_index) in written {
                    index.insert(log_index, entry_index);
                }
                Ok(())
            }
            Err(error) => {
                // Roll the file back so it never disagrees with the in-memory index.
                let _ = self.file.set_len(start_length);
                let _ = self.file.seek(SeekFrom::Start(start_length));
                let _ = self.file.sync_all();
                Err(storage_error(ErrorSubject::Logs, ErrorVerb::Write, error))
            }
        }
    }

    fn write_json<T: serde::Serialize>(
        &self,
        name: &str,
        value: &T,
    ) -> Result<(), StorageError<ShardNodeId>> {
        let path = self.directory.join(name);
        let temporary = self.directory.join(format!("{name}.tmp"));
        let bytes = serde_json::to_vec(value).map_err(|error| {
            storage_error(
                ErrorSubject::Store,
                ErrorVerb::Write,
                std::io::Error::new(ErrorKind::InvalidData, error),
            )
        })?;
        {
            let mut file = File::create(&temporary)
                .map_err(|error| storage_error(ErrorSubject::Store, ErrorVerb::Write, error))?;
            file.write_all(&bytes)
                .map_err(|error| storage_error(ErrorSubject::Store, ErrorVerb::Write, error))?;
            file.sync_all()
                .map_err(|error| storage_error(ErrorSubject::Store, ErrorVerb::Write, error))?;
        }
        std::fs::rename(&temporary, &path)
            .map_err(|error| storage_error(ErrorSubject::Store, ErrorVerb::Write, error))?;
        sync_directory(&self.directory)
            .map_err(|error| storage_error(ErrorSubject::Store, ErrorVerb::Write, error))?;
        Ok(())
    }
}

/// Fsyncs the directory that contains `path`, making a rename inside it durable.
fn sync_directory(directory: &Path) -> Result<(), std::io::Error> {
    File::open(directory)?.sync_all()
}

fn read_json<T: serde::de::DeserializeOwned>(path: &Path) -> Result<Option<T>, std::io::Error> {
    match std::fs::read(path) {
        Ok(bytes) => serde_json::from_slice(&bytes)
            .map(Some)
            .map_err(|error| std::io::Error::new(ErrorKind::InvalidData, error)),
        Err(error) if error.kind() == ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error),
    }
}

/// Reads one record payload at `position`, verifying the stored length and checksum.
///
/// The length check turns a stale position (the file was rewritten since the position
/// was recorded) into an error instead of silently decoding unrelated bytes.
fn read_payload_from(file: &mut File, position: &EntryIndex) -> Result<Vec<u8>, std::io::Error> {
    file.seek(SeekFrom::Start(position.offset))?;
    let mut header = [0u8; RECORD_HEADER];
    file.read_exact(&mut header)?;
    let length = u32::from_be_bytes(header[..4].try_into().unwrap()) as usize;
    let checksum = u32::from_be_bytes(header[4..].try_into().unwrap());
    if length != position.len || length > MAX_RECORD {
        return Err(invalid_data("shard log record length mismatch"));
    }
    let mut payload = vec![0u8; length];
    file.read_exact(&mut payload)?;
    if crc32c(&payload) != checksum {
        return Err(invalid_data("shard log checksum mismatch"));
    }
    Ok(payload)
}

fn decode_entry(
    position: &EntryIndex,
    payload: &[u8],
) -> Result<Entry<ShardRaftTypeConfig>, std::io::Error> {
    let entry: Entry<ShardRaftTypeConfig> = serde_json::from_slice(payload)
        .map_err(|error| std::io::Error::new(ErrorKind::InvalidData, error))?;
    if entry.log_id.index != position.index {
        return Err(invalid_data("shard log record index mismatch"));
    }
    Ok(entry)
}

fn encode_entry(entry: &Entry<ShardRaftTypeConfig>) -> Result<Vec<u8>, std::io::Error> {
    serde_json::to_vec(entry).map_err(|error| std::io::Error::new(ErrorKind::InvalidData, error))
}

/// Scans the log file and verifies every record checksum. Corruption fails closed.
///
/// Records are inserted keyed by their own `log_id.index`, and the index sequence must
/// be contiguous (each index exactly the previous one plus one).
fn scan(file: &mut File) -> Result<BTreeMap<u64, EntryIndex>, std::io::Error> {
    file.seek(SeekFrom::Start(0))?;
    let mut magic = [0u8; 4];
    file.read_exact(&mut magic)?;
    if &magic != LOG_MAGIC {
        return Err(invalid_data("shard log magic mismatch"));
    }
    let mut index = BTreeMap::new();
    let mut offset = LOG_MAGIC.len() as u64;
    let mut previous: Option<u64> = None;
    loop {
        let mut header = [0u8; RECORD_HEADER];
        let mut filled = 0;
        while filled < RECORD_HEADER {
            let read = file.read(&mut header[filled..])?;
            if read == 0 {
                break;
            }
            filled += read;
        }
        if filled == 0 {
            // A clean end of file is fine.
            break;
        }
        if filled < RECORD_HEADER {
            return Err(invalid_data("truncated shard log record header"));
        }
        let length = u32::from_be_bytes(header[..4].try_into().unwrap()) as usize;
        let checksum = u32::from_be_bytes(header[4..].try_into().unwrap());
        if length > MAX_RECORD {
            return Err(invalid_data("shard log record too large"));
        }
        let mut payload = vec![0u8; length];
        file.read_exact(&mut payload).map_err(|error| {
            if error.kind() == ErrorKind::UnexpectedEof {
                invalid_data("truncated shard log record body")
            } else {
                error
            }
        })?;
        if crc32c(&payload) != checksum {
            return Err(invalid_data("shard log checksum mismatch"));
        }
        let entry: Entry<ShardRaftTypeConfig> = serde_json::from_slice(&payload)
            .map_err(|error| std::io::Error::new(ErrorKind::InvalidData, error))?;
        let log_index = entry.log_id.index;
        if let Some(previous) = previous {
            if log_index != previous.saturating_add(1) {
                return Err(invalid_data("shard log index sequence is not contiguous"));
            }
        }
        index.insert(
            log_index,
            EntryIndex {
                index: log_index,
                offset,
                len: length,
            },
        );
        previous = Some(log_index);
        offset += (RECORD_HEADER + length) as u64;
    }
    Ok(index)
}

/// Reader over the durable log entries.
///
/// It holds the log path and the live shared index, and opens its own read handle for
/// every read, so it never shares a file descriptor with the writer and never misses an
/// append that landed after the reader was created.
pub struct FileLogReader {
    path: PathBuf,
    index: Arc<RwLock<BTreeMap<u64, EntryIndex>>>,
}

impl FileLogReader {
    fn entries<RB>(
        &self,
        range: RB,
    ) -> Result<Vec<Entry<ShardRaftTypeConfig>>, StorageError<ShardNodeId>>
    where
        RB: std::ops::RangeBounds<u64>,
    {
        let mut entries = Vec::new();
        // The read lock is taken before the file is opened and held while the payloads
        // are read: a rewrite replaces the log file under the write lock, so a reader
        // must never hold a handle to the replaced file while the index already
        // describes the new one.
        let index = self
            .index
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        // `BTreeMap::range` yields the stored keys in ascending index order, and an
        // empty result is allowed only for a range that holds no entry.
        let positions: Vec<EntryIndex> =
            index.range(range).map(|(_, position)| *position).collect();
        let mut file = File::open(&self.path)
            .map_err(|error| storage_error(ErrorSubject::Logs, ErrorVerb::Read, error))?;
        for position in positions {
            let payload = read_payload_from(&mut file, &position)
                .map_err(|error| storage_error(ErrorSubject::Logs, ErrorVerb::Read, error))?;
            let entry = decode_entry(&position, &payload)
                .map_err(|error| storage_error(ErrorSubject::Logs, ErrorVerb::Read, error))?;
            entries.push(entry);
        }
        Ok(entries)
    }
}

impl RaftLogReader<ShardRaftTypeConfig> for FileLogReader {
    async fn try_get_log_entries<RB>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry<ShardRaftTypeConfig>>, StorageError<ShardNodeId>>
    where
        RB: std::ops::RangeBounds<u64> + Clone + std::fmt::Debug + openraft::OptionalSend,
    {
        self.entries(range)
    }
}

impl RaftLogReader<ShardRaftTypeConfig> for FileLogStorage {
    async fn try_get_log_entries<RB>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry<ShardRaftTypeConfig>>, StorageError<ShardNodeId>>
    where
        RB: std::ops::RangeBounds<u64> + Clone + std::fmt::Debug + openraft::OptionalSend,
    {
        self.check_healthy()?;
        let mut reader = self.get_log_reader().await;
        reader.try_get_log_entries(range).await
    }
}

impl RaftLogStorage<ShardRaftTypeConfig> for FileLogStorage {
    type LogReader = FileLogReader;

    async fn get_log_state(
        &mut self,
    ) -> Result<LogState<ShardRaftTypeConfig>, StorageError<ShardNodeId>> {
        self.check_healthy()?;
        let last_log_id = match self
            .index_read()
            .last_key_value()
            .map(|(key, position)| (*key, *position))
        {
            Some((_, position)) => Some(self.read_entry(&position)?.log_id),
            // An empty log still reports the purge point, otherwise a restart sees an
            // empty log where openraft expects the purged id.
            None => self.purged,
        };
        Ok(LogState {
            last_purged_log_id: self.purged,
            last_log_id,
        })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        FileLogReader {
            path: self.log_path(),
            index: self.index.clone(),
        }
    }

    async fn save_vote(
        &mut self,
        vote: &Vote<ShardNodeId>,
    ) -> Result<(), StorageError<ShardNodeId>> {
        self.write_json("vote.json", vote)?;
        self.vote = Some(*vote);
        Ok(())
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<ShardNodeId>>, StorageError<ShardNodeId>> {
        Ok(self.vote)
    }

    async fn save_committed(
        &mut self,
        committed: Option<LogId<ShardNodeId>>,
    ) -> Result<(), StorageError<ShardNodeId>> {
        self.write_json("committed.json", &committed)?;
        self.committed = committed;
        Ok(())
    }

    async fn read_committed(
        &mut self,
    ) -> Result<Option<LogId<ShardNodeId>>, StorageError<ShardNodeId>> {
        Ok(self.committed)
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: openraft::storage::LogFlushed<ShardRaftTypeConfig>,
    ) -> Result<(), StorageError<ShardNodeId>>
    where
        I: IntoIterator<Item = Entry<ShardRaftTypeConfig>>,
        I::IntoIter: Send,
    {
        self.check_healthy()?;
        let entries: Vec<Entry<ShardRaftTypeConfig>> = entries.into_iter().collect();
        match self.append_sync(&entries) {
            Ok(()) => {
                callback.log_io_completed(Ok(()));
                Ok(())
            }
            Err(error) => {
                let message = error.to_string();
                callback.log_io_completed(Err(std::io::Error::new(ErrorKind::Other, message)));
                Err(error)
            }
        }
    }

    async fn truncate(
        &mut self,
        log_id: LogId<ShardNodeId>,
    ) -> Result<(), StorageError<ShardNodeId>> {
        self.check_healthy()?;
        // Truncate logs since `log_id`, inclusive: every entry with index >= log_id.index
        // is dropped, the rest is rewritten in ascending index order.
        let retained: Vec<(u64, EntryIndex)> = self
            .index_read()
            .range(..log_id.index)
            .map(|(log_index, position)| (*log_index, *position))
            .collect();
        let mut payloads = Vec::new();
        for (log_index, position) in retained {
            let payload = self.read_payload(&position)?;
            payloads.push((log_index, payload));
        }
        self.rewrite(payloads)
    }

    async fn purge(&mut self, log_id: LogId<ShardNodeId>) -> Result<(), StorageError<ShardNodeId>> {
        self.check_healthy()?;
        if let Some(purged) = self.purged {
            if log_id.index < purged.index {
                return Err(storage_error(
                    ErrorSubject::Logs,
                    ErrorVerb::Write,
                    invalid_data("shard log purge point moved backwards"),
                ));
            }
        }
        // Persist the purge pointer first, then drop the purged prefix and rewrite.
        self.write_json("purged.json", &Some(log_id))?;
        self.purged = Some(log_id);
        let retained: Vec<(u64, EntryIndex)> = self
            .index_read()
            .range(log_id.index.saturating_add(1)..)
            .map(|(log_index, position)| (*log_index, *position))
            .collect();
        let mut payloads = Vec::new();
        for (log_index, position) in retained {
            let payload = self.read_payload(&position)?;
            payloads.push((log_index, payload));
        }
        self.rewrite(payloads)
    }
}

#[cfg(any(test, feature = "fault-injection"))]
impl FileLogStorage {
    /// Fault injection: the next durable append fails before it writes anything.
    pub fn inject_append_failure(&mut self) {
        self.fault = Some(Fault::BeforeWrite);
    }

    /// Fault injection: the next durable append fails at the flush step, after the bytes
    /// were written but before they were synced.
    pub fn inject_flush_failure(&mut self) {
        self.fault = Some(Fault::AtFlush);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::ShardRaftTypeConfig;
    use openraft::storage::{RaftLogReader, RaftLogStorageExt};
    use openraft::{Entry, EntryPayload, LeaderId};

    fn log_id(index: u64) -> LogId<ShardNodeId> {
        LogId::new(LeaderId::new(2, 1), index)
    }

    fn entry(index: u64) -> Entry<ShardRaftTypeConfig> {
        Entry {
            log_id: log_id(index),
            payload: EntryPayload::Blank,
        }
    }

    #[tokio::test]
    async fn a_failed_append_is_never_acknowledged_and_leaves_the_log_unchanged() {
        let directory = tempfile::tempdir().expect("temp dir");
        let mut storage = FileLogStorage::open(directory.path()).expect("open");
        storage
            .blocking_append(vec![entry(0)])
            .await
            .expect("append");
        let before = storage.get_log_state().await.expect("state");

        storage.inject_append_failure();
        let failed = storage.blocking_append(vec![entry(1)]).await;
        assert!(
            failed.is_err(),
            "a failed durable append must never be acknowledged"
        );

        let after = storage.get_log_state().await.expect("state");
        assert_eq!(
            after.last_log_id, before.last_log_id,
            "a failed append must not change the log"
        );
        drop(storage);
        // The durable file must not contain the failed entry either.
        let mut reopened = FileLogStorage::open(directory.path()).expect("reopen");
        assert_eq!(
            reopened.get_log_state().await.expect("state").last_log_id,
            before.last_log_id
        );
    }

    #[tokio::test]
    async fn an_uncertain_durable_write_fails_closed() {
        let directory = tempfile::tempdir().expect("temp dir");
        let mut storage = FileLogStorage::open(directory.path()).expect("open");
        storage
            .blocking_append(vec![entry(0)])
            .await
            .expect("append");

        storage.inject_flush_failure();
        let failed = storage.blocking_append(vec![entry(1)]).await;
        assert!(failed.is_err(), "a failed flush must never be acknowledged");
        assert!(
            storage.get_log_state().await.is_err(),
            "an uncertain durable write must fail closed instead of serving a log"
        );
    }

    #[tokio::test]
    async fn a_live_reader_sees_a_later_append_and_agrees_with_the_log_state() {
        let directory = tempfile::tempdir().expect("temp dir");
        let mut storage = FileLogStorage::open(directory.path()).expect("open");
        storage
            .blocking_append(vec![entry(0)])
            .await
            .expect("append");

        // The reader is created before the second append: it must still see it, because
        // OpenRaft requires a non-empty result for a non-empty range.
        let mut reader = storage.get_log_reader().await;
        storage
            .blocking_append(vec![entry(1)])
            .await
            .expect("append");

        let entries = reader.try_get_log_entries(1..).await.expect("read");
        assert_eq!(
            entries.len(),
            1,
            "a live reader must see an entry that was appended after it was created"
        );
        assert_eq!(entries[0].log_id, log_id(1));

        let state = storage.get_log_state().await.expect("state");
        assert_eq!(
            state.last_log_id,
            entries.last().map(|entry| entry.log_id),
            "the reader and the log state must agree"
        );
    }

    #[tokio::test]
    async fn a_purge_pointer_ahead_of_the_file_recovers_instead_of_failing_the_node() {
        let directory = tempfile::tempdir().expect("temp dir");
        let mut storage = FileLogStorage::open(directory.path()).expect("open");
        storage
            .blocking_append((0..6).map(entry).collect::<Vec<_>>())
            .await
            .expect("append");
        // A real purge persists the pointer and then rewrites the file.
        RaftLogStorage::purge(&mut storage, log_id(2))
            .await
            .expect("purge");
        drop(storage);
        // A crash between the two steps leaves the pointer ahead of the file: the entries
        // at or below the purge point were purged by design and are covered by the
        // published snapshot, so the node must recover instead of refusing to start.
        let purged_path = directory.path().join("purged.json");
        std::fs::write(&purged_path, serde_json::to_vec(&Some(log_id(4))).unwrap())
            .expect("purge pointer");
        let mut reopened = FileLogStorage::open(directory.path()).expect("the log recovers");
        let state = reopened.get_log_state().await.expect("state");
        assert_eq!(
            state.last_purged_log_id,
            Some(log_id(4)),
            "the durable purge point is authoritative"
        );
        assert_eq!(
            state.last_log_id,
            Some(log_id(5)),
            "the entries after the purge point are kept"
        );
        let entries = reopened.try_get_log_entries(5..).await.expect("read");
        assert_eq!(entries.len(), 1, "the stale entries are never served");
        assert_eq!(entries[0].log_id, log_id(5));
    }

    #[tokio::test]
    async fn a_missing_entry_after_the_purge_point_still_fails_closed() {
        let directory = tempfile::tempdir().expect("temp dir");
        let mut storage = FileLogStorage::open(directory.path()).expect("open");
        storage
            .blocking_append((0..6).map(entry).collect::<Vec<_>>())
            .await
            .expect("append");
        RaftLogStorage::purge(&mut storage, log_id(2))
            .await
            .expect("purge");
        drop(storage);
        // The file now starts at 3. A purge pointer that claims only 0 was purged makes
        // the first two entries disappear without a purge: that is a real gap, not a torn
        // purge.
        let purged_path = directory.path().join("purged.json");
        std::fs::write(&purged_path, serde_json::to_vec(&Some(log_id(0))).unwrap())
            .expect("purge pointer");
        let error = match FileLogStorage::open(directory.path()) {
            Ok(_) => panic!("a lost entry must fail closed"),
            Err(error) => format!("{error}"),
        };
        assert!(
            error.contains("does not follow the purge point"),
            "the refusal must name the lost entry: {error}"
        );
    }

    #[tokio::test]
    async fn truncate_is_inclusive_and_a_subsequent_append_continues_at_the_right_index() {
        let directory = tempfile::tempdir().expect("temp dir");
        let mut storage = FileLogStorage::open(directory.path()).expect("open");
        storage
            .blocking_append((0..4).map(entry).collect::<Vec<_>>())
            .await
            .expect("append");

        // Truncate drops `log_id` itself and keeps everything below it.
        storage.truncate(log_id(2)).await.expect("truncate");
        let remaining = storage.try_get_log_entries(..).await.expect("read");
        assert_eq!(remaining.len(), 2, "truncate is inclusive of log_id");
        assert_eq!(remaining[0].log_id, log_id(0));
        assert_eq!(remaining[1].log_id, log_id(1));
        let state = storage.get_log_state().await.expect("state");
        assert_eq!(
            state.last_log_id,
            Some(log_id(1)),
            "the log state must agree with the retained entries"
        );

        // The next append continues directly after the retained suffix: index 2, not 4.
        storage
            .blocking_append((2..4).map(entry).collect::<Vec<_>>())
            .await
            .expect("re-append");
        let state = storage.get_log_state().await.expect("state");
        assert_eq!(state.last_log_id, Some(log_id(3)));
        let entries = storage.try_get_log_entries(2..).await.expect("read");
        assert_eq!(entries.len(), 2, "the append continues at the right index");
        assert_eq!(entries[0].log_id, log_id(2));
        assert_eq!(entries[1].log_id, log_id(3));
        drop(storage);

        // The recovered file must agree: a hole after the truncate would fail closed.
        let mut reopened = FileLogStorage::open(directory.path()).expect("reopen");
        let state = reopened.get_log_state().await.expect("state");
        assert_eq!(state.last_log_id, Some(log_id(3)));
        let entries = reopened.try_get_log_entries(..).await.expect("read");
        assert_eq!(entries.len(), 4);
        assert_eq!(entries[2].log_id, log_id(2));
    }

    #[tokio::test]
    async fn a_command_entry_survives_a_durable_append_and_reopen() {
        let directory = tempfile::tempdir().expect("temp dir");
        let command = crate::types::ShardCommand {
            request_id: 17,
            partition: rovqet_core::PartitionRef {
                tenant_id: 1,
                topic_id: 2,
                partition_id: 3,
            },
            payload: crate::types::ShardPayload::Inline(vec![1, 2, 3]),
            metadata_epoch: 4,
        };
        let command_entry = Entry {
            log_id: log_id(1),
            payload: EntryPayload::Normal(command.clone()),
        };
        {
            let mut storage = FileLogStorage::open(directory.path()).expect("open");
            storage
                .blocking_append(vec![entry(0), command_entry.clone()])
                .await
                .expect("append");
        }
        let mut reopened = FileLogStorage::open(directory.path()).expect("reopen");
        let entries = reopened.try_get_log_entries(..).await.expect("read");
        assert_eq!(entries.len(), 2, "both entries must be durable");
        assert_eq!(entries[0].log_id, log_id(0));
        assert_eq!(entries[1].log_id, log_id(1));
        match &entries[1].payload {
            EntryPayload::Normal(stored) => assert_eq!(stored, &command),
            other => panic!("expected a command entry, got {other:?}"),
        }
    }
}
