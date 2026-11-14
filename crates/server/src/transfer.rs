//! Bounded staging of a Q1 WAL transfer.
//!
//! The staging area never exposes a path from the network: a prepare request declares a
//! list of plain file names, and a chunk refers to a file by its index in that list. Every
//! chunk is verified against its own digest before it is written, every file is verified
//! against its declared digest before the transfer may be finalized, and the staged bytes
//! stay on disk: nothing is buffered in memory.
use rovqet_control::rpc::{
    MAX_TRANSFER_CHUNK_BYTES, MAX_TRANSFER_FILES, Q1TransferChunkRequest, Q1TransferChunkResponse,
    Q1TransferFile, Q1TransferFinalizeRequest, Q1TransferFinalizeResponse,
    Q1TransferPrepareRequest, Q1TransferPrepareResponse, Q1TransferStatus,
    validate_transfer_file_name,
};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

/// Largest staged inventory of one migration.
pub const MAX_STAGED_BYTES: u64 = 4 * 1024 * 1024 * 1024;

/// Why a transfer operation was refused.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TransferError {
    /// The staging area does not exist for this migration.
    NotPrepared,
    /// The chunk belongs to another migration.
    MigrationMismatch,
    /// The chunk refers to a file that was not declared.
    UnknownFile,
    /// The chunk lies outside the declared file.
    OutOfRange,
    /// The chunk payload does not match its digest.
    DigestMismatch,
    /// The same chunk identity arrived with different bytes.
    Conflict,
    /// The inventory is incomplete.
    Incomplete,
    /// The inventory does not match the declared digests.
    Mismatch,
    /// The transfer would exceed its bounds.
    TooLarge,
    /// The staging area rejected a file name.
    InvalidName,
    /// The staging area failed.
    Io(String),
}

impl std::fmt::Display for TransferError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}

impl std::error::Error for TransferError {}

/// The outcome of one accepted chunk.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChunkOutcome {
    /// The chunk was stored.
    Stored,
    /// The chunk was already stored with the same content.
    Duplicate,
}

/// The outcome of a finalization.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FinalizeOutcome {
    /// The inventory is complete and matches the declared digests.
    Complete {
        /// The verified files with their digests.
        files: Vec<(String, u64, Vec<u8>)>,
    },
}

/// One staged migration.
#[derive(Debug)]
pub struct TransferStaging {
    root: PathBuf,
    migration_id: u128,
    files: Vec<Q1TransferFile>,
    /// Received chunks keyed by (file index, offset): the length and the digest.
    received: BTreeMap<(u32, u64), (u64, Vec<u8>)>,
    /// Bytes written per file.
    written: Vec<u64>,
    total: u64,
}

impl TransferStaging {
    /// Prepares (or reopens) the staging area of one migration.
    ///
    /// A repeated prepare with the same inventory is idempotent; a prepare with a different
    /// inventory is refused, so a migration id can never describe two inventories.
    pub fn prepare(root: &Path, request: &Q1TransferPrepareRequest) -> Result<Self, TransferError> {
        if request.files.len() > MAX_TRANSFER_FILES {
            return Err(TransferError::TooLarge);
        }
        for file in &request.files {
            if !validate_transfer_file_name(&file.name) {
                return Err(TransferError::InvalidName);
            }
        }
        let total: u64 = request.files.iter().map(|file| file.size).sum();
        if total > MAX_STAGED_BYTES {
            return Err(TransferError::TooLarge);
        }
        let directory = root.join(format!(
            "migration-{:032x}-inv-{}",
            request.migration_id,
            bytes_to_hex(&request.manifest_digest)
        ));
        std::fs::create_dir_all(&directory)
            .map_err(|error| TransferError::Io(error.to_string()))?;
        let manifest = directory.join("staging.json");
        if manifest.exists() {
            // The inventory of a migration id may not change.
            let existing =
                std::fs::read(&manifest).map_err(|error| TransferError::Io(error.to_string()))?;
            let existing: serde_json::Value = serde_json::from_slice(&existing)
                .map_err(|error| TransferError::Io(error.to_string()))?;
            let expected = serde_json::json!({
                "migration_id": request.migration_id.to_string(),
                "files": request.files.iter().map(|file| serde_json::json!({
                    "name": file.name,
                    "size": file.size,
                    "digest": bytes_to_hex(&file.digest),
                })).collect::<Vec<_>>(),
            });
            if existing != expected {
                return Err(TransferError::Conflict);
            }
        } else {
            let document = serde_json::json!({
                "migration_id": request.migration_id.to_string(),
                "files": request.files.iter().map(|file| serde_json::json!({
                    "name": file.name,
                    "size": file.size,
                    "digest": bytes_to_hex(&file.digest),
                })).collect::<Vec<_>>(),
            });
            let bytes = serde_json::to_vec(&document)
                .map_err(|error| TransferError::Io(error.to_string()))?;
            write_atomic(&manifest, &bytes)?;
        }
        // The received chunks are persisted, so an interrupted transfer resumes with the
        // chunks that are already durable.
        let mut received: BTreeMap<(u32, u64), (u64, Vec<u8>)> = BTreeMap::new();
        let journal = directory.join("chunks.jsonl");
        if journal.exists() {
            let text = std::fs::read_to_string(&journal)
                .map_err(|error| TransferError::Io(error.to_string()))?;
            for line in text.lines().filter(|line| !line.trim().is_empty()) {
                let entry: serde_json::Value = serde_json::from_str(line)
                    .map_err(|error| TransferError::Io(error.to_string()))?;
                let file_index = entry["file_index"].as_u64().unwrap_or(0) as u32;
                let offset = entry["offset"].as_u64().unwrap_or(0);
                let length = entry["length"].as_u64().unwrap_or(0);
                let digest = entry["digest"]
                    .as_str()
                    .map(hex_to_bytes)
                    .unwrap_or_default();
                received.insert((file_index, offset), (length, digest));
            }
        }
        Ok(Self {
            root: directory,
            migration_id: request.migration_id,
            files: request.files.clone(),
            received,
            written: vec![0u64; request.files.len()],
            total: 0,
        })
    }

    /// Reopens a staging area of a migration after a restart.
    ///
    /// The declared inventory and the received chunks are read back from the staging
    /// directory, so an interrupted transfer resumes with the chunks that are durable.
    pub fn reopen(
        root: &Path,
        migration_id: u128,
        inventory_digest: &[u8],
    ) -> Result<Self, TransferError> {
        let directory = root.join(format!(
            "migration-{migration_id:032x}-inv-{}",
            bytes_to_hex(inventory_digest)
        ));
        let manifest = directory.join("staging.json");
        let text = std::fs::read_to_string(&manifest)
            .map_err(|error| TransferError::Io(error.to_string()))?;
        let document: serde_json::Value =
            serde_json::from_str(&text).map_err(|error| TransferError::Io(error.to_string()))?;
        let mut files = Vec::new();
        for file in document["files"].as_array().cloned().unwrap_or_default() {
            let name = file["name"].as_str().unwrap_or_default().to_string();
            let size = file["size"].as_u64().unwrap_or(0);
            let digest = file["digest"]
                .as_str()
                .map(hex_to_bytes)
                .unwrap_or_default();
            files.push(Q1TransferFile { name, size, digest });
        }
        let request = Q1TransferPrepareRequest {
            migration_id,
            source_node: 0,
            target_node: 0,
            tenant_id: 0,
            topic_id: 0,
            partition_id: 0,
            incarnation: 0,
            original_epoch: 0,
            next_epoch: 0,
            manifest_generation: 0,
            manifest_digest: Vec::new(),
            files,
        };
        Self::prepare(root, &request)
    }

    /// The migration this staging area belongs to.
    pub fn migration_id(&self) -> u128 {
        self.migration_id
    }

    /// The directory that holds the staged bytes.
    pub fn directory(&self) -> &Path {
        &self.root
    }

    fn part_path(&self, file_index: u32) -> PathBuf {
        self.root.join(format!("part-{file_index:06}.bin"))
    }

    /// Accepts one chunk.
    ///
    /// The chunk payload is verified against its digest before it is written. A repeated
    /// chunk with the same identity and the same content is idempotent; the same identity
    /// with different bytes is a conflict and is refused.
    pub fn accept_chunk(
        &mut self,
        request: &Q1TransferChunkRequest,
    ) -> Result<ChunkOutcome, TransferError> {
        if request.migration_id != self.migration_id {
            return Err(TransferError::MigrationMismatch);
        }
        if request.data.len() > MAX_TRANSFER_CHUNK_BYTES {
            return Err(TransferError::TooLarge);
        }
        let index = request.file_index as usize;
        let Some(file) = self.files.get(index) else {
            return Err(TransferError::UnknownFile);
        };
        let end = request.offset.saturating_add(request.data.len() as u64);
        if end > file.size {
            return Err(TransferError::OutOfRange);
        }
        let digest = sha256(&request.data);
        if digest != request.digest {
            return Err(TransferError::DigestMismatch);
        }
        let key = (request.file_index, request.offset);
        if let Some((length, digest)) = self.received.get(&key) {
            if *length == request.data.len() as u64 && digest == &request.digest {
                return Ok(ChunkOutcome::Duplicate);
            }
            return Err(TransferError::Conflict);
        }
        let path = self.part_path(request.file_index);
        let mut handle = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)
            .map_err(|error| TransferError::Io(error.to_string()))?;
        handle
            .seek(SeekFrom::Start(request.offset))
            .and_then(|_| handle.write_all(&request.data))
            .and_then(|()| handle.sync_all())
            .map_err(|error| TransferError::Io(error.to_string()))?;
        self.written[index] = self.written[index].max(end);
        self.total += request.data.len() as u64;
        if self.total > MAX_STAGED_BYTES {
            return Err(TransferError::TooLarge);
        }
        self.received
            .insert(key, (request.data.len() as u64, request.digest.clone()));
        // The chunk index is persisted, so a restart knows what is already durable.
        let journal = self.root.join("chunks.jsonl");
        let mut handle = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&journal)
            .map_err(|error| TransferError::Io(error.to_string()))?;
        let line = serde_json::json!({
            "file_index": request.file_index,
            "offset": request.offset,
            "length": request.data.len(),
            "digest": bytes_to_hex(&request.digest),
        });
        writeln!(handle, "{line}").map_err(|error| TransferError::Io(error.to_string()))?;
        handle
            .sync_all()
            .map_err(|error| TransferError::Io(error.to_string()))?;
        Ok(ChunkOutcome::Stored)
    }

    /// The chunks that are still missing, as (file index, offset) pairs.
    pub fn missing_chunks(&self) -> Vec<(u32, u64)> {
        let mut missing = Vec::new();
        for (index, file) in self.files.iter().enumerate() {
            let mut covered: Vec<(u64, u64)> = self
                .received
                .iter()
                .filter(|((file_index, _), _)| *file_index as usize == index)
                .map(|((_, offset), (length, _))| (*offset, offset + length))
                .collect();
            covered.sort();
            let mut position = 0u64;
            for (start, end) in covered {
                if start > position {
                    missing.push((index as u32, position));
                }
                position = position.max(end);
            }
            if position < file.size {
                missing.push((index as u32, position));
            }
        }
        missing
    }

    /// Verifies the complete inventory.
    ///
    /// Every declared file must be completely covered and must match its declared digest.
    /// The finalization does not publish anything: the caller decides what happens with the
    /// verified files.
    pub fn finalize(
        &self,
        request: &Q1TransferFinalizeRequest,
    ) -> Result<FinalizeOutcome, TransferError> {
        if request.migration_id != self.migration_id {
            return Err(TransferError::MigrationMismatch);
        }
        let missing = self.missing_chunks();
        if !missing.is_empty() {
            return Err(TransferError::Incomplete);
        }
        let mut verified = Vec::with_capacity(self.files.len());
        for (index, file) in self.files.iter().enumerate() {
            let path = self.part_path(index as u32);
            let mut handle =
                File::open(&path).map_err(|error| TransferError::Io(error.to_string()))?;
            let mut hasher = Sha256::new();
            let mut buffer = vec![0u8; 64 * 1024];
            let mut total = 0u64;
            loop {
                let read = handle
                    .read(&mut buffer)
                    .map_err(|error| TransferError::Io(error.to_string()))?;
                if read == 0 {
                    break;
                }
                total += read as u64;
                hasher.update(&buffer[..read]);
            }
            if total != file.size {
                return Err(TransferError::Mismatch);
            }
            let digest = hasher.finalize().to_vec();
            if digest != file.digest {
                return Err(TransferError::Mismatch);
            }
            verified.push((file.name.clone(), file.size, digest));
        }
        Ok(FinalizeOutcome::Complete { files: verified })
    }

    /// Publishes the verified files under their declared names.
    ///
    /// The names were validated when the transfer was prepared, so a published file can
    /// never leave the staging directory.
    pub fn publish(&self) -> Result<Vec<PathBuf>, TransferError> {
        let mut published = Vec::with_capacity(self.files.len());
        for (index, file) in self.files.iter().enumerate() {
            let source = self.part_path(index as u32);
            let target = self.root.join(&file.name);
            std::fs::copy(&source, &target)
                .map_err(|error| TransferError::Io(error.to_string()))?;
            published.push(target);
        }
        Ok(published)
    }
}

/// Owns the staging areas of the incoming transfers of this node.
///
/// Every transfer is bounded: the number of files, the size of a chunk and the total
/// staged bytes are limited, and a chunk never carries a path.
pub struct TransferManager {
    root: PathBuf,
    areas: BTreeMap<(u128, String), TransferStaging>,
}

impl TransferManager {
    /// Creates a manager with the given staging root.
    pub fn new(root: impl AsRef<Path>) -> Self {
        let root = root.as_ref().to_path_buf();
        let _ = std::fs::create_dir_all(&root);
        Self {
            root,
            areas: BTreeMap::new(),
        }
    }

    /// The staging root.
    pub fn root(&self) -> &Path {
        &self.root
    }

    fn area(
        &mut self,
        migration_id: u128,
        inventory_digest: &[u8],
    ) -> Result<&mut TransferStaging, TransferError> {
        let key = (migration_id, bytes_to_hex(inventory_digest));
        if !self.areas.contains_key(&key) {
            let area = TransferStaging::reopen(&self.root, migration_id, inventory_digest)?;
            self.areas.insert(key.clone(), area);
        }
        Ok(self.areas.get_mut(&key).expect("the area exists"))
    }

    /// Accepts a transfer preparation.
    pub fn prepare(
        &mut self,
        request: &Q1TransferPrepareRequest,
    ) -> Result<Q1TransferPrepareResponse, TransferError> {
        let area = TransferStaging::prepare(&self.root, request)?;
        let staging_id = request.migration_id;
        self.areas.insert(
            (request.migration_id, bytes_to_hex(&request.manifest_digest)),
            area,
        );
        Ok(Q1TransferPrepareResponse {
            accepted: true,
            staging_id,
            error: None,
        })
    }

    /// Accepts one transfer chunk.
    pub fn chunk(
        &mut self,
        request: &Q1TransferChunkRequest,
    ) -> Result<Q1TransferChunkResponse, TransferError> {
        let area = self.area(request.migration_id, &request.inventory_digest)?;
        match area.accept_chunk(request) {
            Ok(ChunkOutcome::Stored) => Ok(Q1TransferChunkResponse {
                accepted: true,
                duplicate: false,
                error: None,
            }),
            Ok(ChunkOutcome::Duplicate) => Ok(Q1TransferChunkResponse {
                accepted: true,
                duplicate: true,
                error: None,
            }),
            Err(error) => Ok(Q1TransferChunkResponse {
                accepted: false,
                duplicate: false,
                error: Some(match error {
                    TransferError::DigestMismatch | TransferError::Conflict => {
                        rovqet_control::rpc::RpcErrorCode::Malformed
                    }
                    TransferError::OutOfRange | TransferError::UnknownFile => {
                        rovqet_control::rpc::RpcErrorCode::Malformed
                    }
                    TransferError::TooLarge => rovqet_control::rpc::RpcErrorCode::Capacity,
                    _ => rovqet_control::rpc::RpcErrorCode::Internal,
                }),
            }),
        }
    }

    /// Verifies the complete transferred inventory.
    pub fn finalize(
        &mut self,
        request: &Q1TransferFinalizeRequest,
    ) -> Result<Q1TransferFinalizeResponse, TransferError> {
        let area = self.area(request.migration_id, &request.inventory_digest)?;
        match area.finalize(request) {
            Ok(FinalizeOutcome::Complete { .. }) => {
                // Without the fence evidence the inventory is complete but the target is not
                // allowed to activate yet.
                let status = if request.fence_offset.is_some() {
                    Q1TransferStatus::ReadyForActivation
                } else {
                    Q1TransferStatus::ReadyForFence
                };
                Ok(Q1TransferFinalizeResponse {
                    status,
                    error: None,
                })
            }
            Err(TransferError::Incomplete) => Ok(Q1TransferFinalizeResponse {
                status: Q1TransferStatus::Mismatch,
                error: Some(rovqet_control::rpc::RpcErrorCode::Malformed),
            }),
            Err(error) => Ok(Q1TransferFinalizeResponse {
                status: Q1TransferStatus::Mismatch,
                error: Some(match error {
                    TransferError::Mismatch => rovqet_control::rpc::RpcErrorCode::Malformed,
                    TransferError::TooLarge => rovqet_control::rpc::RpcErrorCode::Capacity,
                    _ => rovqet_control::rpc::RpcErrorCode::Internal,
                }),
            }),
        }
    }
}

/// The canonical digest of a transferred inventory.
///
/// It covers the file names, the sizes and the file digests, so two inventories are equal
/// exactly when they describe the same durable history.
pub fn inventory_digest(files: &[Q1TransferFile]) -> Vec<u8> {
    let mut hasher = Sha256::new();
    let mut sorted: Vec<&Q1TransferFile> = files.iter().collect();
    sorted.sort_by(|left, right| left.name.cmp(&right.name));
    for file in sorted {
        hasher.update(file.name.as_bytes());
        hasher.update(file.size.to_be_bytes());
        hasher.update(&file.digest);
    }
    hasher.finalize().to_vec()
}

/// The hexadecimal form of a digest.
pub fn bytes_to_hex(bytes: &[u8]) -> String {
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        encoded.push_str(&format!("{byte:02x}"));
    }
    encoded
}

/// Decodes a hexadecimal digest.
pub fn hex_to_bytes(value: &str) -> Vec<u8> {
    let bytes = value.as_bytes();
    let mut decoded = Vec::with_capacity(bytes.len() / 2);
    let mut index = 0;
    while index + 1 < bytes.len() {
        let high = (bytes[index] as char).to_digit(16).unwrap_or(0) as u8;
        let low = (bytes[index + 1] as char).to_digit(16).unwrap_or(0) as u8;
        decoded.push((high << 4) | low);
        index += 2;
    }
    decoded
}

/// The digest of one buffer.
pub fn sha256(data: &[u8]) -> Vec<u8> {
    let mut hasher = Sha256::new();
    hasher.update(data);
    hasher.finalize().to_vec()
}

/// Atomically replaces one file.
fn write_atomic(path: &Path, bytes: &[u8]) -> Result<(), TransferError> {
    let mut temporary = path.to_path_buf();
    let mut name = path
        .file_name()
        .map(|name| name.to_string_lossy().to_string())
        .unwrap_or_default();
    name.push_str(".tmp");
    temporary.set_file_name(name);
    {
        let mut handle =
            File::create(&temporary).map_err(|error| TransferError::Io(error.to_string()))?;
        handle
            .write_all(bytes)
            .and_then(|()| handle.sync_all())
            .map_err(|error| TransferError::Io(error.to_string()))?;
    }
    std::fs::rename(&temporary, path).map_err(|error| TransferError::Io(error.to_string()))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use rovqet_control::rpc::{Q1TransferFinalizeRequest, Q1TransferPrepareRequest};

    fn prepare(migration_id: u128, files: Vec<(&str, u64, Vec<u8>)>) -> Q1TransferPrepareRequest {
        Q1TransferPrepareRequest {
            migration_id,
            source_node: 1,
            target_node: 2,
            tenant_id: 1,
            topic_id: 3,
            partition_id: 0,
            incarnation: 77,
            original_epoch: 5,
            next_epoch: 6,
            manifest_generation: 3,
            manifest_digest: vec![1u8; 32],
            files: files
                .into_iter()
                .map(|(name, size, digest)| Q1TransferFile {
                    name: name.to_string(),
                    size,
                    digest,
                })
                .collect(),
        }
    }

    fn chunk(
        migration_id: u128,
        index: u64,
        file_index: u32,
        offset: u64,
        data: Vec<u8>,
    ) -> Q1TransferChunkRequest {
        let digest = sha256(&data);
        Q1TransferChunkRequest {
            migration_id,
            inventory_digest: vec![9u8; 32],
            chunk_index: index,
            file_index,
            offset,
            data,
            digest,
        }
    }

    fn finalize(migration_id: u128) -> Q1TransferFinalizeRequest {
        Q1TransferFinalizeRequest {
            migration_id,
            inventory_digest: vec![9u8; 32],
            final_digest: vec![2u8; 32],
            last_committed_offset: 10,
            transfer_watermark: 10,
            manifest_generation: 3,
            fence_offset: None,
            fence_digest: None,
            old_epoch: 5,
        }
    }

    #[test]
    fn a_complete_inventory_is_verified() {
        let directory = tempfile::tempdir().expect("temp dir");
        let payload = b"q1 wal bytes".to_vec();
        let digest = sha256(&payload);
        let request = prepare(1, vec![("seg-000000.wal", payload.len() as u64, digest)]);
        let mut staging = TransferStaging::prepare(directory.path(), &request).expect("prepare");
        assert_eq!(
            staging
                .accept_chunk(&chunk(1, 0, 0, 0, payload.clone()))
                .expect("chunk"),
            ChunkOutcome::Stored
        );
        let outcome = staging.finalize(&finalize(1)).expect("finalize");
        match outcome {
            FinalizeOutcome::Complete { files } => {
                assert_eq!(files.len(), 1);
                assert_eq!(files[0].0, "seg-000000.wal");
                assert_eq!(files[0].1, payload.len() as u64);
            }
        }
    }

    #[test]
    fn a_repeated_chunk_is_idempotent_and_a_conflicting_chunk_is_refused() {
        let directory = tempfile::tempdir().expect("temp dir");
        let payload = b"q1 wal bytes".to_vec();
        let digest = sha256(&payload);
        let request = prepare(1, vec![("seg-000000.wal", payload.len() as u64, digest)]);
        let mut staging = TransferStaging::prepare(directory.path(), &request).expect("prepare");
        assert_eq!(
            staging
                .accept_chunk(&chunk(1, 0, 0, 0, payload.clone()))
                .expect("chunk"),
            ChunkOutcome::Stored
        );
        assert_eq!(
            staging
                .accept_chunk(&chunk(1, 0, 0, 0, payload.clone()))
                .expect("chunk"),
            ChunkOutcome::Duplicate
        );
        // The same identity with different bytes is a conflict. The payload keeps the
        // length of the declared file so that it is in range.
        let other = b"q1 wal bytFs".to_vec();
        let conflict = chunk(1, 0, 0, 0, other);
        assert_eq!(
            staging.accept_chunk(&conflict),
            Err(TransferError::Conflict)
        );
    }

    #[test]
    fn a_chunk_with_a_wrong_digest_is_refused() {
        let directory = tempfile::tempdir().expect("temp dir");
        let payload = b"q1 wal bytes".to_vec();
        let digest = sha256(&payload);
        let request = prepare(1, vec![("seg-000000.wal", payload.len() as u64, digest)]);
        let mut staging = TransferStaging::prepare(directory.path(), &request).expect("prepare");
        let mut broken = chunk(1, 0, 0, 0, payload);
        broken.digest = vec![0u8; 32];
        assert_eq!(
            staging.accept_chunk(&broken),
            Err(TransferError::DigestMismatch)
        );
    }

    #[test]
    fn a_missing_chunk_prevents_the_finalize() {
        let directory = tempfile::tempdir().expect("temp dir");
        let first = b"first half ".to_vec();
        let second = b"second half".to_vec();
        let mut whole = first.clone();
        whole.extend_from_slice(&second);
        let request = prepare(
            1,
            vec![("seg-000000.wal", whole.len() as u64, sha256(&whole))],
        );
        let mut staging = TransferStaging::prepare(directory.path(), &request).expect("prepare");
        staging
            .accept_chunk(&chunk(1, 0, 0, 0, first.clone()))
            .expect("chunk");
        // The second half is missing.
        assert_eq!(staging.missing_chunks(), vec![(0, first.len() as u64)]);
        assert_eq!(
            staging.finalize(&finalize(1)),
            Err(TransferError::Incomplete)
        );
        // Out of order arrival is accepted once the gap is filled.
        staging
            .accept_chunk(&chunk(1, 1, 0, first.len() as u64, second))
            .expect("chunk");
        assert!(staging.missing_chunks().is_empty());
        assert!(staging.finalize(&finalize(1)).is_ok());
    }

    #[test]
    fn a_chunk_outside_the_declared_file_is_refused() {
        let directory = tempfile::tempdir().expect("temp dir");
        let payload = b"q1 wal bytes".to_vec();
        let request = prepare(
            1,
            vec![("seg-000000.wal", payload.len() as u64, sha256(&payload))],
        );
        let mut staging = TransferStaging::prepare(directory.path(), &request).expect("prepare");
        assert_eq!(
            staging.accept_chunk(&chunk(1, 0, 0, 4096, payload)),
            Err(TransferError::OutOfRange)
        );
        assert_eq!(
            staging.accept_chunk(&chunk(1, 0, 7, 0, b"x".to_vec())),
            Err(TransferError::UnknownFile)
        );
        assert_eq!(
            staging.accept_chunk(&chunk(2, 0, 0, 0, b"x".to_vec())),
            Err(TransferError::MigrationMismatch)
        );
    }

    #[test]
    fn a_prepare_with_an_invalid_name_or_a_changed_inventory_is_refused() {
        let directory = tempfile::tempdir().expect("temp dir");
        let payload = b"q1 wal bytes".to_vec();
        let escaping = prepare(
            1,
            vec![("../escape", payload.len() as u64, sha256(&payload))],
        );
        assert_eq!(
            TransferStaging::prepare(directory.path(), &escaping).unwrap_err(),
            TransferError::InvalidName
        );
        let request = prepare(
            2,
            vec![("seg-000000.wal", payload.len() as u64, sha256(&payload))],
        );
        TransferStaging::prepare(directory.path(), &request).expect("prepare");
        let changed = prepare(
            2,
            vec![("seg-000001.wal", payload.len() as u64, sha256(&payload))],
        );
        assert_eq!(
            TransferStaging::prepare(directory.path(), &changed).unwrap_err(),
            TransferError::Conflict
        );
    }

    #[test]
    fn a_file_that_does_not_match_its_digest_is_refused() {
        let directory = tempfile::tempdir().expect("temp dir");
        let payload = b"q1 wal bytes".to_vec();
        // The declared digest does not describe the transferred bytes.
        let request = prepare(
            1,
            vec![("seg-000000.wal", payload.len() as u64, vec![0u8; 32])],
        );
        let mut staging = TransferStaging::prepare(directory.path(), &request).expect("prepare");
        staging
            .accept_chunk(&chunk(1, 0, 0, 0, payload))
            .expect("chunk");
        assert_eq!(staging.finalize(&finalize(1)), Err(TransferError::Mismatch));
    }
}
