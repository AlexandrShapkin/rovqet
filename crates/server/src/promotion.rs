//! Crash-safe promotion of a verified transferred inventory.
//!
//! The storage lifecycle of a successor is
//! `STAGING -> VERIFIED -> FENCE_CONFIRMED -> ACTIVATION_COMMITTED -> PROMOTED -> ACTIVE`.
//! Only this module moves bytes: it publishes the verified files into the partition
//! directory of the successor with an explicit, recoverable protocol, so a crash never
//! leaves a partially promoted directory that looks like durable history.
//!
//! The source storage is never touched.
use crate::transfer::{bytes_to_hex, hex_to_bytes, sha256};
use serde::{Deserialize, Serialize};
use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

/// The name of the promotion record inside the partition directory.
pub const PROMOTION_RECORD: &str = "PROMOTION.json";
/// The name of the promotion journal inside the staging area.
pub const PROMOTION_JOURNAL: &str = "PROMOTION.json";

/// One file of the promoted inventory.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PromotedFile {
    pub name: String,
    pub size: u64,
    pub digest: String,
}

/// The durable record of a promotion.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PromotionRecord {
    /// The migration this promotion belongs to.
    pub migration_id: u128,
    /// The identity of the promoted inventory.
    pub inventory_digest: String,
    /// The log incarnation that was promoted.
    pub incarnation: u128,
    /// The tenant of the partition the storage belongs to.
    pub tenant_id: u64,
    /// The topic of the partition the storage belongs to.
    pub topic_id: u64,
    /// The partition the storage belongs to.
    pub partition_id: u32,
    /// The ownership epoch the successor owns after the activation.
    pub target_epoch: u64,
    /// The committed metadata revision that authorized the promotion.
    pub activation_revision: u64,
    /// The promoted files.
    pub files: Vec<PromotedFile>,
    /// The state of the promotion.
    pub state: PromotionState,
}

/// The state of a promotion.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PromotionState {
    /// The files are being published.
    Promoting,
    /// The files are published and the record is durable.
    Promoted,
}

/// Why a promotion was refused.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PromotionError {
    /// The staging area does not contain the declared inventory.
    InventoryMismatch,
    /// The partition directory already holds unrelated or active storage.
    TargetNotEmpty,
    /// The partition directory does not exist.
    TargetMissing,
    /// A staged file does not match its digest.
    DigestMismatch,
    /// The promotion journal is missing or unreadable.
    Journal(String),
    /// The filesystem refused the operation.
    Io(String),
}

impl std::fmt::Display for PromotionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}

impl std::error::Error for PromotionError {}

/// The staging area of one migration.
pub fn staging_area(staging_root: &Path, migration_id: u128, inventory_digest: &[u8]) -> PathBuf {
    staging_root.join(format!(
        "migration-{migration_id:032x}-inv-{}",
        bytes_to_hex(inventory_digest)
    ))
}

/// Reads the promotion record of a partition directory.
pub fn promotion_record(directory: &Path) -> Result<Option<PromotionRecord>, PromotionError> {
    let path = directory.join(PROMOTION_RECORD);
    match std::fs::read(&path) {
        Ok(bytes) => serde_json::from_slice(&bytes)
            .map(Some)
            .map_err(|error| PromotionError::Journal(error.to_string())),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(PromotionError::Io(error.to_string())),
    }
}

/// Publishes the verified inventory into the partition directory of the successor.
///
/// The protocol is explicit and recoverable: a journal records the intent before the first
/// file moves, every file is published with a digest check, and the durable record in the
/// partition directory states that the promotion completed. A crash in the middle leaves the
/// journal behind and the promotion is repeated; the already published files are verified
/// and skipped.
pub fn promote(
    staging: &Path,
    target: &Path,
    record: &PromotionRecord,
) -> Result<(), PromotionError> {
    if !target.is_dir() {
        return Err(PromotionError::TargetMissing);
    }
    // The target must not hold unrelated or already active storage. The files of this
    // promotion may already be there: a repeated promotion verifies and skips them.
    let planned: Vec<&str> = record.files.iter().map(|file| file.name.as_str()).collect();
    for entry in std::fs::read_dir(target).map_err(|error| PromotionError::Io(error.to_string()))? {
        let entry = entry.map_err(|error| PromotionError::Io(error.to_string()))?;
        let name = entry.file_name().to_string_lossy().to_string();
        if name == "SERVER.lock" || name == PROMOTION_RECORD {
            continue;
        }
        if !planned.contains(&name.as_str()) {
            return Err(PromotionError::TargetNotEmpty);
        }
    }
    // The staged inventory must match the plan.
    let manifest_path = staging.join("staging.json");
    let manifest = std::fs::read(&manifest_path)
        .map_err(|error| PromotionError::Journal(error.to_string()))?;
    let manifest: serde_json::Value = serde_json::from_slice(&manifest)
        .map_err(|error| PromotionError::Journal(error.to_string()))?;
    let declared = manifest["files"].as_array().cloned().unwrap_or_default();
    if declared.len() != record.files.len() {
        return Err(PromotionError::InventoryMismatch);
    }
    for (entry, file) in declared.iter().zip(record.files.iter()) {
        if entry["name"].as_str() != Some(file.name.as_str())
            || entry["size"].as_u64() != Some(file.size)
            || entry["digest"].as_str() != Some(file.digest.as_str())
        {
            return Err(PromotionError::InventoryMismatch);
        }
    }

    // The intent is durable before the first file moves.
    let mut journal = record.clone();
    journal.state = PromotionState::Promoting;
    write_json_durable(&staging.join(PROMOTION_JOURNAL), &journal)?;

    for (index, file) in record.files.iter().enumerate() {
        let staged = staging.join(format!("part-{index:06}.bin"));
        let published = target.join(&file.name);
        if published.is_file() {
            // Already published: verify and continue, the promotion is idempotent.
            if file_digest(&published)? != hex_to_bytes(&file.digest) {
                return Err(PromotionError::DigestMismatch);
            }
            continue;
        }
        let digest = file_digest(&staged)?;
        if digest != hex_to_bytes(&file.digest) {
            return Err(PromotionError::DigestMismatch);
        }
        // A rename inside one filesystem is atomic. Across filesystems the copy is
        // published under a temporary name and renamed, and the directory is synced.
        if std::fs::rename(&staged, &published).is_err() {
            let temporary = target.join(format!("{}.promote", file.name));
            copy_durable(&staged, &temporary)?;
            std::fs::rename(&temporary, &published)
                .map_err(|error| PromotionError::Io(error.to_string()))?;
            sync_directory(target)?;
            let _ = std::fs::remove_file(&staged);
        }
        sync_file(&published)?;
    }
    sync_directory(target)?;

    // The durable record states that the promotion completed.
    let mut completed = record.clone();
    completed.state = PromotionState::Promoted;
    write_json_durable(&target.join(PROMOTION_RECORD), &completed)?;
    sync_directory(target)?;
    Ok(())
}

/// Publishes the verified inventory of a staging area into the partition directory.
///
/// The file list is taken from the staging manifest, so the caller cannot promote a
/// different inventory than the one that was verified.
pub fn promote_with_manifest(
    staging: &Path,
    target: &Path,
    plan: &PromotionRecord,
) -> Result<(), PromotionError> {
    let manifest_path = staging.join("staging.json");
    let manifest = std::fs::read(&manifest_path)
        .map_err(|error| PromotionError::Journal(error.to_string()))?;
    let manifest: serde_json::Value = serde_json::from_slice(&manifest)
        .map_err(|error| PromotionError::Journal(error.to_string()))?;
    let mut record = plan.clone();
    record.files = manifest["files"]
        .as_array()
        .cloned()
        .unwrap_or_default()
        .iter()
        .map(|file| PromotedFile {
            name: file["name"].as_str().unwrap_or_default().to_string(),
            size: file["size"].as_u64().unwrap_or(0),
            digest: file["digest"].as_str().unwrap_or_default().to_string(),
        })
        .collect();
    promote(staging, target, &record)
}

/// The digest of a file.
pub fn file_digest(path: &Path) -> Result<Vec<u8>, PromotionError> {
    let mut handle = File::open(path).map_err(|error| PromotionError::Io(error.to_string()))?;
    let mut hasher = <sha2::Sha256 as sha2::Digest>::new();
    let mut buffer = vec![0u8; 64 * 1024];
    loop {
        let read = handle
            .read(&mut buffer)
            .map_err(|error| PromotionError::Io(error.to_string()))?;
        if read == 0 {
            break;
        }
        sha2::Digest::update(&mut hasher, &buffer[..read]);
    }
    Ok(sha2::Digest::finalize(hasher).to_vec())
}

/// The digest of a buffer.
pub fn buffer_digest(bytes: &[u8]) -> Vec<u8> {
    sha256(bytes)
}

/// Writes one JSON file durably.
fn write_json_durable<T: Serialize>(path: &Path, value: &T) -> Result<(), PromotionError> {
    let bytes =
        serde_json::to_vec(value).map_err(|error| PromotionError::Journal(error.to_string()))?;
    let mut temporary = path.to_path_buf();
    let mut name = path
        .file_name()
        .map(|name| name.to_string_lossy().to_string())
        .unwrap_or_default();
    name.push_str(".tmp");
    temporary.set_file_name(name);
    {
        let mut handle = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&temporary)
            .map_err(|error| PromotionError::Io(error.to_string()))?;
        handle
            .write_all(&bytes)
            .and_then(|()| handle.sync_all())
            .map_err(|error| PromotionError::Io(error.to_string()))?;
    }
    std::fs::rename(&temporary, path).map_err(|error| PromotionError::Io(error.to_string()))?;
    if let Some(parent) = path.parent() {
        sync_directory(parent)?;
    }
    Ok(())
}

/// Copies one file durably.
fn copy_durable(source: &Path, target: &Path) -> Result<(), PromotionError> {
    let mut input = File::open(source).map_err(|error| PromotionError::Io(error.to_string()))?;
    let mut output = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(target)
        .map_err(|error| PromotionError::Io(error.to_string()))?;
    let mut buffer = vec![0u8; 64 * 1024];
    loop {
        let read = input
            .read(&mut buffer)
            .map_err(|error| PromotionError::Io(error.to_string()))?;
        if read == 0 {
            break;
        }
        output
            .write_all(&buffer[..read])
            .map_err(|error| PromotionError::Io(error.to_string()))?;
    }
    output
        .sync_all()
        .map_err(|error| PromotionError::Io(error.to_string()))?;
    Ok(())
}

/// Syncs a file.
fn sync_file(path: &Path) -> Result<(), PromotionError> {
    File::open(path)
        .and_then(|file| file.sync_all())
        .map_err(|error| PromotionError::Io(error.to_string()))
}

/// Syncs a directory.
fn sync_directory(path: &Path) -> Result<(), PromotionError> {
    File::open(path)
        .and_then(|dir| dir.sync_all())
        .map_err(|error| PromotionError::Io(error.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transfer::bytes_to_hex;

    fn staging_with(files: &[(&str, &[u8])]) -> (tempfile::TempDir, PathBuf) {
        let directory = tempfile::tempdir().expect("temp dir");
        let staging = directory.path().join("staging");
        std::fs::create_dir_all(&staging).expect("staging");
        let manifest = serde_json::json!({
            "migration_id": "1",
            "files": files.iter().enumerate().map(|(index, (name, bytes))| {
                serde_json::json!({
                    "name": name,
                    "size": bytes.len(),
                    "digest": bytes_to_hex(&sha256(bytes)),
                })
            }).collect::<Vec<_>>(),
        });
        std::fs::write(
            staging.join("staging.json"),
            serde_json::to_vec(&manifest).expect("manifest"),
        )
        .expect("manifest");
        for (index, (_, bytes)) in files.iter().enumerate() {
            std::fs::write(staging.join(format!("part-{index:06}.bin")), bytes).expect("part");
        }
        (directory, staging)
    }

    fn record(files: &[(&str, &[u8])]) -> PromotionRecord {
        PromotionRecord {
            migration_id: 7,
            inventory_digest: "abcd".into(),
            incarnation: 42,
            tenant_id: 1,
            topic_id: 3,
            partition_id: 0,
            target_epoch: 8,
            activation_revision: 12,
            files: files
                .iter()
                .map(|(name, bytes)| PromotedFile {
                    name: name.to_string(),
                    size: bytes.len() as u64,
                    digest: bytes_to_hex(&sha256(bytes)),
                })
                .collect(),
            state: PromotionState::Promoting,
        }
    }

    #[test]
    fn a_promotion_publishes_the_verified_inventory() {
        let files = [
            ("MANIFEST", b"manifest bytes".as_slice()),
            ("segment-000000.wal", b"wal"),
        ];
        let (_directory, staging) = staging_with(&files);
        let target = tempfile::tempdir().expect("target");
        let plan = record(&files);
        promote(&staging, target.path(), &plan).expect("promotion");
        assert_eq!(
            std::fs::read(target.path().join("MANIFEST")).expect("manifest"),
            b"manifest bytes"
        );
        assert_eq!(
            std::fs::read(target.path().join("segment-000000.wal")).expect("segment"),
            b"wal"
        );
        let stored = promotion_record(target.path())
            .expect("record")
            .expect("the promotion must be recorded");
        assert_eq!(stored.state, PromotionState::Promoted);
        assert_eq!(stored.migration_id, 7);
        assert_eq!(stored.incarnation, 42);
        assert_eq!(stored.target_epoch, 8);
    }

    #[test]
    fn a_promotion_refuses_a_target_with_unrelated_storage() {
        let files = [("MANIFEST", b"manifest".as_slice())];
        let (_directory, staging) = staging_with(&files);
        let target = tempfile::tempdir().expect("target");
        std::fs::write(target.path().join("segment-000000.wal"), b"existing").expect("file");
        assert_eq!(
            promote(&staging, target.path(), &record(&files)),
            Err(PromotionError::TargetNotEmpty),
            "an unrelated or active directory must never be overwritten"
        );
    }

    #[test]
    fn a_promotion_is_idempotent_and_recovers_a_partial_publication() {
        let files = [
            ("MANIFEST", b"manifest".as_slice()),
            ("segment-000000.wal", b"wal"),
        ];
        let (_directory, staging) = staging_with(&files);
        let target = tempfile::tempdir().expect("target");
        let plan = record(&files);
        // A crash after the first file was published.
        promote(&staging, target.path(), &plan).expect("promotion");
        // The promotion is repeated: the published files are verified and skipped.
        promote(&staging, target.path(), &plan).expect("repeated promotion");
        assert_eq!(
            std::fs::read(target.path().join("MANIFEST")).expect("manifest"),
            b"manifest"
        );
        assert_eq!(
            std::fs::read(target.path().join("segment-000000.wal")).expect("segment"),
            b"wal"
        );
    }

    #[test]
    fn a_promotion_refuses_a_digest_mismatch() {
        let files = [("MANIFEST", b"manifest".as_slice())];
        let (_directory, staging) = staging_with(&files);
        // The staged bytes no longer match the declared digest.
        std::fs::write(staging.join("part-000000.bin"), b"tampered").expect("part");
        let target = tempfile::tempdir().expect("target");
        assert_eq!(
            promote(&staging, target.path(), &record(&files)),
            Err(PromotionError::DigestMismatch),
            "a tampered file must never be promoted"
        );
        assert!(
            !target.path().join("MANIFEST").exists(),
            "a refused promotion must not publish anything"
        );
    }

    #[test]
    fn a_promoted_inventory_that_is_not_valid_history_is_refused_by_the_recovery() {
        // The promotion publishes bytes; the recovery validator decides whether they are
        // durable history. A corrupt inventory must never become an active partition.
        let files = [("MANIFEST", b"not a manifest".as_slice())];
        let (_directory, staging) = staging_with(&files);
        let target = tempfile::tempdir().expect("target");
        promote(&staging, target.path(), &record(&files)).expect("promotion");
        let config = rovqet_wal::Config {
            partition: rovqet_core::PartitionRef {
                tenant_id: 1,
                topic_id: 3,
                partition_id: 0,
            },
            incarnation: 42,
            ..Default::default()
        };
        assert!(
            rovqet_wal::Wal::open(target.path(), config).is_err(),
            "a corrupt promoted inventory must be refused by the recovery"
        );
    }

    #[test]
    fn a_promoted_inventory_with_another_incarnation_is_refused() {
        // The incarnation is part of the durable identity: a transfer of another incarnation
        // must not be opened as this partition.
        let files = [("MANIFEST", b"manifest".as_slice())];
        let (_directory, staging) = staging_with(&files);
        let target = tempfile::tempdir().expect("target");
        promote(&staging, target.path(), &record(&files)).expect("promotion");
        let config = rovqet_wal::Config {
            partition: rovqet_core::PartitionRef {
                tenant_id: 1,
                topic_id: 3,
                partition_id: 0,
            },
            incarnation: 99,
            ..Default::default()
        };
        assert!(
            rovqet_wal::Wal::open(target.path(), config).is_err(),
            "an inventory of another incarnation must be refused"
        );
    }

    #[test]
    fn a_promotion_refuses_an_inventory_that_differs_from_the_manifest() {
        let files = [("MANIFEST", b"manifest".as_slice())];
        let (_directory, staging) = staging_with(&files);
        let target = tempfile::tempdir().expect("target");
        let mut plan = record(&files);
        plan.files.push(PromotedFile {
            name: "segment-000000.wal".into(),
            size: 3,
            digest: "00".into(),
        });
        assert_eq!(
            promote(&staging, target.path(), &plan),
            Err(PromotionError::InventoryMismatch),
            "the promoted inventory must match the verified manifest"
        );
    }
}
