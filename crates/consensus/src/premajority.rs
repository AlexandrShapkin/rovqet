//! Deterministic pre-majority fault seam for the C-04 "leader crash before a durable
//! majority" proof.
//!
//! # Compiled only for `fault-injection`
//!
//! The whole seam — including every environment read — exists only in a build with
//! the `fault-injection` cargo feature. A default build compiles only the inert
//! entry points at the end of this module: they never read an environment variable,
//! never open a barrier directory and can never write a barrier document, so a
//! default build has no active premajority behavior at all.
//!
//! # Production disabled by construction (fault-injection builds)
//!
//! In a fault-injection build the seam is compiled in but inert until both of the
//! following hold:
//!
//! * the `ROVQET_Q2_PREMAJORITY_BARRIER` environment variable names a directory, and
//! * that directory holds an `arm.json` naming exactly one producer token.
//!
//! A test harness arms it for a single publication, so the failure is deterministic:
//! no sleeps, no probabilities, no fabricated acknowledgment and no retry guessing.
//! A broker started without the environment variable behaves exactly as before.
//!
//! # What the seam observes and what it holds
//!
//! [`hold_publish_replication`] is called by the bounded TCP transport immediately
//! before it sends `AppendEntries`. It engages only for an entry that carries the
//! armed [`ShardCommand::Publish`](crate::types::ShardCommand::Publish), and only
//! after [`observe_durable`] has recorded that this node's own log reached that exact
//! index through `fsync` (`FileLogStorage::append` publishes it after a successful
//! `sync_all`). The barrier therefore witnesses a real local durable append rather
//! than an in-memory one.
//!
//! It then writes an externally observable `barrier.json` — node, log index, durable
//! index and the armed token — and never returns, so the entry cannot reach a quorum
//! and no `COMMITTED_QUORUM` acknowledgment can exist for it. The hold happens inside
//! the per-follower replication task before any socket is opened: it takes no
//! state-machine lock, no storage lock and no raft-core lock, and openraft's
//! unbounded replication channel keeps the core running while the harness reads the
//! barrier, SIGKILLs the named leader and later retries the same token.

#[cfg(feature = "fault-injection")]
mod active {
    use crate::types::{ShardCommand, ShardNodeId, ShardRaftTypeConfig};
    use openraft::{Entry, EntryPayload};
    use std::path::{Path, PathBuf};
    use std::sync::OnceLock;
    use std::sync::atomic::{AtomicBool, Ordering};
    use tokio::sync::watch;

    /// Environment variable naming the barrier directory. Unset means the seam is off.
    pub const DIRECTORY_ENV: &str = "ROVQET_Q2_PREMAJORITY_BARRIER";

    /// Arm document inside the barrier directory: the one token the seam may hold.
    pub const ARM_FILE: &str = "arm.json";

    /// Externally observable barrier document written at the pre-majority point.
    pub const BARRIER_FILE: &str = "barrier.json";

    /// The single producer token the harness armed the seam for.
    #[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
    pub struct ArmedToken {
        /// Producer identity of the publication that may be held.
        pub producer_id: u128,
        /// Producer epoch of the publication that may be held.
        pub producer_epoch: u64,
        /// Producer sequence of the publication that may be held.
        pub sequence: u64,
        /// Message identity the publication carries.
        pub message_id: u128,
    }

    /// One armed, production-disabled pre-majority seam.
    pub struct Premajority {
        directory: PathBuf,
        /// Last log index this node has durably appended, published after `fsync`.
        durable: watch::Sender<u64>,
        /// The barrier is written at most once per process.
        fired: AtomicBool,
    }

    static SEAM: OnceLock<Option<Premajority>> = OnceLock::new();

    /// The process-wide seam, initialized from [`DIRECTORY_ENV`] on first use.
    fn seam() -> Option<&'static Premajority> {
        SEAM.get_or_init(|| {
            let directory = std::env::var_os(DIRECTORY_ENV).map(PathBuf::from)?;
            Some(Premajority::open(directory))
        })
        .as_ref()
    }

    impl Premajority {
        /// Opens a seam over `directory`. Without a later [`Self::arm`] document the seam
        /// holds nothing, whatever else is configured.
        pub fn open(directory: impl Into<PathBuf>) -> Self {
            let (durable, _) = watch::channel(0u64);
            Self {
                directory: directory.into(),
                durable,
                fired: AtomicBool::new(false),
            }
        }

        /// Records that the durable log reached `index`, called only after `fsync`.
        pub fn observe_durable(&self, index: u64) {
            self.durable.send_modify(|seen| {
                if *seen < index {
                    *seen = index;
                }
            });
        }

        /// The armed token, read from the arm document, or `None` when not armed.
        pub fn armed(&self) -> Option<ArmedToken> {
            let bytes = std::fs::read(self.directory.join(ARM_FILE)).ok()?;
            serde_json::from_slice(&bytes).ok()
        }

        /// Holds the replication of the armed publication until the process dies.
        ///
        /// Returns immediately — without any barrier — when the seam is unarmed, when the
        /// request carries no armed publication, or when this node's durable log has not
        /// reached the entry yet. Only a durable, armed publication is held.
        pub async fn hold_publish_replication(
            &self,
            local: ShardNodeId,
            entries: &[Entry<ShardRaftTypeConfig>],
        ) {
            let Some(token) = self.armed() else {
                return;
            };
            let Some(entry) = entries
                .iter()
                .find(|entry| is_armed_publish(&entry.payload, &token))
            else {
                return;
            };
            let index = entry.log_id.index;
            // The barrier may only claim durability this node actually completed.
            let mut durable = self.durable.subscribe();
            if durable.wait_for(|seen| *seen >= index).await.is_err() {
                return;
            }
            let observed = *durable.borrow_and_update();
            self.emit_barrier(local, index, observed, &token);
            // Never released: the harness SIGKILLs the node the barrier names. This parks
            // one replication task and nothing else.
            std::future::pending::<()>().await;
        }

        /// Writes the externally observable barrier document, at most once.
        fn emit_barrier(&self, node: ShardNodeId, index: u64, durable: u64, token: &ArmedToken) {
            if self.fired.swap(true, Ordering::SeqCst) {
                return;
            }
            let document = serde_json::json!({
                "event": "premajority_barrier",
                "node": node,
                "log_index": index,
                "durable_index": durable,
                "producer_id": token.producer_id,
                "producer_epoch": token.producer_epoch,
                "sequence": token.sequence,
                "message_id": token.message_id,
            });
            let Ok(bytes) = serde_json::to_vec_pretty(&document) else {
                return;
            };
            // A barrier that cannot be written is never silently skipped: the seam still
            // holds, and the harness reports the missing document as a failed observation.
            let _ = write_durable(&self.directory.join(BARRIER_FILE), &bytes);
        }
    }

    /// Publishes the durable completion of `index` (called after `fsync`).
    pub fn observe_durable(index: u64) {
        if let Some(seam) = seam() {
            seam.observe_durable(index);
        }
    }

    /// Holds the armed publication before it can reach a quorum. A no-op when the seam is
    /// disabled by the environment.
    pub async fn hold_publish_replication(
        local: ShardNodeId,
        entries: &[Entry<ShardRaftTypeConfig>],
    ) {
        if let Some(seam) = seam() {
            seam.hold_publish_replication(local, entries).await;
        }
    }

    /// Whether `payload` is exactly the armed Q2 publication.
    fn is_armed_publish(payload: &EntryPayload<ShardRaftTypeConfig>, token: &ArmedToken) -> bool {
        match payload {
            EntryPayload::Normal(ShardCommand::Publish(publish)) => {
                publish.producer_id == token.producer_id
                    && publish.producer_epoch == token.producer_epoch
                    && publish.sequence == token.sequence
                    && publish.message_id == token.message_id
            }
            _ => false,
        }
    }

    /// Atomically replaces `path` with `bytes` and makes the replacement durable.
    fn write_durable(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
        use std::io::Write;
        let directory = path
            .parent()
            .ok_or_else(|| std::io::Error::other("barrier path has no parent directory"))?;
        let temporary = directory.join(format!(
            "{}.tmp",
            path.file_name()
                .and_then(|name| name.to_str())
                .unwrap_or("barrier")
        ));
        {
            let mut file = std::fs::File::create(&temporary)?;
            file.write_all(bytes)?;
            file.sync_all()?;
        }
        std::fs::rename(&temporary, path)?;
        let parent = std::fs::File::open(directory)?;
        parent.sync_all()
    }

    #[cfg(test)]
    mod tests {
        use super::*;
        use crate::types::{ShardPublish, ShardRequest};
        use openraft::{Entry, EntryPayload, LogId};
        use rovqet_core::PartitionRef;
        use std::time::Duration;

        fn token() -> ArmedToken {
            ArmedToken {
                producer_id: 11,
                producer_epoch: 0,
                sequence: 5,
                message_id: 105,
            }
        }

        fn publish_entry(index: u64) -> Entry<ShardRaftTypeConfig> {
            Entry {
                log_id: LogId::new(openraft::LeaderId::new(1, 3), index),
                payload: EntryPayload::Normal(ShardCommand::Publish(ShardPublish {
                    partition: PartitionRef {
                        tenant_id: 1,
                        topic_id: 4,
                        partition_id: 0,
                    },
                    durable_class: 2,
                    incarnation: 42,
                    metadata_epoch: 5,
                    principal: 1,
                    producer_id: 11,
                    producer_epoch: 0,
                    sequence: 5,
                    message_id: 105,
                    payload: b"pre-majority".to_vec(),
                    payload_digest: [0u8; 32],
                })),
            }
        }

        fn blank_entry(index: u64) -> Entry<ShardRaftTypeConfig> {
            Entry {
                log_id: LogId::new(openraft::LeaderId::new(1, 3), index),
                payload: EntryPayload::Blank,
            }
        }

        fn request_entry(index: u64) -> Entry<ShardRaftTypeConfig> {
            Entry {
                log_id: LogId::new(openraft::LeaderId::new(1, 3), index),
                payload: EntryPayload::Normal(ShardCommand::Request(ShardRequest {
                    request_id: 7,
                    partition: PartitionRef {
                        tenant_id: 1,
                        topic_id: 4,
                        partition_id: 0,
                    },
                    payload: crate::types::ShardPayload::Inline(b"request".to_vec()),
                    metadata_epoch: 5,
                })),
            }
        }

        fn barrier(directory: &Path) -> Option<serde_json::Value> {
            let bytes = std::fs::read(directory.join(BARRIER_FILE)).ok()?;
            serde_json::from_slice(&bytes).ok()
        }

        fn arm(directory: &Path, token: &ArmedToken) {
            std::fs::write(
                directory.join(ARM_FILE),
                serde_json::to_vec(token).expect("arm document"),
            )
            .expect("write arm document");
        }

        #[tokio::test]
        async fn an_unarmed_seam_never_holds_a_replication() {
            let directory = tempfile::tempdir().expect("temp dir");
            let seam = Premajority::open(directory.path());
            seam.observe_durable(9);
            tokio::time::timeout(
                Duration::from_millis(100),
                seam.hold_publish_replication(2, &[publish_entry(9)]),
            )
            .await
            .expect("an unarmed seam must not hold the replication");
            assert!(
                barrier(directory.path()).is_none(),
                "no barrier without arming"
            );
        }

        #[tokio::test]
        async fn an_armed_seam_ignores_entries_of_another_token() {
            let directory = tempfile::tempdir().expect("temp dir");
            let seam = Premajority::open(directory.path());
            arm(
                directory.path(),
                &ArmedToken {
                    sequence: 6,
                    ..token()
                },
            );
            seam.observe_durable(9);
            for entry in [blank_entry(9), request_entry(9), publish_entry(9)] {
                tokio::time::timeout(
                    Duration::from_millis(100),
                    seam.hold_publish_replication(2, std::slice::from_ref(&entry)),
                )
                .await
                .expect("a different token must never be held");
            }
            assert!(
                barrier(directory.path()).is_none(),
                "no barrier for another token"
            );
        }

        #[tokio::test]
        async fn a_barrier_is_never_emitted_before_the_local_durable_append() {
            let directory = tempfile::tempdir().expect("temp dir");
            let seam = Premajority::open(directory.path());
            arm(directory.path(), &token());
            // The entry is armed but this node has not durably appended it yet.
            let result = tokio::time::timeout(
                Duration::from_millis(100),
                seam.hold_publish_replication(2, &[publish_entry(9)]),
            )
            .await;
            assert!(result.is_err(), "the hold must wait for the durable append");
            assert!(
                barrier(directory.path()).is_none(),
                "a barrier without a completed local durable append is not evidence"
            );

            // Once the durable append completes, the barrier is written with both indexes
            // and the hold still never returns: the harness, not the seam, ends it.
            seam.observe_durable(9);
            let held = tokio::time::timeout(
                Duration::from_millis(100),
                seam.hold_publish_replication(2, &[publish_entry(9)]),
            )
            .await;
            assert!(held.is_err(), "the armed hold must engage and never return");
            let written = barrier(directory.path()).expect("the barrier must be observable");
            assert_eq!(written["node"], 2);
            assert_eq!(written["log_index"], 9);
            assert_eq!(written["durable_index"], 9);
            assert_eq!(written["sequence"], 5);
            assert_eq!(written["message_id"], 105);
            assert!(written["durable_index"].as_u64().expect("durable index") >= 9);
        }

        #[tokio::test]
        async fn a_durable_append_behind_the_entry_never_releases_the_hold() {
            let directory = tempfile::tempdir().expect("temp dir");
            let seam = Premajority::open(directory.path());
            arm(directory.path(), &token());
            seam.observe_durable(8);
            let result = tokio::time::timeout(
                Duration::from_millis(100),
                seam.hold_publish_replication(2, &[publish_entry(9)]),
            )
            .await;
            assert!(
                result.is_err(),
                "an earlier durable index is not this entry"
            );
            assert!(
                barrier(directory.path()).is_none(),
                "no barrier for an earlier append"
            );
        }

        #[test]
        fn the_seam_is_disabled_without_the_environment_variable() {
            // No test in this process sets `DIRECTORY_ENV`, so the process-wide seam is
            // disabled and every public entry point is a no-op.
            assert!(seam().is_none(), "the seam must stay production disabled");
            observe_durable(1);
        }
    }
}

#[cfg(feature = "fault-injection")]
pub use active::*;

/// The barrier environment variable, named literally so a default build's test can
/// prove it is never consulted. Compiled only into the default build's test binary.
#[cfg(all(test, not(feature = "fault-injection")))]
const DIRECTORY_ENV: &str = "ROVQET_Q2_PREMAJORITY_BARRIER";

#[cfg(not(feature = "fault-injection"))]
use crate::types::{ShardNodeId, ShardRaftTypeConfig};
#[cfg(not(feature = "fault-injection"))]
use openraft::Entry;

/// Inert in a default build: the durable log index is not tracked, because no barrier
/// can be armed without the `fault-injection` feature.
#[cfg(not(feature = "fault-injection"))]
pub fn observe_durable(_index: u64) {}

/// Inert in a default build: returns immediately without reading any environment
/// variable, so no replication is ever held.
#[cfg(not(feature = "fault-injection"))]
pub async fn hold_publish_replication(
    _local: ShardNodeId,
    _entries: &[Entry<ShardRaftTypeConfig>],
) {
}

#[cfg(all(test, not(feature = "fault-injection")))]
mod tests {
    use super::*;
    use std::time::Duration;

    /// Env marker naming the child process of the re-execution below.
    const CHILD: &str = "ROVQET_Q2_PREMAJORITY_DEFAULT_BUILD_CHILD";

    /// A default build never consults the barrier environment variable: this test
    /// re-executes itself in a child process with `DIRECTORY_ENV` naming an armed
    /// directory, and the child's inert entry points must return immediately without
    /// writing a barrier document.
    #[tokio::test]
    async fn the_default_build_never_reads_the_barrier_environment() {
        if std::env::var_os(CHILD).is_some() {
            // Child: the barrier directory is armed in the environment, yet a default
            // build must ignore it completely.
            let directory = std::env::var_os(DIRECTORY_ENV)
                .map(std::path::PathBuf::from)
                .expect("the parent named the barrier directory");
            observe_durable(9);
            let entries: Vec<Entry<ShardRaftTypeConfig>> = Vec::new();
            tokio::time::timeout(
                Duration::from_millis(100),
                hold_publish_replication(2, &entries),
            )
            .await
            .expect("a default build must never hold a replication");
            assert!(
                !directory.join("barrier.json").exists(),
                "a default build must never write a barrier"
            );
            return;
        }

        // Parent: run only this test again, with the barrier environment armed.
        let directory = tempfile::tempdir().expect("temp dir");
        let test_name = "premajority::tests::the_default_build_never_reads_the_barrier_environment";
        let output =
            std::process::Command::new(std::env::current_exe().expect("the test executable"))
                .arg(test_name)
                .arg("--exact")
                .env(CHILD, "1")
                .env(DIRECTORY_ENV, directory.path())
                .output()
                .expect("run the child test process");
        let stdout = String::from_utf8_lossy(&output.stdout);
        assert!(
            output.status.success(),
            "the child process must pass:\nstdout: {stdout}\nstderr: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(
            stdout.contains("1 passed"),
            "the child must have run exactly this test: {stdout}"
        );
        assert!(
            !directory.path().join("barrier.json").exists(),
            "a default build must never write a barrier"
        );
    }
}
