//! Deterministic post-majority fault seam for the C-04 "leader crash after a durable
//! majority but before the client response" proof.
//!
//! # Compiled only for `fault-injection`
//!
//! The whole seam — including every environment read — exists only in a build with
//! the `fault-injection` cargo feature. A default build compiles only the inert
//! entry points at the end of this module: they never read an environment variable,
//! never open a barrier directory and can never write a barrier document, so a
//! default build has no active post-majority behavior at all.
//!
//! # Production disabled by construction (fault-injection builds)
//!
//! In a fault-injection build the seam is compiled in but inert until both of the
//! following hold:
//!
//! * the `ROVQET_Q2_POSTMAJORITY_BARRIER` environment variable names a directory, and
//! * that directory holds an `arm.json` naming exactly one producer token.
//!
//! A test harness arms it for a single publication, so the failure is deterministic:
//! no sleeps, no probabilities, no fabricated acknowledgment and no retry guessing.
//! A broker started without the environment variable behaves exactly as before.
//!
//! # What the seam observes and what it holds
//!
//! [`hold_publish_response`] is called from the **client-write completion** of the
//! node that committed the entry: [`crate::ShardNode::propose`] (a publication the
//! local route sent to its own Raft core) and the peer handler's `client_write` (the
//! same completion for a publication a follower forwarded to this leader). Both
//! call sites run only after `client_write` returned `Ok`, which openraft returns
//! after the entry is **committed by a majority and applied** to the deterministic
//! state machine — and both run **before** the caller can build, encode or send the
//! `COMMITTED_QUORUM` acknowledgment. A follower never reaches either hook with a
//! success: its local write fails with `ForwardToLeader`, so the seam engages on the
//! leader exactly and names the leader.
//!
//! It then writes an externally observable `barrier.json` — node, committed log
//! index, logical offset and the armed token — and never returns, so the response of
//! that one publication can never leave this process while the entry it describes is
//! already committed by the majority. The parked future is the request task of one
//! client connection or of one forwarded RPC connection: it takes no state-machine
//! lock, no storage lock, no raft-core lock and no replication channel, the peer
//! server serves every other connection on its own task, and openraft keeps
//! committing and sending heartbeats. The harness reads the barrier, SIGKILLs the
//! named node and later retries the identical token on the restarted group.

#[cfg(feature = "fault-injection")]
mod active {
    use crate::types::{ShardCommand, ShardCommandResponse, ShardNodeId};
    use std::path::{Path, PathBuf};
    use std::sync::OnceLock;
    use std::sync::atomic::{AtomicBool, Ordering};

    /// Environment variable naming the barrier directory. Unset means the seam is off.
    pub const DIRECTORY_ENV: &str = "ROVQET_Q2_POSTMAJORITY_BARRIER";

    /// Arm document inside the barrier directory: the one token the seam may hold.
    pub const ARM_FILE: &str = "arm.json";

    /// Externally observable barrier document written at the post-majority point.
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

    /// One armed, production-disabled post-majority seam.
    pub struct Postmajority {
        directory: PathBuf,
        /// The barrier is written at most once per process.
        fired: AtomicBool,
    }

    static SEAM: OnceLock<Option<Postmajority>> = OnceLock::new();

    /// The process-wide seam, initialized from [`DIRECTORY_ENV`] on first use.
    fn seam() -> Option<&'static Postmajority> {
        SEAM.get_or_init(|| {
            let directory = std::env::var_os(DIRECTORY_ENV).map(PathBuf::from)?;
            Some(Postmajority::open(directory))
        })
        .as_ref()
    }

    impl Postmajority {
        /// Shares the existing feature-gated seam/directory with publications.
        /// The full ACK command is required: no cross-topic or cross-lease arm.
        pub async fn hold_group_ack(
            &self,
            local: ShardNodeId,
            index: u64,
            stage: &str,
            command: &ShardCommand,
        ) {
            #[derive(serde::Deserialize)]
            struct Arm {
                stage: String,
                command: ShardCommand,
            }
            let Ok(bytes) = std::fs::read(self.directory.join(ARM_FILE)) else {
                return;
            };
            let Ok(arm) = serde_json::from_slice::<Arm>(&bytes) else {
                return;
            };
            if arm.stage != stage
                || arm.command != *command
                || !matches!(command, ShardCommand::GroupAck { .. })
            {
                return;
            }
            #[derive(serde::Serialize)]
            struct Barrier<'a> {
                event: &'a str,
                stage: &'a str,
                node: u64,
                committed_index: u64,
                command: &'a ShardCommand,
            }
            if !self.fired.swap(true, Ordering::SeqCst) {
                let document = Barrier {
                    event: "consumer_ack_barrier",
                    stage,
                    node: local,
                    committed_index: index,
                    command,
                };
                if let Ok(bytes) = serde_json::to_vec_pretty(&document) {
                    let _ = write_durable(&self.directory.join(BARRIER_FILE), &bytes);
                }
            }
            std::future::pending::<()>().await;
        }

        /// Opens a seam over `directory`. Without a later `arm.json` document the seam
        /// holds nothing, whatever else is configured.
        pub fn open(directory: impl Into<PathBuf>) -> Self {
            Self {
                directory: directory.into(),
                fired: AtomicBool::new(false),
            }
        }

        /// The armed token, read from the arm document, or `None` when not armed.
        pub fn armed(&self) -> Option<ArmedToken> {
            let bytes = std::fs::read(self.directory.join(ARM_FILE)).ok()?;
            serde_json::from_slice(&bytes).ok()
        }

        /// Holds the committed response of the armed publication until the process dies.
        ///
        /// Returns immediately — without any barrier — when the seam is unarmed, when
        /// `response` is not the `Published` outcome of a committed publication, or when
        /// that publication is not the armed token. Only an armed, already committed and
        /// applied publication is held, and only between the commit and the response.
        pub async fn hold_publish_response(
            &self,
            local: ShardNodeId,
            response: &ShardCommandResponse,
            command: &ShardCommand,
        ) {
            let ShardCommandResponse::Published {
                index,
                logical_offset,
                ..
            } = response
            else {
                return;
            };
            let Some(token) = self.armed() else {
                return;
            };
            if !is_armed_publish(command, &token) {
                return;
            }
            self.emit_barrier(local, *index, *logical_offset, &token);
            // Never released: the harness SIGKILLs the node the barrier names. This parks
            // one request task and nothing else.
            std::future::pending::<()>().await;
        }

        /// Writes the externally observable barrier document, at most once.
        fn emit_barrier(
            &self,
            node: ShardNodeId,
            index: u64,
            logical_offset: u64,
            token: &ArmedToken,
        ) {
            if self.fired.swap(true, Ordering::SeqCst) {
                return;
            }
            let document = serde_json::json!({
                "event": "postmajority_barrier",
                "node": node,
                "committed_index": index,
                "logical_offset": logical_offset,
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

    /// Holds the committed response of the armed publication before the caller can
    /// answer the client. A no-op when the seam is disabled by the environment.
    pub async fn hold_publish_response(
        local: ShardNodeId,
        response: &ShardCommandResponse,
        command: &ShardCommand,
    ) {
        if let Some(seam) = seam() {
            seam.hold_publish_response(local, response, command).await;
        }
    }

    /// Whether `command` is exactly the armed Q2 publication.
    pub async fn hold_group_ack(
        local: ShardNodeId,
        index: u64,
        stage: &str,
        command: &ShardCommand,
    ) {
        if let Some(seam) = seam() {
            seam.hold_group_ack(local, index, stage, command).await;
        }
    }

    /// Whether `command` is exactly the armed Q2 publication.
    fn is_armed_publish(command: &ShardCommand, token: &ArmedToken) -> bool {
        match command {
            ShardCommand::Publish(publish) => {
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
        use crate::types::{ShardPayload, ShardPublish, ShardRequest};
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

        #[tokio::test]
        async fn the_ack_arm_requires_the_exact_stage_and_full_delivery_identity() {
            let directory = tempfile::tempdir().unwrap();
            let seam = Postmajority::open(directory.path());
            let command = ShardCommand::GroupAck {
                partition: PartitionRef {
                    tenant_id: 1,
                    topic_id: 4,
                    partition_id: 0,
                },
                incarnation: 42,
                metadata_epoch: 5,
                generation: 1,
                group_id: 7,
                offset: 0,
                message_id: 100,
                principal: 1,
                lease_id: u128::MAX,
                attempt: 1,
            };
            let arm = format!(
                "{{\"stage\":\"after_majority\",\"command\":{}}}",
                serde_json::to_string(&command).unwrap()
            );
            std::fs::write(directory.path().join(ARM_FILE), arm).unwrap();
            tokio::time::timeout(
                Duration::from_millis(100),
                seam.hold_group_ack(1, 9, "before_proposal", &command),
            )
            .await
            .unwrap();
            assert!(!directory.path().join(BARRIER_FILE).exists());
            assert!(
                tokio::time::timeout(
                    Duration::from_millis(100),
                    seam.hold_group_ack(1, 9, "after_majority", &command)
                )
                .await
                .is_err()
            );
            let raw = std::fs::read_to_string(directory.path().join(BARRIER_FILE)).unwrap();
            assert!(
                raw.contains(&u128::MAX.to_string()),
                "barrier must not round the full lease identity"
            );
            assert!(raw.contains("\"committed_index\": 9"));
        }

        fn publish_command() -> ShardCommand {
            ShardCommand::Publish(ShardPublish {
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
                payload: b"post-majority".to_vec(),
                payload_digest: [0u8; 32],
            })
        }

        fn request_command() -> ShardCommand {
            ShardCommand::Request(ShardRequest {
                request_id: 7,
                partition: PartitionRef {
                    tenant_id: 1,
                    topic_id: 4,
                    partition_id: 0,
                },
                payload: ShardPayload::Inline(b"request".to_vec()),
                metadata_epoch: 5,
            })
        }

        /// The committed, applied outcome of a publication at `index`.
        fn published(index: u64, logical_offset: u64) -> ShardCommandResponse {
            ShardCommandResponse::Published {
                index,
                partition: PartitionRef {
                    tenant_id: 1,
                    topic_id: 4,
                    partition_id: 0,
                },
                logical_offset,
                message_id: 105,
                duplicate: false,
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
        async fn an_unarmed_seam_never_holds_a_response() {
            let directory = tempfile::tempdir().expect("temp dir");
            let seam = Postmajority::open(directory.path());
            tokio::time::timeout(
                Duration::from_millis(100),
                seam.hold_publish_response(2, &published(9, 0), &publish_command()),
            )
            .await
            .expect("an unarmed seam must not hold the response");
            assert!(
                barrier(directory.path()).is_none(),
                "no barrier without arming"
            );
        }

        #[tokio::test]
        async fn an_armed_seam_ignores_another_token_and_a_committed_rejection() {
            let directory = tempfile::tempdir().expect("temp dir");
            let seam = Postmajority::open(directory.path());
            arm(
                directory.path(),
                &ArmedToken {
                    sequence: 6,
                    ..token()
                },
            );
            for (response, command) in [
                (published(9, 0), request_command()),
                (published(9, 0), publish_command()),
                (
                    ShardCommandResponse::PublishRejected {
                        index: 9,
                        code: crate::types::PublishErrorCode::StaleEpoch,
                        reason: "refused".into(),
                    },
                    publish_command(),
                ),
            ] {
                tokio::time::timeout(
                    Duration::from_millis(100),
                    seam.hold_publish_response(2, &response, &command),
                )
                .await
                .expect("a different token or outcome must never be held");
            }
            assert!(
                barrier(directory.path()).is_none(),
                "no barrier for another token"
            );
        }

        #[tokio::test]
        async fn an_armed_seam_writes_the_barrier_and_never_returns() {
            let directory = tempfile::tempdir().expect("temp dir");
            let seam = Postmajority::open(directory.path());
            arm(directory.path(), &token());
            let held = tokio::time::timeout(
                Duration::from_millis(100),
                seam.hold_publish_response(2, &published(9, 4), &publish_command()),
            )
            .await;
            assert!(
                held.is_err(),
                "the armed hold must engage after the commit and never return"
            );
            let written = barrier(directory.path()).expect("the barrier must be observable");
            assert_eq!(written["event"], "postmajority_barrier");
            assert_eq!(written["node"], 2);
            assert_eq!(written["committed_index"], 9);
            assert_eq!(written["logical_offset"], 4);
            assert_eq!(written["producer_id"], 11);
            assert_eq!(written["producer_epoch"], 0);
            assert_eq!(written["sequence"], 5);
            assert_eq!(written["message_id"], 105);
        }

        #[tokio::test]
        async fn the_barrier_is_written_at_most_once() {
            let directory = tempfile::tempdir().expect("temp dir");
            let seam = Postmajority::open(directory.path());
            arm(directory.path(), &token());
            let _ = tokio::time::timeout(
                Duration::from_millis(50),
                seam.hold_publish_response(2, &published(9, 4), &publish_command()),
            )
            .await;
            let first = barrier(directory.path()).expect("the barrier must be observable");
            // A second armed command of the same process may not move the barrier.
            let _ = tokio::time::timeout(
                Duration::from_millis(50),
                seam.hold_publish_response(3, &published(11, 5), &publish_command()),
            )
            .await;
            let second = barrier(directory.path()).expect("the barrier must stay observable");
            assert_eq!(first, second, "the barrier is written at most once");
        }

        #[test]
        fn the_seam_is_disabled_without_the_environment_variable() {
            // No test in this process sets `DIRECTORY_ENV`, so the process-wide seam is
            // disabled and every public entry point is a no-op.
            assert!(seam().is_none(), "the seam must stay production disabled");
        }
    }
}

#[cfg(feature = "fault-injection")]
pub use active::*;

/// The barrier environment variable, named literally so a default build's test can
/// prove it is never consulted. Compiled only into the default build's test binary.
#[cfg(all(test, not(feature = "fault-injection")))]
const DIRECTORY_ENV: &str = "ROVQET_Q2_POSTMAJORITY_BARRIER";

#[cfg(not(feature = "fault-injection"))]
use crate::types::{ShardCommand, ShardCommandResponse, ShardNodeId};

/// Inert in a default build: no barrier can be armed without the `fault-injection`
/// feature, so the response of a committed publication is never held.
#[cfg(not(feature = "fault-injection"))]
pub async fn hold_publish_response(
    _local: ShardNodeId,
    _response: &ShardCommandResponse,
    _command: &ShardCommand,
) {
}

#[cfg(not(feature = "fault-injection"))]
pub async fn hold_group_ack(
    _local: ShardNodeId,
    _index: u64,
    _stage: &str,
    _command: &ShardCommand,
) {
}

#[cfg(all(test, not(feature = "fault-injection")))]
mod tests {
    use super::*;
    use crate::types::{ShardPayload, ShardPublish, ShardRequest};
    use rovqet_core::PartitionRef;
    use std::time::Duration;

    /// Env marker naming the child process of the re-execution below.
    const CHILD: &str = "ROVQET_Q2_POSTMAJORITY_DEFAULT_BUILD_CHILD";

    fn publish_command() -> ShardCommand {
        ShardCommand::Publish(ShardPublish {
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
            payload: b"post-majority".to_vec(),
            payload_digest: [0u8; 32],
        })
    }

    fn request_command() -> ShardCommand {
        ShardCommand::Request(ShardRequest {
            request_id: 7,
            partition: PartitionRef {
                tenant_id: 1,
                topic_id: 4,
                partition_id: 0,
            },
            payload: ShardPayload::Inline(b"request".to_vec()),
            metadata_epoch: 5,
        })
    }

    /// The committed, applied outcome of a publication at `index`.
    fn published(index: u64, logical_offset: u64) -> ShardCommandResponse {
        ShardCommandResponse::Published {
            index,
            partition: PartitionRef {
                tenant_id: 1,
                topic_id: 4,
                partition_id: 0,
            },
            logical_offset,
            message_id: 105,
            duplicate: false,
        }
    }

    /// A default build never consults the barrier environment variable: this test
    /// re-executes itself in a child process with `DIRECTORY_ENV` naming an armed
    /// directory, and the child's inert entry point must return immediately without
    /// writing a barrier document.
    #[tokio::test]
    async fn the_default_build_never_reads_the_barrier_environment() {
        if std::env::var_os(CHILD).is_some() {
            // Child: the barrier directory is armed in the environment, yet a default
            // build must ignore it completely.
            let directory = std::env::var_os(DIRECTORY_ENV)
                .map(std::path::PathBuf::from)
                .expect("the parent named the barrier directory");
            tokio::time::timeout(
                Duration::from_millis(100),
                hold_publish_response(2, &published(9, 4), &publish_command()),
            )
            .await
            .expect("a default build must never hold a response");
            tokio::time::timeout(
                Duration::from_millis(100),
                hold_publish_response(2, &published(9, 4), &request_command()),
            )
            .await
            .expect("a default build must never hold a response");
            assert!(
                !directory.join("barrier.json").exists(),
                "a default build must never write a barrier"
            );
            return;
        }

        // Parent: run only this test again, with the barrier environment armed.
        let directory = tempfile::tempdir().expect("temp dir");
        let test_name =
            "postmajority::tests::the_default_build_never_reads_the_barrier_environment";
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
