//! Deterministic crash windows for isolated test processes only.
use crate::{Error, Result};
use rovqet_wal::FaultPoint;
use std::{
    path::PathBuf,
    thread,
    time::{Duration, Instant},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TestStage {
    BeforeAppend,
    AfterAppendBeforeSync,
    AfterSyncBeforeReply,
    /// Crash after a cursor-checkpoint record was appended but before it was synced.
    CheckpointAfterAppendBeforeSync,
}
impl TestStage {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::BeforeAppend => "before-append",
            Self::AfterAppendBeforeSync => "after-append-before-sync",
            Self::AfterSyncBeforeReply => "after-sync-before-reply",
            Self::CheckpointAfterAppendBeforeSync => "checkpoint-after-append-before-sync",
        }
    }
}
/// One-shot controls scoped to a caller-selected disposable marker directory.
/// Normal builds cannot instantiate or access these controls.
#[derive(Debug)]
pub struct TestControl {
    pub directory: PathBuf,
    pub stage: Option<TestStage>,
    pub sequence: u64,
    pub wal_fault: Option<FaultPoint>,
}
impl TestControl {
    pub(crate) fn pause(&mut self, stage: TestStage, sequence: u64) -> Result<()> {
        if self.sequence != sequence || self.stage != Some(stage) {
            return Ok(());
        }
        self.stage = None;
        std::fs::write(
            self.directory.join(format!("reached-{}", stage.as_str())),
            b"reached\n",
        )
        .map_err(|_| Error::OutcomeUnknown)?;
        let release = self.directory.join(format!("release-{}", stage.as_str()));
        let end = Instant::now() + Duration::from_secs(30);
        while !release.exists() {
            if Instant::now() >= end {
                return Err(Error::OutcomeUnknown);
            }
            thread::sleep(Duration::from_millis(2));
        }
        Ok(())
    }
}
