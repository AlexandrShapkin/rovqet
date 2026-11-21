//! Deterministic metadata barriers for the crash and leader-failure matrix.
//!
//! A barrier is a named pause point on the metadata path. It is enabled only in a build
//! with the `fault-injection` feature and only for the one name the parent process asks
//! for, so a normal build has no test-only behaviour at all. The process announces the
//! barrier on its diagnostic stream and then waits: the parent either kills the process
//! here, or it releases the wait by creating the release marker.
//!
//! The release marker matters for a barrier that every voter reaches: the parent kills
//! the leader and then releases the followers, so the group can elect a replacement.

/// The environment variable that selects the one barrier this process pauses at.
pub const BARRIER_ENV: &str = "ROVQET_MIGRATION_BARRIER";

/// The environment variable that names the file that releases a paused barrier.
///
/// The barrier waits for this file to appear. Without it the barrier sleeps for a bounded
/// time and lets the parent kill the process.
pub const BARRIER_RELEASE_ENV: &str = "ROVQET_MIGRATION_BARRIER_RELEASE";

/// The barrier that fires after a committed migration transition was applied and persisted
/// but before the caller receives the confirmation.
pub const AFTER_MIGRATION_COMMIT: &str = "after_migration_commit";

/// The barrier that fires when a migration transition reached the leader but was not
/// committed yet.
pub const AFTER_MIGRATION_PROPOSAL: &str = "after_migration_proposal";

#[cfg(feature = "fault-injection")]
pub fn barrier(name: &str) {
    if std::env::var(BARRIER_ENV).ok().as_deref() != Some(name) {
        return;
    }
    eprintln!("MIGRATION_BARRIER {name}");
    if let Some(release) = std::env::var_os(BARRIER_RELEASE_ENV) {
        // The parent releases the wait by creating the marker file.
        let end = std::time::Instant::now() + std::time::Duration::from_secs(60);
        while !std::path::Path::new(&release).exists() {
            if std::time::Instant::now() >= end {
                return;
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        return;
    }
    // The parent kills the process here. The wait is bounded so a lost parent cannot
    // leave a test process alive for ever.
    std::thread::sleep(std::time::Duration::from_secs(120));
}

#[cfg(not(feature = "fault-injection"))]
pub fn barrier(_name: &str) {}
