//! Deterministic migration barriers for the crash matrix.
//!
//! A barrier is a named pause point on the migration path. It is enabled only in a build
//! with the `fault-injection` feature and only for the one name the parent process asks
//! for, so a normal build has no test-only behaviour at all. The process announces the
//! barrier on its diagnostic stream and then waits to be killed: the parent never relies on
//! a sleep to hit the intended window.

/// The environment variable that selects the one barrier this process pauses at.
pub const BARRIER_ENV: &str = "ROVQET_MIGRATION_BARRIER";

#[cfg(feature = "fault-injection")]
pub fn barrier(name: &str) {
    if std::env::var(BARRIER_ENV).ok().as_deref() == Some(name) {
        eprintln!("MIGRATION_BARRIER {name}");
        // The parent kills the process here. The wait is bounded so a lost parent cannot
        // leave a test process alive for ever.
        std::thread::sleep(std::time::Duration::from_secs(120));
    }
}

#[cfg(not(feature = "fault-injection"))]
pub fn barrier(_name: &str) {}
