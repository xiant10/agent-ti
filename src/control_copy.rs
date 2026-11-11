//! The last good copy of what t10-agent-control answered, one file per tenant and feed, in the
//! format agent-pi keeps. Control is never on a turn's path for long: a read is fresh for a few
//! seconds, and when control can't be reached the last good copy stands in with no time limit,
//! because an older copy of the tenant's choice is better than none. The copy is kept on disk, so
//! a replica that restarts while control is down starts from it. Past `STALE_AFTER` without an
//! answer, each failed read logs `control.feed_stale` at error level, which is what alerts. See
//! "Reading control" in t10-agent-control's docs/design.md.
use agent_ti_config::control_copy::ControlCopyFiles;
use serde_json::Value;
use std::{
    path::PathBuf,
    time::{Duration, Instant},
};

/// How long a copy may go without an answer from control before it's reported stale. The same
/// for every feed.
pub const STALE_AFTER: Duration = Duration::from_secs(15 * 60);

/// One saved answer: its ETag, when control sent it, and the body.
pub struct SavedAnswer {
    pub etag: Option<String>,
    /// When control sent it, on this process's clock.
    pub at: Instant,
    pub body: Value,
}

pub struct ControlCopies {
    files: Option<ControlCopyFiles>,
    feed: &'static str,
}

impl ControlCopies {
    /// `dir` `None` keeps nothing on disk.
    pub fn new(dir: Option<PathBuf>, feed: &'static str) -> Self {
        let files = dir.and_then(|dir| match ControlCopyFiles::open(dir.join(feed)) {
            Ok(files) => Some(files),
            Err(e) => {
                tracing::warn!(feed, error = %e, "control.copy_dir_failed");
                None
            }
        });
        Self { files, feed }
    }

    /// The saved answer for a key, read when this process first needs it.
    pub fn load(&self, key: &str) -> Option<SavedAnswer> {
        match self.files.as_ref()?.load(key) {
            Ok(saved) => saved.map(|saved| SavedAnswer {
                etag: saved.etag,
                at: Instant::now()
                    .checked_sub(saved.age)
                    .unwrap_or_else(Instant::now),
                body: saved.body,
            }),
            Err(e) => {
                tracing::warn!(feed = self.feed, error = %e, "control.copy_unreadable");
                None
            }
        }
    }

    /// Keep an answer control just sent.
    pub fn save(&self, key: &str, etag: Option<&str>, body: &Value) {
        if let Some(files) = &self.files
            && let Err(e) = files.save(key, etag, body)
        {
            tracing::warn!(feed = self.feed, error = %e, "control.copy_not_saved");
        }
    }

    /// Log a failed read that falls back to a copy control answered at `ok_at`, or to the shipped
    /// default when there's none.
    pub fn failed(&self, tenant: &str, error: &str, ok_at: Option<Instant>, using: &str) {
        match ok_at.map(|at| at.elapsed()) {
            Some(age) if age < STALE_AFTER => tracing::warn!(
                feed = self.feed, tenant = %tenant, error, using, age_seconds = age.as_secs(),
                "control.feed_unavailable"
            ),
            Some(age) => tracing::error!(
                feed = self.feed, tenant = %tenant, error, using, age_seconds = age.as_secs(),
                "control.feed_stale"
            ),
            None => tracing::error!(
                feed = self.feed, tenant = %tenant, error, using,
                "control.feed_stale"
            ),
        }
    }
}
