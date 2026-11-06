//! A store's retention and backups, when the host file asks for them. Both are off by default:
//! removing conversations and where copies of them go are the operator's decisions.
use crate::agents::Agent;
use agent_ti_config::{BackupConfig, RetentionConfig};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

const DAY_MS: i64 = 24 * 60 * 60 * 1000;
const RETENTION_EVERY: Duration = Duration::from_secs(60 * 60);

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_millis() as i64)
}

/// Runs retention at startup, then hourly.
pub async fn retain(agent: Arc<Agent>, config: RetentionConfig) {
    let mut tick = tokio::time::interval(RETENTION_EVERY);
    loop {
        tick.tick().await;
        let cutoff = |days: Option<u64>| days.map(|d| now_ms() - d as i64 * DAY_MS);
        match agent
            .store
            .prune(
                cutoff(config.events_days),
                cutoff(config.closed_sessions_days),
            )
            .await
        {
            Ok(pruned) if pruned.idle_sessions + pruned.closed_sessions > 0 => {
                tracing::info!(agent = %agent.name, idle = pruned.idle_sessions, closed = pruned.closed_sessions, "retention pruned sessions");
            }
            Ok(_) => {}
            Err(e) => tracing::error!(agent = %agent.name, error = ?e, "retention failed"),
        }
    }
}

/// Takes a backup at startup, then every interval, keeping the newest `keep`.
pub async fn back_up(agent: Arc<Agent>, config: BackupConfig) {
    let mut tick = tokio::time::interval(Duration::from_secs(config.interval_minutes * 60));
    loop {
        tick.tick().await;
        match backup_once(&agent, &config.dir, config.keep).await {
            Ok(path) => {
                tracing::info!(agent = %agent.name, path = %path.display(), "backup written")
            }
            Err(e) => tracing::error!(agent = %agent.name, error = %e, "backup failed"),
        }
    }
}

/// `<dir>/<agent>/<millis>.sqlite`, named only once it's whole.
pub async fn backup_once(agent: &Agent, dir: &Path, keep: usize) -> Result<PathBuf, String> {
    let dir = dir.join(&agent.name);
    let unreadable = |e: std::io::Error| format!("{}: {e}", dir.display());
    tokio::fs::create_dir_all(&dir).await.map_err(unreadable)?;
    let path = dir.join(format!("{}.sqlite", now_ms()));
    let partial = path.with_extension("partial");
    let _ = tokio::fs::remove_file(&partial).await;
    agent
        .store
        .backup(&partial)
        .await
        .map_err(|e| format!("{e:?}"))?;
    tokio::fs::rename(&partial, &path)
        .await
        .map_err(unreadable)?;
    let mut entries = tokio::fs::read_dir(&dir).await.map_err(unreadable)?;
    let mut backups = Vec::new();
    while let Some(entry) = entries.next_entry().await.map_err(unreadable)? {
        let name = entry.path();
        if name.extension().is_some_and(|e| e == "sqlite") {
            backups.push(name);
        }
    }
    backups.sort();
    let surplus = backups.len().saturating_sub(keep);
    for old in &backups[..surplus] {
        let _ = tokio::fs::remove_file(old).await;
    }
    Ok(path)
}
