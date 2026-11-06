//! Keeping a store's size in check, and copying it. Pruning takes small transactions, so a store
//! with a lot to remove doesn't hold up the sessions that are running.
use super::*;

const PRUNE_BATCH: i64 = 100;
/// Tables a session's rows live in, children before what they refer to.
const SESSION_TABLES: [&str; 12] = [
    "task_messages",
    "tasks",
    "steps",
    "cancel_keys",
    "create_keys",
    "messages",
    "events",
    "commands",
    "plugin_state",
    "session_tools",
    "compactions",
    "sessions",
];

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Pruned {
    /// Idle sessions whose events and finished steps were removed.
    pub idle_sessions: u64,
    /// Closed sessions removed whole, their children with them.
    pub closed_sessions: u64,
}

impl AgentStore {
    /// Removes what a retention policy says has aged out, each cutoff a time in milliseconds:
    /// events and finished steps of sessions idle since `events_before`, and everything of sessions
    /// closed and idle since `closed_before`. A session with work in progress keeps all of it.
    pub async fn prune(
        &self,
        events_before: Option<i64>,
        closed_before: Option<i64>,
    ) -> StoreResult<Pruned> {
        let mut pruned = Pruned::default();
        if let Some(cutoff) = closed_before {
            loop {
                let removed = self.prune_closed(cutoff).await?;
                pruned.closed_sessions += removed;
                if removed < PRUNE_BATCH as u64 {
                    break;
                }
            }
        }
        if let Some(cutoff) = events_before {
            loop {
                let removed = self.prune_idle(cutoff).await?;
                pruned.idle_sessions += removed;
                if removed < PRUNE_BATCH as u64 {
                    break;
                }
            }
        }
        Ok(pruned)
    }
    async fn prune_closed(&self, cutoff: i64) -> StoreResult<u64> {
        let _guard = self.write_guard(4096)?;
        let mut conn = self.begin().await?;
        let mut tx = conn.begin_with("BEGIN IMMEDIATE").await.map_err(db)?;
        let roots: Vec<(String, String, String)> = sqlx::query_as(
            "SELECT tenant_id,agent_id,session_id FROM sessions s WHERE status='closed' AND parent IS NULL AND last_active_at<? \
             AND NOT EXISTS (SELECT 1 FROM commands c WHERE c.tenant_id=s.tenant_id AND c.agent_id=s.agent_id AND c.session_id=s.session_id AND c.status IN ('queued','running')) \
             LIMIT ?",
        )
        .bind(cutoff)
        .bind(PRUNE_BATCH)
        .fetch_all(&mut *tx)
        .await
        .map_err(db)?;
        for (tenant, agent, root) in &roots {
            // One level deep, as delegation is. A child's rows go first, for the foreign keys.
            let mut ids: Vec<String> = sqlx::query_scalar(
                "SELECT session_id FROM sessions WHERE tenant_id=? AND agent_id=? AND parent=?",
            )
            .bind(tenant)
            .bind(agent)
            .bind(root)
            .fetch_all(&mut *tx)
            .await
            .map_err(db)?;
            ids.push(root.clone());
            for id in &ids {
                for table in SESSION_TABLES {
                    let statement = format!(
                        "DELETE FROM {table} WHERE tenant_id=? AND agent_id=? AND session_id=?"
                    );
                    sqlx::query(&statement)
                        .bind(tenant)
                        .bind(agent)
                        .bind(id)
                        .execute(&mut *tx)
                        .await
                        .map_err(db)?;
                }
            }
        }
        tx.commit().await.map_err(db)?;
        Ok(roots.len() as u64)
    }
    async fn prune_idle(&self, cutoff: i64) -> StoreResult<u64> {
        let _guard = self.write_guard(4096)?;
        let mut conn = self.begin().await?;
        let mut tx = conn.begin_with("BEGIN IMMEDIATE").await.map_err(db)?;
        // `next_event` stays, so a stream that resumes later can tell its events are gone.
        let sessions: Vec<(String, String, String)> = sqlx::query_as(
            "SELECT tenant_id,agent_id,session_id FROM sessions s WHERE last_active_at<? \
             AND EXISTS (SELECT 1 FROM events e WHERE e.tenant_id=s.tenant_id AND e.agent_id=s.agent_id AND e.session_id=s.session_id) \
             AND NOT EXISTS (SELECT 1 FROM commands c WHERE c.tenant_id=s.tenant_id AND c.agent_id=s.agent_id AND c.session_id=s.session_id AND c.status IN ('queued','running')) \
             LIMIT ?",
        )
        .bind(cutoff)
        .bind(PRUNE_BATCH)
        .fetch_all(&mut *tx)
        .await
        .map_err(db)?;
        for (tenant, agent, id) in &sessions {
            for statement in [
                "DELETE FROM events WHERE tenant_id=? AND agent_id=? AND session_id=?",
                "DELETE FROM steps WHERE tenant_id=? AND agent_id=? AND session_id=?",
            ] {
                sqlx::query(statement)
                    .bind(tenant)
                    .bind(agent)
                    .bind(id)
                    .execute(&mut *tx)
                    .await
                    .map_err(db)?;
            }
        }
        tx.commit().await.map_err(db)?;
        Ok(sessions.len() as u64)
    }
    /// Whether events after `after` have been removed, so a stream can't resume from there.
    pub async fn events_gone(&self, s: &UserScope, id: &str, after: i64) -> StoreResult<bool> {
        let mut conn = self.read().await?;
        owned(&mut conn, s, id).await?;
        let (first, last): (Option<i64>, i64) = sqlx::query_as(
            "SELECT (SELECT MIN(sequence) FROM events e WHERE e.tenant_id=s.tenant_id AND e.agent_id=s.agent_id AND e.session_id=s.session_id), next_event \
             FROM sessions s WHERE tenant_id=? AND agent_id=? AND session_id=?",
        )
        .bind(&s.tenant_id)
        .bind(&s.agent_id)
        .bind(id)
        .fetch_one(&mut *conn)
        .await
        .map_err(db)?;
        Ok(after < last && first.is_none_or(|first| first > after + 1))
    }
    /// A consistent copy of the store, taken while it runs: `VACUUM INTO`, on a reader, so writers
    /// carry on. Copying the file alone while WAL is active isn't a backup. `path` mustn't exist.
    pub async fn backup(&self, path: &std::path::Path) -> StoreResult<()> {
        let target = path.to_str().ok_or(StoreError::Invalid)?;
        sqlx::query("VACUUM INTO ?")
            .bind(target)
            .execute(&self.readers)
            .await
            .map_err(db)?;
        Ok(())
    }
}
