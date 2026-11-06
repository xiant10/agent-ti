//! One agent's SQLite journal: sessions, commands, steps, messages and events. One process owns
//! the file, enforced by an OS lock, and the file records its agent.
use agent_ti_core::Message;
use fs2::FileExt;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use sqlx::{
    SqlitePool,
    sqlite::{SqliteConnectOptions, SqliteJournalMode, SqliteSynchronous},
};
use std::{
    collections::BTreeMap,
    fs::{File, OpenOptions},
    path::Path,
    time::Duration,
};

mod sessions;
pub use sessions::{
    Ended, Pruned, SessionUsage, Settled, Summary, TaskMessage, TaskRow, UnattendedBudget,
};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct UserScope {
    pub tenant_id: String,
    pub agent_id: String,
    pub user_id: String,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Session {
    pub session_id: String,
    pub config_version: String,
    pub profile: String,
    pub name: String,
    pub status: String,
    pub origin: Value,
    pub version: i64,
    pub metadata: Value,
    pub created_at: i64,
    pub last_active_at: i64,
    pub busy: bool,
    /// Commands parked after `restartLimit` restarts without progress.
    pub parked: i64,
    /// For a child session: the session whose tool call runs it.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parent: Option<String>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DurableEvent {
    pub sequence: i64,
    pub payload: Value,
}
#[derive(Clone, Debug)]
pub struct SessionState {
    pub session: Session,
    pub high_water: i64,
    pub busy: bool,
}
#[derive(Clone, Debug)]
pub struct Admission {
    pub command_id: String,
    pub turn_id: String,
    pub duplicate: bool,
    /// A plugin's reply, if this input was answered without the model.
    pub reply: Option<Reply>,
    /// The reply was written at once (the session was idle); otherwise it waits its turn.
    pub placed: bool,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Reply {
    pub text: String,
    pub source: String,
}
/// What an earlier input with the same request key became.
#[derive(Clone, Debug)]
pub struct Prior {
    pub fingerprint: String,
    pub admission: Admission,
}
#[derive(Clone, Debug)]
pub struct Work {
    pub scope: UserScope,
    pub session_id: String,
    pub command_id: String,
    pub turn_id: String,
    pub input: String,
    pub profile: String,
    pub traceparent: Option<String>,
    pub attempt: i64,
    pub reply: Option<Reply>,
    /// Cancelled before a restart; finish it without running.
    pub cancel_requested: bool,
    /// A child session's spec, for a command of one.
    pub child: Option<Value>,
}
/// A command the scheduler may claim: the oldest of its session, which has nothing running.
#[derive(Clone, Debug)]
pub struct Candidate {
    pub tenant_id: String,
    pub user_id: String,
    pub command_id: String,
    pub ordinal: i64,
    /// A child session's command, which a parent's run waits for, so it is outside run limits.
    pub child: bool,
    /// A background task's child: within process capacity and `maxBackgroundRunsPerUser` instead.
    pub task: bool,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StoreError {
    NotFound,
    Conflict,
    QuotaExceeded,
    Unavailable,
    Invalid,
    /// The file belongs to another agent.
    WrongAgent(String),
}
#[derive(Clone, Debug)]
pub struct TranscriptEntry {
    pub sequence: i64,
    pub message: Message,
    pub turn_id: String,
    pub request_id: String,
    pub status: String,
}
#[derive(Clone, Debug)]
pub struct TranscriptPage {
    pub entries: Vec<TranscriptEntry>,
    pub has_more: bool,
}
#[derive(Clone, Debug, Default, PartialEq)]
pub struct SessionTools {
    pub offered: Vec<String>,
    pub dynamic: Vec<String>,
    pub skills: BTreeMap<String, Vec<String>>,
}
#[derive(Clone, Debug)]
pub struct ActivationPolicy {
    pub pinned: Vec<String>,
    pub max_active: usize,
}
#[derive(Clone, Debug, Default, PartialEq)]
pub struct ActivationResult {
    pub added: Vec<String>,
    pub evicted: Vec<String>,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StoredGrant {
    pub grant_id: String,
    pub parent: Option<String>,
}
pub type StoreResult<T> = Result<T, StoreError>;
#[derive(Clone)]
pub struct CreateSession {
    pub scope: UserScope,
    pub session_id: String,
    pub request_key: String,
    pub profile: String,
    pub config_version: String,
    pub origin: Value,
    pub metadata: Value,
    /// Set for a child session, which isn't listed and doesn't count toward session limits.
    pub child: Option<Child>,
}
#[derive(Clone, Debug)]
pub struct Child {
    pub parent: String,
    pub spec: Value,
}
#[derive(Clone)]
pub struct Submit {
    pub scope: UserScope,
    pub session_id: String,
    pub request_key: String,
    /// Of the input as the caller sent it, before any rewrite.
    pub fingerprint: String,
    /// The input to store: the caller's, or a plugin's rewrite.
    pub input: String,
    pub turn_id: String,
    pub traceparent: Option<String>,
    pub reply: Option<Reply>,
    pub when_busy: Option<String>,
}
#[derive(Clone)]
pub struct Completion {
    pub status: String,
    pub messages: Vec<Message>,
    pub error: Option<String>,
    pub input_tokens: u64,
    pub output_tokens: u64,
    /// The final model message's ID, which its live deltas carried.
    pub message_id: Option<String>,
    /// Text already streamed when the run was cancelled; it ends the message but isn't stored.
    pub partial: Option<String>,
}
#[derive(Clone)]
pub struct Operation {
    pub id: String,
    pub kind: String,
    pub input: Value,
    pub unsafe_effect: bool,
    /// Committed with the prepared record, the first time only.
    pub event: Option<Value>,
}
#[derive(Clone, Debug, PartialEq)]
pub enum Prepared {
    /// Make the call.
    Run,
    /// The call's recorded result; don't call again.
    Recorded(Value),
    /// An unsafe call was prepared before a crash and has no result; don't repeat it.
    Interrupted,
}
/// The store's admission limits.
#[derive(Clone, Debug)]
pub struct StoreLimits {
    pub max_sessions_per_tenant: u64,
    pub max_sessions_per_user: u64,
    pub max_queued_per_tenant: u64,
    pub max_queued_per_session: u64,
    pub max_input_bytes: usize,
    /// Restarts in a row without a new step before a command is parked.
    pub restart_limit: u32,
}

pub fn digest(value: &impl Serialize) -> String {
    let bytes = serde_json::to_vec(value).expect("JSON value");
    format!(
        "sha256:{}",
        Sha256::digest(bytes)
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect::<String>()
    )
}

/// Called after each commit with the events it wrote, in order, by session.
pub type CommitListener = Box<dyn Fn(Vec<(String, DurableEvent)>) + Send + Sync>;
/// Connections for reads, beside the one writer. Backups and streams resuming use them too.
const READ_CONNECTIONS: u32 = 4;
pub struct AgentStore {
    pool: SqlitePool,
    readers: SqlitePool,
    listener: std::sync::OnceLock<CommitListener>,
    agent: String,
    limits: StoreLimits,
    writes: tokio::sync::Semaphore,
    write_bytes: tokio::sync::Semaphore,
    // OS lock lasts for the store lifetime, including outstanding SQL operations.
    _lock: File,
}
impl AgentStore {
    /// Opens (creating if missing) `path` as the store of `agent`.
    pub async fn open(path: &Path, agent: &str, limits: StoreLimits) -> StoreResult<Self> {
        let path = if path.exists() {
            path.canonicalize().map_err(|_| StoreError::Unavailable)?
        } else {
            let parent = path
                .parent()
                .filter(|p| !p.as_os_str().is_empty())
                .unwrap_or(Path::new("."));
            parent
                .canonicalize()
                .map_err(|_| StoreError::Unavailable)?
                .join(path.file_name().ok_or(StoreError::Unavailable)?)
        };
        let mut lock_path = path.as_os_str().to_owned();
        lock_path.push(".owner.lock");
        let lock = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(lock_path)
            .map_err(|_| StoreError::Unavailable)?;
        lock.try_lock_exclusive()
            .map_err(|_| StoreError::Conflict)?;
        let options = SqliteConnectOptions::new()
            .filename(&path)
            .create_if_missing(true)
            .foreign_keys(true)
            .journal_mode(SqliteJournalMode::Wal)
            .synchronous(SqliteSynchronous::Full)
            .busy_timeout(Duration::from_secs(5));
        let pool = sqlx::sqlite::SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(options)
            .await
            .map_err(|_| StoreError::Unavailable)?;
        sqlx::migrate!("./migrations/sqlite")
            .run(&pool)
            .await
            .map_err(|_| StoreError::Unavailable)?;
        sqlx::query("INSERT INTO store_meta VALUES('agent',?) ON CONFLICT DO NOTHING")
            .bind(agent)
            .execute(&pool)
            .await
            .map_err(|_| StoreError::Unavailable)?;
        let owner: String = sqlx::query_scalar("SELECT value FROM store_meta WHERE key='agent'")
            .fetch_one(&pool)
            .await
            .map_err(|_| StoreError::Unavailable)?;
        if owner != agent {
            pool.close().await;
            return Err(StoreError::WrongAgent(owner));
        }
        // Readers don't wait for the writer under WAL, or for each other.
        let readers = sqlx::sqlite::SqlitePoolOptions::new()
            .max_connections(READ_CONNECTIONS)
            .connect_with(
                SqliteConnectOptions::new()
                    .filename(&path)
                    .read_only(true)
                    .busy_timeout(Duration::from_secs(5)),
            )
            .await
            .map_err(|_| StoreError::Unavailable)?;
        Ok(Self {
            pool,
            readers,
            listener: std::sync::OnceLock::new(),
            agent: agent.into(),
            limits,
            _lock: lock,
            writes: tokio::sync::Semaphore::new(64),
            write_bytes: tokio::sync::Semaphore::new(8 * 1024 * 1024),
        })
    }
    /// Events are published only after their transaction commits, through this listener.
    pub fn on_commit(&self, listener: CommitListener) {
        let _ = self.listener.set(listener);
    }
    pub(crate) fn publish(&self, events: Vec<(String, DurableEvent)>) {
        if let (false, Some(listener)) = (events.is_empty(), self.listener.get()) {
            listener(events);
        }
    }
    pub fn agent(&self) -> &str {
        &self.agent
    }
    /// The replica's instance ID: the configured one, or one kept in the store when unset, as agent-pi does.
    pub async fn instance_id(&self, configured: Option<&str>) -> StoreResult<String> {
        if let Some(id) = configured {
            return Ok(id.into());
        }
        let generated: String = uuid::Uuid::new_v4().simple().to_string()[..8].into();
        sqlx::query("INSERT INTO store_meta VALUES('instance',?) ON CONFLICT DO NOTHING")
            .bind(&generated)
            .execute(&self.pool)
            .await
            .map_err(|_| StoreError::Unavailable)?;
        sqlx::query_scalar("SELECT value FROM store_meta WHERE key='instance'")
            .fetch_one(&self.pool)
            .await
            .map_err(|_| StoreError::Unavailable)
    }
    pub async fn close(self) {
        self.readers.close().await;
        self.pool.close().await;
    }
}
