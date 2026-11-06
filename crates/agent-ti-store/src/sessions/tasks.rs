//! Background tasks (agent-pi's `app.tasks`, `docs/async-tasks.md`): what a parent session's
//! tasks are and how they ended, and the messages between parent and child. The runtime's task
//! supervisor moves messages along; every step here is one transaction, and every message has the
//! request key that dedupes its submission, so a restart repeats nothing.
use super::*;

/// A session's whole tree's tokens, in a statement on `sessions` as `s` or the bare table. As
/// agent-pi's `maxUnattendedTokens` counts: no cache reads, which this store doesn't record.
const TREE_SPENT: &str = "(input_tokens+output_tokens+COALESCE((SELECT SUM(c.input_tokens+c.output_tokens) FROM sessions c WHERE c.tenant_id=sessions.tenant_id AND c.agent_id=sessions.agent_id AND c.parent=sessions.session_id),0))";

/// How far a session may wake itself with nobody there: `limits.maxUnattendedRuns` and
/// `maxUnattendedTokens`, both counted since a user last spoke.
#[derive(Clone, Copy, Debug)]
pub struct UnattendedBudget {
    pub runs: u64,
    pub tokens: u64,
}

/// What a session spent, as (input, output) tokens.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SessionUsage {
    pub own: (u64, u64),
    pub foreground: (u64, u64),
    /// By task ID.
    pub tasks: Vec<(String, (u64, u64))>,
}

/// One background task of a session.
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TaskRow {
    pub task_id: String,
    pub name: String,
    pub status: String,
    #[serde(skip)]
    pub child: Option<String>,
    #[serde(skip)]
    pub pending: i64,
    #[serde(skip)]
    pub run_at: i64,
    #[serde(skip)]
    pub timed_out: bool,
    #[serde(skip)]
    pub heard_at: Option<i64>,
    #[serde(skip)]
    pub last_report: Option<String>,
    #[serde(skip)]
    pub started_at: i64,
}
impl TaskRow {
    pub fn live(&self) -> bool {
        matches!(self.status.as_str(), "working" | "idle" | "failed")
    }
}
/// A message the supervisor has yet to move: an input to give a child, or a report to give a parent.
#[derive(Clone, Debug)]
pub struct TaskMessage {
    /// The parent session's owner.
    pub scope: UserScope,
    pub session_id: String,
    pub task_id: String,
    pub request_key: String,
    pub kind: String,
    pub content: String,
    pub urgent: bool,
    pub level: String,
    pub status: String,
}
/// The report of a task that ended: its marked `content` for the parent, and its plain `text` for
/// `task_list`.
pub struct Ended<'a> {
    pub key: &'a str,
    pub text: &'a str,
    pub content: &'a str,
    pub wanted: &'a str,
    pub budget: UnattendedBudget,
}
/// What a settled input reports.
pub struct Settled {
    /// `working` if the child has more queued, else `idle`, or `failed` without an answer.
    pub status: String,
    pub content: String,
    pub last_report: String,
    pub report_key: String,
    /// The profile's `taskReports`: `wake` unless the parent takes reports as notes.
    pub wanted: String,
    pub budget: UnattendedBudget,
}

fn task_row(r: &sqlx::sqlite::SqliteRow) -> TaskRow {
    TaskRow {
        task_id: r.get("task_id"),
        name: r.get("name"),
        status: r.get("status"),
        child: r.get("child"),
        pending: r.get("pending"),
        run_at: r.get("run_at"),
        timed_out: r.get("timed_out"),
        heard_at: r.get("heard_at"),
        last_report: r.get("last_report"),
        started_at: r.get("started_at"),
    }
}
/// Commits `task.updated` when a task's status changes, as agent-pi's hub emits it.
#[allow(clippy::too_many_arguments)]
async fn status_event(
    tx: &mut Transaction<'_, Sqlite>,
    out: &mut Committed,
    s: &UserScope,
    session_id: &str,
    task_id: &str,
    name: &str,
    before: &str,
    after: &str,
) -> StoreResult<()> {
    if before == after {
        return Ok(());
    }
    event(
        tx,
        out,
        s,
        session_id,
        json!({"type":"task.updated","taskId":task_id,"name":name,"status":after}),
    )
    .await
}
/// Spends one of the parent's unattended runs for a wake, or makes it a note when none are left.
async fn claim(
    tx: &mut Transaction<'_, Sqlite>,
    s: &UserScope,
    session_id: &str,
    wanted: &str,
    budget: UnattendedBudget,
) -> StoreResult<&'static str> {
    if wanted == "note" {
        return Ok("note");
    }
    let n = sqlx::query(&format!("UPDATE sessions SET unattended=unattended+1 WHERE tenant_id=? AND agent_id=? AND session_id=? AND unattended<? AND {TREE_SPENT}-spent_at_input<?"))
        .bind(&s.tenant_id).bind(&s.agent_id).bind(session_id).bind(budget.runs as i64).bind(budget.tokens.min(i64::MAX as u64) as i64).execute(&mut **tx).await.map_err(db)?;
    Ok(if n.rows_affected() == 1 {
        "wake"
    } else {
        "note"
    })
}

impl AgentStore {
    /// Starts a task for the tool call `call_key`, with its first input, unless a rerun already
    /// did. `None` when the session has `max_live` tasks alive.
    pub async fn task_start(
        &self,
        s: &UserScope,
        session_id: &str,
        call_key: &str,
        name: &str,
        input: &str,
        max_live: usize,
    ) -> StoreResult<Option<TaskRow>> {
        let _guard = self.write_guard(input.len() + 4096)?;
        let mut conn = self.begin().await?;
        let mut tx = conn.begin_with("BEGIN IMMEDIATE").await.map_err(db)?;
        let mut out = Committed::new();
        owned(&mut tx, s, session_id).await?;
        let q = "SELECT * FROM tasks WHERE tenant_id=? AND agent_id=? AND session_id=?";
        let rows = sqlx::query(q)
            .bind(&s.tenant_id)
            .bind(&s.agent_id)
            .bind(session_id)
            .fetch_all(&mut *tx)
            .await
            .map_err(db)?;
        if let Some(existing) = rows
            .iter()
            .find(|r| r.get::<String, _>("call_key") == call_key)
        {
            return Ok(Some(task_row(existing)));
        }
        let tasks: Vec<TaskRow> = rows.iter().map(task_row).collect();
        if tasks.iter().filter(|t| t.live()).count() >= max_live {
            return Ok(None);
        }
        let task_id = format!("t{}", tasks.len() + 1);
        let now = now_ms();
        sqlx::query("INSERT INTO tasks(tenant_id,agent_id,session_id,task_id,call_key,name,status,pending,run_at,started_at,active_at) VALUES(?,?,?,?,?,?,'working',1,?,?,?)")
            .bind(&s.tenant_id).bind(&s.agent_id).bind(session_id).bind(&task_id).bind(call_key).bind(name).bind(now).bind(now).bind(now)
            .execute(&mut *tx).await.map_err(db)?;
        sqlx::query("INSERT INTO task_messages(tenant_id,agent_id,session_id,task_id,request_key,kind,content,created_at) VALUES(?,?,?,?,?,'input',?,?)")
            .bind(&s.tenant_id).bind(&s.agent_id).bind(session_id).bind(&task_id).bind(format!("task-input:{call_key}")).bind(input).bind(now)
            .execute(&mut *tx).await.map_err(db)?;
        status_event(
            &mut tx, &mut out, s, session_id, &task_id, name, "", "working",
        )
        .await?;
        let row = sqlx::query(
            "SELECT * FROM tasks WHERE tenant_id=? AND agent_id=? AND session_id=? AND task_id=?",
        )
        .bind(&s.tenant_id)
        .bind(&s.agent_id)
        .bind(session_id)
        .bind(&task_id)
        .fetch_one(&mut *tx)
        .await
        .map_err(db)?;
        let started = task_row(&row);
        tx.commit().await.map_err(db)?;
        self.publish(out);
        Ok(Some(started))
    }
    pub async fn task_set_child(
        &self,
        s: &UserScope,
        session_id: &str,
        task_id: &str,
        child: &str,
    ) -> StoreResult<()> {
        sqlx::query("UPDATE tasks SET child=? WHERE tenant_id=? AND agent_id=? AND session_id=? AND task_id=? AND child IS NULL")
            .bind(child).bind(&s.tenant_id).bind(&s.agent_id).bind(session_id).bind(task_id).execute(&self.pool).await.map_err(db)?;
        Ok(())
    }
    /// A session's tasks, oldest first.
    pub async fn tasks(&self, s: &UserScope, session_id: &str) -> StoreResult<Vec<TaskRow>> {
        let rows = sqlx::query("SELECT * FROM tasks WHERE tenant_id=? AND agent_id=? AND session_id=? ORDER BY started_at, task_id")
            .bind(&s.tenant_id).bind(&s.agent_id).bind(session_id).fetch_all(&self.pool).await.map_err(db)?;
        Ok(rows.iter().map(task_row).collect())
    }
    /// Every live task of this agent, with its parent's scope, for the supervisor's deadlines.
    pub async fn live_tasks(&self) -> StoreResult<Vec<(UserScope, String, TaskRow)>> {
        let rows = sqlx::query("SELECT t.*,s.owner_user_id FROM tasks t JOIN sessions s USING(tenant_id,agent_id,session_id) WHERE t.status IN ('working','idle','failed')")
            .fetch_all(&self.pool).await.map_err(db)?;
        Ok(rows
            .iter()
            .map(|r| {
                let scope = UserScope {
                    tenant_id: r.get("tenant_id"),
                    agent_id: r.get("agent_id"),
                    user_id: r.get("owner_user_id"),
                };
                (scope, r.get("session_id"), task_row(r))
            })
            .collect())
    }
    /// Gives a task more work: `Err` with what the model should read when it can't take any.
    pub async fn task_send(
        &self,
        s: &UserScope,
        session_id: &str,
        task_id: &str,
        request_key: &str,
        message: &str,
        urgent: bool,
    ) -> StoreResult<Result<(), String>> {
        let _guard = self.write_guard(message.len() + 4096)?;
        let mut conn = self.begin().await?;
        let mut tx = conn.begin_with("BEGIN IMMEDIATE").await.map_err(db)?;
        let mut out = Committed::new();
        owned(&mut tx, s, session_id).await?;
        let Some(row) = sqlx::query(
            "SELECT * FROM tasks WHERE tenant_id=? AND agent_id=? AND session_id=? AND task_id=?",
        )
        .bind(&s.tenant_id)
        .bind(&s.agent_id)
        .bind(session_id)
        .bind(task_id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(db)?
        else {
            return Ok(Err(format!("There is no task {task_id}.")));
        };
        let task = task_row(&row);
        let sent: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM task_messages WHERE tenant_id=? AND agent_id=? AND session_id=? AND request_key=?")
            .bind(&s.tenant_id).bind(&s.agent_id).bind(session_id).bind(request_key).fetch_one(&mut *tx).await.map_err(db)?;
        if sent > 0 {
            return Ok(Ok(()));
        }
        if !task.live() {
            return Ok(Err(format!("Task {task_id} has ended ({}).", task.status)));
        }
        if task.child.is_none() {
            return Ok(Err(format!(
                "Task {task_id} is still starting. Try again shortly."
            )));
        }
        let now = now_ms();
        let run_at = if task.status == "working" {
            task.run_at
        } else {
            now
        };
        sqlx::query("UPDATE tasks SET pending=pending+1,status='working',run_at=?,active_at=?,heard_at=NULL WHERE tenant_id=? AND agent_id=? AND session_id=? AND task_id=?")
            .bind(run_at).bind(now).bind(&s.tenant_id).bind(&s.agent_id).bind(session_id).bind(task_id).execute(&mut *tx).await.map_err(db)?;
        sqlx::query("INSERT INTO task_messages(tenant_id,agent_id,session_id,task_id,request_key,kind,content,urgent,created_at) VALUES(?,?,?,?,?,'input',?,?,?)")
            .bind(&s.tenant_id).bind(&s.agent_id).bind(session_id).bind(task_id).bind(request_key).bind(message).bind(urgent).bind(now)
            .execute(&mut *tx).await.map_err(db)?;
        status_event(
            &mut tx,
            &mut out,
            s,
            session_id,
            task_id,
            &task.name,
            &task.status,
            "working",
        )
        .await?;
        tx.commit().await.map_err(db)?;
        self.publish(out);
        Ok(Ok(()))
    }
    /// Messages not yet moved, oldest first, across this agent's sessions.
    pub async fn open_task_messages(&self, limit: u32) -> StoreResult<Vec<TaskMessage>> {
        let rows = sqlx::query("SELECT m.*,s.owner_user_id FROM task_messages m JOIN sessions s USING(tenant_id,agent_id,session_id) WHERE m.status<>'done' ORDER BY m.created_at LIMIT ?")
            .bind(limit).fetch_all(&self.pool).await.map_err(db)?;
        Ok(rows
            .iter()
            .map(|r| TaskMessage {
                scope: UserScope {
                    tenant_id: r.get("tenant_id"),
                    agent_id: r.get("agent_id"),
                    user_id: r.get("owner_user_id"),
                },
                session_id: r.get("session_id"),
                task_id: r.get("task_id"),
                request_key: r.get("request_key"),
                kind: r.get("kind"),
                content: r.get("content"),
                urgent: r.get("urgent"),
                level: r.get("level"),
                status: r.get("status"),
            })
            .collect())
    }
    pub async fn task_message_status(&self, m: &TaskMessage, status: &str) -> StoreResult<()> {
        sqlx::query("UPDATE task_messages SET status=? WHERE tenant_id=? AND agent_id=? AND session_id=? AND request_key=?")
            .bind(status).bind(&m.scope.tenant_id).bind(&m.scope.agent_id).bind(&m.session_id).bind(&m.request_key)
            .execute(&self.pool).await.map_err(db)?;
        Ok(())
    }
    /// An input to the child settled: the task's new state, and its report queued for the parent,
    /// in one transaction. `report` is `None` for an input that settled without an answer of its
    /// own, as a steer that joined a run does.
    pub async fn task_settle(
        &self,
        input: &TaskMessage,
        report: Option<Settled>,
    ) -> StoreResult<()> {
        let (s, session_id) = (&input.scope, &input.session_id);
        let _guard = self.write_guard(report.as_ref().map_or(0, |r| r.content.len()) + 4096)?;
        let mut conn = self.begin().await?;
        let mut tx = conn.begin_with("BEGIN IMMEDIATE").await.map_err(db)?;
        let mut out = Committed::new();
        let row = sqlx::query(
            "SELECT * FROM tasks WHERE tenant_id=? AND agent_id=? AND session_id=? AND task_id=?",
        )
        .bind(&s.tenant_id)
        .bind(&s.agent_id)
        .bind(session_id)
        .bind(&input.task_id)
        .fetch_one(&mut *tx)
        .await
        .map_err(db)?;
        let task = task_row(&row);
        sqlx::query("UPDATE task_messages SET status='done' WHERE tenant_id=? AND agent_id=? AND session_id=? AND request_key=?")
            .bind(&s.tenant_id).bind(&s.agent_id).bind(session_id).bind(&input.request_key).execute(&mut *tx).await.map_err(db)?;
        // The task ended meanwhile, and whatever ended it told the parent.
        if task.live() {
            let pending = (task.pending - 1).max(0);
            let status = match &report {
                _ if pending > 0 => "working",
                Some(r) if r.status == "failed" => "failed",
                _ => "idle",
            };
            let now = now_ms();
            sqlx::query("UPDATE tasks SET pending=?,status=?,notes=0,timed_out=0,heard_at=NULL,active_at=?,run_at=?,last_report=COALESCE(?,last_report) WHERE tenant_id=? AND agent_id=? AND session_id=? AND task_id=?")
                .bind(pending).bind(status).bind(now).bind(now).bind(report.as_ref().map(|r| r.last_report.clone()))
                .bind(&s.tenant_id).bind(&s.agent_id).bind(session_id).bind(&input.task_id).execute(&mut *tx).await.map_err(db)?;
            if let Some(r) = &report {
                let level = claim(&mut tx, s, session_id, &r.wanted, r.budget).await?;
                sqlx::query("INSERT OR IGNORE INTO task_messages(tenant_id,agent_id,session_id,task_id,request_key,kind,content,level,created_at) VALUES(?,?,?,?,?,'report',?,?,?)")
                    .bind(&s.tenant_id).bind(&s.agent_id).bind(session_id).bind(&input.task_id).bind(&r.report_key).bind(&r.content).bind(level).bind(now)
                    .execute(&mut *tx).await.map_err(db)?;
            }
            status_event(
                &mut tx,
                &mut out,
                s,
                session_id,
                &input.task_id,
                &task.name,
                &task.status,
                status,
            )
            .await?;
        }
        tx.commit().await.map_err(db)?;
        self.publish(out);
        Ok(())
    }
    /// Ends a live task as `stopped` or `expired`, with an optional report to the parent (`wanted`
    /// is its level before the unattended budget). Returns its child, to stop, or `None` when it
    /// had already ended.
    pub async fn task_end(
        &self,
        s: &UserScope,
        session_id: &str,
        task_id: &str,
        status: &str,
        report: Option<Ended<'_>>,
    ) -> StoreResult<Option<Option<String>>> {
        let _guard = self.write_guard(4096)?;
        let mut conn = self.begin().await?;
        let mut tx = conn.begin_with("BEGIN IMMEDIATE").await.map_err(db)?;
        let mut out = Committed::new();
        let Some(row) = sqlx::query(
            "SELECT * FROM tasks WHERE tenant_id=? AND agent_id=? AND session_id=? AND task_id=?",
        )
        .bind(&s.tenant_id)
        .bind(&s.agent_id)
        .bind(session_id)
        .bind(task_id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(db)?
        else {
            return Ok(None);
        };
        let task = task_row(&row);
        if !task.live() {
            return Ok(None);
        }
        let now = now_ms();
        let text = report.as_ref().map(|r| r.text);
        sqlx::query("UPDATE tasks SET status=?,active_at=?,last_report=COALESCE(?,last_report) WHERE tenant_id=? AND agent_id=? AND session_id=? AND task_id=?")
            .bind(status).bind(now).bind(text).bind(&s.tenant_id).bind(&s.agent_id).bind(session_id).bind(task_id).execute(&mut *tx).await.map_err(db)?;
        // Inputs it hadn't taken are dropped with it.
        sqlx::query("UPDATE task_messages SET status='done' WHERE tenant_id=? AND agent_id=? AND session_id=? AND task_id=? AND kind='input'")
            .bind(&s.tenant_id).bind(&s.agent_id).bind(session_id).bind(task_id).execute(&mut *tx).await.map_err(db)?;
        if let Some(r) = report {
            let level = claim(&mut tx, s, session_id, r.wanted, r.budget).await?;
            sqlx::query("INSERT OR IGNORE INTO task_messages(tenant_id,agent_id,session_id,task_id,request_key,kind,content,level,created_at) VALUES(?,?,?,?,?,'report',?,?,?)")
                .bind(&s.tenant_id).bind(&s.agent_id).bind(session_id).bind(task_id).bind(r.key).bind(r.content).bind(level).bind(now)
                .execute(&mut *tx).await.map_err(db)?;
        }
        status_event(
            &mut tx,
            &mut out,
            s,
            session_id,
            task_id,
            &task.name,
            &task.status,
            status,
        )
        .await?;
        tx.commit().await.map_err(db)?;
        self.publish(out);
        Ok(Some(task.child))
    }
    /// A run's deadline passed: mark it, once, so the settled input reports a timeout. False if it
    /// was already marked, or the run ended meanwhile.
    pub async fn task_time_out(
        &self,
        s: &UserScope,
        session_id: &str,
        task_id: &str,
        run_at: i64,
    ) -> StoreResult<bool> {
        let n = sqlx::query("UPDATE tasks SET timed_out=1 WHERE tenant_id=? AND agent_id=? AND session_id=? AND task_id=? AND status='working' AND timed_out=0 AND run_at=?")
            .bind(&s.tenant_id).bind(&s.agent_id).bind(session_id).bind(task_id).bind(run_at).execute(&self.pool).await.map_err(db)?;
        Ok(n.rows_affected() == 1)
    }
    /// Whether a session has a command running, rather than queued.
    pub async fn running(&self, s: &UserScope, session_id: &str) -> StoreResult<bool> {
        let n: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM commands WHERE tenant_id=? AND agent_id=? AND session_id=? AND status='running'")
            .bind(&s.tenant_id).bind(&s.agent_id).bind(session_id).fetch_one(&self.pool).await.map_err(db)?;
        Ok(n > 0)
    }
    /// A task's run hasn't started, so its deadline hasn't either.
    pub async fn task_waiting(
        &self,
        s: &UserScope,
        session_id: &str,
        task_id: &str,
        run_at: i64,
    ) -> StoreResult<()> {
        sqlx::query("UPDATE tasks SET run_at=? WHERE tenant_id=? AND agent_id=? AND session_id=? AND task_id=? AND status='working' AND run_at=?")
            .bind(now_ms()).bind(&s.tenant_id).bind(&s.agent_id).bind(session_id).bind(task_id).bind(run_at).execute(&self.pool).await.map_err(db)?;
        Ok(())
    }
    /// What a session spent, its own and through its children. A foreground child's spend counts
    /// as the parent's own, as in agent-pi, where it reaches `usage` through the tool's result; a
    /// background task's is apart, in `tasks`. Computed from the children's rows, so a parent's
    /// tool running again after a restart can't count anything twice.
    pub async fn usage(&self, s: &UserScope, session_id: &str) -> StoreResult<SessionUsage> {
        let mut conn = self.read().await?;
        let (input, output): (i64, i64) = sqlx::query_as(
            "SELECT input_tokens,output_tokens FROM sessions WHERE tenant_id=? AND agent_id=? AND session_id=?",
        )
        .bind(&s.tenant_id)
        .bind(&s.agent_id)
        .bind(session_id)
        .fetch_one(&mut *conn)
        .await
        .map_err(db)?;
        let (child_input, child_output): (i64, i64) = sqlx::query_as(
            "SELECT COALESCE(SUM(input_tokens),0),COALESCE(SUM(output_tokens),0) FROM sessions c WHERE c.tenant_id=? AND c.agent_id=? AND c.parent=? \
             AND NOT EXISTS (SELECT 1 FROM tasks t WHERE t.tenant_id=c.tenant_id AND t.agent_id=c.agent_id AND t.session_id=c.parent AND t.child=c.session_id)",
        )
        .bind(&s.tenant_id)
        .bind(&s.agent_id)
        .bind(session_id)
        .fetch_one(&mut *conn)
        .await
        .map_err(db)?;
        let tasks: Vec<(String, i64, i64)> = sqlx::query_as(
            "SELECT t.task_id,c.input_tokens,c.output_tokens FROM tasks t JOIN sessions c ON c.tenant_id=t.tenant_id AND c.agent_id=t.agent_id AND c.session_id=t.child \
             WHERE t.tenant_id=? AND t.agent_id=? AND t.session_id=?",
        )
        .bind(&s.tenant_id)
        .bind(&s.agent_id)
        .bind(session_id)
        .fetch_all(&mut *conn)
        .await
        .map_err(db)?;
        Ok(SessionUsage {
            own: (input as u64, output as u64),
            foreground: (child_input as u64, child_output as u64),
            tasks: tasks
                .into_iter()
                .map(|(id, i, o)| (id, (i as u64, o as u64)))
                .collect(),
        })
    }
    /// A user spoke in the session: idle tasks' reply clocks start, and unattended runs reset.
    pub async fn tasks_heard(&self, s: &UserScope, session_id: &str) -> StoreResult<()> {
        sqlx::query("UPDATE tasks SET heard_at=? WHERE tenant_id=? AND agent_id=? AND session_id=? AND status IN ('idle','failed') AND heard_at IS NULL")
            .bind(now_ms()).bind(&s.tenant_id).bind(&s.agent_id).bind(session_id).execute(&self.pool).await.map_err(db)?;
        sqlx::query(&format!(
            "UPDATE sessions SET unattended=0,spent_at_input={TREE_SPENT} WHERE tenant_id=? AND agent_id=? AND session_id=?"
        ))
        .bind(&s.tenant_id)
        .bind(&s.agent_id)
        .bind(session_id)
        .execute(&self.pool)
        .await
        .map_err(db)?;
        Ok(())
    }
    /// A child's `task_note`: counted against `max` per input it was given, and queued as a report
    /// (urgent: a wake that joins the parent's run) or a note. `Err` when it may send no more.
    #[allow(clippy::too_many_arguments)]
    pub async fn task_note(
        &self,
        s: &UserScope,
        session_id: &str,
        task_id: &str,
        request_key: &str,
        content: &str,
        urgent: Option<(&str, UnattendedBudget)>,
        max: i64,
    ) -> StoreResult<Result<(), String>> {
        let _guard = self.write_guard(content.len() + 4096)?;
        let mut conn = self.begin().await?;
        let mut tx = conn.begin_with("BEGIN IMMEDIATE").await.map_err(db)?;
        let done: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM task_messages WHERE tenant_id=? AND agent_id=? AND session_id=? AND request_key=?")
            .bind(&s.tenant_id).bind(&s.agent_id).bind(session_id).bind(request_key).fetch_one(&mut *tx).await.map_err(db)?;
        if done > 0 {
            return Ok(Ok(()));
        }
        let n = sqlx::query("UPDATE tasks SET notes=notes+1 WHERE tenant_id=? AND agent_id=? AND session_id=? AND task_id=? AND status IN ('working','idle','failed') AND notes<?")
            .bind(&s.tenant_id).bind(&s.agent_id).bind(session_id).bind(task_id).bind(max).execute(&mut *tx).await.map_err(db)?;
        if n.rows_affected() == 0 {
            return Ok(Err(
                "That is enough notes for now. Put the rest in your answer.".into(),
            ));
        }
        let level = match urgent {
            Some((wanted, budget)) => claim(&mut tx, s, session_id, wanted, budget).await?,
            None => "note",
        };
        sqlx::query("INSERT INTO task_messages(tenant_id,agent_id,session_id,task_id,request_key,kind,content,urgent,level,created_at) VALUES(?,?,?,?,?,'report',?,1,?,?)")
            .bind(&s.tenant_id).bind(&s.agent_id).bind(session_id).bind(task_id).bind(request_key).bind(content).bind(level).bind(now_ms())
            .execute(&mut *tx).await.map_err(db)?;
        tx.commit().await.map_err(db)?;
        Ok(Ok(()))
    }
    /// Puts a message in front of the session's next model request without starting a run, as
    /// pi-durable's queued `write`: once per `key`, and only at a boundary. While the session has
    /// work running or queued this places nothing and returns false, so a note never lands between
    /// a run's input and the request it makes; a command claimed later sees it ahead of its input.
    pub async fn note_message(
        &self,
        s: &UserScope,
        session_id: &str,
        key: &str,
        content: &str,
    ) -> StoreResult<bool> {
        let _guard = self.write_guard(content.len() + 4096)?;
        let mut conn = self.begin().await?;
        let mut tx = conn.begin_with("BEGIN IMMEDIATE").await.map_err(db)?;
        owned(&mut tx, s, session_id).await?;
        let busy: i64 = sqlx::query_scalar(&format!("SELECT COUNT(*) FROM commands WHERE tenant_id=? AND agent_id=? AND session_id=? AND status IN {PENDING}"))
            .bind(&s.tenant_id).bind(&s.agent_id).bind(session_id).fetch_one(&mut *tx).await.map_err(db)?;
        if busy > 0 {
            return Ok(false);
        }
        message(
            &mut tx,
            s,
            session_id,
            key,
            0,
            &Message::text("user", content),
        )
        .await?;
        tx.commit().await.map_err(db)?;
        Ok(true)
    }
    /// Whether a command ran far enough to place its input in the transcript.
    pub async fn placed(
        &self,
        s: &UserScope,
        session_id: &str,
        request_key: &str,
    ) -> StoreResult<bool> {
        let n: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM messages m JOIN commands c USING(tenant_id,agent_id,session_id,command_id) WHERE m.tenant_id=? AND m.agent_id=? AND m.session_id=? AND c.request_key=?")
            .bind(&s.tenant_id).bind(&s.agent_id).bind(session_id).bind(request_key).fetch_one(&self.pool).await.map_err(db)?;
        Ok(n > 0)
    }
}
