//! Domain operations on one agent's store. Every user operation takes the tenant, agent and user
//! scope; another user's session is not found.
use super::*;
use serde_json::json;
use sqlx::{Connection, Row, Sqlite, SqliteConnection, Transaction};
use std::collections::{BTreeMap, HashSet};
use tokio::sync::SemaphorePermit;

mod maintenance;
pub use maintenance::Pruned;

type Committed = Vec<(String, DurableEvent)>;
mod tasks;
pub use tasks::{Ended, SessionUsage, Settled, TaskMessage, TaskRow, UnattendedBudget};
const PENDING: &str = "('queued','running','parked')";
fn db(_: sqlx::Error) -> StoreError {
    StoreError::Unavailable
}
/// The most history one model request may carry, compacted or not.
const MAX_HISTORY_BYTES: usize = 4 * 1024 * 1024;
const SUMMARY_PREFIX: &str = "The conversation history before this point was compacted into the following summary:\n\n<summary>\n";
const SUMMARY_SUFFIX: &str = "\n</summary>";
/// A compaction: the summary of every message before `head`.
#[derive(Clone, Debug)]
pub struct Summary {
    pub head: i64,
    pub text: String,
}
impl Summary {
    /// The user message a model request starts with, as pi-durable words it.
    pub fn message(&self) -> String {
        format!("{SUMMARY_PREFIX}{}{SUMMARY_SUFFIX}", self.text)
    }
}
fn encode(v: &impl serde::Serialize) -> StoreResult<String> {
    serde_json::to_string(v).map_err(|_| StoreError::Invalid)
}
fn decode<T: serde::de::DeserializeOwned>(text: &str) -> StoreResult<T> {
    serde_json::from_str(text).map_err(|_| StoreError::Invalid)
}
fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}
fn session(row: &sqlx::sqlite::SqliteRow) -> StoreResult<Session> {
    Ok(Session {
        session_id: row.get("session_id"),
        config_version: row.get("config_version"),
        profile: row.get("profile"),
        name: row.get("name"),
        status: row.get("status"),
        origin: decode(row.get("origin"))?,
        version: row.get("version"),
        metadata: decode(row.get("metadata"))?,
        created_at: row.get("created_at"),
        last_active_at: row.get("last_active_at"),
        busy: row.get("busy"),
        parked: row.get("parked"),
        parent: row.get("parent"),
    })
}
const SESSION: &str = "SELECT s.*, \
 EXISTS(SELECT 1 FROM commands c WHERE c.tenant_id=s.tenant_id AND c.agent_id=s.agent_id AND c.session_id=s.session_id AND c.status IN ('queued','running','parked')) AS busy, \
 (SELECT COUNT(*) FROM commands c WHERE c.tenant_id=s.tenant_id AND c.agent_id=s.agent_id AND c.session_id=s.session_id AND c.status='parked') AS parked \
 FROM sessions s";
async fn owned(
    conn: &mut SqliteConnection,
    s: &UserScope,
    id: &str,
) -> StoreResult<sqlx::sqlite::SqliteRow> {
    sqlx::query(&format!(
        "{SESSION} WHERE tenant_id=? AND agent_id=? AND owner_user_id=? AND session_id=?"
    ))
    .bind(&s.tenant_id)
    .bind(&s.agent_id)
    .bind(&s.user_id)
    .bind(id)
    .fetch_optional(conn)
    .await
    .map_err(db)?
    .ok_or(StoreError::NotFound)
}
/// Writes an event and adds it to `out`, which is published once the transaction commits.
async fn write_flow(
    tx: &mut Transaction<'_, Sqlite>,
    out: &mut Committed,
    s: &UserScope,
    id: &str,
    trace: Value,
) -> StoreResult<()> {
    sqlx::query("UPDATE sessions SET flow=? WHERE tenant_id=? AND agent_id=? AND session_id=?")
        .bind(encode(&trace)?)
        .bind(&s.tenant_id)
        .bind(&s.agent_id)
        .bind(id)
        .execute(&mut **tx)
        .await
        .map_err(db)?;
    event(tx, out, s, id, json!({"type":"flow.updated","trace":trace})).await?;
    Ok(())
}
async fn event(
    tx: &mut Transaction<'_, Sqlite>,
    out: &mut Committed,
    s: &UserScope,
    id: &str,
    payload: Value,
) -> StoreResult<()> {
    let seq: i64 = sqlx::query_scalar("UPDATE sessions SET next_event=next_event+1, version=version+1, last_active_at=? WHERE tenant_id=? AND agent_id=? AND session_id=? RETURNING next_event")
        .bind(now_ms()).bind(&s.tenant_id).bind(&s.agent_id).bind(id).fetch_one(&mut **tx).await.map_err(db)?;
    sqlx::query("INSERT INTO events VALUES(?,?,?,?,?)")
        .bind(&s.tenant_id)
        .bind(&s.agent_id)
        .bind(id)
        .bind(seq)
        .bind(encode(&payload)?)
        .execute(&mut **tx)
        .await
        .map_err(db)?;
    out.push((
        id.to_owned(),
        DurableEvent {
            sequence: seq,
            payload,
        },
    ));
    Ok(())
}
async fn message(
    tx: &mut Transaction<'_, Sqlite>,
    s: &UserScope,
    session_id: &str,
    command_id: &str,
    position: i64,
    m: &Message,
) -> StoreResult<()> {
    let exists: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM messages WHERE tenant_id=? AND agent_id=? AND session_id=? AND command_id=? AND position=?")
        .bind(&s.tenant_id).bind(&s.agent_id).bind(session_id).bind(command_id).bind(position).fetch_one(&mut **tx).await.map_err(db)?;
    if exists != 0 {
        return Ok(());
    }
    let seq: i64 = sqlx::query_scalar("UPDATE sessions SET next_message=next_message+1 WHERE tenant_id=? AND agent_id=? AND session_id=? RETURNING next_message")
        .bind(&s.tenant_id).bind(&s.agent_id).bind(session_id).fetch_one(&mut **tx).await.map_err(db)?;
    sqlx::query("INSERT INTO messages VALUES(?,?,?,?,?,?,?)")
        .bind(&s.tenant_id)
        .bind(&s.agent_id)
        .bind(session_id)
        .bind(seq)
        .bind(command_id)
        .bind(position)
        .bind(encode(m)?)
        .execute(&mut **tx)
        .await
        .map_err(db)?;
    Ok(())
}
/// The events of a plugin's answer, as for a model answer, with `source` naming the plugin.
async fn reply_events(
    tx: &mut Transaction<'_, Sqlite>,
    out: &mut Committed,
    s: &UserScope,
    session_id: &str,
    command_id: &str,
    turn_id: &str,
    reply: &Reply,
) -> StoreResult<()> {
    event(tx, out, s, session_id, json!({"type":"text.delta","turnId":turn_id,"messageId":command_id,"offset":0,"text":reply.text})).await?;
    event(tx, out, s, session_id,json!({"type":"message.end","turnId":turn_id,"messageId":command_id,"text":reply.text,"stopReason":"stop","source":reply.source,"utf16Length":reply.text.encode_utf16().count()})).await
}
fn key(s: &str) -> bool {
    !s.is_empty() && s.len() <= 128 && !s.chars().any(char::is_control)
}
fn user_message(input: &str, reply: Option<&Reply>) -> Message {
    Message {
        source: reply.map(|r| r.source.clone()),
        ..Message::text("user", input)
    }
}

impl AgentStore {
    fn write_guard(&self, bytes: usize) -> StoreResult<(SemaphorePermit<'_>, SemaphorePermit<'_>)> {
        let count = self
            .writes
            .try_acquire()
            .map_err(|_| StoreError::QuotaExceeded)?;
        let size = u32::try_from(bytes.max(1)).map_err(|_| StoreError::QuotaExceeded)?;
        let bytes = self
            .write_bytes
            .try_acquire_many(size)
            .map_err(|_| StoreError::QuotaExceeded)?;
        Ok((count, bytes))
    }
    async fn begin(&self) -> StoreResult<sqlx::pool::PoolConnection<Sqlite>> {
        self.pool.acquire().await.map_err(db)
    }
    /// A connection for reading only. What has committed is there to read.
    async fn read(&self) -> StoreResult<sqlx::pool::PoolConnection<Sqlite>> {
        self.readers.acquire().await.map_err(db)
    }
    pub async fn healthy(&self) -> StoreResult<()> {
        sqlx::query("SELECT 1")
            .execute(&self.readers)
            .await
            .map_err(db)?;
        Ok(())
    }
    pub async fn create(&self, r: CreateSession) -> StoreResult<(Session, bool)> {
        if !key(&r.request_key)
            || r.scope.user_id.is_empty()
            || r.scope.user_id.len() > 256
            || encode(&r.origin)?.len() > 1024
            || !r.metadata.is_object()
            || encode(&r.metadata)?.len() > 4096
        {
            return Err(StoreError::Invalid);
        }
        let _guard = self.write_guard(4096)?;
        let mut conn = self.begin().await?;
        let mut tx = conn.begin_with("BEGIN IMMEDIATE").await.map_err(db)?;
        let fingerprint =
            digest(&json!({"profile":r.profile,"origin":r.origin,"metadata":r.metadata}));
        let existing = sqlx::query("SELECT fingerprint,session_id FROM create_keys WHERE tenant_id=? AND agent_id=? AND owner_user_id=? AND request_key=?")
            .bind(&r.scope.tenant_id).bind(&r.scope.agent_id).bind(&r.scope.user_id).bind(&r.request_key).fetch_optional(&mut *tx).await.map_err(db)?;
        if let Some(row) = existing {
            if row.get::<String, _>(0) != fingerprint {
                return Err(StoreError::Conflict);
            }
            return Ok((session(&owned(&mut tx, &r.scope, row.get(1)).await?)?, true));
        }
        let tenant: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM sessions WHERE tenant_id=? AND status='open' AND parent IS NULL",
        )
        .bind(&r.scope.tenant_id)
        .fetch_one(&mut *tx)
        .await
        .map_err(db)?;
        let user: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM sessions WHERE tenant_id=? AND owner_user_id=? AND status='open' AND parent IS NULL",
        )
        .bind(&r.scope.tenant_id)
        .bind(&r.scope.user_id)
        .fetch_one(&mut *tx)
        .await
        .map_err(db)?;
        if r.child.is_none()
            && (tenant as u64 >= self.limits.max_sessions_per_tenant
                || user as u64 >= self.limits.max_sessions_per_user)
        {
            return Err(StoreError::QuotaExceeded);
        }
        let now = now_ms();
        let (parent, child) = match &r.child {
            Some(c) => (Some(c.parent.clone()), Some(encode(&c.spec)?)),
            None => (None, None),
        };
        sqlx::query("INSERT INTO sessions(tenant_id,agent_id,session_id,owner_user_id,config_version,profile,origin,metadata,created_at,last_active_at,parent,child) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)")
            .bind(&r.scope.tenant_id).bind(&r.scope.agent_id).bind(&r.session_id).bind(&r.scope.user_id).bind(&r.config_version).bind(&r.profile)
            .bind(encode(&r.origin)?).bind(encode(&r.metadata)?).bind(now).bind(now).bind(parent).bind(child).execute(&mut *tx).await.map_err(db)?;
        sqlx::query("INSERT INTO create_keys VALUES(?,?,?,?,?,?)")
            .bind(&r.scope.tenant_id)
            .bind(&r.scope.agent_id)
            .bind(&r.scope.user_id)
            .bind(r.request_key)
            .bind(fingerprint)
            .bind(&r.session_id)
            .execute(&mut *tx)
            .await
            .map_err(db)?;
        let result = session(&owned(&mut tx, &r.scope, &r.session_id).await?)?;
        tx.commit().await.map_err(db)?;
        Ok((result, false))
    }
    pub async fn state(&self, scope: &UserScope, id: &str) -> StoreResult<SessionState> {
        let mut conn = self.read().await?;
        let row = owned(&mut conn, scope, id).await?;
        Ok(SessionState {
            session: session(&row)?,
            high_water: row.get("next_event"),
            busy: row.get("busy"),
        })
    }
    pub async fn list(&self, s: &UserScope, after: &str, limit: u32) -> StoreResult<Vec<Session>> {
        let rows = sqlx::query(&format!("{SESSION} WHERE tenant_id=? AND agent_id=? AND owner_user_id=? AND status='open' AND parent IS NULL AND session_id>? ORDER BY session_id LIMIT ?"))
            .bind(&s.tenant_id).bind(&s.agent_id).bind(&s.user_id).bind(after).bind(limit.clamp(1, 100)).fetch_all(&self.readers).await.map_err(db)?;
        rows.iter().map(session).collect()
    }
    /// A parent's most recent children, newest last, as agent-pi's `session.get` lists them.
    pub async fn children(&self, s: &UserScope, parent: &str) -> StoreResult<Vec<String>> {
        let mut ids: Vec<String> = sqlx::query_scalar("SELECT session_id FROM sessions WHERE tenant_id=? AND agent_id=? AND owner_user_id=? AND parent=? ORDER BY created_at DESC, session_id DESC LIMIT 20")
            .bind(&s.tenant_id).bind(&s.agent_id).bind(&s.user_id).bind(parent).fetch_all(&self.readers).await.map_err(db)?;
        ids.reverse();
        Ok(ids)
    }
    /// How a command ended, by its request key: its status and the last assistant message's text.
    pub async fn outcome(
        &self,
        s: &UserScope,
        session_id: &str,
        request_key: &str,
    ) -> StoreResult<Option<(String, String)>> {
        let mut conn = self.read().await?;
        let Some(row) = sqlx::query("SELECT command_id,status FROM commands WHERE tenant_id=? AND agent_id=? AND session_id=? AND request_key=?")
            .bind(&s.tenant_id).bind(&s.agent_id).bind(session_id).bind(request_key).fetch_optional(&mut *conn).await.map_err(db)? else {
            return Ok(None);
        };
        let (command, status): (String, String) = (row.get(0), row.get(1));
        let payloads: Vec<String> = sqlx::query_scalar("SELECT payload FROM messages WHERE tenant_id=? AND agent_id=? AND session_id=? AND command_id=? ORDER BY sequence")
            .bind(&s.tenant_id).bind(&s.agent_id).bind(session_id).bind(&command).fetch_all(&mut *conn).await.map_err(db)?;
        let mut answer = String::new();
        for p in payloads {
            let m: Message = decode(&p)?;
            if m.role == "assistant" && !m.content.is_empty() {
                answer = m.content;
            }
        }
        Ok(Some((status, answer)))
    }
    pub async fn transcript(
        &self,
        s: &UserScope,
        id: &str,
        before: i64,
        limit: u32,
    ) -> StoreResult<TranscriptPage> {
        let mut conn = self.read().await?;
        owned(&mut conn, s, id).await?;
        let limit = limit.clamp(1, 100) as usize;
        // Messages no command wrote (a plugin's history, a task's note) have no turn.
        let rows=sqlx::query("SELECT m.sequence,m.payload,COALESCE(c.turn_id,''),COALESCE(c.request_key,''),COALESCE(c.status,'completed') FROM messages m LEFT JOIN commands c USING(tenant_id,agent_id,session_id,command_id) WHERE m.tenant_id=? AND m.agent_id=? AND m.session_id=? AND m.sequence<? AND json_extract(m.payload,'$.role')<>'record' ORDER BY m.sequence DESC LIMIT ?")
            .bind(&s.tenant_id).bind(&s.agent_id).bind(id).bind(before).bind((limit+1) as i64).fetch_all(&mut *conn).await.map_err(db)?;
        let has_more = rows.len() > limit;
        let mut entries = rows
            .into_iter()
            .take(limit)
            .map(|r| {
                Ok(TranscriptEntry {
                    sequence: r.get(0),
                    message: decode(r.get(1))?,
                    turn_id: r.get(2),
                    request_id: r.get(3),
                    status: r.get(4),
                })
            })
            .collect::<StoreResult<Vec<_>>>()?;
        entries.reverse();
        Ok(TranscriptPage { entries, has_more })
    }
    pub async fn tool_payload(
        &self,
        s: &UserScope,
        id: &str,
        call_id: &str,
    ) -> StoreResult<Option<Value>> {
        let mut conn = self.read().await?;
        owned(&mut conn, s, id).await?;
        let rows = sqlx::query(
            "SELECT payload FROM messages WHERE tenant_id=? AND agent_id=? AND session_id=? AND ( \
             json_extract(payload, '$.tool_call_id')=? OR \
             (json_extract(payload, '$.tool_calls') IS NOT NULL AND EXISTS (SELECT 1 FROM json_each(json_extract(payload, '$.tool_calls')) WHERE json_extract(value, '$.id')=?)) \
            ) ORDER BY sequence ASC",
        )
        .bind(&s.tenant_id)
        .bind(&s.agent_id)
        .bind(id)
        .bind(call_id)
        .bind(call_id)
        .fetch_all(&mut *conn)
        .await
        .map_err(db)?;

        let mut request: Option<Value> = None;
        let mut response: Option<Value> = None;
        let mut tool_name: Option<String> = None;

        for row in rows {
            let payload: String = row.get(0);
            if let Ok(m) = decode::<Message>(&payload) {
                if request.is_none() {
                    if let Some(call) = m.tool_calls.iter().find(|c| c.id == call_id) {
                        tool_name = Some(call.name.clone());
                        request = Some(json!({
                            "type": "toolCall",
                            "id": call.id,
                            "name": call.name,
                            "arguments": call.arguments,
                        }));
                    }
                }
                if response.is_none()
                    && m.role == "tool"
                    && m.tool_call_id.as_deref() == Some(call_id)
                {
                    response = Some(json!({
                        "role": "toolResult",
                        "toolCallId": call_id,
                        "content": [
                            {
                                "type": "text",
                                "text": m.content,
                            }
                        ],
                        "isError": m.is_error,
                    }));
                }
            }
        }

        if let Some(ref name) = tool_name {
            if let Some(resp) = response.as_mut() {
                if resp.get("toolName").is_none() {
                    resp["toolName"] = json!(name);
                }
            }
        }

        if request.is_none() && response.is_none() {
            return Ok(None);
        }

        let mut result = serde_json::Map::new();
        if let Some(req) = request {
            result.insert("request".to_string(), req);
        }
        if let Some(resp) = response {
            result.insert("response".to_string(), resp);
        }
        Ok(Some(Value::Object(result)))
    }
    pub async fn events(
        &self,
        s: &UserScope,
        id: &str,
        after: i64,
        limit: u32,
    ) -> StoreResult<Vec<DurableEvent>> {
        let mut conn = self.read().await?;
        owned(&mut conn, s, id).await?;
        let rows = sqlx::query("SELECT sequence,payload FROM events WHERE tenant_id=? AND agent_id=? AND session_id=? AND sequence>? ORDER BY sequence LIMIT ?")
            .bind(&s.tenant_id).bind(&s.agent_id).bind(id).bind(after).bind(limit.clamp(1, 100)).fetch_all(&mut *conn).await.map_err(db)?;
        rows.iter()
            .map(|r| {
                Ok(DurableEvent {
                    sequence: r.get(0),
                    payload: decode(r.get(1))?,
                })
            })
            .collect()
    }
    pub async fn update(
        &self,
        s: &UserScope,
        id: &str,
        name: Option<String>,
        metadata: Option<Value>,
        close: bool,
    ) -> StoreResult<Session> {
        if name.as_ref().is_some_and(|v| v.len() > 256)
            || metadata
                .as_ref()
                .is_some_and(|m| !m.is_object() || m.to_string().len() > 4096)
        {
            return Err(StoreError::Invalid);
        }
        let _guard = self.write_guard(4096)?;
        let mut conn = self.begin().await?;
        let mut tx = conn.begin_with("BEGIN IMMEDIATE").await.map_err(db)?;
        owned(&mut tx, s, id).await?;
        if close {
            let busy: i64 = sqlx::query_scalar(&format!("SELECT COUNT(*) FROM commands WHERE tenant_id=? AND agent_id=? AND session_id=? AND status IN {PENDING}"))
                .bind(&s.tenant_id).bind(&s.agent_id).bind(id).fetch_one(&mut *tx).await.map_err(db)?;
            if busy > 0 {
                return Err(StoreError::Conflict);
            }
        }
        sqlx::query("UPDATE sessions SET name=COALESCE(?,name),metadata=COALESCE(?,metadata),status=CASE WHEN ? THEN 'closed' ELSE status END,version=version+1 WHERE tenant_id=? AND agent_id=? AND session_id=?")
            .bind(name).bind(metadata.map(|m| m.to_string())).bind(close).bind(&s.tenant_id).bind(&s.agent_id).bind(id).execute(&mut *tx).await.map_err(db)?;
        let result = session(&owned(&mut tx, s, id).await?)?;
        tx.commit().await.map_err(db)?;
        Ok(result)
    }
    /// What an earlier input with this request key became, so a retry returns it without running hooks again.
    pub async fn prior(
        &self,
        s: &UserScope,
        id: &str,
        request_key: &str,
    ) -> StoreResult<Option<Prior>> {
        let mut conn = self.begin().await?;
        owned(&mut conn, s, id).await?;
        let row = sqlx::query("SELECT command_id,turn_id,fingerprint,reply,source FROM commands WHERE tenant_id=? AND agent_id=? AND session_id=? AND request_key=?")
            .bind(&s.tenant_id).bind(&s.agent_id).bind(id).bind(request_key).fetch_optional(&mut *conn).await.map_err(db)?;
        Ok(row.map(|row| {
            let reply = row.get::<Option<String>, _>(3).map(|text| Reply {
                text,
                source: row.get(4),
            });
            Prior {
                fingerprint: row.get(2),
                admission: Admission {
                    command_id: row.get(0),
                    turn_id: row.get(1),
                    duplicate: true,
                    placed: reply.is_some(),
                    reply,
                },
            }
        }))
    }
    pub async fn admit(&self, r: Submit) -> StoreResult<Admission> {
        if !key(&r.request_key)
            || !key(&r.turn_id)
            || r.input.is_empty()
            || r.input.len() > self.limits.max_input_bytes
        {
            return Err(StoreError::Invalid);
        }
        let _guard =
            self.write_guard(r.input.len() + r.reply.as_ref().map_or(0, |p| p.text.len()) + 4096)?;
        let mut conn = self.begin().await?;
        let mut tx = conn.begin_with("BEGIN IMMEDIATE").await.map_err(db)?;
        let mut out = Committed::new();
        let row = owned(&mut tx, &r.scope, &r.session_id).await?;
        let duplicate = sqlx::query("SELECT command_id,turn_id,fingerprint,reply,source FROM commands WHERE tenant_id=? AND agent_id=? AND session_id=? AND request_key=?")
            .bind(&r.scope.tenant_id).bind(&r.scope.agent_id).bind(&r.session_id).bind(&r.request_key).fetch_optional(&mut *tx).await.map_err(db)?;
        if let Some(row) = duplicate {
            if row.get::<String, _>(2) != r.fingerprint {
                return Err(StoreError::Conflict);
            }
            let reply = row.get::<Option<String>, _>(3).map(|text| Reply {
                text,
                source: row.get(4),
            });
            return Ok(Admission {
                command_id: row.get(0),
                turn_id: row.get(1),
                duplicate: true,
                placed: reply.is_some(),
                reply,
            });
        }
        if row.get::<String, _>("status") != "open" {
            return Err(StoreError::Conflict);
        }
        let tenant: i64 = sqlx::query_scalar(&format!(
            "SELECT COUNT(*) FROM commands WHERE tenant_id=? AND status IN {PENDING}"
        ))
        .bind(&r.scope.tenant_id)
        .fetch_one(&mut *tx)
        .await
        .map_err(db)?;
        let pending: i64 = sqlx::query_scalar(&format!("SELECT COUNT(*) FROM commands WHERE tenant_id=? AND agent_id=? AND session_id=? AND status IN {PENDING}"))
            .bind(&r.scope.tenant_id).bind(&r.scope.agent_id).bind(&r.session_id).fetch_one(&mut *tx).await.map_err(db)?;
        if r.when_busy.as_deref() == Some("reject") && pending > 0 {
            return Err(StoreError::Conflict);
        }
        if tenant as u64 >= self.limits.max_queued_per_tenant
            || pending as u64 >= self.limits.max_queued_per_session
        {
            return Err(StoreError::QuotaExceeded);
        }
        let command = uuid::Uuid::new_v4().to_string();
        // A plugin's reply to an idle session is written at once; a busy session gets it at its turn.
        let placed = r.reply.is_some() && pending == 0;
        let when_busy = match r.when_busy.as_deref() {
            Some("steer") => "steer",
            Some("reject") => "reject",
            _ => "queue",
        };
        sqlx::query("INSERT INTO commands(tenant_id,agent_id,session_id,command_id,turn_id,request_key,fingerprint,input,traceparent,reply,source,status,when_busy) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)")
            .bind(&r.scope.tenant_id).bind(&r.scope.agent_id).bind(&r.session_id).bind(&command).bind(&r.turn_id).bind(&r.request_key).bind(&r.fingerprint)
            .bind(&r.input).bind(&r.traceparent).bind(r.reply.as_ref().map(|p| &p.text)).bind(r.reply.as_ref().map(|p| &p.source))
            .bind(if placed { "completed" } else { "queued" })
            .bind(when_busy)
            .execute(&mut *tx).await.map_err(db)?;
        event(&mut tx, &mut out,&r.scope,&r.session_id,json!({"type":"submission","submissionId":command,"turnId":r.turn_id,"status":"accepted"})).await?;
        if let (true, Some(reply)) = (placed, &r.reply) {
            message(
                &mut tx,
                &r.scope,
                &r.session_id,
                &command,
                0,
                &user_message(&r.input, Some(reply)),
            )
            .await?;
            let answer = Message {
                source: Some(reply.source.clone()),
                ..Message::text("assistant", &reply.text)
            };
            message(&mut tx, &r.scope, &r.session_id, &command, 1, &answer).await?;
            reply_events(
                &mut tx,
                &mut out,
                &r.scope,
                &r.session_id,
                &command,
                &r.turn_id,
                reply,
            )
            .await?;
        }
        tx.commit().await.map_err(db)?;
        self.publish(out);
        Ok(Admission {
            command_id: command,
            turn_id: r.turn_id,
            duplicate: false,
            reply: r.reply,
            placed,
        })
    }
    /// Sets the cancel flag of the running command and gives up a parked one. Returns the running
    /// commands now flagged, for the caller to signal in-process.
    pub async fn cancel(
        &self,
        s: &UserScope,
        id: &str,
        key_value: &str,
    ) -> StoreResult<Vec<String>> {
        if !key(key_value) {
            return Err(StoreError::Invalid);
        }
        let _guard = self.write_guard(4096)?;
        let mut conn = self.begin().await?;
        let mut tx = conn.begin_with("BEGIN IMMEDIATE").await.map_err(db)?;
        let mut out = Committed::new();
        owned(&mut tx, s, id).await?;
        let inserted =
            sqlx::query("INSERT INTO cancel_keys VALUES(?,?,?,?) ON CONFLICT DO NOTHING")
                .bind(&s.tenant_id)
                .bind(&s.agent_id)
                .bind(id)
                .bind(key_value)
                .execute(&mut *tx)
                .await
                .map_err(db)?
                .rows_affected();
        let mut running = Vec::new();
        if inserted > 0 {
            running = sqlx::query_scalar("UPDATE commands SET cancel_requested=1 WHERE tenant_id=? AND agent_id=? AND session_id=? AND status='running' RETURNING command_id")
                .bind(&s.tenant_id).bind(&s.agent_id).bind(id).fetch_all(&mut *tx).await.map_err(db)?;
            let parked: Vec<(String, String)> = sqlx::query_as("UPDATE commands SET status='cancelled' WHERE tenant_id=? AND agent_id=? AND session_id=? AND status='parked' RETURNING command_id,turn_id")
                .bind(&s.tenant_id).bind(&s.agent_id).bind(id).fetch_all(&mut *tx).await.map_err(db)?;
            for (command, turn) in parked {
                event(&mut tx, &mut out,s,id,json!({"type":"message.end","turnId":turn,"messageId":command,"text":"","stopReason":"aborted","utf16Length":0})).await?;
                event(&mut tx, &mut out, s, id, json!({"type":"run.finished","turnId":turn,"runId":command,"status":"cancelled","usage":{}})).await?;
            }
        }
        tx.commit().await.map_err(db)?;
        self.publish(out);
        Ok(running)
    }
    /// Cancels all queued and parked commands for this session, returning their command IDs.
    pub async fn withdraw_pending(&self, s: &UserScope, id: &str) -> StoreResult<Vec<String>> {
        let _guard = self.write_guard(4096)?;
        let mut conn = self.begin().await?;
        let mut tx = conn.begin_with("BEGIN IMMEDIATE").await.map_err(db)?;
        let mut out = Committed::new();
        owned(&mut tx, s, id).await?;
        let queued: Vec<(String, String)> = sqlx::query_as(
            "UPDATE commands SET status='cancelled' WHERE tenant_id=? AND agent_id=? AND session_id=? AND status IN ('queued','parked') RETURNING command_id, turn_id"
        )
        .bind(&s.tenant_id).bind(&s.agent_id).bind(id).fetch_all(&mut *tx).await.map_err(db)?;
        for (command, turn) in &queued {
            event(&mut tx, &mut out, s, id, json!({"type":"message.end","turnId":turn,"messageId":command,"text":"","stopReason":"aborted","utf16Length":0})).await?;
            event(&mut tx, &mut out, s, id, json!({"type":"run.finished","turnId":turn,"runId":command,"status":"cancelled","usage":{}})).await?;
        }
        tx.commit().await.map_err(db)?;
        self.publish(out);
        Ok(queued.into_iter().map(|(c, _)| c).collect())
    }
    /// Unparks parked commands for this session, setting status back to queued.
    pub async fn retry(&self, s: &UserScope, id: &str) -> StoreResult<u64> {
        let _guard = self.write_guard(4096)?;
        let mut conn = self.begin().await?;
        let mut tx = conn.begin_with("BEGIN IMMEDIATE").await.map_err(db)?;
        owned(&mut tx, s, id).await?;
        let rows = sqlx::query(
            "UPDATE commands SET status='queued', stalls=0, error=NULL WHERE tenant_id=? AND agent_id=? AND session_id=? AND status='parked'"
        )
        .bind(&s.tenant_id).bind(&s.agent_id).bind(id).execute(&mut *tx).await.map_err(db)?.rows_affected();
        tx.commit().await.map_err(db)?;
        Ok(rows)
    }
    /// Finds any queued commands with when_busy='steer' for this session, marks them completed,
    /// writes their user message to the messages table, and returns them for the running turn.
    pub async fn claim_steered(&self, s: &UserScope, id: &str) -> StoreResult<Vec<Message>> {
        let _guard = self.write_guard(4096)?;
        let mut conn = self.begin().await?;
        let mut tx = conn.begin_with("BEGIN IMMEDIATE").await.map_err(db)?;
        let mut out = Committed::new();
        owned(&mut tx, s, id).await?;
        let rows: Vec<(String, String, String)> = sqlx::query_as(
            "SELECT command_id, turn_id, input FROM commands WHERE tenant_id=? AND agent_id=? AND session_id=? AND status='queued' AND when_busy='steer' ORDER BY ordinal ASC"
        )
        .bind(&s.tenant_id).bind(&s.agent_id).bind(id).fetch_all(&mut *tx).await.map_err(db)?;
        let mut messages = Vec::new();
        for (cmd_id, turn_id, input) in rows {
            sqlx::query("UPDATE commands SET status='completed' WHERE tenant_id=? AND agent_id=? AND session_id=? AND command_id=?")
                .bind(&s.tenant_id).bind(&s.agent_id).bind(id).bind(&cmd_id)
                .execute(&mut *tx).await.map_err(db)?;
            let msg = Message::text("user", &input);
            message(&mut tx, s, id, &cmd_id, 0, &msg).await?;
            event(&mut tx, &mut out, s, id, json!({"type":"run.finished","turnId":turn_id,"runId":cmd_id,"status":"completed","usage":{}})).await?;
            messages.push(msg);
        }
        tx.commit().await.map_err(db)?;
        self.publish(out);
        Ok(messages)
    }
    /// The model-visible transcript of the last eight commands, oldest first.
    /// What a model request starts from: the newest compaction's summary, then every message from
    /// its head on; without one, every message.
    pub async fn history(&self, w: &Work) -> StoreResult<Vec<Message>> {
        let (summary, messages) = self.compaction_source(&w.scope, &w.session_id).await?;
        let mut result = Vec::with_capacity(messages.len() + 1);
        if let Some(summary) = summary {
            result.push(Message::text("user", summary.message()));
        }
        result.extend(messages.into_iter().map(|(_, m)| m));
        Ok(result)
    }
    /// What a run that started from the summary at `head` sends: that summary, then every message
    /// from its head on; with no head, every message.
    pub async fn history_at(&self, w: &Work, head: Option<i64>) -> StoreResult<Vec<Message>> {
        let mut conn = self.read().await?;
        let mut result = Vec::new();
        if let Some(head) = head {
            let text: String = sqlx::query_scalar("SELECT summary FROM compactions WHERE tenant_id=? AND agent_id=? AND session_id=? AND head=?")
                .bind(&w.scope.tenant_id).bind(&w.scope.agent_id).bind(&w.session_id).bind(head).fetch_one(&mut *conn).await.map_err(db)?;
            result.push(Message::text("user", Summary { head, text }.message()));
        }
        let rows = sqlx::query("SELECT payload FROM messages WHERE tenant_id=? AND agent_id=? AND session_id=? AND sequence>=? ORDER BY sequence")
            .bind(&w.scope.tenant_id).bind(&w.scope.agent_id).bind(&w.session_id).bind(head.unwrap_or(0)).fetch_all(&mut *conn).await.map_err(db)?;
        let mut bytes = 0;
        for row in rows {
            let text: String = row.get(0);
            bytes += text.len();
            if bytes > MAX_HISTORY_BYTES {
                return Err(StoreError::Invalid);
            }
            result.push(decode(&text)?);
        }
        Ok(result)
    }
    /// The newest compaction, and the messages after its head with their sequences.
    pub async fn compaction_source(
        &self,
        s: &UserScope,
        session_id: &str,
    ) -> StoreResult<(Option<Summary>, Vec<(i64, Message)>)> {
        let mut conn = self.begin().await?;
        let summary = sqlx::query("SELECT head,summary FROM compactions WHERE tenant_id=? AND agent_id=? AND session_id=? ORDER BY head DESC LIMIT 1")
            .bind(&s.tenant_id).bind(&s.agent_id).bind(session_id).fetch_optional(&mut *conn).await.map_err(db)?
            .map(|r| Summary { head: r.get(0), text: r.get(1) });
        let head = summary.as_ref().map_or(0, |s| s.head);
        let rows = sqlx::query("SELECT sequence,payload FROM messages WHERE tenant_id=? AND agent_id=? AND session_id=? AND sequence>=? ORDER BY sequence")
            .bind(&s.tenant_id).bind(&s.agent_id).bind(session_id).bind(head).fetch_all(&mut *conn).await.map_err(db)?;
        let mut bytes = 0;
        let mut messages = Vec::with_capacity(rows.len());
        for row in rows {
            let text: String = row.get(1);
            bytes += text.len();
            // A safety net beside compaction: a session can't make a request this large.
            if bytes > MAX_HISTORY_BYTES {
                return Err(StoreError::Invalid);
            }
            messages.push((row.get(0), decode(&text)?));
        }
        Ok((summary, messages))
    }
    /// Places a summary of the messages before `head`, unless one already cuts as far: when
    /// several are made at once, the furthest cut stays in effect. Commits `end` with it.
    pub async fn place_compaction(
        &self,
        s: &UserScope,
        session_id: &str,
        head: i64,
        summary: &str,
        reason: &str,
        end: Value,
    ) -> StoreResult<bool> {
        let _guard = self.write_guard(summary.len() + 4096)?;
        let mut conn = self.begin().await?;
        let mut tx = conn.begin_with("BEGIN IMMEDIATE").await.map_err(db)?;
        let mut out = Committed::new();
        owned(&mut tx, s, session_id).await?;
        let furthest: Option<i64> = sqlx::query_scalar(
            "SELECT MAX(head) FROM compactions WHERE tenant_id=? AND agent_id=? AND session_id=?",
        )
        .bind(&s.tenant_id)
        .bind(&s.agent_id)
        .bind(session_id)
        .fetch_one(&mut *tx)
        .await
        .map_err(db)?;
        let placed = furthest.is_none_or(|f| head > f);
        if placed {
            sqlx::query("INSERT INTO compactions VALUES(?,?,?,?,?,?,?)")
                .bind(&s.tenant_id)
                .bind(&s.agent_id)
                .bind(session_id)
                .bind(head)
                .bind(summary)
                .bind(reason)
                .bind(now_ms())
                .execute(&mut *tx)
                .await
                .map_err(db)?;
        }
        event(&mut tx, &mut out, s, session_id, end).await?;
        tx.commit().await.map_err(db)?;
        self.publish(out);
        Ok(placed)
    }
    /// Commits one event of a session that no command owns, such as a compaction starting.
    pub async fn session_event(
        &self,
        s: &UserScope,
        session_id: &str,
        payload: Value,
    ) -> StoreResult<()> {
        let _guard = self.write_guard(4096)?;
        let mut conn = self.begin().await?;
        let mut tx = conn.begin_with("BEGIN IMMEDIATE").await.map_err(db)?;
        let mut out = Committed::new();
        owned(&mut tx, s, session_id).await?;
        event(&mut tx, &mut out, s, session_id, payload).await?;
        tx.commit().await.map_err(db)?;
        self.publish(out);
        Ok(())
    }
    /// The trace of the skill flow a session last ran.
    pub async fn flow(&self, s: &UserScope, session_id: &str) -> StoreResult<Option<Value>> {
        let mut conn = self.read().await?;
        owned(&mut conn, s, session_id).await?;
        let row: Option<String> = sqlx::query_scalar(
            "SELECT flow FROM sessions WHERE tenant_id=? AND agent_id=? AND session_id=?",
        )
        .bind(&s.tenant_id)
        .bind(&s.agent_id)
        .bind(session_id)
        .fetch_one(&mut *conn)
        .await
        .map_err(db)?;
        row.map(|text| decode(&text)).transpose()
    }
    /// Replaces a session's flow trace and commits its `flow.updated`.
    pub async fn set_flow(&self, s: &UserScope, session_id: &str, trace: Value) -> StoreResult<()> {
        let encoded = encode(&trace)?;
        let _guard = self.write_guard(2 * encoded.len() + 4096)?;
        let mut conn = self.begin().await?;
        let mut tx = conn.begin_with("BEGIN IMMEDIATE").await.map_err(db)?;
        let mut out = Committed::new();
        owned(&mut tx, s, session_id).await?;
        write_flow(&mut tx, &mut out, s, session_id, trace).await?;
        tx.commit().await.map_err(db)?;
        self.publish(out);
        Ok(())
    }
    /// Changes a session's flow trace if it is still running. Parallel tool calls each do this, so
    /// reading and writing it are one transaction.
    pub async fn advance_flow(
        &self,
        s: &UserScope,
        session_id: &str,
        change: impl FnOnce(Value) -> Option<Value> + Send,
    ) -> StoreResult<()> {
        let mut conn = self.begin().await?;
        let running: Option<String> = sqlx::query_scalar(
            "SELECT flow FROM sessions WHERE tenant_id=? AND agent_id=? AND session_id=? AND json_extract(flow,'$.status')='running'",
        )
        .bind(&s.tenant_id)
        .bind(&s.agent_id)
        .bind(session_id)
        .fetch_optional(&mut *conn)
        .await
        .map_err(db)?;
        // Most tool calls have no flow to move, and they don't need the write lock to find out.
        if running.is_none() {
            return Ok(());
        }
        let _guard = self.write_guard(running.as_ref().map_or(0, |t| 2 * t.len()) + 4096)?;
        let mut tx = conn.begin_with("BEGIN IMMEDIATE").await.map_err(db)?;
        let mut out = Committed::new();
        owned(&mut tx, s, session_id).await?;
        let current: Option<String> = sqlx::query_scalar(
            "SELECT flow FROM sessions WHERE tenant_id=? AND agent_id=? AND session_id=? AND json_extract(flow,'$.status')='running'",
        )
        .bind(&s.tenant_id)
        .bind(&s.agent_id)
        .bind(session_id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(db)?;
        let Some(current) = current else {
            return Ok(());
        };
        if let Some(trace) = change(decode(&current)?) {
            write_flow(&mut tx, &mut out, s, session_id, trace).await?;
            tx.commit().await.map_err(db)?;
            self.publish(out);
        }
        Ok(())
    }
    /// At startup: commands a crash cut off run again, each counting one restart without progress.
    pub async fn recover(&self) -> StoreResult<u64> {
        // Exclusive file ownership ensures no old process can commit after this.
        let n = sqlx::query(
            "UPDATE commands SET status='queued',stalls=stalls+1 WHERE status='running'",
        )
        .execute(&self.pool)
        .await
        .map_err(db)?;
        Ok(n.rows_affected())
    }
    pub async fn runnable(&self, limit: u32) -> StoreResult<Vec<Candidate>> {
        let rows = sqlx::query(&format!("SELECT c.tenant_id,s.owner_user_id,c.command_id,c.ordinal,s.parent IS NOT NULL,json_extract(s.child,'$.task') IS NOT NULL FROM commands c JOIN sessions s USING(tenant_id,agent_id,session_id) WHERE c.status='queued' AND NOT EXISTS(SELECT 1 FROM commands prior WHERE prior.tenant_id=c.tenant_id AND prior.agent_id=c.agent_id AND prior.session_id=c.session_id AND prior.ordinal<c.ordinal AND prior.status IN {PENDING}) ORDER BY c.ordinal LIMIT ?"))
            .bind(limit).fetch_all(&self.readers).await.map_err(db)?;
        Ok(rows
            .iter()
            .map(|r| Candidate {
                tenant_id: r.get(0),
                user_id: r.get(1),
                command_id: r.get(2),
                ordinal: r.get(3),
                child: r.get(4),
                task: r.get(5),
            })
            .collect())
    }
    /// Marks a queued command running. A command restarted `restart_limit` times in a row without a
    /// new step is parked instead, and `None` returned.
    pub async fn claim(&self, command_id: &str) -> StoreResult<Option<Work>> {
        let _guard = self.write_guard(4096)?;
        let mut conn = self.begin().await?;
        let mut tx = conn.begin_with("BEGIN IMMEDIATE").await.map_err(db)?;
        let mut out = Committed::new();
        let row = sqlx::query("SELECT c.*,s.owner_user_id,s.profile,s.child FROM commands c JOIN sessions s USING(tenant_id,agent_id,session_id) WHERE c.command_id=? AND c.status='queued'")
            .bind(command_id)
            .fetch_optional(&mut *tx)
            .await
            .map_err(db)?;
        let Some(row) = row else { return Ok(None) };
        let w = Work {
            scope: UserScope {
                tenant_id: row.get("tenant_id"),
                agent_id: row.get("agent_id"),
                user_id: row.get("owner_user_id"),
            },
            session_id: row.get("session_id"),
            command_id: row.get("command_id"),
            turn_id: row.get("turn_id"),
            input: row.get("input"),
            profile: row.get("profile"),
            traceparent: row.get("traceparent"),
            attempt: row.get::<i64, _>("attempt") + 1,
            reply: row.get::<Option<String>, _>("reply").map(|text| Reply {
                text,
                source: row.get("source"),
            }),
            cancel_requested: row.get("cancel_requested"),
            child: match row.get::<Option<String>, _>("child") {
                Some(spec) => Some(decode(&spec)?),
                None => None,
            },
        };
        if row.get::<i64, _>("stalls") >= i64::from(self.limits.restart_limit)
            && !w.cancel_requested
        {
            let error = format!(
                "parked after {} restarts without progress",
                self.limits.restart_limit
            );
            sqlx::query("UPDATE commands SET status='parked',error=? WHERE command_id=?")
                .bind(&error)
                .bind(command_id)
                .execute(&mut *tx)
                .await
                .map_err(db)?;
            event(
                &mut tx,
                &mut out,
                &w.scope,
                &w.session_id,
                json!({"type":"error","turnId":w.turn_id,"code":"parked","message":error}),
            )
            .await?;
            tx.commit().await.map_err(db)?;
            self.publish(out);
            return Ok(None);
        }
        sqlx::query("UPDATE commands SET status='running',attempt=attempt+1 WHERE command_id=?")
            .bind(command_id)
            .execute(&mut *tx)
            .await
            .map_err(db)?;
        message(
            &mut tx,
            &w.scope,
            &w.session_id,
            &w.command_id,
            0,
            &user_message(&w.input, w.reply.as_ref()),
        )
        .await?;
        if w.reply.is_none() {
            event(&mut tx, &mut out,&w.scope,&w.session_id,json!({"type":"run.started","turnId":w.turn_id,"runId":w.command_id,"generationId":format!("{}:{}",w.command_id,w.attempt)})).await?;
        }
        tx.commit().await.map_err(db)?;
        self.publish(out);
        Ok(Some(w))
    }
    async fn current(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        w: &Work,
        uncancelled: bool,
    ) -> StoreResult<()> {
        let active: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM commands WHERE tenant_id=? AND agent_id=? AND session_id=? AND command_id=? AND status='running' AND attempt=? AND (cancel_requested=0 OR NOT ?)")
            .bind(&w.scope.tenant_id).bind(&w.scope.agent_id).bind(&w.session_id).bind(&w.command_id).bind(w.attempt).bind(uncancelled).fetch_one(&mut **tx).await.map_err(db)?;
        if active == 0 {
            Err(StoreError::Conflict)
        } else {
            Ok(())
        }
    }
    pub async fn prepare(&self, w: &Work, op: Operation) -> StoreResult<Prepared> {
        let input_digest = digest(&op.input);
        let _guard = self.write_guard(encode(&op.input)?.len() + 4096)?;
        let mut conn = self.begin().await?;
        let mut tx = conn.begin_with("BEGIN IMMEDIATE").await.map_err(db)?;
        let mut out = Committed::new();
        self.current(&mut tx, w, true).await?;
        let row=sqlx::query("SELECT input_digest,unsafe_effect,result FROM steps WHERE tenant_id=? AND agent_id=? AND session_id=? AND command_id=? AND operation_id=?")
            .bind(&w.scope.tenant_id).bind(&w.scope.agent_id).bind(&w.session_id).bind(&w.command_id).bind(&op.id).fetch_optional(&mut *tx).await.map_err(db)?;
        if let Some(row) = row {
            if row.get::<String, _>(0) != input_digest {
                return Err(StoreError::Conflict);
            }
            if let Some(result) = row.get::<Option<String>, _>(2) {
                return Ok(Prepared::Recorded(decode(&result)?));
            }
            return Ok(if row.get::<bool, _>(1) {
                Prepared::Interrupted
            } else {
                Prepared::Run
            });
        }
        sqlx::query("INSERT INTO steps(tenant_id,agent_id,session_id,command_id,operation_id,kind,input_digest,unsafe_effect) VALUES(?,?,?,?,?,?,?,?)")
            .bind(&w.scope.tenant_id).bind(&w.scope.agent_id).bind(&w.session_id).bind(&w.command_id).bind(&op.id).bind(&op.kind)
            .bind(input_digest).bind(op.unsafe_effect).execute(&mut *tx).await.map_err(db)?;
        if let Some(payload) = op.event {
            event(&mut tx, &mut out, &w.scope, &w.session_id, payload).await?;
        }
        tx.commit().await.map_err(db)?;
        self.publish(out);
        Ok(Prepared::Run)
    }
    /// Records a step's result, and counts as progress: the command's restart count starts again.
    pub async fn record(
        &self,
        w: &Work,
        id: &str,
        result: Value,
        payload: Option<Value>,
    ) -> StoreResult<()> {
        self.record_spent(w, id, result, payload, (0, 0)).await
    }
    /// [`record`](Self::record) for a model call, with what it spent. The same transaction adds it
    /// to the session, so a run that fails or is cancelled later keeps its spend, and a step reused
    /// after a crash, which is never recorded again, can't count twice.
    pub async fn record_spent(
        &self,
        w: &Work,
        id: &str,
        result: Value,
        payload: Option<Value>,
        (input, output): (u64, u64),
    ) -> StoreResult<()> {
        let encoded = encode(&result)?;
        if encoded.len() > 512 * 1024 {
            return Err(StoreError::Invalid);
        }
        let _guard = self.write_guard(encoded.len() + 4096)?;
        let mut conn = self.begin().await?;
        let mut tx = conn.begin_with("BEGIN IMMEDIATE").await.map_err(db)?;
        let mut out = Committed::new();
        self.current(&mut tx, w, false).await?;
        sqlx::query("UPDATE steps SET result=? WHERE tenant_id=? AND agent_id=? AND session_id=? AND command_id=? AND operation_id=? AND result IS NULL")
            .bind(encoded).bind(&w.scope.tenant_id).bind(&w.scope.agent_id).bind(&w.session_id).bind(&w.command_id).bind(id)
            .execute(&mut *tx).await.map_err(db)?.rows_affected().eq(&1).then_some(()).ok_or(StoreError::Conflict)?;
        sqlx::query("UPDATE commands SET stalls=0 WHERE command_id=?")
            .bind(&w.command_id)
            .execute(&mut *tx)
            .await
            .map_err(db)?;
        if input + output > 0 {
            sqlx::query("UPDATE sessions SET input_tokens=input_tokens+?,output_tokens=output_tokens+? WHERE tenant_id=? AND agent_id=? AND session_id=?")
                .bind(input as i64)
                .bind(output as i64)
                .bind(&w.scope.tenant_id)
                .bind(&w.scope.agent_id)
                .bind(&w.session_id)
                .execute(&mut *tx)
                .await
                .map_err(db)?;
        }
        if let Some(payload) = payload {
            event(&mut tx, &mut out, &w.scope, &w.session_id, payload).await?;
        }
        tx.commit().await.map_err(db)?;
        self.publish(out);
        Ok(())
    }
    pub async fn finish(&self, w: &Work, mut completion: Completion) -> StoreResult<()> {
        let _guard = self.write_guard(encode(&completion.messages)?.len() + 4096)?;
        let mut conn = self.begin().await?;
        let mut tx = conn.begin_with("BEGIN IMMEDIATE").await.map_err(db)?;
        let mut out = Committed::new();
        let row=sqlx::query("SELECT status,attempt,cancel_requested FROM commands WHERE tenant_id=? AND agent_id=? AND session_id=? AND command_id=?")
            .bind(&w.scope.tenant_id).bind(&w.scope.agent_id).bind(&w.session_id).bind(&w.command_id).fetch_one(&mut *tx).await.map_err(db)?;
        if row.get::<String, _>(0) != "running" || row.get::<i64, _>(1) != w.attempt {
            return Err(StoreError::Conflict);
        }
        if row.get::<bool, _>(2) && completion.status != "cancelled" {
            completion.status = "cancelled".into();
            completion.messages.clear();
            completion.error = None;
        }
        if !["completed", "cancelled", "failed"].contains(&completion.status.as_str()) {
            return Err(StoreError::Invalid);
        }
        for (i, m) in completion.messages.iter().enumerate() {
            message(
                &mut tx,
                &w.scope,
                &w.session_id,
                &w.command_id,
                i as i64 + 1,
                m,
            )
            .await?;
        }
        match (&w.reply, completion.status.as_str()) {
            (Some(reply), "completed") => {
                reply_events(
                    &mut tx,
                    &mut out,
                    &w.scope,
                    &w.session_id,
                    &w.command_id,
                    &w.turn_id,
                    reply,
                )
                .await?
            }
            _ => {
                // The text went out live as deltas; the committed `message.end` carries all of it.
                let cancelled = completion.status == "cancelled";
                let text = match (cancelled, &completion.partial) {
                    (true, Some(partial)) => partial.as_str(),
                    _ => completion
                        .messages
                        .last()
                        .filter(|m| m.role == "assistant")
                        .map(|m| m.content.as_str())
                        .unwrap_or(""),
                };
                let message_id = completion
                    .message_id
                    .clone()
                    .unwrap_or_else(|| w.command_id.clone());
                let stop = if cancelled { "aborted" } else { "stop" };
                let end = json!({"type":"message.end","turnId":w.turn_id,"messageId":message_id,"text":text,"stopReason":stop,"utf16Length":text.encode_utf16().count()});
                event(&mut tx, &mut out, &w.scope, &w.session_id, end).await?;
                if let Some(error) = &completion.error {
                    event(
                        &mut tx,
                        &mut out,
                        &w.scope,
                        &w.session_id,
                        json!({"type":"error","turnId":w.turn_id,"code":error,"message":error}),
                    )
                    .await?;
                }
                event(&mut tx, &mut out,&w.scope,&w.session_id,json!({"type":"run.finished","turnId":w.turn_id,"runId":w.command_id,"status":completion.status,"usage":{"inputTokens":completion.input_tokens,"outputTokens":completion.output_tokens}})).await?;
            }
        }
        sqlx::query("UPDATE commands SET status=?,error=? WHERE tenant_id=? AND agent_id=? AND session_id=? AND command_id=? AND attempt=?")
            .bind(&completion.status).bind(&completion.error).bind(&w.scope.tenant_id).bind(&w.scope.agent_id).bind(&w.session_id).bind(&w.command_id).bind(w.attempt)
            .execute(&mut *tx).await.map_err(db)?;
        tx.commit().await.map_err(db)?;
        self.publish(out);
        Ok(())
    }

    pub async fn plugin_state_get(
        &self,
        s: &UserScope,
        session_id: &str,
        plugin: &str,
    ) -> StoreResult<Option<Value>> {
        let mut conn = self.read().await?;
        owned(&mut conn, s, session_id).await?;
        let row = sqlx::query("SELECT state FROM plugin_state WHERE tenant_id=? AND agent_id=? AND session_id=? AND plugin=?")
            .bind(&s.tenant_id).bind(&s.agent_id).bind(session_id).bind(plugin)
            .fetch_optional(&mut *conn).await.map_err(db)?;
        row.map(|r| decode(r.get(0))).transpose()
    }

    pub async fn plugin_state_set(
        &self,
        s: &UserScope,
        session_id: &str,
        plugin: &str,
        state: &Value,
    ) -> StoreResult<()> {
        let _guard = self.write_guard(4096)?;
        let mut conn = self.begin().await?;
        let mut tx = conn.begin_with("BEGIN IMMEDIATE").await.map_err(db)?;
        owned(&mut tx, s, session_id).await?;
        let encoded = encode(state)?;
        sqlx::query("INSERT INTO plugin_state(tenant_id,agent_id,session_id,plugin,state) VALUES(?,?,?,?,?) ON CONFLICT(tenant_id,agent_id,session_id,plugin) DO UPDATE SET state=excluded.state")
            .bind(&s.tenant_id).bind(&s.agent_id).bind(session_id).bind(plugin).bind(encoded)
            .execute(&mut *tx).await.map_err(db)?;
        tx.commit().await.map_err(db)?;
        Ok(())
    }

    pub async fn history_say(
        &self,
        s: &UserScope,
        session_id: &str,
        plugin: &str,
        text: &str,
    ) -> StoreResult<()> {
        let _guard = self.write_guard(text.len() + 4096)?;
        let mut conn = self.begin().await?;
        let mut tx = conn.begin_with("BEGIN IMMEDIATE").await.map_err(db)?;
        let mut out = Committed::new();
        owned(&mut tx, s, session_id).await?;
        let command_id = format!("say_{}", uuid::Uuid::new_v4().simple());
        let turn_id = command_id.clone();
        let answer = Message {
            source: Some(plugin.to_string()),
            ..Message::text("assistant", text)
        };
        message(&mut tx, s, session_id, &command_id, 0, &answer).await?;
        event(
            &mut tx,
            &mut out,
            s,
            session_id,
            json!({
                "type": "text.delta",
                "turnId": turn_id,
                "messageId": command_id,
                "offset": 0,
                "text": text,
            }),
        )
        .await?;
        event(
            &mut tx,
            &mut out,
            s,
            session_id,
            json!({
                "type": "message.end",
                "turnId": turn_id,
                "messageId": command_id,
                "text": text,
                "stopReason": "stop",
                "source": plugin,
                "utf16Length": text.encode_utf16().count(),
            }),
        )
        .await?;
        tx.commit().await.map_err(db)?;
        self.publish(out);
        Ok(())
    }

    pub async fn history_record(
        &self,
        s: &UserScope,
        session_id: &str,
        plugin: &str,
        record_type: &str,
        data: &Value,
    ) -> StoreResult<()> {
        let payload_str = encode(data)?;
        let _guard = self.write_guard(payload_str.len() + 4096)?;
        let mut conn = self.begin().await?;
        let mut tx = conn.begin_with("BEGIN IMMEDIATE").await.map_err(db)?;
        let mut out = Committed::new();
        owned(&mut tx, s, session_id).await?;
        let command_id = format!("rec_{}", uuid::Uuid::new_v4().simple());
        let rec = Message::record(
            plugin,
            json!({"type": record_type, "data": data}).to_string(),
        );
        message(&mut tx, s, session_id, &command_id, 0, &rec).await?;
        event(
            &mut tx,
            &mut out,
            s,
            session_id,
            json!({
                "type": "record",
                "recordType": record_type,
                "data": data,
                "source": plugin,
            }),
        )
        .await?;
        tx.commit().await.map_err(db)?;
        self.publish(out);
        Ok(())
    }

    pub async fn history_exchange(
        &self,
        s: &UserScope,
        session_id: &str,
        plugin: &str,
        input: &str,
        answer: &str,
    ) -> StoreResult<()> {
        let _guard = self.write_guard(input.len() + answer.len() + 4096)?;
        let mut conn = self.begin().await?;
        let mut tx = conn.begin_with("BEGIN IMMEDIATE").await.map_err(db)?;
        let mut out = Committed::new();
        owned(&mut tx, s, session_id).await?;
        let command_id = format!("ex_{}", uuid::Uuid::new_v4().simple());
        let turn_id = command_id.clone();
        let user_msg = Message {
            source: Some(plugin.to_string()),
            ..Message::text("user", input)
        };
        message(&mut tx, s, session_id, &command_id, 0, &user_msg).await?;
        let ans_msg = Message {
            source: Some(plugin.to_string()),
            ..Message::text("assistant", answer)
        };
        message(&mut tx, s, session_id, &command_id, 1, &ans_msg).await?;
        event(
            &mut tx,
            &mut out,
            s,
            session_id,
            json!({
                "type": "text.delta",
                "turnId": turn_id,
                "messageId": command_id,
                "offset": 0,
                "text": answer,
            }),
        )
        .await?;
        event(
            &mut tx,
            &mut out,
            s,
            session_id,
            json!({
                "type": "message.end",
                "turnId": turn_id,
                "messageId": command_id,
                "text": answer,
                "stopReason": "stop",
                "source": plugin,
                "utf16Length": answer.encode_utf16().count(),
            }),
        )
        .await?;
        tx.commit().await.map_err(db)?;
        self.publish(out);
        Ok(())
    }

    pub async fn get_session_tools(
        &self,
        s: &UserScope,
        session_id: &str,
    ) -> StoreResult<SessionTools> {
        let mut conn = self.read().await?;
        owned(&mut conn, s, session_id).await?;
        let row = sqlx::query(
            "SELECT offered, dynamic, skills FROM session_tools WHERE tenant_id=? AND agent_id=? AND session_id=?",
        )
        .bind(&s.tenant_id)
        .bind(&s.agent_id)
        .bind(session_id)
        .fetch_optional(&mut *conn)
        .await
        .map_err(db)?;
        match row {
            Some(r) => {
                let offered: Vec<String> = decode(r.get(0))?;
                let dynamic: Vec<String> = decode(r.get(1))?;
                let skills: BTreeMap<String, Vec<String>> = decode(r.get(2))?;
                Ok(SessionTools {
                    offered,
                    dynamic,
                    skills,
                })
            }
            None => Ok(SessionTools::default()),
        }
    }

    pub async fn set_grant(
        &self,
        s: &UserScope,
        session_id: &str,
        grant: &StoredGrant,
    ) -> StoreResult<()> {
        let _guard = self.write_guard(4096)?;
        let mut conn = self.begin().await?;
        owned(&mut conn, s, session_id).await?;
        sqlx::query(
            "UPDATE sessions SET grant_id=?, grant_parent=? WHERE tenant_id=? AND agent_id=? AND session_id=?",
        )
        .bind(&grant.grant_id)
        .bind(&grant.parent)
        .bind(&s.tenant_id)
        .bind(&s.agent_id)
        .bind(session_id)
        .execute(&mut *conn)
        .await
        .map_err(db)?;
        Ok(())
    }

    pub async fn get_grant(
        &self,
        s: &UserScope,
        session_id: &str,
    ) -> StoreResult<Option<StoredGrant>> {
        let mut conn = self.read().await?;
        owned(&mut conn, s, session_id).await?;
        let row = sqlx::query(
            "SELECT grant_id, grant_parent FROM sessions WHERE tenant_id=? AND agent_id=? AND session_id=?",
        )
        .bind(&s.tenant_id)
        .bind(&s.agent_id)
        .bind(session_id)
        .fetch_optional(&mut *conn)
        .await
        .map_err(db)?;
        match row {
            Some(r) => {
                let grant_id: Option<String> = r.get(0);
                let parent: Option<String> = r.get(1);
                Ok(grant_id.map(|gid| StoredGrant {
                    grant_id: gid,
                    parent,
                }))
            }
            None => Ok(None),
        }
    }

    pub async fn find_grant(&self, session_id: &str) -> StoreResult<Option<StoredGrant>> {
        let mut conn = self.read().await?;
        let row = sqlx::query("SELECT grant_id, grant_parent FROM sessions WHERE session_id=?")
            .bind(session_id)
            .fetch_optional(&mut *conn)
            .await
            .map_err(db)?;
        match row {
            Some(r) => {
                let grant_id: Option<String> = r.get(0);
                let parent: Option<String> = r.get(1);
                Ok(grant_id.map(|gid| StoredGrant {
                    grant_id: gid,
                    parent,
                }))
            }
            None => Ok(None),
        }
    }

    pub async fn activate_tools(
        &self,
        s: &UserScope,
        session_id: &str,
        names: &[String],
        policy: &ActivationPolicy,
    ) -> StoreResult<ActivationResult> {
        let _guard = self.write_guard(4096)?;
        let mut conn = self.begin().await?;
        let mut tx = conn.begin_with("BEGIN IMMEDIATE").await.map_err(db)?;
        owned(&mut tx, s, session_id).await?;
        let mut out = Vec::new();
        let plan = ToolActivationPlan {
            names,
            policy,
            record_skill: None,
            updated_skills: None,
        };
        let result = tx_activate_tools(&mut tx, &mut out, s, session_id, plan).await?;
        tx.commit().await.map_err(db)?;
        self.publish(out);
        Ok(result)
    }

    pub async fn activate_skill(
        &self,
        s: &UserScope,
        session_id: &str,
        skill: &str,
        names: &[String],
        policy: &ActivationPolicy,
    ) -> StoreResult<ActivationResult> {
        let _guard = self.write_guard(4096)?;
        let mut conn = self.begin().await?;
        let mut tx = conn.begin_with("BEGIN IMMEDIATE").await.map_err(db)?;
        owned(&mut tx, s, session_id).await?;
        let mut out = Vec::new();
        let plan = ToolActivationPlan {
            names,
            policy,
            record_skill: Some((skill, names)),
            updated_skills: None,
        };
        let result = tx_activate_tools(&mut tx, &mut out, s, session_id, plan).await?;
        tx.commit().await.map_err(db)?;
        self.publish(out);
        Ok(result)
    }

    pub async fn refresh_skills<F>(
        &self,
        s: &UserScope,
        session_id: &str,
        matches: F,
        policy: &ActivationPolicy,
    ) -> StoreResult<ActivationResult>
    where
        F: Fn(&str) -> Option<Vec<String>>,
    {
        let _guard = self.write_guard(4096)?;
        let mut conn = self.begin().await?;
        let mut tx = conn.begin_with("BEGIN IMMEDIATE").await.map_err(db)?;
        owned(&mut tx, s, session_id).await?;
        let row = sqlx::query(
            "SELECT offered, dynamic, skills FROM session_tools WHERE tenant_id=? AND agent_id=? AND session_id=?",
        )
        .bind(&s.tenant_id)
        .bind(&s.agent_id)
        .bind(session_id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(db)?;

        let mut skills: BTreeMap<String, Vec<String>> = match row {
            Some(ref r) => decode(r.get(2))?,
            None => BTreeMap::new(),
        };

        let mut fresh = Vec::new();
        for (skill, known) in skills.iter_mut() {
            if let Some(matched) = matches(skill) {
                let added: Vec<String> = matched
                    .into_iter()
                    .filter(|name| !known.contains(name))
                    .collect();
                if !added.is_empty() {
                    known.extend(added.clone());
                    fresh.extend(added);
                }
            }
        }

        if fresh.is_empty() {
            return Ok(ActivationResult::default());
        }

        let mut out = Vec::new();
        let plan = ToolActivationPlan {
            names: &fresh,
            policy,
            record_skill: None,
            updated_skills: Some(skills),
        };
        let result = tx_activate_tools(&mut tx, &mut out, s, session_id, plan).await?;
        tx.commit().await.map_err(db)?;
        self.publish(out);
        Ok(result)
    }
}

struct ToolActivationPlan<'a> {
    names: &'a [String],
    policy: &'a ActivationPolicy,
    record_skill: Option<(&'a str, &'a [String])>,
    updated_skills: Option<BTreeMap<String, Vec<String>>>,
}

async fn tx_activate_tools(
    tx: &mut Transaction<'_, Sqlite>,
    out: &mut Committed,
    s: &UserScope,
    session_id: &str,
    plan: ToolActivationPlan<'_>,
) -> StoreResult<ActivationResult> {
    let names = plan.names;
    let policy = plan.policy;
    let record_skill = plan.record_skill;
    let updated_skills = plan.updated_skills;
    let row = sqlx::query(
        "SELECT offered, dynamic, skills FROM session_tools WHERE tenant_id=? AND agent_id=? AND session_id=?",
    )
    .bind(&s.tenant_id)
    .bind(&s.agent_id)
    .bind(session_id)
    .fetch_optional(&mut **tx)
    .await
    .map_err(db)?;

    let (mut offered, mut dynamic, mut skills) = match row {
        Some(r) => {
            let off: Vec<String> = decode(r.get(0))?;
            let dyn_tools: Vec<String> = decode(r.get(1))?;
            let sk: BTreeMap<String, Vec<String>> = match updated_skills {
                Some(s) => s,
                None => decode(r.get(2))?,
            };
            (off, dyn_tools, sk)
        }
        None => (
            policy.pinned.clone(),
            Vec::new(),
            updated_skills.unwrap_or_default(),
        ),
    };

    if let Some((skill_name, skill_tools)) = record_skill {
        skills.insert(skill_name.to_string(), skill_tools.to_vec());
    }

    let pinned_set: HashSet<&str> = policy.pinned.iter().map(String::as_str).collect();
    let requested_set: HashSet<&str> = names.iter().map(String::as_str).collect();
    let mut added = Vec::new();

    for name in names {
        if !offered.contains(name) {
            offered.push(name.clone());
            added.push(name.clone());
        }
        if pinned_set.contains(name.as_str()) {
            continue;
        }
        if let Some(pos) = dynamic.iter().position(|x| x == name) {
            dynamic.remove(pos);
        }
        dynamic.push(name.clone());
    }

    let mut evicted = Vec::new();
    let room = policy.max_active.saturating_sub(policy.pinned.len());
    let mut index = 0;
    while dynamic.len() > room && index < dynamic.len() {
        let name = &dynamic[index];
        if requested_set.contains(name.as_str()) {
            index += 1;
            continue;
        }
        let removed = dynamic.remove(index);
        if let Some(pos) = offered.iter().position(|x| x == &removed) {
            offered.remove(pos);
        }
        evicted.push(removed);
    }

    if !evicted.is_empty() {
        skills
            .retain(|_, known| known.is_empty() || known.iter().any(|name| offered.contains(name)));
    }

    let offered_json = encode(&offered)?;
    let dynamic_json = encode(&dynamic)?;
    let skills_json = encode(&skills)?;

    sqlx::query(
        "INSERT INTO session_tools(tenant_id,agent_id,session_id,offered,dynamic,skills) VALUES(?,?,?,?,?,?) \
         ON CONFLICT(tenant_id,agent_id,session_id) DO UPDATE SET offered=excluded.offered, dynamic=excluded.dynamic, skills=excluded.skills"
    )
    .bind(&s.tenant_id)
    .bind(&s.agent_id)
    .bind(session_id)
    .bind(offered_json)
    .bind(dynamic_json)
    .bind(skills_json)
    .execute(&mut **tx)
    .await
    .map_err(db)?;

    if !added.is_empty() || !evicted.is_empty() {
        event(
            tx,
            out,
            s,
            session_id,
            json!({ "type": "tools.active", "names": offered }),
        )
        .await?;
    }

    Ok(ActivationResult { added, evicted })
}
