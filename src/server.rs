use crate::{
    agents::{self, Agent},
    auth::{Auth, Principal, now},
    events::{EventId, Subscription},
    worker::{self, Scheduler},
};
use agent_ti_config::HostConfig;
use agent_ti_plugin_sdk::{PromptDecision, Turn};
use agent_ti_store::*;
use axum::{
    Json, Router,
    body::Bytes,
    extract::{DefaultBodyLimit, Path, State},
    http::{HeaderMap, HeaderValue, StatusCode},
    response::{
        IntoResponse, Response,
        sse::{Event, KeepAlive, Sse},
    },
    routing::{get, post},
};
use serde::Deserialize;
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    convert::Infallible,
    error::Error,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use tokio::sync::Semaphore;

pub struct Host {
    pub config: HostConfig,
    pub agents: BTreeMap<String, Arc<Agent>>,
    pub default_agent: String,
    pub client: reqwest::Client,
    pub auth: Auth,
    pub accepting: AtomicBool,
    pub scheduler: Scheduler,
    pub metrics: crate::metrics::Metrics,
    streams: Arc<Semaphore>,
    requests: Arc<Semaphore>,
}
/// `GET /metrics`, unauthenticated as the probes are. The gateway forwards only agent paths, and
/// the workload's NetworkPolicy decides who reaches this port.
async fn metrics(State(host): State<Arc<Host>>) -> Response {
    let running = host.scheduler.active() as f64;
    let mut gauges = vec![
        ("agent_ti_runs_active", String::new(), running),
        (
            "agent_ti_streams_open",
            String::new(),
            (host.config.limits.max_streams - host.streams.available_permits()) as f64,
        ),
    ];
    for agent in host.agents.values() {
        let path = host
            .config
            .storage_dir
            .join(format!("{}.sqlite", agent.name));
        let bytes = std::fs::metadata(path).map_or(0, |m| m.len());
        gauges.push((
            "agent_ti_store_bytes",
            crate::metrics::gauge_labels(&[("agent", &agent.name)]),
            bytes as f64,
        ));
    }
    (
        [("content-type", "text/plain; version=0.0.4")],
        host.metrics.render(&gauges),
    )
        .into_response()
}
/// agent-pi's JSON-RPC error code for a status (`errors.ts`).
fn rpc_code(status: StatusCode) -> i32 {
    match status.as_u16() {
        400 => -32602,
        401 => -32001,
        403 => -32003,
        404 => -32004,
        409 => -32009,
        421 => -32021,
        422 => -32022,
        429 => -32029,
        _ => -32603,
    }
}
fn fail(status: StatusCode, message: &str) -> Response {
    let mut response = (
        status,
        Json(
            json!({"jsonrpc":"2.0","id":null,"error":{"code":rpc_code(status),"message":message}}),
        ),
    )
        .into_response();
    if status == StatusCode::TOO_MANY_REQUESTS || status == StatusCode::SERVICE_UNAVAILABLE {
        response
            .headers_mut()
            .insert("retry-after", HeaderValue::from_static("1"));
    }
    response
}
/// Refuses a session ID this replica doesn't own, before anything is read for it. A public ID is
/// `<agent>.<replica>.<n>`: another replica's answers 421 naming that replica, and anything else
/// reads as not found.
fn misdirected(agent: &Agent, id: &str) -> Option<Response> {
    let name = |c: char| c.is_ascii_alphanumeric() || "_.-".contains(c);
    let Some((instance, _)) = id.rsplit_once('.').filter(|(instance, n)| {
        !instance.is_empty()
            && instance.chars().all(name)
            && !n.is_empty()
            && n.chars().all(|c| c.is_ascii_alphanumeric())
    }) else {
        return Some(storage(StoreError::NotFound));
    };
    if instance == format!("{}.{}", agent.name, agent.instance_id) {
        return None;
    }
    let mut response = fail(
        StatusCode::MISDIRECTED_REQUEST,
        "session belongs to another instance",
    );
    if let Ok(value) = HeaderValue::from_str(instance) {
        response.headers_mut().insert("agent-instance", value);
    }
    Some(response)
}
fn storage(e: StoreError) -> Response {
    let (status, message) = match e {
        StoreError::NotFound => (StatusCode::NOT_FOUND, "not found"),
        StoreError::Conflict => (StatusCode::CONFLICT, "conflict"),
        StoreError::QuotaExceeded => (StatusCode::TOO_MANY_REQUESTS, "capacity exceeded"),
        StoreError::Invalid => (StatusCode::BAD_REQUEST, "invalid input"),
        _ => (StatusCode::SERVICE_UNAVAILABLE, "storage unavailable"),
    };
    fail(status, message)
}
fn ok(id: Value, result: Value, session: Option<&str>) -> Response {
    let mut response = Json(json!({"jsonrpc":"2.0","id":id,"result":result})).into_response();
    if let Some(s) = session
        && let Ok(h) = HeaderValue::from_str(s)
    {
        response.headers_mut().insert("agent-session-id", h);
    }
    response
}
fn header<'a>(h: &'a HeaderMap, name: &str) -> Option<&'a str> {
    h.get(name).and_then(|v| v.to_str().ok())
}
const NOTE_OPEN: &str = "<turn-note>";
const NOTE_CLOSE: &str = "</turn-note>\n\n";
/// Puts a turn's note ahead of what the person said, as agent-pi does, so the model reads it and
/// the transcript can leave it out.
fn with_note(input: &str, note: &str) -> String {
    format!("{NOTE_OPEN}{note}{NOTE_CLOSE}{input}")
}
fn without_note(text: &str) -> &str {
    text.strip_prefix(NOTE_OPEN)
        .and_then(|rest| rest.split_once(NOTE_CLOSE))
        .map_or(text, |(_, input)| input)
}
/// What every session supports, as in agent-pi's `capabilitiesOf`.
const CAPABILITIES: [&str; 6] = [
    "durable_runs",
    "steer",
    "interrupt",
    "voice",
    "retry",
    "background_tasks",
];
/// A spend in agent-pi's `Usage` shape. This store keeps input and output tokens only.
fn usage_value((input, output): (u64, u64)) -> Value {
    json!({"input":input,"output":output,"cacheRead":0,"cacheWrite":0,"totalTokens":input+output})
}
fn session_value(s: Session) -> Value {
    let parked = s.parked;
    let mut v = serde_json::to_value(s).unwrap();
    v["capabilities"] = json!(CAPABILITIES);
    v["tasks"] = json!([]);
    v["usage"] = json!({});
    v["parked"] = json!(parked);
    v
}

struct SessionStateAdapter {
    store: Arc<AgentStore>,
    scope: UserScope,
    session_id: String,
    plugin: String,
}
impl agent_ti_plugin_sdk::PluginState for SessionStateAdapter {
    fn get(&self) -> agent_ti_core::BoxFuture<'_, Result<Option<Value>, String>> {
        Box::pin(async move {
            self.store
                .plugin_state_get(&self.scope, &self.session_id, &self.plugin)
                .await
                .map_err(|e| format!("{e:?}"))
        })
    }
    fn set(&self, value: Value) -> agent_ti_core::BoxFuture<'_, Result<(), String>> {
        Box::pin(async move {
            self.store
                .plugin_state_set(&self.scope, &self.session_id, &self.plugin, &value)
                .await
                .map_err(|e| format!("{e:?}"))
        })
    }
}

struct SessionHistoryAdapter {
    store: Arc<AgentStore>,
    scope: UserScope,
    session_id: String,
    plugin: String,
}
impl agent_ti_plugin_sdk::SessionHistory for SessionHistoryAdapter {
    fn say(&self, text: &str) -> agent_ti_core::BoxFuture<'_, Result<(), String>> {
        let text = text.to_string();
        Box::pin(async move {
            self.store
                .history_say(&self.scope, &self.session_id, &self.plugin, &text)
                .await
                .map_err(|e| format!("{e:?}"))
        })
    }
    fn exchange(
        &self,
        input: &str,
        answer: &str,
    ) -> agent_ti_core::BoxFuture<'_, Result<(), String>> {
        let input = input.to_string();
        let answer = answer.to_string();
        Box::pin(async move {
            self.store
                .history_exchange(&self.scope, &self.session_id, &self.plugin, &input, &answer)
                .await
                .map_err(|e| format!("{e:?}"))
        })
    }
    fn record(
        &self,
        record_type: &str,
        data: Value,
    ) -> agent_ti_core::BoxFuture<'_, Result<(), String>> {
        let record_type = record_type.to_string();
        Box::pin(async move {
            self.store
                .history_record(
                    &self.scope,
                    &self.session_id,
                    &self.plugin,
                    &record_type,
                    &data,
                )
                .await
                .map_err(|e| format!("{e:?}"))
        })
    }
}

struct ApiError(StatusCode, &'static str);
impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        fail(self.0, self.1)
    }
}
fn endpoint(
    host: &Host,
    name: Option<String>,
    headers: &HeaderMap,
) -> Result<Arc<Agent>, ApiError> {
    let name = name.unwrap_or_else(|| host.default_agent.clone());
    if header(headers, "agent-name").is_some_and(|v| v != name) {
        return Err(ApiError(StatusCode::BAD_REQUEST, "unknown Agent-Name"));
    }
    host.agents
        .get(&name)
        .cloned()
        .ok_or(ApiError(StatusCode::NOT_FOUND, "unknown agent"))
}
async fn principal(
    host: &Host,
    agent: Option<&Agent>,
    headers: &HeaderMap,
) -> Result<Principal, ApiError> {
    let mut p = host
        .auth
        .verify(headers)
        .await
        .map_err(|_| ApiError(StatusCode::UNAUTHORIZED, "invalid bearer token"))?;
    if let Some(agent) = agent {
        for plugin in &agent.plugins {
            let info = agent_ti_plugin_sdk::PrincipalInfo {
                user_id: p.user_id.clone(),
                tenant_id: p.tenant_id.clone(),
                scopes: p.scopes.clone(),
                claims: p.claims.clone(),
            };
            let limit = Duration::from_millis(host.config.limits.hook_timeout_ms);
            let updated = crate::plugins::within(limit, "authenticate", plugin.authenticate(info)).await.map_err(|e| {
                tracing::warn!(agent = %agent.name, plugin = plugin.name(), error = %e, "authenticate hook rejected");
                ApiError(StatusCode::FORBIDDEN, "authentication hook rejected")
            })?;
            p.scopes = updated.scopes;
            p.claims = updated.claims;
        }
    }
    if host.auth.missing_scope(&p).is_some() {
        return Err(ApiError(StatusCode::FORBIDDEN, "required scope missing"));
    }
    if let Some(grant_id) = p.claims.get("grant").and_then(Value::as_str)
        && let Some(agent) = agent
        && let Some(ref broker) = agent.grants
        && broker.confirm_chain(grant_id).await.is_err()
    {
        return Err(ApiError(
            StatusCode::FORBIDDEN,
            "grant chain confirmation failed",
        ));
    }
    Ok(p)
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Rpc {
    jsonrpc: String,
    id: Value,
    method: String,
    #[serde(default)]
    params: Value,
}
/// Workbench sends text-only content blocks as well as strings. Unsupported
/// attachments fail explicitly rather than losing part of the user's input.
fn text_input(value: &Value) -> Result<String, &'static str> {
    if let Some(text) = value.as_str() {
        return Ok(text.into());
    }
    let parts = value
        .as_array()
        .filter(|p| !p.is_empty() && p.len() <= 128)
        .ok_or("input must be text")?;
    let mut text = String::new();
    for part in parts {
        if part["type"] != "text" {
            return Err("this agent supports text messages only");
        }
        text.push_str(part["text"].as_str().ok_or("invalid text block")?);
    }
    Ok(text)
}
async fn rpc_default(State(host): State<Arc<Host>>, headers: HeaderMap, body: Bytes) -> Response {
    rpc(host, None, headers, body).await
}
async fn rpc_named(
    State(host): State<Arc<Host>>,
    Path(name): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    rpc(host, Some(name), headers, body).await
}
fn admitted(a: Admission) -> Value {
    let placement = if a.placed { "placed" } else { "queued" };
    match &a.reply {
        Some(reply) => json!({
            "turnId": a.turn_id, "status": "done", "answeredBy": reply.source, "text": reply.text,
            "placement": placement, "duplicate": a.duplicate, "submissionId": a.ordinal
        }),
        None => {
            json!({"accepted": true, "submissionId": a.ordinal, "turnId": a.turn_id, "placement": placement, "duplicate": a.duplicate})
        }
    }
}
/// The session's state, and the skills and tenant customization its turn pins: the session
/// owner's drafts in their own test conversation, or else the published ones, less the skills the
/// customization turns off.
async fn turn_lookup(
    agent: &Agent,
    s: &UserScope,
    session_id: &str,
) -> Result<
    (
        SessionState,
        Arc<Vec<agent_ti_config::SkillConfig>>,
        Option<Arc<crate::tenant_config::TenantConfig>>,
    ),
    StoreError,
> {
    let session_state = agent.store.state(s, session_id).await?;
    let metadata = &session_state.session.metadata;
    let skill_draft = metadata
        .get("skillDraft")
        .and_then(Value::as_str)
        .filter(|n| !n.is_empty());
    let config_draft = metadata.get("configDraft").and_then(Value::as_bool) == Some(true);
    let skills = async {
        match (&agent.tenant_skills, skill_draft) {
            (Some(ts), Some(draft)) => ts.with_draft(Some(&s.tenant_id), &s.user_id, draft).await,
            (Some(ts), None) => ts.for_tenant(Some(&s.tenant_id)).await,
            (None, _) => Arc::new(agent.config.skills.clone()),
        }
    };
    let config = async {
        match &agent.tenant_config {
            Some(tc) if config_draft => Some(tc.draft(&s.tenant_id, &s.user_id).await),
            Some(tc) => Some(tc.for_principal(&s.tenant_id, &s.user_id).await),
            None => None,
        }
    };
    let (turn_skills, turn_config) = tokio::join!(skills, config);
    let turn_skills = match &turn_config {
        Some(c) if !c.skills_off.is_empty() => Arc::new(
            turn_skills
                .iter()
                .filter(|sk| c.skill_on(&sk.name))
                .cloned()
                .collect::<Vec<_>>(),
        ),
        _ => turn_skills,
    };
    Ok((session_state, turn_skills, turn_config))
}
/// `session.prompt`: hooks run before admission, so a rejection is a 422 and a plugin's reply never
/// queues for the model. A retried `requestId` returns what it became without running hooks again.
#[allow(clippy::result_large_err, clippy::too_many_arguments)] // the error is the finished HTTP response
async fn prompt(
    host: &Host,
    agent: &Agent,
    s: UserScope,
    session_id: &str,
    params: &Value,
    headers: &HeaderMap,
    when_busy_override: Option<&str>,
    principal: &Principal,
) -> Result<Value, Response> {
    if params["wait"] != false && !params["wait"].is_null() {
        return Err(fail(
            StatusCode::BAD_REQUEST,
            "only wait:false is supported",
        ));
    }
    let traceparent = headers.get("traceparent").and_then(|v| v.to_str().ok());
    let attach = async {
        match agent.grants {
            Some(ref broker) => broker
                .attach(&agent.store, &s, session_id, principal, traceparent)
                .await
                .map(|_| ()),
            None => Ok(()),
        }
    };
    // Extending the grant and reading the turn's session, skills and customization each may wait
    // on another service, so they go together.
    let (attached, looked_up) = tokio::join!(attach, turn_lookup(agent, &s, session_id));
    attached.map_err(|e| match e {
        crate::grants::GrantError::Expired => fail(StatusCode::UNAUTHORIZED, "grant expired"),
        crate::grants::GrantError::Stopped => fail(StatusCode::FORBIDDEN, "grant stopped"),
        crate::grants::GrantError::Unavailable(m) => fail(StatusCode::SERVICE_UNAVAILABLE, &m),
    })?;
    let when_busy = match when_busy_override {
        Some(wb) => Some(wb.to_string()),
        None => params
            .get("whenBusy")
            .and_then(|v| v.as_str())
            .map(str::to_owned),
    };
    if let Some(ref wb) = when_busy
        && !matches!(wb.as_str(), "queue" | "followUp" | "steer" | "reject")
    {
        return Err(fail(
            StatusCode::BAD_REQUEST,
            "whenBusy must be followUp, steer, or reject",
        ));
    }
    let input = text_input(&params["input"]).map_err(|m| fail(StatusCode::BAD_REQUEST, m))?;
    if input.chars().count() > agent.config.limits.max_input_chars {
        return Err(fail(
            StatusCode::PAYLOAD_TOO_LARGE,
            "input is longer than limits.maxInputChars",
        ));
    }
    let mut note = match &params["note"] {
        Value::Null => None,
        Value::String(n) if n.chars().count() <= agent.config.limits.max_input_chars => {
            Some(n.clone())
        }
        Value::String(_) => {
            return Err(fail(
                StatusCode::BAD_REQUEST,
                "note is longer than limits.maxInputChars",
            ));
        }
        _ => return Err(fail(StatusCode::BAD_REQUEST, "note must be a string")),
    };
    let Some(key) = params["requestId"].as_str() else {
        return Err(fail(StatusCode::BAD_REQUEST, "requestId required"));
    };
    let fingerprint = match &note {
        None => digest(&json!({"kind":"prompt","input":input})),
        Some(note) => digest(&json!({"kind":"prompt","input":input,"note":note})),
    };
    if let Some(prior) = agent
        .store
        .prior(&s, session_id, key)
        .await
        .map_err(storage)?
    {
        if prior.fingerprint != fingerprint {
            return Err(storage(StoreError::Conflict));
        }
        return Ok(admitted(prior.admission));
    }
    let turn_id = params["turnId"]
        .as_str()
        .map(str::to_owned)
        .unwrap_or_else(|| uuid::Uuid::new_v4().simple().to_string());
    let mut text = input;
    let mut reply = None;
    let mut plugin_tools = Vec::new();
    for plugin in &agent.plugins {
        let turn = Turn {
            text: &text,
            note: note.as_deref(),
            agent: &agent.name,
            session_id,
            tenant_id: &s.tenant_id,
            user_id: &s.user_id,
        };
        let limit = Duration::from_millis(host.config.limits.hook_timeout_ms);
        let decision = crate::plugins::within(limit, "before_prompt", plugin.before_prompt(&turn)).await.map_err(|e| {
            tracing::error!(agent = %agent.name, plugin = plugin.name(), error = %e, "before_prompt failed");
            fail(StatusCode::INTERNAL_SERVER_ERROR, "a plugin failed")
        })?;
        match decision {
            None => {}
            Some(PromptDecision::Input(rewrite)) => text = rewrite,
            Some(PromptDecision::Rewrite {
                text: rewrite,
                note: rewritten,
            }) => {
                text = rewrite;
                if rewritten.is_some() {
                    note = rewritten;
                }
            }
            Some(PromptDecision::InputAndTools {
                text: rewrite,
                activate_tools,
            }) => {
                text = rewrite;
                plugin_tools.extend(activate_tools);
            }
            Some(PromptDecision::Reject(reason)) => {
                tracing::info!(agent = %agent.name, plugin = plugin.name(), session_id, %reason, "prompt rejected");
                return Err(fail(StatusCode::UNPROCESSABLE_ENTITY, &reason));
            }
            Some(PromptDecision::Reply(answer)) => {
                reply = Some(Reply {
                    text: answer,
                    source: plugin.name().into(),
                });
                break;
            }
        }
    }
    let (session_state, turn_skills, turn_config) = looked_up.map_err(storage)?;
    agent
        .turn_pins
        .pin(session_id, turn_skills.clone(), turn_config.clone())
        .await;

    // Someone is here: idle tasks' reply clocks start, and tasks may wake the session again.
    if session_state.session.parent.is_none() && agent.has_background_tasks() {
        let _ = agent.store.tasks_heard(&s, session_id).await;
    }
    let profile_name = session_state.session.profile;
    if let Some(prof) = agent.config.profiles.get(&profile_name) {
        // A plugin's reply is spoken too, so the session speaks before it is committed.
        if prof.speech {
            agent.hub.speak(session_id, session_state.high_water);
        }
        let pinned = agent.pinned_tools(&s.tenant_id, prof).await;
        let policy = agent_ti_store::ActivationPolicy {
            pinned,
            max_active: prof.max_active_tools as usize,
        };
        // Pinned tools added to the agent since this session began are offered from its next
        // turn, so an older conversation doesn't have to search for them.
        if let Ok(active) = agent.store.get_session_tools(&s, session_id).await {
            let missing: Vec<String> = policy
                .pinned
                .iter()
                .filter(|name| !active.offered.contains(name))
                .cloned()
                .collect();
            if !missing.is_empty() {
                let _ = agent
                    .store
                    .activate_tools(&s, session_id, &missing, &policy)
                    .await;
            }
        }
        if !plugin_tools.is_empty() {
            let matched = agent.match_tools(&plugin_tools, &s.tenant_id).await;
            if !matched.is_empty() {
                let _ = agent
                    .store
                    .activate_tools(&s, session_id, &matched, &policy)
                    .await;
            }
        }
        if prof.preload_max_tools > 0 {
            let preloaded = agent
                .preload_tools(
                    &text,
                    prof,
                    &s.tenant_id,
                    &turn_skills,
                    turn_config.as_deref(),
                )
                .await;
            if !preloaded.is_empty() {
                let _ = agent
                    .store
                    .activate_tools(&s, session_id, &preloaded, &policy)
                    .await;
            }
        }
        let _ = agent
            .store
            .refresh_skills(
                &s,
                session_id,
                |skill_name| {
                    turn_skills
                        .iter()
                        .find(|sk| sk.name == skill_name)
                        .map(|sk| agent.match_tools_sync(&sk.tools, &s.tenant_id))
                },
                &policy,
            )
            .await;
    }
    let traceparent = header(headers, "traceparent")
        .filter(|t| t.len() == 55 && t.bytes().all(|c| c.is_ascii_hexdigit() || c == b'-'))
        .map(str::to_owned);
    let submit = Submit {
        scope: s.clone(),
        session_id: session_id.into(),
        request_key: key.into(),
        fingerprint,
        input: match &note {
            Some(note) => with_note(&text, note),
            None => text,
        },
        turn_id,
        traceparent,
        reply,
        when_busy,
    };
    let admission = agent.store.admit(submit).await.map_err(storage)?;
    if let (Some(r), true, false) = (&admission.reply, admission.placed, admission.duplicate) {
        let profile = agent
            .store
            .state(&s, session_id)
            .await
            .map(|st| st.session.profile)
            .unwrap_or_default();
        let info = agent_ti_plugin_sdk::RunInfo {
            agent: &agent.name,
            session_id,
            turn_id: &admission.turn_id,
            tenant_id: &s.tenant_id,
            user_id: &s.user_id,
            profile: &profile,
            answered_by: &r.source,
            status: "completed",
            reason: None,
            text: &r.text,
        };
        for plugin in &agent.plugins {
            plugin.after_run(&info);
        }
    } else if !admission.duplicate {
        host.scheduler.wake();
    }
    Ok(admitted(admission))
}
async fn rpc(host: Arc<Host>, name: Option<String>, headers: HeaderMap, body: Bytes) -> Response {
    let agent = match endpoint(&host, name, &headers) {
        Ok(v) => v,
        Err(e) => return e.into_response(),
    };
    let p = match principal(&host, Some(&agent), &headers).await {
        Ok(p) => p,
        Err(e) => return e.into_response(),
    };
    let _permit = match host.requests.try_acquire() {
        Ok(p) => p,
        Err(_) => return fail(StatusCode::TOO_MANY_REQUESTS, "too many requests"),
    };
    let r: Rpc = match serde_json::from_slice(&body) {
        Ok(r) => r,
        Err(_) => return fail(StatusCode::BAD_REQUEST, "invalid JSON-RPC"),
    };
    if r.jsonrpc != "2.0" || (!r.params.is_null() && !r.params.is_object()) {
        return fail(StatusCode::BAD_REQUEST, "invalid JSON-RPC");
    }
    let s = UserScope {
        tenant_id: p.tenant_id.clone(),
        agent_id: agent.name.clone(),
        user_id: p.user_id.clone(),
    };
    match r.method.as_str() {
        "agents.list" => return ok(r.id, json!({"agents":[{"name":agent.name}]}), None),
        "skills.list" => {
            let skills_view = agent.visible_skills(&s.tenant_id, &s.user_id).await;
            let skills: Vec<Value> = skills_view
                .iter()
                .map(|s| {
                    let mut val = json!({
                        "name": s.name,
                        "description": s.description,
                        "tools": s.tools,
                    });
                    if let Some(ref flow) = s.flow {
                        val["flow"] = flow.clone();
                    }
                    if let Some(ref source) = s.source {
                        val["source"] = json!({
                            "kind": source.kind,
                            "version": source.version,
                            "digest": source.digest,
                        });
                    }
                    val
                })
                .collect();
            return ok(r.id, json!({ "skills": skills }), None);
        }
        "skills.get" => {
            let skill_name = match r.params.get("skill").and_then(Value::as_str) {
                Some(name) if !name.is_empty() => name,
                _ => return fail(StatusCode::BAD_REQUEST, "skill must be a non-empty string"),
            };
            let skills_view = agent.visible_skills(&s.tenant_id, &s.user_id).await;
            let skill = match skills_view.iter().find(|s| s.name == skill_name) {
                Some(s) => s,
                None => {
                    return fail(
                        StatusCode::NOT_FOUND,
                        &format!("unknown skill {skill_name}"),
                    );
                }
            };
            let files: Vec<String> = skill.files.keys().cloned().collect();
            let mut skill_val = json!({
                "name": skill.name,
                "description": skill.description,
                "tools": skill.tools,
                "keywords": skill.keywords,
                "body": skill.body,
                "files": files,
            });
            if let Some(ref flow) = skill.flow {
                skill_val["flow"] = flow.clone();
            }
            if let Some(ref source) = skill.source {
                skill_val["source"] = json!({
                    "kind": source.kind,
                    "version": source.version,
                    "digest": source.digest,
                });
            }
            return ok(
                r.id,
                json!({
                    "skill": skill_val
                }),
                None,
            );
        }
        // Saves nothing: a tenant admin publishes at t10-agent-control, and this says first what the
        // runtime will refuse. A built-in skill's name checks an override; any other a custom skill.
        "skills.checkFlow" => {
            let Some(name) = r
                .params
                .get("skill")
                .and_then(Value::as_str)
                .filter(|n| !n.is_empty())
            else {
                return fail(StatusCode::BAD_REQUEST, "skill must be a non-empty string");
            };
            let Some(flow) = r.params.get("flow").filter(|f| f.is_object()) else {
                return fail(StatusCode::BAD_REQUEST, "flow must be an object");
            };
            let tools = r
                .params
                .get("tools")
                .and_then(Value::as_array)
                .map(|tools| {
                    tools
                        .iter()
                        .filter_map(|t| t.as_str().map(str::to_string))
                        .collect::<Vec<_>>()
                });
            let checked = match agent.config.skills.iter().find(|skill| skill.name == name) {
                Some(builtin) => agent_ti_config::override_flow(builtin, flow),
                None => {
                    let content = agent_ti_config::TenantSkillContent {
                        description: Some(name.to_string()),
                        tools,
                        flow: Some(flow.clone()),
                        ..Default::default()
                    };
                    agent_ti_config::custom_skill(name, &content, None)
                        .map(|skill| skill.flow.unwrap_or_default())
                }
            };
            return match checked.and_then(|flow| agent_ti_config::flow::validate_skill_flow(&flow))
            {
                Ok((tools, flow)) => ok(
                    r.id,
                    json!({"skill": name, "flow": flow, "tools": tools}),
                    None,
                ),
                Err(e) => fail(StatusCode::BAD_REQUEST, &e),
            };
        }
        "skills.tools" => {
            let tools = agent.catalog_tools(&s.tenant_id).await;
            let result: Vec<Value> = tools
                .into_iter()
                .map(|t| {
                    // owner is how a tenant's tool list and policy name the tool: the module
                    // that owns it, or else its server. The harness's own tools have none.
                    json!({
                        "name": t.name,
                        "server": t.server,
                        "tool": t.mcp_name,
                        "description": t.description,
                        "owner": t.owner,
                    })
                })
                .collect();
            return ok(r.id, json!({ "tools": result }), None);
        }
        _ => {}
    }
    let session_id = header(&headers, "agent-session-id")
        .or_else(|| r.params["sessionId"].as_str())
        .unwrap_or("")
        .to_owned();
    if !["session.create", "session.list"].contains(&r.method.as_str()) {
        if session_id.is_empty() || session_id.len() > 256 {
            return fail(StatusCode::BAD_REQUEST, "session ID required");
        }
        if let Some(response) = misdirected(&agent, &session_id) {
            return response;
        }
        match agent.store.state(&s, &session_id).await {
            // A child session can be read, never driven: its parent's tool call owns it.
            Ok(state)
                if state.session.parent.is_some()
                    && !["session.get", "session.messages", "session.tool"]
                        .contains(&r.method.as_str()) =>
            {
                return storage(StoreError::NotFound);
            }
            Ok(state) if state.session.status == "open" => {}
            Ok(_) => return storage(StoreError::NotFound),
            Err(e) => return storage(e),
        }
    }
    let accepting = host.accepting.load(Ordering::SeqCst) && agent.accepting.load(Ordering::SeqCst);
    if !accepting && ["session.create", "session.prompt"].contains(&r.method.as_str()) {
        return fail(StatusCode::SERVICE_UNAVAILABLE, "draining");
    }
    let result: StoreResult<Value> = match r.method.as_str() {
        "session.create" => {
            let profile = r.params["profile"].as_str().unwrap_or(&agent.config.default_profile).to_owned();
            if !agent.config.profiles.contains_key(&profile) {
                return fail(StatusCode::BAD_REQUEST, "unknown profile");
            }
            let id = format!("{}.{}.{}", agent.name, agent.instance_id, uuid::Uuid::new_v4().simple());
            let key = header(&headers, "agent-request-id").map(str::to_owned).unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
            // The tenant's model is recorded with the session, as its profile is, so a later
            // version doesn't move an existing conversation to another model.
            let mut origin = p.origin();
            if let Some(ref tc) = agent.tenant_config {
                let draft = r.params["metadata"]["configDraft"].as_bool() == Some(true);
                let config = if draft {
                    tc.draft(&s.tenant_id, &s.user_id).await
                } else {
                    tc.for_principal(&s.tenant_id, &s.user_id).await
                };
                if let Some(model) = agent.chosen_model(&agent.config.profiles[&profile], &config) {
                    origin["model"] = json!(model);
                }
            }
            let request = CreateSession {
                scope: s.clone(),
                session_id: id,
                request_key: key,
                profile,
                config_version: agent.version.clone(),
                origin,
                metadata: r.params.get("metadata").cloned().unwrap_or(json!({})),
                child: None,
            };
            return match agent.store.create(request).await {
                Ok((session, duplicate)) => {
                    let id = session.session_id.clone();
                    if !duplicate {
                        if let Some(ref broker) = agent.grants
                            && !p.token.is_empty()
                        {
                            let traceparent = header(&headers, "traceparent");
                            if let Err(e) = broker.attach(&agent.store, &s, &id, &p, traceparent).await {
                                tracing::warn!(session_id = %id, error = %e, "grant attach failed on session create");
                            }
                        }
                        if let Some(profile_cfg) = agent.config.profiles.get(&session.profile) {
                            let pinned = agent.pinned_tools(&s.tenant_id, profile_cfg).await;
                            let policy = agent_ti_store::ActivationPolicy {
                                pinned: pinned.clone(),
                                max_active: profile_cfg.max_active_tools as usize,
                            };
                            let _ = agent
                                .store
                                .activate_tools(&s, &id, &pinned, &policy)
                                .await;
                        }
                        for plugin in &agent.plugins {
                            let state_adapter = Arc::new(SessionStateAdapter {
                                store: agent.store.clone(),
                                scope: s.clone(),
                                session_id: id.clone(),
                                plugin: plugin.name().to_string(),
                            });
                            let history_adapter = Arc::new(SessionHistoryAdapter {
                                store: agent.store.clone(),
                                scope: s.clone(),
                                session_id: id.clone(),
                                plugin: plugin.name().to_string(),
                            });
                            let ctx = agent_ti_plugin_sdk::SessionContext {
                                agent: &agent.name,
                                session_id: &id,
                                tenant_id: &s.tenant_id,
                                user_id: &s.user_id,
                                profile: &session.profile,
                                state: state_adapter,
                                history: history_adapter,
                            };
                            let limit = Duration::from_millis(host.config.limits.hook_timeout_ms);
                            if let Err(e) = crate::plugins::within(limit, "session_created", plugin.session_created(&ctx)).await {
                                tracing::warn!(agent = %agent.name, plugin = plugin.name(), error = %e, "session_created failed");
                            }
                        }
                    }
                    ok(r.id, session_value(session), Some(&id))
                }
                Err(e) => storage(e),
            };
        }
        "session.list" => agent.store.list(&s, r.params["after"].as_str().unwrap_or(""), r.params["limit"].as_u64().unwrap_or(50).min(100) as u32).await.map(|sessions| {
            let next = sessions.last().map(|s| s.session_id.clone());
            json!({"sessions":sessions.into_iter().map(session_value).collect::<Vec<_>>(),"nextCursor":next})
        }),
        "session.get" => match agent.store.state(&s, &session_id).await {
            Ok(state) => match agent.store.tasks(&s, &session_id).await {
                Ok(tasks) => {
                    let flow = agent.store.flow(&s, &session_id).await.unwrap_or_default();
                    let spent = agent.store.usage(&s, &session_id).await.unwrap_or_default();
                    agent.store.children(&s, &session_id).await.map(|children| {
                        let profile = state.session.profile.clone();
                        let mut value = session_value(state.session);
                        value["subagents"] = json!(children);
                        value["tasks"] = json!(tasks);
                        for task in value["tasks"].as_array_mut().into_iter().flatten() {
                            let id = task["taskId"].as_str().unwrap_or_default();
                            if let Some((_, used)) = spent.tasks.iter().find(|(t, _)| t == id) {
                                task["usage"] = usage_value(*used);
                            }
                        }
                        let task_total = spent.tasks.iter().fold((0, 0), |sum, (_, used)| {
                            (sum.0 + used.0, sum.1 + used.1)
                        });
                        value["taskUsage"] = usage_value(task_total);
                        let model = agent
                            .config
                            .profiles
                            .get(&profile)
                            .map_or(String::new(), |p| p.model.clone());
                        value["usage"] = json!({
                            "models": {model: usage_value(spent.own)},
                            "tools": {"subagents": usage_value(spent.foreground)},
                        });
                        if let Some(flow) = flow {
                            value["flow"] = flow;
                        }
                        value
                    })
                }
                Err(e) => Err(e),
            },
            Err(e) => Err(e),
        },
        "session.tool" => {
            let call_id = match r.params.get("callId") {
                Some(Value::String(s)) if !s.is_empty() && s.len() <= 1000 => s.as_str(),
                _ => return fail(StatusCode::BAD_REQUEST, "callId must be a non-empty string"),
            };
            match agent.store.tool_payload(&s, &session_id, call_id).await {
                Ok(Some(payload)) => Ok(payload),
                Ok(None) => return storage(StoreError::NotFound),
                Err(e) => return storage(e),
            }
        }
        "session.messages" => {
            let before = match r.params.get("cursor") {
                None | Some(Value::Null) => i64::MAX,
                Some(Value::String(cursor)) => match cursor.strip_prefix(&format!("{session_id}:")).and_then(|n| n.parse::<i64>().ok()).filter(|n| *n > 0) {
                    Some(n) => n,
                    None => return fail(StatusCode::BAD_REQUEST, "invalid transcript cursor"),
                },
                _ => return fail(StatusCode::BAD_REQUEST, "invalid transcript cursor"),
            };
            agent.store.transcript(&s, &session_id, before, r.params["limit"].as_u64().unwrap_or(100).min(100) as u32).await.map(|page| {
                let cursor = page.entries.first().map(|e| format!("{session_id}:{}", e.sequence));
                // A tool result names its tool by the assistant call it answers; the result row itself holds only the call id.
                let tool_names: std::collections::HashMap<String, String> = page
                    .entries
                    .iter()
                    .flat_map(|e| e.message.tool_calls.iter().map(|c| (c.id.clone(), c.name.clone())))
                    .collect();
                let messages = page
                    .entries
                    .into_iter()
                    .map(|e| {
                        let mut value = json!({"id":e.sequence,"sequence":e.sequence,"role":e.message.role,"text":e.message.content,"toolCalls":e.message.tool_calls,"callId":e.message.tool_call_id,"stopReason":if e.status=="cancelled"{"aborted"}else{"stop"}});
                        if e.message.role == "tool" {
                            value["name"] = json!(e.message.tool_call_id.as_ref().and_then(|id| tool_names.get(id)).cloned().unwrap_or_default());
                        }
                        if e.message.is_error {
                            value["isError"] = json!(true);
                        }
                        if let Some(source) = &e.message.source {
                            value["source"] = json!(source);
                        }
                        // A task's report or note: data from a child, not something the user said.
                        if e.message.role == "user"
                            && let Some(line) = crate::tasks::transcript_line(&e.message.content)
                        {
                            let mut line = line;
                            line["id"] = json!(e.sequence);
                            line["sequence"] = json!(e.sequence);
                            return line;
                        }
                        if e.message.role == "user" {
                            value["text"] = json!(without_note(&e.message.content));
                            value["turnId"] = json!(e.turn_id);
                            value["requestId"] = json!(e.request_id);
                        }
                        value
                    })
                    .collect::<Vec<_>>();
                let mut result = json!({"messages":messages});
                if page.has_more {
                    result["nextCursor"] = json!(cursor);
                }
                result
            })
        }
        "session.update" => agent.store.update(&s, &session_id, r.params["name"].as_str().map(str::to_owned), r.params.get("metadata").cloned(), false).await.map(session_value),
        "session.close" => {
            let res = agent.store.update(&s, &session_id, None, None, true).await.map(|_| json!({"closed":true}));
            if res.is_ok() {
                // A plain close would leave background children running.
                crate::tasks::end_all(&host, &agent, &s, &session_id).await;
                if let Some(ref broker) = agent.grants {
                    broker.close(&session_id).await;
                }
                for plugin in &agent.plugins {
                    let state_adapter = Arc::new(SessionStateAdapter {
                        store: agent.store.clone(),
                        scope: s.clone(),
                        session_id: session_id.clone(),
                        plugin: plugin.name().to_string(),
                    });
                    let history_adapter = Arc::new(SessionHistoryAdapter {
                        store: agent.store.clone(),
                        scope: s.clone(),
                        session_id: session_id.clone(),
                        plugin: plugin.name().to_string(),
                    });
                    let ctx = agent_ti_plugin_sdk::SessionContext {
                        agent: &agent.name,
                        session_id: &session_id,
                        tenant_id: &s.tenant_id,
                        user_id: &s.user_id,
                        profile: "",
                        state: state_adapter,
                        history: history_adapter,
                    };
                    let limit = Duration::from_millis(host.config.limits.hook_timeout_ms);
                    if let Err(e) = crate::plugins::within(limit, "session_closed", plugin.session_closed(&ctx)).await {
                        tracing::warn!(agent = %agent.name, plugin = plugin.name(), error = %e, "session_closed failed");
                    }
                }
            }
            res
        }
        "session.prompt" => {
            match prompt(&host, &agent, s.clone(), &session_id, &r.params, &headers, None, &p).await {
                Ok(value) => Ok(value),
                Err(response) => return response,
            }
        }
        "session.steer" => {
            if r.params.get("input").is_none() || r.params["input"].is_null() {
                return fail(StatusCode::BAD_REQUEST, "input required");
            }
            match prompt(&host, &agent, s.clone(), &session_id, &r.params, &headers, Some("steer"), &p).await {
                Ok(val) => {
                    let placement = val["placement"].as_str().unwrap_or("queued");
                    if val.get("answeredBy").is_some() {
                        Ok(json!({
                            "turnId": val["turnId"],
                            "placement": "answered",
                            "answeredBy": val["answeredBy"],
                            "text": val["text"]
                        }))
                    } else {
                        Ok(json!({
                            "turnId": val["turnId"],
                            "submissionId": val["submissionId"],
                            "placement": placement
                        }))
                    }
                }
                Err(res) => return res,
            }
        }
        "session.interrupt" => {
            let during_tools = r.params.get("duringTools").and_then(|v| v.as_str());
            if let Some(dt) = during_tools
                && dt != "abort"
                && dt != "steer"
            {
                return fail(StatusCode::BAD_REQUEST, "duringTools must be abort or steer");
            }
            if r.params.get("input").is_none() || r.params["input"].is_null() {
                let key = r.params["requestId"].as_str().map(str::to_owned).unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
                let running = match agent.store.cancel(&s, &session_id, &key).await {
                    Ok(r) => r,
                    Err(e) => return storage(e),
                };
                host.scheduler.cancel(&running);
                if let Err(e) = agent.store.withdraw_pending(&s, &session_id).await {
                    return storage(e);
                }
                host.scheduler.wake();
                Ok(json!({"aborted": true}))
            } else {
                let in_tools = host.scheduler.is_running_tools(&agent.name, &s.tenant_id, &session_id);
                if during_tools == Some("steer") && in_tools {
                    match prompt(&host, &agent, s.clone(), &session_id, &r.params, &headers, Some("steer"), &p).await {
                        Ok(val) => Ok(val),
                        Err(res) => return res,
                    }
                } else {
                    let key = r.params["requestId"].as_str().unwrap_or("");
                    let prior = if !key.is_empty() {
                        match agent.store.prior(&s, &session_id, key).await {
                            Ok(p) => p,
                            Err(e) => return storage(e),
                        }
                    } else {
                        None
                    };
                    if prior.is_none() {
                        let cancel_key = format!("cancel_{}", uuid::Uuid::new_v4().simple());
                        let running = match agent.store.cancel(&s, &session_id, &cancel_key).await {
                            Ok(r) => r,
                            Err(e) => return storage(e),
                        };
                        host.scheduler.cancel(&running);
                        if let Err(e) = agent.store.withdraw_pending(&s, &session_id).await {
                            return storage(e);
                        }
                        host.scheduler.wake();
                    }
                    match prompt(&host, &agent, s.clone(), &session_id, &r.params, &headers, None, &p).await {
                        Ok(val) => Ok(val),
                        Err(res) => return res,
                    }
                }
            }
        }
        "session.retry" => {
            agent.store.retry(&s, &session_id).await.map(|count| {
                host.scheduler.wake();
                json!({"retried": count})
            })
        }
        "session.cancel" => {
            let key = r.params["requestId"].as_str().map(str::to_owned).unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
            // `tasks: true` ends background tasks too; a plain cancel leaves them, as agent-pi.
            if r.params["tasks"] == true {
                crate::tasks::end_all(&host, &agent, &s, &session_id).await;
            }
            agent.store.cancel(&s, &session_id, &key).await.map(|running| {
                host.scheduler.cancel(&running);
                host.scheduler.wake();
                json!({"cancelled":true})
            })
        }
        _ => {
            for plugin in &agent.plugins {
                if plugin.rpc_methods().contains(&r.method) {
                    if session_id.is_empty() || session_id.len() > 256 {
                        return fail(StatusCode::BAD_REQUEST, "session ID required");
                    }
                    let state_adapter = Arc::new(SessionStateAdapter {
                        store: agent.store.clone(),
                        scope: s.clone(),
                        session_id: session_id.clone(),
                        plugin: plugin.name().to_string(),
                    });
                    let history_adapter = Arc::new(SessionHistoryAdapter {
                        store: agent.store.clone(),
                        scope: s.clone(),
                        session_id: session_id.clone(),
                        plugin: plugin.name().to_string(),
                    });
                    let rpc_ctx = agent_ti_plugin_sdk::RpcContext {
                        agent: &agent.name,
                        tenant_id: &s.tenant_id,
                        user_id: &s.user_id,
                        session_id: Some(&session_id),
                        state: Some(state_adapter),
                        history: Some(history_adapter),
                    };
                    return match plugin.call_rpc(&r.method, r.params, &rpc_ctx).await {
                        Ok(val) => ok(r.id, val, Some(&session_id)),
                        Err(e) => fail(StatusCode::INTERNAL_SERVER_ERROR, &e),
                    };
                }
            }
            return fail(StatusCode::BAD_REQUEST, "method not implemented");
        }
    };
    let wants_stream = (r.method == "session.prompt" || r.method == "session.interrupt")
        && r.params.get("stream") != Some(&json!(false))
        && headers
            .get("accept")
            .and_then(|v| v.to_str().ok())
            .is_some_and(|a| a.contains("text/event-stream"));
    match result {
        Ok(value) => {
            if wants_stream {
                let ordinal = value.get("submissionId").and_then(|v| v.as_i64());
                let sub_id = ordinal.map(|n| n.to_string());
                // A streamed POST ends with the JSON-RPC response when its submission settles,
                // as agent-pi's does; a voice client waits for it.
                let streamed = ordinal.map(|ordinal| Streamed {
                    rpc_id: r.id.clone(),
                    turn_id: value["turnId"].as_str().unwrap_or_default().to_owned(),
                    ordinal,
                });
                open_event_stream(host.clone(), agent, p, session_id, &headers, sub_id, streamed).await
            } else {
                ok(
                    r.id,
                    value,
                    (!session_id.is_empty()).then_some(session_id.as_str()),
                )
            }
        }
        Err(e) => storage(e),
    }
}
/// `DELETE`: `session.close`, with no body and a 204.
async fn close(host: Arc<Host>, name: Option<String>, headers: HeaderMap) -> Response {
    if header(&headers, "agent-session-id").is_none_or(str::is_empty) {
        return fail(StatusCode::BAD_REQUEST, "missing agent-session-id header");
    }
    let body = json!({"jsonrpc":"2.0","id":null,"method":"session.close","params":{}});
    let response = rpc(host, name, headers, Bytes::from(body.to_string())).await;
    if response.status().is_success() {
        StatusCode::NO_CONTENT.into_response()
    } else {
        response
    }
}
async fn close_default(State(host): State<Arc<Host>>, headers: HeaderMap) -> Response {
    close(host, None, headers).await
}
async fn close_named(
    State(host): State<Arc<Host>>,
    Path(name): Path<String>,
    headers: HeaderMap,
) -> Response {
    close(host, Some(name), headers).await
}
async fn events_default(State(host): State<Arc<Host>>, headers: HeaderMap) -> Response {
    events(host, None, headers).await
}
async fn events_named(
    State(host): State<Arc<Host>>,
    Path(name): Path<String>,
    headers: HeaderMap,
) -> Response {
    events(host, Some(name), headers).await
}
async fn events(host: Arc<Host>, name: Option<String>, headers: HeaderMap) -> Response {
    let agent = match endpoint(&host, name, &headers) {
        Ok(v) => v,
        Err(e) => return e.into_response(),
    };
    let p = match principal(&host, Some(&agent), &headers).await {
        Ok(p) => p,
        Err(e) => return e.into_response(),
    };
    let id = header(&headers, "agent-session-id")
        .unwrap_or("")
        .to_owned();
    open_event_stream(host, agent, p, id, &headers, None, None).await
}

/// The submission a streamed `POST` answers: its JSON-RPC response closes the stream.
struct Streamed {
    rpc_id: Value,
    turn_id: String,
    ordinal: i64,
}
/// The JSON-RPC response that ends a streamed run, shaped like agent-pi's: `done` with the answer's
/// text, or `unanswered` with a reason.
fn run_result(streamed: &Streamed, status: &str, text: &str) -> Value {
    let mut result = json!({"turnId": streamed.turn_id, "submissionId": streamed.ordinal});
    if status == "completed" {
        result["status"] = json!("done");
        result["text"] = json!(text);
    } else {
        result["status"] = json!("unanswered");
        result["reason"] = json!(if status == "cancelled" { "aborted" } else { "error" });
    }
    json!({"jsonrpc": "2.0", "id": streamed.rpc_id, "result": result})
}
async fn open_event_stream(
    host: Arc<Host>,
    agent: Arc<Agent>,
    p: Principal,
    id: String,
    headers: &HeaderMap,
    submission_id: Option<String>,
    streamed: Option<Streamed>,
) -> Response {
    let s = UserScope {
        tenant_id: p.tenant_id.clone(),
        agent_id: agent.name.clone(),
        user_id: p.user_id.clone(),
    };
    // Authorize ownership before examining any cursor.
    if let Some(response) = misdirected(&agent, &id) {
        return response;
    }
    let state = match agent.store.state(&s, &id).await {
        Ok(v) if v.session.status == "open" => v,
        Ok(_) => return storage(StoreError::NotFound),
        Err(e) => return storage(e),
    };
    let permit = match host.streams.clone().try_acquire_owned() {
        Ok(v) => v,
        Err(_) => return fail(StatusCode::TOO_MANY_REQUESTS, "stream capacity exceeded"),
    };
    let cursor = header(headers, "last-event-id").and_then(|v| EventId::parse(&id, v));
    // Retention removed what the cursor would resume from: start with a snapshot instead.
    let cursor = match cursor {
        Some(c)
            if agent
                .store
                .events_gone(&s, &id, c.seq)
                .await
                .unwrap_or(false) =>
        {
            None
        }
        other => other,
    };
    let sub = agent.hub.subscribe(&id, state.high_water, cursor);
    let frame =
        |event_id: String, mut payload: Value| {
            payload["eventId"] = json!(event_id);
            Ok::<_, Infallible>(Event::default().id(event_id).data(
                json!({"jsonrpc":"2.0","method":"session/event","params":payload}).to_string(),
            ))
        };
    let event_agent = agent.clone();
    let map_event = move |payload: Value| -> Option<Value> {
        let mut cur = payload;
        for plugin in &event_agent.plugins {
            cur = plugin.map_event(cur)?;
        }
        Some(cur)
    };
    let session_id_header = id.clone();
    let stream = async_stream::stream! {
        let _permit = permit;
        let Subscription { mut rx, position, high_water, partial, deltas } = sub;
        let mut last = position;
        match (cursor, deltas) {
            // Resume exactly: committed events from the store up to the hub's high-water mark,
            // merged with the deltas still in memory, in event order.
            (Some(c), Some(deltas)) => {
                let mut items: Vec<(EventId, Value)> = deltas.into_iter().map(|d| (d.id, (*d.payload).clone())).collect();
                let mut after = c.seq;
                'read: loop {
                    let page = match agent.store.events(&s, &id, after, 100).await {
                        Ok(page) => page,
                        Err(_) => return,
                    };
                    let full = page.len() == 100;
                    for event in page {
                        if event.sequence > high_water {
                            break 'read;
                        }
                        after = event.sequence;
                        items.push((EventId { seq: event.sequence, delta: 0 }, event.payload));
                    }
                    if !full {
                        break;
                    }
                }
                items.sort_by_key(|(id, _)| *id);
                for (event_id, payload) in items.into_iter().filter(|(e, _)| *e > c) {
                    if let Some(mapped) = map_event(payload) {
                        yield frame(event_id.render(&id), mapped);
                    }
                }
                last = position.max(c);
            }
            // A fresh stream, an unknown cursor, or one whose deltas are gone: start from a snapshot.
            _ => {
                let busy = agent.store.state(&s, &id).await.map(|st| st.busy).unwrap_or(state.busy);
                let tasks = agent.store.tasks(&s, &id).await.unwrap_or_default();
                let mut payload = json!({"type":"snapshot","session":session_value(state.session),"busy":busy,"highWater":high_water,"partialText":partial.as_ref().map_or("", |p| p.text.as_str()),"inbox":[],"tasks":tasks});
                if let Some(partial) = &partial {
                    payload["partial"] = partial.value();
                }
                if let Ok(Some(flow)) = agent.store.flow(&s, &id).await {
                    payload["flow"] = flow;
                }
                yield frame(position.render(&id), payload);
            }
        }
        // A submission that settled before this stream began has no events left to wait for.
        if let Some(run) = &streamed
            && let Ok(Some((status, text))) = agent.store.settled(&s, &id, run.ordinal).await
        {
            yield Ok::<_, Infallible>(Event::default().data(run_result(run, &status, &text).to_string()));
            return;
        }
        let mut tick = tokio::time::interval(Duration::from_secs(1));
        loop {
            tokio::select! {
                received = rx.recv() => match received {
                    Ok(event) if event.id > last => {
                        last = event.id;
                        let ours = streamed.as_ref().is_some_and(|run| {
                            event.payload["type"] == "run.finished" && event.payload["turnId"] == run.turn_id.as_str()
                        });
                        if let Some(mapped) = map_event((*event.payload).clone()) {
                            yield frame(event.id.render(&id), mapped);
                        }
                        if ours
                            && let Some(run) = &streamed
                            && let Ok(Some((status, text))) = agent.store.settled(&s, &id, run.ordinal).await
                        {
                            yield Ok::<_, Infallible>(Event::default().data(run_result(run, &status, &text).to_string()));
                            break;
                        }
                    }
                    Ok(_) => {}
                    // Too slow to keep up: the client resumes from its cursor.
                    Err(_) => break,
                },
                _ = tick.tick() => {
                    if now() >= p.expires_at || !host.accepting.load(Ordering::SeqCst) {
                        break;
                    }
                    // The run's own event can be missed by a stream that lagged; the store knows.
                    if let Some(run) = &streamed
                        && let Ok(Some((status, text))) = agent.store.settled(&s, &id, run.ordinal).await
                    {
                        yield Ok::<_, Infallible>(Event::default().data(run_result(run, &status, &text).to_string()));
                        break;
                    }
                }
            }
        }
    };
    let mut resp = Sse::new(stream)
        .keep_alive(KeepAlive::new().interval(Duration::from_secs(10)))
        .into_response();
    if !session_id_header.is_empty()
        && let Ok(v) = HeaderValue::from_str(&session_id_header)
    {
        resp.headers_mut().insert("agent-session-id", v);
    }
    if let Some(sub_id) = submission_id
        && let Ok(v) = HeaderValue::from_str(&sub_id)
    {
        resp.headers_mut().insert("agent-submission-id", v);
    }
    resp
}
async fn manifest_default(State(host): State<Arc<Host>>, headers: HeaderMap) -> Response {
    manifest(host, None, headers)
}
async fn manifest_named(
    State(host): State<Arc<Host>>,
    Path(name): Path<String>,
    headers: HeaderMap,
) -> Response {
    manifest(host, Some(name), headers)
}
fn manifest(host: Arc<Host>, name: Option<String>, headers: HeaderMap) -> Response {
    let agent = match endpoint(&host, name, &headers) {
        Ok(v) => v,
        Err(e) => return e.into_response(),
    };
    let profiles: Vec<&String> = agent.config.profiles.keys().collect();
    // An agent that takes a tenant's customization says so, with the model entries a tenant
    // may pick among, which t10-agent-control checks against what it allows.
    let mut capabilities: Vec<&str> = CAPABILITIES.to_vec();
    if agent.tenant_config.is_some() {
        capabilities.push("tenant_config");
    }
    let models: Vec<&String> = agent.config.models.keys().collect();
    Json(json!({
        "contract":0,"harness":"ti","harness_version":env!("CARGO_PKG_VERSION"),"pool":agent.name,"agent":agent.name,
        "bundle":agent.version,"profiles":profiles,"capabilities":capabilities,"models":models,"builtin_tools":[],"methods":[]
    }))
    .into_response()
}
async fn ready(State(host): State<Arc<Host>>) -> Response {
    let mut healthy = host.accepting.load(Ordering::SeqCst);
    for agent in host.agents.values() {
        healthy &= agent.accepting.load(Ordering::SeqCst) && agent.store.healthy().await.is_ok();
    }
    if healthy {
        StatusCode::OK.into_response()
    } else {
        StatusCode::SERVICE_UNAVAILABLE.into_response()
    }
}
pub fn router(host: Arc<Host>) -> Router {
    let limit = host.config.limits.max_body_bytes;
    Router::new()
        .route("/healthz", get(|| async { StatusCode::OK }))
        .route("/readyz", get(ready))
        .route("/metrics", get(metrics))
        .route("/v1/runtime", get(manifest_default))
        .route("/agents/{name}/readyz", get(ready))
        .route("/agents/{name}/v1/runtime", get(manifest_named))
        .route(
            "/agent",
            post(rpc_default).get(events_default).delete(close_default),
        )
        .route(
            "/agents/{name}/agent",
            post(rpc_named).get(events_named).delete(close_named),
        )
        .layer(DefaultBodyLimit::max(limit))
        .with_state(host)
}
/// Loads every agent and serves them until SIGTERM. Running steps get a deadline to finish; the
/// rest resume on the next start.
pub async fn start(config: HostConfig) -> Result<(), Box<dyn Error>> {
    // No overall timeout: model requests and tool calls each carry their own.
    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .connect_timeout(Duration::from_secs(5))
        .build()?;
    let loaded = agents::load_all(&config, &client).await?;
    let default_agent = config
        .default_agent
        .clone()
        .unwrap_or_else(|| loaded[0].name.clone());
    if !loaded.iter().any(|a| a.name == default_agent) {
        return Err(
            format!("defaultAgent {default_agent:?} is not one of the loaded agents").into(),
        );
    }
    for agent in &loaded {
        let n = agent
            .store
            .recover()
            .await
            .map_err(|e| format!("{}: recovery: {e:?}", agent.name))?;
        if n > 0 {
            tracing::info!(agent = %agent.name, commands = n, "resuming runs cut off by a restart");
        }
    }
    let addr = format!("{}:{}", config.host, config.port);
    let host = Arc::new(Host {
        streams: Arc::new(Semaphore::new(config.limits.max_streams)),
        requests: Arc::new(Semaphore::new(1000)),
        auth: Auth::new(config.auth.clone(), client.clone()),
        agents: loaded
            .into_iter()
            .map(|a| (a.name.clone(), Arc::new(a)))
            .collect(),
        default_agent,
        config,
        client,
        accepting: AtomicBool::new(true),
        scheduler: Scheduler::default(),
        metrics: Default::default(),
    });
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    for agent in host.agents.values() {
        let host_info = agent_ti_plugin_sdk::HostInfo {
            storage_dir: &host.config.storage_dir,
        };
        for plugin in &agent.plugins {
            plugin
                .setup(&host_info)
                .await
                .map_err(|e| format!("{}: plugin {} setup: {e}", agent.name, plugin.name()))?;
        }
        agent.start_mcp_listeners(&host.client);
        agent.start_tenant_skills_watcher(shutdown_rx.clone());
        if agent.has_background_tasks() {
            tokio::spawn(crate::tasks::supervise(host.clone(), agent.clone()));
        }
        let retention = &host.config.retention;
        if retention.events_days.is_some() || retention.closed_sessions_days.is_some() {
            tokio::spawn(crate::maintenance::retain(agent.clone(), retention.clone()));
        }
        if let Some(backup) = &host.config.backup {
            tokio::spawn(crate::maintenance::back_up(agent.clone(), backup.clone()));
        }
    }
    let scheduler = tokio::spawn(worker::schedule(host.clone()));
    let sweeper = host.clone();
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(Duration::from_secs(10));
        loop {
            tick.tick().await;
            for agent in sweeper.agents.values() {
                agent.hub.sweep();
            }
        }
    });
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    tracing::info!(%addr, agents = ?host.agents.keys().collect::<Vec<_>>(), default = %host.default_agent, "agent-ti listening");
    let shutdown_host = host.clone();
    let shutdown_signal = shutdown_tx.clone();
    axum::serve(listener, router(host.clone()))
        .with_graceful_shutdown(async move {
            #[cfg(unix)]
            {
                let mut term =
                    tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                        .expect("SIGTERM handler");
                tokio::select! {_=tokio::signal::ctrl_c()=>{},_=term.recv()=>{}}
            }
            #[cfg(not(unix))]
            {
                let _ = tokio::signal::ctrl_c().await;
            }
            shutdown_host.accepting.store(false, Ordering::SeqCst);
            let _ = shutdown_signal.send(true);
            shutdown_host.scheduler.wake();
        })
        .await?;
    host.accepting.store(false, Ordering::SeqCst);
    let _ = shutdown_tx.send(true);
    host.scheduler.wake();
    scheduler.await?;
    for agent in host.agents.values() {
        for plugin in agent.plugins.iter().rev() {
            let _ = plugin.dispose().await;
        }
    }
    drop(host);
    Ok(())
}
