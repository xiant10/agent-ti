//! The scheduler, in memory and spanning every agent's store, and the session worker: the
//! model/tool loop with each external call journaled as a step.
use crate::{
    adapters,
    agents::{Agent, ToolEntry},
    compaction,
    flow::FlowRunTrace,
    server::Host,
};
use agent_ti_core::*;
use agent_ti_plugin_sdk::{RequestContext, RunInfo};
use agent_ti_store::*;
use serde_json::{Value, json};
use std::{
    collections::HashMap,
    sync::{
        Arc, Mutex, RwLock,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};
use tokio::{sync::Notify, task::JoinSet};
use tokio_util::sync::CancellationToken;

/// Running work, counted for the process, tenant and user limits, and the cancellation token of
/// each running command.
#[derive(Default)]
struct Running {
    total: usize,
    tenants: HashMap<String, usize>,
    users: HashMap<(String, String, String), usize>,
    /// Round-robin position of each tenant; not stored, so fairness starts afresh after a restart.
    served: HashMap<String, u64>,
    tick: u64,
    tokens: HashMap<String, CancellationToken>,
    session_tools: HashMap<(String, String, String), usize>,
    /// Background children's runs per user (`maxBackgroundRunsPerUser`).
    background: HashMap<(String, String, String), usize>,
}
/// How a command counts against run limits: a foreground child's holds none (its parent's run
/// does); a background child's counts toward process capacity and its owner's background runs.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Lease {
    Run,
    Foreground,
    Background,
}
fn lease(child: Option<&serde_json::Value>) -> Lease {
    match child {
        None => Lease::Run,
        Some(spec) if spec.get("task").is_some() => Lease::Background,
        Some(_) => Lease::Foreground,
    }
}
#[derive(Default)]
pub struct Scheduler {
    wake: Notify,
    running: Mutex<Running>,
}
impl Scheduler {
    /// New work was admitted or a run ended.
    /// Runs in progress, for metrics.
    pub fn active(&self) -> usize {
        self.running.lock().unwrap().total
    }
    pub fn wake(&self) {
        self.wake.notify_one();
    }
    pub fn set_running_tools(&self, agent: &str, tenant: &str, session: &str, count: usize) {
        let mut r = self.running.lock().unwrap();
        let key = (agent.to_string(), tenant.to_string(), session.to_string());
        if count == 0 {
            r.session_tools.remove(&key);
        } else {
            r.session_tools.insert(key, count);
        }
    }
    pub fn is_running_tools(&self, agent: &str, tenant: &str, session: &str) -> bool {
        let r = self.running.lock().unwrap();
        r.session_tools
            .get(&(agent.to_string(), tenant.to_string(), session.to_string()))
            .copied()
            .unwrap_or(0)
            > 0
    }
    /// Signals running commands whose cancel flag was just committed.
    pub fn cancel(&self, commands: &[String]) {
        let running = self.running.lock().unwrap();
        for command in commands {
            if let Some(token) = running.tokens.get(command) {
                token.cancel();
            }
        }
    }
    fn release(&self, agent: &str, w: &Work) {
        let mut r = self.running.lock().unwrap();
        let user = (
            agent.to_owned(),
            w.scope.tenant_id.clone(),
            w.scope.user_id.clone(),
        );
        if lease(w.child.as_ref()) == Lease::Background {
            r.total -= 1;
            *r.background.get_mut(&user).unwrap() -= 1;
        }
        // A foreground child's run holds no slot: its parent's does, as agent-pi's `RunTracker`.
        if w.child.is_none() {
            r.total -= 1;
            *r.tenants.get_mut(&w.scope.tenant_id).unwrap() -= 1;
            *r.users
                .get_mut(&(
                    agent.to_owned(),
                    w.scope.tenant_id.clone(),
                    w.scope.user_id.clone(),
                ))
                .unwrap() -= 1;
        }
        r.tokens.remove(&w.command_id);
        r.session_tools.remove(&(
            agent.to_string(),
            w.scope.tenant_id.clone(),
            w.session_id.clone(),
        ));
        drop(r);
        self.wake();
    }
}

fn is_child_tool(name: &str) -> bool {
    name == crate::agents::EXPERT_TOOL || name.starts_with(crate::agents::SUBAGENT_PREFIX)
}
pub(crate) fn error(e: StoreError) -> ExecutionError {
    match e {
        StoreError::Conflict => ExecutionError::Denied,
        _ => ExecutionError::Unavailable,
    }
}
fn first_sentence(text: &str) -> String {
    let limit = text.char_indices().nth(200).map_or(text.len(), |(i, _)| i);
    let slice = &text[..limit];
    let end = slice
        .find(['.', '!', '?', '。', '！', '？'])
        .map_or(limit, |i| {
            let mut char_indices = slice[i..].char_indices();
            let punc_len = char_indices.next().map_or(1, |(_, c)| c.len_utf8());
            i + punc_len
        });
    slice[..end]
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

struct Journal {
    host: Arc<Host>,
    agent: Arc<Agent>,
    work: Work,
    model_index: AtomicUsize,
    tools: Arc<RwLock<Vec<Arc<crate::agents::OfferedTool>>>>,
    streamed: Arc<Mutex<Streamed>>,
    /// Ahead of every step ID of this run's generation (`generation_prefix`).
    prefix: String,
    /// The profile's, or a child's own.
    thinking_level: String,
    /// The tenant's customization this turn pinned, which cuts every tool offered.
    config: Option<Arc<crate::tenant_config::TenantConfig>>,
}
/// The current model message: its ID, and the text streamed of it so far.
#[derive(Default)]
struct Streamed {
    message_id: Option<String>,
    text: String,
}
/// Publishes one model message's deltas to the hub, from memory.
struct LiveText<'a> {
    journal: &'a Journal,
    message_id: String,
    /// UTF-16 code units sent so far, as JavaScript clients count them.
    offset: usize,
    thinking: bool,
    started: Instant,
    first: bool,
}
impl LiveText<'_> {
    fn first_token(&mut self) {
        if std::mem::take(&mut self.first) {
            self.journal.host.metrics.observe(
                "agent_ti_first_token_seconds",
                &[("agent", &self.journal.agent.name)],
                self.started.elapsed(),
            );
        }
    }
}
impl adapters::DeltaSink for LiveText<'_> {
    fn text(&mut self, text: &str) {
        self.first_token();
        let w = &self.journal.work;
        let payload = json!({"type":"text.delta","turnId":w.turn_id,"messageId":self.message_id,"generationId":format!("{}:{}",w.command_id,w.attempt),"offset":self.offset,"text":text});
        self.offset += text.encode_utf16().count();
        self.journal.streamed.lock().unwrap().text.push_str(text);
        self.journal.agent.hub.delta(&w.session_id, payload);
    }
    fn thinking(&mut self, text: &str) {
        self.first_token();
        if self.thinking {
            let w = &self.journal.work;
            self.journal.agent.hub.delta(
                &w.session_id,
                json!({"type":"thinking.delta","turnId":w.turn_id,"text":text}),
            );
        }
    }
    fn retry(&mut self, attempt: u32, at: u64, error: &str) {
        let w = &self.journal.work;
        self.journal.agent.hub.delta(
            &w.session_id,
            json!({"type":"retry","attempt":attempt,"at":at,"error":error}),
        );
    }
}
/// `memo` for one plugin's hook, journaled as a step of the run.
struct Memo<'a> {
    journal: &'a Journal,
    plugin: &'a str,
}
impl RequestContext for Memo<'_> {
    fn user_id(&self) -> &str {
        &self.journal.work.scope.user_id
    }
    fn tenant_id(&self) -> &str {
        &self.journal.work.scope.tenant_id
    }
    fn memo<'a>(
        &'a self,
        name: &'a str,
        compute: Box<dyn FnOnce() -> Value + Send + 'a>,
    ) -> BoxFuture<'a, Result<Value, String>> {
        Box::pin(async move {
            let j = self.journal;
            let id = format!("{}memo:{}:{name}", j.prefix, self.plugin);
            let op = Operation {
                id: id.clone(),
                kind: "memo".into(),
                input: json!({}),
                unsafe_effect: false,
                event: None,
            };
            match j
                .agent
                .store
                .prepare(&j.work, op)
                .await
                .map_err(|e| format!("{e:?}"))?
            {
                Prepared::Recorded(value) => Ok(value),
                _ => {
                    let value = compute();
                    j.agent
                        .store
                        .record(&j.work, &id, value.clone(), None)
                        .await
                        .map_err(|e| format!("{e:?}"))?;
                    Ok(value)
                }
            }
        })
    }
}
impl ModelPort for Journal {
    fn generate<'a>(
        &'a self,
        request: ModelRequest,
    ) -> BoxFuture<'a, Result<ModelReply, ExecutionError>> {
        Box::pin(async move {
            let index = self.model_index.fetch_add(1, Ordering::SeqCst);
            let id = format!("{}model:{index}", self.prefix);
            let message_id = format!("{}:{index}", self.work.command_id);
            *self.streamed.lock().unwrap() = Streamed {
                message_id: Some(message_id.clone()),
                text: String::new(),
            };
            // The step's digest covers the request before plugins add their per-request messages.
            let input =
                serde_json::to_value(&request).map_err(|_| ExecutionError::InvalidResponse)?;
            let op = Operation {
                id: id.clone(),
                kind: "model".into(),
                input,
                unsafe_effect: false,
                event: None,
            };
            if let Prepared::Recorded(result) = self
                .agent
                .store
                .prepare(&self.work, op)
                .await
                .map_err(error)?
            {
                return serde_json::from_value(result).map_err(|_| ExecutionError::InvalidResponse);
            }
            let mut request = request;
            for plugin in &self.agent.plugins {
                let ctx = Memo {
                    journal: self,
                    plugin: plugin.name(),
                };
                let limit = self.hook_limit();
                request.messages = crate::plugins::within(limit, "before_model_request", plugin.before_model_request(request.messages, &ctx)).await.map_err(|e| {
                    tracing::error!(agent = %self.agent.name, plugin = plugin.name(), error = %e, "before_model_request failed");
                    ExecutionError::Denied
                })?;
            }
            let profile = &self.agent.config.profiles[&self.work.profile];
            let config = &self.agent.config.models[&request.model];
            let run_token = model_token(&self.agent, config, &self.work).await;
            let call = adapters::ModelCall {
                config,
                agent: &self.agent.name,
                thinking_level: &self.thinking_level,
                timeout: Duration::from_millis(self.agent.config.durable.stream_timeout_ms),
                run_token: run_token.as_deref(),
                retry: &self.agent.config.durable.retry,
            };
            let mut live = LiveText {
                journal: self,
                message_id: message_id.clone(),
                offset: 0,
                thinking: profile.thinking_events,
                started: Instant::now(),
                first: true,
            };
            let reply =
                adapters::model(&self.host.client, call, &self.work, request, &mut live).await?;
            for plugin in &self.agent.plugins {
                let ctx = Memo {
                    journal: self,
                    plugin: plugin.name(),
                };
                let observed = crate::plugins::within(
                    self.hook_limit(),
                    "after_model_response",
                    plugin.after_model_response(&reply.message, &ctx),
                )
                .await;
                if let Err(e) = observed {
                    tracing::warn!(agent = %self.agent.name, plugin = plugin.name(), error = %e, "after_model_response failed");
                }
            }
            let value =
                serde_json::to_value(&reply).map_err(|_| ExecutionError::InvalidResponse)?;
            // Text before tool calls ends its own message; the final one ends with the run.
            let text = &reply.message.content;
            let end = (!reply.message.tool_calls.is_empty() && !text.is_empty()).then(|| {
                json!({"type":"message.end","turnId":self.work.turn_id,"messageId":message_id,"text":text,"stopReason":"toolUse","utf16Length":text.encode_utf16().count()})
            });
            self.agent
                .store
                .record_spent(
                    &self.work,
                    &id,
                    value,
                    end,
                    (reply.input_tokens, reply.output_tokens),
                )
                .await
                .map_err(error)?;
            for (kind, tokens) in [
                ("input", reply.input_tokens),
                ("output", reply.output_tokens),
            ] {
                self.host.metrics.add(
                    "agent_ti_tokens_total",
                    &[("agent", &self.agent.name), ("kind", kind)],
                    tokens,
                );
            }
            if reply.message.tool_calls.is_empty() {
                self.move_flow(crate::flow::complete_after_answer).await;
            }
            Ok(reply)
        })
    }
}
impl Journal {
    fn hook_limit(&self) -> Duration {
        Duration::from_millis(self.host.config.limits.hook_timeout_ms)
    }
    /// Which server a tool's latency counts under.
    fn server_of<'t>(&'t self, tool: &'t ToolEntry) -> &'t str {
        if let Some(conn) = &tool.connector {
            return &conn.connector_name;
        }
        self.agent
            .config
            .mcp_servers
            .get(tool.server)
            .map_or("agent", |server| server.name.as_str())
    }
    /// Changes the session's running flow trace, if it has one. The trace is a picture of the run;
    /// failing to draw it doesn't stop the run.
    async fn move_flow(&self, change: impl FnOnce(&mut FlowRunTrace) + Send) {
        let moved = self
            .agent
            .store
            .advance_flow(&self.work.scope, &self.work.session_id, |value| {
                let mut trace: FlowRunTrace = serde_json::from_value(value).ok()?;
                change(&mut trace);
                serde_json::to_value(trace).ok()
            })
            .await;
        if let Err(e) = moved {
            tracing::warn!(session_id = %self.work.session_id, error = ?e, "flow trace not updated");
        }
    }
    /// Runs a tool of the catalog: a plugin's, a tenant connector's or an MCP server's.
    async fn call_entry(&self, tool: &ToolEntry, arguments: Value) -> ToolOutcome {
        if let Some(ref handler) = tool.handler {
            return (handler)(arguments).await;
        }
        if let Some(ref conn) = tool.connector {
            if conn.tenant_id != self.work.scope.tenant_id {
                return ToolOutcome::error(format!(
                    "Tool {} belongs to another tenant.",
                    tool.name
                ));
            }
            let Some(ref tc) = self.agent.tenant_connectors else {
                return ToolOutcome::error("Tenant connectors are not configured.");
            };
            let grant_token = match self.grant_token(&tool.name).await {
                Ok(token) => token,
                Err(outcome) => return outcome,
            };
            let srv = agent_ti_config::McpServerConfig {
                name: conn.connector_name.clone(),
                url: format!(
                    "{}/mcp/connector/{}",
                    tc.gateway_url().trim_end_matches('/'),
                    conn.connector_id
                ),
                auth: agent_ti_config::McpAuth::Grant { token_header: None },
                headers: std::collections::BTreeMap::new(),
                include: vec![],
                exclude: vec![],
                prefix: String::new(),
                max_description_chars: 1000,
                call_timeout_ms: 30_000,
                agent_context: false,
            };
            return adapters::CALL_HEADERS
                .scope(
                    self.call_headers(),
                    adapters::mcp_call(
                        &self.host.client,
                        &srv,
                        &tool.mcp_name,
                        arguments,
                        Some(&grant_token),
                        None,
                    ),
                )
                .await;
        }
        let Some(server) = self.agent.config.mcp_servers.get(tool.server) else {
            return ToolOutcome::error(format!("Tool {} not found", tool.name));
        };
        let grant_token = if let agent_ti_config::McpAuth::Grant { .. } = &server.auth {
            match self.grant_token(&tool.name).await {
                Ok(token) => Some(token),
                Err(outcome) => return outcome,
            }
        } else {
            None
        };
        let meta = if server.agent_context {
            let mut m = json!({ "turn": self.work.turn_id });
            if let Some(ref tp) = self.work.traceparent {
                m["traceparent"] = json!(tp);
            }
            Some(m)
        } else {
            None
        };
        adapters::CALL_HEADERS
            .scope(
                self.call_headers(),
                adapters::mcp_call(
                    &self.host.client,
                    server,
                    &tool.mcp_name,
                    arguments,
                    grant_token.as_deref(),
                    meta,
                ),
            )
            .await
    }
    fn call_headers(&self) -> Vec<(&'static str, String)> {
        let mut headers = vec![
            ("X-Session-ID", self.work.session_id.clone()),
            ("X-Turn-ID", self.work.turn_id.clone()),
        ];
        if let Some(ref trace) = self.work.traceparent {
            headers.push(("traceparent", trace.clone()));
        }
        headers
    }
    async fn grant_token(&self, tool: &str) -> Result<String, ToolOutcome> {
        let Some(ref broker) = self.agent.grants else {
            return Err(ToolOutcome::error(format!(
                "{tool} needs an agent grant, and this agent has none configured."
            )));
        };
        broker
            .token(&self.agent.store, &self.work.session_id)
            .await
            .map_err(|e| match e {
                crate::grants::GrantError::Stopped => ToolOutcome::error(
                    "The agent's access was stopped; end this task and tell the user.",
                ),
                crate::grants::GrantError::Expired => ToolOutcome::error(
                    "The agent's access has expired; ask the user to send a message to renew it.",
                ),
                crate::grants::GrantError::Unavailable(e) => {
                    ToolOutcome::error(format!("the auth service is unavailable: {e}"))
                }
            })
    }
    /// What `load_skill` adds for a skill's flow: a deterministic flow given a query runs now and
    /// returns what it found; any other starts a trace for the model's own tool calls to move.
    async fn run_skill_flow(
        &self,
        step: &str,
        skill: &agent_ti_config::SkillConfig,
        flow: &Value,
        query: Option<&str>,
        has_tools: bool,
    ) -> Result<String, String> {
        let flow: agent_ti_config::flow::SkillFlow =
            serde_json::from_value(flow.clone()).map_err(|e| e.to_string())?;
        if let Some(query) = query.filter(|_| flow.mode == "deterministic" && has_tools) {
            let host = SkillFlowHost {
                journal: self,
                step,
            };
            let result =
                crate::flow::execute_flow(&flow, query, crate::flow::language_of(query), &host)
                    .await?;
            return Ok(match (result.immediate_reply, result.evidence) {
                (Some(reply), _) if result.status == "needs_input" => {
                    format!("\n\n<flow_needs_input>\n{reply}\n</flow_needs_input>")
                }
                (_, Some(evidence)) => format!("\n\n{evidence}"),
                _ => String::new(),
            });
        }
        let trace = crate::flow::guided_trace(&flow, &skill.name);
        self.save_flow(trace).await;
        Ok(String::new())
    }
    async fn save_flow(&self, trace: FlowRunTrace) {
        let saved = match serde_json::to_value(trace) {
            Ok(value) => {
                self.agent
                    .store
                    .set_flow(&self.work.scope, &self.work.session_id, value)
                    .await
            }
            Err(_) => Err(agent_ti_store::StoreError::Invalid),
        };
        if let Err(e) = saved {
            tracing::warn!(session_id = %self.work.session_id, error = ?e, "flow trace not saved");
        }
    }
    /// A flow's tool, by the name the flow gives it: the catalog's name, or an MCP tool's on any
    /// server. Only what this tenant may use is found.
    async fn flow_tool(&self, name: &str) -> Option<Arc<crate::agents::OfferedTool>> {
        let tenant = &self.work.scope.tenant_id;
        let names = self
            .agent
            .match_tools(&[name.to_string(), format!("*:{name}")], tenant)
            .await;
        let found = names.iter().find(|n| *n == name).or(names.first())?.clone();
        let profile = &self.agent.config.profiles[&self.work.profile];
        self.agent
            .offered(tenant, profile, &[found], self.config.as_deref())
            .await
            .into_iter()
            .next()
    }
    async fn execute_load_skill(&self, step: &str, call: &ToolCall) -> ToolOutcome {
        let name = match call.arguments.get("name").and_then(Value::as_str) {
            Some(n) if !n.is_empty() => n,
            _ => {
                return ToolOutcome::error(
                    "load_skill requires a non-empty string `name` parameter.",
                );
            }
        };
        let file = call.arguments.get("file").and_then(Value::as_str);

        let skills = match self.agent.turn_pins.get(&self.work.session_id).await {
            Some(s) => s,
            None => {
                if let Some(ref ts) = self.agent.tenant_skills {
                    ts.for_tenant(Some(&self.work.scope.tenant_id)).await
                } else {
                    Arc::new(self.agent.config.skills.clone())
                }
            }
        };

        let skill = match skills.iter().find(|s| s.name == name) {
            Some(s) => s,
            None => {
                let available = skills
                    .iter()
                    .map(|s| s.name.as_str())
                    .collect::<Vec<_>>()
                    .join(", ");
                let list = if available.is_empty() {
                    "none"
                } else {
                    &available
                };
                return ToolOutcome::error(format!("Unknown skill {name:?}. Available: {list}"));
            }
        };

        if let Some(f) = file {
            return match agent_ti_config::read_skill_file(skill, f) {
                Ok(text) => ToolOutcome::ok(text),
                Err(e) => ToolOutcome::error(e),
            };
        }

        let tenant = &self.work.scope.tenant_id;
        let matched_names = self.agent.match_tools(&skill.tools, tenant).await;
        let profile = &self.agent.config.profiles[&self.work.profile];
        let pinned = self.agent.pinned_tools(tenant, profile).await;
        let policy = agent_ti_store::ActivationPolicy {
            pinned,
            max_active: profile.max_active_tools as usize,
        };

        if let Err(e) = self
            .agent
            .store
            .activate_skill(
                &self.work.scope,
                &self.work.session_id,
                &skill.name,
                &matched_names,
                &policy,
            )
            .await
        {
            return ToolOutcome::error(format!("Failed to activate skill: {e:?}"));
        }

        let active = self
            .agent
            .store
            .get_session_tools(&self.work.scope, &self.work.session_id)
            .await
            .unwrap_or_default();
        *self.tools.write().unwrap() = self
            .agent
            .offered(
                &self.work.scope.tenant_id,
                profile,
                &active.offered,
                self.config.as_deref(),
            )
            .await;

        // A tool the tenant turned off is activated with the skill, and never offered.
        let matched_names: Vec<String> = {
            let offered = self.tools.read().unwrap();
            matched_names
                .into_iter()
                .filter(|n| offered.iter().any(|t| &t.name == n))
                .collect()
        };
        let footer = if matched_names.is_empty() && !skill.tools.is_empty() {
            "\n\n(The tools this skill uses are unavailable right now; they are enabled automatically once their server connects.)".to_string()
        } else if !matched_names.is_empty() {
            format!(
                "\n\nTools enabled for this skill: {}",
                matched_names.join(", ")
            )
        } else {
            String::new()
        };

        let files_appendix = if !skill.files.is_empty() {
            let file_names = skill.files.keys().cloned().collect::<Vec<_>>();
            format!(
                "\n\nReference files (read one with load_skill and file): {}",
                file_names.join(", ")
            )
        } else {
            String::new()
        };

        let flow_appendix = match &skill.flow {
            Some(flow) => {
                let query = call.arguments.get("query").and_then(Value::as_str);
                let has_tools = !matched_names.is_empty();
                match self
                    .run_skill_flow(step, skill, flow, query, has_tools)
                    .await
                {
                    Ok(appendix) => appendix,
                    Err(e) => return ToolOutcome::error(e),
                }
            }
            None => String::new(),
        };

        let content = format!("{}{footer}{files_appendix}{flow_appendix}", skill.body);
        // A flow can bring back several tools' results at once; the journal holds a bounded step.
        if content.len() > agent_ti_core::MAX_TOOL_RESULT_BYTES {
            return ToolOutcome::error(format!(
                "Skill {name}'s flow found more than {} KiB. Use the skill's tools directly, with narrower queries.",
                agent_ti_core::MAX_TOOL_RESULT_BYTES / 1024
            ));
        }
        ToolOutcome::ok(content)
    }

    async fn execute_search_tools(&self, call: &ToolCall) -> ToolOutcome {
        let query = match call.arguments.get("query").and_then(Value::as_str) {
            Some(q) if !q.is_empty() => q,
            _ => {
                return ToolOutcome::error(
                    "search_tools requires a non-empty string `query` parameter.",
                );
            }
        };
        let limit = call
            .arguments
            .get("limit")
            .and_then(Value::as_u64)
            .unwrap_or(5)
            .clamp(1, 20) as usize;

        let hits = self
            .agent
            .search_tools(
                query,
                limit,
                &self.work.scope.tenant_id,
                &self.agent.config.profiles[&self.work.profile],
                self.config.as_deref(),
            )
            .await;
        if hits.is_empty() {
            return ToolOutcome::ok("No tools matched. Try other words.");
        }

        let names: Vec<String> = hits.iter().map(|h| h.tool.name.clone()).collect();
        let lines: Vec<String> = hits
            .iter()
            .map(|h| format!("- {}: {}", h.tool.name, first_sentence(&h.tool.description)))
            .collect();
        let reply_text = format!(
            "Enabled these tools; call them directly:\n{}",
            lines.join("\n")
        );

        let tenant = &self.work.scope.tenant_id;
        let profile = &self.agent.config.profiles[&self.work.profile];
        let pinned = self.agent.pinned_tools(tenant, profile).await;
        let policy = agent_ti_store::ActivationPolicy {
            pinned,
            max_active: profile.max_active_tools as usize,
        };

        if let Err(e) = self
            .agent
            .store
            .activate_tools(&self.work.scope, &self.work.session_id, &names, &policy)
            .await
        {
            return ToolOutcome::error(format!("Failed to activate tools: {e:?}"));
        }

        let active = self
            .agent
            .store
            .get_session_tools(&self.work.scope, &self.work.session_id)
            .await
            .unwrap_or_default();
        *self.tools.write().unwrap() = self
            .agent
            .offered(
                &self.work.scope.tenant_id,
                profile,
                &active.offered,
                self.config.as_deref(),
            )
            .await;

        ToolOutcome::ok(reply_text)
    }
}

impl ToolPort for Journal {
    fn invoke<'a>(&'a self, call: ToolCall) -> BoxFuture<'a, Result<ToolOutcome, ExecutionError>> {
        Box::pin(async move {
            let tool = self
                .tools
                .read()
                .unwrap()
                .iter()
                .find(|t| t.name == call.name)
                .cloned();
            // Model call IDs repeat across rounds with some providers, so the step ID names the round.
            let round = self.model_index.load(Ordering::SeqCst).saturating_sub(1);
            let id = format!("{}tool:{round}:{}", self.prefix, call.id);
            let call_id = call.id.clone();
            let call_name = call.name.clone();
            let call_name_for_flow = call.name.clone();
            let turn_id = self.work.turn_id.clone();
            let finished = move |o: &ToolOutcome| {
                Some(
                    json!({"type":"tool.finished","turnId":turn_id,"callId":call_id,"name":call_name,"isError":o.is_error}),
                )
            };
            // A child's tool is safe to rerun: it finds the same child and the same submission.
            let unsafe_effect = tool.as_ref().is_some_and(|t| {
                t.name != "load_skill"
                    && t.name != "search_tools"
                    && !is_child_tool(&t.name)
                    && !crate::tasks::is_task_tool(&t.name)
            });
            let op = Operation {
                id: id.clone(),
                kind: "tool".into(),
                input: serde_json::to_value(&call).map_err(|_| ExecutionError::InvalidResponse)?,
                unsafe_effect,
                event: Some(
                    json!({"type":"tool.started","turnId":self.work.turn_id,"callId":call.id,"name":call.name}),
                ),
            };
            let outcome = match self
                .agent
                .store
                .prepare(&self.work, op)
                .await
                .map_err(error)?
            {
                Prepared::Recorded(result) => {
                    return serde_json::from_value(result)
                        .map_err(|_| ExecutionError::InvalidResponse);
                }
                Prepared::Interrupted => ToolOutcome::error(
                    "interrupted: this call was cut off by a restart and was not repeated. It may or may not have taken effect; check before trying again.",
                ),
                Prepared::Run => {
                    self.host.scheduler.set_running_tools(
                        &self.agent.name,
                        &self.work.scope.tenant_id,
                        &self.work.session_id,
                        1,
                    );
                    let mut call = call;
                    for plugin in &self.agent.plugins {
                        let ctx = Memo {
                            journal: self,
                            plugin: plugin.name(),
                        };
                        let decision = crate::plugins::within(
                            self.hook_limit(),
                            "before_tool_call",
                            plugin.before_tool_call(&call, &ctx),
                        )
                        .await
                        // A security hook that can't answer refuses.
                        .unwrap_or_else(|e| {
                            tracing::error!(agent = %self.agent.name, plugin = plugin.name(), error = %e, "before_tool_call failed; refusing the call");
                            Some(agent_ti_plugin_sdk::ToolCallDecision::Block(format!(
                                "{} refused this call: its check failed.",
                                plugin.name()
                            )))
                        });
                        match decision {
                            Some(agent_ti_plugin_sdk::ToolCallDecision::Block(reason)) => {
                                let outcome = ToolOutcome::error(reason);
                                self.agent
                                    .store
                                    .record(
                                        &self.work,
                                        &id,
                                        serde_json::to_value(&outcome).unwrap(),
                                        finished(&outcome),
                                    )
                                    .await
                                    .map_err(error)?;
                                return Ok(outcome);
                            }
                            Some(agent_ti_plugin_sdk::ToolCallDecision::Arguments(new_args)) => {
                                call.arguments = new_args;
                            }
                            None => {}
                        }
                    }
                    let Some(tool) = tool else {
                        let outcome = ToolOutcome::error(format!(
                            "Unknown tool {:?}. Use one of the tools offered.",
                            call.name
                        ));
                        self.agent
                            .store
                            .record(
                                &self.work,
                                &id,
                                serde_json::to_value(&outcome).unwrap(),
                                finished(&outcome),
                            )
                            .await
                            .map_err(error)?;
                        return Ok(outcome);
                    };
                    if !tool.validator.is_valid(&call.arguments) {
                        let problems: Vec<String> = tool
                            .validator
                            .iter_errors(&call.arguments)
                            .take(5)
                            .map(|e| e.to_string())
                            .collect();
                        let outcome = ToolOutcome::error(format!(
                            "Invalid arguments for {}: {}",
                            call.name,
                            problems.join("; ")
                        ));
                        self.agent
                            .store
                            .record(
                                &self.work,
                                &id,
                                serde_json::to_value(&outcome).unwrap(),
                                finished(&outcome),
                            )
                            .await
                            .map_err(error)?;
                        return Ok(outcome);
                    }
                    let tool_started = Instant::now();
                    let mut outcome = if tool.name == "load_skill" {
                        self.execute_load_skill(&id, &call).await
                    } else if tool.name == "search_tools" {
                        self.execute_search_tools(&call).await
                    } else if crate::tasks::is_task_tool(&tool.name) && tool.handler.is_none() {
                        crate::tasks::run_tool(&self.host, &self.agent, &self.work, &id, &call)
                            .await
                    } else if is_child_tool(&tool.name) && tool.handler.is_none() {
                        crate::children::run_child(
                            self.host.clone(),
                            self.agent.clone(),
                            &self.work,
                            &id,
                            &call,
                        )
                        .await
                    } else {
                        self.call_entry(&tool, call.arguments.clone()).await
                    };
                    self.host.metrics.observe(
                        "agent_ti_tool_seconds",
                        &[
                            ("agent", &self.agent.name),
                            ("server", self.server_of(&tool)),
                        ],
                        tool_started.elapsed(),
                    );
                    for plugin in &self.agent.plugins {
                        outcome = plugin.after_tool_call(&call, outcome);
                    }
                    outcome
                }
            };
            let value =
                serde_json::to_value(&outcome).map_err(|_| ExecutionError::InvalidResponse)?;
            self.host.scheduler.set_running_tools(
                &self.agent.name,
                &self.work.scope.tenant_id,
                &self.work.session_id,
                0,
            );
            self.agent
                .store
                .record(&self.work, &id, value, finished(&outcome))
                .await
                .map_err(error)?;
            if call_name_for_flow != "load_skill" {
                self.move_flow(|trace| {
                    crate::flow::advance_after_tool(trace, &call_name_for_flow, outcome.is_error)
                })
                .await;
            }
            Ok(outcome)
        })
    }
}

/// A flow's tool calls, each its own journaled step: after a crash, a call that finished is reused,
/// and an unsafe one that was cut off comes back as `interrupted` rather than running again.
struct SkillFlowHost<'a> {
    journal: &'a Journal,
    step: &'a str,
}

impl crate::flow::FlowHost for SkillFlowHost<'_> {
    fn call_tool(
        &self,
        step: String,
        name: String,
        args: Value,
    ) -> BoxFuture<'_, crate::flow::ToolCallOutcome> {
        Box::pin(async move {
            let outcome = self.journaled(&step, &name, args).await;
            crate::flow::ToolCallOutcome {
                output: outcome.content,
                is_error: outcome.is_error,
            }
        })
    }
    fn update(&self, trace: FlowRunTrace) -> BoxFuture<'_, ()> {
        Box::pin(self.journal.save_flow(trace))
    }
}

impl SkillFlowHost<'_> {
    async fn journaled(&self, step: &str, name: &str, args: Value) -> ToolOutcome {
        let journal = self.journal;
        let id = format!("{}:flow:{step}", self.step);
        let op = Operation {
            id: id.clone(),
            kind: "tool".into(),
            input: json!({"name": name, "arguments": args}),
            unsafe_effect: true,
            event: None,
        };
        let outcome = match journal.agent.store.prepare(&journal.work, op).await {
            Err(e) => return ToolOutcome::error(format!("{name}: {e:?}")),
            Ok(Prepared::Recorded(result)) => {
                return serde_json::from_value(result).unwrap_or_else(|_| {
                    ToolOutcome::error(format!("{name}: the recorded result can't be read"))
                });
            }
            Ok(Prepared::Interrupted) => ToolOutcome::error(
                "interrupted: this call was cut off by a restart and was not repeated. It may or may not have taken effect.",
            ),
            Ok(Prepared::Run) => match journal.flow_tool(name).await {
                Some(tool) => journal.call_entry(&tool, args).await,
                None => ToolOutcome::error(format!("Tool {name} not found")),
            },
        };
        let outcome = if outcome.content.len() > agent_ti_core::MAX_TOOL_RESULT_BYTES {
            ToolOutcome::error(format!(
                "{name} returned more than {} KiB.",
                agent_ti_core::MAX_TOOL_RESULT_BYTES / 1024
            ))
        } else {
            outcome
        };
        let value = serde_json::to_value(&outcome).unwrap_or(Value::Null);
        if let Err(e) = journal
            .agent
            .store
            .record(&journal.work, &id, value, None)
            .await
        {
            return ToolOutcome::error(format!("{name}: {e:?}"));
        }
        outcome
    }
}

impl ExecutionHooks for Journal {
    fn on_final_answer<'a>(
        &'a self,
        answer: &'a Message,
    ) -> BoxFuture<'a, Result<Option<String>, ExecutionError>> {
        Box::pin(async move {
            for plugin in &self.agent.plugins {
                let ctx = Memo {
                    journal: self,
                    plugin: plugin.name(),
                };
                match crate::plugins::within(
                    self.hook_limit(),
                    "on_final_answer",
                    plugin.on_final_answer(answer, &ctx),
                )
                .await
                {
                    Ok(Some(continuation)) => return Ok(Some(continuation)),
                    Err(e) => {
                        tracing::error!(agent = %self.agent.name, plugin = plugin.name(), error = %e, "on_final_answer failed");
                        return Err(ExecutionError::Denied);
                    }
                    _ => {}
                }
            }
            Ok(None)
        })
    }
    fn post_tools(&self) -> BoxFuture<'_, Result<Vec<Message>, ExecutionError>> {
        Box::pin(async move {
            self.agent
                .store
                .claim_steered(&self.work.scope, &self.work.session_id)
                .await
                .map_err(error)
        })
    }
    fn current_tools(&self) -> BoxFuture<'_, Result<Option<Vec<ToolDescriptor>>, ExecutionError>> {
        Box::pin(async move {
            let tools = self.tools.read().unwrap();
            let descriptors = tools
                .iter()
                .map(|t| ToolDescriptor {
                    name: t.name.clone(),
                    description: t.description.clone(),
                    parameters: t.schema.clone(),
                })
                .collect();
            Ok(Some(descriptors))
        })
    }
}

/// Step IDs of a run's second generation, after an overflow compacted it, so they don't meet the
/// first one's recorded requests.
fn generation_prefix(generation: u32) -> String {
    if generation == 0 {
        String::new()
    } else {
        format!("g{generation}:")
    }
}
/// The head of the summary a new run starts from, compacting first as pi-durable does: over the
/// blocking threshold the run waits for its summary, and fails if making it fails (one that finds
/// nothing to compact goes ahead); over the background threshold a summary is made for later runs.
/// With the history it read, when the run starts from the summary that history began with.
async fn start_compacted(
    host: &Arc<Host>,
    agent: &Arc<Agent>,
    work: &Work,
    instructions: &[String],
    model_name: &str,
) -> Result<(Option<i64>, Option<Vec<Message>>), ExecutionError> {
    let model = &agent.config.models[model_name];
    let (summary, messages) = agent
        .store
        .compaction_source(&work.scope, &work.session_id)
        .await
        .map_err(error)?;
    let read_head = summary.as_ref().map(|s| s.head);
    let mut history = Vec::with_capacity(messages.len() + 1);
    if let Some(summary) = summary {
        history.push(Message::text("user", summary.message()));
    }
    history.extend(messages.into_iter().map(|(_, m)| m));
    let tokens = compaction::context_tokens(instructions, &history);
    let window = model.context_window.unwrap_or(128_000);
    match compaction::threshold(tokens, window, &agent.config.durable.compaction) {
        Some(compaction::Need::Blocking) => {
            compaction::compact(&host.client, agent, work, "threshold").await?;
        }
        Some(compaction::Need::Background) => {
            compaction::background(host.client.clone(), agent.clone(), work.clone())
        }
        None => {}
    }
    let head = compaction_head(agent, work).await?;
    Ok((head, (head == read_head).then_some(history)))
}
async fn compaction_head(agent: &Agent, work: &Work) -> Result<Option<i64>, ExecutionError> {
    agent
        .store
        .compaction_head(&work.scope, &work.session_id)
        .await
        .map_err(error)
}
async fn run(
    host: Arc<Host>,
    agent: Arc<Agent>,
    work: Work,
    streamed: Arc<Mutex<Streamed>>,
    generation: u32,
) -> Result<TurnResult, ExecutionError> {
    let profile = agent
        .config
        .profiles
        .get(&work.profile)
        .ok_or(ExecutionError::Denied)?;
    let child = match &work.child {
        Some(spec) => Some(crate::children::ChildRun::of(&agent, profile, spec)?),
        None => None,
    };
    let stored = agent
        .store
        .state(&work.scope, &work.session_id)
        .await
        .map_err(error)?;
    let model_name = match &child {
        Some(c) => c.model.clone(),
        None => agent.session_model(profile, &stored.session.origin),
    };
    let model = &agent.config.models[&model_name];
    // A child's event stream never speaks.
    if profile.speech && child.is_none() {
        agent.hub.speak(&work.session_id, stored.high_water);
    }
    let config = agent
        .turn_config(
            &work.session_id,
            stored.session.parent.as_deref(),
            &work.scope.tenant_id,
            &work.scope.user_id,
        )
        .await;
    let session_skills = match agent.turn_pins.get(&work.session_id).await {
        Some(s) => s,
        None => {
            if let Some(ref ts) = agent.tenant_skills {
                ts.for_tenant(Some(&work.scope.tenant_id)).await
            } else {
                Arc::new(agent.config.skills.clone())
            }
        }
    };
    let instructions = match &child {
        Some(c) => agent.render_child_instructions(
            &work.profile,
            &session_skills,
            c.instructions.as_deref(),
            work.child.as_ref().is_some_and(|s| s.get("task").is_some()),
            config.as_deref(),
        ),
        None => agent.render_instructions(&work.profile, &session_skills, config.as_deref()),
    };
    let spec = ExecutionSpec {
        instructions,
        model: model_name.clone(),
        max_output_tokens: model.max_output_tokens.unwrap_or(4096).min(u32::MAX as u64) as u32,
        max_tool_rounds: agent.config.limits.max_tool_rounds,
        max_parallel_tools: host.config.limits.max_parallel_tools,
        // A child works for its parent, so it always answers in text.
        end_turn_tools: if child.is_some() {
            vec![]
        } else {
            profile.end_turn_tools.clone()
        },
        first_tool: if child.is_some() {
            None
        } else {
            profile.first_tool.clone()
        },
    };
    // Which summary the run starts from is a step of its own, so a replay after a restart starts
    // from the same history even when a newer summary has been placed since.
    let prefix = generation_prefix(generation);
    let context_step = Operation {
        id: format!("{prefix}context"),
        kind: "context".into(),
        input: json!({}),
        unsafe_effect: false,
        event: None,
    };
    let (head, read) = match agent
        .store
        .prepare(&work, context_step.clone())
        .await
        .map_err(error)?
    {
        Prepared::Recorded(value) => (value["head"].as_i64(), None),
        _ => {
            let (head, read) =
                start_compacted(&host, &agent, &work, &spec.instructions, &model_name).await?;
            agent
                .store
                .record(&work, &context_step.id, json!({ "head": head }), None)
                .await
                .map_err(error)?;
            (head, read)
        }
    };
    let history = match read {
        Some(history) => history,
        None => agent.store.history_at(&work, head).await.map_err(error)?,
    };
    agent.refresh_tools_later(&host.client);
    let session_tools = agent
        .store
        .get_session_tools(&work.scope, &work.session_id)
        .await
        .unwrap_or_default();
    // A child's starting set was fixed when it was made; empty means no tools.
    let offered_names = if session_tools.offered.is_empty() && child.is_none() {
        agent.pinned_tools(&work.scope.tenant_id, profile).await
    } else {
        session_tools.offered
    };
    let offered_list = agent
        .offered(
            &work.scope.tenant_id,
            profile,
            &offered_names,
            config.as_deref(),
        )
        .await;
    let descriptors = offered_list
        .iter()
        .map(|t| ToolDescriptor {
            name: t.name.clone(),
            description: t.description.clone(),
            parameters: t.schema.clone(),
        })
        .collect();
    let tools = Arc::new(RwLock::new(offered_list));
    // A conservative byte budget until a provider supplies a tokenizer: three bytes per token.
    let budget = (model.context_window.unwrap_or(128_000) as usize)
        .saturating_mul(3)
        .min(4 * 1024 * 1024);
    let journal = Journal {
        host: host.clone(),
        agent: agent.clone(),
        work,
        model_index: AtomicUsize::new(0),
        tools,
        streamed,
        prefix,
        thinking_level: child
            .as_ref()
            .map_or(&profile.thinking_level, |c| &c.thinking_level)
            .clone(),
        config,
    };
    execute_with_hooks(
        &spec,
        history,
        descriptors,
        &journal,
        &journal,
        &journal,
        budget,
    )
    .await
}
/// A run whose request the provider refuses as too long compacts and runs once more, as
/// pi-durable retries an overflow once after compacting.
async fn run_compacting(
    host: Arc<Host>,
    agent: Arc<Agent>,
    work: Work,
    streamed: Arc<Mutex<Streamed>>,
) -> Result<TurnResult, ExecutionError> {
    let result = run(
        host.clone(),
        agent.clone(),
        work.clone(),
        streamed.clone(),
        0,
    )
    .await;
    match result {
        Err(ExecutionError::ContextOverflow | ExecutionError::ContextLimit)
            if agent.config.durable.compaction.enabled =>
        {
            let context_step = Operation {
                id: "context".into(),
                kind: "context".into(),
                input: json!({}),
                unsafe_effect: false,
                event: None,
            };
            let started = match agent
                .store
                .prepare(&work, context_step)
                .await
                .map_err(error)?
            {
                Prepared::Recorded(value) => value["head"].as_i64(),
                _ => None,
            };
            compaction::compact(&host.client, &agent, &work, "overflow").await?;
            // A background compaction may have placed the summary first; either way the retry
            // goes ahead only if there is a summary the first generation didn't start from.
            if compaction_head(&agent, &work).await? != started {
                run(host, agent, work, streamed, 1).await
            } else {
                result
            }
        }
        result => result,
    }
}
async fn process(host: Arc<Host>, agent: Arc<Agent>, work: Work, token: CancellationToken) {
    let run_started = Instant::now();
    let done = |status: &str, messages: Vec<Message>, error: Option<String>, tokens: (u64, u64)| {
        Completion {
            status: status.into(),
            messages,
            error,
            input_tokens: tokens.0,
            output_tokens: tokens.1,
            message_id: None,
            partial: None,
        }
    };
    let completion = if work.cancel_requested {
        done("cancelled", vec![], None, (0, 0))
    } else if let Some(reply) = &work.reply {
        let answer = Message {
            source: Some(reply.source.clone()),
            ..Message::text("assistant", &reply.text)
        };
        done("completed", vec![answer], None, (0, 0))
    } else {
        let streamed = Arc::new(Mutex::new(Streamed::default()));
        let ack = crate::progress::Ack::begin(&host, &agent, &work);
        let mut completion = tokio::select! {
            result = run_compacting(host.clone(), agent.clone(), work.clone(), streamed.clone()) => match result {
                Ok(r) => done("completed", r.messages, None, (r.input_tokens, r.output_tokens)),
                Err(e) => {
                    tracing::warn!(agent = %agent.name, session_id = %work.session_id, error = ?e, "run failed");
                    done("failed", vec![], Some(format!("{e:?}")), (0, 0))
                }
            },
            _ = token.cancelled() => done("cancelled", vec![], None, (0, 0)),
        };
        if let Some(ack) = ack {
            let (input, output) = ack.finish().await;
            completion.input_tokens += input;
            completion.output_tokens += output;
        }
        let streamed = std::mem::take(&mut *streamed.lock().unwrap());
        completion.message_id = streamed.message_id;
        completion.partial = Some(streamed.text);
        completion
    };
    let text = completion
        .messages
        .last()
        .filter(|m| m.role == "assistant")
        .map(|m| m.content.clone())
        .unwrap_or_default();
    let status = completion.status.clone();
    let reason = completion.error.clone();
    let metrics = &host.metrics;
    metrics.add(
        "agent_ti_runs_total",
        &[("agent", &agent.name), ("status", &status)],
        1,
    );
    metrics.observe(
        "agent_ti_run_seconds",
        &[("agent", &agent.name)],
        run_started.elapsed(),
    );
    match agent.store.finish(&work, completion).await {
        Ok(()) => {
            let answered_by = work.reply.as_ref().map_or("model", |r| r.source.as_str());
            let info = RunInfo {
                agent: &agent.name,
                session_id: &work.session_id,
                turn_id: &work.turn_id,
                tenant_id: &work.scope.tenant_id,
                user_id: &work.scope.user_id,
                profile: &work.profile,
                answered_by,
                status: &status,
                reason: reason.as_deref(),
                text: &text,
            };
            // As agent-pi, `after_run` is for the user's runs, not a child's.
            if work.child.is_none() {
                for plugin in &agent.plugins {
                    plugin.after_run(&info);
                }
            }
        }
        Err(e) => {
            tracing::error!(agent = %agent.name, error = ?e, "terminal checkpoint failed; this agent stops admitting work");
            agent.accepting.store(false, Ordering::SeqCst);
        }
    }
    host.scheduler.release(&agent.name, &work);
}

/// Claims the oldest runnable command of the least recently served tenant, within the process,
/// tenant and user run limits, until nothing more can start; then waits to be woken. A periodic
/// scan catches anything a wake missed.
pub async fn schedule(host: Arc<Host>) {
    let mut tasks = JoinSet::new();
    let limits = &host.config.limits;
    while host.accepting.load(Ordering::SeqCst) {
        while tasks.try_join_next().is_some() {}
        let mut best: Option<(Arc<Agent>, Candidate, u64)> = None;
        // Children run outside every run limit, so a parent waiting on its child never holds
        // the slot the child needs; `maxSubagentsPerSession` bounds them instead.
        let full = host.scheduler.running.lock().unwrap().total >= limits.max_concurrent_runs;
        {
            for agent in host.agents.values() {
                if !agent.accepting.load(Ordering::SeqCst) {
                    continue;
                }
                let candidates = match agent.store.runnable(256).await {
                    Ok(c) => c,
                    Err(e) => {
                        tracing::error!(agent = %agent.name, error = ?e, "scheduler storage failure; this agent stops admitting work");
                        agent.accepting.store(false, Ordering::SeqCst);
                        continue;
                    }
                };
                let r = host.scheduler.running.lock().unwrap();
                for c in candidates {
                    let user = (agent.name.clone(), c.tenant_id.clone(), c.user_id.clone());
                    if c.task
                        && (full
                            || r.background.get(&user).copied().unwrap_or(0)
                                >= agent.config.limits.max_background_runs_per_user)
                    {
                        continue;
                    }
                    if !c.child
                        && (full
                            || r.tenants.get(&c.tenant_id).copied().unwrap_or(0)
                                >= limits.max_concurrent_runs_per_tenant
                            || r.users.get(&user).copied().unwrap_or(0)
                                >= agent.config.limits.max_concurrent_runs_per_user)
                    {
                        continue;
                    }
                    let served = r.served.get(&c.tenant_id).copied().unwrap_or(0);
                    if best
                        .as_ref()
                        .is_none_or(|(_, b, s)| (served, c.ordinal) < (*s, b.ordinal))
                    {
                        best = Some((agent.clone(), c, served));
                    }
                }
            }
        }
        let Some((agent, candidate, _)) = best else {
            tokio::select! { _ = host.scheduler.wake.notified() => {}, _ = tokio::time::sleep(Duration::from_secs(1)) => {} }
            continue;
        };
        let work = match agent.store.claim(&candidate.command_id).await {
            Ok(Some(work)) => work,
            Ok(None) => continue,
            Err(StoreError::QuotaExceeded) => {
                tokio::time::sleep(Duration::from_millis(50)).await;
                continue;
            }
            Err(e) => {
                tracing::error!(agent = %agent.name, error = ?e, "claim failed; this agent stops admitting work");
                agent.accepting.store(false, Ordering::SeqCst);
                continue;
            }
        };
        let token = CancellationToken::new();
        {
            let mut r = host.scheduler.running.lock().unwrap();
            if lease(work.child.as_ref()) == Lease::Background {
                r.total += 1;
                *r.background
                    .entry((
                        agent.name.clone(),
                        work.scope.tenant_id.clone(),
                        work.scope.user_id.clone(),
                    ))
                    .or_default() += 1;
            }
            if work.child.is_none() {
                r.total += 1;
                *r.tenants.entry(work.scope.tenant_id.clone()).or_default() += 1;
                *r.users
                    .entry((
                        agent.name.clone(),
                        work.scope.tenant_id.clone(),
                        work.scope.user_id.clone(),
                    ))
                    .or_default() += 1;
            }
            r.tick += 1;
            let tick = r.tick;
            r.served.insert(work.scope.tenant_id.clone(), tick);
            r.tokens.insert(work.command_id.clone(), token.clone());
        }
        tasks.spawn(process(host.clone(), agent, work, token));
    }
    if tokio::time::timeout(Duration::from_secs(20), async {
        while tasks.join_next().await.is_some() {}
    })
    .await
    .is_err()
    {
        tasks.abort_all();
        while tasks.join_next().await.is_some() {}
    }
}

/// The run's token for a model call, when the model's identity headers send one.
pub(crate) async fn model_token(
    agent: &Agent,
    config: &agent_ti_config::ModelConfig,
    work: &Work,
) -> Option<String> {
    if !config.identity_headers.contains_key("runToken") {
        return None;
    }
    agent
        .grants
        .as_ref()?
        .model_token(&agent.store, &work.session_id)
        .await
}
