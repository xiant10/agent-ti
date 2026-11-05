//! The scheduler, in memory and spanning every agent's store, and the session worker: the
//! model/tool loop with each external call journaled as a step.
use crate::{adapters, agents::Agent, server::Host};
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
    time::Duration,
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
}
#[derive(Default)]
pub struct Scheduler {
    wake: Notify,
    running: Mutex<Running>,
}
impl Scheduler {
    /// New work was admitted or a run ended.
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
        r.total -= 1;
        *r.tenants.get_mut(&w.scope.tenant_id).unwrap() -= 1;
        *r.users
            .get_mut(&(
                agent.to_owned(),
                w.scope.tenant_id.clone(),
                w.scope.user_id.clone(),
            ))
            .unwrap() -= 1;
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

fn error(e: StoreError) -> ExecutionError {
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
}
impl adapters::DeltaSink for LiveText<'_> {
    fn text(&mut self, text: &str) {
        let w = &self.journal.work;
        let payload = json!({"type":"text.delta","turnId":w.turn_id,"messageId":self.message_id,"generationId":format!("{}:{}",w.command_id,w.attempt),"offset":self.offset,"text":text});
        self.offset += text.encode_utf16().count();
        self.journal.streamed.lock().unwrap().text.push_str(text);
        self.journal.agent.hub.delta(&w.session_id, payload);
    }
    fn thinking(&mut self, text: &str) {
        if self.thinking {
            let w = &self.journal.work;
            self.journal.agent.hub.delta(
                &w.session_id,
                json!({"type":"thinking.delta","turnId":w.turn_id,"text":text}),
            );
        }
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
            let id = format!("memo:{}:{name}", self.plugin);
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
            let id = format!("model:{index}");
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
                request.messages = plugin.before_model_request(request.messages, &ctx).await.map_err(|e| {
                    tracing::error!(agent = %self.agent.name, plugin = plugin.name(), error = %e, "before_model_request failed");
                    ExecutionError::Denied
                })?;
            }
            let profile = &self.agent.config.profiles[&self.work.profile];
            let call = adapters::ModelCall {
                config: &self.agent.config.models[&request.model],
                agent: &self.agent.name,
                thinking_level: &profile.thinking_level,
                timeout: Duration::from_millis(self.agent.config.durable.stream_timeout_ms),
            };
            let mut live = LiveText {
                journal: self,
                message_id: message_id.clone(),
                offset: 0,
                thinking: profile.thinking_events,
            };
            let reply =
                adapters::model(&self.host.client, call, &self.work, request, &mut live).await?;
            for plugin in &self.agent.plugins {
                let ctx = Memo {
                    journal: self,
                    plugin: plugin.name(),
                };
                let _ = plugin.after_model_response(&reply.message, &ctx).await;
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
                .record(&self.work, &id, value, end)
                .await
                .map_err(error)?;
            Ok(reply)
        })
    }
}
impl Journal {
    async fn execute_load_skill(&self, call: &ToolCall) -> ToolOutcome {
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
            .offered(&self.work.scope.tenant_id, profile, &active.offered)
            .await;

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

        ToolOutcome::ok(format!("{}{}{}", skill.body, footer, files_appendix))
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
            .search_tools(query, limit, &self.work.scope.tenant_id)
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
            .offered(&self.work.scope.tenant_id, profile, &active.offered)
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
            let id = format!("tool:{round}:{}", call.id);
            let call_id = call.id.clone();
            let call_name = call.name.clone();
            let turn_id = self.work.turn_id.clone();
            let finished = move |o: &ToolOutcome| {
                Some(
                    json!({"type":"tool.finished","turnId":turn_id,"callId":call_id,"name":call_name,"isError":o.is_error}),
                )
            };
            let unsafe_effect = tool
                .as_ref()
                .is_some_and(|t| t.name != "load_skill" && t.name != "search_tools");
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
                        match plugin.before_tool_call(&call, &ctx).await {
                            Ok(Some(agent_ti_plugin_sdk::ToolCallDecision::Block(reason))) => {
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
                            Ok(Some(agent_ti_plugin_sdk::ToolCallDecision::Arguments(
                                new_args,
                            ))) => {
                                call.arguments = new_args;
                            }
                            _ => {}
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
                    let mut outcome = if tool.name == "load_skill" {
                        self.execute_load_skill(&call).await
                    } else if tool.name == "search_tools" {
                        self.execute_search_tools(&call).await
                    } else if let Some(ref handler) = tool.handler {
                        (handler)(call.arguments.clone()).await
                    } else if let Some(ref conn) = tool.connector {
                        if conn.tenant_id != self.work.scope.tenant_id {
                            return Ok(ToolOutcome::error(format!(
                                "Tool {} belongs to another tenant.",
                                tool.name
                            )));
                        }
                        let Some(ref tc) = self.agent.tenant_connectors else {
                            return Ok(ToolOutcome::error("Tenant connectors are not configured."));
                        };
                        let grant_token = match self.agent.grants {
                            Some(ref broker) => {
                                match broker.token(&self.agent.store, &self.work.session_id).await {
                                    Ok(tok) => tok,
                                    Err(crate::grants::GrantError::Stopped) => {
                                        return Ok(ToolOutcome::error(
                                            "The agent's access was stopped; end this task and tell the user.",
                                        ));
                                    }
                                    Err(crate::grants::GrantError::Expired) => {
                                        return Ok(ToolOutcome::error(
                                            "The agent's access has expired; ask the user to send a message to renew it.",
                                        ));
                                    }
                                    Err(crate::grants::GrantError::Unavailable(e)) => {
                                        return Ok(ToolOutcome::error(format!(
                                            "the auth service is unavailable: {e}"
                                        )));
                                    }
                                }
                            }
                            None => {
                                return Ok(ToolOutcome::error(format!(
                                    "{} needs an agent grant, and this agent has none configured.",
                                    tool.name
                                )));
                            }
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
                        adapters::mcp_call(
                            &self.host.client,
                            &srv,
                            &tool.mcp_name,
                            call.arguments.clone(),
                            Some(&grant_token),
                            None,
                        )
                        .await
                    } else {
                        let server = &self.agent.config.mcp_servers[tool.server];
                        let grant_token = if let agent_ti_config::McpAuth::Grant { .. } =
                            &server.auth
                        {
                            match self.agent.grants {
                                Some(ref broker) => {
                                    match broker
                                        .token(&self.agent.store, &self.work.session_id)
                                        .await
                                    {
                                        Ok(tok) => Some(tok),
                                        Err(crate::grants::GrantError::Stopped) => {
                                            return Ok(ToolOutcome::error(
                                                "The agent's access was stopped; end this task and tell the user.",
                                            ));
                                        }
                                        Err(crate::grants::GrantError::Expired) => {
                                            return Ok(ToolOutcome::error(
                                                "The agent's access has expired; ask the user to send a message to renew it.",
                                            ));
                                        }
                                        Err(crate::grants::GrantError::Unavailable(e)) => {
                                            return Ok(ToolOutcome::error(format!(
                                                "the auth service is unavailable: {e}"
                                            )));
                                        }
                                    }
                                }
                                None => {
                                    return Ok(ToolOutcome::error(format!(
                                        "{} needs an agent grant, and this agent has none configured.",
                                        tool.name
                                    )));
                                }
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
                        adapters::mcp_call(
                            &self.host.client,
                            server,
                            &tool.mcp_name,
                            call.arguments.clone(),
                            grant_token.as_deref(),
                            meta,
                        )
                        .await
                    };
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
            Ok(outcome)
        })
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
                match plugin.on_final_answer(answer, &ctx).await {
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

async fn run(
    host: Arc<Host>,
    agent: Arc<Agent>,
    work: Work,
    streamed: Arc<Mutex<Streamed>>,
) -> Result<TurnResult, ExecutionError> {
    let profile = agent
        .config
        .profiles
        .get(&work.profile)
        .ok_or(ExecutionError::Denied)?;
    let model = &agent.config.models[&profile.model];
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
    let instructions = agent.render_instructions(&work.profile, &session_skills);
    let spec = ExecutionSpec {
        instructions,
        model: profile.model.clone(),
        max_output_tokens: model.max_output_tokens.unwrap_or(4096).min(u32::MAX as u64) as u32,
        max_tool_rounds: agent.config.limits.max_tool_rounds,
        max_parallel_tools: host.config.limits.max_parallel_tools,
    };
    let history = agent.store.history(&work).await.map_err(error)?;
    agent.refresh_tools(&host.client).await;
    let session_tools = agent
        .store
        .get_session_tools(&work.scope, &work.session_id)
        .await
        .unwrap_or_default();
    let offered_names = if session_tools.offered.is_empty() {
        agent.pinned_tools(&work.scope.tenant_id, profile).await
    } else {
        session_tools.offered
    };
    let offered_list = agent
        .offered(&work.scope.tenant_id, profile, &offered_names)
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
async fn process(host: Arc<Host>, agent: Arc<Agent>, work: Work, token: CancellationToken) {
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
        let mut completion = tokio::select! {
            result = run(host.clone(), agent.clone(), work.clone(), streamed.clone()) => match result {
                Ok(r) => done("completed", r.messages, None, (r.input_tokens, r.output_tokens)),
                Err(e) => {
                    tracing::warn!(agent = %agent.name, session_id = %work.session_id, error = ?e, "run failed");
                    done("failed", vec![], Some(format!("{e:?}")), (0, 0))
                }
            },
            _ = token.cancelled() => done("cancelled", vec![], None, (0, 0)),
        };
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
            for plugin in &agent.plugins {
                plugin.after_run(&info);
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
        if host.scheduler.running.lock().unwrap().total < limits.max_concurrent_runs {
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
                    if r.tenants.get(&c.tenant_id).copied().unwrap_or(0)
                        >= limits.max_concurrent_runs_per_tenant
                        || r.users.get(&user).copied().unwrap_or(0)
                            >= agent.config.limits.max_concurrent_runs_per_user
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
            r.total += 1;
            *r.tenants.entry(work.scope.tenant_id.clone()).or_default() += 1;
            *r.users
                .entry((
                    agent.name.clone(),
                    work.scope.tenant_id.clone(),
                    work.scope.user_id.clone(),
                ))
                .or_default() += 1;
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
