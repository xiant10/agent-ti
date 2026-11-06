//! agent-pi's delegation (`docs/delegation.md`, `agent/subagents.ts`): `consult_expert` and the
//! `agent_<name>` tools run a task in a child session. A child is a session with a parent: it has
//! the parent's owner, tenant, profile, origin and metadata, its own grant under the parent's,
//! its own model, instructions and starting tools, and its own event stream, and it survives a
//! restart as any session does. Clients can read it but not drive it, it isn't listed, and it
//! counts toward no session or run limit; `maxSubagentsPerSession` bounds the ones running.
//!
//! The parent's tool call waits for the child. The tool is safe to rerun: after a restart it
//! finds the same child by its create key and the same submission by its request key. A cancel or
//! barge-in of the parent's run cancels the child too.
use crate::{
    agents::{Agent, EXPERT_TOOL, SUBAGENT_PREFIX},
    server::Host,
};
use agent_ti_config::ProfileConfig;
use agent_ti_core::{ExecutionError, ToolCall, ToolOutcome};
use agent_ti_store::{ActivationPolicy, Child, CreateSession, Submit, UserScope, Work, digest};
use serde_json::{Value, json};
use std::{
    sync::{Arc, atomic::Ordering},
    time::Duration,
};

const EXPERT_INSTRUCTIONS: &str = "You are consulted by another assistant that is talking with a user. Think the problem through, then answer with the conclusion first and only the reasoning the assistant needs to relay it.";

/// How a child session runs, from its stored spec.
pub struct ChildRun {
    pub model: String,
    pub thinking_level: String,
    pub instructions: Option<String>,
}
impl ChildRun {
    pub fn of(
        agent: &Agent,
        profile: &ProfileConfig,
        spec: &Value,
    ) -> Result<Self, ExecutionError> {
        match spec["kind"].as_str() {
            Some("expert") => {
                let expert = profile.expert.as_ref().ok_or(ExecutionError::Denied)?;
                Ok(Self {
                    model: expert.model.clone(),
                    thinking_level: expert.thinking_level.clone(),
                    instructions: Some(EXPERT_INSTRUCTIONS.into()),
                })
            }
            Some("subagent") => {
                let sub = subagent(agent, spec["name"].as_str().unwrap_or_default())
                    .ok_or(ExecutionError::Denied)?;
                Ok(Self {
                    model: sub.model.clone(),
                    thinking_level: sub.thinking_level.clone(),
                    instructions: sub.instructions.clone(),
                })
            }
            _ => Err(ExecutionError::Denied),
        }
    }
}
pub(crate) fn subagent<'a>(
    agent: &'a Agent,
    name: &str,
) -> Option<&'a agent_ti_config::SubagentConfig> {
    agent.config.subagents.iter().find(|s| s.name == name)
}

/// Runs `call` of a parent's run as a child session and returns the child's answer. `step` is
/// the call's step ID, which names the child and its submission.
pub async fn run_child(
    host: Arc<Host>,
    agent: Arc<Agent>,
    parent: &Work,
    step: &str,
    call: &ToolCall,
) -> ToolOutcome {
    let (spec, label, param, timeout) = if call.name == EXPERT_TOOL {
        (json!({"kind":"expert"}), "expert", "question", None)
    } else {
        let name = call.name.trim_start_matches(SUBAGENT_PREFIX);
        let Some(sub) = subagent(&agent, name) else {
            return ToolOutcome::error(format!("Unknown agent {name:?}."));
        };
        (
            json!({"kind":"subagent","name":name}),
            "sub-agent",
            "task",
            Some(Duration::from_millis(sub.timeout_ms)),
        )
    };
    // One level deep. Checked here rather than by leaving the tool out, because `search_tools`
    // can activate any catalog tool again.
    if parent.child.is_some() {
        return ToolOutcome::error("A sub-agent cannot delegate to another agent.");
    }
    if call.name == EXPERT_TOOL
        && agent
            .config
            .profiles
            .get(&parent.profile)
            .is_none_or(|p| p.expert.is_none())
    {
        return ToolOutcome::error("No expert model is configured for this session.");
    }
    let task = call.arguments[param]
        .as_str()
        .unwrap_or_default()
        .trim()
        .to_owned();
    if task.is_empty() {
        return ToolOutcome::error(format!("{} needs a non-empty `{param}`.", call.name));
    }
    let key = |what: &str| step_key(what, parent, step);
    // A background sub-agent returns at once and reports when its turn ends. `maxTasksPerSession`
    // bounds these; the slots below are for children the parent waits for.
    if let Some(sub) = spec["name"].as_str().and_then(|n| subagent(&agent, n))
        && sub.background
    {
        return crate::tasks::start(&host, &agent, parent, &key("task"), &sub.name, &task).await;
    }
    let Some(slot) = Slot::take(&agent, &parent.session_id) else {
        return ToolOutcome::error(format!(
            "Too many {label}s are running. Wait for one to finish, then try again."
        ));
    };
    let child = match open_child(&agent, parent, &spec, &key("child"), &call.name).await {
        Ok(id) => id,
        Err(e) => return ToolOutcome::error(format!("The {label} could not start: {e}")),
    };
    let request_key = key(label);
    let mut guard = Guard {
        host: host.clone(),
        agent: agent.clone(),
        scope: parent.scope.clone(),
        child: child.clone(),
        armed: true,
        _slot: slot,
    };
    let submit = Submit {
        scope: parent.scope.clone(),
        session_id: child.clone(),
        request_key: request_key.clone(),
        fingerprint: digest(&json!({"kind":"prompt","input":task})),
        input: task,
        turn_id: uuid::Uuid::new_v4().simple().to_string(),
        traceparent: parent.traceparent.clone(),
        reply: None,
        when_busy: None,
    };
    if let Err(e) = agent.store.admit(submit).await {
        return ToolOutcome::error(format!("The {label} could not start: {e:?}"));
    }
    host.scheduler.wake();
    let waited = wait(&agent, &parent.scope, &child, &request_key);
    let outcome = match timeout {
        Some(limit) => tokio::time::timeout(limit, waited).await.ok(),
        None => Some(waited.await),
    };
    match outcome {
        None => ToolOutcome::error(format!("The {label} ran out of time.")),
        Some(Err(e)) => ToolOutcome::error(format!("The {label} could not answer ({e:?}).")),
        Some(Ok((status, answer))) => {
            guard.armed = false;
            if status == "completed" {
                ToolOutcome::ok(if answer.is_empty() {
                    "(no answer)".into()
                } else {
                    answer
                })
            } else {
                ToolOutcome::error(format!("The {label} could not answer ({status})."))
            }
        }
    }
}

/// A key naming what this tool step makes, short enough for the store's key limit.
pub(crate) fn step_key(what: &str, parent: &Work, step: &str) -> String {
    let d = digest(&json!([what, parent.command_id, step]));
    format!("{what}:{}", &d[d.len() - 32..])
}
/// The child session this call owns, made unless a rerun already made it, with its grant and
/// its starting tools.
pub(crate) async fn open_child(
    agent: &Agent,
    parent: &Work,
    spec: &Value,
    create_key: &str,
    tool: &str,
) -> Result<String, String> {
    let scope = &parent.scope;
    let state = agent
        .store
        .state(scope, &parent.session_id)
        .await
        .map_err(|e| format!("{e:?}"))?;
    let request = CreateSession {
        scope: scope.clone(),
        session_id: format!(
            "{}.{}.{}",
            agent.name,
            agent.instance_id,
            uuid::Uuid::new_v4().simple()
        ),
        request_key: create_key.to_owned(),
        profile: state.session.profile.clone(),
        config_version: agent.version.clone(),
        origin: state.session.origin.clone(),
        metadata: state.session.metadata.clone(),
        child: Some(Child {
            parent: parent.session_id.clone(),
            spec: spec.clone(),
        }),
    };
    let (session, existed) = agent
        .store
        .create(request)
        .await
        .map_err(|e| format!("{e:?}"))?;
    let child = session.session_id;
    if !existed {
        let start = starting_tools(agent, parent, spec, tool).await;
        if !start.is_empty() {
            let profile = &agent.config.profiles[&parent.profile];
            let policy = ActivationPolicy {
                pinned: start.clone(),
                max_active: (profile.max_active_tools as usize).max(start.len()),
            };
            agent
                .store
                .activate_tools(scope, &child, &start, &policy)
                .await
                .map_err(|e| format!("{e:?}"))?;
        }
    }
    if let Some(broker) = &agent.grants {
        broker
            .spawn(&agent.store, scope, &child, &parent.session_id)
            .await
            .map_err(|e| format!("{e:?}"))?;
    }
    Ok(child)
}

/// A sub-agent's declared set (catalog patterns, plus the meta-tools by name), or for the expert
/// the parent's offered tools without the ones that would delegate again.
async fn starting_tools(agent: &Agent, parent: &Work, spec: &Value, tool: &str) -> Vec<String> {
    let tenant = &parent.scope.tenant_id;
    if spec["kind"] == "expert" {
        let offered = agent
            .store
            .get_session_tools(&parent.scope, &parent.session_id)
            .await
            .map(|t| t.offered)
            .unwrap_or_default();
        return offered
            .into_iter()
            .filter(|n| n != tool && n != EXPERT_TOOL && !n.starts_with(SUBAGENT_PREFIX))
            .collect();
    }
    let Some(sub) = subagent(agent, spec["name"].as_str().unwrap_or_default()) else {
        return Vec::new();
    };
    let mut start: Vec<String> = sub
        .tools
        .iter()
        .filter(|p| *p == "load_skill" || *p == "search_tools")
        .cloned()
        .collect();
    let patterns: Vec<String> = sub
        .tools
        .iter()
        .filter(|p| *p != "load_skill" && *p != "search_tools")
        .cloned()
        .collect();
    for name in agent.match_tools(&patterns, tenant).await {
        if !name.starts_with(SUBAGENT_PREFIX)
            && name != EXPERT_TOOL
            && !crate::tasks::PARENT_TOOLS.contains(&name.as_str())
            && !start.contains(&name)
        {
            start.push(name);
        }
    }
    // A background child tells its parent things with `task_note`.
    if spec.get("task").is_some() {
        start.push(crate::tasks::NOTE_TOOL.into());
    }
    start
}

/// Waits for the child's command to end: its status and answer.
async fn wait(
    agent: &Agent,
    scope: &UserScope,
    child: &str,
    request_key: &str,
) -> Result<(String, String), ExecutionError> {
    let mut pause = Duration::from_millis(20);
    loop {
        if let Some((status, answer)) = agent
            .store
            .outcome(scope, child, request_key)
            .await
            .map_err(crate::worker::error)?
            && matches!(status.as_str(), "completed" | "failed" | "cancelled")
        {
            return Ok((status, answer));
        }
        tokio::time::sleep(pause).await;
        pause = (pause * 2).min(Duration::from_millis(250));
    }
}

/// While armed, ending the parent's call early (a cancel, barge-in or timeout) cancels the child.
/// The child's grant is released when the call ends, unless the process is stopping: then the
/// child resumes under it in the next process.
struct Guard {
    host: Arc<Host>,
    agent: Arc<Agent>,
    scope: UserScope,
    child: String,
    armed: bool,
    _slot: Slot,
}
impl Drop for Guard {
    fn drop(&mut self) {
        let stopping = !self.host.accepting.load(Ordering::SeqCst);
        let (host, agent, scope, child, armed) = (
            self.host.clone(),
            self.agent.clone(),
            self.scope.clone(),
            self.child.clone(),
            self.armed,
        );
        if stopping {
            return;
        }
        tokio::spawn(async move {
            if armed {
                let key = format!("cancel_{}", uuid::Uuid::new_v4().simple());
                if let Ok(running) = agent.store.cancel(&scope, &child, &key).await {
                    host.scheduler.cancel(&running);
                    host.scheduler.wake();
                }
            }
            if let Some(broker) = &agent.grants {
                broker.release(&child).await;
            }
        });
    }
}

/// One of a session's `maxSubagentsPerSession` children, released on drop. In memory, as
/// agent-pi's `ChildSlots`: a child resumed after a restart counts once its parent's tool reruns.
struct Slot {
    agent: Arc<Agent>,
    session: String,
}
impl Slot {
    fn take(agent: &Arc<Agent>, session: &str) -> Option<Self> {
        let mut running = agent.children.lock().unwrap();
        let count = running.entry(session.to_owned()).or_default();
        if *count >= agent.config.limits.max_subagents_per_session {
            return None;
        }
        *count += 1;
        Some(Self {
            agent: agent.clone(),
            session: session.to_owned(),
        })
    }
}
impl Drop for Slot {
    fn drop(&mut self) {
        let mut running = self.agent.children.lock().unwrap();
        if let Some(count) = running.get_mut(&self.session) {
            *count -= 1;
            if *count == 0 {
                running.remove(&self.session);
            }
        }
    }
}
