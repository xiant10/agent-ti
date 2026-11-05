//! agent-pi's plugin hooks in Rust. Every member has a default that does nothing, as every member
//! of an agent-pi plugin is optional.
pub use agent_ti_config::Snapshot;
pub use agent_ti_core::{BoxFuture, Message, ToolCall, ToolOutcome};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::sync::Arc;

/// What `before_prompt` decided about one input. Recorded with the command, so a retried
/// `requestId` gets the same decision without running the hook again.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum PromptDecision {
    /// Replace the input; the rewrite is what is stored and what later turns see.
    Input(String),
    /// Replace input and activate dynamic tools.
    InputAndTools {
        text: String,
        activate_tools: Vec<String>,
    },
    /// Answer without the model.
    Reply(String),
    /// Refuse the input: a 422 to the caller.
    Reject(String),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ToolCallDecision {
    Block(String),
    Arguments(Value),
}

#[derive(Clone, Debug)]
pub struct PluginSection {
    pub name: String,
    pub content: String,
    pub tag: bool,
}

pub type ToolHandler = Arc<dyn Fn(Value) -> BoxFuture<'static, ToolOutcome> + Send + Sync>;

#[derive(Clone)]
pub struct PluginTool {
    pub name: String,
    pub description: String,
    pub parameters: Value,
    pub handler: ToolHandler,
}

pub trait PluginState: Send + Sync {
    fn get(&self) -> BoxFuture<'_, Result<Option<Value>, String>>;
    fn set(&self, value: Value) -> BoxFuture<'_, Result<(), String>>;
}

pub trait SessionHistory: Send + Sync {
    fn say(&self, text: &str) -> BoxFuture<'_, Result<(), String>>;
    fn exchange(&self, input: &str, answer: &str) -> BoxFuture<'_, Result<(), String>>;
    fn record(&self, record_type: &str, data: Value) -> BoxFuture<'_, Result<(), String>>;
}

pub struct SessionContext<'a> {
    pub agent: &'a str,
    pub session_id: &'a str,
    pub tenant_id: &'a str,
    pub user_id: &'a str,
    pub profile: &'a str,
    pub state: Arc<dyn PluginState>,
    pub history: Arc<dyn SessionHistory>,
}

pub struct RpcContext<'a> {
    pub agent: &'a str,
    pub tenant_id: &'a str,
    pub user_id: &'a str,
    pub session_id: Option<&'a str>,
    pub state: Option<Arc<dyn PluginState>>,
    pub history: Option<Arc<dyn SessionHistory>>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PrincipalInfo {
    pub user_id: String,
    pub tenant_id: String,
    pub scopes: Vec<String>,
    pub claims: Value,
}

pub struct HostInfo<'a> {
    pub storage_dir: &'a std::path::Path,
}

pub struct Turn<'a> {
    pub text: &'a str,
    pub agent: &'a str,
    pub session_id: &'a str,
    pub tenant_id: &'a str,
    pub user_id: &'a str,
}

pub struct SessionInfo<'a> {
    pub agent: &'a str,
    pub session_id: &'a str,
    pub tenant_id: &'a str,
    pub user_id: &'a str,
    pub profile: &'a str,
}

pub struct RunInfo<'a> {
    pub agent: &'a str,
    pub session_id: &'a str,
    pub turn_id: &'a str,
    pub tenant_id: &'a str,
    pub user_id: &'a str,
    pub profile: &'a str,
    /// `model`, or the plugin that answered.
    pub answered_by: &'a str,
    pub status: &'a str,
    pub reason: Option<&'a str>,
    pub text: &'a str,
}

/// Per-request services for hooks that run inside a journaled step.
pub trait RequestContext: Send + Sync {
    fn user_id(&self) -> &str {
        ""
    }
    fn tenant_id(&self) -> &str {
        ""
    }
    /// The value recorded under `name` for this run, computed and recorded the first time, so a
    /// replay after a crash sees the same value.
    fn memo<'a>(
        &'a self,
        name: &'a str,
        compute: Box<dyn FnOnce() -> Value + Send + 'a>,
    ) -> BoxFuture<'a, Result<Value, String>>;
}

pub trait Plugin: Send + Sync {
    fn name(&self) -> &str;
    /// Recorded in logs instead of a hash of the source, so editing a comment changes nothing.
    fn version(&self) -> &str {
        env!("CARGO_PKG_VERSION")
    }

    fn setup<'a>(&'a self, _host: &'a HostInfo<'a>) -> BoxFuture<'a, Result<(), String>> {
        Box::pin(async { Ok(()) })
    }
    fn dispose(&self) -> BoxFuture<'_, Result<(), String>> {
        Box::pin(async { Ok(()) })
    }

    fn instructions(&self, _profile: &str) -> Vec<PluginSection> {
        vec![]
    }
    fn tools(&self) -> Vec<PluginTool> {
        vec![]
    }
    fn rpc_methods(&self) -> Vec<String> {
        vec![]
    }
    fn call_rpc<'a>(
        &'a self,
        _method: &'a str,
        _params: Value,
        _ctx: &'a RpcContext<'a>,
    ) -> BoxFuture<'a, Result<Value, String>> {
        Box::pin(async { Err("method not found".into()) })
    }

    fn authenticate<'a>(
        &'a self,
        principal: PrincipalInfo,
    ) -> BoxFuture<'a, Result<PrincipalInfo, String>> {
        Box::pin(async move { Ok(principal) })
    }

    fn session_created<'a>(
        &'a self,
        _session: &'a SessionContext<'a>,
    ) -> BoxFuture<'a, Result<(), String>> {
        Box::pin(async { Ok(()) })
    }
    fn session_closed<'a>(
        &'a self,
        _session: &'a SessionContext<'a>,
    ) -> BoxFuture<'a, Result<(), String>> {
        Box::pin(async { Ok(()) })
    }

    fn before_prompt<'a>(
        &'a self,
        _turn: &'a Turn<'a>,
    ) -> BoxFuture<'a, Result<Option<PromptDecision>, String>> {
        Box::pin(async { Ok(None) })
    }
    /// Messages for this request only; never stored.
    fn before_model_request<'a>(
        &'a self,
        messages: Vec<Message>,
        _ctx: &'a dyn RequestContext,
    ) -> BoxFuture<'a, Result<Vec<Message>, String>> {
        Box::pin(async move { Ok(messages) })
    }
    fn after_model_response<'a>(
        &'a self,
        _message: &'a Message,
        _ctx: &'a dyn RequestContext,
    ) -> BoxFuture<'a, Result<(), String>> {
        Box::pin(async { Ok(()) })
    }
    fn on_final_answer<'a>(
        &'a self,
        _answer: &'a Message,
        _ctx: &'a dyn RequestContext,
    ) -> BoxFuture<'a, Result<Option<String>, String>> {
        Box::pin(async { Ok(None) })
    }
    fn before_tool_call<'a>(
        &'a self,
        _call: &'a ToolCall,
        _ctx: &'a dyn RequestContext,
    ) -> BoxFuture<'a, Result<Option<ToolCallDecision>, String>> {
        Box::pin(async { Ok(None) })
    }
    /// Change a tool's result before it is recorded.
    fn after_tool_call(&self, _call: &ToolCall, result: ToolOutcome) -> ToolOutcome {
        result
    }
    /// After a submission settled; observers only, never delays the answer.
    fn after_run(&self, _run: &RunInfo<'_>) {}
    fn map_event(&self, event: Value) -> Option<Value> {
        Some(event)
    }
}

/// Builds a plugin from its `options` in `agent.json`. Files the plugin needs are read from the
/// agent's config snapshot, never the filesystem. An error stops startup.
pub type Factory = fn(&Value, &Snapshot) -> Result<Arc<dyn Plugin>, String>;

/// A required, non-empty string option.
pub fn text_option(options: &Value, name: &str) -> Result<String, String> {
    options[name]
        .as_str()
        .filter(|s| !s.is_empty())
        .map(str::to_owned)
        .ok_or_else(|| format!("option {name} must be a non-empty string"))
}
