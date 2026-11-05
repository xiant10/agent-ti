//! Provider-neutral bounded execution. No source config, HTTP, SQL or features.
use futures_util::{StreamExt, stream};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{future::Future, pin::Pin};

pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;
/// Larger tool results go back to the model as an error instead.
pub const MAX_TOOL_RESULT_BYTES: usize = 256 * 1024;

#[derive(Clone, Debug)]
pub struct ExecutionSpec {
    pub instructions: Vec<String>,
    pub model: String,
    pub max_output_tokens: u32,
    pub max_tool_rounds: u32,
    /// Independent tool calls of one round run at once, up to this many.
    pub max_parallel_tools: usize,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct ToolCall {
    pub id: String,
    pub name: String,
    pub arguments: Value,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct Message {
    pub role: String,
    pub content: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tool_calls: Vec<ToolCall>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tool_call_id: Option<String>,
    /// A tool result that reports a failure, as MCP's `isError`.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub is_error: bool,
    /// The plugin that wrote this message without the model.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source: Option<String>,
}
impl Message {
    pub fn text(role: &str, content: impl Into<String>) -> Self {
        Self {
            role: role.into(),
            content: content.into(),
            tool_calls: vec![],
            tool_call_id: None,
            is_error: false,
            source: None,
        }
    }
    pub fn tool(call_id: &str, outcome: ToolOutcome) -> Self {
        Self {
            tool_call_id: Some(call_id.into()),
            is_error: outcome.is_error,
            ..Self::text("tool", outcome.content)
        }
    }
    pub fn record(source: &str, content: impl Into<String>) -> Self {
        Self {
            role: "record".into(),
            content: content.into(),
            tool_calls: vec![],
            tool_call_id: None,
            is_error: false,
            source: Some(source.into()),
        }
    }
}
/// A tool's result. Failures of the call itself are results with `is_error`, which go back to
/// the model so it can correct itself; only infrastructure failures are `ExecutionError`s.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct ToolOutcome {
    pub content: String,
    #[serde(default)]
    pub is_error: bool,
}
impl ToolOutcome {
    pub fn ok(content: impl Into<String>) -> Self {
        Self {
            content: content.into(),
            is_error: false,
        }
    }
    pub fn error(content: impl Into<String>) -> Self {
        Self {
            content: content.into(),
            is_error: true,
        }
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ToolDescriptor {
    pub name: String,
    pub description: String,
    pub parameters: Value,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ModelRequest {
    pub model: String,
    pub messages: Vec<Message>,
    pub tools: Vec<ToolDescriptor>,
    pub max_output_tokens: u32,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ModelReply {
    pub message: Message,
    pub input_tokens: u64,
    pub output_tokens: u64,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ExecutionError {
    ContextLimit,
    InvalidToolRound,
    ToolRoundLimit,
    Unavailable,
    UnknownOutcome,
    MissingArtifact,
    Denied,
    InvalidResponse,
}
pub trait ModelPort: Send + Sync {
    fn generate<'a>(
        &'a self,
        request: ModelRequest,
    ) -> BoxFuture<'a, Result<ModelReply, ExecutionError>>;
}
pub trait ToolPort: Send + Sync {
    fn invoke<'a>(&'a self, call: ToolCall) -> BoxFuture<'a, Result<ToolOutcome, ExecutionError>>;
}
pub trait ExecutionHooks: Send + Sync {
    fn on_final_answer<'a>(
        &'a self,
        _answer: &'a Message,
    ) -> BoxFuture<'a, Result<Option<String>, ExecutionError>> {
        Box::pin(async { Ok(None) })
    }
    fn post_tools(&self) -> BoxFuture<'_, Result<Vec<Message>, ExecutionError>> {
        Box::pin(async { Ok(Vec::new()) })
    }
    fn current_tools(&self) -> BoxFuture<'_, Result<Option<Vec<ToolDescriptor>>, ExecutionError>> {
        Box::pin(async { Ok(None) })
    }
}
pub struct DefaultHooks;
impl ExecutionHooks for DefaultHooks {}

#[derive(Debug)]
pub struct TurnResult {
    pub messages: Vec<Message>,
    pub input_tokens: u64,
    pub output_tokens: u64,
}
/// Conservative byte budget until a provider supplies an exact tokenizer. A
/// transcript group starts at a user message; eviction removes complete groups.
pub fn context(
    instructions: &[String],
    history: &[Message],
    budget: usize,
) -> Result<Vec<Message>, ExecutionError> {
    let mut result = vec![Message::text("system", instructions.join("\n\n"))];
    let mandatory = serde_json::to_vec(&result)
        .map_err(|_| ExecutionError::ContextLimit)?
        .len();
    if mandatory > budget {
        return Err(ExecutionError::ContextLimit);
    }
    // Filter out "record" role: records are for UIs/audits/plugins, model never sees them.
    let filtered: Vec<&Message> = history.iter().filter(|m| m.role != "record").collect();
    if filtered.is_empty() {
        return Ok(result);
    }
    // Identify turn group starts. A normal turn starts at a user message.
    // If there is an initial greeting assistant message before any user message, start 0 includes it.
    let starts: Vec<usize> = if filtered.first().map(|m| m.role.as_str()) == Some("assistant")
        && filtered
            .first()
            .map(|m| m.tool_calls.is_empty())
            .unwrap_or(false)
        && filtered.iter().skip(1).any(|m| m.role == "user")
    {
        let mut s = vec![0];
        s.extend(
            filtered
                .iter()
                .enumerate()
                .filter_map(|(i, m)| (i > 0 && m.role == "user").then_some(i)),
        );
        s
    } else {
        filtered
            .iter()
            .enumerate()
            .filter_map(|(i, m)| (m.role == "user").then_some(i))
            .collect()
    };
    if starts.is_empty() {
        return Err(ExecutionError::InvalidToolRound);
    }
    for start in starts {
        let slice = &filtered[start..];
        validate_rounds(slice)?;
        let serialized = serde_json::to_vec(slice).map_err(|_| ExecutionError::ContextLimit)?;
        if mandatory + serialized.len() <= budget {
            result.extend(slice.iter().copied().cloned());
            return Ok(result);
        }
    }
    Err(ExecutionError::ContextLimit)
}
fn validate_rounds(messages: &[&Message]) -> Result<(), ExecutionError> {
    let mut pending = std::collections::BTreeSet::new();
    for m in messages {
        match m.role.as_str() {
            "tool" => {
                if !m.tool_calls.is_empty()
                    || !m.tool_call_id.as_ref().is_some_and(|id| pending.remove(id))
                {
                    return Err(ExecutionError::InvalidToolRound);
                }
            }
            "assistant" | "user" => {
                if !pending.is_empty()
                    || m.tool_call_id.is_some()
                    || (m.role == "user" && !m.tool_calls.is_empty())
                {
                    return Err(ExecutionError::InvalidToolRound);
                }
                for call in &m.tool_calls {
                    if call.id.is_empty() || !pending.insert(call.id.clone()) {
                        return Err(ExecutionError::InvalidToolRound);
                    }
                }
            }
            _ => return Err(ExecutionError::InvalidToolRound),
        }
    }
    if pending.is_empty() {
        Ok(())
    } else {
        Err(ExecutionError::InvalidToolRound)
    }
}
pub async fn execute(
    spec: &ExecutionSpec,
    history: Vec<Message>,
    tools: Vec<ToolDescriptor>,
    model: &dyn ModelPort,
    dispatcher: &dyn ToolPort,
    context_budget: usize,
) -> Result<TurnResult, ExecutionError> {
    execute_with_hooks(
        spec,
        history,
        tools,
        model,
        dispatcher,
        &DefaultHooks,
        context_budget,
    )
    .await
}

pub async fn execute_with_hooks(
    spec: &ExecutionSpec,
    history: Vec<Message>,
    tools: Vec<ToolDescriptor>,
    model: &dyn ModelPort,
    dispatcher: &dyn ToolPort,
    hooks: &dyn ExecutionHooks,
    context_budget: usize,
) -> Result<TurnResult, ExecutionError> {
    let mut transcript = history;
    let first = transcript.len();
    let mut input_tokens = 0;
    let mut output_tokens = 0;
    let mut tools = tools;
    for round in 0..=spec.max_tool_rounds {
        if round > 0
            && let Some(updated) = hooks.current_tools().await?
        {
            tools = updated;
        }
        let names: std::collections::BTreeSet<_> = tools.iter().map(|t| t.name.clone()).collect();
        if names.len() != tools.len() {
            return Err(ExecutionError::Denied);
        }
        let schema_bytes = serde_json::to_vec(&tools)
            .map_err(|_| ExecutionError::ContextLimit)?
            .len();
        let budget = context_budget
            .checked_sub(schema_bytes + spec.max_output_tokens as usize)
            .ok_or(ExecutionError::ContextLimit)?;
        let reply = model
            .generate(ModelRequest {
                model: spec.model.clone(),
                messages: context(&spec.instructions, &transcript, budget)?,
                tools: tools.clone(),
                max_output_tokens: spec.max_output_tokens,
            })
            .await?;
        if reply.message.role != "assistant"
            || reply.message.tool_call_id.is_some()
            || reply.message.content.len() > 256 * 1024
            || reply.message.tool_calls.len() > 32
        {
            return Err(ExecutionError::InvalidResponse);
        }
        input_tokens += reply.input_tokens;
        output_tokens += reply.output_tokens;
        let calls = reply.message.tool_calls.clone();
        transcript.push(reply.message);
        if calls.is_empty() {
            if let Some(continuation) = hooks.on_final_answer(transcript.last().unwrap()).await? {
                if round == spec.max_tool_rounds {
                    return Err(ExecutionError::ToolRoundLimit);
                }
                transcript.push(Message::text("user", continuation));
                continue;
            }
            return Ok(TurnResult {
                messages: transcript[first..].to_vec(),
                input_tokens,
                output_tokens,
            });
        }
        if round == spec.max_tool_rounds {
            return Err(ExecutionError::ToolRoundLimit);
        }
        let mut ids = std::collections::BTreeSet::new();
        if calls
            .iter()
            .any(|call| call.id.is_empty() || !ids.insert(&call.id))
        {
            return Err(ExecutionError::InvalidResponse);
        }
        // Results keep the calls' order whatever order they finish in. The dispatcher answers an
        // unknown name or invalid arguments with an error result, as for any failed call.
        let results: Vec<_> = stream::iter(calls)
            .map(|call| async move {
                let id = call.id.clone();
                let outcome = dispatcher.invoke(call).await?;
                if outcome.content.len() > MAX_TOOL_RESULT_BYTES {
                    return Ok((
                        id,
                        ToolOutcome::error(
                            "The tool's result is larger than 256 KiB and was dropped.",
                        ),
                    ));
                }
                Ok((id, outcome))
            })
            .buffered(spec.max_parallel_tools.max(1))
            .collect()
            .await;
        for result in results {
            let (id, outcome) = result?;
            transcript.push(Message::tool(&id, outcome));
        }
        let steered = hooks.post_tools().await?;
        for msg in steered {
            transcript.push(msg);
        }
    }
    Err(ExecutionError::ToolRoundLimit)
}
