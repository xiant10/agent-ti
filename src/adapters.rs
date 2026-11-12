//! The OpenAI-compatible model adapter and the HTTP MCP client.
use agent_ti_config::{McpAuth, McpServerConfig, ModelConfig};
use agent_ti_core::*;
use agent_ti_store::Work;
use futures_util::StreamExt;
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, HashMap},
    sync::{LazyLock, Mutex},
    time::{Duration, Instant},
};

pub async fn bounded(response: reqwest::Response, limit: usize) -> Result<Vec<u8>, ExecutionError> {
    if !response.status().is_success()
        || response.content_length().is_some_and(|n| n > limit as u64)
    {
        return Err(ExecutionError::Unavailable);
    }
    let mut stream = response.bytes_stream();
    let mut body = Vec::new();
    while let Some(part) = stream.next().await {
        let part = part.map_err(|_| ExecutionError::Unavailable)?;
        if body.len() + part.len() > limit {
            return Err(ExecutionError::InvalidResponse);
        }
        body.extend_from_slice(&part);
    }
    Ok(body)
}
fn model_message(m: &Message) -> Value {
    let mut value = json!({"role":m.role,"content":m.content});
    if let Some(id) = &m.tool_call_id {
        value["tool_call_id"] = json!(id);
    }
    if !m.tool_calls.is_empty() {
        value["tool_calls"] = Value::Array(
            m.tool_calls
                .iter()
                .map(|c| json!({"id":c.id,"type":"function","function":{"name":c.name,"arguments":c.arguments.to_string()}}))
                .collect(),
        );
    }
    value
}
/// What a model request is sent with, beyond the messages.
pub struct ModelCall<'a> {
    pub config: &'a ModelConfig,
    pub agent: &'a str,
    pub thinking_level: &'a str,
    /// agent-pi's `durable.stream.timeoutMs`: one model request, not the whole run.
    pub timeout: Duration,
    /// The run's token for the model gateway, sent only where `identityHeaders` names `runToken`.
    pub run_token: Option<&'a str>,
}
/// Text and reasoning as they arrive, in batches.
pub trait DeltaSink: Send {
    fn text(&mut self, text: &str);
    fn thinking(&mut self, text: &str);
}
/// agent-pi's floor on delta latency: a batch goes out at most every 100 ms, or at 16 KiB.
const FLUSH_EVERY: Duration = Duration::from_millis(100);
const FLUSH_BYTES: usize = 16 * 1024;
const MAX_REPLY_BYTES: usize = 1024 * 1024;

pub async fn model(
    client: &reqwest::Client,
    call: ModelCall<'_>,
    w: &Work,
    request: ModelRequest,
    sink: &mut dyn DeltaSink,
) -> Result<ModelReply, ExecutionError> {
    let config = call.config;
    let mut body = json!({"model":config.model_id,"messages":request.messages.iter().map(model_message).collect::<Vec<_>>(),"stream":true,"max_tokens":request.max_output_tokens});
    if config.reasoning && call.thinking_level != "off" {
        body["reasoning_effort"] = json!(match call.thinking_level {
            "minimal" | "low" => "low",
            "medium" => "medium",
            _ => "high",
        });
    } else if config.model_id.to_lowercase().contains("qwen") {
        body["enable_thinking"] = json!(false);
    }
    if !request.tools.is_empty() {
        body["tools"] = Value::Array(
            request.tools.iter().map(|t| json!({"type":"function","function":{"name":t.name,"description":t.description,"parameters":t.parameters}})).collect(),
        );
        if request.answer_only {
            body["tool_choice"] = json!("none");
        } else if let Some(name) = &request.force_tool {
            body["tool_choice"] = json!({"type":"function","function":{"name":name}});
        }
    }
    let mut req = client
        .post(format!(
            "{}/chat/completions",
            config.base_url.trim_end_matches('/')
        ))
        .timeout(call.timeout)
        .json(&body);
    if let Some(env) = &config.api_key_env {
        req = req.bearer_auth(std::env::var(env).map_err(|_| ExecutionError::Denied)?);
    }
    for (field, header) in &config.identity_headers {
        let value = match field.as_str() {
            "agent" => call.agent,
            "sessionId" => &w.session_id,
            "turnId" => &w.turn_id,
            "userId" => &w.scope.user_id,
            "tenantId" => &w.scope.tenant_id,
            "runToken" => match call.run_token {
                Some(token) => token,
                None => continue,
            },
            _ => continue,
        };
        req = req.header(header, value);
    }
    if let Some(trace) = &w.traceparent {
        req = req.header("traceparent", trace);
    }
    let response = req.send().await.map_err(|_| ExecutionError::Unavailable)?;
    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        if status == reqwest::StatusCode::PAYLOAD_TOO_LARGE || context_overflow(&body) {
            return Err(ExecutionError::ContextOverflow);
        }
        return Err(ExecutionError::Unavailable);
    }
    let mut stream = response.bytes_stream();
    let mut reply = Reply::default();
    let mut buffer: Vec<u8> = Vec::new();
    let (mut text, mut thinking) = (String::new(), String::new());
    let mut read = 0;
    let mut last_flush = tokio::time::Instant::now() - FLUSH_EVERY;
    let mut flush = |text: &mut String, thinking: &mut String, last: &mut tokio::time::Instant| {
        if !thinking.is_empty() {
            sink.thinking(thinking);
            thinking.clear();
        }
        if !text.is_empty() {
            sink.text(text);
            text.clear();
        }
        *last = tokio::time::Instant::now();
    };
    loop {
        let waiting = !text.is_empty() || !thinking.is_empty();
        let part = tokio::select! {
            part = stream.next() => part,
            _ = tokio::time::sleep_until(last_flush + FLUSH_EVERY), if waiting => {
                flush(&mut text, &mut thinking, &mut last_flush);
                continue;
            }
        };
        let Some(part) = part else { break };
        let part = part.map_err(|_| ExecutionError::Unavailable)?;
        read += part.len();
        if read > MAX_REPLY_BYTES {
            return Err(ExecutionError::InvalidResponse);
        }
        buffer.extend_from_slice(&part);
        // A plain JSON answer is parsed whole at the end.
        if reply.json || buffer.first().is_some_and(|b| *b == b'{') {
            reply.json = true;
            continue;
        }
        while let Some(end) = buffer.iter().position(|b| *b == b'\n') {
            let line: Vec<u8> = buffer.drain(..=end).collect();
            let line = std::str::from_utf8(&line).map_err(|_| ExecutionError::InvalidResponse)?;
            reply.line(line.trim_end(), &mut text, &mut thinking)?;
        }
        if tokio::time::Instant::now() >= last_flush + FLUSH_EVERY
            || text.len() + thinking.len() >= FLUSH_BYTES
        {
            flush(&mut text, &mut thinking, &mut last_flush);
        }
    }
    if reply.json {
        return parse_model(&buffer);
    }
    let rest = std::str::from_utf8(&buffer)
        .map_err(|_| ExecutionError::InvalidResponse)?
        .to_owned();
    reply.line(rest.trim_end(), &mut text, &mut thinking)?;
    flush(&mut text, &mut thinking, &mut last_flush);
    reply.finish()
}
/// A chat-completions answer assembled from its stream.
#[derive(Default)]
struct Reply {
    json: bool,
    text: String,
    calls: BTreeMap<u64, (String, String, String)>,
    usage: (u64, u64),
    finished: bool,
}
impl Reply {
    /// One SSE line; new text and reasoning are appended to `text` and `thinking`.
    fn line(
        &mut self,
        line: &str,
        text: &mut String,
        thinking: &mut String,
    ) -> Result<(), ExecutionError> {
        let Some(data) = line.strip_prefix("data:").map(str::trim) else {
            return Ok(());
        };
        if data == "[DONE]" || data.is_empty() {
            return Ok(());
        }
        let v: Value = serde_json::from_str(data).map_err(|_| ExecutionError::InvalidResponse)?;
        let choice = &v["choices"][0];
        let delta = &choice["delta"];
        if let Some(t) = delta["content"].as_str() {
            self.text.push_str(t);
            text.push_str(t);
        }
        if let Some(t) = delta["reasoning_content"]
            .as_str()
            .or(delta["reasoning"].as_str())
        {
            thinking.push_str(t);
        }
        for c in delta["tool_calls"].as_array().into_iter().flatten() {
            let index = c["index"].as_u64().ok_or(ExecutionError::InvalidResponse)?;
            if index >= 32 {
                return Err(ExecutionError::InvalidResponse);
            }
            let item = self.calls.entry(index).or_default();
            if let Some(id) = c["id"].as_str() {
                item.0.push_str(id);
            }
            if let Some(name) = c["function"]["name"].as_str() {
                item.1.push_str(name);
            }
            if let Some(args) = c["function"]["arguments"].as_str() {
                item.2.push_str(args);
            }
        }
        if choice["finish_reason"].is_string() {
            self.finished = true;
        }
        if let Some(n) = v["usage"]["prompt_tokens"].as_u64() {
            self.usage.0 = n;
        }
        if let Some(n) = v["usage"]["completion_tokens"].as_u64() {
            self.usage.1 = n;
        }
        Ok(())
    }
    fn finish(self) -> Result<ModelReply, ExecutionError> {
        if !self.finished || self.text.len() > 256 * 1024 {
            return Err(ExecutionError::InvalidResponse);
        }
        let tool_calls = self
            .calls
            .into_values()
            .map(|(id, name, args)| {
                // Arguments that aren't JSON go to the tool as given, where validation reports them to the model.
                let arguments = if args.trim().is_empty() {
                    json!({})
                } else {
                    serde_json::from_str(&args).unwrap_or(Value::String(args))
                };
                ToolCall {
                    id,
                    name,
                    arguments,
                }
            })
            .collect();
        Ok(ModelReply {
            message: Message {
                tool_calls,
                ..Message::text("assistant", self.text)
            },
            input_tokens: self.usage.0,
            output_tokens: self.usage.1,
        })
    }
}
/// A whole answer: a chat-completions JSON object, or its SSE stream.
pub fn parse_model(data: &[u8]) -> Result<ModelReply, ExecutionError> {
    let source = std::str::from_utf8(data).map_err(|_| ExecutionError::InvalidResponse)?;
    let mut reply = Reply::default();
    if source.trim_start().starts_with('{') {
        let v: Value = serde_json::from_str(source).map_err(|_| ExecutionError::InvalidResponse)?;
        let m = &v["choices"][0]["message"];
        if !m.is_object() {
            return Err(ExecutionError::InvalidResponse);
        }
        reply.text = m["content"].as_str().unwrap_or("").into();
        for (index, c) in m["tool_calls"].as_array().into_iter().flatten().enumerate() {
            let field = |v: &Value| v.as_str().unwrap_or("").to_owned();
            reply.calls.insert(
                index as u64,
                (
                    field(&c["id"]),
                    field(&c["function"]["name"]),
                    field(&c["function"]["arguments"]),
                ),
            );
        }
        reply.usage = (
            v["usage"]["prompt_tokens"].as_u64().unwrap_or(0),
            v["usage"]["completion_tokens"].as_u64().unwrap_or(0),
        );
        reply.finished = true;
    } else {
        let (mut text, mut thinking) = (String::new(), String::new());
        for line in source.lines() {
            reply.line(line, &mut text, &mut thinking)?;
        }
    }
    reply.finish()
}

/// A tool as an MCP server lists it.
#[derive(Clone, Debug)]
pub struct McpTool {
    pub name: String,
    pub description: String,
    pub input_schema: Value,
    /// The module that owns the tool, when the server serves several (the tool gateway's
    /// `/mcp/modules` puts it in `_meta`, as `t10/module`).
    pub owner: Option<String>,
}
/// One JSON-RPC exchange over Streamable HTTP; the response may be JSON or an SSE stream.
tokio::task_local! {
    /// What belongs to one tool call: its turn, session and trace, for the tool gateway's spans
    /// and audit record. A connection may be shared, so it doesn't belong on the client.
    pub static CALL_HEADERS: Vec<(&'static str, String)>;
}
struct McpClient<'a> {
    client: &'a reqwest::Client,
    server: &'a McpServerConfig,
    session: Option<reqwest::header::HeaderValue>,
    timeout: Duration,
    grant_token: Option<&'a str>,
    /// The last request reached the server and it answered with success.
    answered: bool,
    /// The last request named a session the server no longer has, so it wasn't run.
    stale: bool,
}
/// Open MCP sessions by server and credential, reused between tool calls as agent-pi keeps a
/// client per grant: a call is then one request, not `initialize`, `initialized` and the call.
/// A grant's renewed token opens a session of its own; idle ones are dropped.
static SESSIONS: LazyLock<Mutex<HashMap<String, KeptSession>>> = LazyLock::new(Default::default);
const KEPT_SESSIONS: usize = 256;
const SESSION_IDLE: Duration = Duration::from_secs(300);
struct KeptSession {
    /// `None` for a server that runs without sessions.
    id: Option<reqwest::header::HeaderValue>,
    used: Instant,
}
fn session_key(server: &McpServerConfig, grant_token: Option<&str>) -> String {
    format!("{}\0{}", server.url, grant_token.unwrap_or(""))
}
fn kept_session(key: &str) -> Option<Option<reqwest::header::HeaderValue>> {
    let mut sessions = SESSIONS.lock().unwrap();
    let kept = sessions.get_mut(key)?;
    if kept.used.elapsed() > SESSION_IDLE {
        sessions.remove(key);
        return None;
    }
    kept.used = Instant::now();
    Some(kept.id.clone())
}
/// Keeps the client's session after a call the server answered, and drops it after one that
/// failed on the way, so the next call starts a new one.
fn settle_session(key: String, c: &McpClient<'_>) {
    let mut sessions = SESSIONS.lock().unwrap();
    if !c.answered {
        sessions.remove(&key);
        return;
    }
    sessions.retain(|_, s| s.used.elapsed() <= SESSION_IDLE);
    if sessions.len() >= KEPT_SESSIONS
        && !sessions.contains_key(&key)
        && let Some(oldest) = sessions
            .iter()
            .min_by_key(|(_, s)| s.used)
            .map(|(k, _)| k.clone())
    {
        sessions.remove(&oldest);
    }
    sessions.insert(
        key,
        KeptSession {
            id: c.session.clone(),
            used: Instant::now(),
        },
    );
}
impl McpClient<'_> {
    fn request(&self, body: &Value) -> reqwest::RequestBuilder {
        // Operator credentials stay in the host. Never forward caller bearer tokens.
        let mut r = self
            .client
            .post(&self.server.url)
            .timeout(self.timeout)
            .header("accept", "application/json, text/event-stream")
            .header("mcp-protocol-version", "2025-03-26")
            .json(body);
        for (name, value) in &self.server.headers {
            r = r.header(name, value);
        }
        if let McpAuth::Bearer { token, .. } = &self.server.auth {
            r = r.bearer_auth(token);
        } else if let McpAuth::Grant { token_header } = &self.server.auth
            && let Some(token) = self.grant_token
        {
            let header = token_header.as_deref().unwrap_or("Authorization");
            let value = if header.eq_ignore_ascii_case("authorization") {
                format!("Bearer {token}")
            } else {
                token.to_string()
            };
            r = r.header(header, value);
            // Only grant servers get these: they're t10's own, and the gateway strips them before
            // a tenant's server.
            for (name, value) in CALL_HEADERS.try_with(Clone::clone).unwrap_or_default() {
                r = r.header(name, value);
            }
        }
        if let Some(s) = &self.session {
            r = r.header("mcp-session-id", s);
        }
        r
    }
    async fn call(&mut self, id: u64, method: &str, params: Value) -> Result<Value, String> {
        let response = self
            .request(&json!({"jsonrpc":"2.0","id":id,"method":method,"params":params}))
            .send()
            .await
            .map_err(|e| {
                if e.is_timeout() {
                    "the tool server timed out".to_owned()
                } else {
                    "the tool server is unreachable".to_owned()
                }
            })?;
        let status = response.status();
        self.answered = status.is_success();
        // MCP answers 404 for a session it ended; some servers say 400.
        self.stale = self.session.is_some()
            && matches!(
                status,
                reqwest::StatusCode::NOT_FOUND | reqwest::StatusCode::BAD_REQUEST
            );
        if let Some(s) = response.headers().get("mcp-session-id") {
            self.session = Some(s.clone());
        }
        let sse = response
            .headers()
            .get("content-type")
            .and_then(|v| v.to_str().ok())
            .is_some_and(|v| v.starts_with("text/event-stream"));
        let bytes = bounded(response, 1024 * 1024)
            .await
            .map_err(|_| format!("the tool server answered HTTP {status}"))?;
        let text = String::from_utf8(bytes)
            .map_err(|_| "the tool server's answer is not UTF-8".to_owned())?;
        let message = if sse {
            text.lines()
                .filter_map(|l| l.strip_prefix("data:").map(str::trim))
                .filter_map(|l| serde_json::from_str::<Value>(l).ok())
                .find(|v| v["id"] == json!(id))
                .ok_or_else(|| "the tool server's stream has no answer".to_owned())?
        } else {
            serde_json::from_str(&text)
                .map_err(|_| "the tool server's answer is not JSON".to_owned())?
        };
        if let Some(error) = message.get("error") {
            return Err(error["message"]
                .as_str()
                .unwrap_or("the tool server returned an error")
                .to_owned());
        }
        message
            .get("result")
            .cloned()
            .ok_or_else(|| "the tool server's answer has no result".to_owned())
    }
    async fn open<'a>(
        client: &'a reqwest::Client,
        server: &'a McpServerConfig,
        timeout: Duration,
        grant_token: Option<&'a str>,
    ) -> Result<McpClient<'a>, String> {
        let mut c = McpClient {
            client,
            server,
            session: None,
            timeout,
            grant_token,
            answered: false,
            stale: false,
        };
        c.call(1, "initialize", json!({"protocolVersion":"2025-03-26","capabilities":{},"clientInfo":{"name":"agent-ti","version":env!("CARGO_PKG_VERSION")}}))
            .await?;
        let response = c
            .request(&json!({"jsonrpc":"2.0","method":"notifications/initialized"}))
            .send()
            .await
            .map_err(|e| e.to_string())?;
        if !response.status().is_success() {
            return Err(format!("initialized: HTTP {}", response.status()));
        }
        Ok(c)
    }
}
pub async fn mcp_list(
    client: &reqwest::Client,
    server: &McpServerConfig,
) -> Result<Vec<McpTool>, String> {
    let mut c = McpClient::open(client, server, Duration::from_secs(10), None).await?;
    let mut tools = Vec::new();
    let mut cursor: Option<String> = None;
    for page in 0..20 {
        let params = cursor.as_ref().map_or(json!({}), |c| json!({"cursor":c}));
        let result = c.call(2 + page, "tools/list", params).await?;
        for t in result["tools"].as_array().into_iter().flatten() {
            let Some(name) = t["name"].as_str() else {
                continue;
            };
            let description = t["description"]
                .as_str()
                .or(t["title"].as_str())
                .unwrap_or(name)
                .trim();
            tools.push(McpTool {
                name: name.into(),
                description: description.into(),
                input_schema: if t["inputSchema"].is_object() {
                    t["inputSchema"].clone()
                } else {
                    json!({"type":"object"})
                },
                owner: t["_meta"]["t10/module"]
                    .as_str()
                    .filter(|o| !o.is_empty())
                    .map(str::to_owned),
            });
        }
        cursor = result["nextCursor"].as_str().map(str::to_owned);
        if cursor.is_none() {
            break;
        }
    }
    Ok(tools)
}
/// Calls a tool. A failure of the call (an MCP error, `isError`, a timeout, an unreachable server)
/// is a result with `is_error` for the model, not an error of the run.
pub async fn mcp_call(
    client: &reqwest::Client,
    server: &McpServerConfig,
    name: &str,
    args: Value,
    grant_token: Option<&str>,
    meta: Option<Value>,
) -> ToolOutcome {
    let timeout = Duration::from_millis(server.call_timeout_ms);
    let mut params = json!({"name": name, "arguments": args});
    if let Some(m) = meta {
        params["_meta"] = m;
    }
    let key = session_key(server, grant_token);
    let result = async {
        if let Some(session) = kept_session(&key) {
            let mut c = McpClient {
                client,
                server,
                session,
                timeout,
                grant_token,
                answered: false,
                stale: false,
            };
            let result = c.call(2, "tools/call", params.clone()).await;
            // A session the server has ended didn't run the call, so it goes again on a new one.
            if !c.stale {
                settle_session(key, &c);
                return result;
            }
            SESSIONS.lock().unwrap().remove(&key);
        }
        let mut c = McpClient::open(client, server, timeout, grant_token).await?;
        let result = c.call(2, "tools/call", params).await;
        settle_session(key, &c);
        result
    }
    .await;
    match result {
        Err(message) => ToolOutcome::error(message),
        Ok(result) => {
            let blocks = result["content"].as_array().cloned().unwrap_or_default();
            let text: Vec<String> = blocks
                .iter()
                .map(|b| match b["type"].as_str() {
                    Some("text") => b["text"].as_str().unwrap_or("").to_owned(),
                    _ => b.to_string(),
                })
                .collect();
            let content = if text.is_empty() && result.get("structuredContent").is_some() {
                result["structuredContent"].to_string()
            } else {
                text.join("\n")
            };
            ToolOutcome {
                content,
                is_error: result["isError"] == true,
            }
        }
    }
}

/// pi-ai's `isContextOverflow` for an error body: a provider's way of saying the request is longer
/// than the model's context window, and not a rate limit.
pub fn context_overflow(body: &str) -> bool {
    static OVERFLOW: std::sync::LazyLock<regex::Regex> = std::sync::LazyLock::new(|| {
        regex::Regex::new(concat!(
            r"(?i)prompt (?:is )?too long|prompt exceeds max length|request_too_large|",
            r"input is too long for requested model|exceeds the context window|",
            r"exceeds (?:the )?(?:model'?s )?maximum context length(?: of [\d,]+ tokens?|\s*\([\d,]+\))|",
            r"input token count.*exceeds the maximum|maximum prompt length is \d+|",
            r"reduce the length of the messages|maximum context length is \d+ tokens|",
            r"exceeds (?:the )?maximum allowed input length of [\d,]+ tokens?|",
            r"input \(\d+ tokens\) is longer than the model'?s context length \(\d+ tokens\)|",
            r"exceeds the limit of \d+|exceeds the available context size|greater than the context length|",
            r"context window exceeds limit|exceeded model token limit|too large for model with \d+ maximum context length|",
            r"prompt has [\d,]+ tokens?, but the configured context size is [\d,]+ tokens?|",
            r"model_context_window_exceeded|prompt too long; exceeded (?:max )?context length|",
            r"range of input length should be|context[_ ]length[_ ]exceeded|too many tokens|token limit exceeded"
        ))
        .unwrap()
    });
    static NOT: std::sync::LazyLock<regex::Regex> = std::sync::LazyLock::new(|| {
        regex::Regex::new(
            r"(?i)^(Throttling error|Service unavailable):|rate limit|too many requests",
        )
        .unwrap()
    });
    let mut end = body.len().min(64 * 1024);
    while !body.is_char_boundary(end) {
        end -= 1;
    }
    let body = &body[..end];
    !NOT.is_match(body) && OVERFLOW.is_match(body)
}
