//! agent-pi's `flow/engine.ts`: a skill's deterministic flow, run by `load_skill` with `query`.
//! Extraction, branching and tool calls happen here without the model, and what they found goes back
//! to it as one `<flow_evidence>` block. A skill's tools and conditions come from `flow.json`;
//! conditions are parsed by a small comparison grammar, never evaluated.
use agent_ti_config::flow::{
    FlowExtractField, FlowNode, SkillFlow, check_flow, extract_field, extract_pattern,
};
use agent_ti_core::BoxFuture;
use futures_util::future::join_all;
use regex::Regex;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use std::collections::HashMap;
use std::sync::LazyLock;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

/// How much of a message an `extract` pattern reads.
const MAX_PATTERN_INPUT: usize = 2000;

#[derive(Clone, Debug)]
pub struct ToolCallOutcome {
    pub output: String,
    pub is_error: bool,
}

/// What a flow needs from the session it runs in.
pub trait FlowHost: Send + Sync {
    /// Calls a tool. `step` names the call within the flow: the node, and for a parallel call the
    /// item, so a journal can tell a flow's calls apart.
    fn call_tool(&self, step: String, name: String, args: Value) -> BoxFuture<'_, ToolCallOutcome>;
    fn update(&self, trace: FlowRunTrace) -> BoxFuture<'_, ()>;
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FlowStepToolCall {
    pub name: String,
    pub status: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub duration_ms: Option<u64>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FlowStepTrace {
    pub step_id: String,
    pub status: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub duration_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub summary: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub branch_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tool_calls: Option<Vec<FlowStepToolCall>>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FlowRunTrace {
    pub flow_id: String,
    pub skill: String,
    pub title: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title_zh: Option<String>,
    pub mode: String,
    pub status: String,
    pub started_at: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub total_duration_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub llm_calls_saved: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parallel_tools_run: Option<u32>,
    pub steps: Vec<FlowStepTrace>,
}

#[derive(Clone, Debug)]
pub struct FlowExecution {
    pub status: String,
    pub trace: FlowRunTrace,
    pub vars: Map<String, Value>,
    pub steps: Map<String, Value>,
    /// Set when an `ask_user` node stopped the flow to ask for something.
    pub immediate_reply: Option<String>,
    /// What the flow found, for the model's final answer.
    pub evidence: Option<String>,
    pub tools_used: Vec<String>,
}

/// `zh` for a message with Chinese in it, else `en`.
pub fn language_of(text: &str) -> &'static str {
    if text.chars().any(|c| ('\u{4e00}'..='\u{9fa5}').contains(&c)) {
        "zh"
    } else {
        "en"
    }
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_millis() as u64)
}

fn elapsed(since: Instant) -> u64 {
    (since.elapsed().as_millis() as u64).max(1)
}

fn extract_value(input: &str, field: &FlowExtractField) -> Option<Value> {
    if let Some(keywords) = &field.keywords {
        let lowered = input.to_lowercase();
        for (keyword, mapped) in keywords {
            if lowered.contains(&keyword.to_lowercase()) {
                return Some(mapped.clone());
            }
        }
    }
    if let Some(pattern) = &field.pattern {
        // Bounded, because a tenant writes the pattern and it runs in the shared process.
        let end = input
            .char_indices()
            .nth(MAX_PATTERN_INPUT)
            .map_or(input.len(), |(at, _)| at);
        // Checked when the skill was loaded.
        if let Ok(regex) = extract_pattern(pattern)
            && let Some(found) = regex.captures(&input[..end])
        {
            let captured = found
                .get(1)
                .or_else(|| found.get(0))
                .map(|m| m.as_str().trim());
            if let Some(text) = captured.filter(|t| !t.is_empty()) {
                return Some(json!(text));
            }
        }
    }
    if field.fallback_input == Some(true) {
        return Some(json!(clean_query_input(input)));
    }
    field.default.clone()
}

static POLITE_PREFIX: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"^(请|帮我|麻烦|能|可以)?(准备|生成|整理|查询|查一下|看看|看看关于|获取|做一份)?")
        .unwrap()
});
static TOPIC_SUFFIX: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(的)?(客户)?(拜访简报|拜访准备|拜访材料|沟通材料|客户背景|项目交流|天气预报|天气|销售日报|销售周报|销售月报).*$").unwrap()
});
static WEATHER_PREFIX: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?i)^(what('s| is) the )?(weather|forecast) (in|for|at) ").unwrap()
});
static TRAILING_MARKS: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"[?？!！。.，,\s]+$").unwrap());

/// Strips common conversational prefixes and suffixes to leave an entity name or query.
pub fn clean_query_input(input: &str) -> String {
    let text = POLITE_PREFIX.replace(input, "");
    let text = TOPIC_SUFFIX.replace(&text, "");
    let text = WEATHER_PREFIX.replace(&text, "");
    let text = TRAILING_MARKS.replace(&text, "");
    let cleaned = text.trim();
    if cleaned.is_empty() {
        input.trim().to_string()
    } else {
        cleaned.to_string()
    }
}

/// What a flow's expressions can read.
pub struct FlowContext<'a> {
    pub vars: &'a Map<String, Value>,
    pub steps: &'a Map<String, Value>,
    pub input: &'a str,
}

fn lookup_path(path: &str, context: &FlowContext) -> Option<Value> {
    let trimmed = path.trim();
    if trimmed == "$input" || trimmed == "input" {
        return Some(json!(context.input));
    }
    let mut parts = trimmed.strip_prefix('$').unwrap_or(trimmed).split('.');
    let root = match parts.next()? {
        "vars" => context.vars,
        "steps" => context.steps,
        _ => return None,
    };
    let Some(first) = parts.next() else {
        return Some(Value::Object(root.clone()));
    };
    let mut cursor = root.get(first)?;
    for part in parts {
        cursor = match cursor {
            Value::Object(map) => map.get(part)?,
            Value::Array(items) if part == "length" => return Some(json!(items.len())),
            Value::Array(items) => items.get(part.parse::<usize>().ok()?)?,
            _ => return None,
        };
    }
    Some(cursor.clone())
}

static TEMPLATE: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"\{\{\s*([^}]+?)\s*\}\}").unwrap());

/// `None` is JavaScript's `undefined`: a path that led nowhere.
fn resolve_template(value: &Value, context: &FlowContext) -> Option<Value> {
    match value {
        Value::String(text) if text.starts_with('$') => lookup_path(text, context),
        Value::String(text) => {
            let filled = TEMPLATE.replace_all(text, |found: &regex::Captures| {
                match lookup_path(&found[1], context) {
                    None | Some(Value::Null) => String::new(),
                    Some(Value::String(text)) => text,
                    Some(other) => other.to_string(),
                }
            });
            Some(json!(filled))
        }
        Value::Array(items) => Some(Value::Array(
            items
                .iter()
                .map(|item| resolve_template(item, context).unwrap_or(Value::Null))
                .collect(),
        )),
        Value::Object(map) => {
            let mut out = Map::new();
            for (key, item) in map {
                match resolve_template(item, context) {
                    None => {}
                    Some(Value::String(text)) if text.is_empty() => {}
                    Some(resolved) => {
                        out.insert(key.clone(), resolved);
                    }
                }
            }
            Some(Value::Object(out))
        }
        other => Some(other.clone()),
    }
}

fn resolve_args(args: Option<&Value>, context: &FlowContext) -> Value {
    args.and_then(|a| resolve_template(a, context))
        .unwrap_or_else(|| json!({}))
}

fn truthy(value: Option<&Value>) -> bool {
    match value {
        None | Some(Value::Null) => false,
        Some(Value::Bool(b)) => *b,
        Some(Value::Number(n)) => n.as_f64().is_some_and(|n| n != 0.0),
        Some(Value::String(s)) => !s.is_empty(),
        Some(_) => true,
    }
}

static COMPARISON: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^([a-zA-Z0-9_$.]+)\s*(==|!=|>=|<=|>|<)\s*(.+)$").unwrap());

/// Simple expressions such as `steps.lookup.count > 1` or `vars.period == 'weekly'`.
pub fn evaluate_condition(expr: &str, context: &FlowContext) -> bool {
    if expr.contains("||") {
        return expr
            .split("||")
            .any(|part| evaluate_condition(part, context));
    }
    if expr.contains("&&") {
        return expr
            .split("&&")
            .all(|part| evaluate_condition(part, context));
    }
    let Some(found) = COMPARISON.captures(expr.trim()) else {
        return truthy(lookup_path(expr.trim(), context).as_ref());
    };
    let lhs = lookup_path(&found[1], context);
    let raw = found[3].trim();
    let quoted = raw.len() >= 2
        && ((raw.starts_with('\'') && raw.ends_with('\''))
            || (raw.starts_with('"') && raw.ends_with('"')));
    let rhs = if quoted {
        Some(json!(raw[1..raw.len() - 1]))
    } else {
        match raw {
            "true" => Some(json!(true)),
            "false" => Some(json!(false)),
            "null" | "undefined" => None,
            _ => match raw.parse::<f64>() {
                Ok(n) if n.is_finite() => Some(json!(n)),
                _ => Some(json!(raw)),
            },
        }
    };
    let numbers = lhs
        .as_ref()
        .and_then(Value::as_f64)
        .zip(rhs.as_ref().and_then(Value::as_f64));
    let equal = match numbers {
        Some((l, r)) => l == r,
        None => lhs == rhs,
    };
    match &found[2] {
        "==" => equal,
        "!=" => !equal,
        ">" => numbers.is_some_and(|(l, r)| l > r),
        "<" => numbers.is_some_and(|(l, r)| l < r),
        ">=" => numbers.is_some_and(|(l, r)| l >= r),
        "<=" => numbers.is_some_and(|(l, r)| l <= r),
        _ => false,
    }
}

/// What later steps can read of a tool's output: `count`, `items`, `firstId`, `firstName`, ...
fn summarize_output(raw: &str, is_error: bool) -> (Value, String) {
    let trimmed = raw.trim();
    let parsed = (trimmed.starts_with('{') || trimmed.starts_with('['))
        .then(|| serde_json::from_str::<Value>(raw).ok())
        .flatten();
    let objects = |list: &Vec<Value>| -> Vec<Value> {
        list.iter().filter(|v| v.is_object()).cloned().collect()
    };
    let mut items = Vec::new();
    match &parsed {
        Some(Value::Array(list)) => items = objects(list),
        Some(Value::Object(record)) => {
            let keys = [
                "items",
                "companies",
                "contacts",
                "deals",
                "activities",
                "documents",
                "cases",
                "results",
                "data",
            ];
            if let Some(list) = keys.iter().find_map(|k| record.get(*k)?.as_array()) {
                items = objects(list);
            }
            if items.is_empty()
                && ["id", "name", "companyId"]
                    .iter()
                    .any(|k| record.contains_key(*k))
            {
                items = vec![Value::Object(record.clone())];
            }
        }
        _ => {}
    }
    let text_of = |item: &Value, keys: &[&str]| -> Option<String> {
        keys.iter()
            .find_map(|k| item.get(*k)?.as_str().map(str::to_string))
    };
    let names: Vec<String> = items
        .iter()
        .enumerate()
        .map(|(index, item)| {
            let label = text_of(item, &["name", "title"]).unwrap_or(format!("#{}", index + 1));
            let extra = text_of(item, &["industry", "city"])
                .map_or(String::new(), |extra| format!(" ({extra})"));
            format!("{}. {label}{extra}", index + 1)
        })
        .collect();
    let summary = if items.is_empty() {
        raw.chars().take(80).collect()
    } else {
        format!("{} record(s)", items.len())
    };
    let mut info = json!({
        "count": items.len(),
        "items": items,
        "candidateNames": names.join("\n"),
        "summary": summary,
        "data": parsed.unwrap_or_else(|| json!(raw)),
        "isError": is_error,
        "raw": raw,
    });
    if let Some(id) = items_first(&info, &["id", "companyId"]) {
        info["firstId"] = json!(id);
    }
    if let Some(name) = items_first(&info, &["name", "title"]) {
        info["firstName"] = json!(name);
    }
    (info, summary)
}

fn items_first(info: &Value, keys: &[&str]) -> Option<String> {
    let first = info["items"].get(0)?;
    keys.iter()
        .find_map(|k| first.get(*k)?.as_str().map(str::to_string))
}

/// One run's progress through the graph.
struct Walk<'a> {
    flow: &'a SkillFlow,
    started_at: u64,
    started: Instant,
    traces: HashMap<&'a str, FlowStepTrace>,
    edge_state: HashMap<&'a str, &'static str>,
    out_edges: HashMap<&'a str, Vec<usize>>,
    in_edges: HashMap<&'a str, Vec<usize>>,
    llm_calls_saved: u32,
    parallel_tools_run: u32,
}

impl<'a> Walk<'a> {
    fn new(flow: &'a SkillFlow) -> Self {
        let mut out_edges: HashMap<&str, Vec<usize>> = HashMap::new();
        let mut in_edges: HashMap<&str, Vec<usize>> = HashMap::new();
        for (index, edge) in flow.edges.iter().enumerate() {
            out_edges.entry(&edge.source).or_default().push(index);
            in_edges.entry(&edge.target).or_default().push(index);
        }
        Self {
            flow,
            started_at: now_ms(),
            started: Instant::now(),
            traces: flow
                .nodes
                .iter()
                .map(|node| {
                    let trace = FlowStepTrace {
                        step_id: node.id.clone(),
                        status: "pending".into(),
                        ..Default::default()
                    };
                    (node.id.as_str(), trace)
                })
                .collect(),
            edge_state: flow
                .edges
                .iter()
                .map(|e| (e.id.as_str(), "pending"))
                .collect(),
            out_edges,
            in_edges,
            llm_calls_saved: 0,
            parallel_tools_run: 0,
        }
    }
    fn snapshot(&self, status: &str) -> FlowRunTrace {
        FlowRunTrace {
            flow_id: self.flow.id.clone(),
            skill: self.flow.id.clone(),
            title: self.flow.title.clone(),
            title_zh: self.flow.title_zh.clone(),
            mode: self.flow.mode.clone(),
            status: status.into(),
            started_at: self.started_at,
            total_duration_ms: Some(elapsed(self.started)),
            llm_calls_saved: Some(self.llm_calls_saved),
            parallel_tools_run: Some(self.parallel_tools_run),
            steps: self
                .flow
                .nodes
                .iter()
                .map(|node| self.traces[node.id.as_str()].clone())
                .collect(),
        }
    }
    fn status(&self, node: &str) -> &str {
        self.traces.get(node).map_or("", |t| t.status.as_str())
    }
    fn set(&mut self, node: &'a str, trace: FlowStepTrace) {
        self.traces.insert(
            node,
            FlowStepTrace {
                step_id: node.to_string(),
                ..trace
            },
        );
    }
    fn incoming_all(&self, node: &str, state: &str) -> bool {
        self.in_edges
            .get(node)
            .into_iter()
            .flatten()
            .all(|&i| self.edge_state.get(self.flow.edges[i].id.as_str()) == Some(&state))
    }
    fn incoming_any_pending(&self, node: &str) -> bool {
        self.in_edges
            .get(node)
            .into_iter()
            .flatten()
            .any(|&i| self.edge_state.get(self.flow.edges[i].id.as_str()) == Some(&"pending"))
    }
    /// Skips a node and what only it leads to, and returns the nodes this leaves ready: a step
    /// another branch reached can be waiting on nothing but this one's edge.
    fn propagate_skip(&mut self, node: &'a str) -> Vec<&'a str> {
        if self.status(node) == "pending" {
            self.set(
                node,
                FlowStepTrace {
                    status: "skipped".into(),
                    ..Default::default()
                },
            );
        }
        let flow = self.flow;
        let mut ready = Vec::new();
        for &index in self.out_edges.get(node).cloned().iter().flatten() {
            let edge = &flow.edges[index];
            if self.edge_state.get(edge.id.as_str()) == Some(&"pending") {
                self.edge_state.insert(&edge.id, "skipped");
                if self.incoming_all(&edge.target, "skipped") {
                    ready.extend(self.propagate_skip(&edge.target));
                } else if !self.incoming_any_pending(&edge.target)
                    && self.status(&edge.target) == "pending"
                {
                    ready.push(edge.target.as_str());
                }
            }
        }
        ready
    }
    /// Marks a node's way out, and returns the nodes that are now ready to run.
    fn activate(&mut self, node: &str, chosen: Option<&str>) -> Vec<&'a str> {
        let flow = self.flow;
        let edges = self.out_edges.get(node).cloned().unwrap_or_default();
        for &index in &edges {
            let edge = &flow.edges[index];
            let taken =
                chosen.is_none_or(|c| edge.target == c || edge.branch.as_deref() == Some(c));
            self.edge_state
                .insert(&edge.id, if taken { "active" } else { "skipped" });
        }
        let mut ready = Vec::new();
        for &index in &edges {
            let target = flow.edges[index].target.as_str();
            if self.incoming_any_pending(target) {
                continue;
            }
            if self.incoming_all(target, "skipped") {
                ready.extend(self.propagate_skip(target));
            } else if self.status(target) == "pending" {
                ready.push(target);
            }
        }
        ready
    }
}

/// Runs a deterministic flow: `extract` without a model, `tool` and `parallel` calls directly,
/// `switch` with its untaken branches skipped, `ask_user` stopping the flow with a reply. An error
/// is a flow that fails its checks.
pub async fn execute_flow(
    flow: &SkillFlow,
    input: &str,
    language: &str,
    host: &dyn FlowHost,
) -> Result<FlowExecution, String> {
    check_flow(flow)?;
    let mut vars = Map::new();
    let mut steps = Map::new();
    let mut tools_used: Vec<String> = Vec::new();
    let mut walk = Walk::new(flow);
    let nodes: HashMap<&str, &FlowNode> = flow.nodes.iter().map(|n| (n.id.as_str(), n)).collect();

    let mut queue: std::collections::VecDeque<&str> = flow
        .nodes
        .iter()
        .filter(|node| !walk.in_edges.contains_key(node.id.as_str()))
        .map(|node| node.id.as_str())
        .collect();

    while let Some(id) = queue.pop_front() {
        let Some(&node) = nodes.get(id) else { continue };
        let id = node.id.as_str();
        if walk.status(id) != "pending" {
            continue;
        }
        let step_start = Instant::now();
        walk.set(
            id,
            FlowStepTrace {
                status: "running".into(),
                ..Default::default()
            },
        );
        host.update(walk.snapshot("running")).await;

        match node.kind.as_str() {
            "start" => {
                walk.set(
                    id,
                    FlowStepTrace {
                        status: "completed".into(),
                        duration_ms: Some(elapsed(step_start)),
                        ..Default::default()
                    },
                );
                queue.extend(walk.activate(id, None));
            }
            "extract" => {
                for (key, spec) in node.extract.iter().flatten() {
                    let spec = extract_field(spec)?;
                    if let Some(found) = extract_value(input, &spec) {
                        vars.insert(key.clone(), found);
                    }
                }
                steps.insert(id.to_string(), Value::Object(vars.clone()));
                let summary: Vec<String> = vars
                    .iter()
                    .map(|(k, v)| {
                        format!("{k}={}", v.as_str().map_or(v.to_string(), str::to_string))
                    })
                    .collect();
                walk.set(
                    id,
                    FlowStepTrace {
                        status: "completed".into(),
                        duration_ms: Some(elapsed(step_start)),
                        summary: Some(summary.join(", ")),
                        ..Default::default()
                    },
                );
                queue.extend(walk.activate(id, None));
            }
            "tool" if node.call.is_some() => {
                let call = node.call.as_ref().unwrap();
                let args = {
                    let context = FlowContext {
                        vars: &vars,
                        steps: &steps,
                        input,
                    };
                    resolve_args(call.args.as_ref(), &context)
                };
                walk.set(
                    id,
                    FlowStepTrace {
                        status: "running".into(),
                        tool_calls: Some(vec![FlowStepToolCall {
                            name: call.tool.clone(),
                            status: "running".into(),
                            duration_ms: None,
                        }]),
                        ..Default::default()
                    },
                );
                host.update(walk.snapshot("running")).await;
                let call_start = Instant::now();
                let outcome = host
                    .call_tool(id.to_string(), call.tool.clone(), args)
                    .await;
                let duration = elapsed(call_start);
                tools_used.push(call.tool.clone());
                walk.llm_calls_saved += 1;
                let (info, summary) = summarize_output(&outcome.output, outcome.is_error);
                let key = call.store_as.as_deref().unwrap_or(id);
                steps.insert(key.to_string(), info.clone());
                steps.insert(id.to_string(), info);
                walk.set(
                    id,
                    FlowStepTrace {
                        status: if outcome.is_error {
                            "failed"
                        } else {
                            "completed"
                        }
                        .into(),
                        duration_ms: Some(duration),
                        summary: Some(summary),
                        tool_calls: Some(vec![FlowStepToolCall {
                            name: call.tool.clone(),
                            status: if outcome.is_error { "failed" } else { "done" }.into(),
                            duration_ms: Some(duration),
                        }]),
                        ..Default::default()
                    },
                );
                queue.extend(walk.activate(id, None));
            }
            "parallel" if node.parallel.is_some() => {
                let items = node.parallel.as_ref().unwrap();
                let (runnable, not_applicable): (Vec<_>, Vec<_>) = {
                    let context = FlowContext {
                        vars: &vars,
                        steps: &steps,
                        input,
                    };
                    items.iter().partition(|item| {
                        item.when
                            .as_deref()
                            .is_none_or(|when| evaluate_condition(when, &context))
                    })
                };
                let skipped_calls: Vec<FlowStepToolCall> = not_applicable
                    .iter()
                    .map(|item| FlowStepToolCall {
                        name: item.tool.clone(),
                        status: "skipped".into(),
                        duration_ms: None,
                    })
                    .collect();
                let running = runnable.iter().map(|item| FlowStepToolCall {
                    name: item.tool.clone(),
                    status: "running".into(),
                    duration_ms: None,
                });
                walk.set(
                    id,
                    FlowStepTrace {
                        status: "running".into(),
                        tool_calls: Some(running.chain(skipped_calls.iter().cloned()).collect()),
                        ..Default::default()
                    },
                );
                host.update(walk.snapshot("running")).await;
                let calls = {
                    let context = FlowContext {
                        vars: &vars,
                        steps: &steps,
                        input,
                    };
                    runnable
                        .iter()
                        .map(|item| {
                            let args = resolve_args(item.args.as_ref(), &context);
                            let step = format!("{id}:{}", item.id);
                            async move {
                                let call_start = Instant::now();
                                let outcome = host.call_tool(step, item.tool.clone(), args).await;
                                (*item, outcome, elapsed(call_start))
                            }
                        })
                        .collect::<Vec<_>>()
                };
                let outcomes = join_all(calls).await;
                let mut result = Map::new();
                let mut call_traces = Vec::new();
                for (item, outcome, duration) in outcomes {
                    tools_used.push(item.tool.clone());
                    let key = item.store_as.as_deref().unwrap_or(&item.id);
                    let (info, _) = summarize_output(&outcome.output, outcome.is_error);
                    result.insert(key.to_string(), info.clone());
                    steps.insert(key.to_string(), info);
                    call_traces.push(FlowStepToolCall {
                        name: item.tool.clone(),
                        status: if outcome.is_error { "failed" } else { "done" }.into(),
                        duration_ms: Some(duration),
                    });
                }
                call_traces.extend(skipped_calls);
                if !runnable.is_empty() {
                    walk.llm_calls_saved += 1;
                    walk.parallel_tools_run += runnable.len() as u32;
                }
                steps.insert(id.to_string(), Value::Object(result));
                walk.set(
                    id,
                    FlowStepTrace {
                        status: if runnable.is_empty() {
                            "skipped"
                        } else {
                            "completed"
                        }
                        .into(),
                        duration_ms: Some(elapsed(step_start)),
                        summary: Some(if runnable.is_empty() {
                            "Skipped (not applicable)".into()
                        } else {
                            format!("{} tools in parallel", runnable.len())
                        }),
                        tool_calls: Some(call_traces),
                        ..Default::default()
                    },
                );
                queue.extend(walk.activate(id, None));
            }
            "switch" => {
                let context = FlowContext {
                    vars: &vars,
                    steps: &steps,
                    input,
                };
                let branch = node
                    .branches
                    .iter()
                    .flatten()
                    .find(|branch| evaluate_condition(&branch.when, &context));
                let chosen = branch
                    .map(|b| b.target.clone())
                    .or_else(|| node.default_target.clone());
                let label = branch.map_or("default".to_string(), |b| b.label.clone());
                walk.set(
                    id,
                    FlowStepTrace {
                        status: "completed".into(),
                        duration_ms: Some(elapsed(step_start)),
                        branch_id: chosen.clone(),
                        summary: Some(label),
                        ..Default::default()
                    },
                );
                queue.extend(walk.activate(id, chosen.as_deref()));
            }
            "ask_user" => {
                let (first, second, otherwise) = if language == "en" {
                    (
                        &node.prompt_template,
                        &node.prompt_template_zh,
                        "Please clarify your selection.",
                    )
                } else {
                    (
                        &node.prompt_template_zh,
                        &node.prompt_template,
                        "请确认您要选择的对象。",
                    )
                };
                let template = first.as_deref().or(second.as_deref()).unwrap_or(otherwise);
                let context = FlowContext {
                    vars: &vars,
                    steps: &steps,
                    input,
                };
                let reply = match resolve_template(&json!(template), &context) {
                    Some(Value::String(text)) => text,
                    Some(other) => other.to_string(),
                    None => "undefined".to_string(),
                };
                walk.llm_calls_saved += 1;
                walk.set(
                    id,
                    FlowStepTrace {
                        status: "needs_input".into(),
                        duration_ms: Some(elapsed(step_start)),
                        summary: Some("Waiting for user disambiguation".into()),
                        ..Default::default()
                    },
                );
                for trace in walk.traces.values_mut() {
                    if trace.status == "pending" {
                        trace.status = "skipped".into();
                    }
                }
                let trace = walk.snapshot("needs_input");
                host.update(trace.clone()).await;
                return Ok(FlowExecution {
                    status: "needs_input".into(),
                    trace,
                    vars,
                    steps,
                    immediate_reply: Some(reply),
                    evidence: None,
                    tools_used,
                });
            }
            "llm_reply" => {
                walk.set(
                    id,
                    FlowStepTrace {
                        status: "completed".into(),
                        duration_ms: Some(elapsed(step_start)),
                        summary: Some("Evidence assembled for synthesis".into()),
                        ..Default::default()
                    },
                );
                queue.extend(walk.activate(id, None));
            }
            // A `tool` or `parallel` node with nothing to call is for drawing only, and goes no further.
            _ => continue,
        }
        host.update(walk.snapshot("running")).await;
    }

    let trace = walk.snapshot("completed");
    host.update(trace.clone()).await;

    let mut lines = vec![
        format!(
            "<flow_evidence skill=\"{}\" mode=\"deterministic\" duration_ms=\"{}\" llm_calls_saved=\"{}\">",
            flow.id,
            trace.total_duration_ms.unwrap_or(0),
            walk.llm_calls_saved
        ),
        format!(" Extracted Variables: {}", Value::Object(vars.clone())),
    ];
    // A result stored under another name is also kept under its step's, for later steps to read;
    // the model gets it once.
    let renamed = |key: &str| {
        flow.nodes.iter().any(|node| {
            node.id == key
                && node
                    .call
                    .as_ref()
                    .is_some_and(|c| c.store_as.as_deref().is_some_and(|name| name != key))
        })
    };
    for (key, value) in &steps {
        if renamed(key) {
            continue;
        }
        let Some(raw) = value.get("raw").and_then(Value::as_str) else {
            continue;
        };
        let status = if value["isError"] == true {
            "failed"
        } else if value["count"] == 0 {
            "not_found"
        } else {
            "completed"
        };
        lines.push(format!(
            "  <step id=\"{key}\" status=\"{status}\">\n{raw}\n  </step>"
        ));
    }
    lines.push("</flow_evidence>".into());

    let mut unique = Vec::new();
    for tool in tools_used {
        if !unique.contains(&tool) {
            unique.push(tool);
        }
    }
    Ok(FlowExecution {
        status: "completed".into(),
        trace,
        vars,
        steps,
        immediate_reply: None,
        evidence: Some(lines.join("\n")),
        tools_used: unique,
    })
}

/// The trace of a guided flow as `load_skill` starts it: the model does the work from here.
pub fn guided_trace(flow: &SkillFlow, skill: &str) -> FlowRunTrace {
    FlowRunTrace {
        flow_id: flow.id.clone(),
        skill: skill.to_string(),
        title: flow.title.clone(),
        title_zh: flow.title_zh.clone(),
        mode: flow.mode.clone(),
        status: "running".into(),
        started_at: now_ms(),
        total_duration_ms: None,
        llm_calls_saved: None,
        parallel_tools_run: None,
        steps: flow
            .nodes
            .iter()
            .enumerate()
            .map(|(index, node)| FlowStepTrace {
                step_id: node.id.clone(),
                status: match index {
                    0 => "completed",
                    1 => "running",
                    _ => "pending",
                }
                .into(),
                duration_ms: (index == 0).then_some(1),
                ..Default::default()
            })
            .collect(),
    }
}

/// A model-driven tool call moves a running trace on one step.
pub fn advance_after_tool(trace: &mut FlowRunTrace, tool: &str, is_error: bool) {
    let since = now_ms().saturating_sub(trace.started_at).max(1);
    for (index, step) in trace.steps.iter_mut().enumerate() {
        if step.status == "running" || (index == 1 && step.status == "pending") {
            step.status = if is_error { "failed" } else { "completed" }.into();
            step.duration_ms = Some(since);
            step.tool_calls
                .get_or_insert_default()
                .push(FlowStepToolCall {
                    name: tool.to_string(),
                    status: if is_error { "failed" } else { "done" }.into(),
                    duration_ms: None,
                });
        }
    }
    if let Some(next) = trace.steps.iter_mut().find(|s| s.status == "pending") {
        next.status = "running".into();
    }
    trace.total_duration_ms = Some(since);
}

/// The model answered: a running trace is done.
pub fn complete_after_answer(trace: &mut FlowRunTrace) {
    let since = now_ms().saturating_sub(trace.started_at).max(1);
    for step in &mut trace.steps {
        if step.status == "running" || step.step_id == "reply" {
            step.status = "completed".into();
            step.duration_ms.get_or_insert(since);
        }
    }
    trace.status = "completed".into();
    trace.total_duration_ms = Some(since);
}
