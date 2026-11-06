//! agent-pi's `flow/types.ts` and the parts of `flow/engine.ts` that loading a skill needs: checking
//! a `flow.json`, and drawing a guided flow for a skill without one. A skill keeps its flow as JSON,
//! so `skills.list` returns what the file said; running one parses it here.
use regex::{Regex, RegexBuilder};
use serde::Deserialize;
use serde_json::{Map, Value, json};
use std::collections::{HashMap, HashSet};
use std::sync::LazyLock;

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SkillFlow {
    #[serde(default)]
    pub id: String,
    #[serde(default)]
    pub title: String,
    pub title_zh: Option<String>,
    #[serde(default)]
    pub mode: String,
    #[serde(default)]
    pub nodes: Vec<FlowNode>,
    pub edges: Vec<FlowEdge>,
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FlowNode {
    #[serde(default)]
    pub id: String,
    #[serde(default)]
    pub kind: String,
    pub tools: Option<Vec<String>>,
    /// In file order: the first field to extract first, as `vars` lists them.
    pub extract: Option<serde_json::Map<String, Value>>,
    pub call: Option<FlowCall>,
    pub parallel: Option<Vec<FlowParallelCall>>,
    pub branches: Option<Vec<FlowBranchRule>>,
    pub default_target: Option<String>,
    pub prompt_template: Option<String>,
    pub prompt_template_zh: Option<String>,
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FlowExtractField {
    /// A regular expression with a capture group, applied to the user's message.
    pub pattern: Option<String>,
    /// In file order: the first keyword found in the message gives the value.
    pub keywords: Option<Map<String, Value>>,
    /// Use the cleaned message when no pattern or keyword matched.
    pub fallback_input: Option<bool>,
    pub default: Option<Value>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FlowCall {
    pub tool: String,
    pub args: Option<Value>,
    pub store_as: Option<String>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FlowParallelCall {
    pub id: String,
    pub tool: String,
    pub args: Option<Value>,
    pub when: Option<String>,
    pub store_as: Option<String>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FlowBranchRule {
    #[serde(default)]
    pub id: String,
    #[serde(default)]
    pub label: String,
    pub when: String,
    pub target: String,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FlowEdge {
    #[serde(default)]
    pub id: String,
    pub source: String,
    pub target: String,
    pub branch: Option<String>,
}

/// An `extract` pattern, as it runs: case-insensitive. The `regex` crate has no lookaround or
/// backreferences, so a pattern using them fails here rather than when a user reaches it.
pub fn extract_pattern(pattern: &str) -> Result<Regex, String> {
    RegexBuilder::new(pattern)
        .case_insensitive(true)
        .build()
        .map_err(|e| e.to_string())
}

/// Checks unique node IDs, edge endpoints, switch targets and acyclicity, and returns every tool
/// the flow names, in the order it names them.
pub fn check_flow(flow: &SkillFlow) -> Result<Vec<String>, String> {
    let id = &flow.id;
    if id.is_empty() {
        return Err("SkillFlow requires a non-empty id".into());
    }
    if flow.nodes.is_empty() {
        return Err(format!("SkillFlow {id}: nodes must be non-empty"));
    }
    let mut seen = HashSet::new();
    let mut tools = Vec::new();
    for node in &flow.nodes {
        if node.id.is_empty() {
            return Err(format!("SkillFlow {id}: node missing id"));
        }
        if !seen.insert(node.id.as_str()) {
            return Err(format!("SkillFlow {id}: duplicate node id {}", node.id));
        }
        for tool in node_tools(node) {
            if !tools.contains(&tool) {
                tools.push(tool);
            }
        }
    }
    let mut next: HashMap<&str, Vec<&str>> = HashMap::new();
    for edge in &flow.edges {
        for (side, end) in [("source", &edge.source), ("target", &edge.target)] {
            if !seen.contains(end.as_str()) {
                return Err(format!(
                    "SkillFlow {id}: edge {} references unknown {side} {end}",
                    edge.id
                ));
            }
        }
        next.entry(&edge.source).or_default().push(&edge.target);
    }
    for node in &flow.nodes {
        if node.kind == "switch" {
            for branch in node.branches.iter().flatten() {
                if !seen.contains(branch.target.as_str()) {
                    return Err(format!(
                        "SkillFlow {id}: switch {} branch {} targets unknown node {}",
                        node.id, branch.id, branch.target
                    ));
                }
            }
            if let Some(target) = &node.default_target
                && !seen.contains(target.as_str())
            {
                return Err(format!(
                    "SkillFlow {id}: switch {} defaultTarget references unknown node {target}",
                    node.id
                ));
            }
        }
        for (field, spec) in node.extract.iter().flatten() {
            let spec = extract_field(spec)
                .map_err(|e| format!("SkillFlow {id}: step {}: {field}: {e}", node.id))?;
            if let Some(pattern) = &spec.pattern {
                extract_pattern(pattern).map_err(|e| {
                    format!(
                        "SkillFlow {id}: step {}: the pattern for {field} doesn't compile: {e}",
                        node.id
                    )
                })?;
            }
        }
    }
    let mut color: HashMap<&str, u8> = HashMap::new();
    fn visit<'a>(
        node: &'a str,
        next: &HashMap<&'a str, Vec<&'a str>>,
        color: &mut HashMap<&'a str, u8>,
    ) -> Result<(), String> {
        color.insert(node, 1);
        for &target in next.get(node).into_iter().flatten() {
            match color.get(target).copied().unwrap_or(0) {
                1 => return Err(format!("cycle detected involving node {target}")),
                0 => visit(target, next, color)?,
                _ => {}
            }
        }
        color.insert(node, 2);
        Ok(())
    }
    for node in &flow.nodes {
        if !color.contains_key(node.id.as_str()) {
            visit(&node.id, &next, &mut color).map_err(|e| format!("SkillFlow {id}: {e}"))?;
        }
    }
    Ok(tools)
}

pub fn extract_field(spec: &Value) -> Result<FlowExtractField, String> {
    serde_json::from_value(spec.clone()).map_err(|e| e.to_string())
}

fn node_tools(node: &FlowNode) -> Vec<String> {
    let mut tools = node.tools.clone().unwrap_or_default();
    let called = node.call.iter().map(|c| &c.tool);
    for tool in called.chain(node.parallel.iter().flatten().map(|p| &p.tool)) {
        if !tools.contains(tool) {
            tools.push(tool.clone());
        }
    }
    tools
}

/// [`check_flow`] for a flow as JSON. Returns its tools and the flow with each node's `tools`
/// filled in from what it calls; anything else in the JSON stays as it was.
pub fn validate_skill_flow(flow: &Value) -> Result<(Vec<String>, Value), String> {
    let parsed: SkillFlow =
        serde_json::from_value(flow.clone()).map_err(|e| format!("SkillFlow: {e}"))?;
    let tools = check_flow(&parsed)?;
    let mut normalized = flow.clone();
    if let Some(nodes) = normalized.get_mut("nodes").and_then(Value::as_array_mut) {
        for (value, node) in nodes.iter_mut().zip(&parsed.nodes) {
            let named = node_tools(node);
            if !named.is_empty() && node.tools.as_ref().is_none_or(Vec::is_empty) {
                value["tools"] = json!(named);
            }
        }
    }
    Ok((tools, normalized))
}

static HEADING: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^#{2,3}\s+(?:\d+\.\s*)?(.+)$").unwrap());
static NUMBERED: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"^\d+\.\s+(.+)$").unwrap());

/// A `guided` flow drawn from a skill's headings and tools when it has no `flow.json`, so every
/// skill can be shown as a graph.
pub fn synthesize_skill_flow(name: &str, description: &str, tools: &[String], body: &str) -> Value {
    let mut headings: Vec<(String, String)> = Vec::new();
    let mut current: Option<(String, Vec<&str>)> = None;
    for line in body.lines() {
        let line = line.trim();
        if let Some(heading) = HEADING.captures(line) {
            if let Some((title, lines)) = current.take() {
                headings.push((title, lines.join("\n")));
            }
            current = Some((heading[1].trim().to_string(), Vec::new()));
        } else if let Some(numbered) = NUMBERED.captures(line).filter(|_| headings.is_empty()) {
            headings.push((numbered[1].chars().take(48).collect(), line.to_string()));
        } else if let Some((_, lines)) = current.as_mut() {
            lines.push(line);
        }
    }
    if let Some((title, lines)) = current {
        headings.push((title, lines.join("\n")));
    }

    let bare: Vec<&str> = tools
        .iter()
        .map(|t| t.split(':').nth(1).unwrap_or(t))
        .collect();
    let mut nodes = vec![json!({
        "id": "start",
        "kind": "start",
        "label": "User Request",
        "labelZh": "用户请求",
        "description": format!("Match skill: {name}"),
        "descriptionZh": format!("匹配技能：{name}"),
    })];
    let mut edges = Vec::new();
    let mut previous = "start".to_string();
    let mut link = |from: &mut String, to: &str| {
        edges.push(json!({"id": format!("{from}->{to}"), "source": &*from, "target": to}));
        *from = to.to_string();
    };
    if !headings.is_empty() {
        for (index, (title, section)) in headings.iter().take(4).enumerate() {
            let id = format!("step_{}", index + 1);
            let matched: Vec<&str> = bare
                .iter()
                .copied()
                .filter(|tool| !tool.contains('*') && section.contains(tool))
                .collect();
            let assigned = if !matched.is_empty() {
                matched
            } else if index == 0 {
                bare.iter().copied().take(3).collect()
            } else {
                Vec::new()
            };
            let kind = match assigned.len() {
                0 => "extract",
                1 => "tool",
                _ => "parallel",
            };
            let mut node = json!({"id": id, "kind": kind, "label": title, "labelZh": title});
            if !assigned.is_empty() {
                node["tools"] = json!(assigned);
            }
            nodes.push(node);
            link(&mut previous, &id);
        }
    } else if !bare.is_empty() {
        let (kind, label, label_zh) = if bare.len() > 1 {
            (
                "parallel",
                "Execute Skill Tools".to_string(),
                "并行调用技能工具".to_string(),
            )
        } else {
            (
                "tool",
                format!("Call {}", bare[0]),
                format!("调用 {}", bare[0]),
            )
        };
        nodes.push(json!({"id": "tools", "kind": kind, "label": label, "labelZh": label_zh, "tools": bare}));
        link(&mut previous, "tools");
    }
    nodes.push(json!({
        "id": "reply",
        "kind": "llm_reply",
        "label": "Synthesize Response",
        "labelZh": "生成结构化回复",
        "description": description,
        "descriptionZh": description,
    }));
    link(&mut previous, "reply");
    json!({"id": name, "title": name, "titleZh": name, "mode": "guided", "nodes": nodes, "edges": edges})
}
