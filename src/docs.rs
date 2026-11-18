//! This repo's docs for coding agents, over MCP at `/docs/mcp`, as the t10 workspace's
//! `contracts/docs/README.md` describes: overview, search and read, and each doc as a resource.
//! It serves only docs whose front matter says `audience: consumer` and that aren't records,
//! because the route takes no token. build.rs carries the repo's `docs/` in the binary.

use std::{collections::BTreeMap, sync::OnceLock};

use axum::{
    Router,
    body::Bytes,
    http::{HeaderMap, Method, StatusCode, header},
    response::{IntoResponse, Response},
    routing::any,
};
use regex::Regex;
use serde_json::{Value, json};

pub const ROUTE: &str = "/docs/mcp";
const COMPONENT: &str = "agent-ti";
const SUMMARY: &str = "agent-pi's runtime in Rust: several agents per process from the same agent.json, skills and plugin hooks, reached over the /agent contract at /agents/<name>/agent, with SQLite storage.";
const VERSIONS: [&str; 4] = ["2025-11-25", "2025-06-18", "2025-03-26", "2024-11-05"];
const MAX_BODY: usize = 64 * 1024;

#[derive(Clone)]
struct Section {
    heading: String,
    slug: String,
    level: usize,
    start: usize,
    end: usize,
    text: String,
}

#[derive(Clone)]
pub struct Doc {
    pub id: String,
    pub title: String,
    pub audience: String,
    pub kind: String,
    pub status: String,
    pub summary: String,
    body: String,
    sections: Vec<Section>,
}

impl Doc {
    fn meta(&self) -> Value {
        json!({"id": self.id, "title": self.title, "audience": self.audience, "kind": self.kind, "status": self.status, "summary": self.summary})
    }
    fn headings(&self) -> Vec<String> {
        self.sections
            .iter()
            .map(|s| format!("{} {}", "#".repeat(s.level), s.heading))
            .collect()
    }
}

/// A field of simple YAML front matter: a bare or double-quoted string.
fn field(front: &str, key: &str) -> String {
    for line in front.lines() {
        if let Some(rest) = line.strip_prefix(key).and_then(|r| r.strip_prefix(':')) {
            let raw = rest.trim();
            if raw.starts_with('"') {
                return serde_json::from_str(raw).unwrap_or_default();
            }
            return raw.trim_matches('\'').to_owned();
        }
    }
    String::new()
}

fn slug(heading: &str) -> String {
    heading
        .to_lowercase()
        .chars()
        .filter(|c| c.is_alphanumeric() || matches!(c, ' ' | '_' | '-'))
        .map(|c| if c == ' ' { '-' } else { c })
        .collect()
}

fn sections(body: &str) -> Vec<Section> {
    static HEADING: OnceLock<Regex> = OnceLock::new();
    let heading = HEADING.get_or_init(|| Regex::new(r"^(#{1,6})\s+(.+?)\s*#*\s*$").expect("valid"));
    let mut out: Vec<Section> = Vec::new();
    let mut fenced = false;
    let mut offset = 0;
    for line in body.split_inclusive('\n') {
        let trimmed = line.trim();
        if trimmed.starts_with("```") || trimmed.starts_with("~~~") {
            fenced = !fenced;
        } else if !fenced && let Some(m) = heading.captures(line.trim_end_matches('\n')) {
            out.push(Section {
                heading: m[2].to_owned(),
                slug: slug(&m[2]),
                level: m[1].len(),
                start: offset,
                end: body.len(),
                text: String::new(),
            });
        }
        offset += line.len();
    }
    for i in 0..out.len() {
        out[i].end = out[i + 1..]
            .iter()
            .find(|n| n.level <= out[i].level)
            .map_or(body.len(), |n| n.start);
        let next = out.get(i + 1).map_or(body.len(), |n| n.start);
        out[i].text = body[out[i].start..next].to_owned();
    }
    out
}

// The repo's docs as build.rs found them: (path under docs/, text).
include!(concat!(env!("OUT_DIR"), "/docs.rs"));

/// Every served doc, keyed by its path without `.md`. Records were never carried.
pub fn load(files: &[(&str, &str)]) -> Result<BTreeMap<String, Doc>, String> {
    let mut docs = BTreeMap::new();
    for (rel, text) in files {
        let Some(id) = rel.strip_suffix(".md") else {
            continue;
        };
        if id == "records" || id.starts_with("records/") {
            continue;
        }
        let text = text.replace("\r\n", "\n");
        let Some(rest) = text.strip_prefix("---\n") else {
            continue;
        };
        let end = rest
            .find("\n---\n")
            .ok_or_else(|| format!("{rel}: front matter isn't closed"))?;
        let front = &rest[..end];
        let body = rest[end + 5..].trim_start_matches('\n').to_owned();
        let doc = Doc {
            id: id.to_owned(),
            title: field(front, "title"),
            audience: field(front, "audience"),
            kind: field(front, "kind"),
            status: field(front, "status"),
            summary: field(front, "summary"),
            sections: sections(&body),
            body,
        };
        if doc.audience == "consumer" && doc.status != "record" && doc.kind != "record" {
            docs.insert(doc.id.clone(), doc);
        }
    }
    Ok(docs)
}

fn version() -> &'static str {
    option_env!("VERSION").unwrap_or("unknown")
}

struct Served {
    docs: BTreeMap<String, Doc>,
    ids: Vec<String>,
}

fn served() -> &'static Served {
    static SERVED: OnceLock<Served> = OnceLock::new();
    SERVED.get_or_init(|| {
        let docs = load(DOCS).unwrap_or_else(|e| {
            tracing::warn!(error = %e, "docs unavailable");
            BTreeMap::new()
        });
        let order = |k: &str| match k {
            "guide" => 0,
            "reference" => 1,
            "design" => 2,
            _ => 3,
        };
        let mut ids: Vec<String> = docs.keys().cloned().collect();
        ids.sort_by(|a, b| {
            order(&docs[a].kind)
                .cmp(&order(&docs[b].kind))
                .then(a.cmp(b))
        });
        Served { docs, ids }
    })
}

/// The route, for any state: it answers every method itself, so others get 405.
pub fn router<S: Clone + Send + Sync + 'static>() -> Router<S> {
    Router::new().route(ROUTE, any(handle))
}

async fn handle(method: Method, headers: HeaderMap, body: Bytes) -> Response {
    if method != Method::POST {
        return (
            StatusCode::METHOD_NOT_ALLOWED,
            [(header::ALLOW, "POST")],
            "this route takes POST",
        )
            .into_response();
    }
    // The protocol asks a server to refuse what a browser page sent, so a local one can't be
    // reached by DNS rebinding.
    if headers.contains_key(header::ORIGIN) {
        return (StatusCode::FORBIDDEN, "this route doesn't serve browsers").into_response();
    }
    if body.len() > MAX_BODY {
        return (StatusCode::PAYLOAD_TOO_LARGE, "too large").into_response();
    }
    let Ok(m) = serde_json::from_slice::<Value>(&body) else {
        return reply(
            StatusCode::BAD_REQUEST,
            json!({"jsonrpc": "2.0", "id": null, "error": {"code": -32700, "message": "the body isn't a JSON-RPC message"}}),
        );
    };
    let id = m.get("id").cloned().unwrap_or(Value::Null);
    if id.is_null() {
        return StatusCode::ACCEPTED.into_response();
    }
    let method = m.get("method").and_then(Value::as_str);
    if m.get("jsonrpc").and_then(Value::as_str) != Some("2.0") || method.is_none() {
        return reply(
            StatusCode::OK,
            json!({"jsonrpc": "2.0", "id": id, "error": {"code": -32600, "message": "not a JSON-RPC 2.0 request"}}),
        );
    }
    let params = m.get("params").cloned().unwrap_or(Value::Null);
    match answer(method.unwrap_or_default(), &params) {
        Ok(result) => reply(
            StatusCode::OK,
            json!({"jsonrpc": "2.0", "id": id, "result": result}),
        ),
        Err((code, message)) => reply(
            StatusCode::OK,
            json!({"jsonrpc": "2.0", "id": id, "error": {"code": code, "message": message}}),
        ),
    }
}

fn reply(status: StatusCode, body: Value) -> Response {
    (
        status,
        [(header::CONTENT_TYPE, "application/json")],
        body.to_string(),
    )
        .into_response()
}

fn result(text: String, structured: Value) -> Value {
    json!({"content": [{"type": "text", "text": text}], "structuredContent": structured})
}

fn failure(text: String) -> Value {
    json!({"content": [{"type": "text", "text": text}], "isError": true})
}

fn tools() -> Value {
    let c = COMPONENT;
    let object = |required: Value, properties: Value| json!({"type": "object", "properties": properties, "required": required, "additionalProperties": false});
    let read_only = json!({"readOnlyHint": true, "openWorldHint": false});
    json!([
        {"name": "docs_overview", "description": format!("What {c} offers someone building on t10, its version, and every doc it serves with a one-line summary. Start here."),
         "inputSchema": object(json!([]), json!({})), "annotations": read_only},
        {"name": "docs_search", "description": format!("Search {c}'s docs. Answers matching sections, best first, each with the id and section to pass to docs_read."),
         "inputSchema": object(json!(["query"]), json!({
            "query": {"type": "string", "description": "Words to look for. Every word must appear in a doc for it to match."},
            "limit": {"type": "integer", "minimum": 1, "maximum": 50, "description": "At most this many results. Default 10."}})),
         "annotations": read_only},
        {"name": "docs_read", "description": format!("Read one of {c}'s docs, whole or one section of it."),
         "inputSchema": object(json!(["id"]), json!({
            "id": {"type": "string", "description": "The doc's id, as docs_overview or docs_search gives it, such as \"api\"."},
            "section": {"type": "string", "description": "Optional. A heading, or its anchor such as \"work-items\". The section comes with its subsections."}})),
         "annotations": read_only},
    ])
}

fn answer(method: &str, params: &Value) -> Result<Value, (i64, String)> {
    let s = served();
    match method {
        "initialize" => {
            let asked = params
                .get("protocolVersion")
                .and_then(Value::as_str)
                .unwrap_or_default();
            let protocol = VERSIONS
                .iter()
                .find(|v| **v == asked)
                .unwrap_or(&VERSIONS[0]);
            Ok(json!({
                "protocolVersion": protocol,
                "capabilities": {"tools": {}, "resources": {}},
                "serverInfo": {"name": COMPONENT, "title": format!("{COMPONENT} docs"), "version": version()},
                "instructions": format!("The docs of {COMPONENT} for people building on t10. Start with docs_overview, then docs_search or docs_read."),
            }))
        }
        "ping" => Ok(json!({})),
        "tools/list" => Ok(json!({"tools": tools()})),
        "tools/call" => {
            let name = params
                .get("name")
                .and_then(Value::as_str)
                .ok_or((-32602, "params need a tool name".to_owned()))?;
            let args = params
                .get("arguments")
                .cloned()
                .unwrap_or_else(|| json!({}));
            if !args.is_object() {
                return Err((-32602, "arguments must be an object".to_owned()));
            }
            call(s, name, &args)
        }
        "resources/list" => Ok(json!({"resources": s.ids.iter().map(|id| {
            let d = &s.docs[id];
            json!({"uri": format!("t10://{COMPONENT}/{id}"), "name": id, "title": d.title, "description": d.summary, "mimeType": "text/markdown"})
        }).collect::<Vec<_>>()})),
        "resources/read" => {
            let uri = params
                .get("uri")
                .and_then(Value::as_str)
                .unwrap_or_default();
            let doc = uri
                .strip_prefix(&format!("t10://{COMPONENT}/"))
                .and_then(|id| s.docs.get(id))
                .ok_or((-32002, "unknown resource".to_owned()))?;
            Ok(json!({"contents": [{"uri": uri, "mimeType": "text/markdown", "text": doc.body}]}))
        }
        "resources/templates/list" => Ok(json!({"resourceTemplates": []})),
        _ => Err((-32601, "unknown method".to_owned())),
    }
}

fn text_arg(args: &Value, key: &str) -> Result<String, Value> {
    match args.get(key) {
        None | Some(Value::Null) => Ok(String::new()),
        Some(Value::String(v)) => Ok(v.clone()),
        Some(_) => Err(failure(format!("{key} must be a string"))),
    }
}

fn terms(q: &str) -> Vec<String> {
    static WORD: OnceLock<Regex> = OnceLock::new();
    let word = WORD.get_or_init(|| Regex::new(r"[\p{L}\p{N}_./-]+").expect("valid"));
    word.find_iter(&q.to_lowercase())
        .map(|m| m.as_str().to_owned())
        .filter(|w| w.chars().count() > 1 || !w.is_ascii())
        .collect()
}

fn snippet(text: &str, ts: &[String]) -> String {
    let lower = text.to_lowercase();
    let at = ts
        .iter()
        .filter_map(|t| lower.find(t.as_str()))
        .min()
        .unwrap_or(0);
    // Lower-casing can change byte offsets, so map the match back by characters.
    let at_chars = lower[..at.min(lower.len())].chars().count();
    let chars: Vec<char> = text.chars().collect();
    let start = at_chars.saturating_sub(80).min(chars.len());
    let end = (start + 240).min(chars.len());
    let piece: String = chars[start..end].iter().collect();
    let mut out = piece.split_whitespace().collect::<Vec<_>>().join(" ");
    if start > 0 {
        out.insert(0, '…');
    }
    if end < chars.len() {
        out.push('…');
    }
    out
}

fn search(s: &Served, q: &str, limit: usize) -> Vec<Value> {
    let ts = terms(q);
    let mut hits: Vec<(f64, String, String, String, String)> = Vec::new();
    for d in s.docs.values() {
        let whole = format!("{}\n{}\n{}", d.title, d.summary, d.body).to_lowercase();
        if !ts.iter().all(|t| whole.contains(t.as_str())) {
            continue;
        }
        let head = format!("{} {}", d.title, d.summary).to_lowercase();
        let whole_doc = [Section {
            heading: String::new(),
            slug: String::new(),
            level: 0,
            start: 0,
            end: d.body.len(),
            text: d.body.clone(),
        }];
        let parts: &[Section] = if d.sections.is_empty() {
            &whole_doc
        } else {
            &d.sections
        };
        for sec in parts {
            let text = sec.text.to_lowercase();
            let heading = sec.heading.to_lowercase();
            let mut score = 0.0;
            for t in &ts {
                let n = (text.matches(t.as_str()).count() + 3 * heading.matches(t.as_str()).count())
                    as f64;
                if n > 0.0 {
                    score += 100.0; // a section that has more of the words comes first
                }
                score += n + 2.0 * head.matches(t.as_str()).count() as f64;
            }
            if score == 0.0 {
                continue;
            }
            let title = if sec.heading.is_empty() || sec.heading == d.title {
                d.title.clone()
            } else {
                format!("{} › {}", d.title, sec.heading)
            };
            hits.push((
                score,
                d.id.clone(),
                sec.slug.clone(),
                title,
                snippet(&sec.text, &ts),
            ));
        }
    }
    hits.sort_by(|a, b| b.0.total_cmp(&a.0).then(a.1.cmp(&b.1)).then(a.2.cmp(&b.2)));
    hits.truncate(limit);
    hits.into_iter()
        .map(|(score, id, section, title, snippet)| json!({"id": id, "section": section, "title": title, "snippet": snippet, "score": score}))
        .collect()
}

fn call(s: &Served, name: &str, args: &Value) -> Result<Value, (i64, String)> {
    let v = version();
    match name {
        "docs_overview" => {
            let mut out = format!(
                "# {COMPONENT}\n\n{SUMMARY}\n\nVersion {v}. Read a doc with docs_read and its id.\n"
            );
            let mut last = "";
            for id in &s.ids {
                let d = &s.docs[id];
                if d.kind != last {
                    let head = match d.kind.as_str() {
                        "guide" => "Guides",
                        "reference" => "Reference",
                        "design" => "Design",
                        other => other,
                    };
                    out.push_str(&format!("\n## {head}\n\n"));
                    last = &d.kind;
                }
                let note = if d.status == "proposed" {
                    " (proposed, not built)"
                } else {
                    ""
                };
                out.push_str(&format!("- {}: {}{note}. {}\n", d.id, d.title, d.summary));
            }
            let docs: Vec<Value> = s.ids.iter().map(|id| s.docs[id].meta()).collect();
            Ok(result(
                out,
                json!({"component": COMPONENT, "summary": SUMMARY, "version": v, "docs": docs, "api_operations": 0, "agent_tools": false}),
            ))
        }
        "docs_search" => {
            let q = match text_arg(args, "query") {
                Ok(q) => q,
                Err(f) => return Ok(f),
            };
            if terms(&q).is_empty() {
                return Ok(failure("query needs at least one word".to_owned()));
            }
            let limit = args
                .get("limit")
                .and_then(Value::as_f64)
                .map_or(10, |l| (l as i64).clamp(1, 50) as usize);
            let results = search(s, &q, limit);
            let text = if results.is_empty() {
                "No section matches. Try fewer or other words, or docs_overview for the list of docs.".to_owned()
            } else {
                results
                    .iter()
                    .map(|h| {
                        format!(
                            "- {}, section {:?}: {}\n  {}",
                            h["id"].as_str().unwrap_or_default(),
                            h["section"].as_str().unwrap_or_default(),
                            h["title"].as_str().unwrap_or_default(),
                            h["snippet"].as_str().unwrap_or_default()
                        )
                    })
                    .collect::<Vec<_>>()
                    .join("\n")
            };
            Ok(result(text, json!({"results": results})))
        }
        "docs_read" => {
            let (id, sec) = match (text_arg(args, "id"), text_arg(args, "section")) {
                (Ok(id), Ok(sec)) => (id, sec),
                (Err(f), _) | (_, Err(f)) => return Ok(f),
            };
            let key = id.trim_start_matches('/').trim_end_matches(".md");
            let Some(d) = s.docs.get(key) else {
                return Ok(failure(format!("no doc {id:?}. docs_overview lists them.")));
            };
            let mut structured = json!({"id": d.id, "title": d.title, "kind": d.kind, "status": d.status, "summary": d.summary, "sections": d.headings()});
            let mut body = d.body.as_str();
            if !sec.is_empty() {
                let wanted = sec.trim().trim_start_matches('#');
                let Some(found) = d
                    .sections
                    .iter()
                    .find(|x| x.slug == slug(wanted) || x.heading.eq_ignore_ascii_case(wanted))
                else {
                    return Ok(failure(format!(
                        "{} has no section {sec:?}. Its sections: {}",
                        d.id,
                        d.headings().join("; ")
                    )));
                };
                body = &d.body[found.start..found.end];
                structured["section"] = json!(found.slug);
            }
            Ok(result(
                format!(
                    "<!-- {COMPONENT}/{} · {} · {} -->\n\n{body}",
                    d.id, d.kind, d.status
                ),
                structured,
            ))
        }
        _ => Err((-32602, "unknown tool".to_owned())),
    }
}

/// Serves only the route at `addr`, for the docs contract's suite: `agent-ti docs <addr>`.
pub async fn serve_only(addr: &str) -> Result<(), Box<dyn std::error::Error>> {
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, router::<()>()).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    // The image serves these, so broken front matter must fail here, not at startup.
    #[test]
    fn serves_the_repos_consumer_docs_and_nothing_else() {
        let docs = load(DOCS).expect("docs load");
        assert!(docs.contains_key("api"));
        assert!(!docs.contains_key("design"), "design is for contributors");
        assert!(docs.keys().all(|id| !id.starts_with("records/")));
        assert!(docs.values().all(|d| d.audience == "consumer"));
    }

    #[test]
    fn reads_a_section_with_its_subsections() {
        let body = "# T\n\n## A\n\nx\n\n### B\n\ny\n\n## C\n\nz\n";
        let s = sections(body);
        let a = s.iter().find(|x| x.slug == "a").expect("a");
        assert_eq!(&body[a.start..a.end], "## A\n\nx\n\n### B\n\ny\n\n");
    }
}
