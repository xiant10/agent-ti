use agent_ti::tenant_skills::*;
use agent_ti_config::*;
use axum::{
    Json, Router,
    extract::{Query, State},
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
    routing::{get, post},
};
use serde_json::{Value, json};
use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};
use std::time::Duration;

fn make_weather_skill() -> SkillConfig {
    SkillConfig {
        name: "weather".into(),
        description: "Weather forecasts".into(),
        tools: vec!["crm:get_forecast".into()],
        keywords: vec![],
        body: "Built-in weather instructions.".into(),
        files: BTreeMap::new(),
        flow: Some(json!({
            "id": "weather",
            "title": "Weather",
            "mode": "guided",
            "nodes": [
                { "id": "start", "kind": "start", "label": "Start" },
                { "id": "call", "kind": "tool", "label": "Call", "call": { "tool": "crm:get_forecast" } },
                { "id": "reply", "kind": "llm_reply", "label": "Reply" }
            ],
            "edges": [
                { "id": "e1", "source": "start", "target": "call" },
                { "id": "e2", "source": "call", "target": "reply" }
            ]
        })),
        source: None,
    }
}

fn make_override(title: &str, tool: Option<&str>, version: u64) -> TenantSkillRecord {
    let nodes = if let Some(t) = tool {
        vec![
            json!({ "id": "start", "kind": "start", "label": "Start" }),
            json!({ "id": "call", "kind": "tool", "label": "Call", "call": { "tool": t } }),
            json!({ "id": "reply", "kind": "llm_reply", "label": "Reply" }),
        ]
    } else {
        vec![
            json!({ "id": "start", "kind": "start", "label": "Start" }),
            json!({ "id": "reply", "kind": "llm_reply", "label": "Reply" }),
        ]
    };
    let edges = if tool.is_some() {
        vec![
            json!({ "id": "e1", "source": "start", "target": "call" }),
            json!({ "id": "e2", "source": "call", "target": "reply" }),
        ]
    } else {
        vec![json!({ "id": "e1", "source": "start", "target": "reply" })]
    };

    TenantSkillRecord {
        name: "weather".into(),
        kind: "override".into(),
        version,
        digest: format!("sha256:{version}"),
        content: TenantSkillContent {
            description: None,
            body: None,
            tools: None,
            keywords: None,
            files: None,
            flow: Some(json!({
                "id": "weather",
                "title": title,
                "mode": "guided",
                "nodes": nodes,
                "edges": edges,
            })),
        },
    }
}

fn make_vip_skill() -> TenantSkillRecord {
    let mut files = BTreeMap::new();
    files.insert(
        "refs/tiers.md".to_string(),
        "Tier A: over 1M ARR.".to_string(),
    );
    TenantSkillRecord {
        name: "vip-intake".into(),
        kind: "custom".into(),
        version: 1,
        digest: "sha256:v".into(),
        content: TenantSkillContent {
            description: Some("VIP intake".into()),
            body: Some("Ask for the VIP account number first.".into()),
            tools: Some(vec!["crm:get_forecast".into()]),
            keywords: Some(vec!["vip".into()]),
            files: Some(files),
            flow: None,
        },
    }
}

#[test]
fn test_apply_tenant_skills_and_reference_files() {
    let weather = make_weather_skill();
    let override_rec = make_override("Acme weather", Some("crm:get_forecast"), 1);
    let vip_rec = make_vip_skill();

    let view = apply_tenant_skills(
        std::slice::from_ref(&weather),
        &[override_rec, vip_rec.clone()],
        |_, _| {},
    );
    assert_eq!(view.len(), 2);
    assert_eq!(view[0].name, "vip-intake");
    assert_eq!(view[1].name, "weather");

    let changed = &view[1];
    assert_eq!(changed.flow.as_ref().unwrap()["title"], "Acme weather");
    assert_eq!(changed.body, weather.body);
    assert_eq!(changed.source.as_ref().unwrap().kind, "override");
    assert_eq!(changed.source.as_ref().unwrap().version, 1);

    // Test reference files
    let vip = &view[0];
    assert_eq!(
        read_skill_file(vip, "refs/tiers.md").unwrap(),
        "Tier A: over 1M ARR."
    );
    assert_eq!(
        read_skill_file(vip, "./refs/tiers.md").unwrap(),
        "Tier A: over 1M ARR."
    );
    assert!(read_skill_file(vip, "../weather/SKILL.md").is_err());
    assert!(read_skill_file(vip, "/etc/passwd").is_err());
    assert!(read_skill_file(vip, "refs/../refs/tiers.md").is_err());
    assert!(read_skill_file(vip, "refs/other.md").is_err());

    // Test validation skips
    let mut skipped = Vec::new();
    let bad_regex_nodes = vec![json!({
        "id": "start",
        "kind": "extract",
        "label": "x",
        "extract": { "q": { "pattern": "a".repeat(201) } }
    })];
    let bad_records = vec![
        make_override("Calls anything", Some("delete_everything"), 2),
        TenantSkillRecord {
            name: "weather".into(),
            kind: "custom".into(),
            version: 1,
            digest: "x".into(),
            content: vip_rec.content.clone(),
        },
        TenantSkillRecord {
            name: "missing".into(),
            kind: "override".into(),
            version: 1,
            digest: "x".into(),
            content: TenantSkillContent {
                description: None,
                body: None,
                tools: None,
                keywords: None,
                files: None,
                flow: Some(
                    json!({ "id": "missing", "title": "No such skill", "mode": "guided", "nodes": [], "edges": [] }),
                ),
            },
        },
        TenantSkillRecord {
            name: "vip-bad".into(),
            kind: "custom".into(),
            version: 1,
            digest: "x".into(),
            content: TenantSkillContent {
                description: Some("Bad".into()),
                body: None,
                tools: Some(vec!["crm:get_forecast".into()]),
                keywords: None,
                files: None,
                flow: Some(json!({
                    "id": "vip-bad",
                    "title": "Bad",
                    "mode": "guided",
                    "nodes": [{ "id": "c", "kind": "tool", "label": "call", "call": { "tool": "delete_everything" } }],
                    "edges": []
                })),
            },
        },
        TenantSkillRecord {
            name: "vip-regex".into(),
            kind: "custom".into(),
            version: 1,
            digest: "x".into(),
            content: TenantSkillContent {
                description: Some("Regex".into()),
                body: None,
                tools: None,
                keywords: None,
                files: None,
                flow: Some(json!({
                    "id": "vip-regex",
                    "title": "Regex",
                    "mode": "guided",
                    "nodes": bad_regex_nodes,
                    "edges": []
                })),
            },
        },
    ];

    let view2 = apply_tenant_skills(
        std::slice::from_ref(&weather),
        &bad_records,
        |rec, reason| {
            skipped.push(format!("{}: {reason}", rec.name));
        },
    );
    assert_eq!(view2.len(), 1);
    assert_eq!(view2[0].name, "weather");
    assert_eq!(skipped.len(), 5);
}

#[tokio::test]
async fn test_turn_pins() {
    let weather = make_weather_skill();
    let pins = TurnPins::default();

    pins.pin("sess_1", Arc::new(vec![weather.clone()])).await;
    let read1 = pins.get("sess_1").await.unwrap();
    assert_eq!(read1.len(), 1);
    assert_eq!(read1[0].name, "weather");

    let override_rec = make_override("Acme weather v2", Some("crm:get_forecast"), 2);
    let view = apply_tenant_skills(std::slice::from_ref(&weather), &[override_rec], |_, _| {});
    pins.pin("sess_2", Arc::new(view)).await;

    // sess_1 still has its pinned skills
    let read1_again = pins.get("sess_1").await.unwrap();
    assert_eq!(read1_again[0].flow.as_ref().unwrap()["title"], "Weather");

    // sess_2 has v2
    let read2 = pins.get("sess_2").await.unwrap();
    assert_eq!(read2[0].flow.as_ref().unwrap()["title"], "Acme weather v2");
}

type ControlCall = (String, Option<String>);

#[derive(Default, Clone)]
struct MockControlState {
    skills: Arc<Mutex<HashMap<String, Vec<TenantSkillRecord>>>>,
    drafts: Arc<Mutex<HashMap<String, TenantSkillRecord>>>,
    changes: Arc<Mutex<Vec<String>>>,
    cursor: Arc<Mutex<u64>>,
    calls: Arc<Mutex<Vec<ControlCall>>>,
    down: Arc<Mutex<bool>>,
}

async fn handle_control_skills(
    State(state): State<MockControlState>,
    Query(params): Query<HashMap<String, String>>,
    headers: HeaderMap,
) -> impl IntoResponse {
    let auth = headers.get("authorization").and_then(|v| v.to_str().ok());
    if auth != Some("Bearer service-token") {
        return (
            StatusCode::FORBIDDEN,
            HeaderMap::new(),
            Json(json!({"error":"unauthorized"})),
        );
    }
    if *state.down.lock().unwrap() {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            HeaderMap::new(),
            Json(json!({"error":"down"})),
        );
    }

    let tenant = params.get("tenant").cloned().unwrap_or_default();
    let if_none_match = headers
        .get("if-none-match")
        .and_then(|v| v.to_str().ok())
        .map(str::to_string);
    state
        .calls
        .lock()
        .unwrap()
        .push((tenant.clone(), if_none_match.clone()));

    let skills = state
        .skills
        .lock()
        .unwrap()
        .get(&tenant)
        .cloned()
        .unwrap_or_default();
    let digest = format!("\"{}\"", serde_json::to_string(&skills).unwrap().len());

    if if_none_match.as_deref() == Some(&digest) {
        return (StatusCode::NOT_MODIFIED, HeaderMap::new(), Json(json!({})));
    }

    let mut resp_headers = HeaderMap::new();
    resp_headers.insert(axum::http::header::ETAG, digest.parse().unwrap());
    (
        StatusCode::OK,
        resp_headers,
        Json(json!({ "skills": skills })),
    )
}

async fn handle_control_draft(
    State(state): State<MockControlState>,
    Query(params): Query<HashMap<String, String>>,
    headers: HeaderMap,
) -> impl IntoResponse {
    let auth = headers.get("authorization").and_then(|v| v.to_str().ok());
    if auth != Some("Bearer service-token") {
        return (StatusCode::FORBIDDEN, Json(json!({"error":"unauthorized"})));
    }

    let tenant = params.get("tenant").cloned().unwrap_or_default();
    let name = params.get("name").cloned().unwrap_or_default();
    let author = params.get("author").cloned().unwrap_or_default();
    let key = format!("{tenant}/{author}/{name}");

    if let Some(draft) = state.drafts.lock().unwrap().get(&key).cloned() {
        (StatusCode::OK, Json(serde_json::to_value(draft).unwrap()))
    } else {
        (StatusCode::NOT_FOUND, Json(json!({"error":"not found"})))
    }
}

async fn handle_control_changes(
    State(state): State<MockControlState>,
    Query(_params): Query<HashMap<String, String>>,
    headers: HeaderMap,
) -> impl IntoResponse {
    let auth = headers.get("authorization").and_then(|v| v.to_str().ok());
    if auth != Some("Bearer service-token") {
        return (StatusCode::FORBIDDEN, Json(json!({"error":"unauthorized"})));
    }

    tokio::time::sleep(Duration::from_millis(50)).await;
    let mut changes = state.changes.lock().unwrap();
    let tenants = changes.drain(..).collect::<Vec<_>>();
    let mut cursor = state.cursor.lock().unwrap();
    if !tenants.is_empty() {
        *cursor += 1;
    }
    (
        StatusCode::OK,
        Json(json!({ "cursor": *cursor, "tenants": tenants })),
    )
}

#[tokio::test]
async fn test_running_agent_tenant_skills() {
    let _weather = make_weather_skill();
    let override_v1 = make_override("Acme weather", Some("crm:get_forecast"), 1);
    let vip = make_vip_skill();

    // 1. Mock Control Plane
    let control_state = MockControlState::default();
    control_state
        .skills
        .lock()
        .unwrap()
        .insert("ten_acme".into(), vec![override_v1.clone(), vip.clone()]);
    let draft_record = TenantSkillRecord {
        name: "vip-intake".into(),
        kind: "custom".into(),
        version: 0,
        digest: "draft:x".into(),
        content: TenantSkillContent {
            description: Some("VIP intake (draft)".into()),
            body: Some("Draft: ask for the region too.".into()),
            tools: Some(vec!["crm:get_forecast".into()]),
            keywords: Some(vec!["vip".into()]),
            files: vip.content.files.clone(),
            flow: None,
        },
    };
    control_state
        .drafts
        .lock()
        .unwrap()
        .insert("ten_acme/ada/vip-intake".into(), draft_record);

    let control_app = Router::new()
        .route("/v1/runtime/skills", get(handle_control_skills))
        .route("/v1/runtime/skills/draft", get(handle_control_draft))
        .route("/v1/runtime/skills/changes", get(handle_control_changes))
        .with_state(control_state.clone());

    let control_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let control_port = control_listener.local_addr().unwrap().port();
    tokio::spawn(async move {
        axum::serve(control_listener, control_app).await.unwrap();
    });

    // 2. Mock Auth Server
    let auth_app = Router::new()
        .route(
            "/v1/token",
            post(|| async { Json(json!({ "access_token": "service-token", "expires_in": 3600 })) }),
        )
        .route(
            "/v1/grants",
            post(|| async {
                (
                    StatusCode::CREATED,
                    Json(json!({ "id": "grt_1", "expires_in": 3600 })),
                )
            }),
        );
    let auth_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let auth_port = auth_listener.local_addr().unwrap().port();
    tokio::spawn(async move {
        axum::serve(auth_listener, auth_app).await.unwrap();
    });

    // 3. Mock Model Server (OpenAI chat completions)
    let model_app = Router::new().route(
        "/v1/chat/completions",
        post(|Json(body): Json<Value>| async move {
            let messages = body["messages"].as_array().cloned().unwrap_or_default();
            let last_user_msg = messages
                .iter()
                .rev()
                .find(|m| m["role"] == "user")
                .and_then(|m| m["content"].as_str())
                .unwrap_or("");

            // If last message is tool result, reply with text
            if messages.last().map(|m| m["role"] == "tool").unwrap_or(false) {
                let tool_result = messages.last().unwrap()["content"].as_str().unwrap_or("");
                return Json(json!({
                    "id": "chatcmpl-test",
                    "object": "chat.completion",
                    "created": 1234567,
                    "model": "m",
                    "choices": [{
                        "index": 0,
                        "message": { "role": "assistant", "content": tool_result },
                        "finish_reason": "stop"
                    }],
                    "usage": { "prompt_tokens": 10, "completion_tokens": 5, "total_tokens": 15 }
                }));
            }

            // If user mentions "VIP onboarding please with file", invoke load_skill with file
            if last_user_msg.contains("with file") {
                return Json(json!({
                    "id": "chatcmpl-test",
                    "object": "chat.completion",
                    "created": 1234567,
                    "model": "m",
                    "choices": [{
                        "index": 0,
                        "message": {
                            "role": "assistant",
                            "content": null,
                            "tool_calls": [{
                                "id": "call_1",
                                "type": "function",
                                "function": {
                                    "name": "load_skill",
                                    "arguments": json!({ "name": "vip-intake", "file": "refs/tiers.md" }).to_string()
                                }
                            }]
                        },
                        "finish_reason": "tool_calls"
                    }],
                    "usage": { "prompt_tokens": 10, "completion_tokens": 5, "total_tokens": 15 }
                }));
            }

            // Otherwise invoke load_skill name vip-intake
            Json(json!({
                "id": "chatcmpl-test",
                "object": "chat.completion",
                "created": 1234567,
                "model": "m",
                "choices": [{
                    "index": 0,
                    "message": {
                        "role": "assistant",
                        "content": null,
                        "tool_calls": [{
                            "id": "call_1",
                            "type": "function",
                            "function": {
                                "name": "load_skill",
                                "arguments": json!({ "name": "vip-intake" }).to_string()
                            }
                        }]
                    },
                    "finish_reason": "tool_calls"
                }],
                "usage": { "prompt_tokens": 10, "completion_tokens": 5, "total_tokens": 15 }
            }))
        }),
    );
    let model_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let model_port = model_listener.local_addr().unwrap().port();
    tokio::spawn(async move {
        axum::serve(model_listener, model_app).await.unwrap();
    });

    // 4. Setup agent configuration and server
    let dir = tempfile::tempdir().unwrap();
    let control_url = format!("http://127.0.0.1:{control_port}");
    let auth_url = format!("http://127.0.0.1:{auth_port}");
    let model_url = format!("http://127.0.0.1:{model_port}/v1");

    let agent_json = json!({
        "agent": "skills_test",
        "models": {
            "chat": {
                "provider": "openai-compatible",
                "modelId": "m",
                "baseUrl": model_url,
            }
        },
        "profiles": {
            "chat": {
                "model": "chat",
                "alwaysOnTools": ["*"]
            }
        },
        "skillsDir": "skills",
        "grants": {
            "url": auth_url,
            "clientId": "agent-ti",
            "clientSecret": "secret",
            "actions": ["agent:use"],
            "audiences": ["agent-ti"]
        },
        "tenantSkills": {
            "url": control_url
        }
    });

    let agent_dir = dir.path().join("agent");
    std::fs::create_dir_all(&agent_dir).unwrap();
    std::fs::write(agent_dir.join("agent.json"), agent_json.to_string()).unwrap();

    let skills_dir = agent_dir.join("skills");
    let weather_dir = skills_dir.join("weather");
    std::fs::create_dir_all(&weather_dir).unwrap();
    let weather_md = "---\nname: weather\ndescription: Weather forecasts\ntools: [\"crm:get_forecast\"]\nkeywords: []\n---\nBuilt-in weather instructions.";
    std::fs::write(weather_dir.join("SKILL.md"), weather_md).unwrap();
    let weather_flow = json!({
        "id": "weather",
        "title": "Weather",
        "mode": "guided",
        "nodes": [
            { "id": "start", "kind": "start", "label": "Start" },
            { "id": "call", "kind": "tool", "label": "Call", "call": { "tool": "crm:get_forecast" } },
            { "id": "reply", "kind": "llm_reply", "label": "Reply" }
        ],
        "edges": [
            { "id": "e1", "source": "start", "target": "call" },
            { "id": "e2", "source": "call", "target": "reply" }
        ]
    });
    std::fs::write(weather_dir.join("flow.json"), weather_flow.to_string()).unwrap();

    let host_json = json!({
        "storage": { "dir": dir.path().join("data") },
        "auth": {
            "algorithms": "ES256",
            "jwksUrl": "http://127.0.0.1:18000/jwks",
            "issuer": "t10",
            "audience": "t10",
            "tenantClaim": "tid"
        },
        "agents": [agent_dir.join("agent.json").to_str().unwrap()]
    });
    let host_path = dir.path().join("host.json");
    std::fs::write(&host_path, host_json.to_string()).unwrap();

    let host_config =
        agent_ti_config::load_host(&host_path, &agent_ti_config::process_env).unwrap();
    let snapshot = agent_ti_config::FileSource::snapshot(&agent_dir.join("agent.json")).unwrap();
    let agent = Arc::new(
        agent_ti::agents::Agent::load(&host_config, snapshot)
            .await
            .unwrap(),
    );

    // Test skills.list for Acme vs Globex
    let acme_skills = agent
        .tenant_skills
        .as_ref()
        .unwrap()
        .for_tenant(Some("ten_acme"))
        .await;
    eprintln!(
        "ACME SKILLS: {:?}",
        acme_skills.iter().map(|s| &s.name).collect::<Vec<_>>()
    );
    eprintln!(
        "CALLS TO CONTROL: {:?}",
        control_state.calls.lock().unwrap()
    );
    assert_eq!(acme_skills.len(), 2);
    assert_eq!(acme_skills[0].name, "vip-intake");
    assert_eq!(acme_skills[1].name, "weather");
    assert_eq!(
        acme_skills[1].flow.as_ref().unwrap()["title"],
        "Acme weather"
    );
    assert_eq!(acme_skills[1].source.as_ref().unwrap().kind, "override");

    let globex_skills = agent
        .tenant_skills
        .as_ref()
        .unwrap()
        .for_tenant(Some("ten_globex"))
        .await;
    assert_eq!(globex_skills.len(), 1);
    assert_eq!(globex_skills[0].name, "weather");
    assert_eq!(globex_skills[0].flow.as_ref().unwrap()["title"], "Weather");

    // Test instructions rendering
    let acme_instr = agent.render_instructions("chat", &acme_skills);
    let acme_instr_str = acme_instr.join("\n");
    assert!(acme_instr_str.contains("- vip-intake: VIP intake"));

    let globex_instr = agent.render_instructions("chat", &globex_skills);
    let globex_instr_str = globex_instr.join("\n");
    assert!(!globex_instr_str.contains("vip-intake"));
    assert!(globex_instr_str.contains("- weather: Weather forecasts"));

    // Test withDraft
    let draft_skills = agent
        .tenant_skills
        .as_ref()
        .unwrap()
        .with_draft(Some("ten_acme"), "ada", "vip-intake")
        .await;
    assert_eq!(draft_skills.len(), 2);
    assert_eq!(draft_skills[0].description, "VIP intake (draft)");
    let draft_instr = agent.render_instructions("chat", &draft_skills).join("\n");
    assert!(draft_instr.contains("- vip-intake: VIP intake (draft)"));

    // Test draft author check: bob gets published skill, not ada's draft
    let bob_draft_skills = agent
        .tenant_skills
        .as_ref()
        .unwrap()
        .with_draft(Some("ten_acme"), "bob", "vip-intake")
        .await;
    assert_eq!(bob_draft_skills[0].description, "VIP intake");

    // Test Change Feed invalidation
    let override_v2 = make_override("Acme weather v2", Some("crm:get_forecast"), 2);
    control_state
        .skills
        .lock()
        .unwrap()
        .insert("ten_acme".into(), vec![override_v2, vip.clone()]);
    control_state
        .changes
        .lock()
        .unwrap()
        .push("ten_acme".into());

    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    agent.start_tenant_skills_watcher(shutdown_rx);

    // Within 3 seconds, Acme skills should update to Acme weather v2
    let mut updated = false;
    for _ in 0..60 {
        tokio::time::sleep(Duration::from_millis(50)).await;
        let s = agent
            .tenant_skills
            .as_ref()
            .unwrap()
            .for_tenant(Some("ten_acme"))
            .await;
        if s[1].flow.as_ref().unwrap()["title"] == "Acme weather v2" {
            updated = true;
            break;
        }
    }
    assert!(updated, "Change feed should invalidate cache and fetch v2");
    let _ = shutdown_tx.send(true);
}
