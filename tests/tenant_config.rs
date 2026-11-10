//! A tenant's customization of an agent: read from control, applied to the system prompt, the
//! tools offered and searched, the skills listed and the model, with drafts and outages.
use axum::{
    Json, Router,
    extract::{Query, State},
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
    routing::{get, post},
};
use serde_json::{Value, json};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

#[derive(Default, Clone)]
struct Control {
    /// Each tenant's published version and content.
    configs: Arc<Mutex<HashMap<String, (u64, Value)>>>,
    drafts: Arc<Mutex<HashMap<String, Value>>>,
    principals: Arc<Mutex<Vec<String>>>,
    down: Arc<Mutex<bool>>,
}

async fn config(
    State(c): State<Control>,
    Query(q): Query<HashMap<String, String>>,
    headers: HeaderMap,
) -> impl IntoResponse {
    if *c.down.lock().unwrap() {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            HeaderMap::new(),
            Json(json!({})),
        );
    }
    let tenant = q.get("tenant").cloned().unwrap_or_default();
    c.principals
        .lock()
        .unwrap()
        .push(q.get("principal").cloned().unwrap_or_default());
    let (version, content) = c
        .configs
        .lock()
        .unwrap()
        .get(&tenant)
        .cloned()
        .unwrap_or((0, json!({})));
    let tag = format!("\"{tenant}-{version}\"");
    if headers.get("if-none-match").and_then(|v| v.to_str().ok()) == Some(&tag) {
        return (StatusCode::NOT_MODIFIED, HeaderMap::new(), Json(json!({})));
    }
    let mut out = HeaderMap::new();
    out.insert(axum::http::header::ETAG, tag.parse().unwrap());
    (
        StatusCode::OK,
        out,
        Json(
            json!({"tenant": tenant, "agent": "ti", "version": version, "digest": format!("d{version}"), "content": content}),
        ),
    )
}

async fn draft(
    State(c): State<Control>,
    Query(q): Query<HashMap<String, String>>,
) -> impl IntoResponse {
    let key = format!("{}/{}", q["tenant"], q["author"]);
    match c.drafts.lock().unwrap().get(&key) {
        Some(content) => (
            StatusCode::OK,
            Json(json!({"version": 0, "digest": "draft:x", "content": content})),
        ),
        None => (StatusCode::NOT_FOUND, Json(json!({}))),
    }
}

/// The tool gateway's `/mcp/modules`: one server, each tool naming its module in `_meta`.
async fn modules(Json(body): Json<Value>) -> impl IntoResponse {
    let id = body["id"].clone();
    let tool = |name: &str, owner: &str, about: &str| json!({"name": name, "description": about, "inputSchema": {"type": "object"}, "_meta": {"t10/module": owner}});
    let result = match body["method"].as_str().unwrap_or("") {
        "initialize" => {
            json!({"protocolVersion": "2025-03-26", "capabilities": {"tools": {}}, "serverInfo": {"name": "modules", "version": "1"}})
        }
        "tools/list" => json!({"tools": [
            tool("work_list", "collab", "List work items."),
            tool("work_delete", "collab", "Delete a work item."),
            tool("property_rules", "property", "Read the community rules."),
            tool("property_draft", "property", "Draft a property notice."),
        ]}),
        _ => json!({}),
    };
    Json(json!({"jsonrpc": "2.0", "id": id, "result": result}))
}

async fn serve(app: Router) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    format!("http://127.0.0.1:{port}")
}

async fn agent(control: &Control) -> Arc<agent_ti::agents::Agent> {
    let control_url = serve(
        Router::new()
            .route("/v1/runtime/config", get(config))
            .route("/v1/runtime/config/draft", get(draft))
            .with_state(control.clone()),
    )
    .await;
    let auth_url = serve(Router::new().route(
        "/v1/token",
        post(|| async { Json(json!({"access_token": "service-token", "expires_in": 3600})) }),
    ))
    .await;
    let mcp_url = serve(Router::new().route("/mcp", post(modules))).await;

    let dir = tempfile::tempdir().unwrap().keep();
    let agent_dir = dir.join("agent");
    std::fs::create_dir_all(&agent_dir).unwrap();
    std::fs::write(agent_dir.join("base.md"), "You are Ti.").unwrap();
    std::fs::write(agent_dir.join("after.md"), "Never share secrets.").unwrap();
    let model = |id: &str| json!({"provider": "openai-compatible", "modelId": id, "baseUrl": "http://127.0.0.1:7200/v1"});
    let agent_json = json!({
        "agent": "ti",
        "models": {"chat": model("fast"), "strong": model("big")},
        "defaultProfile": "chat",
        "profiles": {
            "chat": {"model": "chat", "alwaysOnTools": ["*"]},
            "voice": {"model": "chat", "alwaysOnTools": ["*"], "speech": true}
        },
        "instructions": [
            {"name": "base", "file": "base.md", "tag": false},
            {"name": "tenant", "slot": true},
            {"name": "after", "file": "after.md"}
        ],
        "grants": {"url": auth_url, "clientId": "agent-ti", "clientSecret": "s", "actions": ["agent:use"], "audiences": ["agent-ti"]},
        "tenantConfig": {"url": control_url, "keepTools": ["property:property_rules"]},
        "mcpServers": [{"name": "modules", "transport": "http", "prefix": "", "url": format!("{mcp_url}/mcp")}]
    });
    std::fs::write(agent_dir.join("agent.json"), agent_json.to_string()).unwrap();
    let host_json = json!({
        "storage": {"dir": dir.join("data")},
        "auth": {"algorithms": "ES256", "jwksUrl": "http://127.0.0.1:18000/jwks", "issuer": "t10", "audience": "t10", "tenantClaim": "tid"},
        "agents": agent_dir.join("agent.json").to_str().unwrap()
    });
    std::fs::write(dir.join("host.json"), host_json.to_string()).unwrap();
    let host =
        agent_ti_config::load_host(&dir.join("host.json"), &agent_ti_config::process_env).unwrap();
    let snapshot = agent_ti_config::FileSource::snapshot(&agent_dir.join("agent.json")).unwrap();
    let agent = Arc::new(
        agent_ti::agents::Agent::load(&host, snapshot)
            .await
            .unwrap(),
    );
    agent.relist_server(0, &reqwest::Client::new()).await;
    agent
}

fn acme() -> Value {
    json!({
        "instructions": "Answer as Acme Property's front desk.",
        "callInstructions": "Keep it to one sentence.",
        "tools": {"off": ["collab:*", "property:*"], "on": ["collab:work_list"]},
        "skills": {"off": ["weather"]},
        "model": "strong"
    })
}

#[tokio::test]
async fn a_tenants_customization_shapes_its_runs_and_no_one_elses() {
    let control = Control::default();
    control
        .configs
        .lock()
        .unwrap()
        .insert("ten_acme".into(), (3, acme()));
    let agent = agent(&control).await;
    let configs = agent.tenant_config.as_ref().unwrap();
    let acme = configs.for_principal("ten_acme", "usr_ada").await;
    let globex = configs.for_principal("ten_globex", "usr_bo").await;
    assert_eq!((acme.version, globex.version), (3, 0));
    assert!(
        control
            .principals
            .lock()
            .unwrap()
            .contains(&"usr_ada".to_string())
    );

    // Tools: off by owning module, on wins, keepTools stay, and another tenant keeps them all.
    let chat = &agent.config.profiles["chat"];
    let all: Vec<String> = [
        "work_list",
        "work_delete",
        "property_rules",
        "property_draft",
    ]
    .map(String::from)
    .to_vec();
    let names = |tools: Vec<Arc<agent_ti::agents::OfferedTool>>| {
        tools.iter().map(|t| t.name.clone()).collect::<Vec<_>>()
    };
    assert_eq!(
        names(agent.offered("ten_acme", chat, &all, Some(&acme)).await),
        ["work_list", "property_rules"]
    );
    assert_eq!(
        names(agent.offered("ten_globex", chat, &all, Some(&globex)).await),
        all
    );
    // Searching can't turn one back on.
    let hits = agent
        .search_tools("delete a work item", 5, "ten_acme", chat, Some(&acme))
        .await;
    assert!(hits.iter().all(|h| h.tool.name != "work_delete"));
    let hits = agent
        .search_tools("delete a work item", 5, "ten_globex", chat, Some(&globex))
        .await;
    assert!(hits.iter().any(|h| h.tool.name == "work_delete"));

    // Instructions: the tenant's text sits at the slot, after the fixed text and before the
    // rest; a call gets the short text.
    let typed = agent
        .render_instructions("chat", &[], Some(&acme))
        .join("\n\n");
    let base = typed.find("You are Ti.").unwrap();
    let tenant = typed.find("Acme Property's front desk").unwrap();
    let after = typed.find("Never share secrets.").unwrap();
    assert!(base < tenant && tenant < after, "{typed}");
    assert!(typed.contains("<organization>"));
    let spoken = agent
        .render_instructions("voice", &[], Some(&acme))
        .join("\n\n");
    assert!(spoken.contains("one sentence") && !spoken.contains("front desk"));
    let shipped = agent
        .render_instructions("chat", &[], Some(&globex))
        .join("\n\n");
    assert!(!shipped.contains("<organization>"));

    // Model: typed sessions take the tenant's entry, calls keep their own, and a recorded
    // model the agent no longer has falls back to the profile's.
    assert_eq!(agent.chosen_model(chat, &acme).as_deref(), Some("strong"));
    assert_eq!(
        agent.chosen_model(&agent.config.profiles["voice"], &acme),
        None
    );
    assert_eq!(
        agent.session_model(chat, &json!({"model": "strong"})),
        "strong"
    );
    assert_eq!(agent.session_model(chat, &json!({"model": "gone"})), "chat");
    let mut unknown = (*acme).clone();
    unknown.model = Some("gone".into());
    assert_eq!(agent.chosen_model(chat, &unknown), None);
}

#[tokio::test]
async fn drafts_are_their_authors_and_outages_keep_the_last_copy() {
    let control = Control::default();
    control
        .configs
        .lock()
        .unwrap()
        .insert("ten_acme".into(), (3, acme()));
    control.drafts.lock().unwrap().insert(
        "ten_acme/usr_ada".into(),
        json!({"instructions": "Draft: greet by name."}),
    );
    let agent = agent(&control).await;
    let configs = agent.tenant_config.as_ref().unwrap();

    let ada = configs.draft("ten_acme", "usr_ada").await;
    assert_eq!(ada.instructions, "Draft: greet by name.");
    let bo = configs.draft("ten_acme", "usr_bo").await;
    assert_eq!(bo.version, 3);

    // With control down, the last good copy stays, past its freshness; a tenant never read
    // runs the agent as shipped.
    *control.down.lock().unwrap() = true;
    configs.changed("ten_acme").await;
    assert_eq!(configs.for_principal("ten_acme", "usr_bo").await.version, 3);
    let never = configs.for_principal("ten_new", "usr_x").await;
    assert_eq!(never.version, 0);
    assert_eq!(never.keep_tools, ["property:property_rules"]);

    // A new version shows once control is back and the change feed marks the tenant.
    *control.down.lock().unwrap() = false;
    control
        .configs
        .lock()
        .unwrap()
        .insert("ten_acme".into(), (4, json!({"instructions": "v4"})));
    configs.changed("ten_acme").await;
    assert_eq!(configs.for_principal("ten_acme", "usr_bo").await.version, 4);
}
