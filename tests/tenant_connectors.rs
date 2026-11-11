use agent_ti::adapters;
use agent_ti::tenant_connectors::*;
use axum::{
    Json, Router,
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
    routing::{get, post},
};
use serde_json::{Value, json};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

fn make_crm(id: &str, tools: &[&str]) -> TenantConnectorRecord {
    TenantConnectorRecord {
        id: id.into(),
        name: "crm".into(),
        title: "CRM".into(),
        version: 1,
        tools: tools
            .iter()
            .map(|name| TenantConnectorTool {
                name: (*name).into(),
                description: Some(format!("CRM {name} for the account")),
                input_schema: Some(json!({
                    "type": "object",
                    "properties": {
                        "text": { "type": "string" }
                    }
                })),
            })
            .collect(),
    }
}

type GatewayCall = (String, Option<String>, String);

#[derive(Default, Clone)]
struct MockGatewayState {
    received_calls: Arc<Mutex<Vec<GatewayCall>>>,
}

async fn handle_gateway_post(
    State(state): State<MockGatewayState>,
    Path(connector_id): Path<String>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> impl IntoResponse {
    let method = body.get("method").and_then(Value::as_str).unwrap_or("");
    let id = body.get("id").cloned();
    let auth = headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .map(str::to_string);

    match method {
        "initialize" => (
            StatusCode::OK,
            Json(json!({
                "jsonrpc": "2.0",
                "id": id,
                "result": {
                    "protocolVersion": "2025-06-18",
                    "capabilities": { "tools": {} },
                    "serverInfo": { "name": "gateway", "version": "1" }
                }
            })),
        ),
        "notifications/initialized" => (StatusCode::ACCEPTED, Json(json!({}))),
        "tools/call" => {
            let tool = body["params"]["name"].as_str().unwrap_or("").to_string();
            let path = format!("/mcp/connector/{connector_id}");
            state
                .received_calls
                .lock()
                .unwrap()
                .push((path.clone(), auth, tool));
            (
                StatusCode::OK,
                Json(json!({
                    "jsonrpc": "2.0",
                    "id": id,
                    "result": {
                        "content": [{ "type": "text", "text": format!("answered at {path}") }]
                    }
                })),
            )
        }
        _ => (
            StatusCode::BAD_REQUEST,
            Json(json!({"error":"unsupported"})),
        ),
    }
}

#[derive(Default, Clone)]
struct MockControlState {
    connectors: Arc<Mutex<HashMap<String, Vec<TenantConnectorRecord>>>>,
}

async fn handle_control_connectors(
    State(state): State<MockControlState>,
    Query(params): Query<HashMap<String, String>>,
    headers: HeaderMap,
) -> impl IntoResponse {
    let auth = headers.get("authorization").and_then(|v| v.to_str().ok());
    if auth != Some("Bearer service-token") {
        return (StatusCode::FORBIDDEN, Json(json!({"error":"unauthorized"})));
    }
    let tenant = params.get("tenant").cloned().unwrap_or_default();
    let conns = state
        .connectors
        .lock()
        .unwrap()
        .get(&tenant)
        .cloned()
        .unwrap_or_default();
    (StatusCode::OK, Json(json!({ "connectors": conns })))
}

#[tokio::test]
async fn test_tenant_tools_collision_avoidance() {
    // Platform tools has crm_whoami already. Tenant connector offers crm_whoami and crm_echo.
    // Tenant connector tool crm_whoami must be skipped to avoid colliding with platform tool.
    let platform_names = vec!["crm_whoami".to_string()];
    let crm_record = make_crm("con_a", &["whoami", "echo"]);

    let tenant_tools = build_tenant_tools(&[crm_record], &platform_names);
    assert_eq!(tenant_tools.tools.len(), 1);
    assert_eq!(tenant_tools.tools[0].name, "crm_echo");
    assert_eq!(
        tenant_tools.connectors.get("crm"),
        Some(&"con_a".to_string())
    );
}

#[tokio::test]
async fn test_running_agent_tenant_connectors_and_gateway_dispatch() {
    // 1. Mock Control Plane
    let control_state = MockControlState::default();
    control_state.connectors.lock().unwrap().insert(
        "ten_acme".into(),
        vec![make_crm("con_a", &["whoami", "echo"])],
    );
    control_state
        .connectors
        .lock()
        .unwrap()
        .insert("ten_globex".into(), vec![make_crm("con_b", &["whoami"])]);

    let control_app = Router::new()
        .route("/v1/runtime/connectors", get(handle_control_connectors))
        .route(
            "/v1/runtime/skills",
            get(|| async { Json(json!({"skills":[]})) }),
        )
        .route(
            "/v1/runtime/skills/changes",
            get(|| async {
                tokio::time::sleep(Duration::from_millis(50)).await;
                Json(json!({"cursor": 0, "tenants": []}))
            }),
        )
        .with_state(control_state.clone());

    let control_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let control_port = control_listener.local_addr().unwrap().port();
    tokio::spawn(async move {
        axum::serve(control_listener, control_app).await.unwrap();
    });

    // 2. Mock Gateway Server
    let gateway_state = MockGatewayState::default();
    let gateway_app = Router::new()
        .route("/mcp/connector/{connector_id}", post(handle_gateway_post))
        .with_state(gateway_state.clone());

    let gateway_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let gateway_port = gateway_listener.local_addr().unwrap().port();
    tokio::spawn(async move {
        axum::serve(gateway_listener, gateway_app).await.unwrap();
    });

    // 3. Mock Auth Server
    let auth_app = Router::new()
        .route(
            "/v1/token",
            post(|Json(body): Json<Value>| async move {
                let grant_type = body.get("grant_type").and_then(Value::as_str).unwrap_or("");
                if grant_type == "client_credentials" {
                    return Json(json!({ "access_token": "service-token", "expires_in": 3600 }));
                }
                Json(json!({ "access_token": "minted_for_test", "expires_in": 3600 }))
            }),
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

    // 4. Setup agent
    let dir = tempfile::tempdir().unwrap();
    let control_url = format!("http://127.0.0.1:{control_port}");
    let gateway_url = format!("http://127.0.0.1:{gateway_port}/");
    let auth_url = format!("http://127.0.0.1:{auth_port}");

    let agent_json = json!({
        "agent": "connectors_test",
        "models": {
            "chat": {
                "provider": "openai-compatible",
                "modelId": "m",
                "baseUrl": "http://127.0.0.1:9999/v1"
            }
        },
        "profiles": {
            "chat": {
                "model": "chat",
                "alwaysOnTools": ["*"]
            }
        },
        "grants": {
            "url": auth_url,
            "clientId": "agent-ti",
            "clientSecret": "secret",
            "actions": ["agent:use"],
            "audiences": ["agent-ti"]
        },
        "tenantSkills": {
            "url": control_url
        },
        "tenantConnectors": {
            "gatewayUrl": gateway_url
        }
    });

    let agent_dir = dir.path().join("agent");
    std::fs::create_dir_all(&agent_dir).unwrap();
    std::fs::write(agent_dir.join("agent.json"), agent_json.to_string()).unwrap();

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

    // 5. Test catalog_tools tenant isolation
    let acme_tools = agent.catalog_tools("ten_acme").await;
    let acme_names: Vec<_> = acme_tools.iter().map(|t| t.name.as_str()).collect();
    assert!(acme_names.contains(&"crm_whoami"));
    assert!(acme_names.contains(&"crm_echo"));

    let globex_tools = agent.catalog_tools("ten_globex").await;
    let globex_names: Vec<_> = globex_tools.iter().map(|t| t.name.as_str()).collect();
    assert!(globex_names.contains(&"crm_whoami"));
    assert!(!globex_names.contains(&"crm_echo"));

    // 6. Test direct dispatch to gateway
    let profile = agent.config.profiles.get("chat").unwrap();
    let acme_offered = agent
        .offered("ten_acme", profile, &["crm_echo".into()], None)
        .await;
    assert_eq!(acme_offered.len(), 1);
    let tool_echo = &acme_offered[0];
    assert_eq!(tool_echo.name, "crm_echo");
    assert_eq!(tool_echo.mcp_name, "echo");
    assert!(tool_echo.connector.is_some());
    let target = tool_echo.connector.as_ref().unwrap();
    assert_eq!(target.tenant_id, "ten_acme");
    assert_eq!(target.connector_id, "con_a");

    // Call through adapters::mcp_call simulating worker dispatch
    let srv = agent_ti_config::McpServerConfig {
        name: target.connector_name.clone(),
        url: format!(
            "{}/mcp/connector/{}",
            gateway_url.trim_end_matches('/'),
            target.connector_id
        ),
        auth: agent_ti_config::McpAuth::Grant { token_header: None },
        headers: std::collections::BTreeMap::new(),
        include: vec![],
        exclude: vec![],
        prefix: String::new(),
        max_description_chars: 1000,
        call_timeout_ms: 5_000,
        agent_context: false,
    };

    let client = reqwest::Client::new();
    let outcome = adapters::mcp_call(
        &client,
        &srv,
        &tool_echo.mcp_name,
        json!({"text": "hi"}),
        Some("minted_for_test"),
        None,
    )
    .await;

    assert!(!outcome.is_error);
    assert_eq!(outcome.content, "answered at /mcp/connector/con_a");

    let calls = gateway_state.received_calls.lock().unwrap().clone();
    assert_eq!(calls.len(), 1);
    assert_eq!(calls[0].0, "/mcp/connector/con_a");
    assert_eq!(calls[0].1, Some("Bearer minted_for_test".to_string()));
    assert_eq!(calls[0].2, "echo");

    // 7. Globex dispatch calls con_b
    let globex_offered = agent
        .offered("ten_globex", profile, &["crm_whoami".into()], None)
        .await;
    assert_eq!(globex_offered.len(), 1);
    let target_b = globex_offered[0].connector.as_ref().unwrap();
    assert_eq!(target_b.tenant_id, "ten_globex");
    assert_eq!(target_b.connector_id, "con_b");

    let srv_b = agent_ti_config::McpServerConfig {
        name: target_b.connector_name.clone(),
        url: format!(
            "{}/mcp/connector/{}",
            gateway_url.trim_end_matches('/'),
            target_b.connector_id
        ),
        auth: agent_ti_config::McpAuth::Grant { token_header: None },
        headers: std::collections::BTreeMap::new(),
        include: vec![],
        exclude: vec![],
        prefix: String::new(),
        max_description_chars: 1000,
        call_timeout_ms: 5_000,
        agent_context: false,
    };

    let outcome_b = adapters::mcp_call(
        &client,
        &srv_b,
        &globex_offered[0].mcp_name,
        json!({}),
        Some("minted_for_test"),
        None,
    )
    .await;

    assert!(!outcome_b.is_error);
    assert_eq!(outcome_b.content, "answered at /mcp/connector/con_b");
    let calls2 = gateway_state.received_calls.lock().unwrap().clone();
    assert_eq!(calls2.len(), 2);
    assert_eq!(calls2[1].0, "/mcp/connector/con_b");
    assert_eq!(calls2[1].2, "whoami");
}
