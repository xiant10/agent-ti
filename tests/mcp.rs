use agent_ti::adapters;
use agent_ti::auth::Principal;
use agent_ti::grants::*;
use agent_ti_config::{GrantsConfig, McpAuth, McpServerConfig};
use agent_ti_store::*;
use axum::{
    Json, Router,
    body::Body,
    extract::State,
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::post,
};
use serde_json::{Value, json};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

#[derive(Default, Clone)]
struct MockMcpState {
    received_headers: Arc<Mutex<Vec<HashMap<String, String>>>>,
    received_calls: Arc<Mutex<Vec<Value>>>,
    tools_list_version: Arc<Mutex<usize>>,
}

async fn handle_mcp_post(
    State(state): State<MockMcpState>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> impl IntoResponse {
    let mut header_map = HashMap::new();
    for (k, v) in &headers {
        header_map.insert(
            k.as_str().to_lowercase(),
            v.to_str().unwrap_or("").to_string(),
        );
    }
    state.received_headers.lock().unwrap().push(header_map);

    let method = body.get("method").and_then(Value::as_str).unwrap_or("");
    let id = body.get("id").cloned();

    match method {
        "initialize" => (
            StatusCode::OK,
            [
                ("content-type", "application/json"),
                ("mcp-session-id", "test-session-123"),
            ],
            Json(json!({
                "jsonrpc": "2.0",
                "id": id,
                "result": {
                    "protocolVersion": "2025-03-26",
                    "capabilities": { "tools": { "listChanged": true } },
                    "serverInfo": { "name": "mock-mcp", "version": "1.0.0" }
                }
            })),
        ),
        "notifications/initialized" => (
            StatusCode::OK,
            [
                ("content-type", "application/json"),
                ("mcp-session-id", "test-session-123"),
            ],
            Json(json!({})),
        ),
        "tools/list" => {
            let ver = *state.tools_list_version.lock().unwrap();
            let mut tools = vec![json!({
                "name": "query",
                "description": "Query collab items",
                "inputSchema": { "type": "object" }
            })];
            if ver > 1 {
                tools.push(json!({
                    "name": "update",
                    "description": "Update collab items",
                    "inputSchema": { "type": "object" }
                }));
            }
            (
                StatusCode::OK,
                [
                    ("content-type", "application/json"),
                    ("mcp-session-id", "test-session-123"),
                ],
                Json(json!({
                    "jsonrpc": "2.0",
                    "id": id,
                    "result": { "tools": tools }
                })),
            )
        }
        "tools/call" => {
            state.received_calls.lock().unwrap().push(body.clone());
            let name = body["params"]["name"].as_str().unwrap_or("");
            (
                StatusCode::OK,
                [
                    ("content-type", "application/json"),
                    ("mcp-session-id", "test-session-123"),
                ],
                Json(json!({
                    "jsonrpc": "2.0",
                    "id": id,
                    "result": {
                        "content": [
                            { "type": "text", "text": format!("executed {name}") }
                        ]
                    }
                })),
            )
        }
        _ => (
            StatusCode::NOT_FOUND,
            [
                ("content-type", "application/json"),
                ("mcp-session-id", "test-session-123"),
            ],
            Json(
                json!({ "jsonrpc": "2.0", "id": id, "error": { "code": -32601, "message": "method not found" } }),
            ),
        ),
    }
}

async fn handle_mcp_sse(
    State(state): State<MockMcpState>,
    headers: HeaderMap,
) -> impl IntoResponse {
    let mut header_map = HashMap::new();
    for (k, v) in &headers {
        header_map.insert(
            k.as_str().to_lowercase(),
            v.to_str().unwrap_or("").to_string(),
        );
    }
    state.received_headers.lock().unwrap().push(header_map);

    // Stream SSE notification
    let stream = async_stream::stream! {
        yield Ok::<_, std::io::Error>(
            "event: message\ndata: {\"jsonrpc\":\"2.0\",\"method\":\"notifications/tools/list_changed\"}\n\n".to_string()
        );
        // keep alive until client drops
        tokio::time::sleep(Duration::from_secs(30)).await;
    };

    Response::builder()
        .status(StatusCode::OK)
        .header("content-type", "text/event-stream")
        .header("cache-control", "no-cache")
        .body(Body::from_stream(stream))
        .unwrap()
}

#[tokio::test]
async fn test_mcp_grant_headers_and_agent_context() {
    let state = MockMcpState::default();
    let app = Router::new()
        .route("/mcp", post(handle_mcp_post).get(handle_mcp_sse))
        .with_state(state.clone());

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    let client = reqwest::Client::new();
    let url = format!("http://127.0.0.1:{}/mcp", addr.port());

    // 1. MCP call with McpAuth::Grant and default Authorization header
    let server_default_header = McpServerConfig {
        name: "collab".into(),
        prefix: "collab_".into(),
        url: url.clone(),
        headers: Default::default(),
        include: vec![],
        exclude: vec![],
        auth: McpAuth::Grant { token_header: None },
        agent_context: true,
        call_timeout_ms: 5_000,
        max_description_chars: 1000,
    };

    let outcome = adapters::mcp_call(
        &client,
        &server_default_header,
        "collab_query",
        json!({"query": "workspace"}),
        Some("minted_grant_token_123"),
        Some(json!({ "turn": "turn_abc", "traceparent": "00-tp-01" })),
    )
    .await;

    assert!(!outcome.is_error);
    assert_eq!(outcome.content, "executed collab_query");

    // Verify headers and agent context in received calls
    let calls = state.received_calls.lock().unwrap().clone();
    assert_eq!(calls.len(), 1);
    let params = &calls[0]["params"];
    assert_eq!(params["name"], "collab_query");
    assert_eq!(params["arguments"]["query"], "workspace");
    assert_eq!(params["_meta"]["turn"], "turn_abc");
    assert_eq!(params["_meta"]["traceparent"], "00-tp-01");

    let headers = state.received_headers.lock().unwrap().clone();
    let auth_headers: Vec<_> = headers
        .iter()
        .filter_map(|h| h.get("authorization"))
        .collect();
    assert!(auth_headers.contains(&&"Bearer minted_grant_token_123".to_string()));

    // 2. MCP call with custom token_header
    state.received_calls.lock().unwrap().clear();
    state.received_headers.lock().unwrap().clear();

    let server_custom_header = McpServerConfig {
        name: "collab".into(),
        prefix: "collab_".into(),
        url: url.clone(),
        headers: Default::default(),
        include: vec![],
        exclude: vec![],
        auth: McpAuth::Grant {
            token_header: Some("X-Custom-Grant".into()),
        },
        agent_context: false,
        call_timeout_ms: 5_000,
        max_description_chars: 1000,
    };

    let outcome2 = adapters::mcp_call(
        &client,
        &server_custom_header,
        "collab_query",
        json!({"query": "workspace2"}),
        Some("minted_grant_token_456"),
        None,
    )
    .await;

    assert!(!outcome2.is_error);
    assert_eq!(outcome2.content, "executed collab_query");
    let calls2 = state.received_calls.lock().unwrap().clone();
    assert_eq!(calls2.len(), 1);
    assert!(calls2[0]["params"].get("_meta").is_none());

    let headers2 = state.received_headers.lock().unwrap().clone();
    let custom_headers: Vec<_> = headers2
        .iter()
        .filter_map(|h| h.get("x-custom-grant"))
        .collect();
    assert!(custom_headers.contains(&&"minted_grant_token_456".to_string()));
}

#[derive(Default, Clone)]
struct MockAuthState {
    minted_tokens: Arc<Mutex<Vec<String>>>,
    created_grants: Arc<Mutex<Vec<Value>>>,
}

async fn handle_auth_token(
    State(state): State<MockAuthState>,
    Json(body): Json<Value>,
) -> impl IntoResponse {
    let grant_type = body.get("grant_type").and_then(Value::as_str).unwrap_or("");
    if grant_type == "client_credentials" {
        return (
            StatusCode::OK,
            Json(json!({ "access_token": "service_token_xyz", "expires_in": 3600 })),
        );
    }
    if grant_type == "agent_grant" {
        let grant_id = body.get("grant_id").and_then(Value::as_str).unwrap_or("");
        let tok = format!("minted_for_{grant_id}");
        state.minted_tokens.lock().unwrap().push(tok.clone());
        return (
            StatusCode::OK,
            Json(json!({ "access_token": tok, "expires_in": 3600 })),
        );
    }
    (
        StatusCode::BAD_REQUEST,
        Json(json!({ "error": "unsupported_grant_type" })),
    )
}

async fn handle_auth_grants(
    State(state): State<MockAuthState>,
    Json(body): Json<Value>,
) -> impl IntoResponse {
    state.created_grants.lock().unwrap().push(body);
    (
        StatusCode::CREATED,
        Json(json!({
            "id": "grant_offline_test_99",
            "expires_in": 3600
        })),
    )
}

#[tokio::test]
async fn test_offline_user_recovery_and_grant_token_separation() {
    // 1. Start mock auth server
    let auth_state = MockAuthState::default();
    let auth_app = Router::new()
        .route("/v1/token", post(handle_auth_token))
        .route("/v1/grants", post(handle_auth_grants))
        .with_state(auth_state.clone());

    let auth_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let auth_port = auth_listener.local_addr().unwrap().port();
    tokio::spawn(async move {
        axum::serve(auth_listener, auth_app).await.unwrap();
    });

    // 2. Start mock MCP server
    let mcp_state = MockMcpState::default();
    let mcp_app = Router::new()
        .route("/mcp", post(handle_mcp_post).get(handle_mcp_sse))
        .with_state(mcp_state.clone());

    let mcp_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mcp_port = mcp_listener.local_addr().unwrap().port();
    tokio::spawn(async move {
        axum::serve(mcp_listener, mcp_app).await.unwrap();
    });

    // 3. Initialize SQLite store
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("offline_test.sqlite");
    let store = AgentStore::open(
        &db_path,
        "test_agent",
        StoreLimits {
            max_sessions_per_tenant: 100,
            max_sessions_per_user: 10,
            max_queued_per_tenant: 10,
            max_queued_per_session: 5,
            max_input_bytes: 1024,
            restart_limit: 1,
        },
    )
    .await
    .unwrap();

    let scope = UserScope {
        tenant_id: "ten_offline".into(),
        agent_id: "test_agent".into(),
        user_id: "usr_alice".into(),
    };
    let session_id = "test_agent.inst1.offline_sess_1";

    store
        .create(CreateSession {
            scope: scope.clone(),
            session_id: session_id.into(),
            request_key: "req_key_1".into(),
            profile: "default".into(),
            config_version: "v1".into(),
            origin: json!({"trigger": "user"}),
            metadata: json!({}),
        })
        .await
        .unwrap();

    let grants_cfg = GrantsConfig {
        url: format!("http://127.0.0.1:{auth_port}"),
        client_id: "ti-client".into(),
        client_secret: "ti-secret".into(),
        actions: vec!["collab:read".into(), "collab:write".into()],
        audiences: vec!["collab-svc".into()],
    };

    let user_secret_jwt = "USER_SECRET_JWT_HEADER.PAYLOAD.SIGNATURE";

    // 4. User attaches root grant on session creation
    {
        let broker = GrantBroker::new(grants_cfg.clone(), reqwest::Client::new());
        let user_principal = Principal {
            user_id: "usr_alice".into(),
            tenant_id: "ten_offline".into(),
            scopes: vec!["user".into()],
            expires_at: 9999999999,
            claims: json!({ "sub": "usr_alice" }),
            token: user_secret_jwt.to_string(),
        };

        let stored_grant = broker
            .attach(&store, &scope, session_id, &user_principal, None)
            .await
            .unwrap();
        assert_eq!(stored_grant.grant_id, "grant_offline_test_99");

        // Verify stored in SQLite
        let stored = store.get_grant(&scope, session_id).await.unwrap().unwrap();
        assert_eq!(stored.grant_id, "grant_offline_test_99");
    }

    // 5. Simulate Agent-ti Restart: User is OFFLINE.
    // Create a brand new GrantBroker with no in-memory cache.
    let restarted_broker = GrantBroker::new(grants_cfg, reqwest::Client::new());

    // Call broker.token without user JWT
    let minted_token = restarted_broker
        .token(&store, session_id)
        .await
        .expect("offline recovery should succeed from SQLite stored grant");

    assert_eq!(minted_token, "minted_for_grant_offline_test_99");

    // 6. Execute MCP tool call with this minted token
    let mcp_server = McpServerConfig {
        name: "collab".into(),
        prefix: "collab_".into(),
        url: format!("http://127.0.0.1:{mcp_port}/mcp"),
        headers: Default::default(),
        include: vec![],
        exclude: vec![],
        auth: McpAuth::Grant { token_header: None },
        agent_context: true,
        call_timeout_ms: 5_000,
        max_description_chars: 1000,
    };

    let client = reqwest::Client::new();
    let outcome = adapters::mcp_call(
        &client,
        &mcp_server,
        "collab_query",
        json!({"item": 42}),
        Some(&minted_token),
        Some(json!({"turn": 1})),
    )
    .await;

    assert!(!outcome.is_error);
    assert_eq!(outcome.content, "executed collab_query");

    // 7. Verify MCP server NEVER received the user's secret JWT!
    let mcp_headers = mcp_state.received_headers.lock().unwrap().clone();
    for headers in &mcp_headers {
        for (k, v) in headers {
            assert!(
                !v.contains(user_secret_jwt),
                "MCP server must NEVER receive the user's incoming JWT! Found in header {k}: {v}"
            );
        }
    }
}

#[tokio::test]
async fn test_mcp_sse_list_changed() {
    let mcp_state = MockMcpState::default();
    let mcp_app = Router::new()
        .route("/mcp", post(handle_mcp_post).get(handle_mcp_sse))
        .with_state(mcp_state.clone());

    let mcp_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mcp_port = mcp_listener.local_addr().unwrap().port();
    tokio::spawn(async move {
        axum::serve(mcp_listener, mcp_app).await.unwrap();
    });

    let dir = tempfile::tempdir().unwrap();
    let mcp_url = format!("http://127.0.0.1:{mcp_port}/mcp");
    let agent_json = json!({
        "agent": "ti_mcp_test",
        "models": {
            "chat": {
                "provider": "openai-compatible",
                "modelId": "m",
                "baseUrl": "http://127.0.0.1:7200/v1"
            }
        },
        "profiles": {
            "chat": {
                "model": "chat",
                "alwaysOnTools": ["*"]
            }
        },
        "mcpServers": [
            {
                "name": "collab",
                "transport": "http",
                "prefix": "collab_",
                "url": mcp_url
            }
        ]
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
        "agents": agent_dir.join("agent.json").to_str().unwrap()
    });
    std::fs::write(dir.path().join("agent-ti.json"), host_json.to_string()).unwrap();

    let host = agent_ti_config::load_host(
        &dir.path().join("agent-ti.json"),
        &agent_ti_config::process_env,
    )
    .unwrap();
    let snapshot = agent_ti_config::FileSource::snapshot(&agent_dir.join("agent.json")).unwrap();
    let agent = Arc::new(
        agent_ti::agents::Agent::load(&host, snapshot)
            .await
            .unwrap(),
    );
    let client = reqwest::Client::new();

    // 1. Initial tools list: empty because server hasn't been listed yet
    let initial_tools = agent.match_tools(&["*".into()], "default").await;
    assert!(initial_tools.is_empty());

    // Relist server initially
    agent.relist_server(0, &client).await;
    let loaded_tools = agent.match_tools(&["*".into()], "default").await;
    assert_eq!(loaded_tools, vec!["collab_query"]);

    // 2. Bump server tools version to 2 (which offers collab_query AND collab_update)
    *mcp_state.tools_list_version.lock().unwrap() = 2;

    // 3. Start background listener loop which connects via SSE and receives notifications/tools/list_changed
    agent.start_mcp_listeners(&client);

    // Wait briefly for SSE event to be delivered and relist_server to be triggered
    let mut updated = false;
    for _ in 0..50 {
        tokio::time::sleep(Duration::from_millis(50)).await;
        let tools = agent.match_tools(&["*".into()], "default").await;
        if tools.contains(&"collab_update".to_string()) {
            updated = true;
            break;
        }
    }
    assert!(
        updated,
        "Dynamic tool re-listing should have been triggered by list_changed SSE event"
    );

    let tools = agent.match_tools(&["*".into()], "default").await;
    assert!(tools.contains(&"collab_query".to_string()));
    assert!(tools.contains(&"collab_update".to_string()));
}
