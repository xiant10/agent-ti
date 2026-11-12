use agent_ti::adapters;
use agent_ti_config::{McpAuth, McpServerConfig};
use axum::{
    Json, Router,
    extract::State,
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
    routing::post,
};
use serde_json::{Value, json};
use std::collections::HashSet;
use std::sync::{Arc, Mutex};

/// An MCP server that gives each `initialize` a session of its own, answers 404 for a session it
/// doesn't know, and records the session each tool call ran on.
#[derive(Default, Clone)]
struct Sessions {
    open: Arc<Mutex<HashSet<String>>>,
    initialized: Arc<Mutex<usize>>,
    calls: Arc<Mutex<Vec<String>>>,
}

async fn handle(
    State(state): State<Sessions>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> axum::response::Response {
    let session = headers
        .get("mcp-session-id")
        .and_then(|v| v.to_str().ok())
        .map(str::to_owned);
    let id = body["id"].clone();
    match body["method"].as_str().unwrap_or("") {
        "initialize" => {
            let mut count = state.initialized.lock().unwrap();
            *count += 1;
            let session = format!("s{count}");
            state.open.lock().unwrap().insert(session.clone());
            (
                [("mcp-session-id", session)],
                Json(json!({"jsonrpc":"2.0","id":id,"result":{"protocolVersion":"2025-03-26","capabilities":{}}})),
            )
                .into_response()
        }
        _ if !session
            .as_ref()
            .is_some_and(|s| state.open.lock().unwrap().contains(s)) =>
        {
            StatusCode::NOT_FOUND.into_response()
        }
        "notifications/initialized" => StatusCode::ACCEPTED.into_response(),
        "tools/call" => {
            state.calls.lock().unwrap().push(session.unwrap());
            Json(json!({"jsonrpc":"2.0","id":id,"result":{"content":[{"type":"text","text":"done"}]}}))
                .into_response()
        }
        _ => StatusCode::BAD_REQUEST.into_response(),
    }
}

fn server(url: String) -> McpServerConfig {
    McpServerConfig {
        name: "collab".into(),
        prefix: String::new(),
        url,
        headers: Default::default(),
        include: vec![],
        exclude: vec![],
        auth: McpAuth::Grant { token_header: None },
        agent_context: false,
        call_timeout_ms: 5_000,
        max_description_chars: 1000,
    }
}

#[tokio::test]
async fn tool_calls_share_a_session_and_start_a_new_one_when_it_ends() {
    let state = Sessions::default();
    let app = Router::new()
        .route("/mcp", post(handle))
        .with_state(state.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let client = reqwest::Client::new();
    let server = server(format!("http://127.0.0.1:{}/mcp", addr.port()));
    let call = |token: &'static str| {
        let (client, server) = (client.clone(), server.clone());
        async move { adapters::mcp_call(&client, &server, "work", json!({}), Some(token), None).await }
    };

    // One handshake, then each call is one request on the same session.
    for _ in 0..3 {
        let outcome = call("token_a").await;
        assert!(!outcome.is_error, "{}", outcome.content);
    }
    assert_eq!(*state.initialized.lock().unwrap(), 1);
    assert_eq!(*state.calls.lock().unwrap(), ["s1", "s1", "s1"]);

    // A renewed token is another credential, so it gets a session of its own.
    assert!(!call("token_b").await.is_error);
    assert_eq!(*state.initialized.lock().unwrap(), 2);
    assert_eq!(state.calls.lock().unwrap().last().unwrap(), "s2");

    // The server ends every session: the call it refused runs once, on a new session.
    state.open.lock().unwrap().clear();
    let outcome = call("token_a").await;
    assert!(!outcome.is_error, "{}", outcome.content);
    assert_eq!(*state.initialized.lock().unwrap(), 3);
    assert_eq!(state.calls.lock().unwrap().len(), 5);
    assert_eq!(state.calls.lock().unwrap().last().unwrap(), "s3");
    assert!(!call("token_a").await.is_error);
    assert_eq!(*state.initialized.lock().unwrap(), 3);
}
