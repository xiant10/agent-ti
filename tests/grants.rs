use agent_ti::auth::Principal;
use agent_ti::grants::*;
use agent_ti_config::GrantsConfig;
use agent_ti_store::*;
use axum::{
    Json, Router,
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
    routing::{get, post},
};
use serde_json::{Value, json};
use std::collections::HashSet;
use std::sync::{Arc, Mutex};
use std::time::Duration;

#[derive(Default, Clone)]
struct AuthState {
    created: Arc<Mutex<Vec<Value>>>,
    extended: Arc<Mutex<Vec<String>>>,
    revoked: Arc<Mutex<Vec<String>>>,
    stopped: Arc<Mutex<HashSet<String>>>,
    expired: Arc<Mutex<HashSet<String>>>,
    ending: Arc<Mutex<HashSet<String>>>,
    mints: Arc<Mutex<usize>>,
    service_mints: Arc<Mutex<usize>>,
    chain_reads: Arc<Mutex<usize>>,
    traces: Arc<Mutex<Vec<String>>>,
    down_count: Arc<Mutex<usize>>,
    expires_in: Arc<Mutex<u64>>,
    next_id: Arc<Mutex<usize>>,
}

async fn handle_token(
    State(state): State<AuthState>,
    Json(body): Json<Value>,
) -> impl IntoResponse {
    let mut down = state.down_count.lock().unwrap();
    if *down > 0 {
        *down -= 1;
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": "server_error"})),
        );
    }
    drop(down);

    let grant_type = body.get("grant_type").and_then(Value::as_str).unwrap_or("");
    if grant_type == "client_credentials" {
        *state.service_mints.lock().unwrap() += 1;
        return (
            StatusCode::OK,
            Json(json!({"access_token": "service-token", "expires_in": 300})),
        );
    }

    if grant_type == "agent_grant" {
        let grant_id = body.get("grant_id").and_then(Value::as_str).unwrap_or("");
        if state.stopped.lock().unwrap().contains(grant_id) {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({"error": "invalid_grant"})),
            );
        }
        if state.expired.lock().unwrap().contains(grant_id) {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({"error": "grant_expired"})),
            );
        }
        let mut mints = state.mints.lock().unwrap();
        *mints += 1;
        let n = *mints;
        let expires_in = *state.expires_in.lock().unwrap();
        let tok = format!("token-{grant_id}-{n}");
        return (
            StatusCode::OK,
            Json(json!({"access_token": tok, "expires_in": expires_in})),
        );
    }

    (
        StatusCode::BAD_REQUEST,
        Json(json!({"error": "unsupported_grant_type"})),
    )
}

async fn handle_create_grant(
    State(state): State<AuthState>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> impl IntoResponse {
    let mut down = state.down_count.lock().unwrap();
    if *down > 0 {
        *down -= 1;
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": "server_error"})),
        );
    }
    drop(down);

    if headers.get("authorization").and_then(|v| v.to_str().ok()) != Some("Bearer service-token") {
        return (
            StatusCode::UNAUTHORIZED,
            Json(json!({"error": "unauthorized"})),
        );
    }
    if let Some(tp) = headers.get("traceparent").and_then(|v| v.to_str().ok()) {
        state.traces.lock().unwrap().push(tp.to_string());
    }

    let mut next = state.next_id.lock().unwrap();
    *next += 1;
    let id = format!("grt_{}", *next);
    let mut grant = body;
    grant["id"] = json!(id);
    state.created.lock().unwrap().push(grant.clone());

    (StatusCode::CREATED, Json(grant))
}

async fn handle_extend_grant(
    State(state): State<AuthState>,
    Path(id): Path<String>,
    headers: HeaderMap,
) -> impl IntoResponse {
    let mut down = state.down_count.lock().unwrap();
    if *down > 0 {
        *down -= 1;
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": "server_error"})),
        );
    }
    drop(down);

    if headers.get("authorization").and_then(|v| v.to_str().ok()) != Some("Bearer service-token") {
        return (
            StatusCode::UNAUTHORIZED,
            Json(json!({"error": "unauthorized"})),
        );
    }
    if let Some(tp) = headers.get("traceparent").and_then(|v| v.to_str().ok()) {
        state.traces.lock().unwrap().push(tp.to_string());
    }

    if state.stopped.lock().unwrap().contains(&id) {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": "invalid_grant"})),
        );
    }
    state.extended.lock().unwrap().push(id.clone());
    (StatusCode::OK, Json(json!({"id": id})))
}

async fn handle_revoke_grant(
    State(state): State<AuthState>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    state.revoked.lock().unwrap().push(id);
    StatusCode::NO_CONTENT
}

async fn handle_chain_grant(
    State(state): State<AuthState>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    *state.chain_reads.lock().unwrap() += 1;
    if state.stopped.lock().unwrap().contains(&id) {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": "invalid_grant"})),
        );
    }
    if state.expired.lock().unwrap().contains(&id) {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": "grant_expired"})),
        );
    }
    let left = if state.ending.lock().unwrap().contains(&id) {
        600
    } else {
        7200
    };
    let ends = jiff::Timestamp::now().as_second() + left;
    let ends = jiff::Timestamp::from_second(ends).unwrap().to_string();
    (
        StatusCode::OK,
        Json(json!({"chain": [{"id": "grt_parent"}, {"id": id, "expires_at": ends}]})),
    )
}

async fn setup_auth_server() -> (String, AuthState) {
    let state = AuthState {
        expires_in: Arc::new(Mutex::new(300)),
        ..Default::default()
    };
    let app = Router::new()
        .route("/v1/token", post(handle_token))
        .route("/v1/grants", post(handle_create_grant))
        .route("/v1/grants/{id}/extend", post(handle_extend_grant))
        .route("/v1/grants/{id}/revoke", post(handle_revoke_grant))
        .route("/v1/grants/{id}/chain", get(handle_chain_grant))
        .with_state(state.clone());

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    (format!("http://{addr}"), state)
}

fn test_store_limits() -> StoreLimits {
    StoreLimits {
        max_sessions_per_tenant: 10_000,
        max_sessions_per_user: 1000,
        max_queued_per_tenant: 128,
        max_queued_per_session: 16,
        max_input_bytes: 32 * 1024,
        restart_limit: 1,
    }
}

fn user_principal(token: &str) -> Principal {
    Principal {
        user_id: "ada".into(),
        tenant_id: "ten_acme".into(),
        scopes: vec!["agent:use".into()],
        expires_at: 9999999999,
        claims: json!({}),
        token: token.into(),
    }
}

fn delegated_principal(grant: &str, scopes: Vec<&str>, aud: Vec<&str>) -> Principal {
    Principal {
        user_id: "ada".into(),
        tenant_id: "ten_acme".into(),
        scopes: scopes.into_iter().map(String::from).collect(),
        expires_at: 9999999999,
        claims: json!({
            "grant": grant,
            "aud": aud,
            "act": {"sub": "svc_livekit"},
        }),
        token: format!("delegated-{grant}"),
    }
}

#[tokio::test]
async fn test_grant_broker_full_lifecycle() {
    let (url, state) = setup_auth_server().await;
    let config = GrantsConfig {
        url,
        client_id: "agent-ti".into(),
        client_secret: "secret".into(),
        actions: vec!["agent:use".into(), "crm:read".into()],
        audiences: vec!["agent-ti".into(), "crm-mcp".into()],
    };
    let broker =
        GrantBroker::new(config, reqwest::Client::new()).with_retry_delay(Duration::from_millis(1));

    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("test.sqlite");
    let store = AgentStore::open(&db_path, "assistant", test_store_limits())
        .await
        .unwrap();
    let scope = UserScope {
        tenant_id: "ten_acme".into(),
        agent_id: "assistant".into(),
        user_id: "ada".into(),
    };
    let session_id = "sess_grant_1";
    store
        .create(CreateSession {
            scope: scope.clone(),
            session_id: session_id.into(),
            request_key: "k1".into(),
            profile: "chat".into(),
            config_version: "test".into(),
            origin: json!({"trigger": "user"}),
            metadata: json!({}),
            child: None,
        })
        .await
        .unwrap();

    let traceparent = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01";
    let p_user = user_principal("user-token-1");

    // 1. First attach creates root grant and passes traceparent
    let first = broker
        .attach(&store, &scope, session_id, &p_user, Some(traceparent))
        .await
        .unwrap();
    assert_eq!(first.grant_id, "grt_1");
    assert_eq!(first.parent, None);
    assert_eq!(state.traces.lock().unwrap().as_slice(), &[traceparent]);

    // 2. Second attach extends root grant
    let second = broker
        .attach(&store, &scope, session_id, &p_user, Some(traceparent))
        .await
        .unwrap();
    assert_eq!(second, first);
    assert_eq!(
        state.extended.lock().unwrap().as_slice(),
        &["grt_1".to_string()]
    );
    assert_eq!(
        state.traces.lock().unwrap().as_slice(),
        &[traceparent, traceparent]
    );

    // 3. Token minting and caching
    let token1 = broker.token(&store, session_id).await.unwrap();
    assert_eq!(token1, "token-grt_1-1");
    // Second token call within TTL returns cached token
    let token2 = broker.token(&store, session_id).await.unwrap();
    assert_eq!(token2, token1);
    assert_eq!(*state.mints.lock().unwrap(), 1);

    // 4. Recovery after broker restart (simulating node crash / process reboot)
    let new_broker = GrantBroker::new(broker.config().clone(), reqwest::Client::new())
        .with_retry_delay(Duration::from_millis(1));
    // The stored grant is recovered from sqlite store!
    let recovered_grant = new_broker.grant_of(&store, session_id).await.unwrap();
    assert_eq!(recovered_grant, first);
    let recovered_token = new_broker.token(&store, session_id).await.unwrap();
    assert_eq!(recovered_token, "token-grt_1-2"); // Minted fresh token using recovered grant id

    // 5. Delegated caller opens a child grant with narrowed scopes/audiences
    let sess_child = "sess_child_1";
    store
        .create(CreateSession {
            scope: scope.clone(),
            session_id: sess_child.into(),
            request_key: "k2".into(),
            profile: "chat".into(),
            config_version: "test".into(),
            origin: json!({"trigger": "user"}),
            metadata: json!({}),
            child: None,
        })
        .await
        .unwrap();

    let p_delegated = delegated_principal(
        "grt_parent",
        vec!["agent:use", "unwanted:scope"],
        vec!["agent-ti", "unwanted-aud"],
    );
    let child_grant = new_broker
        .attach(&store, &scope, sess_child, &p_delegated, None)
        .await
        .unwrap();
    assert_eq!(child_grant.parent, Some("grt_parent".into()));

    // A later turn keeps a child grant with time left, without trying to extend it
    let kept = new_broker
        .attach(&store, &scope, sess_child, &p_delegated, None)
        .await
        .unwrap();
    assert_eq!(kept.grant_id, child_grant.grant_id);
    assert!(
        !state
            .extended
            .lock()
            .unwrap()
            .contains(&child_grant.grant_id)
    );

    // Close to its end, or past it, a turn opens a new one under the same parent
    state
        .ending
        .lock()
        .unwrap()
        .insert(child_grant.grant_id.clone());
    let near_end = new_broker
        .attach(&store, &scope, sess_child, &p_delegated, None)
        .await
        .unwrap();
    assert_ne!(near_end.grant_id, child_grant.grant_id);
    assert_eq!(near_end.parent, Some("grt_parent".into()));
    state
        .expired
        .lock()
        .unwrap()
        .insert(near_end.grant_id.clone());
    let lapsed = new_broker
        .attach(&store, &scope, sess_child, &p_delegated, None)
        .await
        .unwrap();
    assert_ne!(lapsed.grant_id, near_end.grant_id);
    new_broker.mint(&lapsed.grant_id).await.unwrap();

    // A stopped one stays stopped
    state
        .stopped
        .lock()
        .unwrap()
        .insert(lapsed.grant_id.clone());
    assert_eq!(
        new_broker
            .attach(&store, &scope, sess_child, &p_delegated, None)
            .await
            .unwrap_err(),
        GrantError::Stopped
    );
    state.stopped.lock().unwrap().remove(&lapsed.grant_id);
    *state.chain_reads.lock().unwrap() = 0;

    {
        let created = state.created.lock().unwrap();
        let child_created = created
            .iter()
            .find(|g| g["id"] == json!(child_grant.grant_id))
            .unwrap();
        // Narrowed actions & audiences
        assert_eq!(child_created["actions"], json!(["agent:use"]));
        assert_eq!(child_created["audiences"], json!(["agent-ti"]));
    }

    // 6. Confirm chain
    new_broker
        .confirm_chain(&child_grant.grant_id)
        .await
        .unwrap();
    assert_eq!(*state.chain_reads.lock().unwrap(), 1);
    // Cached chain read
    new_broker
        .confirm_chain(&child_grant.grant_id)
        .await
        .unwrap();
    assert_eq!(*state.chain_reads.lock().unwrap(), 1);

    // 7. Revoke & Close
    new_broker.close(session_id).await;
    assert_eq!(
        state.revoked.lock().unwrap().as_slice(),
        &["grt_1".to_string()]
    );
}

#[tokio::test]
async fn test_grant_broker_retries_and_errors() {
    let (url, state) = setup_auth_server().await;
    let config = GrantsConfig {
        url,
        client_id: "agent-ti".into(),
        client_secret: "secret".into(),
        actions: vec!["agent:use".into()],
        audiences: vec!["agent-ti".into()],
    };
    let broker =
        GrantBroker::new(config, reqwest::Client::new()).with_retry_delay(Duration::from_millis(1));

    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("test.sqlite");
    let store = AgentStore::open(&db_path, "assistant", test_store_limits())
        .await
        .unwrap();
    let scope = UserScope {
        tenant_id: "ten_acme".into(),
        agent_id: "assistant".into(),
        user_id: "ada".into(),
    };
    let session_id = "sess_err";
    store
        .create(CreateSession {
            scope: scope.clone(),
            session_id: session_id.into(),
            request_key: "k_err".into(),
            profile: "chat".into(),
            config_version: "test".into(),
            origin: json!({"trigger": "user"}),
            metadata: json!({}),
            child: None,
        })
        .await
        .unwrap();

    // 1. Transient 500 error is retried and succeeds on attempt 2
    *state.down_count.lock().unwrap() = 1;
    let grant = broker
        .attach(&store, &scope, session_id, &user_principal("tok"), None)
        .await
        .unwrap();
    assert_eq!(grant.grant_id, "grt_1");

    // 2. Stopped grant returns GrantError::Stopped
    state.stopped.lock().unwrap().insert("grt_1".into());
    let err = broker.token(&store, session_id).await.unwrap_err();
    assert_eq!(err, GrantError::Stopped);

    // 3. Expired grant returns GrantError::Expired
    state.stopped.lock().unwrap().remove("grt_1");
    state.expired.lock().unwrap().insert("grt_1".into());
    let err2 = broker.token(&store, session_id).await.unwrap_err();
    assert_eq!(err2, GrantError::Expired);
}
