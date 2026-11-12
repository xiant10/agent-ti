use agent_ti::adapters;
use agent_ti_config::{ModelConfig, Retry};
use agent_ti_core::{ExecutionError, Message, ModelRequest};
use agent_ti_store::{UserScope, Work};
use axum::{
    Router,
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::post,
};
use std::sync::{Arc, Mutex};
use std::time::Duration;

const ANSWER: &str = "data: {\"choices\":[{\"delta\":{\"content\":\"Hello\"}}]}\n\ndata: {\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n";

/// A model that answers each request with the next of its scripted replies, the last one forever.
#[derive(Clone)]
struct Script {
    replies: Arc<Vec<(StatusCode, &'static str)>>,
    requests: Arc<Mutex<usize>>,
}

async fn complete(State(script): State<Script>) -> Response {
    let n = {
        let mut requests = script.requests.lock().unwrap();
        *requests += 1;
        *requests
    };
    let (status, body) = script.replies[(n - 1).min(script.replies.len() - 1)];
    (status, [("content-type", "text/event-stream")], body).into_response()
}

#[derive(Default)]
struct Sink {
    text: String,
    retries: Vec<(u32, String)>,
}
impl adapters::DeltaSink for Sink {
    fn text(&mut self, text: &str) {
        self.text.push_str(text);
    }
    fn thinking(&mut self, _: &str) {}
    fn retry(&mut self, attempt: u32, _at: u64, error: &str) {
        self.retries.push((attempt, error.to_owned()));
    }
}

async fn run(
    replies: Vec<(StatusCode, &'static str)>,
    max_retries: u32,
) -> (Result<String, ExecutionError>, Sink, usize) {
    let script = Script {
        replies: Arc::new(replies),
        requests: Arc::default(),
    };
    let app = Router::new()
        .route("/v1/chat/completions", post(complete))
        .with_state(script.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let config = ModelConfig {
        model_id: "test".into(),
        base_url: format!("http://127.0.0.1:{}/v1", addr.port()),
        api_key_env: None,
        reasoning: false,
        context_window: None,
        max_output_tokens: None,
        identity_headers: Default::default(),
    };
    let retry = Retry {
        max_retries,
        base_delay_ms: 10,
        max_delay_ms: 100,
    };
    let call = adapters::ModelCall {
        config: &config,
        agent: "test",
        thinking_level: "off",
        timeout: Duration::from_secs(5),
        run_token: None,
        retry: &retry,
    };
    let work = Work {
        scope: UserScope {
            tenant_id: "t".into(),
            agent_id: "a".into(),
            user_id: "u".into(),
        },
        session_id: "s".into(),
        command_id: "c".into(),
        turn_id: "turn".into(),
        input: "hi".into(),
        profile: "chat".into(),
        traceparent: None,
        attempt: 0,
        reply: None,
        cancel_requested: false,
        child: None,
    };
    let request = ModelRequest {
        model: "chat".into(),
        messages: vec![Message::text("user", "hi")],
        tools: vec![],
        max_output_tokens: 100,
        answer_only: false,
        force_tool: None,
    };
    let mut sink = Sink::default();
    let reply = adapters::model(&reqwest::Client::new(), call, &work, request, &mut sink)
        .await
        .map(|r| r.message.content);
    let requests = *script.requests.lock().unwrap();
    (reply, sink, requests)
}

#[tokio::test]
async fn a_busy_provider_is_tried_again_until_it_answers() {
    let (reply, sink, requests) = run(
        vec![
            (StatusCode::SERVICE_UNAVAILABLE, "busy"),
            (StatusCode::TOO_MANY_REQUESTS, "slow down"),
            (StatusCode::OK, ANSWER),
        ],
        3,
    )
    .await;
    assert_eq!(reply.unwrap(), "Hello");
    assert_eq!(requests, 3);
    assert_eq!(sink.text, "Hello");
    assert_eq!(
        sink.retries.iter().map(|(a, _)| *a).collect::<Vec<_>>(),
        [1, 2]
    );
    assert!(
        sink.retries[0].1.starts_with("503"),
        "{}",
        sink.retries[0].1
    );
}

#[tokio::test]
async fn retries_stop_at_the_limit() {
    let (reply, sink, requests) = run(vec![(StatusCode::BAD_GATEWAY, "down")], 2).await;
    assert!(matches!(reply, Err(ExecutionError::Unavailable)));
    assert_eq!(requests, 3);
    assert_eq!(sink.retries.len(), 2);
}

#[tokio::test]
async fn a_refused_request_is_not_tried_again() {
    let (reply, sink, requests) = run(vec![(StatusCode::UNAUTHORIZED, "no")], 3).await;
    assert!(matches!(reply, Err(ExecutionError::Unavailable)));
    assert_eq!(requests, 1);
    assert!(sink.retries.is_empty());
}

#[tokio::test]
async fn an_answer_cut_off_after_its_text_was_shown_is_not_tried_again() {
    // Text, then the stream ends with no finish: what was shown can't be taken back.
    let cut = "data: {\"choices\":[{\"delta\":{\"content\":\"Hel\"}}]}\n\n";
    let (reply, sink, requests) =
        run(vec![(StatusCode::OK, cut), (StatusCode::OK, ANSWER)], 3).await;
    assert!(matches!(reply, Err(ExecutionError::Unavailable)));
    assert_eq!(requests, 1);
    assert_eq!(sink.text, "Hel");
}

#[tokio::test]
async fn an_answer_cut_off_before_any_text_is_tried_again() {
    let empty = "data: {\"choices\":[{\"delta\":{}}]}\n\n";
    let (reply, _, requests) =
        run(vec![(StatusCode::OK, empty), (StatusCode::OK, ANSWER)], 3).await;
    assert_eq!(reply.unwrap(), "Hello");
    assert_eq!(requests, 2);
}
