use agent_ti_core::*;
use agent_ti_store::*;
use serde_json::json;
use std::sync::{Arc, Mutex};

fn scope(tenant: &str, user: &str) -> UserScope {
    UserScope {
        tenant_id: tenant.into(),
        agent_id: "assistant".into(),
        user_id: user.into(),
    }
}

fn limits() -> StoreLimits {
    StoreLimits {
        max_sessions_per_tenant: 10_000,
        max_sessions_per_user: 1000,
        max_queued_per_tenant: 128,
        max_queued_per_session: 16,
        max_input_bytes: 32 * 1024,
        restart_limit: 1,
    }
}

fn create(s: UserScope, id: &str, key: &str) -> CreateSession {
    CreateSession {
        scope: s,
        session_id: id.into(),
        request_key: key.into(),
        profile: "chat".into(),
        config_version: "test".into(),
        origin: json!({"trigger":"user"}),
        metadata: json!({}),
        child: None,
    }
}

fn submit_prompt(
    s: UserScope,
    id: &str,
    key: &str,
    input: &str,
    when_busy: Option<&str>,
) -> Submit {
    Submit {
        scope: s,
        session_id: id.into(),
        request_key: key.into(),
        fingerprint: digest(&json!({"input": input})),
        input: input.into(),
        turn_id: format!("turn_{key}"),
        traceparent: None,
        reply: None,
        when_busy: when_busy.map(str::to_owned),
    }
}

struct ScriptModel {
    replies: Mutex<Vec<ModelReply>>,
    seen_messages: Mutex<Vec<Vec<Message>>>,
}

impl ScriptModel {
    fn new(replies: Vec<ModelReply>) -> Self {
        Self {
            replies: Mutex::new(replies),
            seen_messages: Mutex::new(Vec::new()),
        }
    }
}

impl ModelPort for ScriptModel {
    fn generate<'a>(
        &'a self,
        request: ModelRequest,
    ) -> BoxFuture<'a, Result<ModelReply, ExecutionError>> {
        self.seen_messages.lock().unwrap().push(request.messages);
        let reply = self.replies.lock().unwrap().remove(0);
        Box::pin(async move { Ok(reply) })
    }
}

struct MockTools;
impl ToolPort for MockTools {
    fn invoke<'a>(&'a self, call: ToolCall) -> BoxFuture<'a, Result<ToolOutcome, ExecutionError>> {
        Box::pin(async move {
            if call.name == "slow_lookup" {
                Ok(ToolOutcome::ok("account_eu_123"))
            } else {
                Ok(ToolOutcome::error("unknown tool"))
            }
        })
    }
}

struct SteerHooks {
    store: Arc<AgentStore>,
    scope: UserScope,
    session_id: String,
}

impl ExecutionHooks for SteerHooks {
    fn post_tools(&self) -> BoxFuture<'_, Result<Vec<Message>, ExecutionError>> {
        Box::pin(async move {
            self.store
                .claim_steered(&self.scope, &self.session_id)
                .await
                .map_err(|_| ExecutionError::Denied)
        })
    }
}

#[tokio::test]
async fn places_a_steer_after_the_current_tool_round() {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(
        AgentStore::open(&dir.path().join("assistant.sqlite"), "assistant", limits())
            .await
            .unwrap(),
    );
    let s = scope("tenant1", "alice");
    let session_id = "assistant.r0.steer_test";
    store
        .create(create(s.clone(), session_id, "c1"))
        .await
        .unwrap();

    // 1. Initial prompt enters
    let p1 = store
        .admit(submit_prompt(
            s.clone(),
            session_id,
            "req1",
            "Look up my account",
            None,
        ))
        .await
        .unwrap();
    assert!(!p1.duplicate);
    assert!(!p1.placed); // Queued for worker

    // Worker claims prompt 1
    let _work = store
        .claim(&p1.command_id)
        .await
        .unwrap()
        .expect("runnable work");

    // 2. While work is running (before or during tool round), client steers
    let steer_sub = submit_prompt(
        s.clone(),
        session_id,
        "req2",
        "Use the EU account",
        Some("steer"),
    );
    let steer_adm = store.admit(steer_sub).await.unwrap();
    assert!(!steer_adm.placed); // Queued behind running work

    // 3. Execution loop runs with tool round
    let script = ScriptModel::new(vec![
        ModelReply {
            message: Message {
                role: "assistant".into(),
                content: String::new(),
                tool_calls: vec![ToolCall {
                    id: "call_1".into(),
                    name: "slow_lookup".into(),
                    arguments: json!({}),
                }],
                tool_call_id: None,
                is_error: false,
                source: None,
            },
            input_tokens: 10,
            output_tokens: 5,
        },
        ModelReply {
            message: Message::text("assistant", "Using the EU account."),
            input_tokens: 20,
            output_tokens: 8,
        },
    ]);

    let hooks = SteerHooks {
        store: store.clone(),
        scope: s.clone(),
        session_id: session_id.into(),
    };

    let spec = ExecutionSpec {
        instructions: vec!["System prompt".into()],
        model: "gpt".into(),
        max_output_tokens: 1024,
        max_tool_rounds: 5,
        max_parallel_tools: 1,
        end_turn_tools: vec![],
        first_tool: None,
    };

    let descriptors = vec![ToolDescriptor {
        name: "slow_lookup".into(),
        description: "Slow lookup".into(),
        parameters: json!({"type": "object"}),
    }];

    let history = vec![Message::text("user", "Look up my account")];
    let result = execute_with_hooks(
        &spec,
        history,
        descriptors,
        &script,
        &MockTools,
        &hooks,
        4096,
    )
    .await
    .unwrap();

    assert_eq!(
        result.messages.last().unwrap().content,
        "Using the EU account."
    );

    // Verify model saw both user messages: original prompt and steered input!
    {
        let seen = script.seen_messages.lock().unwrap();
        assert_eq!(seen.len(), 2);
        let second_round_users: Vec<&str> = seen[1]
            .iter()
            .filter(|m| m.role == "user")
            .map(|m| m.content.as_str())
            .collect();
        assert_eq!(
            second_round_users,
            vec!["Look up my account", "Use the EU account"]
        );
    }

    // Verify transcript in SQLite now includes the steered message
    let transcript = store
        .transcript(&s, session_id, i64::MAX, 10)
        .await
        .unwrap();
    let user_msgs: Vec<&str> = transcript
        .entries
        .iter()
        .filter(|e| e.message.role == "user")
        .map(|e| e.message.content.as_str())
        .collect();
    assert_eq!(user_msgs, vec!["Look up my account", "Use the EU account"]);

    drop(hooks);
    Arc::try_unwrap(store).ok().unwrap().close().await;
}

#[tokio::test]
async fn session_retry_unparks_parked_commands() {
    let dir = tempfile::tempdir().unwrap();
    let store = AgentStore::open(&dir.path().join("assistant.sqlite"), "assistant", limits())
        .await
        .unwrap();
    let s = scope("tenant1", "alice");
    let session_id = "assistant.r0.retry_test";
    store
        .create(create(s.clone(), session_id, "c1"))
        .await
        .unwrap();

    let p1 = store
        .admit(submit_prompt(
            s.clone(),
            session_id,
            "req1",
            "failing work",
            None,
        ))
        .await
        .unwrap();

    // First claim starts the work
    let work = store
        .claim(&p1.command_id)
        .await
        .unwrap()
        .expect("runnable");
    assert_eq!(work.command_id, p1.command_id);

    // Simulate process crash without finish -> recover increments stalls
    let recovered = store.recover().await.unwrap();
    assert_eq!(recovered, 1);

    // Second claim hits restart_limit (which is 1) and parks the command
    let second = store.claim(&p1.command_id).await.unwrap();
    assert!(second.is_none());

    // Check session shows parked == 1
    let st = store.state(&s, session_id).await.unwrap();
    assert_eq!(st.session.parked, 1);
    assert!(st.busy);

    // Calling retry unparks the command
    let retried = store.retry(&s, session_id).await.unwrap();
    assert_eq!(retried, 1);

    // Session state now shows parked == 0
    let st2 = store.state(&s, session_id).await.unwrap();
    assert_eq!(st2.session.parked, 0);

    // Command is now runnable again!
    let runnable = store.runnable(10).await.unwrap();
    assert_eq!(runnable.len(), 1);
    assert_eq!(runnable[0].command_id, p1.command_id);

    // Subsequent retry returns 0 retried
    let retried_zero = store.retry(&s, session_id).await.unwrap();
    assert_eq!(retried_zero, 0);

    store.close().await;
}

#[tokio::test]
async fn withdraw_pending_and_cancel_give_up_on_parked_and_queued_commands() {
    let dir = tempfile::tempdir().unwrap();
    let store = AgentStore::open(&dir.path().join("assistant.sqlite"), "assistant", limits())
        .await
        .unwrap();
    let s = scope("tenant1", "alice");
    let session_id = "assistant.r0.cancel_test";
    store
        .create(create(s.clone(), session_id, "c1"))
        .await
        .unwrap();

    // Admit two commands: one running, one queued
    let p1 = store
        .admit(submit_prompt(s.clone(), session_id, "req1", "run 1", None))
        .await
        .unwrap();
    let _p2 = store
        .admit(submit_prompt(s.clone(), session_id, "req2", "run 2", None))
        .await
        .unwrap();

    let _claimed = store.claim(&p1.command_id).await.unwrap().expect("work");

    // Cancel running work
    let cancelled_running = store.cancel(&s, session_id, "cancel_key_1").await.unwrap();
    assert_eq!(cancelled_running, vec![p1.command_id.clone()]);

    // Withdraw queued work
    let withdrawn = store.withdraw_pending(&s, session_id).await.unwrap();
    assert_eq!(withdrawn.len(), 1);

    // Check no commands are runnable
    let runnable = store.runnable(10).await.unwrap();
    assert!(runnable.is_empty());

    store.close().await;
}
