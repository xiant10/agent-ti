use agent_ti_core::*;
use agent_ti_plugin_sdk::*;
use agent_ti_store::*;
use serde_json::{Value, json};
use std::sync::{Arc, Mutex};

// ---------------------------------------------------------------------------
// Helpers & Mock Harness
// ---------------------------------------------------------------------------

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

struct MockTools {
    calls: Mutex<Vec<ToolCall>>,
}

impl MockTools {
    fn new() -> Self {
        Self {
            calls: Mutex::new(Vec::new()),
        }
    }
}

impl ToolPort for MockTools {
    fn invoke<'a>(&'a self, call: ToolCall) -> BoxFuture<'a, Result<ToolOutcome, ExecutionError>> {
        self.calls.lock().unwrap().push(call.clone());
        Box::pin(async move {
            if call.name == "read_secret" {
                Ok(ToolOutcome::ok("s3cr3t"))
            } else if call.name == "echo" {
                let text = call.arguments["text"].as_str().unwrap_or("");
                Ok(ToolOutcome::ok(format!("token=abc {text}")))
            } else if call.name == "order_status" {
                let order = call.arguments["order"].as_str().unwrap_or("");
                Ok(ToolOutcome::ok(format!("Order {order} shipped.")))
            } else {
                Ok(ToolOutcome::error(format!("Unknown tool: {}", call.name)))
            }
        })
    }
}

fn reply_text(text: &str) -> ModelReply {
    ModelReply {
        message: Message::text("assistant", text),
        input_tokens: 10,
        output_tokens: 5,
    }
}

fn dialogue(messages: &[Message]) -> Vec<String> {
    messages
        .iter()
        .filter(|m| m.role == "user" || m.role == "assistant")
        .map(|m| format!("{}: {}", m.role, m.content))
        .collect()
}

// ---------------------------------------------------------------------------
// 1. before_prompt: answers without model & model sees exchange in next turn
// ---------------------------------------------------------------------------

struct HoursPlugin;
impl Plugin for HoursPlugin {
    fn name(&self) -> &str {
        "hours"
    }
    fn before_prompt<'a>(
        &'a self,
        turn: &'a Turn<'a>,
    ) -> BoxFuture<'a, Result<Option<PromptDecision>, String>> {
        Box::pin(async move {
            if turn.text.to_lowercase().contains("opening hours") {
                Ok(Some(PromptDecision::Reply(
                    "We are open nine to five.".into(),
                )))
            } else {
                Ok(None)
            }
        })
    }
}

#[tokio::test]
async fn before_prompt_answers_without_model_and_model_sees_exchange_in_next_turn() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("hours.sqlite");
    let limits = StoreLimits {
        max_sessions_per_tenant: 100,
        max_sessions_per_user: 10,
        max_queued_per_tenant: 50,
        max_queued_per_session: 10,
        max_input_bytes: 4096,
        restart_limit: 2,
    };
    let store = AgentStore::open(&path, "test", limits).await.unwrap();
    let scope = UserScope {
        tenant_id: "ten_test".into(),
        agent_id: "test".into(),
        user_id: "alice".into(),
    };
    let (session, _) = store
        .create(CreateSession {
            scope: scope.clone(),
            session_id: "test.inst.1".into(),
            request_key: "req1".into(),
            profile: "chat".into(),
            config_version: "v1".into(),
            origin: json!({"trigger": "user"}),
            metadata: json!({}),
        })
        .await
        .unwrap();

    let plugin = HoursPlugin;
    let turn = Turn {
        text: "What are your opening hours?",
        agent: "test",
        session_id: &session.session_id,
        tenant_id: &scope.tenant_id,
        user_id: &scope.user_id,
    };
    let decision = plugin.before_prompt(&turn).await.unwrap();
    assert_eq!(
        decision,
        Some(PromptDecision::Reply("We are open nine to five.".into()))
    );

    // Admit the plugin reply to the store
    let adm = store
        .admit(Submit {
            scope: scope.clone(),
            session_id: session.session_id.clone(),
            request_key: "prompt1".into(),
            fingerprint: "fp1".into(),
            input: "What are your opening hours?".into(),
            turn_id: "turn1".into(),
            traceparent: None,
            reply: Some(Reply {
                text: "We are open nine to five.".into(),
                source: "hours".into(),
            }),
            when_busy: None,
        })
        .await
        .unwrap();
    assert!(adm.placed);

    // Next turn prompt: "Weekends too?" -> model sees dialogue
    let work = Work {
        scope: scope.clone(),
        session_id: session.session_id.clone(),
        command_id: "cmd2".into(),
        turn_id: "turn2".into(),
        input: "Weekends too?".into(),
        profile: "chat".into(),
        traceparent: None,
        attempt: 0,
        reply: None,
        cancel_requested: false,
    };
    let mut history = store.history(&work).await.unwrap();
    history.push(Message::text("user", "Weekends too?"));

    let script = ScriptModel::new(vec![reply_text("Yes, on Saturdays too.")]);
    let spec = ExecutionSpec {
        instructions: vec!["You are a helpful assistant.".into()],
        model: "test-model".into(),
        max_output_tokens: 128,
        max_tool_rounds: 2,
        max_parallel_tools: 1,
    };
    let res = execute(&spec, history, vec![], &script, &MockTools::new(), 4096)
        .await
        .unwrap();
    assert_eq!(
        res.messages.last().unwrap().content,
        "Yes, on Saturdays too."
    );

    let seen = script.seen_messages.lock().unwrap();
    assert_eq!(
        dialogue(&seen[0]),
        vec![
            "user: What are your opening hours?",
            "assistant: We are open nine to five.",
            "user: Weekends too?",
        ]
    );
}

// ---------------------------------------------------------------------------
// 2. before_prompt: rewrites input, activates tools, and rejects with 422
// ---------------------------------------------------------------------------

struct OrdersPlugin;
impl Plugin for OrdersPlugin {
    fn name(&self) -> &str {
        "orders"
    }
    fn before_prompt<'a>(
        &'a self,
        turn: &'a Turn<'a>,
    ) -> BoxFuture<'a, Result<Option<PromptDecision>, String>> {
        Box::pin(async move {
            if turn.text.contains("password") {
                return Ok(Some(PromptDecision::Reject(
                    "Never share passwords here.".into(),
                )));
            }
            if turn.text.to_lowercase().contains("order") {
                let re = regex::Regex::new(r"\d{16}").unwrap();
                let redacted = re.replace_all(turn.text, "[card]").to_string();
                return Ok(Some(PromptDecision::InputAndTools {
                    text: redacted,
                    activate_tools: vec!["orders:order_status".into()],
                }));
            }
            Ok(None)
        })
    }
}

#[tokio::test]
async fn before_prompt_rewrites_input_and_rejects() {
    let plugin = OrdersPlugin;
    let turn_reject = Turn {
        text: "my password is hunter2",
        agent: "test",
        session_id: "s1",
        tenant_id: "t1",
        user_id: "u1",
    };
    let dec1 = plugin.before_prompt(&turn_reject).await.unwrap();
    assert_eq!(
        dec1,
        Some(PromptDecision::Reject("Never share passwords here.".into()))
    );

    let turn_rewrite = Turn {
        text: "Order 42, card 4111111111111111",
        agent: "test",
        session_id: "s1",
        tenant_id: "t1",
        user_id: "u1",
    };
    let dec2 = plugin.before_prompt(&turn_rewrite).await.unwrap();
    assert_eq!(
        dec2,
        Some(PromptDecision::InputAndTools {
            text: "Order 42, card [card]".into(),
            activate_tools: vec!["orders:order_status".into()],
        })
    );
}

// ---------------------------------------------------------------------------
// 3. plugin replies while busy: whenBusy: reject returns Conflict (409)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn when_busy_reject_returns_conflict() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("busy.sqlite");
    let limits = StoreLimits {
        max_sessions_per_tenant: 100,
        max_sessions_per_user: 10,
        max_queued_per_tenant: 50,
        max_queued_per_session: 10,
        max_input_bytes: 4096,
        restart_limit: 2,
    };
    let store = AgentStore::open(&path, "test", limits).await.unwrap();
    let scope = UserScope {
        tenant_id: "ten_test".into(),
        agent_id: "test".into(),
        user_id: "alice".into(),
    };
    let (session, _) = store
        .create(CreateSession {
            scope: scope.clone(),
            session_id: "test.inst.busy".into(),
            request_key: "k1".into(),
            profile: "chat".into(),
            config_version: "v1".into(),
            origin: json!({"trigger": "user"}),
            metadata: json!({}),
        })
        .await
        .unwrap();

    // Admit a model prompt that stays queued/running
    let adm1 = store
        .admit(Submit {
            scope: scope.clone(),
            session_id: session.session_id.clone(),
            request_key: "p1".into(),
            fingerprint: "fp1".into(),
            input: "Slow lookup".into(),
            turn_id: "t1".into(),
            traceparent: None,
            reply: None,
            when_busy: Some("queue".into()),
        })
        .await
        .unwrap();
    assert_eq!(adm1.placement_string(), "queued");

    // Second prompt with whenBusy: reject should fail with StoreError::Conflict
    let err = store
        .admit(Submit {
            scope: scope.clone(),
            session_id: session.session_id.clone(),
            request_key: "p2".into(),
            fingerprint: "fp2".into(),
            input: "ping".into(),
            turn_id: "t2".into(),
            traceparent: None,
            reply: Some(Reply {
                text: "pong".into(),
                source: "desk".into(),
            }),
            when_busy: Some("reject".into()),
        })
        .await
        .unwrap_err();
    assert_eq!(err, StoreError::Conflict);
}

trait AdmissionExt {
    fn placement_string(&self) -> &'static str;
}
impl AdmissionExt for Admission {
    fn placement_string(&self) -> &'static str {
        if self.placed { "placed" } else { "queued" }
    }
}

// ---------------------------------------------------------------------------
// 4. Session lifecycle: greeting via history.say(), state.set/get(), records
// ---------------------------------------------------------------------------

struct MockPluginState {
    value: Mutex<Option<Value>>,
}
impl PluginState for MockPluginState {
    fn get(&self) -> BoxFuture<'_, Result<Option<Value>, String>> {
        Box::pin(async move { Ok(self.value.lock().unwrap().clone()) })
    }
    fn set(&self, value: Value) -> BoxFuture<'_, Result<(), String>> {
        *self.value.lock().unwrap() = Some(value);
        Box::pin(async { Ok(()) })
    }
}

struct MockSessionHistory {
    messages: Mutex<Vec<Message>>,
}
impl SessionHistory for MockSessionHistory {
    fn say(&self, text: &str) -> BoxFuture<'_, Result<(), String>> {
        self.messages.lock().unwrap().push(Message {
            role: "assistant".into(),
            content: text.into(),
            tool_calls: vec![],
            tool_call_id: None,
            is_error: false,
            source: Some("greeter".into()),
        });
        Box::pin(async { Ok(()) })
    }
    fn exchange(&self, input: &str, answer: &str) -> BoxFuture<'_, Result<(), String>> {
        let mut m = self.messages.lock().unwrap();
        m.push(Message::text("user", input));
        m.push(Message::text("assistant", answer));
        Box::pin(async { Ok(()) })
    }
    fn record(&self, record_type: &str, data: Value) -> BoxFuture<'_, Result<(), String>> {
        self.messages.lock().unwrap().push(Message::record(
            "greeter",
            json!({"type": record_type, "data": data}).to_string(),
        ));
        Box::pin(async { Ok(()) })
    }
}

struct GreeterPlugin;
impl Plugin for GreeterPlugin {
    fn name(&self) -> &str {
        "greeter"
    }
    fn rpc_methods(&self) -> Vec<String> {
        vec!["greeter.state".into()]
    }
    fn call_rpc<'a>(
        &'a self,
        method: &'a str,
        _params: Value,
        ctx: &'a RpcContext<'a>,
    ) -> BoxFuture<'a, Result<Value, String>> {
        Box::pin(async move {
            if method == "greeter.state"
                && let Some(state) = &ctx.state
            {
                let val = state.get().await?.unwrap_or(Value::Null);
                return Ok(val);
            }
            Err("unknown method".into())
        })
    }
    fn session_created<'a>(
        &'a self,
        session: &'a SessionContext<'a>,
    ) -> BoxFuture<'a, Result<(), String>> {
        Box::pin(async move {
            session.state.set(json!({"greeted": true})).await?;
            session
                .history
                .say(&format!("Hello {}, how can I help?", session.user_id))
                .await?;
            session
                .history
                .record("crm.interaction", json!({"channel": session.profile}))
                .await?;
            Ok(())
        })
    }
}

#[tokio::test]
async fn session_created_greets_without_model_and_records_are_excluded_from_model_context() {
    let state = Arc::new(MockPluginState {
        value: Mutex::new(None),
    });
    let history = Arc::new(MockSessionHistory {
        messages: Mutex::new(Vec::new()),
    });
    let ctx = SessionContext {
        agent: "test",
        session_id: "s1",
        tenant_id: "t1",
        user_id: "alice",
        profile: "chat",
        state: state.clone(),
        history: history.clone(),
    };

    let greeter = GreeterPlugin;
    greeter.session_created(&ctx).await.unwrap();

    // Check RPC state retrieval
    let rpc_ctx = RpcContext {
        agent: "test",
        tenant_id: "t1",
        user_id: "alice",
        session_id: Some("s1"),
        state: Some(state),
        history: Some(history.clone()),
    };
    let st = greeter
        .call_rpc("greeter.state", json!({}), &rpc_ctx)
        .await
        .unwrap();
    assert_eq!(st, json!({"greeted": true}));

    // Check that context() includes the assistant greeting, but EXCLUDES the record!
    let mut msgs = history.messages.lock().unwrap().clone();
    msgs.push(Message::text("user", "Book a table"));

    let model_ctx = context(&["Instructions".into()], &msgs, 4096).unwrap();
    assert_eq!(
        dialogue(&model_ctx),
        vec![
            "assistant: Hello alice, how can I help?",
            "user: Book a table",
        ]
    );
}

// ---------------------------------------------------------------------------
// 5. Model & tool hooks: persona instruction, before_model_request,
//    on_final_answer, before_tool_call, after_tool_call
// ---------------------------------------------------------------------------

struct TestReqCtx {
    memos: Mutex<std::collections::HashMap<String, Value>>,
}
impl RequestContext for TestReqCtx {
    fn user_id(&self) -> &str {
        "alice"
    }
    fn memo<'a>(
        &'a self,
        name: &'a str,
        compute: Box<dyn FnOnce() -> Value + Send + 'a>,
    ) -> BoxFuture<'a, Result<Value, String>> {
        let mut map = self.memos.lock().unwrap();
        let val = map.entry(name.to_string()).or_insert_with(compute).clone();
        Box::pin(async move { Ok(val) })
    }
}

struct PolicyPlugin;
impl Plugin for PolicyPlugin {
    fn name(&self) -> &str {
        "policy"
    }
    fn instructions(&self, profile: &str) -> Vec<PluginSection> {
        vec![PluginSection {
            name: "persona".into(),
            content: format!("You are Ava, on the {profile} channel."),
            tag: true,
        }]
    }
    fn before_model_request<'a>(
        &'a self,
        mut messages: Vec<Message>,
        ctx: &'a dyn RequestContext,
    ) -> BoxFuture<'a, Result<Vec<Message>, String>> {
        Box::pin(async move {
            messages.push(Message::text(
                "user",
                format!("<context>user={}</context>", ctx.user_id()),
            ));
            Ok(messages)
        })
    }
    fn on_final_answer<'a>(
        &'a self,
        answer: &'a Message,
        ctx: &'a dyn RequestContext,
    ) -> BoxFuture<'a, Result<Option<String>, String>> {
        Box::pin(async move {
            if answer.content.contains("guarantee") {
                let _ = ctx.memo("retried", Box::new(|| json!(true))).await?;
                return Ok(Some("Do not promise guarantees; rephrase.".into()));
            }
            Ok(None)
        })
    }
    fn before_tool_call<'a>(
        &'a self,
        call: &'a ToolCall,
        _ctx: &'a dyn RequestContext,
    ) -> BoxFuture<'a, Result<Option<ToolCallDecision>, String>> {
        Box::pin(async move {
            if call.name == "read_secret" {
                return Ok(Some(ToolCallDecision::Block("Not allowed.".into())));
            }
            Ok(None)
        })
    }
    fn after_tool_call(&self, _call: &ToolCall, result: ToolOutcome) -> ToolOutcome {
        let re = regex::Regex::new(r"token=\S+").unwrap();
        ToolOutcome {
            content: re.replace_all(&result.content, "token=***").into_owned(),
            ..result
        }
    }
}

struct PolicyHooks<'a> {
    plugin: &'a PolicyPlugin,
    ctx: &'a TestReqCtx,
}
impl ExecutionHooks for PolicyHooks<'_> {
    fn on_final_answer<'a>(
        &'a self,
        answer: &'a Message,
    ) -> BoxFuture<'a, Result<Option<String>, ExecutionError>> {
        Box::pin(async move {
            self.plugin
                .on_final_answer(answer, self.ctx)
                .await
                .map_err(|_| ExecutionError::Denied)
        })
    }
}

struct PolicyDispatcher<'a> {
    tools: &'a MockTools,
    plugin: &'a PolicyPlugin,
    ctx: &'a TestReqCtx,
}
impl ToolPort for PolicyDispatcher<'_> {
    fn invoke<'a>(
        &'a self,
        mut call: ToolCall,
    ) -> BoxFuture<'a, Result<ToolOutcome, ExecutionError>> {
        Box::pin(async move {
            if let Ok(Some(ToolCallDecision::Block(reason))) =
                self.plugin.before_tool_call(&call, self.ctx).await
            {
                return Ok(ToolOutcome::error(reason));
            }
            if let Ok(Some(ToolCallDecision::Arguments(args))) =
                self.plugin.before_tool_call(&call, self.ctx).await
            {
                call.arguments = args;
            }
            let res = self.tools.invoke(call.clone()).await?;
            Ok(self.plugin.after_tool_call(&call, res))
        })
    }
}

#[tokio::test]
async fn model_and_tool_hooks_pipeline() {
    let policy = PolicyPlugin;
    let req_ctx = TestReqCtx {
        memos: Mutex::new(std::collections::HashMap::new()),
    };

    // 1. Instructions: persona section
    let sections = policy.instructions("chat");
    assert_eq!(sections[0].name, "persona");
    assert_eq!(sections[0].content, "You are Ava, on the chat channel.");

    // 2. Mock model script:
    // Turn 1: tool calls (read_secret, echo)
    // Turn 2: answer with "guarantee" -> triggers on_final_answer continuation
    // Turn 3: clean final answer
    let script = ScriptModel::new(vec![
        ModelReply {
            message: Message {
                role: "assistant".into(),
                content: String::new(),
                tool_calls: vec![
                    ToolCall {
                        id: "c1".into(),
                        name: "read_secret".into(),
                        arguments: json!({}),
                    },
                    ToolCall {
                        id: "c2".into(),
                        name: "echo".into(),
                        arguments: json!({"text": "hi"}),
                    },
                ],
                tool_call_id: None,
                is_error: false,
                source: None,
            },
            input_tokens: 10,
            output_tokens: 5,
        },
        reply_text("I guarantee it works."),
        reply_text("It should work."),
    ]);

    let mock_tools = MockTools::new();
    let dispatcher = PolicyDispatcher {
        tools: &mock_tools,
        plugin: &policy,
        ctx: &req_ctx,
    };
    let hooks = PolicyHooks {
        plugin: &policy,
        ctx: &req_ctx,
    };

    let spec = ExecutionSpec {
        instructions: vec!["System prompt".into()],
        model: "main".into(),
        max_output_tokens: 64,
        max_tool_rounds: 5,
        max_parallel_tools: 2,
    };

    let history = vec![Message::text("user", "Do it")];
    let descriptors = vec![
        ToolDescriptor {
            name: "read_secret".into(),
            description: "read secret".into(),
            parameters: json!({"type":"object"}),
        },
        ToolDescriptor {
            name: "echo".into(),
            description: "echo text".into(),
            parameters: json!({"type":"object"}),
        },
    ];

    let result = execute_with_hooks(
        &spec,
        history,
        descriptors,
        &script,
        &dispatcher,
        &hooks,
        4096,
    )
    .await
    .unwrap();

    // Check final settled answer
    assert_eq!(result.messages.last().unwrap().content, "It should work.");

    // Verify tool gating: read_secret was BLOCKED, echo had its token redacted
    let tool_res_1 = result
        .messages
        .iter()
        .find(|m| m.tool_call_id.as_deref() == Some("c1"))
        .unwrap();
    assert!(tool_res_1.is_error);
    assert_eq!(tool_res_1.content, "Not allowed.");

    let tool_res_2 = result
        .messages
        .iter()
        .find(|m| m.tool_call_id.as_deref() == Some("c2"))
        .unwrap();
    assert!(!tool_res_2.is_error);
    assert_eq!(tool_res_2.content, "token=*** hi");

    // Verify that the continuation message was injected by on_final_answer
    let cont = result
        .messages
        .iter()
        .find(|m| m.role == "user" && m.content.contains("Do not promise guarantees"))
        .unwrap();
    assert_eq!(cont.content, "Do not promise guarantees; rephrase.");
}

// ---------------------------------------------------------------------------
// 6. authenticate hook: maps roles / adds scopes
// ---------------------------------------------------------------------------

struct VipAuthPlugin;
impl Plugin for VipAuthPlugin {
    fn name(&self) -> &str {
        "vip_auth"
    }
    fn authenticate<'a>(
        &'a self,
        mut principal: PrincipalInfo,
    ) -> BoxFuture<'a, Result<PrincipalInfo, String>> {
        principal.scopes.push("vip".into());
        Box::pin(async move { Ok(principal) })
    }
}

#[tokio::test]
async fn authenticate_hook_adds_scopes() {
    let plugin = VipAuthPlugin;
    let initial = PrincipalInfo {
        user_id: "alice".into(),
        tenant_id: "ten1".into(),
        scopes: vec!["read".into()],
        claims: json!({}),
    };
    let updated = plugin.authenticate(initial).await.unwrap();
    assert_eq!(updated.scopes, vec!["read", "vip"]);
}

// ---------------------------------------------------------------------------
// 7. map_event: modifies speech, drops thinking delta
// ---------------------------------------------------------------------------

struct PronouncePlugin;
impl Plugin for PronouncePlugin {
    fn name(&self) -> &str {
        "pronounce"
    }
    fn map_event(&self, event: Value) -> Option<Value> {
        let t = event.get("type")?.as_str()?;
        if t == "speech" {
            let text = event["text"].as_str().unwrap_or("").replace("ACME", "Acme");
            let mut modified = event.clone();
            modified["text"] = json!(text);
            return Some(modified);
        }
        if t == "thinking.delta" {
            return None;
        }
        Some(event)
    }
}

#[test]
fn map_event_transforms_and_filters() {
    let plugin = PronouncePlugin;
    let speech = json!({"type": "speech", "text": "Welcome to ACME support."});
    let mapped = plugin.map_event(speech).unwrap();
    assert_eq!(mapped["text"], "Welcome to Acme support.");

    let thinking = json!({"type": "thinking.delta", "text": "pondering"});
    assert!(plugin.map_event(thinking).is_none());

    let delta = json!({"type": "text.delta", "text": "hello"});
    assert!(plugin.map_event(delta).is_some());
}
