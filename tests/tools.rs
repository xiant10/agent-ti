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

#[tokio::test]
async fn test_store_activate_and_evict_tools() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("test.sqlite");
    let store = AgentStore::open(&db_path, "assistant", limits())
        .await
        .unwrap();
    let s = scope("ten_test", "alice");
    let session_id = "assistant.r0.sess_tools";

    store
        .create(create(s.clone(), session_id, "k1"))
        .await
        .unwrap();

    let policy = ActivationPolicy {
        pinned: vec!["load_skill".into()],
        max_active: 4,
    };

    // 1. Activate ["a", "b"]
    let res = store
        .activate_tools(&s, session_id, &["a".into(), "b".into()], &policy)
        .await
        .unwrap();
    assert_eq!(res.added, vec!["a", "b"]);
    assert!(res.evicted.is_empty());

    // 2. Activate ["a"] again -> refreshes "a", making "b" the oldest
    let res = store
        .activate_tools(&s, session_id, &["a".into()], &policy)
        .await
        .unwrap();
    assert!(res.added.is_empty());
    assert!(res.evicted.is_empty());

    // 3. Activate ["c", "d"] -> room is 4 - 1 = 3. Dynamic has ["b", "a", "c", "d"]. "b" is evicted!
    let res = store
        .activate_tools(&s, session_id, &["c".into(), "d".into()], &policy)
        .await
        .unwrap();
    assert_eq!(res.added, vec!["c", "d"]);
    assert_eq!(res.evicted, vec!["b"]);

    let session_tools = store.get_session_tools(&s, session_id).await.unwrap();
    assert_eq!(session_tools.offered, vec!["load_skill", "a", "c", "d"]);
}

#[tokio::test]
async fn test_store_refresh_skills() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("test_skills.sqlite");
    let store = AgentStore::open(&db_path, "assistant", limits())
        .await
        .unwrap();
    let s = scope("ten_test", "alice");
    let session_id = "assistant.r0.sess_refresh";

    store
        .create(create(s.clone(), session_id, "k1"))
        .await
        .unwrap();

    let policy = ActivationPolicy {
        pinned: vec!["load_skill".into()],
        max_active: 3,
    };

    // Activate skill "s" with ["a"]
    store
        .activate_skill(&s, session_id, "s", &["a".into()], &policy)
        .await
        .unwrap();

    // Refresh when nothing changed
    let res = store
        .refresh_skills(
            &s,
            session_id,
            |name| (name == "s").then(|| vec!["a".into()]),
            &policy,
        )
        .await
        .unwrap();
    assert!(res.added.is_empty());

    // Server listed "b", refresh picks up "b"
    let res = store
        .refresh_skills(
            &s,
            session_id,
            |name| (name == "s").then(|| vec!["a".into(), "b".into()]),
            &policy,
        )
        .await
        .unwrap();
    assert_eq!(res.added, vec!["b"]);

    // Activate ["c"] -> room = 2. Dynamic has ["a", "b", "c"]. "a" is evicted!
    let res = store
        .activate_tools(&s, session_id, &["c".into()], &policy)
        .await
        .unwrap();
    assert_eq!(res.added, vec!["c"]);
    assert_eq!(res.evicted, vec!["a"]);

    // Still knows skill "s" because "b" is still offered
    let tools = store.get_session_tools(&s, session_id).await.unwrap();
    assert_eq!(tools.offered, vec!["load_skill", "b", "c"]);
    assert!(tools.skills.contains_key("s"));

    // Activate ["d", "e"] -> evicts "b" and "c".
    let res = store
        .activate_tools(&s, session_id, &["d".into(), "e".into()], &policy)
        .await
        .unwrap();
    assert_eq!(res.evicted, vec!["b", "c"]);

    // Skill "s" is forgotten because none of its tools are offered!
    let tools = store.get_session_tools(&s, session_id).await.unwrap();
    assert!(!tools.skills.contains_key("s"));
}

struct ScriptModel {
    replies: Mutex<Vec<ModelReply>>,
    seen_requests: Mutex<Vec<ModelRequest>>,
}

impl ScriptModel {
    fn new(replies: Vec<ModelReply>) -> Self {
        Self {
            replies: Mutex::new(replies),
            seen_requests: Mutex::new(Vec::new()),
        }
    }
}

impl ModelPort for ScriptModel {
    fn generate<'a>(
        &'a self,
        request: ModelRequest,
    ) -> BoxFuture<'a, Result<ModelReply, ExecutionError>> {
        Box::pin(async move {
            self.seen_requests.lock().unwrap().push(request);
            let mut list = self.replies.lock().unwrap();
            if list.is_empty() {
                return Err(ExecutionError::InvalidResponse);
            }
            Ok(list.remove(0))
        })
    }
}

struct DynamicHooks {
    tools: Arc<std::sync::RwLock<Vec<ToolDescriptor>>>,
}

impl ExecutionHooks for DynamicHooks {
    fn current_tools(&self) -> BoxFuture<'_, Result<Option<Vec<ToolDescriptor>>, ExecutionError>> {
        Box::pin(async move {
            let tools = self.tools.read().unwrap();
            Ok(Some(tools.clone()))
        })
    }
}

struct MockDispatcher {
    tools: Arc<std::sync::RwLock<Vec<ToolDescriptor>>>,
}

impl ToolPort for MockDispatcher {
    fn invoke<'a>(&'a self, call: ToolCall) -> BoxFuture<'a, Result<ToolOutcome, ExecutionError>> {
        Box::pin(async move {
            if call.name == "load_skill" {
                // Mock load_skill activating crm_list_accounts
                let mut t = self.tools.write().unwrap();
                t.push(ToolDescriptor {
                    name: "crm_list_accounts".into(),
                    description: "List accounts".into(),
                    parameters: json!({"type": "object"}),
                });
                return Ok(ToolOutcome::ok(
                    "Skill instructions. Tools enabled: crm_list_accounts",
                ));
            }
            if call.name == "crm_list_accounts" {
                return Ok(ToolOutcome::ok("[\"Acme\", \"Globex\"]"));
            }
            Ok(ToolOutcome::error("unknown tool"))
        })
    }
}

#[tokio::test]
async fn test_dynamic_tool_activation_across_rounds() {
    let initial_tools = vec![
        ToolDescriptor {
            name: "load_skill".into(),
            description: "Load a skill".into(),
            parameters: json!({"type": "object"}),
        },
        ToolDescriptor {
            name: "search_tools".into(),
            description: "Search tools".into(),
            parameters: json!({"type": "object"}),
        },
    ];

    let shared_tools = Arc::new(std::sync::RwLock::new(initial_tools.clone()));
    let hooks = DynamicHooks {
        tools: shared_tools.clone(),
    };
    let dispatcher = MockDispatcher {
        tools: shared_tools.clone(),
    };

    let script = ScriptModel::new(vec![
        // Round 1: Model calls load_skill
        ModelReply {
            message: Message {
                role: "assistant".into(),
                content: String::new(),
                tool_calls: vec![ToolCall {
                    id: "call_1".into(),
                    name: "load_skill".into(),
                    arguments: json!({"name": "accounts"}),
                }],
                tool_call_id: None,
                is_error: false,
                source: None,
            },
            input_tokens: 10,
            output_tokens: 5,
        },
        // Round 2: Model calls newly activated crm_list_accounts
        ModelReply {
            message: Message {
                role: "assistant".into(),
                content: String::new(),
                tool_calls: vec![ToolCall {
                    id: "call_2".into(),
                    name: "crm_list_accounts".into(),
                    arguments: json!({}),
                }],
                tool_call_id: None,
                is_error: false,
                source: None,
            },
            input_tokens: 15,
            output_tokens: 5,
        },
        // Round 3: Model outputs final answer
        ModelReply {
            message: Message::text("assistant", "You have two accounts: Acme and Globex."),
            input_tokens: 20,
            output_tokens: 10,
        },
    ]);

    let spec = ExecutionSpec {
        instructions: vec!["System prompt".into()],
        model: "gpt".into(),
        max_output_tokens: 1024,
        max_tool_rounds: 5,
        max_parallel_tools: 1,
    };

    let history = vec![Message::text("user", "Show my accounts")];
    let result = execute_with_hooks(
        &spec,
        history,
        initial_tools,
        &script,
        &dispatcher,
        &hooks,
        8192,
    )
    .await
    .unwrap();

    assert_eq!(
        result.messages.last().unwrap().content,
        "You have two accounts: Acme and Globex."
    );

    // Verify round 1 saw only meta-tools, and round 2 saw meta-tools + crm_list_accounts!
    let requests = script.seen_requests.lock().unwrap();
    assert_eq!(requests.len(), 3);
    assert_eq!(
        requests[0]
            .tools
            .iter()
            .map(|t| t.name.as_str())
            .collect::<Vec<_>>(),
        vec!["load_skill", "search_tools"]
    );
    assert_eq!(
        requests[1]
            .tools
            .iter()
            .map(|t| t.name.as_str())
            .collect::<Vec<_>>(),
        vec!["load_skill", "search_tools", "crm_list_accounts"]
    );
}
